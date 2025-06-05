#include "ix_index_handle.h"

#include "ix_scan.h"

IxIndexHandle::IxIndexHandle(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, int fd)
    : disk_manager_(disk_manager), buffer_pool_manager_(buffer_pool_manager), fd_(fd) {
    // init file_hdr_
    disk_manager_->read_page(fd, IX_FILE_HDR_PAGE, (char *)&file_hdr_, sizeof(file_hdr_));
    // disk_manager管理的fd对应的文件中，设置从原来编号+1开始分配page_no
    disk_manager_->set_fd2pageno(fd, disk_manager_->get_fd2pageno(fd) + 1);
}

/**
 * @brief 用于查找指定键所在的叶子结点
 *
 * @param key 要查找的目标key值
 * @param operation 查找到目标键值对后要进行的操作类型
 * @param transaction 事务参数，如果不需要则默认传入nullptr
 * @return 返回目标叶子结点
 * @note 需要在外部取消固定叶子节点!
 */
IxNodeHandle *IxIndexHandle::FindLeafPage(const char *key, Operation operation, Transaction *transaction) {
    // Todo:
    // 1. 获取根节点
    // 2. 从根节点开始不断向下查找目标key
    // 3. 找到包含该key值的叶子结点停止查找，并返回叶子节点

    IxNodeHandle* node = this->FetchNode(this->file_hdr_.root_page);  // 获取根节点
    while(!node->IsLeafPage()) {
        page_id_t page_no = node->InternalLookup(key);  // 内部查找目标键值对应的页号
        buffer_pool_manager_->UnpinPage(node->GetPageId(), false);  // 取消固定当前节点
        node = this->FetchNode(page_no);  // 获取下一层子树节点，继续查找
    }
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);  // 取消固定最终找到的叶子节点
    return node;  // 返回找到的叶子节点
}

/**
 * @brief 用于查找指定键在叶子结点中的对应的值result
 *
 * @param key 查找的目标key值
 * @param result 用于存放结果的容器
 * @param transaction 事务指针
 * @return bool 返回目标键值对是否存在
 */
bool IxIndexHandle::GetValue(const char *key, std::vector<Rid> *result, Transaction *transaction) {
    // Todo:
    // 1. 获取目标key值所在的叶子结点
    // 2. 在叶子节点中查找目标key值的位置，并读取key对应的rid
    // 3. 把rid存入result参数中
    // 提示：使用完buffer_pool提供的page之后，记得unpin page；记得处理并发的上锁
    std::scoped_lock lock{root_latch_};  // 加锁保证并发安全

    IxNodeHandle* leaf_node = FindLeafPage(key, Operation::FIND, transaction);  // 获取目标key所在的叶子结点
    Rid* rid;
    bool value = leaf_node->LeafLookup(key, &rid);  // 在叶子结点中查找目标key对应的rid
    if(value) {
        result->push_back(*rid);  // 将找到的rid存入结果容器中
    }
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);  // 取消固定叶子结点
    return value;  // 返回是否成功找到目标键值对
}

/**
 * @brief 将指定键值对插入到B+树中
 *
 * @param (key, value) 要插入的键值对
 * @param transaction 事务指针
 * @return 是否插入成功
 */
bool IxIndexHandle::insert_entry(const char *key, const Rid &value, Transaction *transaction) {
    // Todo:
    // 1. 查找key值应该插入到哪个叶子节点
    // 2. 在该叶子节点中插入键值对
    // 3. 如果结点已满，分裂结点，并把新结点的相关信息插入父节点
    // 提示：记得unpin page；若当前叶子节点是最右叶子节点，则需要更新file_hdr_.last_leaf；记得处理并发的上锁
    std::scoped_lock lock{root_latch_};  // 加锁保证并发安全

    IxNodeHandle* leaf_node = FindLeafPage(key, Operation::INSERT, transaction);  // 查找要插入的叶子节点
    int old_size = leaf_node->GetSize();  // 获取插入前的节点大小
    int new_size = leaf_node->Insert(key, value);  // 在叶子节点中插入键值对
    if (old_size == new_size) {  // 插入失败，大小没有变化
        buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);  // 取消固定页面
        return false;
    } else {
        page_id_t page_no = leaf_node->GetPageNo();  // 获取当前叶子节点的页号

        if (new_size == leaf_node->GetMaxSize()) {  // 如果叶子节点已满
            IxNodeHandle* new_node = Split(leaf_node);  // 分裂叶子节点
            // 将新节点的相关信息插入父节点
            this->InsertIntoParent(leaf_node, new_node->get_key(0), new_node, transaction);  

            if (page_no == file_hdr_.last_leaf) {  // 更新最右叶子节点信息
                file_hdr_.last_leaf = new_node->GetPageNo();
            }
            // 取消固定新节点页面
            buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);  
        }
        // 取消固定当前叶子节点页面
        buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);  
        return true;
    }
}

/**
 * @brief 将传入的一个node拆分(Split)成两个结点，在node的右边生成一个新结点new node
 *
 * @param node 需要拆分的结点
 * @return 拆分得到的new_node
 * @note 本函数执行完毕后，原node和new node都需要在函数外面进行unpin
 */
IxNodeHandle *IxIndexHandle::Split(IxNodeHandle *node) {
    // Todo:
    // 1. 将原结点的键值对平均分配，右半部分分裂为新的右兄弟结点
    //    需要初始化新节点的page_hdr内容
    // 2. 如果新的右兄弟结点是叶子结点，更新新旧节点的prev_leaf和next_leaf指针
    //    为新节点分配键值对，更新旧节点的键值对数记录
    // 3. 如果新的右兄弟结点不是叶子结点，更新该结点的所有孩子结点的父节点信息(使用IxIndexHandle::maintain_child())
    IxNodeHandle* new_node = CreateNode();  // 创建新节点
    new_node->page_hdr->next_free_page_no = IX_NO_PAGE;  // 设置新节点的下一个空闲页号
    new_node->page_hdr->num_key = 0;  // 初始化新节点的键值对数量
    new_node->page_hdr->parent = IX_NO_PAGE;  // 初始化新节点的父节点页号，在InsertIntoParent中会更新
    if (node->IsLeafPage()) {
        // 如果原节点是叶子节点
        new_node->page_hdr->is_leaf = true;  // 设置新节点为叶子节点
        // 更新新旧节点的prev_leaf和next_leaf指针
        IxNodeHandle* next_node = FetchNode(node->GetNextLeaf());
        new_node->SetNextLeaf(node->GetNextLeaf());
        next_node->SetPrevLeaf(new_node->GetPageNo());
        new_node->SetPrevLeaf(node->GetPageNo());
        node->SetNextLeaf(new_node->GetPageNo());
        buffer_pool_manager_->UnpinPage(next_node->GetPageId(), true);  // 取消固定下一个节点
    }
    // 计算分裂位置
    int mid = node->GetMaxSize() / 2;
    int pos = (node->GetMaxSize() + 1) / 2;  // 中间位置，奇数情况左边多一个
    // 将原节点的键值对分裂给新节点
    new_node->insert_pairs(0, node->get_key(pos), node->get_rid(pos), mid);
    node->SetSize(pos);  // 更新原节点的大小
    // 如果新节点不是叶子节点，更新孩子结点的父节点信息
    if (!node->IsLeafPage()) {
        for (int i = 0; i < new_node->GetSize(); ++i) {
            maintain_child(new_node, i);  // 更新孩子结点的父节点信息
        }
    }
    return new_node;  // 返回新创建的节点
}

/**
 * @brief Insert key & value pair into internal page after split
 * 拆分(Split)后，向上找到old_node的父结点
 * 将new_node的第一个key插入到父结点，其位置在 父结点指向old_node的孩子指针 之后
 * 如果插入后>=maxsize，则必须继续拆分父结点，然后在其父结点的父结点再插入，即需要递归
 * 直到找到的old_node为根结点时，结束递归（此时将会新建一个根R，关键字为key，old_node和new_node为其孩子）
 *
 * @param (old_node, new_node) 原结点为old_node，old_node被分裂之后产生了新的右兄弟结点new_node
 * @param key 要插入parent的key
 * @note 一个结点插入了键值对之后需要分裂，分裂后左半部分的键值对保留在原结点，在参数中称为old_node，
 * 右半部分的键值对分裂为新的右兄弟节点，在参数中称为new_node（参考Split函数来理解old_node和new_node）
 * @note 本函数执行完毕后，new node和old node都需要在函数外面进行unpin
 */
void IxIndexHandle::InsertIntoParent(IxNodeHandle *old_node, const char *key, IxNodeHandle *new_node,Transaction *transaction) {
    // Todo:
    // 1. 分裂前的结点（原结点, old_node）是否为根结点，如果为根结点需要分配新的root
    // 2. 获取原结点（old_node）的父亲结点
    // 3. 获取key对应的rid，并将(key, rid)插入到父亲结点
    // 4. 如果父亲结点仍需要继续分裂，则进行递归插入
    // 提示：记得unpin page

    IxNodeHandle* father;
    if(old_node->IsRootPage()) {
        // 新的父节点
        IxNodeHandle* new_root = this->CreateNode();
        new_root -> page_hdr -> is_leaf = false;
        new_root -> page_hdr -> next_free_page_no = IX_NO_PAGE;
        new_root -> page_hdr -> next_leaf = IX_NO_PAGE;
        new_root -> page_hdr -> prev_leaf = IX_NO_PAGE;
        new_root -> page_hdr -> num_key = 0;
        new_root -> page_hdr -> parent = IX_NO_PAGE;
        file_hdr_.root_page = new_root->GetPageNo();// 更新文件头的根页号
        new_root->Insert(old_node->get_key(0), Rid{old_node->GetPageNo(), -1});// 插入key和rid到新根节点
        old_node->SetParentPageNo(new_root->GetPageNo()); // 更新原节点的父节点页号
        father = new_root;// 新根节点作为父节点
    } else {
        father = FetchNode(old_node->GetParentPageNo());// 获取原节点的父节点
    }
    // 将新节点的第一个key插入到父节点
    father->Insert(key, Rid{new_node->GetPageNo(), -1});
    // 更新新节点的父节点页号
    new_node->SetParentPageNo(father->GetPageNo());
    // 是否继续分裂
    if(father->GetSize() == father->GetMaxSize()) {
        // 如果父节点仍然需要分裂
        IxNodeHandle* new_new_node = this->Split(father);// 分裂父节点
        this->InsertIntoParent(father, new_new_node->get_key(0), new_new_node, transaction);
        //递归插入新分裂出的节点到父节点
        buffer_pool_manager_->UnpinPage(new_new_node->GetPageId(), true);
    }
    // 取消固定新节点的页面
    buffer_pool_manager_->UnpinPage(father->GetPageId(), true);
}


/**
 * @brief 用于删除B+树中含有指定key的键值对
 *
 * @param key 要删除的key值
 * @param transaction 事务指针
 * @return 是否删除成功
 */
bool IxIndexHandle::delete_entry(const char *key, Transaction *transaction) {
    // Todo:
    // 1. 获取该键值对所在的叶子结点
    // 2. 在该叶子结点中删除键值对
    // 3. 如果删除成功需要调用CoalesceOrRedistribute来进行合并或重分配操作，并根据函数返回结果判断是否有结点需要删除
    // 4. 如果需要并发，并且需要删除叶子结点，则需要在事务的delete_page_set中添加删除结点的对应页面；记得处理并发的上锁
    std::scoped_lock lock{root_latch_};  // 加锁保证并发安全

    IxNodeHandle* node = FindLeafPage(key, Operation::FIND, transaction);  // 查找含有key的叶子节点
    int old_size = node->GetSize();  // 删除前节点大小
    int new_size = node->Remove(key);  // 在节点中删除key，返回新大小

    maintain_parent(node);  // 更新父节点的第一个key

    if (old_size != new_size) {
    	// 处理合并或重分配操作，确保节点填充度在小于半满时执行
        CoalesceOrRedistribute(node, transaction);  
        buffer_pool_manager_->UnpinPage(node->GetPageId(), true);  // 取消固定
        return true;  // 删除成功
    } else {
        buffer_pool_manager_->UnpinPage(node->GetPageId(), true);  // 取消固定
        return false;  // 删除失败
    }
}

/**
 * @brief 用于处理合并和重分配的逻辑，用于删除键值对后调用
 *
 * @param node 执行完删除操作的结点
 * @param transaction 事务指针
 * @param root_is_latched 传出参数：根节点是否上锁，用于并发操作
 * @return 是否需要删除结点
 * @note 用户需要首先找到输入页面的兄弟结点。
 * 如果兄弟结点的大小加上输入结点的大小大于或等于两倍的页面最小大小，则重新分配。
 * 否则，合并（Coalesce）。
 */
bool IxIndexHandle::CoalesceOrRedistribute(IxNodeHandle *node, Transaction *transaction) {
    if(node->IsRootPage()) { // 如果当前节点为根节点
        return AdjustRoot(node); // 调整根节点
    }
    if(node->GetSize() >= node->GetMinSize()) { // 如果节点的大小大于等于最小大小
        return false; // 不需要进行合并或重分配
    }
    // 需要进行合并或重分配处理
    IxNodeHandle* father = FetchNode(node->GetParentPageNo()); // 获取父节点
    IxNodeHandle* brother;
    int index = father->find_child(node); // 找到当前节点在父节点中的索引位置
    if(index == 0) { // 如果当前节点没有前驱节点
        brother = FetchNode(father->get_rid(index+1)->page_no); // 获取当前节点的后继兄弟节点
    } else {
        brother = FetchNode(father->get_rid(index-1)->page_no); // 获取当前节点的前驱兄弟节点
    }

    if(node->GetSize() + brother->GetSize() >= node->GetMinSize()*2) { // 如果当前节点和兄弟节点的大小可以支持两个节点的最小大小
        Redistribute(brother, node, father, index); // 进行重分配操作
        buffer_pool_manager_->UnpinPage(father->GetPageId(), true); // 取消固定父节点页面
        buffer_pool_manager_->UnpinPage(brother->GetPageId(), true); // 取消固定兄弟节点页面
        return false;
    } else {
        Coalesce(&brother, &node, &father, index, transaction); // 进行合并操作
        buffer_pool_manager_->UnpinPage(father->GetPageId(), true); // 取消固定父节点页面
        buffer_pool_manager_->UnpinPage(brother->GetPageId(), true); // 取消固定兄弟节点页面
        return true;
    }
}



/**
 * @brief 用于当根结点被删除了一个键值对之后的处理
 *
 * @param old_root_node 原根节点
 * @return bool 根结点是否需要被删除
 * @note size of root page can be less than min size and this method is only called within coalesceOrRedistribute()
 */
bool IxIndexHandle::AdjustRoot(IxNodeHandle *old_root_node) {
    // Todo:
    // 1. 如果old_root_node是内部结点，并且大小为1，则直接把它的孩子更新成新的根结点
    // 2. 如果old_root_node是叶结点，且大小为0，则直接更新root page
    // 3. 除了上述两种情况，不需要进行操作
    if(old_root_node->IsLeafPage() && old_root_node->GetSize()==0){  // 根节点无孩子且是最后一个键值对被删除
        file_hdr_.root_page = INVALID_PAGE_ID;
        return false;
    }
    else if(!old_root_node->IsLeafPage() && old_root_node->GetSize()==1){ // 根节点还有一个孩子，根节点无用，孩子变为根节点
        file_hdr_.root_page = old_root_node->RemoveAndReturnOnlyChild();

        IxNodeHandle* new_root = this->FetchNode(file_hdr_.root_page);
        new_root->page_hdr->parent = IX_NO_PAGE;  // root没有father（test时递归遍历树的时候，如果rootfather不修改为IX_NO_PAGE，会出错）
        buffer_pool_manager_->UnpinPage(new_root->GetPageId(), true);

        release_node_handle(*old_root_node); // 更新file_hdr_.num_pages
        return true;
    }
    return false;
}

/**
 * @brief 重新分配node和兄弟结点neighbor_node的键值对
 * Redistribute key & value pairs from one page to its sibling page. If index == 0, move sibling page's first key
 * & value pair into end of input "node", otherwise move sibling page's last key & value pair into head of input "node".
 *
 * @param neighbor_node sibling page of input "node"
 * @param node input from method coalesceOrRedistribute()
 * @param parent the parent of "node" and "neighbor_node"
 * @param index node在parent中的rid_idx
 * @note node是之前刚被删除过一个key的结点
 * index=0，则neighbor是node后继结点，表示：node(left)      neighbor(right)
 * index>0，则neighbor是node前驱结点，表示：neighbor(left)  node(right)
 * 注意更新parent结点的相关kv对
 */
void IxIndexHandle::Redistribute(IxNodeHandle *neighbor_node, IxNodeHandle *node, IxNodeHandle *parent, int index) {
    // Todo:
    // 1. 通过index判断neighbor_node是否为node的前驱结点
    // 2. 从neighbor_node中移动一个键值对到node结点中
    // 3. 更新父节点中的相关信息，并且修改移动键值对对应孩字结点的父结点信息（maintain_child函数）
    // 注意：neighbor_node的位置不同，需要移动的键值对不同，需要分类讨论
    if(index==0){ // node在左，向后插入
        node->insert_pairs(node->GetSize(), neighbor_node->get_key(0), (neighbor_node->get_rid(0)),1);
        neighbor_node->erase_pair(0);
        parent->set_key(parent->find_child(neighbor_node), neighbor_node->get_key(0));  // 最小值被拿走，更新father对应的key
    }
    else{  // node在右，向前插入
        node->insert_pairs(0, neighbor_node->get_key(neighbor_node->GetSize()-1), (neighbor_node->get_rid(neighbor_node->GetSize()-1)),1);
        neighbor_node->erase_pair(neighbor_node->GetSize()-1);
        parent->set_key(index, node->get_key(0));  // 最小值新增，更新father对应的key
    }
    maintain_child(node, node->GetSize()-1);
}

/**
 * @brief 合并(Coalesce)函数是将node和其直接前驱进行合并，也就是和它左边的neighbor_node进行合并；
 * 假设node一定在右边。如果上层传入的index=0，说明node在左边，那么交换node和neighbor_node，保证node在右边；合并到左结点，实际上就是删除了右结点；
 * Move all the key & value pairs from one page to its sibling page, and notify buffer pool manager to delete this page.
 * Parent page must be adjusted to take info of deletion into account. Remember to deal with coalesce or redistribute
 * recursively if necessary.
 *
 * @param neighbor_node sibling page of input "node" (neighbor_node是node的前结点)
 * @param node input from method coalesceOrRedistribute() (node结点是需要被删除的)
 * @param parent parent page of input "node"
 * @param index node在parent中的rid_idx
 * @return true means parent node should be deleted, false means no deletion happend
 * @note Assume that *neighbor_node is the left sibling of *node (neighbor -> node)
 */
bool IxIndexHandle::Coalesce(IxNodeHandle **neighbor_node, IxNodeHandle **node, IxNodeHandle **parent, int index,
                             Transaction *transaction) {
    // Todo:
    // 1. 用index判断neighbor_node是否为node的前驱结点，若不是则交换两个结点，让neighbor_node作为左结点，node作为右结点
    // 2. 把node结点的键值对移动到neighbor_node中，并更新node结点孩子结点的父节点信息（调用maintain_child函数）
    // 3. 释放和删除node结点，并删除parent中node结点的信息，返回parent是否需要被删除
    // 提示：如果是叶子结点且为最右叶子结点，需要更新file_hdr_.last_leaf
    //int flag=0;
    if(index==0){
        std::swap(node,neighbor_node); //可以将任意两个对象交换
    }
    if((*node)->GetPageNo()==file_hdr_.last_leaf){
        file_hdr_.last_leaf=(*neighbor_node)->GetPageNo();
    }
    int pos = (*neighbor_node) -> GetSize();
    int num = (*node)->GetSize();
    (*neighbor_node) -> insert_pairs(pos, (*node) -> get_key(0), (*node) -> get_rid(0), num); // node的键值对添加到neighbor
    for(int i = pos; i < pos+num; i++) {
        maintain_child((*neighbor_node), i);  // 更新node结点孩子结点的父节点信息
    }

    if((*node) -> IsLeafPage()) {
        this->erase_leaf(*node); // 更新指针
    }
    release_node_handle(**node);  // 更新file_hdr_.num_pages
    (*parent)->erase_pair((*parent)->find_child(*node));
    return CoalesceOrRedistribute(*parent);

}

/** -- 以下为辅助函数 -- */
/**
 * @brief 获取一个指定结点
 *
 * @param page_no
 * @return IxNodeHandle*
 * @note pin the page, remember to unpin it outside!
 */
IxNodeHandle *IxIndexHandle::FetchNode(int page_no) const {
    // assert(page_no < file_hdr_.num_pages); // 不再生效，由于删除操作，page_no可以大于个数
    Page *page = buffer_pool_manager_->FetchPage(PageId{fd_, page_no});
    IxNodeHandle *node = new IxNodeHandle(&file_hdr_, page);
    return node;
}

/**
 * @brief 创建一个新结点
 *
 * @return IxNodeHandle*
 * @note pin the page, remember to unpin it outside!
 * 注意：对于Index的处理是，删除某个页面后，认为该被删除的页面是free_page
 * 而first_free_page实际上就是最新被删除的页面，初始为IX_NO_PAGE
 * 在最开始插入时，一直是create node，那么first_page_no一直没变，一直是IX_NO_PAGE
 * 与Record的处理不同，Record将未插入满的记录页认为是free_page
 */
IxNodeHandle *IxIndexHandle::CreateNode() {
    file_hdr_.num_pages++;
    PageId new_page_id = {.fd = fd_, .page_no = INVALID_PAGE_ID};
    // 从3开始分配page_no，第一次分配之后，new_page_id.page_no=3，file_hdr_.num_pages=4
    Page *page = buffer_pool_manager_->NewPage(&new_page_id);
    // 注意，和Record的free_page定义不同，此处【不能】加上：file_hdr_.first_free_page_no = page->GetPageId().page_no
    IxNodeHandle *node = new IxNodeHandle(&file_hdr_, page);
    return node;
}

/**
 * @brief 从node开始更新其父节点的第一个key，一直向上更新直到根节点
 *
 * @param node
 */
void IxIndexHandle::maintain_parent(IxNodeHandle *node) {
    IxNodeHandle *curr = node;
    while (curr->GetParentPageNo() != IX_NO_PAGE) {
        // Load its parent
        IxNodeHandle *parent = FetchNode(curr->GetParentPageNo());
        int rank = parent->find_child(curr);
        char *parent_key = parent->get_key(rank);
        // char *child_max_key = curr.get_key(curr.page_hdr->num_key - 1);
        char *child_first_key = curr->get_key(0);
        if (memcmp(parent_key, child_first_key, file_hdr_.col_len) == 0) {
            assert(buffer_pool_manager_->UnpinPage(parent->GetPageId(), true));
            break;
        }
        memcpy(parent_key, child_first_key, file_hdr_.col_len);  // 修改了parent node
        curr = parent;

        assert(buffer_pool_manager_->UnpinPage(parent->GetPageId(), true));
    }
}

/**
 * @brief 要删除leaf之前调用此函数，更新leaf前驱结点的next指针和后继结点的prev指针
 *
 * @param leaf 要删除的leaf
 */
void IxIndexHandle::erase_leaf(IxNodeHandle *leaf) {
    assert(leaf->IsLeafPage());

    IxNodeHandle *prev = FetchNode(leaf->GetPrevLeaf());
    prev->SetNextLeaf(leaf->GetNextLeaf());
    buffer_pool_manager_->UnpinPage(prev->GetPageId(), true);

    IxNodeHandle *next = FetchNode(leaf->GetNextLeaf());
    next->SetPrevLeaf(leaf->GetPrevLeaf());  // 注意此处是SetPrevLeaf()
    buffer_pool_manager_->UnpinPage(next->GetPageId(), true);
}

/**
 * @brief 删除node时，更新file_hdr_.num_pages
 *
 * @param node
 */
void IxIndexHandle::release_node_handle(IxNodeHandle &node) { file_hdr_.num_pages--; }

/**
 * @brief 将node的第child_idx个孩子结点的父节点置为node
 */
void IxIndexHandle::maintain_child(IxNodeHandle *node, int child_idx) {
    if (!node->IsLeafPage()) {
        //  Current node is inner node, load its child and set its parent to current node
        int child_page_no = node->ValueAt(child_idx);
        IxNodeHandle *child = FetchNode(child_page_no);
        child->SetParentPageNo(node->GetPageNo());
        buffer_pool_manager_->UnpinPage(child->GetPageId(), true);
    }
}

/**
 * @brief 这里把iid转换成了rid，即iid的slot_no作为node的rid_idx(key_idx)
 * node其实就是把slot_no作为键值对数组的下标
 * 换而言之，每个iid对应的索引槽存了一对(key,rid)，指向了(要建立索引的属性首地址,插入/删除记录的位置)
 *
 * @param iid
 * @return Rid
 * @note iid和rid存的不是一个东西，rid是上层传过来的记录位置，iid是索引内部生成的索引槽位置
 */
Rid IxIndexHandle::get_rid(const Iid &iid) const {
    IxNodeHandle *node = FetchNode(iid.page_no);
    if (iid.slot_no >= node->GetSize()) {
        throw IndexEntryNotFoundError();
    }
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);  // unpin it!
    return *node->get_rid(iid.slot_no);
}

/** --以下函数将用于lab3执行层-- */
/**
 * @brief FindLeafPage + lower_bound
 *
 * @param key
 * @return Iid
 * @note 上层传入的key本来是int类型，通过(const char *)&key进行了转换
 * 可用*(int *)key转换回去
 */
Iid IxIndexHandle::lower_bound(const char *key) {
    // int int_key = *(int *)key;
    // printf("my_lower_bound key=%d\n", int_key);

    IxNodeHandle *node = FindLeafPage(key, Operation::FIND, nullptr);
    int key_idx = node->lower_bound(key);

    Iid iid = {.page_no = node->GetPageNo(), .slot_no = key_idx};

    // unpin leaf node
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
    return iid;
}

/**
 * @brief FindLeafPage + upper_bound
 *
 * @param key
 * @return Iid
 */
Iid IxIndexHandle::upper_bound(const char *key) {
    // int int_key = *(int *)key;
    // printf("my_upper_bound key=%d\n", int_key);

    IxNodeHandle *node = FindLeafPage(key, Operation::FIND, nullptr);
    int key_idx = node->upper_bound(key);

    Iid iid;
    if (key_idx == node->GetSize()) {
        // 这种情况无法根据iid找到rid，即后续无法调用ih->get_rid(iid)
        iid = leaf_end();
    } else {
        iid = {.page_no = node->GetPageNo(), .slot_no = key_idx};
    }

    // unpin leaf node
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
    return iid;
}

/**
 * @brief 指向第一个叶子的第一个结点
 * 用处在于可以作为IxScan的第一个
 *
 * @return Iid
 */
Iid IxIndexHandle::leaf_begin() const {
    Iid iid = {.page_no = file_hdr_.first_leaf, .slot_no = 0};
    return iid;
}

/**
 * @brief 指向最后一个叶子的最后一个结点的后一个
 * 用处在于可以作为IxScan的最后一个
 *
 * @return Iid
 */
Iid IxIndexHandle::leaf_end() const {
    IxNodeHandle *node = FetchNode(file_hdr_.last_leaf);
    Iid iid = {.page_no = file_hdr_.last_leaf, .slot_no = node->GetSize()};
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);  // unpin it!
    return iid;
}