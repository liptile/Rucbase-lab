#include "rm_file_handle.h"

/**
 * @brief 由Rid得到指向RmRecord的指针
 *
 * @param rid 指定记录所在的位置
 * @return std::unique_ptr<RmRecord>
 */
std::unique_ptr<RmRecord> RmFileHandle::get_record(const Rid &rid, Context *context) const {
    // Todo:
    // 1. 获取指定记录所在的page handle
    // 2. 初始化一个指向RmRecord的指针（赋值其内部的data和size）

    int page_no = rid.page_no;
    int slot_no = rid.slot_no; //获取记录所在页面以及记录所在slot

    RmPageHandle rph = fetch_page_handle(page_no); //获取对用页面号所在的页面管理 have question

    //std::cout << "get record : " << __LINE__ << std::endl;

    //新建一个指向rmrecord的指针
    auto rr = std::make_unique<RmRecord>(file_hdr_.record_size);
    rr->size = file_hdr_.record_size; //赋值记录大小
    char *slot = rph.get_slot(slot_no);
    memcpy(rr->data, slot, rr->size); //复制记录数据
    
    //std::cout << "get record : " << __LINE__ << std::endl;

    return rr;
    //return nullptr;
}

/**
 * @brief 在该记录文件（RmFileHandle）中插入一条记录
 *
 * @param buf 要插入的数据的地址
 * @return Rid 插入记录的位置
 */
Rid RmFileHandle::insert_record(char *buf, Context *context) { //have question
    // Todo:
    // 1. 获取当前未满的page handle
    // 2. 在page handle中找到空闲slot位置
    // 3. 将buf复制到空闲slot位置
    // 4. 更新page_handle.page_hdr中的数据结构
    // 注意考虑插入一条记录后页面已满的情况，需要更新file_hdr_.first_free_page_no

    //1. 获取当前未满的page handle
    RmPageHandle rph = create_page_handle();
    
    // 2. 在page handle中找到空闲slot位置
    //怎么找空闲slot？ bitmap 记录了所有slot的情况
    //每个slot 存储一行记录
    int bit = Bitmap::first_bit(false, rph.bitmap, rph.file_hdr->num_records_per_page); //获取第一个空闲的slot
    Bitmap::set(rph.bitmap, bit); //将bit位置1

    // 3. 将buf复制到空闲slot位置
    std::copy(buf, buf + rph.file_hdr->record_size, rph.get_slot(bit));//将数据插入该slot

    //4. 更新page_handle.page_hdr中的数据结构
    if( ++ rph.page_hdr->num_records >= rph.file_hdr->num_records_per_page) //插入记录后页面已满
        file_hdr_.first_free_page_no = rph.page_hdr->next_free_page_no;
    
    //RmPageHandle rph = fetch_page_handle(page_no); //获取该页面号的rph  //have question

    //std::cout << __LINE__ << std::endl;

    Rid rid; //返回rid
    rid.page_no = rph.page->GetPageId().page_no;
    rid.slot_no = bit;
    return rid;

    //return Rid{-1, -1};
}

/**
 * @brief 在该记录文件（RmFileHandle）中删除一条指定位置的记录
 *
 * @param rid 要删除的记录所在的指定位置
 */
void RmFileHandle::delete_record(const Rid &rid, Context *context) {
    // Todo:
    // 1. 获取指定记录所在的page handle
    // 2. 更新page_handle.page_hdr中的数据结构
    // 注意考虑删除一条记录后页面未满的情况，需要调用release_page_handle()

    //1. 获取指定记录所在的page handle
    int page_no = rid.page_no;
    int slot_no = rid.slot_no;
    RmPageHandle rph = fetch_page_handle(page_no);

    // 2. 更新page_handle.page_hdr中的数据结构
    if( -- rph.page_hdr->num_records < file_hdr_.num_records_per_page)
        release_page_handle(rph);
    Bitmap::reset(rph.bitmap, slot_no); //重置slot位

}

/**
 * @brief 更新指定位置的记录
 *
 * @param rid 指定位置的记录
 * @param buf 新记录的数据的地址
 */
void RmFileHandle::update_record(const Rid &rid, char *buf, Context *context) {
    // Todo:
    // 1. 获取指定记录所在的page handle
    // 2. 更新记录

    int page_no = rid.page_no;
    int slot_no = rid.slot_no;
    RmPageHandle rph = fetch_page_handle(page_no); //获取指定记录所在的page handle
    std::copy(buf, buf + rph.file_hdr->record_size, rph.get_slot(slot_no));//更新记录
}

/** -- 以下为辅助函数 -- */
/**
 * @brief 获取指定页面编号的page handle
 *
 * @param page_no 要获取的页面编号
 * @return RmPageHandle 返回给上层的page_handle
 * @note pin the page, remember to unpin it outside!
 */
RmPageHandle RmFileHandle::fetch_page_handle(int page_no) const {
    // Todo:
    // 使用缓冲池获取指定页面，并生成page_handle返回给上层
    // if page_no is invalid, throw PageNotExistError exception

    //使用缓冲池获取指定页面
    PageId pId;
    pId.fd = fd_;
    pId.page_no = page_no;
    //std::cout << __LINE__ << std::endl;

    Page* p = buffer_pool_manager_->FetchPage(pId); //have question

    if(page_no < file_hdr_.num_pages){ 
        return RmPageHandle(&file_hdr_, p);
    }else{
        throw PageNotExistError("name", page_no); 
    } 
    
    //return RmPageHandle(&file_hdr_, nullptr);

    //pin the page, remember to unpin it outside!  ??????
}

/**
 * @brief 创建一个新的page handle
 *
 * @return RmPageHandle
 */
RmPageHandle RmFileHandle::create_new_page_handle() {
    // Todo:
    // 1.使用缓冲池来创建一个新page
    // 2.更新page handle中的相关信息
    // 3.更新file_hdr_
    //1.
    PageId pId;
    pId.fd = fd_;
    Page* p = buffer_pool_manager_->NewPage(&pId); //新建page
    
    //2.更新page handle中的相关信息
    //rmpagehandle函数自动设置bitmap 与 slots
    RmPageHandle rph = RmPageHandle(&file_hdr_, p);//根据新建的page与file_hdr_建立页面管理
    rph.page_hdr->next_free_page_no = -1;//下一个可用的page no（初始化为-1）
    rph.page_hdr->num_records = 0; //page中当前分配的record个数（初始化为0）
    
    //3.更新file_hdr_
    file_hdr_.num_pages ++ ;
    file_hdr_.first_free_page_no = p->GetPageId().page_no;
    //file_hdr_.
    return rph;
}

/**
 * @brief 创建或获取一个空闲的page handle
 *
 * @return RmPageHandle 返回生成的空闲page handle
 * @note pin the page, remember to unpin it outside!
 */
RmPageHandle RmFileHandle::create_page_handle() {
    // Todo:
    // 1. 判断file_hdr_中是否还有空闲页
    //     1.1 没有空闲页：使用缓冲池来创建一个新page；可直接调用create_new_page_handle()
    //     1.2 有空闲页：直接获取第一个空闲页
    // 2. 生成page handle并返回给上层

    //1.判断file_hdr_中是否还有空闲页
    //怎么判断是否空闲：1.pin_count? 
    if(file_hdr_.first_free_page_no == -1) //没有空闲页
        return create_new_page_handle();
    else return fetch_page_handle(file_hdr_.first_free_page_no); //取该page_on 对应的pagehandl
}

/**
 * @brief 当page handle中的page从已满变成未满的时候调用
 *
 * @param page_handle
 * @note only used in delete_record()
 */
void RmFileHandle::release_page_handle(RmPageHandle &page_handle) {
    // Todo:
    // 当page从已满变成未满，考虑如何更新：
    // 1. page_handle.page_hdr->next_free_page_no
    // 2. file_hdr_.first_free_page_no
    

    std::swap(page_handle.page_hdr->next_free_page_no, file_hdr_.first_free_page_no);
    
}

// used for recovery (lab4)
void RmFileHandle::insert_record(const Rid &rid, char *buf) {
    if (rid.page_no < file_hdr_.num_pages) {
        create_new_page_handle();
    }
    RmPageHandle pageHandle = fetch_page_handle(rid.page_no);
    Bitmap::set(pageHandle.bitmap, rid.slot_no);
    pageHandle.page_hdr->num_records++;
    if (pageHandle.page_hdr->num_records == file_hdr_.num_records_per_page) {
        file_hdr_.first_free_page_no = pageHandle.page_hdr->next_free_page_no;
    }

    char *slot = pageHandle.get_slot(rid.slot_no);
    memcpy(slot, buf, file_hdr_.record_size);

    buffer_pool_manager_->UnpinPage(pageHandle.page->GetPageId(), true);
}