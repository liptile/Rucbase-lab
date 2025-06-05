#include "rm_scan.h"

#include "rm_file_handle.h"

/**
 * @brief 初始化file_handle和rid
 *
 * @param file_handle
 */
RmScan::RmScan(const RmFileHandle *file_handle) : file_handle_(file_handle) {
    // Todo:
    // 初始化file_handle和rid（指向第一个存放了记录的位置）
    rid_ = {RM_FIRST_RECORD_PAGE, -1}; //起始页
    next(); //指向第一个存放了记录的位置
}

/**
 * @brief 找到文件中下一个存放了记录的位置
 */
void RmScan::next() {
    // Todo:
    // 找到文件中下一个存放了记录的非空闲位置，用rid_来指向这个位置
    while(rid_.page_no < file_handle_->file_hdr_.num_pages){
        RmPageHandle rph = file_handle_->fetch_page_handle(rid_.page_no);
        int slot_no = Bitmap::next_bit(true, rph.bitmap, file_handle_->file_hdr_.num_records_per_page, rid_.slot_no); //找到第一个非空闲位
        rid_.slot_no = slot_no;
        if(slot_no < file_handle_->file_hdr_.num_records_per_page) //指向
            return ;
        rid_.slot_no = -1;
        rid_.page_no ++ ; //找下一页面
    }
    rid_ = {-1, -1};

}

/**
 * @brief ​ 判断是否到达文件末尾
 */
bool RmScan::is_end() const {
    // Todo: 修改返回值
    if(rid_.page_no != RM_NO_PAGE) return false;
    return true;
}

/**
 * @brief RmScan内部存放的rid
 */
Rid RmScan::rid() const {
    // Todo: 修改返回值
    return rid_;
}