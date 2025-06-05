#include "storage/disk_manager.h"

#include <assert.h>    // for assert
#include <string.h>    // for memset
#include <sys/stat.h>  // for stat
#include <unistd.h>    // for lseek

#include "defs.h"
using namespace std;


DiskManager::DiskManager() { memset(fd2pageno_, 0, MAX_FD * (sizeof(std::atomic<page_id_t>) / sizeof(char))); }

/**
 * @brief Write the contents of the specified page into disk file
 *
 */
void DiskManager::write_page(int fd, page_id_t page_no, const char *offset, int num_bytes) {
    // Todo:
    // 1.lseek()定位到文件头，通过(fd,page_no)可以定位指定页面及其在磁盘文件中的偏移量
    // 2.调用write()函数
    // 注意处理异常
    
    //从内存中的offset位置，读取num_bytes字节，写入diskfile中
    //移动文件指针到(page_no) * page_size
    //从文件指针起始的num_bytes 一次性写入磁盘文件中
    //写入时确保num_bytes合法

    // file_offset 是文件内的偏移量，用于定位写操作在文件中的位置。
    // offset 是内存中的数据指针(内存缓冲区)，用于定位要写入的数据在内存中的位置。

        off_t file_offset = page_no * PAGE_SIZE; //page_no 从0开始， file_offset指向磁盘中文件页面位置
        //写入数据就在file_offset 后面写入

        //将文件指针移动到file_offset处
        if(lseek(fd, file_offset, SEEK_SET) == -1) throw UnixError();

        //写入数据(将offset的num_bytes字节写回磁盘)
        ssize_t writen_bytes = write(fd, offset, num_bytes);
        if(writen_bytes == -1 || writen_bytes != num_bytes){
            //perror("write");
            throw UnixError();
        }
        
        // //确保数据写入磁盘
        // if(fsync(fd) == -1){
        //     //perror("fsync");
        //     return ;
        // }
}



/**
 * @brief Read the contents of the specified page into the given memory area
 */
void DiskManager::read_page(int fd, page_id_t page_no, char *offset, int num_bytes) {
    // Todo:
    // 1.lseek()定位到文件头，通过(fd,page_no)可以定位指定页面及其在磁盘文件中的偏移量
    // 2.调用read()函数
    // 注意处理异常
        if(num_bytes < 0 || num_bytes > PAGE_SIZE)  //读取字节数不合法
            throw UnixError();
        
        off_t file_offset = page_no * PAGE_SIZE;

        //移动文件指针
        if(lseek(fd, file_offset, SEEK_SET) == -1){
            throw UnixError();
        }

        //读取数据到offset
        ssize_t read_bytes = read(fd, offset, num_bytes);
        if(read_bytes == -1 || read_bytes != num_bytes)
            throw UnixError();
    
}

/**
 * @brief Allocate new page (operations like create index/table)
 * For now just keep an increasing counter
 */
page_id_t DiskManager::AllocatePage(int fd) {
    // Todo:
    // 简单的自增分配策略，指定文件的页面编号加1

    return fd2pageno_[fd] ++ ;
}

/**
 * @brief Deallocate page (operations like drop index/table)
 * Need bitmap in header page for tracking pages
 * This does not actually need to do anything for now.
 */
void DiskManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {}

bool DiskManager::is_dir(const std::string &path) {
    struct stat st;
    return stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

void DiskManager::create_dir(const std::string &path) {
    // Create a subdirectory
    std::string cmd = "mkdir " + path;
    if (system(cmd.c_str()) < 0) {  // 创建一个名为path的目录
        throw UnixError();
    }
}

void DiskManager::destroy_dir(const std::string &path) {
    std::string cmd = "rm -r " + path;
    if (system(cmd.c_str()) < 0) {
        throw UnixError();
    }
}

/**
 * @brief 用于判断指定路径文件是否存在 
 */
bool DiskManager::is_file(const std::string &path) {
    // Todo:
    // 用struct stat获取文件信息
    struct stat statbuf;
    int ret = stat(path.c_str(), &statbuf);
    if(ret == 0){
        //perror("comes form is_file : stat");
        return S_ISREG(statbuf.st_mode);
    }
    //判断是否为常规文件
    return false;
}

/**
 * @brief 用于创建指定路径文件
 */
void DiskManager::create_file(const std::string &path) {
    // Todo:
    // 调用open()函数，使用O_CREAT模式
    // 注意不能重复创建相同文件
    //文件已经存在
    if(is_file(path)) throw FileExistsError(path);

    //使用open()创建文件
    int fd = open(path.c_str(), O_CREAT, 0777);
    if(fd == -1){
        //perror("open");
        throw UnixError();
    }
    //关闭文件
    close(fd);

}

/**
 * @brief 用于删除指定路径文件 
 */
void DiskManager::destroy_file(const std::string &path) {
    // Todo:
    // 调用unlink()函数
    // 注意不能删除未关闭的文件
    //文件在打开文件列表
    if(path2fd_.find(path) != path2fd_.end()) 
        throw UnixError();

    //文件不存在
    if(is_file(path) == false) 
        throw FileNotFoundError(path);

    if(unlink(path.c_str()) == -1){
        //perror("unlink");
        throw UnixError();
    }

}

/**
 * @brief 用于打开指定路径文件
 */
int DiskManager::open_file(const std::string &path) {
    // Todo:
    // 调用open()函数，使用O_RDWR模式
    // 注意不能重复打开相同文件，并且需要更新文件打开列表
    //文件在打开文件列表
    if(path2fd_.count(path)){
        //cerr << "file not close" << endl;
        throw UnixError();
    }

    //文件不存在
    if(is_file(path) == false){
        //cerr << "comes form open_file : file not exists" << endl;
        throw FileNotFoundError(path);
        //return -1;
    }

    int fd = open(path.c_str(), O_RDWR, 0777);
    if(fd == -1){
        //perror("open");
        throw UnixError();
    }else{
        path2fd_.insert(make_pair(path, fd));
	    fd2path_.insert(make_pair(fd,path));
    }
    return fd;
}

/**
 * @brief 用于关闭指定路径文件
 */
void DiskManager::close_file(int fd) {
    // Todo:
    // 调用close()函数
    // 注意不能关闭未打开的文件，并且需要更新文件打开列表
    //文件在打开文件列表
    if(!fd2path_.count(fd)){
        //cerr << "file not close" << endl;
        throw FileNotOpenError(fd);
    }
    //把文件从打开列表中删除
    path2fd_.erase(fd2path_[fd]);
    fd2path_.erase(fd);
    close(fd);

}

int DiskManager::GetFileSize(const std::string &file_name) {
    struct stat stat_buf;
    int rc = stat(file_name.c_str(), &stat_buf);
    return rc == 0 ? stat_buf.st_size : -1;
}

std::string DiskManager::GetFileName(int fd) {
    if (!fd2path_.count(fd)) {
        throw FileNotOpenError(fd);
    }
    return fd2path_[fd];
}

int DiskManager::GetFileFd(const std::string &file_name) {
    if (!path2fd_.count(file_name)) {
        return open_file(file_name);
    }
    return path2fd_[file_name];
}

bool DiskManager::ReadLog(char *log_data, int size, int offset, int prev_log_end) {
    // read log file from the previous end
    if (log_fd_ == -1) {
        log_fd_ = open_file(LOG_FILE_NAME);
    }
    offset += prev_log_end;
    int file_size = GetFileSize(LOG_FILE_NAME);
    if (offset >= file_size) {
        return false;
    }

    size = std::min(size, file_size - offset);
    lseek(log_fd_, offset, SEEK_SET);
    ssize_t bytes_read = read(log_fd_, log_data, size);
    if (bytes_read != size) {
        throw UnixError();
    }
    return true;
}

void DiskManager::WriteLog(char *log_data, int size) {
    if (log_fd_ == -1) {
        log_fd_ = open_file(LOG_FILE_NAME);
    }

    // write from the file_end
    lseek(log_fd_, 0, SEEK_END);
    ssize_t bytes_write = write(log_fd_, log_data, size);
    if (bytes_write != size) {
        throw UnixError();
    }
}
