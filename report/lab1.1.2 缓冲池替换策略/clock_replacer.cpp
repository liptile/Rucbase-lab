#include "replacer/clock_replacer.h"

#include <algorithm>

ClockReplacer::ClockReplacer(size_t num_pages)
    : circular_{num_pages, ClockReplacer::Status::EMPTY_OR_PINNED}, hand_{0}, capacity_{num_pages} {
    // 成员初始化列表语法
    circular_.reserve(num_pages);
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
    const std::lock_guard<mutex_t> guard(mutex_);
    // Todo: try to find a victim frame in buffer pool with clock scheme
    // and make the *frame_id = victim_frame_id
    // not found, frame_id=nullptr and return false

    *frame_id = -1;
    if(Size() == 0) return false;
    while(true){ // 遍历整个数组
        if(circular_[hand_] == Status::ACCESSED) //如果访问为为1，则置为0
        circular_[hand_] = Status::UNTOUCHED;
        else if(circular_[hand_] == Status::UNTOUCHED){ //如果访问位为0，则命中
            *frame_id = hand_;
            circular_[hand_] = Status::EMPTY_OR_PINNED; //命中后 不是accessed 而是empty or pinned
            hand_ = (hand_ + 1) % capacity_; //移动到下一个位置
            return true;
        }
        hand_ = (hand_ + 1) % capacity_; //移动到下一个位置
    }
    // if(frame_id == nullptr){
    //     while(true){ //再循环一次，找到第一个0
    //         if(circular_[hand_] == Status::UNTOUCHED){
    //             *frame_id = hand_;
    //             return true;
    //         }
    //         hand_ = (hand_ + 1) % capacity_; //移动到下一个位置
    //     }
    // }
    return false;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
    const std::lock_guard<mutex_t> guard(mutex_);
    // Todo: you can implement it!

    if(circular_[frame_id] != Status::ACCESSED) //如果不是已访问状态
        circular_[frame_id] = Status::EMPTY_OR_PINNED; //标记页面
    return ;
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
    const std::lock_guard<mutex_t> guard(mutex_);
    // Todo: you can implement it!
    if(circular_[frame_id] == Status::EMPTY_OR_PINNED)  //只有当页面为
        circular_[frame_id] = Status::ACCESSED; //标记已访问
    return ;
}

size_t ClockReplacer::Size() {
    // Todo:
    // 返回在[arg0, arg1)范围内满足特定条件(arg2)的元素的数目
    // return all items that in the range[circular_.begin, circular_.end )
    // and be met the condition: status!=EMPTY_OR_PINNED
    // That is the number of frames in the buffer pool that storage page (NOT EMPTY_OR_PINNED)
    int count = 0;
    for(int i = 0; i != (int)circular_.size(); i ++ ){ //遍历所有empty的元素
        if(circular_[i] != Status::EMPTY_OR_PINNED)
            count ++ ;
    }
    return count;
}
