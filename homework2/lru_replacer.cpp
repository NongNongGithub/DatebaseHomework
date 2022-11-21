/**
 * LRU implementation
 */
#include "buffer/lru_replacer.h"
#include "page/page.h"

using namespace std;

namespace scudb {

    template <typename T>
    //构造函数
    LRUReplacer<T>::LRUReplacer() {
        head = make_shared<Node> ();
        tail = make_shared<Node> ();
        head->next = tail;
        tail->pre = head;
    }

    template <typename T> LRUReplacer<T>::~LRUReplacer() {}

/*
 * Insert value into LRU
 */

    //插入函数
    template <typename T> void LRUReplacer<T>::Insert(const T &value) {
        lock_guard<mutex> lck(latch);
        shared_ptr<Node> cur;

        // 如果找到，更新
        if(map.find(value) != map.end())
        {
            cur = map[value];
            shared_ptr<Node> prev = cur->pre;
            shared_ptr<Node> nextv = cur->next;
            prev->next = nextv;
            nextv->pre = prev;
        }
        else
            cur = make_shared<Node>(value);

        // 一系列的插入过程
        shared_ptr<Node> fir = head->next;
        cur->next = fir;
        fir->pre = cur;
        cur->pre = head;
        head->next = cur;
        map[value] = cur;
        return;
    }

/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
    template <typename T> bool LRUReplacer<T>::Victim(T &value) {
        lock_guard<mutex> lck(latch);
        if(map.empty())
            return false;

        shared_ptr<Node> last = tail->pre;
        tail->pre = last->pre;
        last->pre->next = tail;
        value = last->value;
        map.erase(last->value);
        return true;
    }

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
    template <typename T> bool LRUReplacer<T>::Erase(const T &value) {
        lock_guard<mutex> lck(latch);
        // 若找到,删除
        if(map.find(value) != map.end())
        {
            shared_ptr<Node> cur = map[value];
            cur->pre->next = cur->next;
            cur->next->pre = cur->pre;
        }
        return map.erase(value);              //利用了erase函数返回值的特性
    }

    template <typename T> size_t LRUReplacer<T>::Size() {
        lock_guard<mutex> lck(latch);
        return map.size();
    }

    template class LRUReplacer<Page *>;
// test only
    template class LRUReplacer<int>;

} // namespace scudb
