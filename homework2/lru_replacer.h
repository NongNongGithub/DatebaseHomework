/**
 * lru_replacer.h
 *
 * Functionality: The buffer pool manager must maintain a LRU list to collect
 * all the pages that are unpinned and ready to be swapped. The simplest way to
 * implement LRU is a FIFO queue, but remember to dequeue or enqueue pages when
 * a page changes from unpinned to pinned, or vice-versa.
 */

#pragma once

#include "buffer/replacer.h"
#include <memory>
#include <mutex>
#include <unordered_map>

using namespace std;

namespace scudb {

    template <typename T>
    class LRUReplacer : public Replacer<T> {
        // 定义页面结构
        struct Node
        {
            Node() {};
            Node(T value) : value(value) {};
            T value;
            shared_ptr<Node> pre;
            shared_ptr<Node> next;
        };

    public:
        // do not change public interface
        //构造函数
        LRUReplacer();

        ~LRUReplacer();

        void Insert(const T &value);

        bool Victim(T &value);

        bool Erase(const T &value);

        size_t Size();

    private:
        // add your member variables here
        shared_ptr<Node> head;
        shared_ptr<Node> tail;
        //map 容器中存储的数据是有序的，unordered_map容器中是无序的。
        unordered_map<T, shared_ptr<Node>> map;
        mutable mutex latch;
    };

} // namespace scudb
