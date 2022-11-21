#include <list>

#include "hash/extendible_hash.h"
#include "page/page.h"
using namespace std;

namespace scudb {

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
    template <typename K, typename V>
    ExtendibleHash<K, V>::ExtendibleHash(size_t size) :  globalDepth(0),bucketSize(size),bucketNum(1) {
        buckets.push_back(make_shared<Bucket>(0));
    }

    template<typename K, typename V>
    ExtendibleHash<K, V>::ExtendibleHash() {
        ExtendibleHash(64);
    }

/*
 * helper function to calculate the hashing address of input key
 */
    template <typename K, typename V>
    size_t ExtendibleHash<K, V>::HashKey(const K &key) const{
        return hash<K>{}(key);
    }

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
    //返回全局深度
    template <typename K, typename V>
    int ExtendibleHash<K, V>::GetGlobalDepth() const{
        lock_guard<mutex> lock(latch);
        return globalDepth;
    }

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
    //返回局部深度
    template <typename K, typename V>
    int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
        //lock_guard<mutex> lck2(latch);
        if (buckets[bucket_id]) {//桶是否存在
            lock_guard<mutex> lock(buckets[bucket_id]->latch);
            if (buckets[bucket_id]->kmap.size() == 0)
                return -1;
            return buckets[bucket_id]->localDepth;
        }
        return -1;
    }

/*
 * helper function to return current number of bucket in hash table
 */
    //得到位数
    template <typename K, typename V>
    int ExtendibleHash<K, V>::GetNumBuckets() const{
        lock_guard<mutex> lock(latch);
        return bucketNum;
    }

    //辅助函数，通过key得到桶编号
    template <typename K, typename V>
    int ExtendibleHash<K, V>::getIndex(const K &key) const{
        lock_guard<mutex> lck(latch);
        return HashKey(key) & ((1 << globalDepth) - 1);//取后globalDepth位
    }

/*
 * lookup function to find value associate with input key
 */
    //查找函数
    template <typename K, typename V>
    bool ExtendibleHash<K, V>::Find(const K &key, V &value) {

        int index = getIndex(key);
        lock_guard<mutex> lock(buckets[index]->latch);
        if (buckets[index]->kmap.find(key) != buckets[index]->kmap.end()) {
            value = buckets[index]->kmap[key];//找到，则将key中的值赋给value
            return true;

        }
        return false;
    }



/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */

    //删除函数
    template <typename K, typename V>
    bool ExtendibleHash<K, V>::Remove(const K &key) {
        int idx = getIndex(key);
        lock_guard<mutex> lock(buckets[idx]->latch);
        shared_ptr<Bucket> cur = buckets[idx];
        if (cur->kmap.find(key) == cur->kmap.end()) {
            return false;//没有找到返回false
        }
        cur->kmap.erase(key);
        return true;//找到返回true并删除
    }

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
    //插入函数
    template <typename K, typename V>
    void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
        int idx = getIndex(key);
        shared_ptr<Bucket> cur = buckets[idx];
        while (true) {
            lock_guard<mutex> lck(cur->latch);
            //能直接插入则直接插入
            if (cur->kmap.find(key) != cur->kmap.end() || cur->kmap.size() < bucketSize) {
                cur->kmap[key] = value;
                break;
            }
            int mask = (1 << (cur->localDepth));
            cur->localDepth++;

            {
                lock_guard<mutex> lck2(latch);
                if (cur->localDepth > globalDepth) {//如果localDepth>globalDepth，则分裂

                    size_t length = buckets.size();
                    for (size_t i = 0; i < length; i++) {
                        buckets.push_back(buckets[i]);//产生的新桶直接进行复制一份
                    }
                    globalDepth++;

                }
                bucketNum++;
                auto newBuc = make_shared<Bucket>(cur->localDepth);

                typename map<K, V>::iterator it;
                for (it = cur->kmap.begin(); it != cur->kmap.end(); ) {//分桶
                    if (HashKey(it->first) & mask) {
                        newBuc->kmap[it->first] = it->second;
                        it = cur->kmap.erase(it);
                    }
                    else
                        it++;
                }
                for (size_t i = 0; i < buckets.size(); i++) {
                    if (buckets[i] == cur && (i & mask))
                        buckets[i] = newBuc;
                }
            }
            idx = getIndex(key);
            cur = buckets[idx];
        }
    }



    template class ExtendibleHash<page_id_t, Page *>;
    template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
    template class ExtendibleHash<int, std::string>;
    template class ExtendibleHash<int, std::list<int>::iterator>;
    template class ExtendibleHash<int, int>;
} // namespace cmudb
