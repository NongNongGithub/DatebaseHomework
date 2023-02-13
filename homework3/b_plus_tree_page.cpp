/**
 * b_plus_tree_page.cpp
 */
#include "page/b_plus_tree_page.h"

namespace scudb {

/*
 * Helper methods to get/set page type
 * Page type enum class is defined in b_plus_tree_page.h
 */
//判断是否为leaf页
bool BPlusTreePage::IsLeafPage() const
{
    return page_type_ == IndexPageType::LEAF_PAGE;
}
//判断是否为root页
bool BPlusTreePage::IsRootPage() const
{
    return parent_page_id_ == INVALID_PAGE_ID;
}
//设置页的类型
void BPlusTreePage::SetPageType(IndexPageType page_type)
{
    page_type_ = page_type;
}

/*
 * Helper methods to get/set size (number of key/value pairs stored in that
 * page)
 */
//得到页中存储的大小
int BPlusTreePage::GetSize() const
{
    return size_;
}
//设置页中存储的大小
void BPlusTreePage::SetSize(int size)
{
    size_ = size;
}
//把页中存储的大小增加amount
void BPlusTreePage::IncreaseSize(int amount)
{
    size_ += amount;
}

/*
 * Helper methods to get/set max size (capacity) of the page
 */
//得到页的容量
int BPlusTreePage::GetMaxSize() const
{
    return max_size_;
}
//设置页的容量
void BPlusTreePage::SetMaxSize(int size)
{
    max_size_ = size;
}

/*
 * Helper method to get min page size
 * Generally, min page size == max page size / 2
 * With n = 4 in our example B+-tree, each leaf must contain at least 2 values, and at most 3 values.
 */
int BPlusTreePage::GetMinSize() const {
  if (IsRootPage()) {//why not 0 is min? because 1 is only have a pointer without key, so just equal to empty
    return IsLeafPage() ? 1
                        : 2;//root page & leaf page may be empty tree, so is 1; otherwise at least 1 node in root, size 2
  }
  return (max_size_ ) / 2;
}

/*
 * Helper methods to get/set parent page id
 */
page_id_t BPlusTreePage::GetParentPageId() const
{
    return parent_page_id_;
}

void BPlusTreePage::SetParentPageId(page_id_t parent_page_id)
{
    parent_page_id_ = parent_page_id;
}

/*
 * Helper methods to get/set self page id
 */
page_id_t BPlusTreePage::GetPageId() const
{
    return page_id_;
}

void BPlusTreePage::SetPageId(page_id_t page_id)
{
    page_id_ = page_id;
}

/*
 * Helper methods to set lsn
 */
void BPlusTreePage::SetLSN(lsn_t lsn)
{
    lsn_ = lsn;
}

/* for concurrent index */
bool BPlusTreePage::IsSafe(OpType op) {
  int size = GetSize();
  if (op == OpType::INSERT) {
    return size < GetMaxSize();
  }
  int minSize = GetMinSize() + 1;
  if (op == OpType::DELETE) {
    return (IsLeafPage()) ? size >= minSize : size > minSize;
  }
  assert(false);//invalid area
}

} // namespace scudb
