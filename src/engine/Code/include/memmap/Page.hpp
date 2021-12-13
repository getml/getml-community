#ifndef MEMMAP_PAGE_HPP_
#define MEMMAP_PAGE_HPP_

namespace memmap
{
// ----------------------------------------------------------------------------

struct Page
{
    Page() : block_size_( 0 ), is_allocated_( false ) {}

    ~Page() = default;

    /// The size of the allocated memory block, in number of pages,
    /// if this is the first page of an allocated memory block, zero
    /// otherwise.
    size_t block_size_;

    /// Whether the page is allocated.
    bool is_allocated_;
};

// ----------------------------------------------------------------------------
}  // namespace memmap

#endif  // MEMMAP_PAGE_HPP_
