#ifndef MULTITHREADING_SPINLOCK_HPP_
#define MULTITHREADING_SPINLOCK_HPP_

namespace multithreading
{
// ----------------------------------------------------------------------------
class Spinlock
{
   public:
    Spinlock() { flag_.clear(); }

    ~Spinlock() = default;

    // -------------------------------

    inline void lock()
    {
        while ( flag_.test_and_set( std::memory_order_acquire ) )
            {
            }
    }

    inline void unlock() { flag_.clear( std::memory_order_release ); }

    // -------------------------------

   private:
    // Atomic flag for synchronization
    std::atomic_flag flag_;
};

// ----------------------------------------------------------------------------
}  // namespace multithreading

#endif  // MULTITHREADING_SPINLOCK_HPP_
