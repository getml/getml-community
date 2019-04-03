#ifndef MULTITHREADING_WRITELOCK_HPP_
#define MULTITHREADING_WRITELOCK_HPP_

namespace multithreading
{
// ----------------------------------------------------------------------------

class WriteLock
{
    // -------------------------------

   public:
    WriteLock( const std::shared_ptr<ReadWriteLock>& _lock )
        : lock_( _lock ), released_( false )
    {
        lock_->write_lock();
    }

    ~WriteLock() { unlock(); };

    // -------------------------------

    /// Because of the boolean variable, this operator is forbidden.
    WriteLock& operator=( const WriteLock& _other ) = delete;

    /// Lock the ReadWriteLock.
    void lock()
    {
        assert( released_ );
        released_ = false;
        lock_->write_lock();
    }

    /// Unlock the ReadWriteLock.
    void unlock()
    {
        if ( !released_ )
            {
                lock_->write_unlock();
                released_ = true;
            }
    }

    // -------------------------------
   private:
    /// Lock to the WriteLock.
    const std::shared_ptr<ReadWriteLock> lock_;

    /// Whether the Acquirer has been released.
    bool released_;
};

// ----------------------------------------------------------------------------
}  // namespace multithreading

#endif  // MULTITHREADING_WRITELOCK_HPP_
