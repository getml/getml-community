#ifndef AUTOSQL_MULTITHREADING_READLOCK_HPP_
#define AUTOSQL_MULTITHREADING_READLOCK_HPP_

namespace multithreading
{
// ----------------------------------------------------------------------------

class ReadLock
{
    // -------------------------------

   public:
    ReadLock( const std::shared_ptr<ReadWriteLock>& _lock )
        : lock_( _lock ), released_( false )
    {
        lock_->read_lock();
    }

    ~ReadLock() { unlock(); };

    // -------------------------------

    /// Because of the boolean variable, this operator is forbidden.
    ReadLock& operator=( const ReadLock& _other ) = delete;

    /// Lock the ReadWriteLock.
    void lock()
    {
        assert_true( released_ );
        released_ = false;
        lock_->read_lock();
    }

    /// Unlock the ReadWriteLock.
    void unlock()
    {
        if ( !released_ )
            {
                lock_->read_unlock();
                released_ = true;
            }
    }

    // -------------------------------
   private:
    /// Lock to the ReadLock.
    const std::shared_ptr<ReadWriteLock> lock_;

    /// Whether the Acquirer has been released.
    bool released_;
};

// ----------------------------------------------------------------------------
}  // namespace multithreading

#endif  // AUTOSQL_MULTITHREADING_READLOCK_HPP_
