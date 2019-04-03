#ifndef MULTITHREADING_COMMUNICATOR_HPP_
#define MULTITHREADING_COMMUNICATOR_HPP_

namespace multithreading
{
// ----------------------------------------------------------------------------

class Communicator
{
   public:
    Communicator( size_t _num_threads )
        : barrier_( _num_threads ),
          num_threads_( _num_threads ),
          num_threads_left_( _num_threads )
    {
        main_thread_id_ = std::this_thread::get_id();
    }

    ~Communicator() = default;

    // -----------------------------------------

    /// Waits until all threads have reached this point
    inline void barrier() { barrier_.wait(); }

    /// Accessor to the shared data
    template <class T>
    inline T* global_data()
    {
        return reinterpret_cast<T*>( global_data_.data() );
    }

    /// Const accessor to the shared data
    template <class T>
    inline const T* global_data_const()
    {
        return reinterpret_cast<const T*>( global_data_.data() );
    }

    /// Locks the spinlock
    inline void lock() { spinlock_.lock(); }

    /// Returns the id of the main thread
    inline const std::thread::id& main_thread_id() { return main_thread_id_; }

    /// Returns the number of threads
    inline const size_t& num_threads() { return num_threads_; }

    /// Returns the number of threads that haven't reached this point
    inline std::atomic<size_t>& num_threads_left() { return num_threads_left_; }

    /// To ensure compatability with MPI
    inline const size_t rank()
    {
        return (
            ( std::this_thread::get_id() == main_thread_id_ ) ? ( 0 ) : ( 1 ) );
    }

    /// Resizes the global (shared) data
    template <class T>
    inline void resize( const size_t _size )
    {
        if ( global_data_.size() < _size * sizeof( T ) )
            {
                global_data_.resize( _size * sizeof( T ) );
            }
    }

    /// Unlocks the spinlock
    inline void unlock() { spinlock_.unlock(); }

    // -----------------------------------------

   private:
    /// Barrier used for the communicator
    Barrier barrier_;

    /// Storage for the global data. Note that
    /// sizeof(char) is 1 by definition.
    std::vector<char> global_data_;

    /// Id of the main thread
    std::thread::id main_thread_id_;

    /// Total number of threads
    size_t num_threads_;

    /// Number of threads that have not updated the global data
    /// in the current generation.
    std::atomic<size_t> num_threads_left_;

    /// Spinlock protecting the global data
    Spinlock spinlock_;
};

// ----------------------------------------------------------------------------
}  // namespace multithreading

#endif  // MULTITHREADING_COMMUNICATOR_HPP_
