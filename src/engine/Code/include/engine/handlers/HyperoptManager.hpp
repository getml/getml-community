#ifndef ENGINE_HANDLERS_HYPEROPTMANAGER_HPP_
#define ENGINE_HANDLERS_HYPEROPTMANAGER_HPP_

namespace engine
{
namespace handlers
{
// ------------------------------------------------------------------------

class HyperoptManager
{
   public:
    HyperoptManager(
        const std::shared_ptr<std::map<std::string, hyperparam::Hyperopt>>&
            _hyperopts,
        const std::shared_ptr<const monitoring::Monitor>& _monitor,
        const std::shared_ptr<std::mutex>& _project_mtx,
        const std::shared_ptr<multithreading::ReadWriteLock>& _read_write_lock )
        : hyperopts_( _hyperopts ),
          monitor_( _monitor ),
          project_mtx_( _project_mtx ),
          read_write_lock_( _read_write_lock )
    {
    }

    ~HyperoptManager() = default;

   public:
    /// Adds a new hyperparameter optimization.
    void add_hyperopt(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Launches a hyperparameter optimization.
    void launch(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

   private:
    /// Trivial (private) accessor
    std::map<std::string, hyperparam::Hyperopt>& hyperopts()
    {
        assert_true( hyperopts_ );
        return *hyperopts_;
    }

    /// Trivial (private) accessor
    std::mutex& project_mtx()
    {
        assert_true( project_mtx_ );
        return *project_mtx_;
    }

   private:
    /// The Hyperopts currently held in memory
    const std::shared_ptr<std::map<std::string, hyperparam::Hyperopt>>
        hyperopts_;

    /// For communication with the monitor
    const std::shared_ptr<const monitoring::Monitor> monitor_;

    /// It is sometimes necessary to prevent us from changing the project.
    const std::shared_ptr<std::mutex> project_mtx_;

    /// For coordinating the read and write process of the data
    const std::shared_ptr<multithreading::ReadWriteLock> read_write_lock_;
};

// ------------------------------------------------------------------------

}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_HYPEROPTMANAGER_HPP_

