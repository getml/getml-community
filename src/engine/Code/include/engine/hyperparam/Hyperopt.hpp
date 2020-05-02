#ifndef ENGINE_HYPERPARAM_HYPEROPT_HPP_
#define ENGINE_HYPERPARAM_HYPEROPT_HPP_

namespace engine
{
namespace hyperparam
{
// ----------------------------------------------------

class Hyperopt
{
   public:
    Hyperopt( const Poco::JSON::Object& _cmd ) : cmd_( _cmd ) {}

    ~Hyperopt() = default;

   public:
    /// Returns the command as a string.
    Poco::JSON::Object cmd() const { return cmd_; }

   private:
    /// The command originally sent by the API to construct this hyperparameter
    /// optimization object.
    Poco::JSON::Object cmd_;
};

// ----------------------------------------------------
}  // namespace hyperparam
}  // namespace engine

#endif  // ENGINE_HYPERPARAM_HYPEROPT_HPP_
