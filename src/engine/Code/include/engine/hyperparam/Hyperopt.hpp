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
    Hyperopt( const Poco::JSON::Object& _cmd )
        : cmd_( Poco::JSON::Object::Ptr( new Poco::JSON::Object( _cmd ) ) )
    {
    }

    ~Hyperopt() = default;

   public:
    /// Returns the command as a string.
    std::string cmd_str() const
    {
        assert_true( cmd_ );
        return JSON::stringify( *cmd_ );
    }

   private:
    /// The command originally sent by the API to construct this hyperparameter
    /// optimization object.
    Poco::JSON::Object::Ptr cmd_;
};

// ----------------------------------------------------
}  // namespace hyperparam
}  // namespace engine

#endif  // ENGINE_HYPERPARAM_HYPEROPT_HPP_
