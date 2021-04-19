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
    Hyperopt( const Poco::JSON::Object& _obj ) : obj_( _obj ) {}

    Hyperopt( const std::string& _path )
        : obj_( jsonutils::JSON::load( _path + "obj.json" ) )
    {
    }

    ~Hyperopt() = default;

   public:
    /// Saves the hyperopt object.
    void save(
        const std::string& _temp_dir,
        const std::string& _path,
        const std::string& _name ) const;

    /// Displays the hyperparameter optimization in a form the monitor can
    /// understand.
    Poco::JSON::Object to_monitor() const;

   public:
    /// Returns the underlying parameters as a JSON object.
    Poco::JSON::Object obj() const { return obj_; }

   private:
    /// The command originally sent by the API to construct this hyperparameter
    /// optimization object.
    Poco::JSON::Object obj_;
};

// ----------------------------------------------------
}  // namespace hyperparam
}  // namespace engine

#endif  // ENGINE_HYPERPARAM_HYPEROPT_HPP_
