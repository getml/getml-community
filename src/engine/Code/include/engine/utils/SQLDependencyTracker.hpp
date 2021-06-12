#ifndef ENGINE_UTILS_SQLDEPENDENCYTRACKER_HPP_
#define ENGINE_UTILS_SQLDEPENDENCYTRACKER_HPP_

namespace engine
{
namespace utils
{
// ------------------------------------------------------------------------

class SQLDependencyTracker
{
   private:
    typedef std::vector<std::tuple<std::string, std::string, std::string>>
        Tuples;

   public:
    SQLDependencyTracker( const std::string& _folder ) : folder_( _folder ) {}

    ~SQLDependencyTracker() = default;

   public:
    /// Returns a dependency graph for the SQL features.
    void save_dependencies( const std::string& _sql ) const;

    /// Saves the SQL code to vector of files.
    Tuples save_sql( const std::string& _sql ) const;

   private:
    /// Finds the dependencies of _tuple _i.
    Poco::JSON::Object::Ptr find_dependencies(
        const Tuples& _tuples, const size_t _i ) const;

    /// Infers the table name from the SQL code.
    std::string infer_table_name( const std::string& _sql ) const;

    /// Writes content to a file.
    void write_to_file(
        const std::string& _fname, const std::string& _content ) const;

   private:
    /// The folder to which we want to store the results.
    const std::string folder_;
};

// ------------------------------------------------------------------------
}  // namespace utils
}  // namespace engine

#endif  // ENGINE_UTILS_SQLDEPENDENCYTRACKER_HPP_
