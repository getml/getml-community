#ifndef HELPERS_SQLGENERATOR_HPP_
#define HELPERS_SQLGENERATOR_HPP_

namespace helpers
{
// -------------------------------------------------------------------------

class SQLGenerator
{
    typedef typename MappingContainer::ColnameMap ColnameMap;

   public:
    /// Removes the Macros from the colname and replaces it with proper SQLite3
    /// code.
    static std::string edit_colname(
        const std::string& _raw_name, const std::string& _alias );

    /// Extract the table name from a raw string.
    static std::string get_table_name( const std::string& _raw_name );

    /// Generates the joins for when there is a many-to-one or one-to-one join.
    static std::string handle_many_to_one_joins(
        const std::string& _table_name, const std::string& _t1_or_t2 );

    /// Makes a clean, but unique colname.
    static std::string make_colname( const std::string& _colname );

    /// Generates the number of seconds (including fractional seconds) since
    /// epoch time.
    static std::string make_epoch_time(
        const std::string& _raw_name, const std::string& _alias );

    /// Generates the joins to be included in every single .
    static std::string make_joins(
        const std::string& _output_name,
        const std::string& _input_name,
        const std::string& _output_join_keys_name,
        const std::string& _input_join_keys_name );

    /// Generates an SQLite3-compatible datetime string. This is to be used when
    /// the absolute value is note important, because the string is only
    /// compared to something else.
    static std::string make_relative_time(
        const std::string& _raw_name, const std::string& _alias );

    /// Generates the staging tables.
    static std::vector<std::string> make_staging_tables(
        const bool& _include_targets,
        const Placeholder& _population_schema,
        const std::vector<Placeholder>& _peripheral_schema,
        const ColnameMap& _colname_map );

    /// Generates the name for a staging table.
    static std::string make_staging_table_name( const std::string& _name );

    /// Generates the unique identifier for a subfeature.
    static std::string make_subfeature_identifier(
        const std::string& _feature_prefix,
        const size_t _peripheral_used,
        const size_t _column );

    /// Generates the code for joining the subfeature tables.
    static std::string make_subfeature_joins(
        const std::string& _feature_prefix,
        const size_t _peripheral_used,
        const std::set<size_t>& _columns );

    /// Generates the diffstring for time stamps.
    static std::string make_time_stamp_diff(
        const Float _diff, const bool _is_rowid );

    /// Generates the SQL code for the time stamp conditions.
    static std::string make_time_stamps(
        const std::string& _time_stamp_name,
        const std::string& _lower_time_stamp_name,
        const std::string& _upper_time_stamp_name,
        const std::string& _output_alias,
        const std::string& _input_alias,
        const std::string& _t1_or_t2 );

    /// Returns the lower case of a string
    static std::string to_lower( const std::string& _str );

    /// Returns the upper case of a string
    static std::string to_upper( const std::string& _str );

   private:
    /// Generates the SQL code for when there are multiple join keys.
    static std::string handle_multiple_join_keys(
        const std::string& _output_join_keys_name,
        const std::string& _input_join_keys_name,
        const std::string& _output_alias,
        const std::string& _input_alias );

    /// Generates the SQL code necessary for joining the mapping tables onto the
    /// staged table.
    static std::string join_mappings(
        const std::string& _name, const std::vector<std::string>& _mappings );

    /// Generates the queries for the join keys.
    static std::string make_join_keys(
        const std::string& _output_join_keys_name,
        const std::string& _input_join_keys_name,
        const std::string& _output_alias,
        const std::string& _input_alias );

    /// Generates the columns for a single staging table.
    static std::vector<std::string> make_staging_columns(
        const bool& _include_targets,
        const Placeholder& _schema,
        const std::vector<std::string>& _mappings );

    /// Generates a single staging table.
    static std::string make_staging_table(
        const bool& _include_targets,
        const Placeholder& _schema,
        const std::vector<std::string>& _mappings );
};

// -------------------------------------------------------------------------
}  // namespace helpers

#endif
