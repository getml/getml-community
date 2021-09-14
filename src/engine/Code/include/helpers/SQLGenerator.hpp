#ifndef HELPERS_SQLGENERATOR_HPP_
#define HELPERS_SQLGENERATOR_HPP_

namespace helpers
{
// -------------------------------------------------------------------------

class SQLGenerator
{
   public:
    static constexpr bool FOR_STAGING = true;
    static constexpr bool NOT_FOR_STAGING = false;

   public:
    /// Extract the table name from a raw string.
    static std::string get_table_name( const std::string& _raw_name );

    /// Generates the joins for when there is a many-to-one or one-to-one join.
    static std::string handle_many_to_one_joins(
        const std::string& _table_name,
        const std::string& _t1_or_t2,
        const SQLDialectGenerator* _sql_dialect_generator );

    /// Generates the SQL code for when there are multiple join keys.
    static std::string handle_multiple_join_keys(
        const std::string& _output_join_keys_name,
        const std::string& _input_join_keys_name,
        const std::string& _output_alias,
        const std::string& _input_alias,
        const bool _for_staging,
        const SQLDialectGenerator* _sql_dialect_generator );

    /// Determines whether we want to include a column.
    static bool include_column( const std::string& _name );

    /// Generates the SQL code needed to impute the features and drop the
    /// feature tables.
    static std::string make_postprocessing(
        const std::vector<std::string>& _sql );

    /// Generates the name for a staging table.
    static std::string make_staging_table_name( const std::string& _name );

    /// Generates the unique identifier for a subfeature.
    static std::string make_subfeature_identifier(
        const std::string& _feature_prefix, const size_t _peripheral_used );

    /// Generates the diffstring for time stamps.
    static std::string make_time_stamp_diff(
        const Float _diff, const bool _is_rowid );

    /// Reverse of make_time_stamp_diff.
    static Float parse_time_stamp_diff( const std::string& _diff );

    /// Replaces all non-alphanumeric characters with '_'.
    static std::string replace_non_alphanumeric( const std::string _old );

    /// Returns the lower case of a string
    static std::string to_lower( const std::string& _str );

    /// Returns the upper case of a string
    static std::string to_upper( const std::string& _str );
};

// -------------------------------------------------------------------------
}  // namespace helpers

#endif
