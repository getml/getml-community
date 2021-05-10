#ifndef HELPERS_SQLGENERATOR_HPP_
#define HELPERS_SQLGENERATOR_HPP_

namespace helpers
{
// -------------------------------------------------------------------------

class SQLGenerator
{
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

    /// Generates the SQL code necessary for joining the mapping tables onto the
    /// staged table.
    static std::string join_mapping(
        const std::string& _name,
        const std::string& _colname,
        const bool _is_text );

    /// Makes a clean, but unique colname.
    static std::string make_colname( const std::string& _colname );

    /// Generates the number of seconds (including fractional seconds) since
    /// epoch time.
    static std::string make_epoch_time(
        const std::string& _raw_name, const std::string& _alias );

    /// Generates the table that contains all the features.
    static std::string make_feature_table(
        const std::string& _main_table,
        const std::vector<std::string>& _autofeatures,
        const std::vector<std::string>& _targets,
        const std::vector<std::string>& _categorical,
        const std::vector<std::string>& _numerical,
        const std::string& _prefix );

    /// Generates the joins to be included in every single .
    static std::string make_joins(
        const std::string& _output_name,
        const std::string& _input_name,
        const std::string& _output_join_keys_name,
        const std::string& _input_join_keys_name );

    /// Generates the SQL code needed to impute the features and drop the
    /// feature tables.
    static std::string make_postprocessing(
        const std::vector<std::string>& _sql );

    /// Generates the select statement for the feature table.
    static std::string make_select(
        const std::string& _main_table,
        const std::vector<std::string>& _autofeatures,
        const std::vector<std::string>& _targets,
        const std::vector<std::string>& _categorical,
        const std::vector<std::string>& _numerical );

    /// Generates an SQLite3-compatible datetime string. This is to be used when
    /// the absolute value is note important, because the string is only
    /// compared to something else.
    static std::string make_relative_time(
        const std::string& _raw_name, const std::string& _alias );

    /// Transpiles the features in SQLite3 code. This
    /// is supposed to replicate the .transform(...) method
    /// of a pipeline.
    static std::string make_sql(
        const std::string& _main_table,
        const std::vector<std::string>& _autofeatures,
        const std::vector<std::string>& _sql,
        const std::vector<std::string>& _targets,
        const std::vector<std::string>& _categorical,
        const std::vector<std::string>& _numerical );

    /// Generates the staging tables.
    static std::vector<std::string> make_staging_tables(
        const bool _population_needs_targets,
        const std::vector<bool>& _peripheral_needs_targets,
        const Schema& _population_schema,
        const std::vector<Schema>& _peripheral_schema );

    /// Generates the name for a staging table.
    static std::string make_staging_table_name( const std::string& _name );

    /// Generates the unique identifier for a subfeature.
    static std::string make_subfeature_identifier(
        const std::string& _feature_prefix, const size_t _peripheral_used );

    /// Generates the code for joining the subfeature tables.
    static std::string make_subfeature_joins(
        const std::string& _feature_prefix,
        const size_t _peripheral_used,
        const std::string& _alias = "t2",
        const std::string& _feature_postfix = "" );

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

    /// Generates the SQL code needed to insert the autofeatures into the
    /// FEATURES table.
    static std::string make_updates(
        const std::vector<std::string>& _autofeatures,
        const std::string& _prefix );

    /// Returns the lower case of a string
    static std::string to_lower( const std::string& _str );

    /// Returns the upper case of a string
    static std::string to_upper( const std::string& _str );

   private:
    /// Creates the indices for a staging table.
    static std::string create_indices(
        const std::string& _table_name, const helpers::Schema& _schema );

    /// Parses the prefix,  the new name and the postfix out of the
    /// raw name.
    static std::tuple<std::string, std::string, std::string> demangle_colname(
        const std::string& _raw_name );

    /// Generates the SQL code for when there are multiple join keys.
    static std::string handle_multiple_join_keys(
        const std::string& _output_join_keys_name,
        const std::string& _input_join_keys_name,
        const std::string& _output_alias,
        const std::string& _input_alias );

    /// Determines whether we want to include a column.
    static bool include_column( const std::string& _name );

    /// Generates the queries for the join keys.
    static std::string make_join_keys(
        const std::string& _output_join_keys_name,
        const std::string& _input_join_keys_name,
        const std::string& _output_alias,
        const std::string& _input_alias );

    /// Generates the columns for a single staging table.
    static std::vector<std::string> make_staging_columns(
        const bool& _include_targets, const Schema& _schema );

    /// Generates a single staging table.
    static std::string make_staging_table(
        const bool& _include_targets, const Schema& _schema );
};

// -------------------------------------------------------------------------
}  // namespace helpers

#endif
