#ifndef HELPERS_SQLDIALECTGENERATOR_HPP_
#define HELPERS_SQLDIALECTGENERATOR_HPP_

namespace helpers
{
// -------------------------------------------------------------------------

class SQLDialectGenerator
{
   public:
    /// Expresses an aggregation in the SQL dialect.
    virtual std::string aggregation(
        const enums::Aggregation& _agg,
        const std::string& _colname1,
        const std::optional<std::string>& _colname2 ) const = 0;

    /// Removes the Macros from the colname and replaces it with proper SQLite3
    /// code.
    virtual std::string edit_colname(
        const std::string& _raw_name, const std::string& _alias ) const = 0;

    /// Generates the SQL code necessary for joining the mapping tables onto the
    /// staged table.
    virtual std::string join_mapping(
        const std::string& _name,
        const std::string& _colname,
        const bool _is_text ) const = 0;

    /// Makes a clean, but unique colname.
    virtual std::string make_colname( const std::string& _colname ) const = 0;

    /// Generates the table that contains all the features.
    virtual std::string make_feature_table(
        const std::string& _main_table,
        const std::vector<std::string>& _autofeatures,
        const std::vector<std::string>& _targets,
        const std::vector<std::string>& _categorical,
        const std::vector<std::string>& _numerical,
        const std::string& _prefix ) const = 0;

    /// Generates the joins to be included in every single .
    virtual std::string make_joins(
        const std::string& _output_name,
        const std::string& _input_name,
        const std::string& _output_join_keys_name,
        const std::string& _input_join_keys_name ) const = 0;

    /// Generates the table header for the resulting SQL code.
    virtual std::string make_mapping_table_header(
        const std::string& _name, const bool _key_is_num ) const = 0;

    /// Generates the SQL code needed to impute the features and drop the
    /// feature tables.
    virtual std::string make_postprocessing(
        const std::vector<std::string>& _sql ) const = 0;

    /// Generates the select statement for the feature table.
    virtual std::string make_select(
        const std::string& _main_table,
        const std::vector<std::string>& _autofeatures,
        const std::vector<std::string>& _targets,
        const std::vector<std::string>& _categorical,
        const std::vector<std::string>& _numerical ) const = 0;

    /// Transpiles the features in SQLite3 code. This
    /// is supposed to replicate the .transform(...) method
    /// of a pipeline.
    virtual std::string make_sql(
        const std::string& _main_table,
        const std::vector<std::string>& _autofeatures,
        const std::vector<std::string>& _sql,
        const std::vector<std::string>& _targets,
        const std::vector<std::string>& _categorical,
        const std::vector<std::string>& _numerical ) const = 0;

    /// Generates the staging tables.
    virtual std::vector<std::string> make_staging_tables(
        const bool _population_needs_targets,
        const std::vector<bool>& _peripheral_needs_targets,
        const Schema& _population_schema,
        const std::vector<Schema>& _peripheral_schema ) const = 0;

    /// Generates the code for joining the subfeature tables.
    virtual std::string make_subfeature_joins(
        const std::string& _feature_prefix,
        const size_t _peripheral_used,
        const std::string& _alias = "t2",
        const std::string& _feature_postfix = "" ) const = 0;

    /// Generates the code for the time stamp conditions.
    virtual std::string make_time_stamps(
        const std::string& _time_stamp_name,
        const std::string& _lower_time_stamp_name,
        const std::string& _upper_time_stamp_name,
        const std::string& _output_alias,
        const std::string& _input_alias,
        const std::string& _t1_or_t2 ) const = 0;

    /// The first quotechar.
    virtual std::string quotechar1() const = 0;

    /// The second quotechar.
    virtual std::string quotechar2() const = 0;

    /// Generates code for the text field splitter.
    virtual std::string split_text_fields(
        const std::shared_ptr<ColumnDescription>& _desc ) const = 0;

    /// Generates code to check whether a string contains another string.
    virtual std::string string_contains(
        const std::string& _colname,
        const std::string& _keyword,
        const bool _contains ) const = 0;
};

// -------------------------------------------------------------------------
}  // namespace helpers

#endif  // HELPERS_SQLDIALECTGENERATOR_HPP_
