#ifndef FASTPROP_CONTAINERS_SQLMAKER_HPP_
#define FASTPROP_CONTAINERS_SQLMAKER_HPP_

// ----------------------------------------------------------------------------

namespace fastprop
{
namespace containers
{
// ------------------------------------------------------------------------

class SQLMaker
{
   public:
    typedef typename AbstractFeature::VocabForDf VocabForDf;

    typedef std::vector<VocabForDf> Vocabulary;

   public:
    /// Creates a condition.
    static std::string condition(
        const std::vector<strings::String>& _categories,
        const std::string& _feature_prefix,
        const Condition& _condition,
        const helpers::Schema& _input,
        const helpers::Schema& _output );

    /// Creates a select statement (SELECT AGGREGATION(VALUE TO TO BE
    /// AGGREGATED)).
    static std::string select_statement(
        const std::vector<strings::String>& _categories,
        const std::string& _feature_prefix,
        const AbstractFeature& _abstract_feature,
        const helpers::Schema& _input,
        const helpers::Schema& _output );

   public:
    /// Whether the aggregation is an aggregation that relies on the
    /// first-last-logic.
    static bool is_first_last( const enums::Aggregation _agg )
    {
        return (
            _agg == enums::Aggregation::first ||
            _agg == enums::Aggregation::last ||
            _agg == enums::Aggregation::ewma1s ||
            _agg == enums::Aggregation::ewma1m ||
            _agg == enums::Aggregation::ewma1h ||
            _agg == enums::Aggregation::ewma1d ||
            _agg == enums::Aggregation::ewma7d ||
            _agg == enums::Aggregation::ewma30d ||
            _agg == enums::Aggregation::ewma90d ||
            _agg == enums::Aggregation::ewma365d ||
            _agg == enums::Aggregation::time_since_first_maximum ||
            _agg == enums::Aggregation::time_since_first_minimum ||
            _agg == enums::Aggregation::time_since_last_maximum ||
            _agg == enums::Aggregation::time_since_last_minimum );
    }

   private:
    /// Returns the column name signified by _column_used and _data_used.
    static std::string get_name(
        const std::string& _feature_prefix,
        const enums::DataUsed _data_used,
        const size_t _peripheral,
        const size_t _input_col,
        const size_t _output_col,
        const helpers::Schema& _input,
        const helpers::Schema& _output );

    /// Returns the column names signified by _column_used and _data_used, for
    /// same_units_...
    static std::pair<std::string, std::string> get_same_units(
        const enums::DataUsed _data_used,
        const size_t _input_col,
        const size_t _output_col,
        const helpers::Schema& _input,
        const helpers::Schema& _output );

    /// Generates an additional argument passed to the aggregation function.
    static std::string make_additional_argument(
        const enums::Aggregation& _aggregation,
        const helpers::Schema& _input,
        const helpers::Schema& _output );

    /// Returns the select statement for AVG_TIME_BETWEEN.
    static std::string select_avg_time_between( const helpers::Schema& _input );

    /// Creates the value to be aggregated (for instance a column name or the
    /// difference between two columns)
    static std::string value_to_be_aggregated(
        const std::vector<strings::String>& _categories,
        const std::string& _feature_prefix,
        const AbstractFeature& _abstract_feature,
        const helpers::Schema& _input,
        const helpers::Schema& _output );
};

// ------------------------------------------------------------------------
}  // namespace containers
}  // namespace fastprop

// ----------------------------------------------------------------------------

#endif  // FASTPROP_CONTAINERS_SQLMAKER_HPP_
