#ifndef DFS_ENUMS_PARSER_HPP_
#define DFS_ENUMS_PARSER_HPP_

// ----------------------------------------------------------------------------

namespace dfs
{
namespace enums
{
// ----------------------------------------------------------------------------

template <class T>
class Parser
{
};

// ----------------------------------------------------------------------------

template <>
struct Parser<Aggregation>
{
    static constexpr const char* AVG = "AVG";
    static constexpr const char* AVG_TIME_BETWEEN = "AVG TIME BETWEEN";
    static constexpr const char* COUNT = "COUNT";
    static constexpr const char* COUNT_DISTINCT = "COUNT DISTINCT";
    static constexpr const char* COUNT_MINUS_COUNT_DISTINCT =
        "COUNT MINUS COUNT DISTINCT";
    static constexpr const char* FIRST = "FIRST";
    static constexpr const char* LAST = "LAST";
    static constexpr const char* MAX = "MAX";
    static constexpr const char* MEDIAN = "MEDIAN";
    static constexpr const char* MIN = "MIN";
    static constexpr const char* SUM = "SUM";
    static constexpr const char* STDDEV = "STDDEV";
    static constexpr const char* VAR = "VAR";

    /// Parse parses a _str.
    static Aggregation parse( const std::string& _str )
    {
        if ( _str == AVG )
            {
                return Aggregation::avg;
            }

        if ( _str == AVG_TIME_BETWEEN )
            {
                return Aggregation::avg_time_between;
            }

        if ( _str == COUNT )
            {
                return Aggregation::count;
            }

        if ( _str == COUNT_DISTINCT )
            {
                return Aggregation::count_distinct;
            }

        if ( _str == COUNT_MINUS_COUNT_DISTINCT )
            {
                return Aggregation::count_minus_count_distinct;
            }

        if ( _str == FIRST )
            {
                return Aggregation::first;
            }

        if ( _str == LAST )
            {
                return Aggregation::last;
            }

        if ( _str == MAX )
            {
                return Aggregation::max;
            }

        if ( _str == MEDIAN )
            {
                return Aggregation::median;
            }

        if ( _str == MIN )
            {
                return Aggregation::min;
            }

        if ( _str == STDDEV )
            {
                return Aggregation::stddev;
            }

        if ( _str == SUM )
            {
                return Aggregation::sum;
            }

        if ( _str == VAR )
            {
                return Aggregation::var;
            }

        throw_unless( false, "DFS: Unknown aggregation: '" + _str + "'" );

        return Aggregation::avg;
    }

    /// to_str expresses the aggregation as a string.
    static std::string to_str( const Aggregation _aggregation )
    {
        switch ( _aggregation )
            {
                case Aggregation::avg:
                    return AVG;

                case Aggregation::avg_time_between:
                    return AVG_TIME_BETWEEN;

                case Aggregation::count:
                    return COUNT;

                case Aggregation::count_distinct:
                    return COUNT_DISTINCT;

                case Aggregation::count_minus_count_distinct:
                    return COUNT_MINUS_COUNT_DISTINCT;

                case Aggregation::first:
                    return FIRST;

                case Aggregation::last:
                    return LAST;

                case Aggregation::max:
                    return MAX;

                case Aggregation::median:
                    return MEDIAN;

                case Aggregation::min:
                    return MIN;

                case Aggregation::stddev:
                    return STDDEV;

                case Aggregation::sum:
                    return SUM;

                case Aggregation::var:
                    return VAR;

                default:
                    throw_unless( false, "DFS: Unknown aggregation." );
                    return "";
            }
    }
};

// ----------------------------------------------------------------------------

template <>
struct Parser<DataUsed>
{
    static constexpr const char* CATEGORICAL = "categorical";
    static constexpr const char* DISCRETE = "discrete";
    static constexpr const char* NOT_APPLICABLE = "na";
    static constexpr const char* NUMERICAL = "numerical";
    static constexpr const char* SAME_UNITS_CATEGORICAL =
        "same_units_categorical";
    static constexpr const char* SAME_UNITS_DISCRETE = "same_units_discrete";
    static constexpr const char* SAME_UNITS_DISCRETE_TS =
        "same_units_discrete_ts";
    static constexpr const char* SAME_UNITS_NUMERICAL = "same_units_numerical";
    static constexpr const char* SAME_UNITS_NUMERICAL_TS =
        "same_units_numerical_ts";
    static constexpr const char* SUBFEATURES = "subfeatures";

    /// Parse parses a _str.
    static DataUsed parse( const std::string& _str )
    {
        if ( _str == CATEGORICAL )
            {
                return DataUsed::categorical;
            }

        if ( _str == DISCRETE )
            {
                return DataUsed::discrete;
            }

        if ( _str == NOT_APPLICABLE )
            {
                return DataUsed::not_applicable;
            }

        if ( _str == NUMERICAL )
            {
                return DataUsed::numerical;
            }

        if ( _str == SAME_UNITS_CATEGORICAL )
            {
                return DataUsed::same_units_categorical;
            }

        if ( _str == SAME_UNITS_DISCRETE )
            {
                return DataUsed::same_units_discrete;
            }

        if ( _str == SAME_UNITS_DISCRETE_TS )
            {
                return DataUsed::same_units_discrete_ts;
            }

        if ( _str == SAME_UNITS_NUMERICAL )
            {
                return DataUsed::same_units_numerical;
            }

        if ( _str == SAME_UNITS_NUMERICAL_TS )
            {
                return DataUsed::same_units_numerical_ts;
            }

        if ( _str == SUBFEATURES )
            {
                return DataUsed::subfeatures;
            }

        throw_unless( false, "DFS: Unknown data used: '" + _str + "'" );

        return DataUsed::same_units_numerical;
    }

    /// to_str expresses the aggregation as a string.
    static std::string to_str( const DataUsed _data_used )
    {
        switch ( _data_used )
            {
                case DataUsed::categorical:
                    return CATEGORICAL;

                case DataUsed::discrete:
                    return DISCRETE;

                case DataUsed::not_applicable:
                    return NOT_APPLICABLE;

                case DataUsed::numerical:
                    return NUMERICAL;

                case DataUsed::same_units_categorical:
                    return SAME_UNITS_CATEGORICAL;

                case DataUsed::same_units_discrete:
                    return SAME_UNITS_DISCRETE;

                case DataUsed::same_units_discrete_ts:
                    return SAME_UNITS_DISCRETE_TS;

                case DataUsed::same_units_numerical:
                    return SAME_UNITS_NUMERICAL;

                case DataUsed::same_units_numerical_ts:
                    return SAME_UNITS_NUMERICAL_TS;

                case DataUsed::subfeatures:
                    return SUBFEATURES;

                default:
                    throw_unless( false, "DFS: Unknown data used." );
                    return "";
            }
    }
};

// ----------------------------------------------------------------------------

}  // namespace enums
}  // namespace dfs

// ----------------------------------------------------------------------------

#endif  // DFS_ENUMS_PARSER_HPP_
