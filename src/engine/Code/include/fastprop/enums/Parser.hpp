#ifndef FASTPROP_ENUMS_PARSER_HPP_
#define FASTPROP_ENUMS_PARSER_HPP_

// ----------------------------------------------------------------------------

namespace fastprop
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
    static constexpr const char* COUNT_ABOVE_MEAN = "COUNT ABOVE MEAN";
    static constexpr const char* COUNT_BELOW_MEAN = "COUNT BELOW MEAN";
    static constexpr const char* COUNT_DISTINCT = "COUNT DISTINCT";
    static constexpr const char* COUNT_DISTINCT_OVER_COUNT =
        "COUNT DISTINCT OVER COUNT";
    static constexpr const char* COUNT_MINUS_COUNT_DISTINCT =
        "COUNT MINUS COUNT DISTINCT";
    static constexpr const char* FIRST = "FIRST";
    static constexpr const char* LAST = "LAST";
    static constexpr const char* KURTOSIS = "KURTOSIS";
    static constexpr const char* MAX = "MAX";
    static constexpr const char* MEDIAN = "MEDIAN";
    static constexpr const char* MIN = "MIN";
    static constexpr const char* MODE = "MODE";
    static constexpr const char* NUM_MAX = "NUM MAX";
    static constexpr const char* NUM_MIN = "NUM MIN";
    static constexpr const char* Q1 = "Q1";
    static constexpr const char* Q5 = "Q5";
    static constexpr const char* Q10 = "Q10";
    static constexpr const char* Q25 = "Q25";
    static constexpr const char* Q75 = "Q75";
    static constexpr const char* Q90 = "Q90";
    static constexpr const char* Q95 = "Q95";
    static constexpr const char* Q99 = "Q99";
    static constexpr const char* SKEW = "SKEW";
    static constexpr const char* SUM = "SUM";
    static constexpr const char* STDDEV = "STDDEV";
    static constexpr const char* TIME_SINCE_FIRST_MAXIMUM =
        "TIME SINCE FIRST MAXIMUM";
    static constexpr const char* TIME_SINCE_FIRST_MINIMUM =
        "TIME SINCE FIRST MINIMUM";
    static constexpr const char* TIME_SINCE_LAST_MAXIMUM =
        "TIME SINCE LAST MAXIMUM";
    static constexpr const char* TIME_SINCE_LAST_MINIMUM =
        "TIME SINCE LAST MINIMUM";
    static constexpr const char* VAR = "VAR";
    static constexpr const char* VARIATION_COEFFICIENT =
        "VARIATION COEFFICIENT";

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

        if ( _str == COUNT_ABOVE_MEAN )
            {
                return Aggregation::count_above_mean;
            }

        if ( _str == COUNT_BELOW_MEAN )
            {
                return Aggregation::count_below_mean;
            }

        if ( _str == COUNT_DISTINCT )
            {
                return Aggregation::count_distinct;
            }

        if ( _str == COUNT_MINUS_COUNT_DISTINCT )
            {
                return Aggregation::count_minus_count_distinct;
            }

        if ( _str == COUNT_DISTINCT_OVER_COUNT )
            {
                return Aggregation::count_distinct_over_count;
            }

        if ( _str == FIRST )
            {
                return Aggregation::first;
            }

        if ( _str == KURTOSIS )
            {
                return Aggregation::kurtosis;
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

        if ( _str == MODE )
            {
                return Aggregation::mode;
            }

        if ( _str == NUM_MAX )
            {
                return Aggregation::num_max;
            }

        if ( _str == NUM_MIN )
            {
                return Aggregation::num_min;
            }

        if ( _str == Q1 )
            {
                return Aggregation::q1;
            }

        if ( _str == Q5 )
            {
                return Aggregation::q5;
            }

        if ( _str == Q10 )
            {
                return Aggregation::q10;
            }

        if ( _str == Q25 )
            {
                return Aggregation::q25;
            }

        if ( _str == Q75 )
            {
                return Aggregation::q75;
            }

        if ( _str == Q90 )
            {
                return Aggregation::q90;
            }

        if ( _str == Q95 )
            {
                return Aggregation::q95;
            }

        if ( _str == Q99 )
            {
                return Aggregation::q99;
            }

        if ( _str == SKEW )
            {
                return Aggregation::skew;
            }

        if ( _str == STDDEV )
            {
                return Aggregation::stddev;
            }

        if ( _str == SUM )
            {
                return Aggregation::sum;
            }

        if ( _str == TIME_SINCE_FIRST_MAXIMUM )
            {
                return Aggregation::time_since_first_maximum;
            }

        if ( _str == TIME_SINCE_FIRST_MINIMUM )
            {
                return Aggregation::time_since_first_minimum;
            }

        if ( _str == TIME_SINCE_LAST_MAXIMUM )
            {
                return Aggregation::time_since_last_maximum;
            }

        if ( _str == TIME_SINCE_LAST_MINIMUM )
            {
                return Aggregation::time_since_last_minimum;
            }

        if ( _str == VAR )
            {
                return Aggregation::var;
            }

        if ( _str == VARIATION_COEFFICIENT )
            {
                return Aggregation::variation_coefficient;
            }

        throw_unless( false, "FastProp: Unknown aggregation: '" + _str + "'" );

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

                case Aggregation::count_above_mean:
                    return COUNT_ABOVE_MEAN;

                case Aggregation::count_below_mean:
                    return COUNT_BELOW_MEAN;

                case Aggregation::count_distinct:
                    return COUNT_DISTINCT;

                case Aggregation::count_minus_count_distinct:
                    return COUNT_MINUS_COUNT_DISTINCT;

                case Aggregation::count_distinct_over_count:
                    return COUNT_DISTINCT_OVER_COUNT;

                case Aggregation::first:
                    return FIRST;

                case Aggregation::kurtosis:
                    return KURTOSIS;

                case Aggregation::last:
                    return LAST;

                case Aggregation::max:
                    return MAX;

                case Aggregation::median:
                    return MEDIAN;

                case Aggregation::min:
                    return MIN;

                case Aggregation::mode:
                    return MODE;

                case Aggregation::num_max:
                    return NUM_MAX;

                case Aggregation::num_min:
                    return NUM_MIN;

                case Aggregation::skew:
                    return SKEW;

                case Aggregation::stddev:
                    return STDDEV;

                case Aggregation::sum:
                    return SUM;

                case Aggregation::time_since_first_maximum:
                    return TIME_SINCE_FIRST_MAXIMUM;

                case Aggregation::time_since_first_minimum:
                    return TIME_SINCE_FIRST_MINIMUM;

                case Aggregation::time_since_last_maximum:
                    return TIME_SINCE_LAST_MAXIMUM;

                case Aggregation::time_since_last_minimum:
                    return TIME_SINCE_LAST_MINIMUM;

                case Aggregation::var:
                    return VAR;

                case Aggregation::variation_coefficient:
                    return VARIATION_COEFFICIENT;

                default:
                    throw_unless( false, "FastProp: Unknown aggregation." );
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
    static constexpr const char* TEXT = "text";

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

        if ( _str == TEXT )
            {
                return DataUsed::text;
            }

        throw_unless( false, "FastProp: Unknown data used: '" + _str + "'" );

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

                case DataUsed::text:
                    return TEXT;

                default:
                    throw_unless( false, "FastProp: Unknown data used." );
                    return "";
            }
    }
};

// ----------------------------------------------------------------------------

}  // namespace enums
}  // namespace fastprop

// ----------------------------------------------------------------------------

#endif  // FASTPROP_ENUMS_PARSER_HPP_
