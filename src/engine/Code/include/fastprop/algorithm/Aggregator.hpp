#ifndef FASTPROP_ALGORITHM_AGGREGATOR_HPP_
#define FASTPROP_ALGORITHM_AGGREGATOR_HPP_

// ----------------------------------------------------------------------------

namespace fastprop
{
namespace algorithm
{
// ------------------------------------------------------------------------

class Aggregator
{
   public:
    typedef std::vector<std::shared_ptr<const textmining::WordIndex>>
        WordIndices;

   public:
    /// Applies the aggregation defined in _abstract feature to each of the
    /// matches.
    static Float apply_aggregation(
        const containers::DataFrame &_population,
        const containers::DataFrame &_peripheral,
        const std::optional<containers::Features> &_subfeatures,
        const std::vector<containers::Match> &_matches,
        const std::function<bool( const containers::Match & )>
            &_condition_function,
        const containers::AbstractFeature &_abstract_feature );

   public:
    /// Whether the aggregation is an aggregation that relies on the
    /// first-last-logic.
    static bool is_first_last( const enums::Aggregation _agg )
    {
        return containers::SQLMaker::is_first_last( _agg );
    }

   private:
    /// Applies an aggregation to a categorical column.
    static Float apply_categorical(
        const containers::DataFrame &_population,
        const containers::DataFrame &_peripheral,
        const std::vector<containers::Match> &_matches,
        const std::function<bool( const containers::Match & )>
            &_condition_function,
        const containers::AbstractFeature &_abstract_feature );

    /// Determines whether a condition is true w.r.t. a match.
    static bool apply_condition(
        const containers::Condition &_condition,
        const containers::Match &_match );

    /// Applies the aggregation to a discrete column.
    static Float apply_discrete(
        const containers::DataFrame &_population,
        const containers::DataFrame &_peripheral,
        const std::vector<containers::Match> &_matches,
        const std::function<bool( const containers::Match & )>
            &_condition_function,
        const containers::AbstractFeature &_abstract_feature );

    /// Applies a COUNT aggregation
    static Float apply_not_applicable(
        const containers::DataFrame &_peripheral,
        const std::vector<containers::Match> &_matches,
        const std::function<bool( const containers::Match & )>
            &_condition_function,
        const containers::AbstractFeature &_abstract_feature );

    /// Applies the aggregation to a numerical column.
    static Float apply_numerical(
        const containers::DataFrame &_population,
        const containers::DataFrame &_peripheral,
        const std::vector<containers::Match> &_matches,
        const std::function<bool( const containers::Match & )>
            &_condition_function,
        const containers::AbstractFeature &_abstract_feature );

    /// Applies the aggregation to categorical columns with the same unit.
    static Float apply_same_units_categorical(
        const containers::DataFrame &_population,
        const containers::DataFrame &_peripheral,
        const std::vector<containers::Match> &_matches,
        const std::function<bool( const containers::Match & )>
            &_condition_function,
        const containers::AbstractFeature &_abstract_feature );

    /// Applies the aggregation to discrete columns with the same unit.
    static Float apply_same_units_discrete(
        const containers::DataFrame &_population,
        const containers::DataFrame &_peripheral,
        const std::vector<containers::Match> &_matches,
        const std::function<bool( const containers::Match & )>
            &_condition_function,
        const containers::AbstractFeature &_abstract_feature );

    /// Applies the aggregation to numerical columns with the same unit.
    static Float apply_same_units_numerical(
        const containers::DataFrame &_population,
        const containers::DataFrame &_peripheral,
        const std::vector<containers::Match> &_matches,
        const std::function<bool( const containers::Match & )>
            &_condition_function,
        const containers::AbstractFeature &_abstract_feature );

    /// Applies the aggregation to a subfeature.
    static Float apply_subfeatures(
        const containers::DataFrame &_population,
        const containers::DataFrame &_peripheral,
        const containers::Features &_subfeatures,
        const std::vector<containers::Match> &_matches,
        const std::function<bool( const containers::Match & )>
            &_condition_function,
        const containers::AbstractFeature &_abstract_feature );

    /// Applies the aggregation to text fields.
    static Float apply_text(
        const containers::DataFrame &_population,
        const containers::DataFrame &_peripheral,
        const std::vector<containers::Match> &_matches,
        const std::function<bool( const containers::Match & )>
            &_condition_function,
        const containers::AbstractFeature &_abstract_feature );

   private:
    /// Aggregates the range from _begin to _end, applying the _aggregation.
    template <class IteratorType>
    static Float aggregate_categorical_range(
        const IteratorType _begin,
        const IteratorType _end,
        const enums::Aggregation _aggregation )
    {
        switch ( _aggregation )
            {
                case enums::Aggregation::count_distinct:
                    return helpers::Aggregations::count_distinct(
                        _begin, _end );

                case enums::Aggregation::count_minus_count_distinct:
                    return helpers::Aggregations::count( _begin, _end ) -
                           helpers::Aggregations::count_distinct(
                               _begin, _end );

                default:
                    assert_true(
                        false && "Unknown aggregation for categorical column" );
                    return 0.0;
            }
    }

    /// Aggregates the range from _begin to _end, applying the _aggregation.
    template <class IteratorType>
    static Float aggregate_first_last(
        const IteratorType _begin,
        const IteratorType _end,
        const enums::Aggregation _aggregation )
    {
        if ( std::distance( _begin, _end ) <= 0 )
            {
                return 0.0;
            }

        constexpr Float t1s = 1.0;
        constexpr Float t1m = 60.0 * t1s;
        constexpr Float t1h = 60.0 * t1m;
        constexpr Float t1d = 24.0 * t1h;
        constexpr Float t7d = 7.0 * t1d;
        constexpr Float t30d = 30.0 * t1d;
        constexpr Float t90d = 90.0 * t1d;
        constexpr Float t365d = 365.0 * t1d;

        switch ( _aggregation )
            {
                case enums::Aggregation::first:
                    return helpers::Aggregations::first( _begin, _end );

                case enums::Aggregation::last:
                    return helpers::Aggregations::last( _begin, _end );

                case enums::Aggregation::ewma1s:
                    return helpers::Aggregations::ewma( t1s, _begin, _end );

                case enums::Aggregation::ewma1m:
                    return helpers::Aggregations::ewma( t1m, _begin, _end );

                case enums::Aggregation::ewma1h:
                    return helpers::Aggregations::ewma( t1h, _begin, _end );

                case enums::Aggregation::ewma1d:
                    return helpers::Aggregations::ewma( t1d, _begin, _end );

                case enums::Aggregation::ewma7d:
                    return helpers::Aggregations::ewma( t7d, _begin, _end );

                case enums::Aggregation::ewma30d:
                    return helpers::Aggregations::ewma( t30d, _begin, _end );

                case enums::Aggregation::ewma90d:
                    return helpers::Aggregations::ewma( t90d, _begin, _end );

                case enums::Aggregation::ewma365d:
                    return helpers::Aggregations::ewma( t365d, _begin, _end );

                case enums::Aggregation::time_since_first_maximum:
                    return helpers::Aggregations::time_since_first_maximum(
                        _begin, _end );

                case enums::Aggregation::time_since_first_minimum:
                    return helpers::Aggregations::time_since_first_minimum(
                        _begin, _end );

                case enums::Aggregation::time_since_last_maximum:
                    return helpers::Aggregations::time_since_last_maximum(
                        _begin, _end );

                case enums::Aggregation::time_since_last_minimum:
                    return helpers::Aggregations::time_since_last_minimum(
                        _begin, _end );

                case enums::Aggregation::trend:
                    return helpers::Aggregations::trend( _begin, _end );

                default:
                    assert_true(
                        false && "Unknown aggregation for first/last column" );
                    return 0.0;
            }
    }

    /// Aggregates the matches using the extract_value lambda function.
    template <class ExtractValueType>
    static Float aggregate_matches_categorical(
        const std::vector<containers::Match> &_matches,
        const ExtractValueType &_extract_value,
        const std::function<bool( const containers::Match & )>
            &_condition_function,
        const containers::AbstractFeature &_abstract_feature )
    {
        // ---------------------------------------------------

        const auto is_non_null = []( Int val ) { return val >= 0; };

        // ---------------------------------------------------

        if ( _abstract_feature.conditions_.size() == 0 )
            {
                auto range = _matches | VIEWS::transform( _extract_value ) |
                             VIEWS::filter( is_non_null );

                return aggregate_categorical_range(
                    range.begin(),
                    range.end(),
                    _abstract_feature.aggregation_ );
            }

        // ---------------------------------------------------

        auto range = _matches | VIEWS::filter( _condition_function ) |
                     VIEWS::transform( _extract_value ) |
                     VIEWS::filter( is_non_null );

        return aggregate_categorical_range(
            range.begin(), range.end(), _abstract_feature.aggregation_ );

        // ---------------------------------------------------
    }

    /// Aggregates the matches using the extract_value lambda function.
    template <class ExtractValueType>
    static Float aggregate_matches_first_last(
        const std::vector<containers::Match> &_matches,
        const ExtractValueType &_extract_value,
        const std::function<bool( const containers::Match & )>
            &_condition_function,
        const containers::AbstractFeature &_abstract_feature )
    {
        // ---------------------------------------------------

        assert_true( is_first_last( _abstract_feature.aggregation_ ) );

        // ---------------------------------------------------

        if ( _abstract_feature.conditions_.size() == 0 )
            {
                auto range = _matches | VIEWS::transform( _extract_value ) |
                             VIEWS::filter( second_is_not_nan_or_inf );

                return aggregate_first_last(
                    range.begin(),
                    range.end(),
                    _abstract_feature.aggregation_ );
            }

        // ---------------------------------------------------

        auto range = _matches | VIEWS::filter( _condition_function ) |
                     VIEWS::transform( _extract_value ) |
                     VIEWS::filter( second_is_not_nan_or_inf );

        return aggregate_first_last(
            range.begin(), range.end(), _abstract_feature.aggregation_ );

        // ---------------------------------------------------
    }

    /// Aggregates the matches using the extract_value lambda function.
    template <class ExtractValueType>
    static Float aggregate_matches_numerical(
        const std::vector<containers::Match> &_matches,
        const ExtractValueType &_extract_value,
        const std::function<bool( const containers::Match & )>
            &_condition_function,
        const containers::AbstractFeature &_abstract_feature )
    {
        // ---------------------------------------------------

        if ( _abstract_feature.conditions_.size() == 0 )
            {
                auto range = _matches | VIEWS::transform( _extract_value ) |
                             VIEWS::filter( is_not_nan_or_inf );

                return aggregate_numerical_range(
                    range.begin(),
                    range.end(),
                    _abstract_feature.aggregation_ );
            }

        // ---------------------------------------------------

        auto range = _matches | VIEWS::filter( _condition_function ) |
                     VIEWS::transform( _extract_value ) |
                     VIEWS::filter( is_not_nan_or_inf );

        return aggregate_numerical_range(
            range.begin(), range.end(), _abstract_feature.aggregation_ );

        // ---------------------------------------------------
    }

    /// Aggregates the range from _begin to _end, applying the _aggregation.
    template <class IteratorType>
    static Float aggregate_numerical_range(
        const IteratorType _begin,
        const IteratorType _end,
        const enums::Aggregation _aggregation )
    {
        switch ( _aggregation )
            {
                case enums::Aggregation::avg:
                    return helpers::Aggregations::avg( _begin, _end );

                case enums::Aggregation::avg_time_between:
                    return calc_avg_time_between( _begin, _end );

                case enums::Aggregation::count:
                    return helpers::Aggregations::count( _begin, _end );

                case enums::Aggregation::count_above_mean:
                    return helpers::Aggregations::count_above_mean(
                        _begin, _end );

                case enums::Aggregation::count_below_mean:
                    return helpers::Aggregations::count_below_mean(
                        _begin, _end );

                case enums::Aggregation::count_distinct:
                    return helpers::Aggregations::count_distinct(
                        _begin, _end );

                case enums::Aggregation::count_distinct_over_count:
                    return helpers::Aggregations::count_distinct_over_count(
                        _begin, _end );

                case enums::Aggregation::count_minus_count_distinct:
                    return helpers::Aggregations::count( _begin, _end ) -
                           helpers::Aggregations::count_distinct(
                               _begin, _end );

                case enums::Aggregation::kurtosis:
                    return helpers::Aggregations::kurtosis( _begin, _end );

                case enums::Aggregation::max:
                    return helpers::Aggregations::maximum( _begin, _end );

                case enums::Aggregation::median:
                    return helpers::Aggregations::median( _begin, _end );

                case enums::Aggregation::min:
                    return helpers::Aggregations::minimum( _begin, _end );

                case enums::Aggregation::mode:
                    return helpers::Aggregations::mode<Float>( _begin, _end );

                case enums::Aggregation::num_max:
                    return helpers::Aggregations::num_max( _begin, _end );

                case enums::Aggregation::num_min:
                    return helpers::Aggregations::num_min( _begin, _end );

                case enums::Aggregation::q1:
                    return helpers::Aggregations::quantile(
                        0.01, _begin, _end );

                case enums::Aggregation::q5:
                    return helpers::Aggregations::quantile(
                        0.05, _begin, _end );

                case enums::Aggregation::q10:
                    return helpers::Aggregations::quantile( 0.1, _begin, _end );

                case enums::Aggregation::q25:
                    return helpers::Aggregations::quantile(
                        0.25, _begin, _end );

                case enums::Aggregation::q75:
                    return helpers::Aggregations::quantile(
                        0.75, _begin, _end );

                case enums::Aggregation::q90:
                    return helpers::Aggregations::quantile(
                        0.90, _begin, _end );

                case enums::Aggregation::q95:
                    return helpers::Aggregations::quantile(
                        0.95, _begin, _end );

                case enums::Aggregation::q99:
                    return helpers::Aggregations::quantile(
                        0.99, _begin, _end );

                case enums::Aggregation::skew:
                    return helpers::Aggregations::skew( _begin, _end );

                case enums::Aggregation::stddev:
                    return helpers::Aggregations::stddev( _begin, _end );

                case enums::Aggregation::sum:
                    return helpers::Aggregations::sum( _begin, _end );

                case enums::Aggregation::var:
                    return helpers::Aggregations::var( _begin, _end );

                case enums::Aggregation::variation_coefficient:
                    return helpers::Aggregations::variation_coefficient(
                        _begin, _end );

                default:
                    assert_true(
                        false && "Unknown aggregation for numerical column" );
                    return 0.0;
            }
    }

    /// Creates a key-value-pair for applying the FIRST and LAST aggregation.
    template <class ExtractValueType>
    static Float apply_first_last(
        const containers::DataFrame &_population,
        const containers::DataFrame &_peripheral,
        const std::vector<containers::Match> &_matches,
        const ExtractValueType &_extract_value,
        const std::function<bool( const containers::Match & )>
            &_condition_function,
        const containers::AbstractFeature &_abstract_feature )
    {
        assert_true( is_first_last( _abstract_feature.aggregation_ ) );

        assert_true( _peripheral.num_time_stamps() > 0 );

        using Pair = std::pair<Float, Float>;

        if ( _abstract_feature.aggregation_ == enums::Aggregation::first ||
             _abstract_feature.aggregation_ == enums::Aggregation::last )
            {
                const auto &ts_col = _peripheral.time_stamp_col();

                const auto extract_pair =
                    [_extract_value,
                     &ts_col]( const containers::Match &m ) -> Pair {
                    const auto key = ts_col[m.ix_input];
                    const auto value = _extract_value( m );
                    return std::make_pair( key, value );
                };

                return aggregate_matches_first_last(
                    _matches,
                    extract_pair,
                    _condition_function,
                    _abstract_feature );
            }

        assert_true( _population.num_time_stamps() > 0 );

        const auto &ts_col1 = _population.time_stamp_col();

        const auto &ts_col2 = _peripheral.time_stamp_col();

        const auto extract_pair = [_extract_value, &ts_col1, &ts_col2](
                                      const containers::Match &m ) -> Pair {
            const auto key = ts_col1[m.ix_output] - ts_col2[m.ix_input];
            const auto value = _extract_value( m );
            return std::make_pair( key, value );
        };

        return aggregate_matches_first_last(
            _matches, extract_pair, _condition_function, _abstract_feature );
    }

    /// Calculates the average time between the time stamps.
    template <class IteratorType>
    static Float calc_avg_time_between(
        const IteratorType _begin, const IteratorType _end )
    {
        const auto count = helpers::Aggregations::count( _begin, _end );

        if ( count <= 1.0 )
            {
                return 0.0;
            }

        const auto max_value =
            aggregate_numerical_range( _begin, _end, enums::Aggregation::max );

        const auto min_value =
            aggregate_numerical_range( _begin, _end, enums::Aggregation::min );

        return ( max_value - min_value ) / ( count - 1.0 );
    }

    /// Determines whether a value is nan or inf
    static bool is_not_nan_or_inf( const Float _val )
    {
        return !std::isnan( _val ) && !std::isinf( _val );
    }

    /// Determines whether a value is nan or inf
    static bool second_is_not_nan_or_inf( const std::pair<Float, Float> &_p )
    {
        return !std::isnan( _p.second ) && !std::isinf( _p.second );
    }
};

// ------------------------------------------------------------------------
}  // namespace algorithm
}  // namespace fastprop

#endif  // FASTPROP_ALGORITHM_AGGREGATOR_HPP_
