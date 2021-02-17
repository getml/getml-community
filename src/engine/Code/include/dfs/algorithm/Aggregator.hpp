#ifndef DFS_ALGORITHM_AGGREGATOR_HPP_
#define DFS_ALGORITHM_AGGREGATOR_HPP_

// ----------------------------------------------------------------------------

namespace dfs
{
namespace algorithm
{
// ------------------------------------------------------------------------

class Aggregator
{
   public:
    /// Applies the aggregation defined in _abstract feature to each of the
    /// matches.
    static Float apply_aggregation(
        const containers::DataFrame &_population,
        const containers::DataFrame &_peripheral,
        const containers::Features &_subfeatures,
        const std::vector<containers::Match> &_matches,
        const std::function<bool( const containers::Match & )>
            &_condition_function,
        const containers::AbstractFeature &_abstract_feature );

   private:
    /// Applies an aggregation to a categorical column.
    static Float apply_categorical(
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
        const containers::DataFrame &_peripheral,
        const containers::Features &_subfeatures,
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
        if ( std::distance( _begin, _end ) <= 0.0 )
            {
                return 0.0;
            }

        switch ( _aggregation )
            {
                case enums::Aggregation::count_distinct:
                    return helpers::ColumnOperators::count_distinct(
                        _begin, _end );

                case enums::Aggregation::count_minus_count_distinct:
                    return helpers::ColumnOperators::count( _begin, _end ) -
                           helpers::ColumnOperators::count_distinct(
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

        switch ( _aggregation )
            {
                case enums::Aggregation::first:
                    return helpers::ColumnOperators::first( _begin, _end );

                case enums::Aggregation::last:
                    return helpers::ColumnOperators::last( _begin, _end );

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
                auto range = _matches |
                             std::views::transform( _extract_value ) |
                             std::views::filter( is_non_null );

                return aggregate_categorical_range(
                    range.begin(),
                    range.end(),
                    _abstract_feature.aggregation_ );
            }

        // ---------------------------------------------------

        auto range = _matches | std::views::filter( _condition_function ) |
                     std::views::transform( _extract_value ) |
                     std::views::filter( is_non_null );

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

        assert_true(
            _abstract_feature.aggregation_ == enums::Aggregation::first ||
            _abstract_feature.aggregation_ == enums::Aggregation::last );

        // ---------------------------------------------------

        if ( _abstract_feature.conditions_.size() == 0 )
            {
                auto range = _matches |
                             std::views::transform( _extract_value ) |
                             std::views::filter( second_is_not_nan_or_inf );

                return aggregate_first_last(
                    range.begin(),
                    range.end(),
                    _abstract_feature.aggregation_ );
            }

        // ---------------------------------------------------

        auto range = _matches | std::views::filter( _condition_function ) |
                     std::views::transform( _extract_value ) |
                     std::views::filter( second_is_not_nan_or_inf );

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
                auto range = _matches |
                             std::views::transform( _extract_value ) |
                             std::views::filter( is_not_nan_or_inf );

                return aggregate_numerical_range(
                    range.begin(),
                    range.end(),
                    _abstract_feature.aggregation_ );
            }

        // ---------------------------------------------------

        auto range = _matches | std::views::filter( _condition_function ) |
                     std::views::transform( _extract_value ) |
                     std::views::filter( is_not_nan_or_inf );

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
        if ( std::distance( _begin, _end ) <= 0 )
            {
                return 0.0;
            }

        switch ( _aggregation )
            {
                case enums::Aggregation::avg:
                    return helpers::ColumnOperators::avg( _begin, _end );

                case enums::Aggregation::avg_time_between:
                    return calc_avg_time_between( _begin, _end );

                case enums::Aggregation::count:
                    return helpers::ColumnOperators::count( _begin, _end );

                case enums::Aggregation::count_distinct:
                    return helpers::ColumnOperators::count_distinct(
                        _begin, _end );

                case enums::Aggregation::count_minus_count_distinct:
                    return helpers::ColumnOperators::count( _begin, _end ) -
                           helpers::ColumnOperators::count_distinct(
                               _begin, _end );

                case enums::Aggregation::max:
                    return helpers::ColumnOperators::maximum( _begin, _end );

                case enums::Aggregation::median:
                    return helpers::ColumnOperators::median( _begin, _end );

                case enums::Aggregation::min:
                    return helpers::ColumnOperators::minimum( _begin, _end );

                case enums::Aggregation::skew:
                    return helpers::ColumnOperators::skew( _begin, _end );

                case enums::Aggregation::stddev:
                    return helpers::ColumnOperators::stddev( _begin, _end );

                case enums::Aggregation::sum:
                    return helpers::ColumnOperators::sum( _begin, _end );

                case enums::Aggregation::var:
                    return helpers::ColumnOperators::var( _begin, _end );

                default:
                    assert_true(
                        false && "Unknown aggregation for numerical column" );
                    return 0.0;
            }
    }

    /// Creates a key-value-pair for applying the FIRST and LAST aggregation.
    template <class ExtractValueType>
    static Float apply_first_last(
        const containers::DataFrame &_peripheral,
        const std::vector<containers::Match> &_matches,
        const ExtractValueType &_extract_value,
        const std::function<bool( const containers::Match & )>
            &_condition_function,
        const containers::AbstractFeature &_abstract_feature )
    {
        assert_true(
            _abstract_feature.aggregation_ == enums::Aggregation::first ||
            _abstract_feature.aggregation_ == enums::Aggregation::last );

        assert_true( _peripheral.num_time_stamps() > 0 );

        using Pair = std::pair<Float, Float>;

        const auto &ts_col = _peripheral.time_stamp_col();

        const auto extract_pair =
            [_extract_value, &ts_col]( const containers::Match &m ) -> Pair {
            const auto key = ts_col[m.ix_input];
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
        const auto count = helpers::ColumnOperators::count( _begin, _end );

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
}  // namespace dfs

#endif  // DFS_ALGORITHM_AGGREGATOR_HPP_
