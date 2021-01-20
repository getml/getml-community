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
        const containers::AbstractFeature &_abstract_feature );

   private:
    /// Applies an aggregation to a categorical column.
    static Float apply_categorical(
        const containers::DataFrame &_peripheral,
        const std::vector<containers::Match> &_matches,
        const containers::AbstractFeature &_abstract_feature );

    /// Applies the aggregation to a discrete column.
    static Float apply_discrete(
        const containers::DataFrame &_peripheral,
        const std::vector<containers::Match> &_matches,
        const containers::AbstractFeature &_abstract_feature );

    /// Applies a COUNT aggregation
    static Float apply_not_applicable(
        const std::vector<containers::Match> &_matches,
        const containers::AbstractFeature &_abstract_feature );

    /// Applies the aggregation to a numerical column.
    static Float apply_numerical(
        const containers::DataFrame &_peripheral,
        const std::vector<containers::Match> &_matches,
        const containers::AbstractFeature &_abstract_feature );

    /// Applies the aggregation to categorical columns with the same unit.
    static Float apply_same_units_categorical(
        const containers::DataFrame &_population,
        const containers::DataFrame &_peripheral,
        const std::vector<containers::Match> &_matches,
        const containers::AbstractFeature &_abstract_feature );

    /// Applies the aggregation to discrete columns with the same unit.
    static Float apply_same_units_discrete(
        const containers::DataFrame &_population,
        const containers::DataFrame &_peripheral,
        const std::vector<containers::Match> &_matches,
        const containers::AbstractFeature &_abstract_feature );

    /// Applies the aggregation to numerical columns with the same unit.
    static Float apply_same_units_numerical(
        const containers::DataFrame &_population,
        const containers::DataFrame &_peripheral,
        const std::vector<containers::Match> &_matches,
        const containers::AbstractFeature &_abstract_feature );

    /// Applies the aggregation to a subfeature.
    static Float apply_subfeatures(
        const containers::Features &_subfeatures,
        const std::vector<containers::Match> &_matches,
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
    static Float aggregate_numerical_range(
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
                case enums::Aggregation::avg:
                    return helpers::ColumnOperators::avg( _begin, _end );

                case enums::Aggregation::count:
                    return helpers::ColumnOperators::count( _begin, _end );

                case enums::Aggregation::max:
                    return helpers::ColumnOperators::maximum( _begin, _end );

                case enums::Aggregation::median:
                    return helpers::ColumnOperators::median( _begin, _end );

                case enums::Aggregation::min:
                    return helpers::ColumnOperators::minimum( _begin, _end );

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

    /// Determines whether a value is nan or inf
    static bool is_not_nan_or_inf( const Float _val )
    {
        return !std::isnan( _val ) && !std::isinf( _val );
    }
};

// ------------------------------------------------------------------------
}  // namespace algorithm
}  // namespace dfs

#endif  // DFS_ALGORITHM_AGGREGATOR_HPP_
