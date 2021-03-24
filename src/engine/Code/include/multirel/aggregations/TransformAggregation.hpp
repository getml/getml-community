#ifndef MULTIREL_AGGREGATIONS_TRANSFORMAGGREGATION_HPP_
#define MULTIREL_AGGREGATIONS_TRANSFORMAGGREGATION_HPP_

namespace multirel
{
namespace aggregations
{
// ----------------------------------------------------------------------------

/// TransformAggregation is used to implement the aggregations in the trees'
/// transform(...) method.
template <typename AggType, enums::DataUsed data_used_, bool is_population_>
class TransformAggregation : public AbstractTransformAggregation
{
   public:
    TransformAggregation( const TransformAggregationParams& _params )
        : AbstractTransformAggregation(),
          value_container_(
              ValueContainerCreator<data_used_, is_population_>::create(
                  _params.same_units_discrete,
                  _params.same_units_numerical,
                  _params.column_to_be_aggregated,
                  _params.population,
                  _params.peripheral,
                  _params.subfeatures ) ){};

    ~TransformAggregation() = default;

   public:
    /// Returns the aggregated values of the match pointers.
    Float aggregate(
        const containers::MatchPtrs& _match_ptrs,
        const size_t _skip,
        const std::optional<containers::Column<Float>>& _time_stamp )
        const final;

   public:
    /// Moves the match pointers aggregating NULL values to the beginning.
    containers::MatchPtrs::iterator separate_null_values(
        containers::MatchPtrs* _match_ptrs ) const final;

   private:
    /// Builds the range that we need to aggregate.
    auto make_range(
        const containers::MatchPtrs& _match_ptrs,
        const size_t _skip,
        const std::optional<containers::Column<Float>>& _time_stamp ) const;

   private:
    /// Contains the values to be aggregated.
    const ValueContainer<data_used_, is_population_> value_container_;
};

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

template <typename AggType, enums::DataUsed data_used_, bool is_population_>
Float TransformAggregation<AggType, data_used_, is_population_>::aggregate(
    const containers::MatchPtrs& _match_ptrs,
    const size_t _skip,
    const std::optional<containers::Column<Float>>& _time_stamp ) const
{
    auto range = make_range( _match_ptrs, _skip, _time_stamp );

    if constexpr ( std::is_same<AggType, AggregationType::Avg>() )
        {
            return helpers::ColumnOperators::avg( range.begin(), range.end() );
        }

    if constexpr ( std::is_same<AggType, AggregationType::Count>() )
        {
            return helpers::ColumnOperators::count(
                range.begin(), range.end() );
        }

    if constexpr ( std::is_same<AggType, AggregationType::CountDistinct>() )
        {
            return helpers::ColumnOperators::count_distinct(
                range.begin(), range.end() );
        }

    if constexpr ( std::is_same<
                       AggType,
                       AggregationType::CountMinusCountDistinct>() )
        {
            return helpers::ColumnOperators::count(
                       range.begin(), range.end() ) -
                   helpers::ColumnOperators::count_distinct(
                       range.begin(), range.end() );
        }

    if constexpr ( std::is_same<AggType, AggregationType::First>() )
        {
            return helpers::ColumnOperators::first(
                range.begin(), range.end() );
        }

    if constexpr ( std::is_same<AggType, AggregationType::Last>() )
        {
            return helpers::ColumnOperators::last( range.begin(), range.end() );
        }

    if constexpr ( std::is_same<AggType, AggregationType::Max>() )
        {
            return helpers::ColumnOperators::maximum(
                range.begin(), range.end() );
        }

    if constexpr ( std::is_same<AggType, AggregationType::Median>() )
        {
            return helpers::ColumnOperators::median(
                range.begin(), range.end() );
        }

    if constexpr ( std::is_same<AggType, AggregationType::Min>() )
        {
            return helpers::ColumnOperators::minimum(
                range.begin(), range.end() );
        }

    if constexpr ( std::is_same<AggType, AggregationType::Skewness>() )
        {
            return helpers::ColumnOperators::skew( range.begin(), range.end() );
        }

    if constexpr ( std::is_same<AggType, AggregationType::Stddev>() )
        {
            return helpers::ColumnOperators::stddev(
                range.begin(), range.end() );
        }

    if constexpr ( std::is_same<AggType, AggregationType::Sum>() )
        {
            return helpers::ColumnOperators::sum( range.begin(), range.end() );
        }

    if constexpr ( std::is_same<AggType, AggregationType::Var>() )
        {
            return helpers::ColumnOperators::var( range.begin(), range.end() );
        }

    assert_msg( false, "Unknown aggregation: " + AggType::type() );
}

// ----------------------------------------------------------------------------

template <typename AggType, enums::DataUsed data_used_, bool is_population_>
auto TransformAggregation<AggType, data_used_, is_population_>::make_range(
    const containers::MatchPtrs& _match_ptrs,
    const size_t _skip,
    const std::optional<containers::Column<Float>>& _time_stamp ) const
{
    constexpr bool is_count = std::is_same<AggType, AggregationType::Count>();

    constexpr bool is_first_or_last =
        std::is_same<AggType, AggregationType::First>() ||
        std::is_same<AggType, AggregationType::Last>();

    if constexpr ( is_count )
        {
            const auto get_value =
                []( const containers::Match* _match ) -> Float { return 0.0; };

            return _match_ptrs | std::views::drop( _skip ) |
                   std::views::transform( get_value );
        }

    if constexpr ( is_first_or_last )
        {
            assert_true( _time_stamp );

            const auto get_pair =
                [this, _time_stamp]( const containers::Match* _match )
                -> std::pair<Float, Float> {
                const auto first = _time_stamp.value()[_match->ix_x_perip];
                const auto second =
                    value_container_.value_to_be_aggregated( _match );
                return std::pair( first, second );
            };

            return _match_ptrs | std::views::drop( _skip ) |
                   std::views::transform( get_pair );
        }

    if constexpr ( !is_first_or_last && !is_count )
        {
            const auto get_value =
                [this]( const containers::Match* _match ) -> Float {
                return value_container_.value_to_be_aggregated( _match );
            };

            return _match_ptrs | std::views::drop( _skip ) |
                   std::views::transform( get_value );
        }
}

// ----------------------------------------------------------------------------

template <typename AggType, enums::DataUsed data_used_, bool is_population_>
containers::MatchPtrs::iterator
TransformAggregation<AggType, data_used_, is_population_>::separate_null_values(
    containers::MatchPtrs* _match_ptrs ) const
{
    constexpr bool is_count = std::is_same<AggType, AggregationType::Count>();

    if constexpr ( !is_count )
        {
            const auto is_null = [this]( const containers::Match* _m ) -> bool {
                const auto val = value_container_.value_to_be_aggregated( _m );
                return ( std::isnan( val ) || std::isinf( val ) );
            };

            return std::partition(
                _match_ptrs->begin(), _match_ptrs->end(), is_null );
        }

    if constexpr ( is_count )
        {
            return _match_ptrs->begin();
        }
}

// ----------------------------------------------------------------------------
}  // namespace aggregations
}  // namespace multirel

#endif  // MULTIREL_AGGREGATIONS_AGGREGATION_HPP_
