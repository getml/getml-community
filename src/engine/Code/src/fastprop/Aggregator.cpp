#include "fastprop/algorithm/algorithm.hpp"

namespace fastprop
{
namespace algorithm
{
// ----------------------------------------------------------------------------

Float Aggregator::apply_aggregation(
    const containers::DataFrame &_population,
    const containers::DataFrame &_peripheral,
    const containers::Features &_subfeatures,
    const std::vector<containers::Match> &_matches,
    const std::function<bool( const containers::Match & )> &_condition_function,
    const containers::AbstractFeature &_abstract_feature )
{
    switch ( _abstract_feature.data_used_ )
        {
            case enums::DataUsed::categorical:
                return apply_categorical(
                    _peripheral,
                    _matches,
                    _condition_function,
                    _abstract_feature );

            case enums::DataUsed::discrete:
                return apply_discrete(
                    _peripheral,
                    _matches,
                    _condition_function,
                    _abstract_feature );

            case enums::DataUsed::not_applicable:
                return apply_not_applicable(
                    _peripheral,
                    _matches,
                    _condition_function,
                    _abstract_feature );

            case enums::DataUsed::numerical:
                return apply_numerical(
                    _peripheral,
                    _matches,
                    _condition_function,
                    _abstract_feature );

            case enums::DataUsed::same_units_categorical:
                return apply_same_units_categorical(
                    _population,
                    _peripheral,
                    _matches,
                    _condition_function,
                    _abstract_feature );

            case enums::DataUsed::same_units_discrete:
            case enums::DataUsed::same_units_discrete_ts:
                return apply_same_units_discrete(
                    _population,
                    _peripheral,
                    _matches,
                    _condition_function,
                    _abstract_feature );

            case enums::DataUsed::same_units_numerical:
            case enums::DataUsed::same_units_numerical_ts:
                return apply_same_units_numerical(
                    _population,
                    _peripheral,
                    _matches,
                    _condition_function,
                    _abstract_feature );

            case enums::DataUsed::subfeatures:
                return apply_subfeatures(
                    _peripheral,
                    _subfeatures,
                    _matches,
                    _condition_function,
                    _abstract_feature );

            default:
                assert_true( false && "Unknown data_used" );
                return 0.0;
        }
}

// ----------------------------------------------------------------------------

Float Aggregator::apply_categorical(
    const containers::DataFrame &_peripheral,
    const std::vector<containers::Match> &_matches,
    const std::function<bool( const containers::Match & )> &_condition_function,
    const containers::AbstractFeature &_abstract_feature )
{
    assert_true(
        _abstract_feature.input_col_ < _peripheral.num_categoricals() );

    const auto &col =
        _peripheral.categorical_col( _abstract_feature.input_col_ );

    if ( _abstract_feature.categorical_value_ ==
         containers::AbstractFeature::NO_CATEGORICAL_VALUE )
        {
            const auto extract_value =
                [&col]( const containers::Match &match ) -> Int {
                return col[match.ix_input];
            };

            return aggregate_matches_categorical(
                _matches,
                extract_value,
                _condition_function,
                _abstract_feature );
        }

    const auto extract_value =
        [&col, &_abstract_feature]( const containers::Match &match ) -> Float {
        if ( col[match.ix_input] == _abstract_feature.categorical_value_ )
            {
                return 1.0;
            }
        return 0.0;
    };

    if ( _abstract_feature.aggregation_ == enums::Aggregation::first ||
         _abstract_feature.aggregation_ == enums::Aggregation::last )
        {
            return apply_first_last(
                _peripheral,
                _matches,
                extract_value,
                _condition_function,
                _abstract_feature );
        }

    return aggregate_matches_numerical(
        _matches, extract_value, _condition_function, _abstract_feature );
}

// ----------------------------------------------------------------------------

Float Aggregator::apply_discrete(
    const containers::DataFrame &_peripheral,
    const std::vector<containers::Match> &_matches,
    const std::function<bool( const containers::Match & )> &_condition_function,
    const containers::AbstractFeature &_abstract_feature )
{
    assert_true( _abstract_feature.input_col_ < _peripheral.num_discretes() );

    const auto &col = _peripheral.discrete_col( _abstract_feature.input_col_ );

    const auto extract_value =
        [&col]( const containers::Match &match ) -> Float {
        return col[match.ix_input];
    };

    if ( _abstract_feature.aggregation_ == enums::Aggregation::first ||
         _abstract_feature.aggregation_ == enums::Aggregation::last )
        {
            return apply_first_last(
                _peripheral,
                _matches,
                extract_value,
                _condition_function,
                _abstract_feature );
        }

    return aggregate_matches_numerical(
        _matches, extract_value, _condition_function, _abstract_feature );
}

// ----------------------------------------------------------------------------

Float Aggregator::apply_not_applicable(
    const containers::DataFrame &_peripheral,
    const std::vector<containers::Match> &_matches,
    const std::function<bool( const containers::Match & )> &_condition_function,
    const containers::AbstractFeature &_abstract_feature )
{
    assert_true(
        _abstract_feature.aggregation_ == enums::Aggregation::count ||
        _abstract_feature.aggregation_ ==
            enums::Aggregation::avg_time_between );

    if ( _abstract_feature.aggregation_ == enums::Aggregation::count )
        {
            const auto extract_value =
                []( const containers::Match &match ) -> Float { return 0.0; };

            return aggregate_matches_numerical(
                _matches,
                extract_value,
                _condition_function,
                _abstract_feature );
        }

    assert_true( _peripheral.num_time_stamps() > 0 );

    const auto &col = _peripheral.time_stamp_col();

    const auto extract_value =
        [&col]( const containers::Match &match ) -> Float {
        return col[match.ix_input];
    };

    return aggregate_matches_numerical(
        _matches, extract_value, _condition_function, _abstract_feature );
}

// ----------------------------------------------------------------------------

Float Aggregator::apply_numerical(
    const containers::DataFrame &_peripheral,
    const std::vector<containers::Match> &_matches,
    const std::function<bool( const containers::Match & )> &_condition_function,
    const containers::AbstractFeature &_abstract_feature )
{
    assert_true( _abstract_feature.input_col_ < _peripheral.num_numericals() );

    const auto &col = _peripheral.numerical_col( _abstract_feature.input_col_ );

    const auto extract_value =
        [&col]( const containers::Match &match ) -> Float {
        return col[match.ix_input];
    };

    if ( _abstract_feature.aggregation_ == enums::Aggregation::first ||
         _abstract_feature.aggregation_ == enums::Aggregation::last )
        {
            return apply_first_last(
                _peripheral,
                _matches,
                extract_value,
                _condition_function,
                _abstract_feature );
        }

    return aggregate_matches_numerical(
        _matches, extract_value, _condition_function, _abstract_feature );
}

// ----------------------------------------------------------------------------

Float Aggregator::apply_same_units_categorical(
    const containers::DataFrame &_population,
    const containers::DataFrame &_peripheral,
    const std::vector<containers::Match> &_matches,
    const std::function<bool( const containers::Match & )> &_condition_function,
    const containers::AbstractFeature &_abstract_feature )
{
    assert_true(
        _abstract_feature.input_col_ < _peripheral.num_categoricals() );

    assert_true(
        _abstract_feature.output_col_ < _population.num_categoricals() );

    const auto &col1 =
        _population.categorical_col( _abstract_feature.output_col_ );

    const auto &col2 =
        _peripheral.categorical_col( _abstract_feature.input_col_ );

    const auto extract_value =
        [&col1, &col2]( const containers::Match &match ) -> Float {
        if ( col1[match.ix_output] == col2[match.ix_input] &&
             col1[match.ix_output] >= 0 )
            {
                return 1.0;
            }
        return 0.0;
    };

    if ( _abstract_feature.aggregation_ == enums::Aggregation::first ||
         _abstract_feature.aggregation_ == enums::Aggregation::last )
        {
            return apply_first_last(
                _peripheral,
                _matches,
                extract_value,
                _condition_function,
                _abstract_feature );
        }

    return aggregate_matches_numerical(
        _matches, extract_value, _condition_function, _abstract_feature );
}

// ----------------------------------------------------------------------------

Float Aggregator::apply_same_units_discrete(
    const containers::DataFrame &_population,
    const containers::DataFrame &_peripheral,
    const std::vector<containers::Match> &_matches,
    const std::function<bool( const containers::Match & )> &_condition_function,
    const containers::AbstractFeature &_abstract_feature )
{
    assert_true( _abstract_feature.input_col_ < _peripheral.num_discretes() );

    assert_true( _abstract_feature.output_col_ < _population.num_discretes() );

    const auto &col1 =
        _population.discrete_col( _abstract_feature.output_col_ );

    const auto &col2 = _peripheral.discrete_col( _abstract_feature.input_col_ );

    const auto extract_value =
        [&col1, &col2]( const containers::Match &match ) -> Float {
        return col1[match.ix_output] - col2[match.ix_input];
    };

    if ( _abstract_feature.aggregation_ == enums::Aggregation::first ||
         _abstract_feature.aggregation_ == enums::Aggregation::last )
        {
            return apply_first_last(
                _peripheral,
                _matches,
                extract_value,
                _condition_function,
                _abstract_feature );
        }

    return aggregate_matches_numerical(
        _matches, extract_value, _condition_function, _abstract_feature );
}

// ----------------------------------------------------------------------------

Float Aggregator::apply_same_units_numerical(
    const containers::DataFrame &_population,
    const containers::DataFrame &_peripheral,
    const std::vector<containers::Match> &_matches,
    const std::function<bool( const containers::Match & )> &_condition_function,
    const containers::AbstractFeature &_abstract_feature )
{
    assert_true( _abstract_feature.input_col_ < _peripheral.num_numericals() );

    assert_true( _abstract_feature.output_col_ < _population.num_numericals() );

    const auto &col1 =
        _population.numerical_col( _abstract_feature.output_col_ );

    const auto &col2 =
        _peripheral.numerical_col( _abstract_feature.input_col_ );

    const auto extract_value =
        [&col1, &col2]( const containers::Match &match ) -> Float {
        return col1[match.ix_output] - col2[match.ix_input];
    };

    if ( _abstract_feature.aggregation_ == enums::Aggregation::first ||
         _abstract_feature.aggregation_ == enums::Aggregation::last )
        {
            return apply_first_last(
                _peripheral,
                _matches,
                extract_value,
                _condition_function,
                _abstract_feature );
        }

    return aggregate_matches_numerical(
        _matches, extract_value, _condition_function, _abstract_feature );
}

// ----------------------------------------------------------------------------

Float Aggregator::apply_subfeatures(
    const containers::DataFrame &_peripheral,
    const containers::Features &_subfeatures,
    const std::vector<containers::Match> &_matches,
    const std::function<bool( const containers::Match & )> &_condition_function,
    const containers::AbstractFeature &_abstract_feature )
{
    assert_true( _abstract_feature.input_col_ < _subfeatures.size() );

    assert_true( _subfeatures.at( _abstract_feature.input_col_ ) );

    const auto &col = *_subfeatures.at( _abstract_feature.input_col_ );

    const auto extract_value =
        [&col]( const containers::Match &match ) -> Float {
        return col[match.ix_input];
    };

    if ( _abstract_feature.aggregation_ == enums::Aggregation::first ||
         _abstract_feature.aggregation_ == enums::Aggregation::last )
        {
            return apply_first_last(
                _peripheral,
                _matches,
                extract_value,
                _condition_function,
                _abstract_feature );
        }

    return aggregate_matches_numerical(
        _matches, extract_value, _condition_function, _abstract_feature );
}

// ----------------------------------------------------------------------------
}  // namespace algorithm
}  // namespace fastprop
