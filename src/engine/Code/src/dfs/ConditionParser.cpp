#include "dfs/algorithm/algorithm.hpp"

namespace dfs
{
namespace algorithm
{
// ----------------------------------------------------------------------------

std::vector<std::function<bool( const containers::Match & )>>
ConditionParser::make_condition_functions(
    const TableHolder &_table_holder,
    const std::vector<containers::Features> &_subfeatures,
    const std::vector<size_t> &_index,
    const std::vector<containers::AbstractFeature> &_abstract_features )
{
    const auto make_function = [&_table_holder,
                                &_subfeatures,
                                &_abstract_features]( const size_t ix ) {
        assert_true( ix < _abstract_features.size() );
        return ConditionParser::make_apply_conditions(
            _table_holder, _subfeatures, _abstract_features.at( ix ) );
    };

    auto range = _index | std::views::transform( make_function );

    return std::vector<std::function<bool( const containers::Match & )>>(
        range.begin(), range.end() );
}

// ----------------------------------------------------------------------------

std::function<bool( const containers::Match & )>
ConditionParser::make_apply_conditions(
    const TableHolder &_table_holder,
    const std::vector<containers::Features> &_subfeatures,
    const containers::AbstractFeature &_abstract_feature )
{
    const auto conditions =
        parse_conditions( _table_holder, _subfeatures, _abstract_feature );

    return [conditions]( const containers::Match &match ) -> bool {
        const auto include_match =
            [&match](
                const std::function<bool( const containers::Match &match )>
                    &cond ) -> bool { return cond( match ); };

        return std::all_of(
            conditions.begin(), conditions.end(), include_match );
    };
}

// ----------------------------------------------------------------------------

std::function<bool( const containers::Match & )>
ConditionParser::make_same_units_categorical(
    const containers::DataFrame &_population,
    const containers::DataFrame &_peripheral,
    const containers::Condition &_condition )
{
    assert_true( _condition.input_col_ < _peripheral.num_categoricals() );

    assert_true( _condition.output_col_ < _population.num_categoricals() );

    const auto col1 = _population.categorical_col( _condition.output_col_ );

    const auto col2 = _peripheral.categorical_col( _condition.input_col_ );

    return [col1, col2]( const containers::Match &match ) -> bool {
        return ( col1[match.ix_output] == col2[match.ix_input] );
    };
}

// ----------------------------------------------------------------------------

std::vector<std::function<bool( const containers::Match & )>>
ConditionParser::parse_conditions(
    const TableHolder &_table_holder,
    const std::vector<containers::Features> &_subfeatures,
    const containers::AbstractFeature &_abstract_feature )
{
    assert_true(
        _table_holder.main_tables_.size() ==
        _table_holder.peripheral_tables_.size() );

    assert_true( _table_holder.main_tables_.size() == _subfeatures.size() );

    assert_true(
        _abstract_feature.peripheral_ < _table_holder.main_tables_.size() );

    const auto &population =
        _table_holder.main_tables_.at( _abstract_feature.peripheral_ ).df();

    const auto &peripheral =
        _table_holder.peripheral_tables_.at( _abstract_feature.peripheral_ );

    const auto &subfeatures = _subfeatures.at( _abstract_feature.peripheral_ );

    const auto parse = [_abstract_feature,
                        &population,
                        &peripheral,
                        &subfeatures]( const containers::Condition &cond )
        -> std::function<bool( const containers::Match & )> {
        assert_true( cond.peripheral_ == _abstract_feature.peripheral_ );
        return ConditionParser::parse_single_condition(
            population, peripheral, subfeatures, cond );
    };

    auto range = _abstract_feature.conditions_ | std::views::transform( parse );

    return std::vector<std::function<bool( const containers::Match & )>>(
        range.begin(), range.end() );
}

// ----------------------------------------------------------------------------

std::function<bool( const containers::Match & )>
ConditionParser::parse_single_condition(
    const containers::DataFrame &_population,
    const containers::DataFrame &_peripheral,
    const containers::Features &_subfeatures,
    const containers::Condition &_condition )
{
    switch ( _condition.data_used_ )
        {
            case enums::DataUsed::same_units_categorical:
                return make_same_units_categorical(
                    _population, _peripheral, _condition );

            default:
                throw_unless( false, "Unknown condition" );
                return []( const containers::Match &match ) { return true; };
        }
}

// ----------------------------------------------------------------------------
}  // namespace algorithm
}  // namespace dfs
