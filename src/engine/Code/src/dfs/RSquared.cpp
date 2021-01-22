#include "dfs/algorithm/algorithm.hpp"

namespace dfs
{
namespace algorithm
{
// ----------------------------------------------------------------------------

std::vector<Float> RSquared::calculate(
    const std::vector<containers::Column<Float>>& _targets,
    const containers::Features& _features )
{
    const auto mean_targets = calc_mean_targets( _targets );

    const auto var_targets = calc_var_targets( _targets );

    const auto calc_r =
        [&mean_targets, &var_targets, &_targets](
            const std::shared_ptr<const std::vector<Float>> feature ) -> Float {
        return RSquared::calc_for_feature(
            mean_targets, var_targets, _targets, feature );
    };

    auto range = _features | std::views::transform( calc_r );

    return std::vector<Float>( range.begin(), range.end() );
}

// ----------------------------------------------------------------------------

Float RSquared::calc_for_feature(
    const std::vector<Float>& _mean_targets,
    const std::vector<Float>& _var_targets,
    const std::vector<containers::Column<Float>>& _targets,
    const std::shared_ptr<const std::vector<Float>>& _feature )
{
    const auto calc = [_mean_targets, _var_targets, _targets, _feature](
                          const size_t ix ) -> Float {
        return RSquared::calc_for_target(
            _mean_targets.at( ix ),
            _var_targets.at( ix ),
            _targets.at( ix ),
            _feature );
    };

    assert_true( _mean_targets.size() == _targets.size() );

    assert_true( _var_targets.size() == _targets.size() );

    auto iota = std::vector<size_t>( _targets.size() );

    std::iota( iota.begin(), iota.end(), 0 );

    auto range = iota | std::views::transform( calc );

    return helpers::ColumnOperators::avg( range.begin(), range.end() );
}

// ----------------------------------------------------------------------------

Float RSquared::calc_for_target(
    const Float _mean_target,
    const Float _var_target,
    const containers::Column<Float>& _targets,
    const std::shared_ptr<const std::vector<Float>> _feature )
{
    assert_true( _feature );

    assert_true( _feature->size() == _targets.nrows_ );

    if ( _var_target == 0.0 )
        {
            return 0.0;
        }

    const auto var_feature =
        helpers::ColumnOperators::var( _feature->begin(), _feature->end() );

    if ( var_feature == 0.0 )
        {
            return 0.0;
        }

    const auto mean_feature =
        helpers::ColumnOperators::avg( _feature->begin(), _feature->end() );

    const auto multiply = [_mean_target, mean_feature](
                              const Float t, const Float f ) -> Float {
        return ( t - _mean_target ) * ( f - mean_feature );
    };

    const auto nrows_float = static_cast<Float>( _targets.nrows_ );

    const auto cross_corr = std::inner_product(
                                _targets.begin(),
                                _targets.end(),
                                _feature->begin(),
                                0.0,
                                std::plus<Float>(),
                                multiply ) /
                            nrows_float;

    return ( cross_corr / var_feature ) * ( cross_corr / _var_target );
}

// ----------------------------------------------------------------------------

std::vector<Float> RSquared::calc_mean_targets(
    const std::vector<containers::Column<Float>>& _targets )
{
    const auto calc_mean =
        []( const containers::Column<Float>& _target ) -> Float {
        return helpers::ColumnOperators::avg( _target.begin(), _target.end() );
    };

    auto range = _targets | std::views::transform( calc_mean );

    return std::vector<Float>( range.begin(), range.end() );
}

// ----------------------------------------------------------------------------

std::vector<Float> RSquared::calc_var_targets(
    const std::vector<containers::Column<Float>>& _targets )
{
    const auto calc_var =
        []( const containers::Column<Float>& _target ) -> Float {
        return helpers::ColumnOperators::var( _target.begin(), _target.end() );
    };

    auto range = _targets | std::views::transform( calc_var );

    return std::vector<Float>( range.begin(), range.end() );
}

// ----------------------------------------------------------------------------
}  // namespace algorithm
}  // namespace dfs
