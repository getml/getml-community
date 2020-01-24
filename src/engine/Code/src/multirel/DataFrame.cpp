#include "multirel/containers/containers.hpp"

namespace multirel
{
namespace containers
{
// ----------------------------------------------------------------------------

DataFrame::DataFrame(
    const std::vector<Column<Int>>& _categoricals,
    const std::vector<Column<Float>>& _discretes,
    const std::vector<std::shared_ptr<Index>>& _indices,
    const std::vector<Column<Int>>& _join_keys,
    const std::string& _name,
    const std::vector<Column<Float>>& _numericals,
    const std::vector<Column<Float>>& _targets,
    const std::vector<Column<Float>>& _time_stamps )
    : categoricals_( _categoricals ),
      discretes_( _discretes ),
      indices_( _indices ),
      join_keys_( _join_keys ),
      name_( _name ),
      numericals_( _numericals ),
      targets_( _targets ),
      time_stamps_( _time_stamps )
{
    assert_true( _join_keys.size() > 0 );
    assert_true( _time_stamps.size() > 0 );
    assert_true( _indices.size() == _join_keys.size() );

    for ( auto& col : _categoricals )
        {
            assert_true( col.nrows_ == nrows() );
        }

    for ( auto& col : _discretes )
        {
            assert_true( col.nrows_ == nrows() );
        }

    for ( auto& col : _join_keys )
        {
            assert_true( col.nrows_ == nrows() );
        }

    for ( auto& col : _numericals )
        {
            assert_true( col.nrows_ == nrows() );
        }

    for ( auto& col : _targets )
        {
            assert_true( col.nrows_ == nrows() );
        }

    for ( auto& col : _time_stamps )
        {
            assert_true( col.nrows_ == nrows() );
        }
}

// ----------------------------------------------------------------------------

DataFrame::DataFrame(
    const std::vector<Column<Int>>& _categorical,
    const std::vector<Column<Float>>& _discrete,
    const std::vector<Column<Int>>& _join_keys,
    const std::string& _name,
    const std::vector<Column<Float>>& _numerical,
    const std::vector<Column<Float>>& _target,
    const std::vector<Column<Float>>& _time_stamps )
    : DataFrame(
          _categorical,
          _discrete,
          DataFrame::create_indices( _join_keys ),
          _join_keys,
          _name,
          _numerical,
          _target,
          _time_stamps )
{
}

// ----------------------------------------------------------------------------

std::vector<std::shared_ptr<Index>> DataFrame::create_indices(
    const std::vector<Column<Int>>& _join_keys )
{
    std::vector<std::shared_ptr<Index>> indices;

    for ( size_t i = 0; i < _join_keys.size(); ++i )
        {
            Index new_index;

            const auto& current_join_key = _join_keys[i];

            for ( size_t ix_x_perip = 0; ix_x_perip < current_join_key.nrows_;
                  ++ix_x_perip )
                {
                    if ( current_join_key[ix_x_perip] >= 0 )
                        {
                            auto it =
                                new_index.find( current_join_key[ix_x_perip] );

                            if ( it == new_index.end() )
                                {
                                    new_index[current_join_key[ix_x_perip]] = {
                                        ix_x_perip};
                                }
                            else
                                {
                                    it->second.push_back( ix_x_perip );
                                }
                        }
                }

            indices.push_back( std::make_shared<Index>( new_index ) );
        }

    return indices;
}

// ----------------------------------------------------------------------------

DataFrame DataFrame::create_subview(
    const std::string& _name,
    const std::string& _join_key,
    const std::string& _time_stamp,
    const std::string& _upper_time_stamp ) const
{
    // ---------------------------------------------------------------------------

    size_t ix_join_key = 0;

    for ( ; ix_join_key < join_keys_.size(); ++ix_join_key )
        {
            if ( join_keys_[ix_join_key].name_ == _join_key )
                {
                    break;
                }
        }

    if ( ix_join_key == join_keys_.size() )
        {
            throw std::runtime_error(
                "Join key named '" + _join_key + "' not found in table '" +
                name_ + "'!" );
        }

    // ---------------------------------------------------------------------------

    size_t ix_time_stamp = 0;

    for ( ; ix_time_stamp < time_stamps_.size(); ++ix_time_stamp )
        {
            if ( time_stamps_[ix_time_stamp].name_ == _time_stamp )
                {
                    break;
                }
        }

    if ( ix_time_stamp == time_stamps_.size() )
        {
            throw std::runtime_error(
                "Time stamp named '" + _time_stamp + "' not found in table '" +
                name_ + "'!" );
        }

    // ---------------------------------------------------------------------------
    // All time stamps that are not upper time stamp are added to numerical
    // and given the unit time stamp - this is so the end users do not have
    // to understand the difference between time stamps as a type and
    // time stamps as a role.

    auto numericals_and_time_stamps = std::vector<containers::Column<Float>>();

    for ( const auto& col : numericals_ )
        {
            numericals_and_time_stamps.push_back( col );
        }

    for ( const auto& col : time_stamps_ )
        {
            if ( _upper_time_stamp != "" && col.name_ == _upper_time_stamp )
                {
                    continue;
                }

            const auto ts = containers::Column<Float>(
                col.data_,
                col.name_,
                col.nrows_,
                "unit: getml_time_stamp, comparison only" );

            numericals_and_time_stamps.push_back( ts );
        }

    // ---------------------------------------------------------------------------

    if ( _upper_time_stamp == "" )
        {
            return DataFrame(
                categoricals_,
                discretes_,
                {indices_[ix_join_key]},
                {join_keys_[ix_join_key]},
                _name,
                numericals_and_time_stamps,
                targets_,
                {time_stamps_[ix_time_stamp]} );
        }

    // ---------------------------------------------------------------------------

    size_t ix_upper_time_stamp = 0;

    for ( ; ix_upper_time_stamp < time_stamps_.size(); ++ix_upper_time_stamp )
        {
            if ( time_stamps_[ix_upper_time_stamp].name_ == _upper_time_stamp )
                {
                    break;
                }
        }

    if ( ix_upper_time_stamp == time_stamps_.size() )
        {
            throw std::runtime_error(
                "Time stamp named '" + _upper_time_stamp +
                "' not found in table '" + name_ + "'!" );
        }

    // ---------------------------------------------------------------------------

    return DataFrame(
        categoricals_,
        discretes_,
        {indices_[ix_join_key]},
        {join_keys_[ix_join_key]},
        _name,
        numericals_and_time_stamps,
        targets_,
        {time_stamps_[ix_time_stamp], time_stamps_[ix_upper_time_stamp]} );

    // ---------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace containers
}  // namespace multirel
