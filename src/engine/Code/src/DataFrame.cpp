#include "containers/containers.hpp"

namespace relboost
{
namespace containers
{
// ----------------------------------------------------------------------------

DataFrame::DataFrame(
    const Matrix<RELBOOST_INT>& _categorical,
    const Matrix<RELBOOST_FLOAT>& _discrete,
    const std::vector<Matrix<RELBOOST_INT>>& _join_keys,
    const std::string& _name,
    const Matrix<RELBOOST_FLOAT>& _numerical,
    const Matrix<RELBOOST_FLOAT>& _target,
    const std::vector<Matrix<RELBOOST_FLOAT>>& _time_stamps )
    : categorical_( _categorical ),
      discrete_( _discrete ),
      indices_( DataFrame::create_indices( _join_keys ) ),
      join_keys_( _join_keys ),
      name_( _name ),
      numerical_( _numerical ),
      target_( _target ),
      time_stamps_( _time_stamps )
{
    assert( _join_keys.size() > 0 );
    assert( _time_stamps.size() > 0 );

    assert( categorical_.nrows_ == nrows() );
    assert( discrete_.nrows_ == nrows() );
    assert( numerical_.nrows_ == nrows() );

    for ( auto& jk : _join_keys )
        {
            assert( jk.nrows_ == nrows() );
        }

    assert( target_.nrows_ == nrows() );
    assert( target_.colnames_.size() < 2 );

    for ( auto& ts : _time_stamps )
        {
            assert( ts.nrows_ == nrows() );
        }
}

// ----------------------------------------------------------------------------

DataFrame::DataFrame(
    const Matrix<RELBOOST_INT>& _categorical,
    const Matrix<RELBOOST_FLOAT>& _discrete,
    const std::vector<std::shared_ptr<RELBOOST_INDEX>>& _indices,
    const std::vector<Matrix<RELBOOST_INT>>& _join_keys,
    const std::string& _name,
    const Matrix<RELBOOST_FLOAT>& _numerical,
    const Matrix<RELBOOST_FLOAT>& _target,
    const std::vector<Matrix<RELBOOST_FLOAT>>& _time_stamps )
    : categorical_( _categorical ),
      discrete_( _discrete ),
      indices_( _indices ),
      join_keys_( _join_keys ),
      name_( _name ),
      numerical_( _numerical ),
      target_( _target ),
      time_stamps_( _time_stamps )
{
    assert( _join_keys.size() > 0 );
    assert( _time_stamps.size() > 0 );

    assert( categorical_.nrows_ == nrows() );
    assert( discrete_.nrows_ == nrows() );
    assert( numerical_.nrows_ == nrows() );

    for ( auto& jk : _join_keys )
        {
            assert( jk.nrows_ == nrows() );
        }

    assert( target_.nrows_ == nrows() );
    assert( target_.colnames_.size() < 2 );

    for ( auto& ts : _time_stamps )
        {
            assert( ts.nrows_ == nrows() );
        }
}

// ----------------------------------------------------------------------------

std::vector<std::shared_ptr<RELBOOST_INDEX>> DataFrame::create_indices(
    const std::vector<Matrix<RELBOOST_INT>>& _join_keys )
{
    std::vector<std::shared_ptr<RELBOOST_INDEX>> indices;

    for ( size_t i = 0; i < _join_keys.size(); ++i )
        {
            RELBOOST_INDEX new_index;

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

            indices.push_back( std::make_shared<RELBOOST_INDEX>( new_index ) );
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
            if ( join_keys_[ix_join_key].colnames_[0] == _join_key )
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
            if ( time_stamps_[ix_time_stamp].colnames_[0] == _time_stamp )
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

    if ( _upper_time_stamp == "" )
        {
            return DataFrame(
                categorical_,
                discrete_,
                {indices_[ix_join_key]},
                {join_keys_[ix_join_key]},
                _name,
                numerical_,
                target_,
                {time_stamps_[ix_time_stamp]} );
        }

    // ---------------------------------------------------------------------------

    size_t ix_upper_time_stamp = 0;

    for ( ; ix_upper_time_stamp < time_stamps_.size(); ++ix_upper_time_stamp )
        {
            if ( time_stamps_[ix_upper_time_stamp].colnames_[0] ==
                 _upper_time_stamp )
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
        categorical_,
        discrete_,
        {indices_[ix_join_key]},
        {join_keys_[ix_join_key]},
        _name,
        numerical_,
        target_,
        {time_stamps_[ix_time_stamp], time_stamps_[ix_upper_time_stamp]} );

    // ---------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace containers
}  // namespace relboost
