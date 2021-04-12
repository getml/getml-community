#include "helpers/helpers.hpp"

namespace helpers
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
    const std::vector<Column<strings::String>>& _text,
    const std::vector<Column<Float>>& _time_stamps,
    const RowIndices& _row_indices,
    const WordIndices& _word_indices )
    : categoricals_( _categoricals ),
      discretes_( _discretes ),
      indices_( _indices ),
      join_keys_( _join_keys ),
      name_( _name ),
      numericals_( _numericals ),
      row_indices_( _row_indices ),
      targets_( _targets ),
      text_( _text ),
      time_stamps_( _time_stamps ),
      word_indices_( _word_indices )
{
    assert_true( _indices.size() == _join_keys.size() );

    assert_true(
        _row_indices.size() == 0 || _row_indices.size() == _text.size() );

    assert_true(
        _word_indices.size() == 0 || _word_indices.size() == _text.size() );

    for ( auto& col : _categoricals )
        {
            assert_msg(
                col.nrows_ == nrows(),
                "categoricals: col.nrows_: " + std::to_string( col.nrows_ ) +
                    ", nrows(): " + std::to_string( nrows() ) );
        }

    for ( auto& col : _discretes )
        {
            assert_msg(
                col.nrows_ == nrows(),
                "discretes: col.nrows_: " + std::to_string( col.nrows_ ) +
                    ", nrows(): " + std::to_string( nrows() ) );
        }

    for ( auto& col : _join_keys )
        {
            assert_msg(
                col.nrows_ == nrows(),
                "join_keys: col.nrows_: " + std::to_string( col.nrows_ ) +
                    ", nrows(): " + std::to_string( nrows() ) );
        }

    for ( auto& col : _numericals )
        {
            assert_msg(
                col.nrows_ == nrows(),
                "numericals: col.nrows_: " + std::to_string( col.nrows_ ) +
                    ", nrows(): " + std::to_string( nrows() ) );
        }

    for ( auto& col : _targets )
        {
            assert_msg(
                col.nrows_ == nrows(),
                "targets: col.nrows_: " + std::to_string( col.nrows_ ) +
                    ", nrows(): " + std::to_string( nrows() ) );
        }

    for ( auto& col : _text )
        {
            assert_msg(
                col.nrows_ == nrows(),
                "text: col.nrows_: " + std::to_string( col.nrows_ ) +
                    ", nrows(): " + std::to_string( nrows() ) );
        }

    for ( auto& col : _time_stamps )
        {
            assert_msg(
                col.nrows_ == nrows(),
                "time_stamps: col.nrows_: " + std::to_string( col.nrows_ ) +
                    ", nrows(): " + std::to_string( nrows() ) );
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
    const std::vector<Column<strings::String>>& _text,
    const std::vector<Column<Float>>& _time_stamps )
    : DataFrame(
          _categorical,
          _discrete,
          DataFrame::create_indices( _join_keys ),
          _join_keys,
          _name,
          _numerical,
          _target,
          _text,
          _time_stamps )
{
}

// ----------------------------------------------------------------------------

std::shared_ptr<Index> DataFrame::create_index( const Column<Int>& _join_key )
{
    const auto new_index = std::make_shared<Index>();

    for ( size_t ix = 0; ix < _join_key.nrows_; ++ix )
        {
            if ( _join_key[ix] >= 0 )
                {
                    const auto it = new_index->find( _join_key[ix] );

                    if ( it == new_index->end() )
                        {
                            new_index->insert_or_assign(
                                _join_key[ix], std::vector<size_t>( { ix } ) );
                        }
                    else
                        {
                            it->second.push_back( ix );
                        }
                }
        }

    return new_index;
}

// ----------------------------------------------------------------------------

std::vector<std::shared_ptr<Index>> DataFrame::create_indices(
    const std::vector<Column<Int>>& _join_keys )
{
    std::vector<std::shared_ptr<Index>> indices;

    for ( const auto& jk : _join_keys )
        {
            indices.push_back( DataFrame::create_index( jk ) );
        }

    return indices;
}

// ----------------------------------------------------------------------------

DataFrame DataFrame::create_subview(
    const std::string& _join_key,
    const std::string& _time_stamp,
    const std::string& _upper_time_stamp,
    const bool _allow_lagged_targets,
    const RowIndices& _row_indices,
    const WordIndices& _word_indices,
    const AdditionalColumns& _additional ) const
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
    // All time stamps that are not upper time stamp are added to numerical
    // and given the unit time stamp - this is so the end users do not have
    // to understand the difference between time stamps as a type and
    // time stamps as a role.

    auto numericals_and_time_stamps = std::vector<Column<Float>>();

    for ( const auto& col : numericals_ )
        {
            numericals_and_time_stamps.push_back( col );
        }

    for ( const auto& col : _additional )
        {
            numericals_and_time_stamps.push_back( col );
        }

    if ( _allow_lagged_targets )
        {
            for ( const auto& col : targets_ )
                {
                    numericals_and_time_stamps.push_back( col );
                }
        }

    for ( const auto& col : time_stamps_ )
        {
            if ( _upper_time_stamp != "" && col.name_ == _upper_time_stamp )
                {
                    continue;
                }

            const auto ts =
                Column<Float>( col.ptr_, col.name_, col.nrows_, col.unit_ );

            numericals_and_time_stamps.push_back( ts );
        }

    // ---------------------------------------------------------------------------

    if ( _time_stamp == "" )
        {
            return DataFrame(
                categoricals_,
                discretes_,
                { indices_.at( ix_join_key ) },
                { join_keys_.at( ix_join_key ) },
                name_,
                numericals_and_time_stamps,
                targets_,
                text_,
                {},
                _row_indices,
                _word_indices );
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

    if ( _upper_time_stamp == "" )
        {
            return DataFrame(
                categoricals_,
                discretes_,
                { indices_.at( ix_join_key ) },
                { join_keys_.at( ix_join_key ) },
                name_,
                numericals_and_time_stamps,
                targets_,
                text_,
                { time_stamps_.at( ix_time_stamp ) },
                _row_indices,
                _word_indices );
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
        { indices_.at( ix_join_key ) },
        { join_keys_.at( ix_join_key ) },
        name_,
        numericals_and_time_stamps,
        targets_,
        text_,
        { time_stamps_.at( ix_time_stamp ),
          time_stamps_.at( ix_upper_time_stamp ) },
        _row_indices,
        _word_indices );

    // ---------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace helpers
