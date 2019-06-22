#include "SampleContainer.hpp"

namespace autosql
{
// ----------------------------------------------------------------------------

std::shared_ptr<const std::vector<Int>>
SampleContainer::create_population_indices(
    const Int _nrows, const containers::MatchPtrs& _sample_container )
{
    std::set<Int> population_indices;

    for ( auto& sample : _sample_container )
        {
            assert( sample->ix_x_perip >= 0 );

            assert( sample->ix_x_perip < _nrows );

            population_indices.insert( sample->ix_x_perip );
        }

    return std::make_shared<const std::vector<Int>>(
        population_indices.begin(), population_indices.end() );
}

// ----------------------------------------------------------------------------

std::shared_ptr<const std::map<Int, Int>>
SampleContainer::create_output_map(
    const std::shared_ptr<const std::vector<Int>>& _indices )
{
    auto output_map = std::make_shared<std::map<Int, Int>>();

    auto size = static_cast<Int>( _indices->size() );

    for ( Int i = 0; i < size; ++i )
        {
            ( *output_map )[( *_indices )[i]] = i;
        }

    return output_map;
}

// ----------------------------------------------------------------------------

void SampleContainer::create_samples(
    const Int _ix_x_popul,
    const bool _use_timestamps,
    const Index& _index,
    const containers::Matrix<Int>& _join_keys_perip,
    const Int _join_key_popul,
    const containers::Matrix<Float>& _time_stamps_perip,
    const containers::Matrix<Float>* _upper_time_stamps,
    const Float _time_stamp_popul,
    containers::Matches& _samples )
{
    debug_message( "SampleContainer::create_samples: Finding join key..." );

    auto it = _index.find( _join_key_popul );

    debug_message(
        "SampleContainer::create_samples: Adding to sample containers map, if "
        "necessary..." );

    if ( it != _index.end() )
        {
            for ( Int ix_x_perip : it->second )
                {
                    const bool use_this_sample =
                        ( !_use_timestamps || time_stamp_popul_in_range(
                                                  ix_x_perip,
                                                  _time_stamps_perip,
                                                  _upper_time_stamps,
                                                  _time_stamp_popul ) );

                    if ( use_this_sample )
                        {
                            _samples.push_back( {
                                false,        // activated
                                0,            // categorical_value
                                ix_x_perip,   // ix_x_perip
                                _ix_x_popul,  // ix_x_popul
                                0.0           // numerical_value
                            } );
                        }
                }
        }

    debug_message( "SampleContainer::create_samples done." );
}

// ----------------------------------------------------------------------------

void SampleContainer::create_sample_container(
    containers::Matches& _samples, containers::MatchPtrs& _sample_container )
{
    _sample_container.resize( _samples.size() );

    for ( AUTOSQL_SIZE i = 0; i < _samples.size(); ++i )
        {
            _sample_container[i] = _samples.data() + i;
        }
}

// ----------------------------------------------------------------------------

void SampleContainer::create_samples_and_sample_containers(
    const descriptors::Hyperparameters& _hyperparameters,
    const std::vector<containers::DataFrame>& _peripheral_tables,
    const containers::DataFrameView& _population_table,
    std::mt19937& _random_number_generator,
    containers::Matrix<Float>& _sample_weights,
    std::vector<containers::Matches>& _samples,
    std::vector<containers::MatchPtrs>& _sample_containers )
{
    debug_message( "create_samples_and_sample_containers ( 1 )..." );

    // ----------------------------------------------------------------------
    // Create the sample weights

    std::uniform_int_distribution<> uniform_distribution(
        0, _sample_weights.nrows() - 1 );

    std::fill( _sample_weights.begin(), _sample_weights.end(), 0.0 );

    Int num_samples = static_cast<Int>(
        static_cast<Float>( _sample_weights.nrows() ) *
        _hyperparameters.sampling_rate );

    for ( Int i = 0; i < num_samples; ++i )
        {
            _sample_weights[uniform_distribution( _random_number_generator )] +=
                1.0;
        }

    // ----------------------------------------------------------------------
    // Create sample containers for all non-zero samples

    _samples = std::vector<containers::Matches>( _peripheral_tables.size() );

    _sample_containers =
        std::vector<containers::MatchPtrs>( _peripheral_tables.size() );

    // ----------------------------------------------------------------------

    assert( _samples.size() == _peripheral_tables.size() );

    for ( Int j = 0;
          j < static_cast<Int>( _peripheral_tables.size() );
          ++j )
        {
            // --------------------------------------------------------------

            for ( Int ix_x_popul = 0;
                  ix_x_popul < _population_table.nrows();
                  ++ix_x_popul )
                {
                    if ( _sample_weights[ix_x_popul] != 0.0 )
                        {
                            create_samples(
                                ix_x_popul,
                                _hyperparameters.use_timestamps,
                                *_peripheral_tables[j].index().get(),
                                _peripheral_tables[j].join_key(),
                                _population_table.join_key( ix_x_popul, j ),
                                _peripheral_tables[j].time_stamps(),
                                _peripheral_tables[j].upper_time_stamps(),
                                _population_table.time_stamp( ix_x_popul, j ),
                                _samples[j] );
                        }
                }

            // --------------------------------------------------------------

            create_sample_container( _samples[j], _sample_containers[j] );

            // --------------------------------------------------------------
        }

    debug_message( "create_samples_and_sample_containers ( 1 )...done." );
}

// ----------------------------------------------------------------------------

void SampleContainer::create_samples_and_sample_containers(
    const descriptors::Hyperparameters& _hyperparameters,
    const std::vector<containers::DataFrame>& _peripheral_tables,
    const containers::DataFrameView& _population_table,
    std::vector<containers::Matches>& _samples,
    std::vector<containers::MatchPtrs>& _sample_containers )
{
    debug_message( "create_samples_and_sample_containers ( 2 )..." );

    for ( Int j = 0;
          j < static_cast<Int>( _peripheral_tables.size() );
          ++j )
        {
            // --------------------------------------------------------------

            for ( Int ix_x_popul = 0;
                  ix_x_popul < _population_table.nrows();
                  ++ix_x_popul )
                {
                    SampleContainer::create_samples(
                        ix_x_popul,
                        _hyperparameters.use_timestamps,
                        *_peripheral_tables[j].index().get(),
                        _peripheral_tables[j].join_key(),
                        _population_table.join_key( ix_x_popul, j ),
                        _peripheral_tables[j].time_stamps(),
                        _peripheral_tables[j].upper_time_stamps(),
                        _population_table.time_stamp( ix_x_popul, j ),
                        _samples[j] );
                }

            // --------------------------------------------------------------

            SampleContainer::create_sample_container(
                _samples[j], _sample_containers[j] );

            // --------------------------------------------------------------
        }

    debug_message( "create_samples_and_sample_containers ( 2 )...done." );
}

// ----------------------------------------------------------------------------

bool SampleContainer::time_stamp_popul_in_range(
    const Int _ix_x_perip,
    const containers::Matrix<Float>& _time_stamps_perip,
    const containers::Matrix<Float>* _upper_time_stamps,
    const Float _time_stamp_popul )
{
    const Float lower_time_stamps_diff =
        _time_stamp_popul - _time_stamps_perip[_ix_x_perip];

    if ( _upper_time_stamps == nullptr )
        {
            return lower_time_stamps_diff >= 0.0;
        }

    const Float upper_time_stamps_diff =
        _time_stamp_popul - ( *_upper_time_stamps )[_ix_x_perip];

    return (
        ( lower_time_stamps_diff >= 0.0 ) &&
        ( std::isnan( upper_time_stamps_diff ) ||
          upper_time_stamps_diff < 0.0 ) );
}

// ----------------------------------------------------------------------------
}  // namespace autosql