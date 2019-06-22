#include "SampleContainer.hpp"

namespace autosql
{
// ----------------------------------------------------------------------------

std::shared_ptr<const std::vector<AUTOSQL_INT>>
SampleContainer::create_population_indices(
    const AUTOSQL_INT _nrows, const containers::MatchPtrs& _sample_container )
{
    std::set<AUTOSQL_INT> population_indices;

    for ( auto& sample : _sample_container )
        {
            assert( sample->ix_x_perip >= 0 );

            assert( sample->ix_x_perip < _nrows );

            population_indices.insert( sample->ix_x_perip );
        }

    return std::make_shared<const std::vector<AUTOSQL_INT>>(
        population_indices.begin(), population_indices.end() );
}

// ----------------------------------------------------------------------------

std::shared_ptr<const std::map<AUTOSQL_INT, AUTOSQL_INT>>
SampleContainer::create_output_map(
    const std::shared_ptr<const std::vector<AUTOSQL_INT>>& _indices )
{
    auto output_map = std::make_shared<std::map<AUTOSQL_INT, AUTOSQL_INT>>();

    auto size = static_cast<AUTOSQL_INT>( _indices->size() );

    for ( AUTOSQL_INT i = 0; i < size; ++i )
        {
            ( *output_map )[( *_indices )[i]] = i;
        }

    return output_map;
}

// ----------------------------------------------------------------------------

void SampleContainer::create_samples(
    const AUTOSQL_INT _ix_x_popul,
    const bool _use_timestamps,
    const Index& _index,
    const containers::Matrix<AUTOSQL_INT>& _join_keys_perip,
    const AUTOSQL_INT _join_key_popul,
    const containers::Matrix<AUTOSQL_FLOAT>& _time_stamps_perip,
    const containers::Matrix<AUTOSQL_FLOAT>* _upper_time_stamps,
    const AUTOSQL_FLOAT _time_stamp_popul,
    containers::Matches& _samples )
{
    debug_message( "SampleContainer::create_samples: Finding join key..." );

    auto it = _index.find( _join_key_popul );

    debug_message(
        "SampleContainer::create_samples: Adding to sample containers map, if "
        "necessary..." );

    if ( it != _index.end() )
        {
            for ( AUTOSQL_INT ix_x_perip : it->second )
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
    containers::Matrix<AUTOSQL_FLOAT>& _sample_weights,
    std::vector<containers::Matches>& _samples,
    std::vector<containers::MatchPtrs>& _sample_containers )
{
    debug_message( "create_samples_and_sample_containers ( 1 )..." );

    // ----------------------------------------------------------------------
    // Create the sample weights

    std::uniform_int_distribution<> uniform_distribution(
        0, _sample_weights.nrows() - 1 );

    std::fill( _sample_weights.begin(), _sample_weights.end(), 0.0 );

    AUTOSQL_INT num_samples = static_cast<AUTOSQL_INT>(
        static_cast<AUTOSQL_FLOAT>( _sample_weights.nrows() ) *
        _hyperparameters.sampling_rate );

    for ( AUTOSQL_INT i = 0; i < num_samples; ++i )
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

    for ( AUTOSQL_INT j = 0;
          j < static_cast<AUTOSQL_INT>( _peripheral_tables.size() );
          ++j )
        {
            // --------------------------------------------------------------

            for ( AUTOSQL_INT ix_x_popul = 0;
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

    for ( AUTOSQL_INT j = 0;
          j < static_cast<AUTOSQL_INT>( _peripheral_tables.size() );
          ++j )
        {
            // --------------------------------------------------------------

            for ( AUTOSQL_INT ix_x_popul = 0;
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
    const AUTOSQL_INT _ix_x_perip,
    const containers::Matrix<AUTOSQL_FLOAT>& _time_stamps_perip,
    const containers::Matrix<AUTOSQL_FLOAT>* _upper_time_stamps,
    const AUTOSQL_FLOAT _time_stamp_popul )
{
    const AUTOSQL_FLOAT lower_time_stamps_diff =
        _time_stamp_popul - _time_stamps_perip[_ix_x_perip];

    if ( _upper_time_stamps == nullptr )
        {
            return lower_time_stamps_diff >= 0.0;
        }

    const AUTOSQL_FLOAT upper_time_stamps_diff =
        _time_stamp_popul - ( *_upper_time_stamps )[_ix_x_perip];

    return (
        ( lower_time_stamps_diff >= 0.0 ) &&
        ( std::isnan( upper_time_stamps_diff ) ||
          upper_time_stamps_diff < 0.0 ) );
}

// ----------------------------------------------------------------------------
}  // namespace autosql