#include "containers/containers.hpp"

namespace autosql
{
namespace containers
{
// ----------------------------------------------------------------------------

void DataFrame::append( DataFrame &_other )
{
    if ( join_keys().size() != _other.join_keys().size() )
        {
            throw std::invalid_argument(
                "Append: Number of join keys does not match!" );
        }

    if ( time_stamps_all().size() != _other.time_stamps_all().size() )
        {
            throw std::invalid_argument(
                "Append: Number of time stamp does not match!" );
        }

    categorical().append( _other.categorical() );

    discrete().append( _other.discrete() );

    for ( size_t i = 0; i < join_keys().size(); ++i )
        {
            join_key( i ).append( _other.join_key( i ) );
        }

    numerical().append( _other.numerical() );

    targets().append( _other.targets() );

    for ( size_t i = 0; i < time_stamps_all().size(); ++i )
        {
            time_stamps( i ).append( _other.time_stamps( i ) );
        }
}

// ----------------------------------------------------------------------------

void DataFrame::clear()
{
    categorical().clear();

    discrete().clear();

    std::for_each(
        join_keys().begin(), join_keys().end(), []( Matrix<SQLNET_INT> &mat ) {
            mat.clear();
        } );

    numerical().clear();

    targets().clear();

    std::for_each(
        time_stamps_all().begin(),
        time_stamps_all().end(),
        []( Matrix<SQLNET_FLOAT> &mat ) { mat.clear(); } );

    indices().clear();
}

// ----------------------------------------------------------------------------

void DataFrame::check_plausibility() const
{
    if ( join_keys().size() == 0 )
        {
            throw std::invalid_argument(
                "You need to provide at least one column of join keys in " +
                name() + "!" );
        }

    if ( time_stamps_all().size() == 0 )
        {
            throw std::invalid_argument(
                "You need to provide at least one column of time stamps in " +
                name() + "!" );
        }

    auto expected_nrows = join_key( 0 ).nrows();

    const bool any_join_key_does_not_match = std::any_of(
        join_keys().begin(),
        join_keys().end(),
        [expected_nrows]( const containers::Matrix<SQLNET_INT> &mat ) {
            return mat.nrows() != expected_nrows;
        } );

    const bool any_time_stamp_does_not_match = std::any_of(
        time_stamps_all().begin(),
        time_stamps_all().end(),
        [expected_nrows]( const containers::Matrix<SQLNET_FLOAT> &mat ) {
            return mat.nrows() != expected_nrows;
        } );

    if ( categorical().nrows() != expected_nrows ||
         discrete().nrows() != expected_nrows ||
         targets().nrows() != expected_nrows || any_join_key_does_not_match ||
         any_time_stamp_does_not_match )
        {
            throw std::runtime_error(
                "Something went very wrong during data loading: The number of "
                "rows in different elements of " +
                name() +
                " do not "
                "match!" );
        }
}

// ----------------------------------------------------------------------------

void DataFrame::create_indices()
{
    if ( indices().size() != join_keys().size() )
        {
            indices().resize( join_keys().size() );
        }

    for ( SQLNET_SIZE i = 0; i < join_keys().size(); ++i )
        {
            if ( !indices()[i] )
                {
                    indices()[i] = std::make_shared<SQLNET_INDEX>();
                }

            SQLNET_INDEX &map = *indices()[i];

            const auto &current_join_key = join_key( i );

            const SQLNET_INT batch_begin =
                ( map.size() == 0
                      ? 0
                      : current_join_key
                            .batches()[current_join_key.num_batches() - 1] );

            for ( SQLNET_INT ix_x_perip = batch_begin;
                  ix_x_perip < current_join_key.nrows();
                  ++ix_x_perip )
                {
                    if ( current_join_key[ix_x_perip] >= 0 )
                        {
                            auto it = map.find( current_join_key[ix_x_perip] );

                            if ( it == map.end() )
                                {
                                    map[current_join_key[ix_x_perip]] = {
                                        ix_x_perip};
                                }
                            else if (
                                it->second.size() > 0 &&
                                it->second.back() < ix_x_perip )
                                {
                                    it->second.push_back( ix_x_perip );
                                }
                        }
                }
        }
}

// ----------------------------------------------------------------------------

void DataFrame::float_matrix(
    containers::Matrix<SQLNET_FLOAT> &_mat,
    const std::string &_role,
    const std::string _name,
    const SQLNET_SIZE _num )
{
    if ( _role == "discrete" )
        {
            discrete_ = _mat;
        }
    else if ( _role == "numerical" )
        {
            numerical_ = _mat;
        }
    else if ( _role == "targets" )
        {
            targets_ = _mat;
        }
    else if ( _role == "time_stamps" )
        {
            if ( _num < num_time_stamps() )
                {
                    time_stamps_[_num] = _mat;

                    time_stamps_[_num].name() = _name;
                }
            else if ( _num == num_time_stamps() )
                {
                    time_stamps_.push_back( _mat );

                    time_stamps_.back().name() = _name;
                }
            else
                {
                    throw std::runtime_error(
                        "Time stamps index " + std::to_string( _num ) +
                        " out of range!" );
                }
        }
    else
        {
            throw std::invalid_argument( "Role for float matrix not known!" );
        }
}

// ----------------------------------------------------------------------------

containers::Matrix<SQLNET_FLOAT> &DataFrame::float_matrix(
    const std::string &_role, const SQLNET_SIZE _num )
{
    if ( _role == "discrete" )
        {
            return discrete_;
        }
    else if ( _role == "numerical" )
        {
            return numerical_;
        }
    else if ( _role == "targets" )
        {
            return targets_;
        }
    else if ( _role == "time_stamps" )
        {
            return time_stamps_[_num];
        }

    throw std::invalid_argument( "Role for float matrix not known!" );
}

// ----------------------------------------------------------------------------

Poco::JSON::Object DataFrame::get_colnames()
{
    // ----------------------------------------

    Poco::JSON::Object obj;

    // ----------------------------------------

    obj.set( "categorical_", categorical().colnames().get()[0] );

    obj.set( "discrete_", discrete().colnames().get()[0] );

    {
        std::vector<std::string> join_keys_names;

        std::for_each(
            join_keys().begin(),
            join_keys().end(),
            [&join_keys_names]( containers::Matrix<SQLNET_INT> &mat ) {
                join_keys_names.push_back( mat.colname( 0 ) );
            } );

        obj.set( "join_keys_", join_keys_names );
    }

    obj.set( "numerical_", numerical().colnames().get()[0] );

    obj.set( "targets_", targets().colnames().get()[0] );

    {
        std::vector<std::string> time_stamps_names;

        std::for_each(
            time_stamps_all().begin(),
            time_stamps_all().end(),
            [&time_stamps_names]( containers::Matrix<SQLNET_FLOAT> &mat ) {
                time_stamps_names.push_back( mat.colname( 0 ) );
            } );

        obj.set( "time_stamps_", time_stamps_names );
    }

    // ----------------------------------------

    return obj;
}

// ----------------------------------------------------------------------------

Poco::JSON::Object DataFrame::get_content(
    const std::int32_t _draw,
    const std::int32_t _start,
    const std::int32_t _length ) const
{
    // ----------------------------------------

    check_plausibility();

    // ----------------------------------------

    if ( _length < 0 )
        {
            throw std::invalid_argument( "length must be positive!" );
        }

    if ( _start < 0 )
        {
            throw std::invalid_argument( "start must be positive!" );
        }

    if ( _start >= nrows() )
        {
            throw std::invalid_argument(
                "start must be smaller than number of rows!" );
        }

    // ----------------------------------------

    Poco::JSON::Object obj;

    // ----------------------------------------

    obj.set( "draw", _draw );

    obj.set( "recordsTotal", nrows() );

    obj.set( "recordsFiltered", nrows() );

    // ----------------------------------------

    Poco::JSON::Array data;

    const auto begin = static_cast<unsigned int>( _start );

    const auto end = ( _start + _length > nrows() )
                         ? nrows()
                         : static_cast<unsigned int>( _start + _length );

    for ( auto i = begin; i < end; ++i )
        {
            Poco::JSON::Array row;

            for ( size_t j = 0; j < num_time_stamps(); ++j )
                {
                    row.add( to_time_stamp( time_stamps( j )( i, 0 ) ) );
                }

            for ( size_t j = 0; j < num_join_keys(); ++j )
                {
                    row.add( join_keys_encoding()[join_key( j )( i, 0 )] );
                }

            for ( std::int32_t j = 0; j < targets().ncols(); ++j )
                {
                    row.add( std::to_string( targets()( i, j ) ) );
                }

            for ( std::int32_t j = 0; j < categorical().ncols(); ++j )
                {
                    row.add( categories()[categorical()( i, j )] );
                }

            for ( std::int32_t j = 0; j < discrete().ncols(); ++j )
                {
                    row.add( std::to_string( discrete()( i, j ) ) );
                }

            for ( std::int32_t j = 0; j < numerical().ncols(); ++j )
                {
                    row.add( std::to_string( numerical()( i, j ) ) );
                }

            data.add( row );
        }

    obj.set( "data", data );

    // ----------------------------------------

    return obj;
}

// ----------------------------------------------------------------------------

void DataFrame::int_matrix(
    containers::Matrix<SQLNET_INT> &_mat,
    const std::string _role,
    const std::string _name,
    const SQLNET_SIZE _num )
{
    if ( _role == "categorical" )
        {
            categorical_ = _mat;
        }
    else if ( _role == "join_key" )
        {
            if ( _num < num_join_keys() )
                {
                    join_keys_[_num] = _mat;

                    join_keys_[_num].name() = _name;
                }
            else if ( _num == num_join_keys() )
                {
                    join_keys_.push_back( _mat );

                    join_keys_.back().name() = _name;
                }
            else
                {
                    throw std::invalid_argument(
                        "Join key index " + std::to_string( _num ) +
                        " out of range!" );
                }
        }
    else
        {
            throw std::invalid_argument( "Role for int matrix not known!" );
        }
}

// ----------------------------------------------------------------------------

containers::Matrix<SQLNET_INT> &DataFrame::int_matrix(
    const std::string &_role, const SQLNET_SIZE _num )
{
    if ( _role == "categorical" )
        {
            return categorical_;
        }
    else if ( _role == "join_key" )
        {
            assert( _num < num_join_keys() );
            return join_keys_[_num];
        }

    throw std::invalid_argument( "Role for int matrix not known!" );
}

// ----------------------------------------------------------------------------

void DataFrame::load( const std::string &_path )
{
    // ---------------------------------------------------------------------
    // Make sure that the _path exists and is directory

    {
        Poco::File file( _path );

        if ( !file.exists() )
            {
                throw std::invalid_argument(
                    "No file or directory named '" +
                    Poco::Path( _path ).makeAbsolute().toString() + "'!" );
            }

        if ( !file.isDirectory() )
            {
                throw std::invalid_argument(
                    "'" + Poco::Path( _path ).makeAbsolute().toString() +
                    "' is not a directory!" );
            }
    }

    // ---------------------------------------------------------------------
    // Load contents

    categorical().load( _path + "categorical" );

    categorical().name() = name();

    discrete().load( _path + "discrete" );

    discrete().name() = name();

    load_join_keys( _path );

    numerical().load( _path + "numerical" );

    numerical().name() = name();

    targets().load( _path + "targets" );

    targets().name() = name();

    load_time_stamps( _path );

    // ---------------------------------------------------------------------
    // Create index

    check_plausibility();

    create_indices();

    // ---------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DataFrame::load_join_keys( const std::string &_path )
{
    join_keys().clear();

    for ( SQLNET_SIZE i = 0; true; ++i )
        {
            std::string join_key_name =
                _path + "join_key_" + std::to_string( i );

            if ( !Poco::File( join_key_name ).exists() )
                {
#ifdef SQLNET_MULTINODE_MPI

                    boost::mpi::communicator comm_world;

                    SQLNET_INT one_more_join_key = 0;

                    boost::mpi::broadcast( comm_world, one_more_join_key, 0 );

                    comm_world.barrier();

#endif  // SQLNET_MULTINODE_MPI

                    break;
                }
            else
                {
#ifdef SQLNET_MULTINODE_MPI

                    boost::mpi::communicator comm_world;

                    SQLNET_INT one_more_join_key = 1;

                    boost::mpi::broadcast( comm_world, one_more_join_key, 0 );

                    comm_world.barrier();

#endif  // SQLNET_MULTINODE_MPI
                }

            containers::Matrix<SQLNET_INT> join_key;

            join_key.load( join_key_name );

            join_key.name() = name();

            join_keys().push_back( join_key );
        }
}

// ----------------------------------------------------------------------------

void DataFrame::load_time_stamps( const std::string &_path )
{
    time_stamps_all().clear();

    for ( SQLNET_SIZE i = 0; true; ++i )
        {
            std::string time_stamps_name =
                _path + "time_stamps_" + std::to_string( i );

            if ( !Poco::File( time_stamps_name ).exists() )
                {
#ifdef SQLNET_MULTINODE_MPI

                    boost::mpi::communicator comm_world;

                    SQLNET_INT one_more_time_stamp = 0;

                    boost::mpi::broadcast( comm_world, one_more_time_stamp, 0 );

                    comm_world.barrier();

#endif  // SQLNET_MULTINODE_MPI

                    break;
                }
            else
                {
#ifdef SQLNET_MULTINODE_MPI

                    boost::mpi::communicator comm_world;

                    SQLNET_INT one_more_time_stamp = 1;

                    boost::mpi::broadcast( comm_world, one_more_time_stamp, 0 );

                    comm_world.barrier();

#endif  // SQLNET_MULTINODE_MPI
                }

            containers::Matrix<SQLNET_FLOAT> time_stamps;

            time_stamps.load( time_stamps_name );

            time_stamps.name() = name();

            time_stamps_all().push_back( time_stamps );
        }
}

// ----------------------------------------------------------------------------

#ifdef SQLNET_MULTINODE_MPI

void DataFrame::load_non_root()
{
    categorical().load();

    categorical().name() = name();

    discrete().load();

    discrete().name() = name();

    join_keys_.clear();

    for ( SQLNET_SIZE i = 0; true; ++i )
        {
            boost::mpi::communicator comm_world;

            SQLNET_INT one_more_join_key = 0;

            boost::mpi::broadcast( comm_world, one_more_join_key, 0 );

            comm_world.barrier();

            if ( one_more_join_key == 0 )
                {
                    break;
                }

            containers::Matrix<SQLNET_INT> join_key;

            join_key.load();

            join_key.name() = name();

            join_keys_.push_back( join_key );
        }

    numerical().load();

    numerical().name() = name();

    targets().load();

    targets().name() = name();

    for ( SQLNET_SIZE i = 0; true; ++i )
        {
            boost::mpi::communicator comm_world;

            SQLNET_INT one_more_time_stamp = 0;

            boost::mpi::broadcast( comm_world, one_more_time_stamp, 0 );

            comm_world.barrier();

            if ( one_more_time_stamp == 0 )
                {
                    break;
                }

            containers::Matrix<SQLNET_FLOAT> time_stamps;

            time_stamps.load();

            time_stamps.name() = name();

            time_stamps_.push_back( time_stamps );
        }

    create_indices();
}

#endif  // SQLNET_MULTINODE_MPI

// ----------------------------------------------------------------------------

SQLNET_UNSIGNED_LONG DataFrame::nbytes()
{
    SQLNET_UNSIGNED_LONG nbytes = categorical().nbytes() + discrete().nbytes() +
                                  numerical().nbytes() + targets().nbytes();

    nbytes = std::accumulate(
        join_keys().begin(),
        join_keys().end(),
        nbytes,
        []( SQLNET_UNSIGNED_LONG &init, containers::Matrix<SQLNET_INT> &mat ) {
            return init + mat.nbytes();
        } );

    nbytes = std::accumulate(
        time_stamps_all().begin(),
        time_stamps_all().end(),
        nbytes,
        []( SQLNET_UNSIGNED_LONG &init,
            containers::Matrix<SQLNET_FLOAT> &mat ) {
            return init + mat.nbytes();
        } );

    return nbytes;
}

// ----------------------------------------------------------------------------

void DataFrame::save( const std::string &_path )
{
    // If the path already exists, delete it to avoid
    // conflicts with already existing files.
    if ( Poco::File( _path ).exists() )
        {
            Poco::File( _path ).remove( true );
        }

    Poco::File( _path ).createDirectories();

    categorical().save( _path + "categorical" );

    discrete().save( _path + "discrete" );

    for ( SQLNET_SIZE i = 0; i < join_keys_.size(); ++i )
        {
            join_keys_[i].save( _path + "join_key_" + std::to_string( i ) );
        }

    numerical().save( _path + "numerical" );

    targets().save( _path + "targets" );

    for ( SQLNET_SIZE i = 0; i < time_stamps_.size(); ++i )
        {
            time_stamps_[i].save(
                _path + "time_stamps_" + std::to_string( i ) );
        }
}

// ----------------------------------------------------------------------------

#ifdef SQLNET_MULTINODE_MPI

void DataFrame::save_non_root()
{
    categorical().save();

    discrete().save();

    for ( auto &jk : join_keys_ )
        {
            jk.save();
        }

    numerical().save();

    targets().save();

    for ( auto &ts : time_stamps_ )
        {
            ts.save();
        }
}

#endif  // SQLNET_MULTINODE_MPI

// ----------------------------------------------------------------------------

Poco::JSON::Object DataFrame::to_monitor( const std::string _name )
{
    Poco::JSON::Object obj;

    // ---------------------------------------------------------------------

    obj.set(
        "categorical_", JSON::vector_to_array( *categorical().colnames() ) );

    obj.set(
        "categorical_units_", JSON::vector_to_array( *categorical().units() ) );

    obj.set( "discrete_", JSON::vector_to_array( *discrete().colnames() ) );

    obj.set( "discrete_units_", JSON::vector_to_array( *discrete().units() ) );

    {
        Poco::JSON::Array join_keys;

        for ( auto &jk : join_keys_ )
            {
                join_keys.add( jk.colname( 0 ) );
            }

        obj.set( "join_keys_", join_keys );
    }

    obj.set( "name_", _name );

    obj.set( "num_categorical_", categorical().ncols() );

    obj.set( "num_discrete_", discrete().ncols() );

    obj.set( "num_join_keys_", num_join_keys() );

    obj.set( "num_numerical_", numerical().ncols() );

    obj.set( "num_rows_", categorical().nrows() );

    obj.set( "num_targets_", targets().ncols() );

    obj.set( "num_time_stamps_", num_time_stamps() );

    obj.set( "numerical_", JSON::vector_to_array( *numerical().colnames() ) );

    obj.set(
        "numerical_units_", JSON::vector_to_array( *numerical().units() ) );

    obj.set( "size_", static_cast<SQLNET_FLOAT>( nbytes() ) / 1000000.0 );

    obj.set( "targets_", JSON::vector_to_array( *targets().colnames() ) );

    {
        Poco::JSON::Array time_stamps;

        for ( auto &ts : time_stamps_ )
            {
                time_stamps.add( ts.colname( 0 ) );
            }

        obj.set( "time_stamps_", time_stamps );
    }

    // ---------------------------------------------------------------------

    obj.set( "summary_", Summarizer::summarize( *this ) );

    // ---------------------------------------------------------------------

    return obj;
}

// ----------------------------------------------------------------------------

std::string DataFrame::to_time_stamp(
    const SQLNET_FLOAT &_time_stamp_float ) const
{
    if ( std::isnan( _time_stamp_float ) )
        {
            return "NULL";
        }

    const std::chrono::time_point<std::chrono::system_clock> epoch_point;

    const auto seconds_since_epoch =
        static_cast<std::time_t>( 86400.0 * _time_stamp_float );

    const auto time_stamp = std::chrono::system_clock::to_time_t(
        epoch_point + std::chrono::seconds( seconds_since_epoch ) );

    return std::ctime( &time_stamp );
}

// ----------------------------------------------------------------------------
}  // namespace containers
}  // namespace autosql
