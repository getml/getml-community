
#include "engine/containers/containers.hpp"

namespace engine
{
namespace containers
{
// ----------------------------------------------------------------------------

void DataFrame::add_float_column(
    const Column<Float> &_col, const std::string &_role )
{
    if ( _role == "discrete" )
        {
            add_column( _col, &discretes_ );
        }
    else if ( _role == "numerical" )
        {
            add_column( _col, &numericals_ );
        }
    else if ( _role == "target" )
        {
            add_column( _col, &targets_ );
        }
    else if ( _role == "time_stamp" )
        {
            add_column( _col, &time_stamps_ );
        }
    else
        {
            throw std::invalid_argument( "Role for float column not known!" );
        }
}

// ----------------------------------------------------------------------------

void DataFrame::add_float_vectors(
    const std::vector<std::string> &_names,
    const std::vector<std::shared_ptr<std::vector<Float>>> &_vectors,
    const std::string &_role )
{
    assert( _names.size() == _vectors.size() );

    for ( size_t i = 0; i < _vectors.size(); ++i )
        {
            assert( _vectors[i] );

            auto col = Column<Float>( _vectors[i]->size(), _vectors[i] );

            col.set_name( _names[i] );

            add_float_column( col, _role );
        }
}

// ----------------------------------------------------------------------------

void DataFrame::add_int_column(
    const Column<Int> &_col, const std::string _role )
{
    if ( _role == "categorical" )
        {
            add_column( _col, &categoricals_ );
        }
    else if ( _role == "join_key" )
        {
            const auto num_join_keys = join_keys_.size();

            add_column( _col, &join_keys_ );

            if ( join_keys_.size() != num_join_keys )
                {
                    indices_.push_back( DataFrameIndex() );

                    create_indices();
                }
        }
    else
        {
            throw std::invalid_argument( "Role for int column not known!" );
        }
}

// ----------------------------------------------------------------------------

void DataFrame::add_int_vectors(
    const std::vector<std::string> &_names,
    const std::vector<std::shared_ptr<std::vector<Int>>> &_vectors,
    const std::string &_role )
{
    assert( _names.size() == _vectors.size() );

    for ( size_t i = 0; i < _vectors.size(); ++i )
        {
            assert( _vectors[i] );

            auto col = Column<Int>( _vectors[i]->size(), _vectors[i] );

            col.set_name( _names[i] );

            add_int_column( col, _role );
        }
}

// ----------------------------------------------------------------------------

void DataFrame::append( const DataFrame &_other )
{
    // -------------------------------------------------------------------------

    if ( categoricals_.size() != _other.categoricals_.size() )
        {
            throw std::invalid_argument(
                "Append: Number of categorical columns does not match!" );
        }

    if ( discretes_.size() != _other.discretes_.size() )
        {
            throw std::invalid_argument(
                "Append: Number of discrete columns does not match!" );
        }

    if ( join_keys_.size() != _other.join_keys_.size() )
        {
            throw std::invalid_argument(
                "Append: Number of join keys does not match!" );
        }

    if ( numericals_.size() != _other.numericals_.size() )
        {
            throw std::invalid_argument(
                "Append: Number of numerical columns does not match!" );
        }

    if ( targets_.size() != _other.targets_.size() )
        {
            throw std::invalid_argument(
                "Append: Number of targets does not match!" );
        }

    if ( time_stamps_.size() != _other.time_stamps_.size() )
        {
            throw std::invalid_argument(
                "Append: Number of time stamps does not match!" );
        }

    // -------------------------------------------------------------------------

    for ( size_t i = 0; i < categoricals_.size(); ++i )
        {
            categoricals_[i].append( _other.categorical( i ) );
        }

    for ( size_t i = 0; i < discretes_.size(); ++i )
        {
            discretes_[i].append( _other.discrete( i ) );
        }

    for ( size_t i = 0; i < join_keys_.size(); ++i )
        {
            join_keys_[i].append( _other.join_key( i ) );
        }

    for ( size_t i = 0; i < numericals_.size(); ++i )
        {
            numericals_[i].append( _other.numerical( i ) );
        }

    for ( size_t i = 0; i < targets_.size(); ++i )
        {
            targets_[i].append( _other.target( i ) );
        }

    for ( size_t i = 0; i < time_stamps_.size(); ++i )
        {
            time_stamps_[i].append( _other.time_stamp( i ) );
        }

    // -------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DataFrame::check_plausibility() const
{
    // -------------------------------------------------------------------------

    auto expected_nrows = nrows();

    const bool any_categorical_does_not_match = std::any_of(
        categoricals_.begin(),
        categoricals_.end(),
        [expected_nrows]( const Column<Int> &mat ) {
            return mat.nrows() != expected_nrows;
        } );

    const bool any_discrete_does_not_match = std::any_of(
        discretes_.begin(),
        discretes_.end(),
        [expected_nrows]( const Column<Float> &mat ) {
            return mat.nrows() != expected_nrows;
        } );

    const bool any_join_key_does_not_match = std::any_of(
        join_keys_.begin(),
        join_keys_.end(),
        [expected_nrows]( const Column<Int> &mat ) {
            return mat.nrows() != expected_nrows;
        } );

    const bool any_numerical_does_not_match = std::any_of(
        numericals_.begin(),
        numericals_.end(),
        [expected_nrows]( const Column<Float> &mat ) {
            return mat.nrows() != expected_nrows;
        } );

    const bool any_target_does_not_match = std::any_of(
        targets_.begin(),
        targets_.end(),
        [expected_nrows]( const Column<Float> &mat ) {
            return mat.nrows() != expected_nrows;
        } );

    const bool any_time_stamp_does_not_match = std::any_of(
        time_stamps_.begin(),
        time_stamps_.end(),
        [expected_nrows]( const Column<Float> &mat ) {
            return mat.nrows() != expected_nrows;
        } );

    // -------------------------------------------------------------------------

    const bool any_mismatch =
        any_categorical_does_not_match || any_discrete_does_not_match ||
        any_join_key_does_not_match || any_numerical_does_not_match ||
        any_target_does_not_match || any_time_stamp_does_not_match;

    if ( any_mismatch )
        {
            throw std::runtime_error(
                "Something went very wrong during data loading: The number of "
                "rows in different elements of " +
                name() +
                " do not "
                "match!" );
        }

    // -------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::vector<std::string> DataFrame::concat_colnames(
    const std::vector<std::string> &_categorical_names,
    const std::vector<std::string> &_discrete_names,
    const std::vector<std::string> &_join_key_names,
    const std::vector<std::string> &_numerical_names,
    const std::vector<std::string> &_target_names,
    const std::vector<std::string> &_time_stamp_names ) const
{
    auto all_colnames = std::vector<std::string>( 0 );

    all_colnames.insert(
        all_colnames.end(),
        _categorical_names.begin(),
        _categorical_names.end() );

    all_colnames.insert(
        all_colnames.end(), _discrete_names.begin(), _discrete_names.end() );

    all_colnames.insert(
        all_colnames.end(), _join_key_names.begin(), _join_key_names.end() );

    all_colnames.insert(
        all_colnames.end(), _numerical_names.begin(), _numerical_names.end() );

    all_colnames.insert(
        all_colnames.end(), _target_names.begin(), _target_names.end() );

    all_colnames.insert(
        all_colnames.end(),
        _time_stamp_names.begin(),
        _time_stamp_names.end() );

    return all_colnames;
}

// ----------------------------------------------------------------------------

void DataFrame::create_indices()
{
    if ( indices().size() != join_keys().size() )
        {
            indices().resize( join_keys().size() );
        }

    for ( size_t i = 0; i < join_keys().size(); ++i )
        {
            index( i ).calculate( join_key( i ) );
        }
}

// ----------------------------------------------------------------------------

const Column<Float> &DataFrame::float_column(
    const std::string &_role, const size_t _num ) const
{
    if ( _role == "discrete" )
        {
            return discrete( _num );
        }
    else if ( _role == "numerical" )
        {
            return numerical( _num );
        }
    else if ( _role == "target" )
        {
            return target( _num );
        }
    else if ( _role == "time_stamp" )
        {
            return time_stamp( _num );
        }

    throw std::invalid_argument( "Role '" + _role + "' not known!" );
}

// ----------------------------------------------------------------------------

const Column<Float> &DataFrame::float_column(
    const std::string &_name, const std::string &_role ) const
{
    if ( _role == "discrete" )
        {
            return discrete( _name );
        }
    else if ( _role == "numerical" )
        {
            return numerical( _name );
        }
    else if ( _role == "target" )
        {
            return target( _name );
        }
    else if ( _role == "time_stamp" )
        {
            return time_stamp( _name );
        }

    throw std::invalid_argument( "Role '" + _role + "' not known!" );
}

// ----------------------------------------------------------------------------

void DataFrame::from_csv(
    const std::string &_fname,
    const std::string &_quotechar,
    const std::string &_sep,
    const std::vector<std::string> &_time_formats,
    const std::vector<std::string> &_categorical_names,
    const std::vector<std::string> &_discrete_names,
    const std::vector<std::string> &_join_key_names,
    const std::vector<std::string> &_numerical_names,
    const std::vector<std::string> &_target_names,
    const std::vector<std::string> &_time_stamp_names )
{
    // ------------------------------------------------------------------------

    if ( _quotechar.size() != 1 )
        {
            throw std::invalid_argument(
                "The quotechar must contain exactly one characeter!" );
        }

    if ( _sep.size() != 1 )
        {
            throw std::invalid_argument(
                "The separator must contain exactly one characeter!" );
        }

    auto reader = csv::Reader( _fname, _quotechar[0], _sep[0] );

    const auto csv_colnames = reader.next_line();

    // ------------------------------------------------------------------------

    auto categoricals = make_vectors<Int>( _categorical_names.size() );

    auto discretes = make_vectors<Float>( _discrete_names.size() );

    auto join_keys = make_vectors<Int>( _join_key_names.size() );

    auto numericals = make_vectors<Float>( _numerical_names.size() );

    auto targets = make_vectors<Float>( _target_names.size() );

    auto time_stamps = make_vectors<Float>( _time_stamp_names.size() );

    // ------------------------------------------------------------------------
    // Define column_indices.

    const auto df_colnames = concat_colnames(
        _categorical_names,
        _discrete_names,
        _join_key_names,
        _numerical_names,
        _target_names,
        _time_stamp_names );

    auto colname_indices = std::vector<size_t>( 0 );

    for ( const auto &colname : df_colnames )
        {
            const auto it =
                std::find( csv_colnames.begin(), csv_colnames.end(), colname );

            if ( it == csv_colnames.end() )
                {
                    throw std::runtime_error(
                        "'" + _fname + "' contains no column named '" +
                        colname + "'." );
                }

            colname_indices.push_back( static_cast<size_t>(
                std::distance( csv_colnames.begin(), it ) ) );
        }

    // ------------------------------------------------------------------------
    // Define lambda expressions.

    const auto to_double = [_time_formats]( const std::string &_str ) {
        auto [val, success] = csv::Parser::to_double( _str );

        if ( success )
            {
                return val;
            }

        std::tie( val, success ) =
            csv::Parser::to_time_stamp( _str, _time_formats );

        if ( success )
            {
                return val;
            }

        return static_cast<Float>( NAN );
    };

    // ------------------------------------------------------------------------
    // Read CSV file content into the vectors

    size_t line_count = 1;

    while ( !reader.eof() )
        {
            const auto line = reader.next_line();

            ++line_count;

            if ( line.size() == 0 )
                {
                    continue;
                }
            else if ( line.size() != csv_colnames.size() )
                {
                    std::cout << "Corrupted line: " << line_count
                              << ". Expected " << csv_colnames.size()
                              << " fields, saw " << line.size() << "."
                              << std::endl;
                    continue;
                }

            size_t col = 0;

            for ( auto &vec : categoricals )
                vec->push_back(
                    ( *categories_ )[line[colname_indices[col++]]] );

            for ( auto &vec : discretes )
                vec->push_back( to_double( line[colname_indices[col++]] ) );

            for ( auto &vec : join_keys )
                vec->push_back(
                    ( *join_keys_encoding_ )[line[colname_indices[col++]]] );

            for ( auto &vec : numericals )
                vec->push_back( to_double( line[colname_indices[col++]] ) );

            for ( auto &vec : targets )
                vec->push_back( to_double( line[colname_indices[col++]] ) );

            for ( auto &vec : time_stamps )
                vec->push_back( to_double( line[colname_indices[col++]] ) );
        }

    // ------------------------------------------------------------------------

    auto df = DataFrame( name(), categories_, join_keys_encoding_ );

    df.add_int_vectors( _categorical_names, categoricals, "categorical" );

    df.add_float_vectors( _discrete_names, discretes, "discrete" );

    df.add_int_vectors( _join_key_names, join_keys, "join_key" );

    df.add_float_vectors( _numerical_names, numericals, "numerical" );

    df.add_float_vectors( _target_names, targets, "target" );

    df.add_float_vectors( _time_stamp_names, time_stamps, "time_stamp" );

    // ------------------------------------------------------------------------

    df.check_plausibility();

    // ------------------------------------------------------------------------

    *this = std::move( df );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DataFrame::from_csv(
    const std::vector<std::string> &_fnames,
    const std::string &_quotechar,
    const std::string &_sep,
    const std::vector<std::string> &_time_formats,
    const std::vector<std::string> &_categorical_names,
    const std::vector<std::string> &_discrete_names,
    const std::vector<std::string> &_join_key_names,
    const std::vector<std::string> &_numerical_names,
    const std::vector<std::string> &_target_names,
    const std::vector<std::string> &_time_stamp_names )
{
    auto df = containers::DataFrame( name(), categories_, join_keys_encoding_ );

    for ( size_t i = 0; i < _fnames.size(); ++i )
        {
            auto local_df = containers::DataFrame(
                name(), categories_, join_keys_encoding_ );

            local_df.from_csv(
                _fnames[i],
                _quotechar,
                _sep,
                _time_formats,
                _categorical_names,
                _discrete_names,
                _join_key_names,
                _numerical_names,
                _target_names,
                _time_stamp_names );

            if ( i == 0 )
                {
                    df = std::move( local_df );
                }
            else
                {
                    df.append( local_df );
                }
        }

    *this = std::move( df );
}

// ----------------------------------------------------------------------------

void DataFrame::from_db(
    const std::shared_ptr<database::Connector> _connector,
    const std::string &_tname,
    const std::vector<std::string> &_categorical_names,
    const std::vector<std::string> &_discrete_names,
    const std::vector<std::string> &_join_key_names,
    const std::vector<std::string> &_numerical_names,
    const std::vector<std::string> &_target_names,
    const std::vector<std::string> &_time_stamp_names )
{
    // ----------------------------------------

    auto categoricals = make_vectors<Int>( _categorical_names.size() );

    auto discretes = make_vectors<Float>( _discrete_names.size() );

    auto join_keys = make_vectors<Int>( _join_key_names.size() );

    auto numericals = make_vectors<Float>( _numerical_names.size() );

    auto targets = make_vectors<Float>( _target_names.size() );

    auto time_stamps = make_vectors<Float>( _time_stamp_names.size() );

    // ----------------------------------------

    const auto all_colnames = concat_colnames(
        _categorical_names,
        _discrete_names,
        _join_key_names,
        _numerical_names,
        _target_names,
        _time_stamp_names );

    // ----------------------------------------

    auto iterator = _connector->select( all_colnames, _tname, "" );

    while ( !iterator->end() )
        {
            for ( auto &vec : categoricals )
                vec->push_back( ( *categories_ )[iterator->get_string()] );

            for ( auto &vec : discretes )
                vec->push_back( iterator->get_double() );

            for ( auto &vec : join_keys )
                vec->push_back(
                    ( *join_keys_encoding_ )[iterator->get_string()] );

            for ( auto &vec : numericals )
                vec->push_back( iterator->get_double() );

            for ( auto &vec : targets )
                vec->push_back( iterator->get_double() );

            for ( auto &vec : time_stamps )
                vec->push_back( iterator->get_time_stamp() );
        }

    // ----------------------------------------

    auto df = DataFrame( name(), categories_, join_keys_encoding_ );

    df.add_int_vectors( _categorical_names, categoricals, "categorical" );

    df.add_float_vectors( _discrete_names, discretes, "discrete" );

    df.add_int_vectors( _join_key_names, join_keys, "join_key" );

    df.add_float_vectors( _numerical_names, numericals, "numerical" );

    df.add_float_vectors( _target_names, targets, "target" );

    df.add_float_vectors( _time_stamp_names, time_stamps, "time_stamp" );

    // ----------------------------------------

    df.check_plausibility();

    // ----------------------------------------

    *this = std::move( df );

    // ----------------------------------------
}

// ----------------------------------------------------------------------------

void DataFrame::from_json(
    const Poco::JSON::Object &_obj,
    const std::vector<std::string> _time_formats,
    const std::vector<std::string> &_categorical_names,
    const std::vector<std::string> &_discrete_names,
    const std::vector<std::string> &_join_key_names,
    const std::vector<std::string> &_numerical_names,
    const std::vector<std::string> &_target_names,
    const std::vector<std::string> &_time_stamp_names )
{
    // ----------------------------------------

    auto df = DataFrame( name(), categories_, join_keys_encoding_ );

    df.from_json( _obj, _categorical_names, "categorical", categories_.get() );

    df.from_json( _obj, _discrete_names, "discrete" );

    df.from_json(
        _obj, _join_key_names, "join_key", join_keys_encoding_.get() );

    df.from_json( _obj, _numerical_names, "numerical" );

    df.from_json( _obj, _target_names, "target" );

    df.from_json( _obj, _time_stamp_names, _time_formats );

    // ----------------------------------------

    df.check_plausibility();

    // ----------------------------------------

    *this = std::move( df );

    // ----------------------------------------
}

// ----------------------------------------------------------------------------

void DataFrame::from_json(
    const Poco::JSON::Object &_obj,
    const std::vector<std::string> &_names,
    const std::string &_type,
    Encoding *_encoding )
{
    for ( size_t i = 0; i < _names.size(); ++i )
        {
            const auto &name = _names[i];

            const auto arr = JSON::get_array( _obj, name );

            auto column = Column<Int>( arr->size() );

            for ( size_t j = 0; j < arr->size(); ++j )
                {
                    column[j] = ( *_encoding )[arr->getElement<std::string>(
                        static_cast<unsigned int>( j ) )];
                }

            column.set_name( _names[i] );

            add_int_column( column, _type );
        }
}

// ----------------------------------------------------------------------------

void DataFrame::from_json(
    const Poco::JSON::Object &_obj,
    const std::vector<std::string> &_names,
    const std::string &_type )
{
    for ( size_t i = 0; i < _names.size(); ++i )
        {
            const auto &name = _names[i];

            if ( _type == "target" && !_obj.has( name ) )
                {
                    continue;
                }

            const auto arr = JSON::get_array( _obj, name );

            auto column = Column<Float>( arr->size() );

            for ( size_t j = 0; j < arr->size(); ++j )
                {
                    column[j] = arr->getElement<Float>(
                        static_cast<unsigned int>( j ) );
                }

            column.set_name( _names[i] );

            add_float_column( column, _type );
        }
}

// ----------------------------------------------------------------------------

void DataFrame::from_json(
    const Poco::JSON::Object &_obj,
    const std::vector<std::string> &_names,
    const std::vector<std::string> &_time_formats )
{
    for ( size_t i = 0; i < _names.size(); ++i )
        {
            const auto &name = _names[i];

            const auto arr = JSON::get_array( _obj, name );

            auto column = Column<Float>( arr->size() );

            for ( size_t j = 0; j < arr->size(); ++j )
                {
                    bool success = true;

                    std::tie( column[j], success ) = csv::Parser::to_time_stamp(
                        arr->getElement<std::string>(
                            static_cast<unsigned int>( j ) ),
                        _time_formats );

                    if ( !success )
                        {
                            column[j] = arr->getElement<Float>(
                                static_cast<unsigned int>( j ) );
                        }
                }

            column.set_name( _names[i] );

            add_float_column( column, "time_stamp" );
        }
}

// ----------------------------------------------------------------------------

Poco::JSON::Object DataFrame::get_colnames()
{
    // ----------------------------------------

    Poco::JSON::Object obj;

    // ----------------------------------------

    obj.set( "categorical_", get_colnames( categoricals_ ) );

    obj.set( "discrete_", get_colnames( discretes_ ) );

    obj.set( "join_keys_", get_colnames( join_keys_ ) );

    obj.set( "numerical_", get_colnames( numericals_ ) );

    obj.set( "targets_", get_colnames( targets_ ) );

    obj.set( "time_stamps_", get_colnames( time_stamps_ ) );

    // ----------------------------------------

    return obj;

    // ----------------------------------------
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
                    row.add( to_time_stamp( time_stamp( j )[i] ) );
                }

            for ( size_t j = 0; j < num_join_keys(); ++j )
                {
                    row.add( join_keys_encoding()[join_key( j )[i]] );
                }

            for ( size_t j = 0; j < num_targets(); ++j )
                {
                    row.add( to_string( target( j )[i] ) );
                }

            for ( size_t j = 0; j < num_categoricals(); ++j )
                {
                    row.add( categories()[categorical( j )[i]] );
                }

            for ( size_t j = 0; j < num_discretes(); ++j )
                {
                    if ( discrete( j ).unit().find( "time stamp" ) !=
                         std::string::npos )
                        {
                            row.add( to_time_stamp( discrete( j )[i] ) );
                        }
                    else
                        {
                            row.add( to_string( discrete( j )[i] ) );
                        }
                }

            for ( size_t j = 0; j < num_numericals(); ++j )
                {
                    if ( numerical( j ).unit().find( "time stamp" ) !=
                         std::string::npos )
                        {
                            row.add( to_time_stamp( numerical( j )[i] ) );
                        }
                    else
                        {
                            row.add( to_string( numerical( j )[i] ) );
                        }
                }

            data.add( row );
        }

    obj.set( "data", data );

    // ----------------------------------------

    return obj;

    // ----------------------------------------
}

// ----------------------------------------------------------------------------

const Column<Int> &DataFrame::int_column(
    const std::string &_role, const size_t _num ) const
{
    if ( _role == "categorical" )
        {
            return categorical( _num );
        }
    else if ( _role == "join_key" )
        {
            return join_key( _num );
        }

    throw std::invalid_argument( "Role '" + _role + "' not known!" );
}

// ----------------------------------------------------------------------------

const Column<Int> &DataFrame::int_column(
    const std::string &_name, const std::string &_role ) const
{
    if ( _role == "categorical" )
        {
            return categorical( _name );
        }
    else if ( _role == "join_key" )
        {
            return join_key( _name );
        }

    throw std::invalid_argument( "Role '" + _role + "' not known!" );
}

// ----------------------------------------------------------------------------

void DataFrame::load( const std::string &_path )
{
    //---------------------------------------------------------------------
    // Make sure that the _path exists and is directory

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

    // ---------------------------------------------------------------------
    // Load contents.

    categoricals_ = load_matrices<Int>( _path, "categorical_" );

    discretes_ = load_matrices<Float>( _path, "discrete_" );

    join_keys_ = load_matrices<Int>( _path, "join_key_" );

    numericals_ = load_matrices<Float>( _path, "numerical_" );

    targets_ = load_matrices<Float>( _path, "target_" );

    time_stamps_ = load_matrices<Float>( _path, "time_stamp_" );

    // ---------------------------------------------------------------------

    check_plausibility();

    // ---------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

ULong DataFrame::nbytes() const
{
    ULong nbytes = 0;

    nbytes += calc_nbytes( categoricals_ );

    nbytes += calc_nbytes( discretes_ );

    nbytes += calc_nbytes( join_keys_ );

    nbytes += calc_nbytes( numericals_ );

    nbytes += calc_nbytes( targets_ );

    nbytes += calc_nbytes( time_stamps_ );

    return nbytes;
}

// ----------------------------------------------------------------------------

const size_t DataFrame::nrows() const
{
    if ( join_keys_.size() > 0 )
        {
            return join_keys_[0].nrows();
        }
    else if ( time_stamps_.size() > 0 )
        {
            return time_stamps_[0].nrows();
        }
    else if ( categoricals_.size() > 0 )
        {
            return categoricals_[0].nrows();
        }
    else if ( discretes_.size() > 0 )
        {
            return discretes_[0].nrows();
        }
    else if ( numericals_.size() > 0 )
        {
            return numericals_[0].nrows();
        }
    else if ( targets_.size() > 0 )
        {
            return targets_[0].nrows();
        }
    else
        {
            return 0;
        }
}

// ----------------------------------------------------------------------------

void DataFrame::remove_column(
    const std::string &_name, const std::string &_role )
{
    if ( _role == "categorical" )
        {
            rm_col( _name, &categoricals_ );
        }
    else if ( _role == "discrete" )
        {
            rm_col( _name, &discretes_ );
        }
    else if ( _role == "join_key" )
        {
            rm_col( _name, &join_keys_, &indices_ );
        }
    else if ( _role == "numerical" )
        {
            rm_col( _name, &numericals_ );
        }
    else if ( _role == "target" )
        {
            rm_col( _name, &targets_ );
        }
    else if ( _role == "time_stamp" )
        {
            rm_col( _name, &time_stamps_ );
        }
    else
        {
            throw std::invalid_argument( "Role for float column not known!" );
        }
}

// ----------------------------------------------------------------------------

void DataFrame::save( const std::string &_path )
{
    // ---------------------------------------------------------------------
    // If the path already exists, delete it to avoid
    // conflicts with already existing files.

    if ( Poco::File( _path ).exists() )
        {
            Poco::File( _path ).remove( true );
        }

    Poco::File( _path ).createDirectories();

    // ---------------------------------------------------------------------

    save_matrices( categoricals_, _path, "categorical_" );

    save_matrices( discretes_, _path, "discrete_" );

    save_matrices( join_keys_, _path, "join_key_" );

    save_matrices( numericals_, _path, "numerical_" );

    save_matrices( targets_, _path, "target_" );

    save_matrices( time_stamps_, _path, "time_stamp_" );

    // ---------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

Poco::JSON::Object DataFrame::to_monitor() const
{
    // ---------------------------------------------------------------------

    Poco::JSON::Object obj;

    // ---------------------------------------------------------------------

    obj.set( "categorical_", get_colnames( categoricals_ ) );

    obj.set( "categorical_units_", get_units( categoricals_ ) );

    obj.set( "discrete_", get_colnames( discretes_ ) );

    obj.set( "discrete_units_", get_units( discretes_ ) );

    obj.set( "join_keys_", get_colnames( join_keys_ ) );

    obj.set( "name_", name_ );

    obj.set( "num_categorical_", num_categoricals() );

    obj.set( "num_discrete_", num_discretes() );

    obj.set( "num_join_keys_", num_join_keys() );

    obj.set( "num_numerical_", num_numericals() );

    obj.set( "num_rows_", nrows() );

    obj.set( "num_targets_", num_targets() );

    obj.set( "num_time_stamps_", num_time_stamps() );

    obj.set( "numerical_", get_colnames( numericals_ ) );

    obj.set( "numerical_units_", get_units( numericals_ ) );

    obj.set( "size_", static_cast<Float>( nbytes() ) / 1000000.0 );

    obj.set( "targets_", get_colnames( targets_ ) );

    obj.set( "time_stamps_", get_colnames( time_stamps_ ) );

    // ---------------------------------------------------------------------

    return obj;

    // ---------------------------------------------------------------------*/
}

// ----------------------------------------------------------------------------

std::string DataFrame::to_time_stamp( const Float &_time_stamp_float ) const
{
    if ( std::isnan( _time_stamp_float ) || std::isinf( _time_stamp_float ) )
        {
            return "NULL";
        }

    const std::chrono::time_point<std::chrono::system_clock> epoch_point;

    const auto seconds_since_epoch =
        static_cast<std::time_t>( 86400.0 * _time_stamp_float );

    const auto time_stamp = std::chrono::system_clock::to_time_t(
        epoch_point + std::chrono::seconds( seconds_since_epoch ) );

    return std::asctime( std::gmtime( &time_stamp ) );
}

// ----------------------------------------------------------------------------

void DataFrame::where( const std::vector<bool> &_condition )
{
    auto df = DataFrame( name(), categories_, join_keys_encoding_ );

    for ( size_t i = 0; i < num_categoricals(); ++i )
        {
            df.add_int_column(
                categorical( i ).where( _condition ), "categorical" );
        }

    for ( size_t i = 0; i < num_discretes(); ++i )
        {
            df.add_float_column(
                discrete( i ).where( _condition ), "discrete" );
        }

    for ( size_t i = 0; i < num_join_keys(); ++i )
        {
            df.add_int_column( join_key( i ).where( _condition ), "join_key" );
        }

    for ( size_t i = 0; i < num_numericals(); ++i )
        {
            df.add_float_column(
                numerical( i ).where( _condition ), "numerical" );
        }

    for ( size_t i = 0; i < num_targets(); ++i )
        {
            df.add_float_column( target( i ).where( _condition ), "target" );
        }

    for ( size_t i = 0; i < num_time_stamps(); ++i )
        {
            df.add_float_column(
                time_stamp( i ).where( _condition ), "time_stamp" );
        }

    df.create_indices();

    *this = std::move( df );
}

// ----------------------------------------------------------------------------
}  // namespace containers
}  // namespace engine
