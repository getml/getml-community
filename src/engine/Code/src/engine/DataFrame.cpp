#include "engine/containers/containers.hpp"

namespace engine
{
namespace containers
{
// ----------------------------------------------------------------------------

void DataFrame::add_float_column(
    const Column<Float> &_col, const std::string &_role )
{
    if ( _role == ROLE_NUMERICAL )
        {
            add_column( _col, &numericals_ );
            update_last_change();
        }
    else if ( _role == ROLE_TARGET )
        {
            check_null( _col );
            add_column( _col, &targets_ );
            update_last_change();
        }
    else if ( _role == ROLE_TIME_STAMP )
        {
            auto col = _col;
            if ( col.unit() == "" )
                col.set_unit( "time stamp, comparison only" );
            add_column( col, &time_stamps_ );
            update_last_change();
        }
    else if ( _role == ROLE_UNUSED || _role == ROLE_UNUSED_FLOAT )
        {
            add_column( _col, &unused_floats_ );
            update_last_change();
        }
    else
        {
            throw std::invalid_argument(
                "Role '" + _role + "' for float column not known!" );
        }
}

// ----------------------------------------------------------------------------

void DataFrame::add_float_vectors(
    const std::vector<std::string> &_names,
    const std::vector<std::shared_ptr<std::vector<Float>>> &_vectors,
    const std::string &_role )
{
    assert_true( _names.size() == _vectors.size() );

    for ( size_t i = 0; i < _vectors.size(); ++i )
        {
            assert_true( _vectors[i] );

            auto col = Column<Float>( _vectors[i] );

            col.set_name( _names[i] );

            add_float_column( col, _role );
        }
}

// ----------------------------------------------------------------------------

void DataFrame::add_int_column(
    const Column<Int> &_col, const std::string _role )
{
    if ( _role == ROLE_CATEGORICAL )
        {
            add_column( _col, &categoricals_ );
            update_last_change();
        }
    else if ( _role == ROLE_JOIN_KEY )
        {
            add_column( _col, &join_keys_ );

            assert_true( join_keys_.size() == indices_.size() + 1 );

            indices_.push_back( DataFrameIndex() );

            create_indices();

            update_last_change();
        }
    else
        {
            throw std::invalid_argument(
                "Role '" + _role + "' for int column not known!" );
        }
}

// ----------------------------------------------------------------------------

void DataFrame::add_int_vectors(
    const std::vector<std::string> &_names,
    const std::vector<std::shared_ptr<std::vector<Int>>> &_vectors,
    const std::string &_role )
{
    assert_true( _names.size() == _vectors.size() );

    for ( size_t i = 0; i < _vectors.size(); ++i )
        {
            assert_true( _vectors[i] );

            auto col = Column<Int>( _vectors[i] );

            col.set_name( _names[i] );

            add_int_column( col, _role );
        }
}

// ----------------------------------------------------------------------------

void DataFrame::add_string_column(
    const Column<strings::String> &_col, const std::string &_role )
{
    if ( _role == ROLE_TEXT )
        {
            add_column( _col, &text_ );
            update_last_change();
        }
    else if ( _role == ROLE_UNUSED_STRING )
        {
            add_column( _col, &unused_strings_ );
            update_last_change();
        }
    else
        {
            throw std::invalid_argument(
                "Role '" + _role + "' for string column not known!" );
        }
}

// ----------------------------------------------------------------------------

void DataFrame::add_string_vectors(
    const std::vector<std::string> &_names,
    const std::vector<std::shared_ptr<std::vector<strings::String>>> &_vectors,
    const std::string &_role )
{
    assert_true( _names.size() == _vectors.size() );

    for ( size_t i = 0; i < _vectors.size(); ++i )
        {
            assert_true( _vectors[i] );

            auto col = Column<strings::String>( _vectors[i] );

            col.set_name( _names[i] );

            add_string_column( col, _role );
        }
}

// ----------------------------------------------------------------------------

void DataFrame::append( const DataFrame &_other )
{
    // -------------------------------------------------------------------------

    if ( categoricals_.size() != _other.categoricals_.size() )
        {
            throw std::invalid_argument(
                "Can not append: Number of categorical columns does not "
                "match!" );
        }

    if ( join_keys_.size() != _other.join_keys_.size() )
        {
            throw std::invalid_argument(
                "Can not append: Number of join keys does not match!" );
        }

    if ( numericals_.size() != _other.numericals_.size() )
        {
            throw std::invalid_argument(
                "Can not append: Number of numerical columns does not match!" );
        }

    if ( targets_.size() != _other.targets_.size() )
        {
            throw std::invalid_argument(
                "Can not append: Number of targets does not match!" );
        }

    if ( text_.size() != _other.text_.size() )
        {
            throw std::invalid_argument(
                "Can not append: Number of text columns does not match!" );
        }

    if ( time_stamps_.size() != _other.time_stamps_.size() )
        {
            throw std::invalid_argument(
                "Can not append: Number of time stamps does not match!" );
        }

    if ( unused_floats_.size() != _other.unused_floats_.size() )
        {
            throw std::invalid_argument(
                "Can not append: Number of unused floats does not match!" );
        }

    if ( unused_strings_.size() != _other.unused_strings_.size() )
        {
            throw std::invalid_argument(
                "Can not append: Number of unused integers does not match!" );
        }

    // -------------------------------------------------------------------------

    for ( const auto &col : categoricals_ )
        {
            if ( !_other.has_categorical( col.name() ) )
                {
                    throw std::invalid_argument(
                        "Can not append: Data frame '" + _other.name() +
                        "' has no categorical column named '" + col.name() +
                        "'!" );
                }
        }

    for ( const auto &col : join_keys_ )
        {
            if ( !_other.has_join_key( col.name() ) )
                {
                    throw std::invalid_argument(
                        "Can not append: Data frame '" + _other.name() +
                        "' has no join key named '" + col.name() + "'!" );
                }
        }

    for ( const auto &col : numericals_ )
        {
            if ( !_other.has_numerical( col.name() ) )
                {
                    throw std::invalid_argument(
                        "Can not append: Data frame '" + _other.name() +
                        "' has no numerical column named '" + col.name() +
                        "'!" );
                }
        }

    for ( const auto &col : targets_ )
        {
            if ( !_other.has_target( col.name() ) )
                {
                    throw std::invalid_argument(
                        "Can not append: Data frame '" + _other.name() +
                        "' has no target named '" + col.name() + "'!" );
                }
        }

    for ( const auto &col : text_ )
        {
            if ( !_other.has_text( col.name() ) )
                {
                    throw std::invalid_argument(
                        "Can not append: Data frame '" + _other.name() +
                        "' has no text column named '" + col.name() + "'!" );
                }
        }

    for ( const auto &col : time_stamps_ )
        {
            if ( !_other.has_time_stamp( col.name() ) )
                {
                    throw std::invalid_argument(
                        "Can not append: Data frame '" + _other.name() +
                        "' has no time stamp named '" + col.name() + "'!" );
                }
        }

    for ( const auto &col : unused_floats_ )
        {
            if ( !_other.has_unused_float( col.name() ) )
                {
                    throw std::invalid_argument(
                        "Can not append: Data frame '" + _other.name() +
                        "' has no unused float column named '" + col.name() +
                        "'!" );
                }
        }

    for ( const auto &col : unused_strings_ )
        {
            if ( !_other.has_unused_string( col.name() ) )
                {
                    throw std::invalid_argument(
                        "Can not append: Data frame '" + _other.name() +
                        "' has no unused string column named '" + col.name() +
                        "'!" );
                }
        }

    // -------------------------------------------------------------------------

    for ( auto &col : categoricals_ )
        {
            col.append( _other.categorical( col.name() ) );
        }

    for ( auto &col : join_keys_ )
        {
            col.append( _other.join_key( col.name() ) );
        }

    for ( auto &col : numericals_ )
        {
            col.append( _other.numerical( col.name() ) );
        }

    for ( auto &col : targets_ )
        {
            col.append( _other.target( col.name() ) );
        }

    for ( auto &col : text_ )
        {
            col.append( _other.text( col.name() ) );
        }

    for ( auto &col : time_stamps_ )
        {
            col.append( _other.time_stamp( col.name() ) );
        }

    for ( auto &col : unused_floats_ )
        {
            col.append( _other.unused_float( col.name() ) );
        }

    for ( auto &col : unused_strings_ )
        {
            col.append( _other.unused_string( col.name() ) );
        }

    // -------------------------------------------------------------------------

    create_indices();

    check_plausibility();

    update_last_change();

    // -------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DataFrame::check_null( const Column<Float> &_col ) const
{
    const auto is_nan_or_inf = []( const Float val ) {
        return std::isnan( val ) || std::isinf( val );
    };
    const bool any_null =
        std::any_of( _col.begin(), _col.end(), is_nan_or_inf );

    if ( any_null )
        {
            throw std::invalid_argument(
                "Values in the target column cannot be NULL or "
                "infinite!" );
        }
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

    const bool any_text_does_not_match = std::any_of(
        text_.begin(),
        text_.end(),
        [expected_nrows]( const Column<strings::String> &mat ) {
            return mat.nrows() != expected_nrows;
        } );

    const bool any_time_stamp_does_not_match = std::any_of(
        time_stamps_.begin(),
        time_stamps_.end(),
        [expected_nrows]( const Column<Float> &mat ) {
            return mat.nrows() != expected_nrows;
        } );

    const bool any_undef_float_does_not_match = std::any_of(
        unused_floats_.begin(),
        unused_floats_.end(),
        [expected_nrows]( const Column<Float> &mat ) {
            return mat.nrows() != expected_nrows;
        } );

    const bool any_undef_string_does_not_match = std::any_of(
        unused_strings_.begin(),
        unused_strings_.end(),
        [expected_nrows]( const Column<strings::String> &mat ) {
            return mat.nrows() != expected_nrows;
        } );

    // -------------------------------------------------------------------------

    const bool any_mismatch =
        any_categorical_does_not_match || any_join_key_does_not_match ||
        any_numerical_does_not_match || any_target_does_not_match ||
        any_time_stamp_does_not_match || any_undef_float_does_not_match ||
        any_undef_string_does_not_match || any_text_does_not_match;

    if ( any_mismatch )
        {
            throw std::runtime_error(
                "Something went very wrong during data loading: The number "
                "of "
                "rows in different elements of " +
                name() +
                " do not "
                "match!" );
        }

    // -------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

DataFrame DataFrame::clone( const std::string _name ) const
{
    auto df = DataFrame( _name, categories_, join_keys_encoding_ );

    for ( size_t i = 0; i < num_categoricals(); ++i )
        {
            df.add_int_column( categorical( i ).clone(), ROLE_CATEGORICAL );
        }

    for ( size_t i = 0; i < num_join_keys(); ++i )
        {
            df.add_int_column( join_key( i ).clone(), ROLE_JOIN_KEY );
        }

    for ( size_t i = 0; i < num_numericals(); ++i )
        {
            df.add_float_column( numerical( i ).clone(), ROLE_NUMERICAL );
        }

    for ( size_t i = 0; i < num_targets(); ++i )
        {
            df.add_float_column( target( i ).clone(), ROLE_TARGET );
        }

    for ( size_t i = 0; i < num_text(); ++i )
        {
            df.add_string_column( text( i ).clone(), ROLE_TEXT );
        }

    for ( size_t i = 0; i < num_time_stamps(); ++i )
        {
            df.add_float_column( time_stamp( i ).clone(), ROLE_TIME_STAMP );
        }

    for ( size_t i = 0; i < num_unused_floats(); ++i )
        {
            df.add_float_column( unused_float( i ).clone(), ROLE_UNUSED_FLOAT );
        }

    for ( size_t i = 0; i < num_unused_strings(); ++i )
        {
            df.add_string_column(
                unused_string( i ).clone(), ROLE_UNUSED_STRING );
        }

    df.create_indices();

    df.check_plausibility();

    return df;
}

// ----------------------------------------------------------------------------

std::vector<std::string> DataFrame::concat_colnames(
    const Schema &_schema ) const
{
    auto all_colnames = std::vector<std::string>( 0 );

    all_colnames.insert(
        all_colnames.end(),
        _schema.categoricals_.begin(),
        _schema.categoricals_.end() );

    all_colnames.insert(
        all_colnames.end(),
        _schema.join_keys_.begin(),
        _schema.join_keys_.end() );

    all_colnames.insert(
        all_colnames.end(),
        _schema.numericals_.begin(),
        _schema.numericals_.end() );

    all_colnames.insert(
        all_colnames.end(), _schema.targets_.begin(), _schema.targets_.end() );

    all_colnames.insert(
        all_colnames.end(), _schema.text_.begin(), _schema.text_.end() );

    all_colnames.insert(
        all_colnames.end(),
        _schema.time_stamps_.begin(),
        _schema.time_stamps_.end() );

    all_colnames.insert(
        all_colnames.end(),
        _schema.unused_floats_.begin(),
        _schema.unused_floats_.end() );

    all_colnames.insert(
        all_colnames.end(),
        _schema.unused_strings_.begin(),
        _schema.unused_strings_.end() );

    return all_colnames;
}

// ----------------------------------------------------------------------------

void DataFrame::create_indices()
{
    if ( indices().size() != join_keys().size() )
        {
            indices() = std::vector<DataFrameIndex>( join_keys().size() );
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
    if ( _role == ROLE_NUMERICAL )
        {
            return numerical( _num );
        }
    else if ( _role == ROLE_TARGET )
        {
            return target( _num );
        }
    else if ( _role == ROLE_TIME_STAMP )
        {
            return time_stamp( _num );
        }
    else if ( _role == ROLE_UNUSED || _role == ROLE_UNUSED_FLOAT )
        {
            return unused_float( _num );
        }

    throw std::invalid_argument( "Role '" + _role + "' not known!" );
}

// ----------------------------------------------------------------------------

const Column<Float> &DataFrame::float_column(
    const std::string &_name, const std::string &_role ) const
{
    if ( _role == ROLE_NUMERICAL )
        {
            return numerical( _name );
        }
    else if ( _role == ROLE_TARGET )
        {
            return target( _name );
        }
    else if ( _role == ROLE_TIME_STAMP )
        {
            return time_stamp( _name );
        }
    else if ( _role == ROLE_UNUSED || _role == ROLE_UNUSED_FLOAT )
        {
            return unused_float( _name );
        }

    throw std::invalid_argument( "Role '" + _role + "' not known!" );
}

// ----------------------------------------------------------------------------

void DataFrame::from_csv(
    const std::optional<std::vector<std::string>> &_colnames,
    const std::vector<std::string> &_fnames,
    const std::string &_quotechar,
    const std::string &_sep,
    const size_t _num_lines_read,
    const size_t _skip,
    const std::vector<std::string> &_time_formats,
    const Schema &_schema )
{
    // ------------------------------------------------------------------------

    if ( _quotechar.size() != 1 )
        {
            throw std::invalid_argument(
                "The quotechar must contain exactly one character!" );
        }

    if ( _sep.size() != 1 )
        {
            throw std::invalid_argument(
                "The separator must contain exactly one character!" );
        }

    auto limit =
        ( _num_lines_read > 0 ) ? ( _num_lines_read + _skip ) : _num_lines_read;

    if ( !_colnames && limit > 0 )
        {
            ++limit;
        }

    // ------------------------------------------------------------------------

    auto df = containers::DataFrame( name(), categories_, join_keys_encoding_ );

    for ( size_t i = 0; i < _fnames.size(); ++i )
        {
            auto local_df = containers::DataFrame(
                name(), categories_, join_keys_encoding_ );

            const std::shared_ptr<io::Reader> reader =
                std::make_shared<io::CSVReader>(
                    _colnames, _fnames[i], limit, _quotechar[0], _sep[0] );

            local_df.from_reader(
                reader, _fnames[i], _skip, _time_formats, _schema );

            if ( i == 0 )
                {
                    df = std::move( local_df );
                }
            else
                {
                    df.append( local_df );
                }
        }

    // ------------------------------------------------------------------------

    *this = std::move( df );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DataFrame::from_db(
    const std::shared_ptr<database::Connector> _connector,
    const std::string &_tname,
    const Schema &_schema )
{
    // ----------------------------------------

    auto categoricals = make_vectors<Int>( _schema.categoricals_.size() );

    auto join_keys = make_vectors<Int>( _schema.join_keys_.size() );

    auto numericals = make_vectors<Float>( _schema.numericals_.size() );

    auto targets = make_vectors<Float>( _schema.targets_.size() );

    auto text = make_vectors<strings::String>( _schema.text_.size() );

    auto time_stamps = make_vectors<Float>( _schema.time_stamps_.size() );

    auto unused_floats = make_vectors<Float>( _schema.unused_floats_.size() );

    auto unused_strings =
        make_vectors<strings::String>( _schema.unused_strings_.size() );

    // ----------------------------------------

    const auto all_colnames = concat_colnames( _schema );

    // ----------------------------------------

    auto iterator = _connector->select( all_colnames, _tname, "" );

    while ( !iterator->end() )
        {
            for ( auto &vec : categoricals )
                vec->push_back( ( *categories_ )[iterator->get_string()] );

            for ( auto &vec : join_keys )
                vec->push_back(
                    ( *join_keys_encoding_ )[iterator->get_string()] );

            for ( auto &vec : numericals )
                vec->push_back( iterator->get_double() );

            for ( auto &vec : targets )
                vec->push_back( iterator->get_double() );

            for ( auto &vec : text )
                vec->emplace_back( strings::String( iterator->get_string() ) );

            for ( auto &vec : time_stamps )
                vec->push_back( iterator->get_time_stamp() );

            for ( auto &vec : unused_floats )
                vec->push_back( iterator->get_double() );

            for ( auto &vec : unused_strings )
                vec->emplace_back( strings::String( iterator->get_string() ) );
        }

    // ----------------------------------------

    auto df = DataFrame( name(), categories_, join_keys_encoding_ );

    df.add_int_vectors( _schema.categoricals_, categoricals, ROLE_CATEGORICAL );

    df.add_int_vectors( _schema.join_keys_, join_keys, ROLE_JOIN_KEY );

    df.add_float_vectors( _schema.numericals_, numericals, ROLE_NUMERICAL );

    df.add_float_vectors( _schema.targets_, targets, ROLE_TARGET );

    df.add_string_vectors( _schema.text_, text, ROLE_TEXT );

    df.add_float_vectors( _schema.time_stamps_, time_stamps, ROLE_TIME_STAMP );

    df.add_float_vectors(
        _schema.unused_floats_, unused_floats, ROLE_UNUSED_FLOAT );

    df.add_string_vectors(
        _schema.unused_strings_, unused_strings, ROLE_UNUSED_STRING );

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
    const Schema &_schema )
{
    // ----------------------------------------

    auto df = DataFrame( name(), categories_, join_keys_encoding_ );

    df.from_json(
        _obj, _schema.categoricals_, ROLE_CATEGORICAL, categories_.get() );

    df.from_json(
        _obj, _schema.join_keys_, ROLE_JOIN_KEY, join_keys_encoding_.get() );

    df.from_json( _obj, _schema.numericals_, ROLE_NUMERICAL );

    df.from_json( _obj, _schema.targets_, ROLE_TARGET );

    df.from_json( _obj, _schema.text_, ROLE_TEXT );

    df.from_json( _obj, _schema.time_stamps_, _time_formats );

    df.from_json( _obj, _schema.unused_floats_, ROLE_UNUSED_FLOAT );

    df.from_json( _obj, _schema.unused_strings_, ROLE_UNUSED_STRING );

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
    const std::string &_role,
    Encoding *_encoding )
{
    for ( size_t i = 0; i < _names.size(); ++i )
        {
            const auto &name = _names[i];

            const auto arr = JSON::get_array( _obj, name );

            if ( _encoding )
                {
                    auto column = Column<Int>( arr->size() );

                    for ( size_t j = 0; j < arr->size(); ++j )
                        {
                            const auto str = arr->getElement<std::string>(
                                static_cast<unsigned int>( j ) );

                            column[j] = ( *_encoding )[str];
                        }

                    column.set_name( _names[i] );

                    add_int_column( column, _role );
                }
            else
                {
                    auto column = Column<strings::String>( arr->size() );

                    for ( size_t j = 0; j < arr->size(); ++j )
                        column[j] = arr->getElement<std::string>(
                            static_cast<unsigned int>( j ) );

                    column.set_name( _names[i] );

                    add_string_column( column, _role );
                }
        }
}

// ----------------------------------------------------------------------------

void DataFrame::from_json(
    const Poco::JSON::Object &_obj,
    const std::vector<std::string> &_names,
    const std::string &_role )
{
    for ( size_t i = 0; i < _names.size(); ++i )
        {
            const auto &name = _names[i];

            if ( _role == ROLE_TARGET && !_obj.has( name ) )
                {
                    continue;
                }

            const auto arr = JSON::get_array( _obj, name );

            if ( _role == ROLE_UNUSED || _role == ROLE_UNUSED_STRING ||
                 _role == ROLE_TEXT )
                {
                    auto column = Column<strings::String>( arr->size() );

                    for ( size_t j = 0; j < arr->size(); ++j )
                        {
                            column[j] = arr->getElement<std::string>(
                                static_cast<unsigned int>( j ) );
                        }

                    column.set_name( _names[i] );

                    add_string_column( column, _role );
                }
            else
                {
                    auto column = Column<Float>( arr->size() );

                    for ( size_t j = 0; j < arr->size(); ++j )
                        {
                            column[j] = arr->getElement<Float>(
                                static_cast<unsigned int>( j ) );
                        }

                    column.set_name( _names[i] );

                    add_float_column( column, _role );
                }
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

                    std::tie( column[j], success ) = io::Parser::to_time_stamp(
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

            add_float_column( column, ROLE_TIME_STAMP );
        }
}

// ----------------------------------------------------------------------------

void DataFrame::from_query(
    const std::shared_ptr<database::Connector> _connector,
    const std::string &_query,
    const Schema &_schema )
{
    // ----------------------------------------

    auto categoricals = make_vectors<Int>( _schema.categoricals_.size() );

    auto join_keys = make_vectors<Int>( _schema.join_keys_.size() );

    auto numericals = make_vectors<Float>( _schema.numericals_.size() );

    auto targets = make_vectors<Float>( _schema.targets_.size() );

    auto text = make_vectors<strings::String>( _schema.text_.size() );

    auto time_stamps = make_vectors<Float>( _schema.time_stamps_.size() );

    auto unused_floats = make_vectors<Float>( _schema.unused_floats_.size() );

    auto unused_strings =
        make_vectors<strings::String>( _schema.unused_strings_.size() );

    // ----------------------------------------

    auto iterator = _connector->select( _query );

    const auto iter_colnames = iterator->colnames();

    // ----------------------------------------

    const auto make_column_indices =
        [iter_colnames]( const std::vector<std::string> &_names ) {
            std::vector<size_t> indices;

            for ( const auto &name : _names )
                {
                    const auto it = std::find(
                        iter_colnames.begin(), iter_colnames.end(), name );

                    if ( it == _names.end() )
                        {
                            throw std::invalid_argument(
                                "No column named '" + name + "' in query!" );
                        }

                    indices.push_back( static_cast<size_t>(
                        std::distance( iter_colnames.begin(), it ) ) );
                }

            return indices;
        };

    // ----------------------------------------

    const auto categorical_ix = make_column_indices( _schema.categoricals_ );

    const auto join_key_ix = make_column_indices( _schema.join_keys_ );

    const auto numerical_ix = make_column_indices( _schema.numericals_ );

    const auto target_ix = make_column_indices( _schema.targets_ );

    const auto text_ix = make_column_indices( _schema.text_ );

    const auto time_stamp_ix = make_column_indices( _schema.time_stamps_ );

    const auto unused_float_ix = make_column_indices( _schema.unused_floats_ );

    const auto unused_string_ix =
        make_column_indices( _schema.unused_strings_ );

    // ----------------------------------------

    const auto time_formats = _connector->time_formats();

    while ( !iterator->end() )
        {
            auto line = std::vector<std::string>( iter_colnames.size() );

            for ( auto &field : line )
                {
                    field = iterator->get_string();
                }

            for ( size_t i = 0; i < categoricals.size(); ++i )
                {
                    const auto &str = line[categorical_ix[i]];
                    categoricals[i]->push_back( ( *categories_ )[str] );
                }

            for ( size_t i = 0; i < join_keys.size(); ++i )
                {
                    const auto &str = line[join_key_ix[i]];
                    join_keys[i]->push_back( ( *join_keys_encoding_ )[str] );
                }

            for ( size_t i = 0; i < numericals.size(); ++i )
                {
                    const auto &str = line[numerical_ix[i]];
                    numericals[i]->push_back(
                        database::Getter::get_double( str, time_formats ) );
                }

            for ( size_t i = 0; i < targets.size(); ++i )
                {
                    const auto &str = line[target_ix[i]];
                    targets[i]->push_back(
                        database::Getter::get_double( str, time_formats ) );
                }

            for ( size_t i = 0; i < text.size(); ++i )
                {
                    const auto &str = line[text_ix[i]];
                    text[i]->push_back( strings::String( str ) );
                }

            for ( size_t i = 0; i < time_stamps.size(); ++i )
                {
                    const auto &str = line[time_stamp_ix[i]];
                    time_stamps[i]->push_back(
                        database::Getter::get_time_stamp( str, time_formats ) );
                }

            for ( size_t i = 0; i < unused_floats.size(); ++i )
                {
                    const auto &str = line[unused_float_ix[i]];
                    unused_floats[i]->push_back(
                        database::Getter::get_double( str, time_formats ) );
                }

            for ( size_t i = 0; i < unused_strings.size(); ++i )
                {
                    const auto &str = line[unused_string_ix[i]];
                    unused_strings[i]->emplace_back( strings::String( str ) );
                }
        }

    // ----------------------------------------

    auto df = DataFrame( name(), categories_, join_keys_encoding_ );

    df.add_int_vectors( _schema.categoricals_, categoricals, ROLE_CATEGORICAL );

    df.add_int_vectors( _schema.join_keys_, join_keys, ROLE_JOIN_KEY );

    df.add_float_vectors( _schema.numericals_, numericals, ROLE_NUMERICAL );

    df.add_float_vectors( _schema.targets_, targets, ROLE_TARGET );

    df.add_string_vectors( _schema.text_, text, ROLE_TEXT );

    df.add_float_vectors( _schema.time_stamps_, time_stamps, ROLE_TIME_STAMP );

    df.add_float_vectors(
        _schema.unused_floats_, unused_floats, ROLE_UNUSED_FLOAT );

    df.add_string_vectors(
        _schema.unused_strings_, unused_strings, ROLE_UNUSED_STRING );

    // ----------------------------------------

    df.check_plausibility();

    // ----------------------------------------

    *this = std::move( df );

    // ----------------------------------------
}

// ----------------------------------------------------------------------------

void DataFrame::from_reader(
    const std::shared_ptr<io::Reader> &_reader,
    const std::string &_fname,
    const size_t _skip,
    const std::vector<std::string> &_time_formats,
    const Schema &_schema )
{
    // ------------------------------------------------------------------------

    assert_true( _reader );

    const auto csv_colnames = _reader->colnames();

    // ------------------------------------------------------------------------

    auto categoricals = make_vectors<Int>( _schema.categoricals_.size() );

    auto join_keys = make_vectors<Int>( _schema.join_keys_.size() );

    auto numericals = make_vectors<Float>( _schema.numericals_.size() );

    auto targets = make_vectors<Float>( _schema.targets_.size() );

    auto text = make_vectors<strings::String>( _schema.text_.size() );

    auto time_stamps = make_vectors<Float>( _schema.time_stamps_.size() );

    auto unused_floats = make_vectors<Float>( _schema.unused_floats_.size() );

    auto unused_strings =
        make_vectors<strings::String>( _schema.unused_strings_.size() );

    // ------------------------------------------------------------------------
    // Define column_indices.

    const auto df_colnames = concat_colnames( _schema );

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
    // Define lambda expressions for parsing doubles.

    const auto to_double = [_time_formats]( const std::string &_str ) {
        auto [val, success] = io::Parser::to_double( _str );

        if ( success )
            {
                return val;
            }

        std::tie( val, success ) =
            io::Parser::to_time_stamp( _str, _time_formats );

        if ( success )
            {
                return val;
            }

        return static_cast<Float>( NAN );
    };

    // ------------------------------------------------------------------------
    // Read CSV file content into the vectors

    size_t line_count = 0;

    for ( size_t i = 0; i < _skip; ++i )
        {
            _reader->next_line();
        }

    while ( !_reader->eof() )
        {
            const auto line = _reader->next_line();

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

            for ( auto &vec : join_keys )
                vec->push_back(
                    ( *join_keys_encoding_ )[line[colname_indices[col++]]] );

            for ( auto &vec : numericals )
                vec->push_back( to_double( line[colname_indices[col++]] ) );

            for ( auto &vec : targets )
                vec->push_back( to_double( line[colname_indices[col++]] ) );

            for ( auto &vec : text )
                vec->emplace_back(
                    strings::String( line[colname_indices[col++]] ) );

            for ( auto &vec : time_stamps )
                vec->push_back( to_double( line[colname_indices[col++]] ) );

            for ( auto &vec : unused_floats )
                vec->push_back( to_double( line[colname_indices[col++]] ) );

            for ( auto &vec : unused_strings )
                vec->emplace_back(
                    strings::String( line[colname_indices[col++]] ) );

            assert_true( col == colname_indices.size() );
        }

    // ------------------------------------------------------------------------

    auto df = DataFrame( name(), categories_, join_keys_encoding_ );

    df.add_int_vectors( _schema.categoricals_, categoricals, ROLE_CATEGORICAL );

    df.add_int_vectors( _schema.join_keys_, join_keys, ROLE_JOIN_KEY );

    df.add_float_vectors( _schema.numericals_, numericals, ROLE_NUMERICAL );

    df.add_float_vectors( _schema.targets_, targets, ROLE_TARGET );

    df.add_string_vectors( _schema.text_, text, ROLE_TEXT );

    df.add_float_vectors( _schema.time_stamps_, time_stamps, ROLE_TIME_STAMP );

    df.add_float_vectors(
        _schema.unused_floats_, unused_floats, ROLE_UNUSED_FLOAT );

    df.add_string_vectors(
        _schema.unused_strings_, unused_strings, ROLE_UNUSED_STRING );

    // ------------------------------------------------------------------------

    df.check_plausibility();

    // ------------------------------------------------------------------------

    *this = std::move( df );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DataFrame::from_s3(
    const std::string &_bucket,
    const std::optional<std::vector<std::string>> &_colnames,
    const std::vector<std::string> &_fnames,
    const std::string &_region,
    const std::string &_sep,
    const size_t _num_lines_read,
    const size_t _skip,
    const std::vector<std::string> &_time_formats,
    const Schema &_schema )
{
    // --------------------------------------------------------------------

#if ( defined( _WIN32 ) || defined( _WIN64 ) )
    throw std::invalid_argument( "S3 is not supported on Windows!" );
#else

    // ------------------------------------------------------------------------

    if ( _sep.size() != 1 )
        {
            throw std::invalid_argument(
                "The separator must contain exactly one character!" );
        }

    auto limit = ( _num_lines_read > 0 )
                     ? static_cast<Int>( _num_lines_read + _skip )
                     : static_cast<Int>( 0 );

    if ( !_colnames && limit > 0 )
        {
            ++limit;
        }

    // ------------------------------------------------------------------------

    auto df = containers::DataFrame( name(), categories_, join_keys_encoding_ );

    for ( size_t i = 0; i < _fnames.size(); ++i )
        {
            auto local_df = containers::DataFrame(
                name(), categories_, join_keys_encoding_ );

            const std::shared_ptr<io::Reader> reader =
                std::make_shared<io::S3Reader>(
                    _bucket, _colnames, _fnames[i], limit, _region, _sep[0] );

            local_df.from_reader(
                reader, _fnames[i], _skip, _time_formats, _schema );

            if ( i == 0 )
                {
                    df = std::move( local_df );
                }
            else
                {
                    df.append( local_df );
                }
        }

    // ------------------------------------------------------------------------

    *this = std::move( df );

    // ------------------------------------------------------------------------

#endif
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

    Poco::JSON::Object obj;

    // ----------------------------------------

    obj.set( "draw", _draw );

    obj.set( "recordsTotal", nrows() );

    obj.set( "recordsFiltered", nrows() );

    if ( nrows() == 0 )
        {
            obj.set( "data", Poco::JSON::Array() );
            return obj;
        }

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

    auto data = Poco::JSON::Array::Ptr( new Poco::JSON::Array() );

    const auto begin = static_cast<unsigned int>( _start );

    const auto end = ( _start + _length > nrows() )
                         ? nrows()
                         : static_cast<unsigned int>( _start + _length );

    for ( auto i = begin; i < end; ++i )
        {
            auto row = Poco::JSON::Array::Ptr( new Poco::JSON::Array() );

            for ( size_t j = 0; j < num_time_stamps(); ++j )
                {
                    row->add( io::Parser::ts_to_string( time_stamp( j )[i] ) );
                }

            for ( size_t j = 0; j < num_join_keys(); ++j )
                {
                    row->add( join_keys_encoding()[join_key( j )[i]].str() );
                }

            for ( size_t j = 0; j < num_targets(); ++j )
                {
                    row->add( io::Parser::to_string( target( j )[i] ) );
                }

            for ( size_t j = 0; j < num_categoricals(); ++j )
                {
                    row->add( categories()[categorical( j )[i]].str() );
                }

            for ( size_t j = 0; j < num_numericals(); ++j )
                {
                    if ( numerical( j ).unit().find( "time stamp" ) !=
                         std::string::npos )
                        {
                            row->add(
                                io::Parser::ts_to_string( numerical( j )[i] ) );
                        }
                    else
                        {
                            row->add(
                                io::Parser::to_string( numerical( j )[i] ) );
                        }
                }

            for ( size_t j = 0; j < num_text(); ++j )
                {
                    row->add( text( j )[i].str() );
                }

            for ( size_t j = 0; j < num_unused_floats(); ++j )
                {
                    if ( unused_float( j ).unit().find( "time stamp" ) !=
                         std::string::npos )
                        {
                            row->add( io::Parser::ts_to_string(
                                unused_float( j )[i] ) );
                        }
                    else
                        {
                            row->add(
                                io::Parser::to_string( unused_float( j )[i] ) );
                        }
                }

            for ( size_t j = 0; j < num_unused_strings(); ++j )
                {
                    row->add( unused_string( j )[i].str() );
                }

            data->add( row );
        }

    obj.set( "data", data );

    // ----------------------------------------

    return obj;

    // ----------------------------------------
}

// ----------------------------------------------------------------------------

std::tuple<
    std::vector<std::string>,
    std::vector<std::string>,
    std::vector<std::string>>
DataFrame::get_headers() const
{
    // ------------------------------------------------------------------------

    std::vector<std::string> colnames;

    std::vector<std::string> roles;

    std::vector<std::string> units;

    // ------------------------------------------------------------------------

    for ( size_t j = 0; j < num_time_stamps(); ++j )
        {
            colnames.push_back( time_stamp( j ).name() );
            roles.push_back( "time stamp" );
            units.push_back( time_stamp( j ).unit() );
        }

    for ( size_t j = 0; j < num_join_keys(); ++j )
        {
            colnames.push_back( join_key( j ).name() );
            roles.push_back( "join key" );
            units.push_back( join_key( j ).unit() );
        }

    for ( size_t j = 0; j < num_targets(); ++j )
        {
            colnames.push_back( target( j ).name() );
            roles.push_back( ROLE_TARGET );
            units.push_back( target( j ).unit() );
        }

    for ( size_t j = 0; j < num_categoricals(); ++j )
        {
            colnames.push_back( categorical( j ).name() );
            roles.push_back( ROLE_CATEGORICAL );
            units.push_back( categorical( j ).unit() );
        }

    for ( size_t j = 0; j < num_numericals(); ++j )
        {
            colnames.push_back( numerical( j ).name() );
            roles.push_back( ROLE_NUMERICAL );
            units.push_back( numerical( j ).unit() );
        }

    for ( size_t j = 0; j < num_text(); ++j )
        {
            colnames.push_back( text( j ).name() );
            roles.push_back( ROLE_TEXT );
            units.push_back( text( j ).unit() );
        }

    for ( size_t j = 0; j < num_unused_floats(); ++j )
        {
            colnames.push_back( unused_float( j ).name() );
            roles.push_back( "unused float" );
            units.push_back( unused_float( j ).unit() );
        }

    for ( size_t j = 0; j < num_unused_strings(); ++j )
        {
            colnames.push_back( unused_string( j ).name() );
            roles.push_back( "unused string" );
            units.push_back( unused_string( j ).unit() );
        }

    // ------------------------------------------------------------------------

    return std::make_tuple( colnames, roles, units );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::string DataFrame::get_html(
    const std::int32_t _max_rows, const std::int32_t _border ) const
{
    const auto [colnames, roles, units] = get_headers();

    const auto rows = get_rows( _max_rows );

    return DataFramePrinter( nrows(), ncols() )
        .get_html( colnames, roles, units, rows, _border );
}

// ----------------------------------------------------------------------------

std::vector<std::vector<std::string>> DataFrame::get_rows(
    const std::int32_t _max_rows ) const
{
    // ------------------------------------------------------------------------

    std::vector<std::vector<std::string>> rows;

    // ------------------------------------------------------------------------

    if ( nrows() == 0 || _max_rows <= 0 )
        {
            return rows;
        }

    // ------------------------------------------------------------------------

    const auto obj = get_content( 1, 0, _max_rows );

    const auto data = JSON::get_array( obj, "data" );

    assert_true( data );

    for ( size_t i = 0; i < data->size(); ++i )
        {
            const auto json_row =
                data->getArray( static_cast<unsigned int>( i ) );

            assert_true( json_row );

            assert_true( json_row->size() == ncols() );

            auto row = std::vector<std::string>( json_row->size() );

            for ( size_t j = 0; j < row.size(); ++j )
                {
                    row[j] = json_row->getElement<std::string>( j );
                }

            rows.emplace_back( row );
        }

    // ------------------------------------------------------------------------

    if ( _max_rows < nrows() )
        {
            assert_true( rows.size() > 0 );

            auto row = std::vector<std::string>( rows.at( 0 ) );

            for ( auto &r : row )
                {
                    r = "...";
                }

            rows.emplace_back( std::move( row ) );
        }

    // ------------------------------------------------------------------------

    return rows;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::string DataFrame::get_string( const std::int32_t _max_rows ) const
{
    const auto [colnames, roles, units] = get_headers();

    const auto rows = get_rows( _max_rows );

    return DataFramePrinter( nrows(), ncols() )
        .get_string( colnames, roles, units, rows );
}

// ----------------------------------------------------------------------------

const Column<Int> &DataFrame::int_column(
    const std::string &_role, const size_t _num ) const
{
    if ( _role == ROLE_CATEGORICAL )
        {
            return categorical( _num );
        }
    else if ( _role == ROLE_JOIN_KEY )
        {
            return join_key( _num );
        }

    throw std::invalid_argument( "Role '" + _role + "' not known!" );
}

// ----------------------------------------------------------------------------

const Column<Int> &DataFrame::int_column(
    const std::string &_name, const std::string &_role ) const
{
    if ( _role == ROLE_CATEGORICAL )
        {
            return categorical( _name );
        }
    else if ( _role == ROLE_JOIN_KEY )
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

    const auto last_change = load_textfile( _path, "last_change.txt" );

    if ( !last_change )
        {
            throw std::runtime_error(
                "'last_change.txt' could not be loaded!" );
        }

    last_change_ = *last_change;

    // ---------------------------------------------------------------------

    const auto build_history = load_textfile( _path, "build_history.json" );

    if ( build_history )
        {
            Poco::JSON::Parser parser;
            build_history_ = parser.parse( *build_history )
                                 .extract<Poco::JSON::Object::Ptr>();
        }

    // ---------------------------------------------------------------------

    categoricals_ = load_columns<Int>( _path, "categorical_" );

    join_keys_ = load_columns<Int>( _path, "join_key_" );

    numericals_ = load_columns<Float>( _path, "numerical_" );

    targets_ = load_columns<Float>( _path, "target_" );

    text_ = load_columns<strings::String>( _path, "text_" );

    time_stamps_ = load_columns<Float>( _path, "time_stamp_" );

    unused_floats_ = load_columns<Float>( _path, "unused_float_" );

    unused_strings_ = load_columns<strings::String>( _path, "unused_string_" );

    // ---------------------------------------------------------------------

    check_plausibility();

    // ---------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::optional<std::string> DataFrame::load_textfile(
    const std::string &_path, const std::string &_fname ) const
{
    std::ifstream textfile( _path + _fname );

    std::string content;

    if ( textfile.is_open() )
        {
            for ( std::string line; std::getline( textfile, line ); )
                {
                    content += line;
                }

            textfile.close();

            return content;
        }

    return std::nullopt;
}

// ----------------------------------------------------------------------------

ULong DataFrame::nbytes() const
{
    ULong nbytes = 0;

    nbytes += calc_nbytes( categoricals_ );

    nbytes += calc_nbytes( join_keys_ );

    nbytes += calc_nbytes( numericals_ );

    nbytes += calc_nbytes( targets_ );

    nbytes += calc_nbytes( text_ );

    nbytes += calc_nbytes( time_stamps_ );

    nbytes += calc_nbytes( unused_floats_ );

    nbytes += calc_nbytes( unused_strings_ );

    return nbytes;
}

// ----------------------------------------------------------------------------

const size_t DataFrame::nrows() const
{
    if ( unused_floats_.size() > 0 )
        {
            return unused_floats_.at( 0 ).nrows();
        }

    if ( unused_strings_.size() > 0 )
        {
            return unused_strings_.at( 0 ).nrows();
        }

    if ( join_keys_.size() > 0 )
        {
            return join_keys_.at( 0 ).nrows();
        }

    if ( time_stamps_.size() > 0 )
        {
            return time_stamps_.at( 0 ).nrows();
        }

    if ( categoricals_.size() > 0 )
        {
            return categoricals_.at( 0 ).nrows();
        }

    if ( numericals_.size() > 0 )
        {
            return numericals_.at( 0 ).nrows();
        }

    if ( targets_.size() > 0 )
        {
            return targets_.at( 0 ).nrows();
        }

    if ( text_.size() > 0 )
        {
            return text_.at( 0 ).nrows();
        }

    return 0;
}

// ----------------------------------------------------------------------------

Poco::JSON::Object DataFrame::refresh() const
{
    // ----------------------------------------

    Poco::JSON::Object obj;

    // ----------------------------------------

    obj.set( ROLE_CATEGORICAL, get_colnames( categoricals_ ) );

    obj.set( ROLE_JOIN_KEY, get_colnames( join_keys_ ) );

    obj.set( ROLE_NUMERICAL, get_colnames( numericals_ ) );

    obj.set( ROLE_TARGET, get_colnames( targets_ ) );

    obj.set( ROLE_TEXT, get_colnames( text_ ) );

    obj.set( ROLE_TIME_STAMP, get_colnames( time_stamps_ ) );

    obj.set( ROLE_UNUSED_FLOAT, get_colnames( unused_floats_ ) );

    obj.set( ROLE_UNUSED_STRING, get_colnames( unused_strings_ ) );

    // ----------------------------------------

    return obj;

    // ----------------------------------------
}

// ----------------------------------------------------------------------------

bool DataFrame::remove_column( const std::string &_name )
{
    bool success = rm_col( _name, &categoricals_ );

    if ( success )
        {
            update_last_change();
            return true;
        }

    success = rm_col( _name, &join_keys_, &indices_ );

    if ( success )
        {
            update_last_change();
            return true;
        }

    success = rm_col( _name, &numericals_ );

    if ( success )
        {
            update_last_change();
            return true;
        }

    success = rm_col( _name, &targets_ );

    if ( success )
        {
            update_last_change();
            return true;
        }

    success = rm_col( _name, &text_ );

    if ( success )
        {
            update_last_change();
            return true;
        }

    success = rm_col( _name, &time_stamps_ );

    if ( success )
        {
            update_last_change();
            return true;
        }

    success = rm_col( _name, &unused_floats_ );

    if ( success )
        {
            update_last_change();
            return true;
        }

    success = rm_col( _name, &unused_strings_ );

    if ( success )
        {
            update_last_change();
            return true;
        }

    return false;
}

// ----------------------------------------------------------------------------

void DataFrame::save( const std::string &_path, const std::string &_name ) const
{
    // ---------------------------------------------------------------------

    auto tfile = Poco::TemporaryFile( engine::temp_dir );

    tfile.createDirectories();

    const auto tpath = tfile.path() + "/";

    // ---------------------------------------------------------------------

    save_matrices( categoricals_, tpath, "categorical_" );

    save_matrices( join_keys_, tpath, "join_key_" );

    save_matrices( numericals_, tpath, "numerical_" );

    save_matrices( targets_, tpath, "target_" );

    save_matrices( text_, tpath, "text_" );

    save_matrices( time_stamps_, tpath, "time_stamp_" );

    save_matrices( unused_floats_, tpath, "unused_float_" );

    save_matrices( unused_strings_, tpath, "unused_string_" );

    // ---------------------------------------------------------------------

    save_text( tpath, "last_change.txt", last_change_ );

    // ---------------------------------------------------------------------

    if ( build_history_ )
        {
            save_text(
                tpath,
                "build_history.json",
                JSON::stringify( *build_history_ ) );
        }

    // ---------------------------------------------------------------------
    // If the path already exists, delete it to avoid
    // conflicts with already existing files.

    auto file = Poco::File( _path + _name );

    if ( file.exists() )
        {
            file.remove( true );
        }

    tfile.renameTo( file.path() );

    tfile.keep();

    // ---------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DataFrame::save_text(
    const std::string &_tpath,
    const std::string &_fname,
    const std::string &_text ) const
{
    std::ofstream textfile;
    textfile.open( _tpath + _fname );
    textfile << _text;
    textfile.close();
}

// ----------------------------------------------------------------------------

Poco::JSON::Object DataFrame::to_monitor() const
{
    // ---------------------------------------------------------------------

    Poco::JSON::Object obj;

    // ---------------------------------------------------------------------

    obj.set( "categorical_", get_colnames( categoricals_ ) );

    obj.set( "categorical_units_", get_units( categoricals_ ) );

    obj.set( "join_keys_", get_colnames( join_keys_ ) );

    obj.set( "name_", name_ );

    obj.set( "num_categorical_", num_categoricals() );

    obj.set( "num_join_keys_", num_join_keys() );

    obj.set( "num_numerical_", num_numericals() );

    obj.set( "num_rows_", nrows() );

    obj.set( "num_targets_", num_targets() );

    obj.set( "num_text_", num_text() );

    obj.set( "num_time_stamps_", num_time_stamps() );

    obj.set( "num_unused_floats_", num_unused_floats() );

    obj.set( "num_unused_strings_", num_unused_strings() );

    obj.set( "numerical_", get_colnames( numericals_ ) );

    obj.set( "numerical_units_", get_units( numericals_ ) );

    obj.set( "size_", static_cast<Float>( nbytes() ) / 1000000.0 );

    obj.set( "targets_", get_colnames( targets_ ) );

    obj.set( "text_", get_colnames( text_ ) );

    obj.set( "text_units_", get_units( text_ ) );

    obj.set( "time_stamps_", get_colnames( time_stamps_ ) );

    obj.set( "time_stamp_units_", get_units( time_stamps_ ) );

    obj.set( "unused_floats_", get_colnames( unused_floats_ ) );

    obj.set( "unused_float_units_", get_units( unused_floats_ ) );

    obj.set( "unused_strings_", get_colnames( unused_strings_ ) );

    obj.set( "unused_string_units_", get_units( unused_strings_ ) );

    // ---------------------------------------------------------------------

    return obj;

    // ---------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

/// Expresses the schema of the DataFrame as a JSON object.
Poco::JSON::Object::Ptr DataFrame::to_schema() const
{
    auto obj = Poco::JSON::Object::Ptr( new Poco::JSON::Object() );

    obj->set( "categorical_", get_colnames( categoricals_ ) );

    obj->set( "join_key_", get_colnames( join_keys_ ) );

    obj->set( "numerical_", get_colnames( numericals_ ) );

    obj->set( "target_", get_colnames( targets_ ) );

    obj->set( "text_", get_colnames( text_ ) );

    obj->set( "time_stamp_", get_colnames( time_stamps_ ) );

    obj->set( "unused_float_", get_colnames( unused_floats_ ) );

    obj->set( "unused_string_", get_colnames( unused_strings_ ) );

    return obj;
}

// ----------------------------------------------------------------------------

void DataFrame::where( const std::vector<bool> &_condition )
{
    auto df = DataFrame( name(), categories_, join_keys_encoding_ );

    for ( size_t i = 0; i < num_categoricals(); ++i )
        {
            df.add_int_column(
                categorical( i ).where( _condition ), ROLE_CATEGORICAL );
        }

    for ( size_t i = 0; i < num_join_keys(); ++i )
        {
            df.add_int_column(
                join_key( i ).where( _condition ), ROLE_JOIN_KEY );
        }

    for ( size_t i = 0; i < num_numericals(); ++i )
        {
            df.add_float_column(
                numerical( i ).where( _condition ), ROLE_NUMERICAL );
        }

    for ( size_t i = 0; i < num_targets(); ++i )
        {
            df.add_float_column( target( i ).where( _condition ), ROLE_TARGET );
        }

    for ( size_t i = 0; i < num_text(); ++i )
        {
            df.add_string_column( text( i ).where( _condition ), ROLE_TEXT );
        }

    for ( size_t i = 0; i < num_time_stamps(); ++i )
        {
            df.add_float_column(
                time_stamp( i ).where( _condition ), ROLE_TIME_STAMP );
        }

    for ( size_t i = 0; i < num_unused_floats(); ++i )
        {
            df.add_float_column(
                unused_float( i ).where( _condition ), ROLE_UNUSED_FLOAT );
        }

    for ( size_t i = 0; i < num_unused_strings(); ++i )
        {
            df.add_string_column(
                unused_string( i ).where( _condition ), ROLE_UNUSED_STRING );
        }

    df.create_indices();

    *this = std::move( df );
}

// ----------------------------------------------------------------------------
}  // namespace containers
}  // namespace engine
