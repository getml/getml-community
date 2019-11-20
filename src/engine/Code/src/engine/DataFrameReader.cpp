
#include "engine/containers/containers.hpp"

namespace engine
{
namespace containers
{
// ----------------------------------------------------------------------------

std::vector<std::string> DataFrameReader::make_colnames(
    const DataFrame& _df, char _quotechar )
{
    // ------------------------------------------------------------------------

    std::vector<std::string> colnames;

    for ( size_t i = 0; i < _df.num_categoricals(); ++i )
        {
            const auto& colname = _df.categorical( i ).name();
            colnames.push_back( colname );
        }

    for ( size_t i = 0; i < _df.num_discretes(); ++i )
        {
            const auto& colname = _df.discrete( i ).name();
            colnames.push_back( colname );
        }

    for ( size_t i = 0; i < _df.num_join_keys(); ++i )
        {
            const auto& colname = _df.join_key( i ).name();
            colnames.push_back( colname );
        }

    for ( size_t i = 0; i < _df.num_numericals(); ++i )
        {
            const auto& colname = _df.numerical( i ).name();
            colnames.push_back( colname );
        }

    for ( size_t i = 0; i < _df.num_targets(); ++i )
        {
            const auto& colname = _df.target( i ).name();
            colnames.push_back( colname );
        }

    for ( size_t i = 0; i < _df.num_time_stamps(); ++i )
        {
            const auto& colname = _df.time_stamp( i ).name();
            colnames.push_back( colname );
        }

    for ( size_t i = 0; i < _df.num_undefined_floats(); ++i )
        {
            const auto& colname = _df.undefined_float( i ).name();
            colnames.push_back( colname );
        }

    for ( size_t i = 0; i < _df.num_undefined_integers(); ++i )
        {
            const auto& colname = _df.undefined_integer( i ).name();
            colnames.push_back( colname );
        }

    for ( size_t i = 0; i < _df.num_undefined_strings(); ++i )
        {
            const auto& colname = _df.undefined_string( i ).name();
            colnames.push_back( colname );
        }

    // ------------------------------------------------------------------------

    for ( auto& name : colnames )
        {
            name = csv::Parser::remove_quotechars( name, _quotechar );
        }

    // ------------------------------------------------------------------------

    return colnames;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::vector<csv::Datatype> DataFrameReader::make_coltypes(
    const DataFrame& _df )
{
    std::vector<csv::Datatype> coltypes;

    for ( size_t i = 0; i < _df.num_categoricals(); ++i )
        {
            coltypes.push_back( csv::Datatype::string );
        }

    for ( size_t i = 0; i < _df.num_discretes(); ++i )
        {
            if ( _df.discrete( i ).unit().find( "time stamp" ) !=
                 std::string::npos )
                {
                    coltypes.push_back( csv::Datatype::string );
                }
            else
                {
                    coltypes.push_back( csv::Datatype::double_precision );
                }
        }

    for ( size_t i = 0; i < _df.num_join_keys(); ++i )
        {
            coltypes.push_back( csv::Datatype::string );
        }

    for ( size_t i = 0; i < _df.num_numericals(); ++i )
        {
            if ( _df.numerical( i ).unit().find( "time stamp" ) !=
                 std::string::npos )
                {
                    coltypes.push_back( csv::Datatype::string );
                }
            else
                {
                    coltypes.push_back( csv::Datatype::double_precision );
                }
        }

    for ( size_t i = 0; i < _df.num_targets(); ++i )
        {
            coltypes.push_back( csv::Datatype::double_precision );
        }

    for ( size_t i = 0; i < _df.num_time_stamps(); ++i )
        {
            coltypes.push_back( csv::Datatype::string );
        }

    for ( size_t i = 0; i < _df.num_undefined_floats(); ++i )
        {
            if ( _df.undefined_float( i ).unit().find( "time stamp" ) !=
                 std::string::npos )
                {
                    coltypes.push_back( csv::Datatype::string );
                }
            else
                {
                    coltypes.push_back( csv::Datatype::double_precision );
                }
        }

    for ( size_t i = 0; i < _df.num_undefined_integers(); ++i )
        {
            coltypes.push_back( csv::Datatype::integer );
        }

    for ( size_t i = 0; i < _df.num_undefined_strings(); ++i )
        {
            coltypes.push_back( csv::Datatype::string );
        }

    return coltypes;
}

// ----------------------------------------------------------------------------

std::vector<std::string> DataFrameReader::next_line()
{
    // ------------------------------------------------------------------------
    // Usually the calling function should make sure that we haven't reached
    // the end of file. But just to be sure, we do it again.

    if ( eof() )
        {
            return std::vector<std::string>();
        }

    // ------------------------------------------------------------------------
    // Chop up lines into fields.

    assert_true( colnames().size() == coltypes().size() );

    std::vector<std::string> result( coltypes().size() );

    size_t col = 0;

    for ( size_t i = 0; i < df_.num_categoricals(); ++i )
        {
            const auto& val = df_.categorical( i )[rownum_];
            result[col++] = categories()[val].str();
        }

    for ( size_t i = 0; i < df_.num_discretes(); ++i )
        {
            const auto& val = df_.discrete( i )[rownum_];
            if ( coltypes()[col] == csv::Datatype::string )
                result[col++] = df_.to_time_stamp( val );
            else
                result[col++] = std::to_string( val );
        }

    for ( size_t i = 0; i < df_.num_join_keys(); ++i )
        {
            const auto& val = df_.join_key( i )[rownum_];
            result[col++] = join_keys_encoding()[val].str();
        }

    for ( size_t i = 0; i < df_.num_numericals(); ++i )
        {
            const auto& val = df_.numerical( i )[rownum_];
            if ( coltypes()[col] == csv::Datatype::string )
                result[col++] = df_.to_time_stamp( val );
            else
                result[col++] = std::to_string( val );
        }

    for ( size_t i = 0; i < df_.num_targets(); ++i )
        {
            const auto& val = df_.target( i )[rownum_];
            result[col++] = std::to_string( val );
        }

    for ( size_t i = 0; i < df_.num_time_stamps(); ++i )
        {
            const auto& val = df_.time_stamp( i )[rownum_];
            result[col++] = df_.to_time_stamp( val );
        }

    for ( size_t i = 0; i < df_.num_undefined_floats(); ++i )
        {
            const auto& val = df_.undefined_float( i )[rownum_];
            if ( coltypes()[col] == csv::Datatype::string )
                result[col++] = df_.to_time_stamp( val );
            else
                result[col++] = std::to_string( val );
        }

    for ( size_t i = 0; i < df_.num_undefined_integers(); ++i )
        {
            const auto& val = df_.undefined_integer( i )[rownum_];
            result[col++] = std::to_string( val );
        }

    for ( size_t i = 0; i < df_.num_undefined_strings(); ++i )
        {
            const auto& val = df_.undefined_string( i )[rownum_];
            result[col++] = val.str();
        }

    assert_true( col == result.size() );

    // ------------------------------------------------------------------------

    ++rownum_;

    // ------------------------------------------------------------------------

    return result;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DataFrameReader::update_counts(
    const std::string& _colname, std::map<std::string, Int>* _counts )
{
    const auto it = _counts->find( _colname );

    if ( it == _counts->end() )
        {
            ( *_counts )[_colname] = 1;
        }
    else
        {
            ( *_counts )[_colname] += 1;
        }
}

// ----------------------------------------------------------------------------
}  // namespace containers
}  // namespace engine
