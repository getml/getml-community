#include "engine/preprocessors/preprocessors.hpp"

namespace engine
{
namespace preprocessors
{
// ----------------------------------------------------

std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
PreprocessorImpl::extract_data_frames(
    const Poco::JSON::Object& _cmd,
    const std::map<std::string, containers::DataFrame>& _data_frames )
{
    // ------------------------------------------------

    const auto population_name =
        JSON::get_value<std::string>( _cmd, "population_name_" );

    const auto population_df =
        utils::Getter::get( population_name, _data_frames );

    // ------------------------------------------------

    const auto peripheral_names = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "peripheral_names_" ) );

    std::vector<containers::DataFrame> peripheral_dfs;

    for ( auto& name : peripheral_names )
        {
            peripheral_dfs.emplace_back(
                utils::Getter::get( name, _data_frames ) );
        }

    // ------------------------------------------------

    return std::make_pair( population_df, peripheral_dfs );

    // ------------------------------------------------
}

// ----------------------------------------------------

std::vector<std::shared_ptr<helpers::ColumnDescription>>
PreprocessorImpl::from_array( const Poco::JSON::Array::Ptr& _arr )
{
    assert_true( _arr );

    auto desc = std::vector<std::shared_ptr<helpers::ColumnDescription>>();

    for ( size_t i = 0; i < _arr->size(); ++i )
        {
            const auto ptr = _arr->getObject( i );

            assert_true( ptr );

            desc.push_back(
                std::make_shared<helpers::ColumnDescription>( *ptr ) );
        }

    return desc;
}

// ----------------------------------------------------

void PreprocessorImpl::insert_data_frames(
    const Poco::JSON::Object& _cmd,
    const containers::DataFrame& _population_df,
    const std::vector<containers::DataFrame>& _peripheral_dfs,
    std::map<std::string, containers::DataFrame>* _data_frames )
{
    // ------------------------------------------------

    auto& data_frames = *_data_frames;

    // ------------------------------------------------

    const auto population_name =
        JSON::get_value<std::string>( _cmd, "population_name_" );

    data_frames[population_name] = _population_df;

    // ------------------------------------------------

    const auto peripheral_names = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "peripheral_names_" ) );

    assert_true( peripheral_names.size() == _peripheral_dfs.size() );

    for ( size_t i = 0; i < _peripheral_dfs.size(); ++i )
        {
            data_frames[peripheral_names.at( i )] = _peripheral_dfs.at( i );
        }

    // ------------------------------------------------
}

// ----------------------------------------------------

std::vector<std::string> PreprocessorImpl::retrieve_names(
    const std::string& _marker,
    const size_t _table,
    const std::vector<std::shared_ptr<helpers::ColumnDescription>>& _desc )
{
    const auto table = std::to_string( _table );

    auto names = std::vector<std::string>();

    for ( const auto& ptr : _desc )
        {
            assert_true( ptr );

            if ( ptr->marker_ == _marker && ptr->table_ == table )
                {
                    names.push_back( ptr->name_ );
                }
        }

    return names;
}

// ----------------------------------------------------

Poco::JSON::Array::Ptr PreprocessorImpl::to_array(
    const std::vector<std::shared_ptr<helpers::ColumnDescription>>& _desc )
{
    auto arr = Poco::JSON::Array::Ptr( new Poco::JSON::Array() );

    for ( const auto& ptr : _desc )
        {
            assert_true( ptr );
            arr->add( ptr->to_json_obj() );
        }

    return arr;
}

// ----------------------------------------------------
}  // namespace preprocessors
}  // namespace engine
