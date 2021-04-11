#include "helpers/helpers.hpp"

namespace helpers
{
// ----------------------------------------------------------------------------

MappingContainer::MappingContainer(
    const std::vector<MappingForDf>& _categorical,
    const std::vector<Colnames>& _categorical_names,
    const std::vector<MappingForDf>& _discrete,
    const std::vector<Colnames>& _discrete_names,
    const std::vector<std::shared_ptr<const MappingContainer>>& _subcontainers,
    const TableNames& _table_names,
    const std::vector<MappingForDf>& _text,
    const std::vector<Colnames>& _text_names )
    : categorical_( _categorical ),
      categorical_names_( _categorical_names ),
      discrete_( _discrete ),
      discrete_names_( _discrete_names ),
      subcontainers_( _subcontainers ),
      table_names_( _table_names ),
      text_( _text ),
      text_names_( _text_names )
{
    check_lengths();
}

// ----------------------------------------------------------------------------

MappingContainer::MappingContainer( const Poco::JSON::Object& _obj )
    : categorical_( extract_mapping_vector( _obj, "categorical_" ) ),
      discrete_( extract_mapping_vector( _obj, "discrete_" ) ),
      subcontainers_( extract_subcontainers( _obj ) ),
      text_( extract_mapping_vector( _obj, "text_" ) )
{
    check_lengths();
}

// ----------------------------------------------------------------------------

MappingContainer::~MappingContainer() = default;

// ----------------------------------------------------------------------------

void MappingContainer::check_lengths() const
{
    assert_msg(
        categorical_.size() == subcontainers_.size(),
        "categorical_.size(): " + std::to_string( categorical_.size() ) +
            ", subcontainers_.size(): " +
            std::to_string( subcontainers_.size() ) );

    assert_msg(
        categorical_.size() == categorical_names_.size(),
        "categorical_.size(): " + std::to_string( categorical_.size() ) +
            ", categorical_names_.size(): " +
            std::to_string( categorical_names_.size() ) );

    assert_msg(
        categorical_.size() == discrete_.size(),
        "categorical_.size(): " + std::to_string( categorical_.size() ) +
            ", discrete_.size(): " + std::to_string( discrete_.size() ) );

    assert_msg(
        categorical_.size() == discrete_names_.size(),
        "categorical_.size(): " + std::to_string( categorical_.size() ) +
            ", discrete_names_.size(): " +
            std::to_string( discrete_names_.size() ) );

    assert_msg(
        categorical_.size() == text_.size(),
        "categorical_.size(): " + std::to_string( categorical_.size() ) +
            ", text_.size(): " + std::to_string( text_.size() ) );

    assert_msg(
        categorical_.size() == text_names_.size(),
        "categorical_.size(): " + std::to_string( categorical_.size() ) +
            ", text_names_.size(): " + std::to_string( text_names_.size() ) );

    assert_true( table_names_ );

    assert_msg(
        categorical_.size() == table_names_->size(),
        "categorical_.size(): " + std::to_string( categorical_.size() ) +
            ", table_names_->size(): " +
            std::to_string( table_names_->size() ) );
}

// ----------------------------------------------------------------------------

std::vector<typename MappingContainer::MappingForDf>
MappingContainer::extract_mapping_vector(
    const Poco::JSON::Object& _obj, const std::string& _key )
{
    // --------------------------------------------------------------

    const auto obj_to_map = []( const Poco::JSON::Object& _obj ) {
        auto m = std::make_shared<std::map<Int, std::vector<Float>>>();

        for ( const auto& [key, _] : _obj )
            {
                const auto arr = _obj.getArray( key );

                assert_msg(
                    arr,
                    "key: " + key +
                        ", _obj: " + jsonutils::JSON::stringify( _obj ) );

                ( *m )[static_cast<Int>( std::atoi( key.c_str() ) )] =
                    jsonutils::JSON::array_to_vector<Float>( arr );
            }

        return m;
    };

    // --------------------------------------------------------------

    const auto extract_mapping = [obj_to_map]( Poco::JSON::Array _arr ) {
        MappingForDf mapping;

        for ( size_t i = 0; i < _arr.size(); ++i )
            {
                auto ptr = _arr.getObject( i );
                throw_unless( ptr, "Expected an object inside the mapping." );
                mapping.push_back( obj_to_map( *ptr ) );
            }

        return mapping;
    };

    // --------------------------------------------------------------

    const auto arr_to_vec = [extract_mapping]( Poco::JSON::Array::Ptr _arr ) {
        throw_unless( _arr, "Expected an array inside the mapping." );

        std::vector<MappingForDf> vec;

        for ( size_t i = 0; i < _arr->size(); ++i )
            {
                auto ptr = _arr->getArray( i );
                throw_unless( ptr, "Expected an array inside the mapping." );
                vec.push_back( extract_mapping( *ptr ) );
            }

        return vec;
    };

    // --------------------------------------------------------------

    return arr_to_vec( _obj.getArray( _key ) );
}

// ----------------------------------------------------------------------------

std::vector<std::shared_ptr<const MappingContainer>>
MappingContainer::extract_subcontainers( const Poco::JSON::Object& _obj )
{
    auto arr = _obj.getArray( "subcontainers_" );

    throw_unless( arr, "Expected array called 'subcontainers_'" );

    std::vector<std::shared_ptr<const MappingContainer>> subcontainers;

    for ( size_t i = 0; i < arr->size(); ++i )
        {
            auto ptr = arr->getObject( i );

            if ( ptr )
                {
                    subcontainers.push_back(
                        std::make_shared<const MappingContainer>( *ptr ) );
                }
            else
                {
                    subcontainers.push_back( nullptr );
                }
        }

    return subcontainers;
}

// ----------------------------------------------------------------------------

Poco::JSON::Object::Ptr MappingContainer::to_json_obj() const
{
    // --------------------------------------------------------------

    const auto map_to_object =
        []( const std::map<Int, std::vector<Float>>& _map ) {
            Poco::JSON::Object::Ptr obj( new Poco::JSON::Object() );

            for ( const auto& [key, value] : _map )
                {
                    obj->set(
                        std::to_string( key ),
                        jsonutils::JSON::vector_to_array_ptr( value ) );
                }

            return obj;
        };

    // --------------------------------------------------------------

    const auto transform_mapping =
        [map_to_object]( const MappingForDf& _mapping ) {
            auto arr = Poco::JSON::Array::Ptr( new Poco::JSON::Array() );

            for ( const auto& ptr : _mapping )
                {
                    assert_true( ptr );
                    arr->add( map_to_object( *ptr ) );
                }

            return arr;
        };

    // --------------------------------------------------------------

    const auto transform_mapping_vec =
        [transform_mapping]( const std::vector<MappingForDf>& _mapping_vec ) {
            auto arr = Poco::JSON::Array::Ptr( new Poco::JSON::Array() );

            for ( const auto& vec : _mapping_vec )
                {
                    arr->add( transform_mapping( vec ) );
                }

            return arr;
        };

    // --------------------------------------------------------------

    const auto transform_subcontainers =
        []( const std::vector<std::shared_ptr<const MappingContainer>>&
                _subcontainers ) {
            auto arr = Poco::JSON::Array::Ptr( new Poco::JSON::Array() );

            for ( const auto& s : _subcontainers )
                {
                    if ( s )
                        {
                            arr->add( s->to_json_obj() );
                        }
                    else
                        {
                            arr->add( "" );
                        }
                }

            return arr;
        };

    // --------------------------------------------------------------

    auto obj = Poco::JSON::Object::Ptr( new Poco::JSON::Object() );

    obj->set( "categorical_", transform_mapping_vec( categorical_ ) );

    obj->set( "discrete_", transform_mapping_vec( discrete_ ) );

    obj->set( "subcontainers_", transform_subcontainers( subcontainers_ ) );

    obj->set( "text_", transform_mapping_vec( text_ ) );

    return obj;

    // --------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::pair<std::vector<std::string>, typename MappingContainer::ColnameMap>
MappingContainer::to_sql(
    const std::shared_ptr<const std::vector<strings::String>>& _categories,
    const std::string& _feature_prefix ) const
{
    // ------------------------------------------------------------------------

    using PtrType = typename MappingForDf::value_type;

    using Pair = std::pair<Int, Float>;

    using Map = typename PtrType::element_type;

    // ------------------------------------------------------------------------

    ColnameMap colname_map;

    // ------------------------------------------------------------------------

    const auto add_to_map = [&colname_map](
                                const std::string& _tname,
                                const std::string& _colname ) {
        auto it = colname_map.find( _tname );

        if ( it == colname_map.end() )
            {
                colname_map[_tname] = { _colname };
            }
        else
            {
                it->second.push_back( _colname );
            }
    };

    // ------------------------------------------------------------------------

    const auto merge_map = [add_to_map]( const ColnameMap& _submap ) {
        for ( const auto& [key, values] : _submap )
            {
                for ( const auto& value : values )
                    {
                        add_to_map( key, value );
                    }
            }
    };

    // ------------------------------------------------------------------------

    const auto by_value = []( const Pair& _p1, const Pair& _p2 ) -> bool {
        return _p1.second > _p2.second;
    };

    // ------------------------------------------------------------------------

    const auto make_pairs =
        [by_value](
            const Map& _m, const size_t _target_num ) -> std::vector<Pair> {
        auto pairs = std::vector<Pair>();
        for ( const auto& p : _m )
            {
                assert_true( _target_num < p.second.size() );
                pairs.push_back(
                    std::make_pair( p.first, p.second.at( _target_num ) ) );
            }
        std::sort( pairs.begin(), pairs.end(), by_value );
        return pairs;
    };

    // ------------------------------------------------------------------------

    const auto make_table_header = []( const std::string& _name,
                                       const bool _key_is_num ) -> std::string {
        std::string sql = "DROP TABLE IF EXISTS \"" + _name + "\";\n\n";
        const std::string key_type = _key_is_num ? "NUMERIC" : "TEXT";
        sql += "CREATE TABLE \"" + _name + "\"(key " + key_type +
               " NOT NULL PRIMARY KEY, value NUMERIC);\n\n";
        sql += "INSERT INTO \"" + _name + "\"(key, value)\nVALUES";
        return sql;
    };

    // ------------------------------------------------------------------------

    assert_true( _categories );

    const auto categorical_to_sql =
        [_categories, make_pairs, make_table_header](
            const std::string& _name,
            const PtrType& _ptr,
            const size_t _target_num ) -> std::string {
        assert_true( _ptr );
        const auto pairs = make_pairs( *_ptr, _target_num );
        std::string sql = make_table_header( _name, false );
        for ( size_t i = 0; i < pairs.size(); ++i )
            {
                const std::string begin = ( i == 0 ) ? "" : "      ";
                const auto& p = pairs.at( i );
                assert_true( p.first >= 0 );
                assert_true(
                    static_cast<size_t>( p.first ) < _categories->size() );
                const std::string end =
                    ( i == pairs.size() - 1 ) ? ";\n\n\n" : ",\n";
                sql += begin + "('" + _categories->at( p.first ).str() + "', " +
                       io::Parser::to_precise_string( p.second ) + ")" + end;
            }
        return sql;
    };

    // ------------------------------------------------------------------------

    const auto discrete_to_sql = [make_pairs, make_table_header](
                                     const std::string& _name,
                                     const PtrType& _ptr,
                                     const size_t _target_num ) -> std::string {
        assert_true( _ptr );
        const auto pairs = make_pairs( *_ptr, _target_num );
        std::string sql = make_table_header( _name, true );
        for ( size_t i = 0; i < pairs.size(); ++i )
            {
                const std::string begin = ( i == 0 ) ? "" : "      ";
                const auto& p = pairs.at( i );
                const std::string end =
                    ( i == pairs.size() - 1 ) ? ";\n\n\n" : ",\n";
                sql += begin + "(" + std::to_string( p.first ) + ", " +
                       io::Parser::to_precise_string( p.second ) + ")" + end;
            }
        return sql;
    };

    // ------------------------------------------------------------------------

    std::vector<std::string> sql;

    // ------------------------------------------------------------------------

    for ( size_t i = 0; i < categorical_.size(); ++i )
        {
            const auto& c = categorical_.at( i );

            const auto& names = categorical_names_.at( i );

            const auto feature_prefix =
                _feature_prefix + std::to_string( i + 1 ) + "_";

            assert_true( names );

            assert_true( c.size() == names->size() );

            const auto num_targets =
                MappingContainerMaker::infer_num_targets( c );

            for ( size_t t = 0; t < num_targets; ++t )
                {
                    for ( size_t j = 0; j < c.size(); ++j )
                        {
                            const auto& ptr = c.at( j );

                            const auto name = SQLGenerator::to_upper(
                                MappingContainerMaker::make_colname(
                                    names->at( j ), feature_prefix, t ) );

                            sql.push_back( categorical_to_sql( name, ptr, t ) );

                            add_to_map( table_names_->at( i ), name );
                        }
                }
        }

    // ------------------------------------------------------------------------

    assert_true( discrete_.size() == discrete_names_.size() );

    for ( size_t i = 0; i < discrete_.size(); ++i )
        {
            const auto& d = discrete_.at( i );

            const auto& names = discrete_names_.at( i );

            const auto feature_prefix =
                _feature_prefix + std::to_string( i + 1 ) + "_";

            assert_true( names );

            assert_true( d.size() == names->size() );

            const auto num_targets =
                MappingContainerMaker::infer_num_targets( d );

            for ( size_t t = 0; t < num_targets; ++t )
                {
                    for ( size_t j = 0; j < d.size(); ++j )
                        {
                            const auto& ptr = d.at( j );

                            const auto name = SQLGenerator::to_upper(
                                MappingContainerMaker::make_colname(
                                    names->at( j ), feature_prefix, t ) );

                            sql.push_back( discrete_to_sql( name, ptr, t ) );

                            add_to_map( table_names_->at( i ), name );
                        }
                }
        }

    // ------------------------------------------------------------------------

    for ( size_t i = 0; i < subcontainers_.size(); ++i )
        {
            const auto& s = subcontainers_.at( i );

            if ( s )
                {
                    const auto feature_prefix =
                        _feature_prefix + std::to_string( i + 1 ) + "_";

                    const auto [subfeatures, submap] =
                        s->to_sql( _categories, feature_prefix );

                    sql.insert(
                        sql.end(), subfeatures.begin(), subfeatures.end() );

                    merge_map( submap );
                }
        }

    // ------------------------------------------------------------------------

    return std::make_pair( sql, colname_map );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace helpers
