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
      categorical_names_( extract_colnames( _obj, "categorical_names_" ) ),
      discrete_( extract_mapping_vector( _obj, "discrete_" ) ),
      discrete_names_( extract_colnames( _obj, "discrete_names_" ) ),
      subcontainers_( extract_subcontainers( _obj ) ),
      table_names_( extract_table_names( _obj ) ),
      text_( extract_mapping_vector( _obj, "text_" ) ),
      text_names_( extract_colnames( _obj, "text_names_" ) )
{
    check_lengths();
}

// ----------------------------------------------------------------------------

MappingContainer::~MappingContainer() = default;

// ----------------------------------------------------------------------------

std::string MappingContainer::categorical_or_text_column_to_sql(
    const std::shared_ptr<const std::vector<strings::String>>& _categories,
    const std::string& _name,
    const PtrType& _ptr,
    const size_t _target_num ) const
{
    assert_true( _categories );

    assert_true( _ptr );

    const auto pairs = make_pairs( *_ptr, _target_num );

    std::string sql = make_table_header( _name, false );

    for ( size_t i = 0; i < pairs.size(); ++i )
        {
            const std::string begin = ( i == 0 ) ? "" : "      ";

            const auto& p = pairs.at( i );

            assert_true( p.first >= 0 );

            assert_true( static_cast<size_t>( p.first ) < _categories->size() );

            const std::string end =
                ( i == pairs.size() - 1 ) ? ";\n\n\n" : ",\n";

            sql += begin + "('" + _categories->at( p.first ).str() + "', " +
                   io::Parser::to_precise_string( p.second ) + ")" + end;
        }

    return sql;
}

// ----------------------------------------------------------------------------

std::vector<std::string> MappingContainer::categorical_to_sql(
    const std::shared_ptr<const std::vector<strings::String>>& _categories,
    const std::string& _feature_prefix,
    const std::function<void( const std::string&, const std::string& )>&
        _add_to_map ) const
{
    std::vector<std::string> sql;

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

                            sql.push_back( categorical_or_text_column_to_sql(
                                _categories, name, ptr, t ) );

                            _add_to_map( table_names_->at( i ), name );
                        }
                }
        }

    return sql;
}

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

std::string MappingContainer::discrete_column_to_sql(
    const std::string& _name,
    const PtrType& _ptr,
    const size_t _target_num ) const
{
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
}

// ----------------------------------------------------------------------------

std::vector<std::string> MappingContainer::discrete_to_sql(
    const std::string& _feature_prefix,
    const std::function<void( const std::string&, const std::string& )>&
        _add_to_map ) const
{
    std::vector<std::string> sql;

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

                            sql.push_back(
                                discrete_column_to_sql( name, ptr, t ) );

                            _add_to_map( table_names_->at( i ), name );
                        }
                }
        }

    return sql;
}

// ----------------------------------------------------------------------------

std::vector<typename MappingContainer::Colnames>
MappingContainer::extract_colnames(
    const Poco::JSON::Object& _obj, const std::string& _key )
{
    auto arr = _obj.getArray( _key );

    throw_unless( arr, "Expected array called '" + _key + "'" );

    auto colnames = std::vector<Colnames>();

    for ( size_t i = 0; i < arr->size(); ++i )
        {
            const auto ptr = arr->getArray( i );

            auto vec = std::make_shared<std::vector<std::string>>(
                jsonutils::JSON::array_to_vector<std::string>( ptr ) );

            colnames.emplace_back( std::move( vec ) );
        }

    return colnames;
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

typename MappingContainer::TableNames MappingContainer::extract_table_names(
    const Poco::JSON::Object& _obj )
{
    auto arr = _obj.getArray( "table_names_" );

    throw_unless( arr, "Expected array called table_names_'" );

    return std::make_shared<typename TableNames::element_type>(
        jsonutils::JSON::array_to_vector<std::string>( arr ) );
}

// ------------------------------------------------------------------------

std::vector<std::pair<Int, Float>> MappingContainer::make_pairs(
    const Map& _m, const size_t _target_num ) const
{
    using Pair = std::pair<Int, Float>;

    auto pairs = std::vector<Pair>();

    for ( const auto& p : _m )
        {
            assert_true( _target_num < p.second.size() );
            pairs.push_back(
                std::make_pair( p.first, p.second.at( _target_num ) ) );
        }

    const auto by_value = []( const Pair& _p1, const Pair& _p2 ) -> bool {
        return _p1.second > _p2.second;
    };

    std::sort( pairs.begin(), pairs.end(), by_value );

    return pairs;
}

// ----------------------------------------------------------------------------

std::string MappingContainer::make_table_header(
    const std::string& _name, const bool _key_is_num ) const
{
    std::string sql = "DROP TABLE IF EXISTS \"" + _name + "\";\n\n";

    const std::string key_type = _key_is_num ? "INTEGER" : "TEXT";

    sql += "CREATE TABLE \"" + _name + "\"(key " + key_type +
           " NOT NULL PRIMARY KEY, value REAL);\n\n";

    sql += "INSERT INTO \"" + _name + "\"(key, value)\nVALUES";

    return sql;
}

// ----------------------------------------------------------------------------

std::vector<std::string> MappingContainer::subcontainers_to_sql(
    const std::shared_ptr<const std::vector<strings::String>>& _categories,
    const VocabularyTree& _vocabulary_tree,
    const std::string& _feature_prefix,
    const std::function<void( const ColnameMap& )>& _merge_map ) const
{
    assert_true( _vocabulary_tree.subtrees().size() == subcontainers_.size() );

    std::vector<std::string> sql;

    for ( size_t i = 0; i < subcontainers_.size(); ++i )
        {
            const auto& s = subcontainers_.at( i );

            if ( s )
                {
                    const auto feature_prefix =
                        _feature_prefix + std::to_string( i + 1 ) + "_";

                    assert_true( _vocabulary_tree.subtrees().at( i ) );

                    const auto& vocab =
                        _vocabulary_tree.subtrees().at( i ).value();

                    const auto [subfeatures, submap] =
                        s->to_sql( _categories, vocab, feature_prefix );

                    sql.insert(
                        sql.end(), subfeatures.begin(), subfeatures.end() );

                    _merge_map( submap );
                }
        }

    return sql;
}

// ----------------------------------------------------------------------------

std::vector<std::string> MappingContainer::text_to_sql(
    const std::vector<VocabForDf>& _vocab,
    const std::string& _feature_prefix,
    const std::function<void( const std::string&, const std::string& )>&
        _add_to_map ) const
{
    assert_true( _vocab.size() == text_.size() );

    std::vector<std::string> sql;

    for ( size_t i = 0; i < text_.size(); ++i )
        {
            const auto& txt = text_.at( i );

            const auto& names = text_names_.at( i );

            const auto& voc = _vocab.at( i );

            const auto feature_prefix =
                _feature_prefix + std::to_string( i + 1 ) + "_";

            assert_true( names );

            assert_true( txt.size() == names->size() );

            assert_true( txt.size() == voc.size() );

            const auto num_targets =
                MappingContainerMaker::infer_num_targets( txt );

            for ( size_t t = 0; t < num_targets; ++t )
                {
                    for ( size_t j = 0; j < txt.size(); ++j )
                        {
                            const auto& ptr = txt.at( j );

                            const auto name = SQLGenerator::to_upper(
                                MappingContainerMaker::make_colname(
                                    names->at( j ), feature_prefix, t ) );

                            sql.push_back( categorical_or_text_column_to_sql(
                                _vocab.at( i ).at( j ), name, ptr, t ) );

                            _add_to_map( table_names_->at( i ), name );
                        }
                }
        }

    return sql;
}

// ----------------------------------------------------------------------------

Poco::JSON::Object::Ptr MappingContainer::to_json_obj() const
{
    assert_true( table_names_ );

    auto obj = Poco::JSON::Object::Ptr( new Poco::JSON::Object() );

    obj->set( "categorical_", transform_mapping_vec( categorical_ ) );

    obj->set( "categorical_names_", transform_colnames( categorical_names_ ) );

    obj->set( "discrete_", transform_mapping_vec( discrete_ ) );

    obj->set( "discrete_names_", transform_colnames( discrete_names_ ) );

    obj->set( "subcontainers_", transform_subcontainers( subcontainers_ ) );

    obj->set(
        "table_names_", jsonutils::JSON::vector_to_array_ptr( *table_names_ ) );

    obj->set( "text_", transform_mapping_vec( text_ ) );

    obj->set( "text_names_", transform_colnames( text_names_ ) );

    return obj;
}

// ----------------------------------------------------------------------------

std::pair<std::vector<std::string>, typename MappingContainer::ColnameMap>
MappingContainer::to_sql(
    const std::shared_ptr<const std::vector<strings::String>>& _categories,
    const VocabularyTree& _vocabulary_tree,
    const std::string& _feature_prefix ) const
{
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

    const auto categorical =
        categorical_to_sql( _categories, _feature_prefix, add_to_map );

    const auto discrete = discrete_to_sql( _feature_prefix, add_to_map );

    const auto text = text_to_sql(
        _vocabulary_tree.peripheral(), _feature_prefix, add_to_map );

    const auto subcontainers = subcontainers_to_sql(
        _categories, _vocabulary_tree, _feature_prefix, merge_map );

    // ------------------------------------------------------------------------

    const auto all = std::vector<std::vector<std::string>>(
        { categorical, discrete, text, subcontainers } );

    const auto sql =
        stl::make::vector<std::string>( all | std::ranges::views::join );

    // ------------------------------------------------------------------------

    return std::make_pair( sql, colname_map );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

Poco::JSON::Array::Ptr MappingContainer::transform_colnames(
    const std::vector<Colnames>& _colnames ) const
{
    auto arr = Poco::JSON::Array::Ptr( new Poco::JSON::Array() );
    for ( const auto& c : _colnames )
        {
            assert_true( c );
            arr->add( jsonutils::JSON::vector_to_array_ptr( *c ) );
        }

    return arr;
}

// ----------------------------------------------------------------------------

Poco::JSON::Array::Ptr MappingContainer::transform_mapping_vec(
    const std::vector<MappingForDf>& _mapping_vec ) const
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

    auto arr = Poco::JSON::Array::Ptr( new Poco::JSON::Array() );

    for ( const auto& vec : _mapping_vec )
        {
            arr->add( transform_mapping( vec ) );
        }

    return arr;

    // --------------------------------------------------------------
}

// ----------------------------------------------------------------------------

Poco::JSON::Array::Ptr MappingContainer::transform_subcontainers(
    const std::vector<std::shared_ptr<const MappingContainer>>& _subcontainers )
    const
{
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
}

// ----------------------------------------------------------------------------
}  // namespace helpers
