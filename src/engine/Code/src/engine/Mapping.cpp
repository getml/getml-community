#include "engine/preprocessors/preprocessors.hpp"

namespace engine
{
namespace preprocessors
{
// ----------------------------------------------------

std::vector<std::string> Mapping::categorical_columns_to_sql(
    const std::shared_ptr<const std::vector<strings::String>>& _categories )
    const
{
    assert_true( categorical_names_ );

    const auto mapping_to_sql = [this, &_categories](
                                    const size_t _i,
                                    const size_t _weight_num ) -> std::string {
        const auto name = helpers::SQLGenerator::make_colname(
            helpers::MappingContainerMaker::make_colname(
                categorical_names_->at( _i ), "", aggregation_, _weight_num ) );

        return helpers::MappingContainer::categorical_or_text_column_to_sql(
            _categories,
            helpers::SQLGenerator::to_upper( name ),
            categorical_.at( _i ),
            _weight_num );
    };

    return columns_to_sql( mapping_to_sql, categorical_, categorical_names_ );
}

// ----------------------------------------------------

std::vector<std::string> Mapping::discrete_columns_to_sql() const
{
    assert_true( discrete_names_ );

    const auto mapping_to_sql =
        [this]( const size_t _i, const size_t _weight_num ) -> std::string {
        const auto name = helpers::SQLGenerator::make_colname(
            helpers::MappingContainerMaker::make_colname(
                discrete_names_->at( _i ), "", aggregation_, _weight_num ) );

        return helpers::MappingContainer::discrete_column_to_sql(
            helpers::SQLGenerator::to_upper( name ),
            discrete_.at( _i ),
            _weight_num );
    };

    return columns_to_sql( mapping_to_sql, discrete_, discrete_names_ );
}

// ----------------------------------------------------

typename Mapping::TextMapping Mapping::extract_text_mapping(
    const Poco::JSON::Object& _obj, const std::string& _key ) const
{
    // --------------------------------------------------------------

    const auto obj_to_map = []( const Poco::JSON::Object& _obj ) {
        auto m = std::make_shared<std::map<std::string, std::vector<Float>>>();

        for ( const auto& [key, _] : _obj )
            {
                const auto arr = _obj.getArray( key );

                assert_msg(
                    arr,
                    "key: " + key +
                        ", _obj: " + jsonutils::JSON::stringify( _obj ) );

                ( *m )[key] = jsonutils::JSON::array_to_vector<Float>( arr );
            }

        return m;
    };

    // --------------------------------------------------------------

    const auto arr = *JSON::get_array( _obj, _key );

    const auto get_obj = [&arr]( const size_t _i ) -> Poco::JSON::Object {
        auto ptr = arr.getObject( _i );
        throw_unless( ptr, "Expected an object inside the mapping." );
        return *ptr;
    };

    // --------------------------------------------------------------

    const auto iota = std::views::iota( static_cast<size_t>( 0 ), arr.size() );

    const auto range = iota | std::views::transform( get_obj ) |
                       std::views::transform( obj_to_map );

    return stl::collect::vector<TextMapping::value_type>( range );
}

// ----------------------------------------------------

Poco::JSON::Object::Ptr Mapping::fingerprint() const
{
    auto obj = Poco::JSON::Object::Ptr( new Poco::JSON::Object() );

    obj->set( "type_", type() );

    obj->set( "dependencies_", JSON::vector_to_array_ptr( dependencies_ ) );

    obj->set( "aggregation_", JSON::vector_to_array_ptr( aggregation_ ) );

    obj->set( "min_freq_", min_freq_ );

    return obj;
}

// ----------------------------------------------------------------------------

std::pair<typename Mapping::MappingForDf, typename Mapping::Colnames>
Mapping::fit_on_categoricals( const containers::DataFrame& _data_frame ) const
{
    const auto get_categorical =
        [&_data_frame]( const size_t _i ) -> containers::Column<Int> {
        return _data_frame.categorical( _i );
    };

    const auto col_to_mapping =
        [this, &_data_frame]( const containers::Column<Int>& _col ) {
            const auto rownum_map = make_rownum_map( _col );
            return make_mapping( rownum_map, _data_frame );
        };

    const auto get_colname =
        []( const containers::Column<Int>& _col ) -> std::string {
        return _col.name();
    };

    const auto iota = std::views::iota(
        static_cast<size_t>( 0 ), _data_frame.num_categoricals() );

    const auto range1 = iota | std::views::transform( get_categorical ) |
                        std::views::transform( col_to_mapping );

    const auto range2 = iota | std::views::transform( get_categorical ) |
                        std::views::transform( get_colname );

    const auto mappings =
        stl::collect::vector<MappingForDf::value_type>( range1 );

    const auto colnames = std::make_shared<const std::vector<std::string>>(
        stl::collect::vector<std::string>( range2 ) );

    return std::make_pair( mappings, colnames );
}

// ----------------------------------------------------------------------------

std::pair<typename Mapping::MappingForDf, typename Mapping::Colnames>
Mapping::fit_on_discretes( const containers::DataFrame& _data_frame ) const
{
    const auto get_numerical =
        [&_data_frame]( const size_t _i ) -> containers::Column<Float> {
        return _data_frame.numerical( _i );
    };

    const auto is_full_number = []( const Float _val ) -> bool {
        return helpers::NullChecker::is_null( _val ) |
               ( std::floor( _val ) == _val );
    };

    const auto is_discrete =
        [is_full_number]( const containers::Column<Float>& _col ) -> bool {
        return std::all_of( _col.begin(), _col.end(), is_full_number );
    };

    const auto col_to_mapping =
        [this, &_data_frame]( const containers::Column<Float>& _col ) {
            const auto rownum_map = make_rownum_map( _col );
            return make_mapping( rownum_map, _data_frame );
        };

    const auto get_colname =
        []( const containers::Column<Float>& _col ) -> std::string {
        return _col.name();
    };

    const auto iota = std::views::iota(
        static_cast<size_t>( 0 ), _data_frame.num_numericals() );

    const auto range1 = iota | std::views::transform( get_numerical ) |
                        std::views::filter( is_discrete );

    const auto discrete_cols =
        stl::collect::vector<containers::Column<Float>>( range1 );

    const auto range2 = discrete_cols | std::views::transform( col_to_mapping );

    const auto range3 = discrete_cols | std::views::transform( get_colname );

    const auto mappings =
        stl::collect::vector<MappingForDf::value_type>( range2 );

    const auto colnames = std::make_shared<const std::vector<std::string>>(
        stl::collect::vector<std::string>( range3 ) );

    return std::make_pair( mappings, colnames );
}

// ----------------------------------------------------------------------------

std::pair<typename Mapping::TextMapping, typename Mapping::Colnames>
Mapping::fit_on_text( const containers::DataFrame& _data_frame ) const
{
    const auto get_text =
        [&_data_frame](
            const size_t _i ) -> containers::Column<strings::String> {
        return _data_frame.text( _i );
    };

    const auto col_to_mapping =
        [this,
         &_data_frame]( const containers::Column<strings::String>& _col ) {
            const auto rownum_map = make_rownum_map( _col );
            return make_mapping( rownum_map, _data_frame );
        };

    const auto get_colname =
        []( const containers::Column<strings::String>& _col ) -> std::string {
        return _col.name();
    };

    const auto iota =
        std::views::iota( static_cast<size_t>( 0 ), _data_frame.num_text() );

    const auto range1 = iota | std::views::transform( get_text ) |
                        std::views::transform( col_to_mapping );

    const auto range2 = iota | std::views::transform( get_text ) |
                        std::views::transform( get_colname );

    const auto mappings =
        stl::collect::vector<TextMapping::value_type>( range1 );

    const auto colnames = std::make_shared<const std::vector<std::string>>(
        stl::collect::vector<std::string>( range2 ) );

    return std::make_pair( mappings, colnames );
}

// ----------------------------------------------------

std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
Mapping::fit_transform(
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<containers::Encoding>& _categories,
    const containers::DataFrame& _population_df,
    const std::vector<containers::DataFrame>& _peripheral_dfs )
{
    assert_true( _categories );

    std::tie( categorical_, categorical_names_ ) =
        fit_on_categoricals( _population_df );

    std::tie( discrete_, discrete_names_ ) = fit_on_discretes( _population_df );

    std::tie( text_, text_names_ ) = fit_on_text( _population_df );

    return transform( _cmd, _categories, _population_df, _peripheral_dfs );
}

// ----------------------------------------------------

Mapping Mapping::from_json_obj( const Poco::JSON::Object& _obj ) const
{
    const auto parse =
        []( const std::string& _str ) -> helpers::MappingAggregation {
        return helpers::MappingContainerMaker::parse_aggregation( _str );
    };

    const auto extract_mapping_vector = []( const Poco::JSON::Object& _obj,
                                            const std::string& _name ) {
        const auto vec =
            helpers::MappingContainer::extract_mapping_vector( _obj, _name );
        throw_unless(
            vec.size() == 1, "Unexpected mapping vector length in Mapping" );
        return vec.at( 0 );
    };

    const auto extract_colnames = []( const Poco::JSON::Object& _obj,
                                      const std::string& _name ) {
        const auto vec =
            helpers::MappingContainer::extract_colnames( _obj, _name );
        throw_unless(
            vec.size() == 1, "Unexpected colname length length in Mapping" );
        return vec.at( 0 );
    };

    Mapping that;

    that.aggregation_ = JSON::array_to_vector<std::string>(
        JSON::get_array( _obj, "aggregation_" ) );

    that.aggregation_enums_ = stl::collect::vector<helpers::MappingAggregation>(
        that.aggregation_ | std::views::transform( parse ) );

    that.min_freq_ = JSON::get_value<size_t>( _obj, "min_freq_" );

    if ( _obj.has( "categorical_" ) )
        {
            that.categorical_ = extract_mapping_vector( _obj, "categorical_" );

            that.categorical_names_ =
                extract_colnames( _obj, "categorical_names_" );
        }

    if ( _obj.has( "discrete_" ) )
        {
            that.discrete_ = extract_mapping_vector( _obj, "discrete_" );

            that.discrete_names_ = extract_colnames( _obj, "discrete_names_" );
        }

    if ( _obj.has( "text_" ) )
        {
            that.text_ = extract_text_mapping( _obj, "text_" );

            that.text_names_ = extract_colnames( _obj, "text_names_" );
        }

    return that;
}

// ----------------------------------------------------

std::vector<containers::Column<Float>> Mapping::make_mapping_columns_int(
    const std::pair<containers::Column<Int>, MappingForDf::value_type>& _p )
    const
{
    // ----------------------------------------------------

    const auto map_value =
        []( const std::map<Int, std::vector<Float>>& _mapping,
            const size_t _weight_num,
            const Int& _val ) -> Float {
        const auto it = _mapping.find( _val );

        if ( it == _mapping.end() )
            {
                return 0.0;
            }

        assert_true( _weight_num < it->second.size() );

        return it->second.at( _weight_num );
    };

    // ----------------------------------------------------

    const auto make_mapping_column =
        [this, map_value, &_p](
            const size_t _weight_num ) -> containers::Column<Float> {
        const auto& col = _p.first;
        const auto& mapping = *_p.second;

        const auto get_val =
            std::bind( map_value, mapping, _weight_num, std::placeholders::_1 );

        const auto range = col | std::views::transform( get_val );

        const auto ptr = std::make_shared<std::vector<Float>>(
            stl::collect::vector<Float>( range ) );

        const auto name = helpers::MappingContainerMaker::make_colname(
            col.name(), "", aggregation_, _weight_num );

        return containers::Column<Float>( ptr, name );
    };

    // ----------------------------------------------------

    assert_true( _p.second );

    if ( _p.second->size() <= 1 )
        {
            return std::vector<containers::Column<Float>>();
        }

    const auto num_weights = _p.second->begin()->second.size();

    const auto iota = std::views::iota( static_cast<size_t>( 0 ), num_weights );

    return stl::collect::vector<containers::Column<Float>>(
        iota | std::views::transform( make_mapping_column ) );

    // ----------------------------------------------------
}

// ----------------------------------------------------

std::vector<containers::Column<Float>> Mapping::make_mapping_columns_text(
    const std::pair<
        containers::Column<strings::String>,
        TextMapping::value_type>& _p ) const
{
    // ----------------------------------------------------

    const auto map_word =
        []( const std::map<std::string, std::vector<Float>>& _mapping,
            const size_t _weight_num,
            const std::string& _word ) -> Float {
        const auto it = _mapping.find( _word );

        if ( it == _mapping.end() )
            {
                return NAN;
            }

        assert_true( _weight_num < it->second.size() );

        return it->second.at( _weight_num );
    };

    // ----------------------------------------------------

    const auto map_text_field =
        [map_word](
            const std::map<std::string, std::vector<Float>>& _mapping,
            const size_t _weight_num,
            const strings::String& _text_field ) -> Float {
        const auto words =
            textmining::Vocabulary::process_text_field( _text_field );

        const auto map =
            std::bind( map_word, _mapping, _weight_num, std::placeholders::_1 );

        auto range = words | std::views::transform( map );

        const auto agg =
            helpers::Aggregations::avg( range.begin(), range.end() );

        if ( helpers::NullChecker::is_null( agg ) )
            {
                return 0.0;
            }

        return agg;
    };

    // ----------------------------------------------------

    const auto make_mapping_column =
        [this, map_text_field, &_p](
            const size_t _weight_num ) -> containers::Column<Float> {
        const auto& col = _p.first;
        const auto& mapping = *_p.second;

        const auto get_val = std::bind(
            map_text_field, mapping, _weight_num, std::placeholders::_1 );

        const auto range = col | std::views::transform( get_val );

        const auto ptr = std::make_shared<std::vector<Float>>(
            stl::collect::vector<Float>( range ) );

        assert_msg(
            ptr->size() == col.nrows(),
            "ptr->size(): " + std::to_string( ptr->size() ) +
                ", col.nrows(): " + std::to_string( col.nrows() ) );

        const auto name = helpers::MappingContainerMaker::make_colname(
            col.name(), "", aggregation_, _weight_num );

        return containers::Column<Float>( ptr, name );
    };

    // ----------------------------------------------------

    assert_true( _p.second );

    if ( _p.second->size() <= 1 )
        {
            return std::vector<containers::Column<Float>>();
        }

    const auto num_weights = _p.second->begin()->second.size();

    const auto iota = std::views::iota( static_cast<size_t>( 0 ), num_weights );

    return stl::collect::vector<containers::Column<Float>>(
        iota | std::views::transform( make_mapping_column ) );

    // ----------------------------------------------------
}

// ----------------------------------------------------------------------------

std::map<strings::String, std::vector<size_t>> Mapping::make_rownum_map_text(
    const helpers::Column<strings::String>& _col ) const
{
    // ---------------------------------------------------------------

    std::map<strings::String, std::vector<size_t>> rownum_map;

    // ---------------------------------------------------------------

    const auto insert_word =
        [&rownum_map]( const std::string& _word, const size_t _rownum ) {
            const auto it = rownum_map.find( _word );

            if ( it == rownum_map.end() )
                {
                    rownum_map[_word] = { _rownum };
                }
            else
                {
                    it->second.push_back( _rownum );
                }
        };

    // ---------------------------------------------------------------

    const auto insert_text_field = [insert_word](
                                       const strings::String& _text_field,
                                       const size_t _rownum ) {
        const auto words =
            textmining::Vocabulary::process_text_field( _text_field );

        for ( const auto& word : words )
            {
                insert_word( word, _rownum );
            }
    };

    // ---------------------------------------------------------------

    for ( size_t rownum = 0; rownum < _col.nrows_; ++rownum )
        {
            insert_text_field( _col[rownum], rownum );
        }

    // ---------------------------------------------------------------

    return rownum_map;
}

// ----------------------------------------------------

std::vector<std::string> Mapping::text_columns_to_sql() const
{
    assert_true( text_names_ );

    using Map = std::map<std::string, std::vector<Float>>;

    using PtrType = std::shared_ptr<const Map>;

    // ----------------------------------------------------

    const auto make_pairs = []( const Map& _m, const size_t _target_num )
        -> std::vector<std::pair<std::string, Float>> {
        using Pair = std::pair<std::string, Float>;

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
    };

    // ----------------------------------------------------

    const auto text_column_to_sql = [make_pairs](
                                        const std::string& _name,
                                        const PtrType& _ptr,
                                        const size_t _target_num ) {
        assert_true( _ptr );

        const auto pairs = make_pairs( *_ptr, _target_num );

        std::string sql =
            helpers::MappingContainer::make_table_header( _name, false );

        for ( size_t i = 0; i < pairs.size(); ++i )
            {
                const std::string begin = ( i == 0 ) ? "" : "      ";

                const auto& p = pairs.at( i );

                const std::string end =
                    ( i == pairs.size() - 1 ) ? ";\n\n\n" : ",\n";

                sql += begin + "('" + p.first + "', " +
                       io::Parser::to_precise_string( p.second ) + ")" + end;
            }

        return sql;
    };

    // ----------------------------------------------------

    const auto mapping_to_sql = [this, text_column_to_sql](
                                    const size_t _i,
                                    const size_t _weight_num ) -> std::string {
        const auto name = helpers::SQLGenerator::make_colname(
            helpers::MappingContainerMaker::make_colname(
                text_names_->at( _i ), "", aggregation_, _weight_num ) );

        return text_column_to_sql(
            helpers::SQLGenerator::to_upper( name ),
            text_.at( _i ),
            _weight_num );
    };

    // ----------------------------------------------------

    return columns_to_sql( mapping_to_sql, text_, text_names_ );
}

// ----------------------------------------------------

Poco::JSON::Object::Ptr Mapping::to_json_obj() const
{
    auto obj = Poco::JSON::Object::Ptr( new Poco::JSON::Object() );

    obj->set( "type_", type() );

    obj->set( "aggregation_", JSON::vector_to_array_ptr( aggregation_ ) );

    obj->set( "min_freq_", min_freq_ );

    obj->set(
        "categorical_",
        helpers::MappingContainer::transform_mapping_vec( { categorical_ } ) );

    obj->set(
        "categorical_names_",
        helpers::MappingContainer::transform_colnames(
            { categorical_names_ } ) );

    obj->set(
        "discrete_",
        helpers::MappingContainer::transform_mapping_vec( { discrete_ } ) );

    obj->set(
        "discrete_names_",
        helpers::MappingContainer::transform_colnames( { discrete_names_ } ) );

    obj->set( "text_", transform_text_mapping( text_ ) );

    obj->set(
        "text_names_",
        helpers::MappingContainer::transform_colnames( { text_names_ } ) );

    return obj;
}

// ----------------------------------------------------

std::vector<std::string> Mapping::to_sql(
    const std::shared_ptr<const std::vector<strings::String>>& _categories )
    const
{
    const auto categorical = categorical_columns_to_sql( _categories );

    const auto discrete = discrete_columns_to_sql();

    const auto text = text_columns_to_sql();

    const auto all = std::vector<std::vector<std::string>>(
        { categorical, discrete, text } );

    return stl::collect::vector<std::string>( all | std::views::join );
}

// ----------------------------------------------------

std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
Mapping::transform(
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<const containers::Encoding> _categories,
    const containers::DataFrame& _population_df,
    const std::vector<containers::DataFrame>& _peripheral_dfs ) const
{
    const auto add_columns =
        []( const std::vector<containers::Column<Float>>& _cols,
            containers::DataFrame* _df ) {
            for ( const auto& col : _cols )
                {
                    _df->add_float_column(
                        col, containers::DataFrame::ROLE_NUMERICAL );
                }
        };

    const auto categorical_mappings = transform_categorical( _population_df );

    const auto discrete_mappings = transform_discrete( _population_df );

    const auto text_mappings = transform_text( _population_df );

    auto population_df = _population_df;

    add_columns( categorical_mappings, &population_df );

    add_columns( discrete_mappings, &population_df );

    add_columns( text_mappings, &population_df );

    return std::make_pair( population_df, _peripheral_dfs );
}

// ----------------------------------------------------

std::vector<containers::Column<Float>> Mapping::transform_categorical(
    const containers::DataFrame& _df ) const
{
    using Pair = std::pair<containers::Column<Int>, MappingForDf::value_type>;

    const auto get_column =
        [&_df, this]( const size_t _i ) -> containers::Column<Int> {
        assert_true( categorical_names_ );
        assert_true( _i < categorical_names_->size() );
        const auto& name = categorical_names_->at( _i );
        return _df.categorical( name );
    };

    const auto get_mapping =
        [this]( const size_t _i ) -> MappingForDf::value_type {
        assert_true( _i < categorical_.size() );
        return categorical_.at( _i );
    };

    const auto make_pair = [get_column,
                            get_mapping]( const size_t _i ) -> Pair {
        return std::make_pair( get_column( _i ), get_mapping( _i ) );
    };

    assert_true( categorical_names_ );

    assert_true( categorical_.size() == categorical_names_->size() );

    const auto make_cols = [this]( const Pair& _p ) {
        return make_mapping_columns_int( _p );
    };

    const auto iota =
        std::views::iota( static_cast<size_t>( 0 ), categorical_.size() );

    const auto range = iota | std::views::transform( make_pair ) |
                       std::views::transform( make_cols );

    const auto all =
        stl::collect::vector<std::vector<containers::Column<Float>>>( range );

    return stl::collect::vector<containers::Column<Float>>(
        all | std::ranges::views::join );
}

// ----------------------------------------------------

std::vector<containers::Column<Float>> Mapping::transform_discrete(
    const containers::DataFrame& _df ) const
{
    using Pair = std::pair<containers::Column<Int>, MappingForDf::value_type>;

    const auto cast_as_int = []( const Float _val ) -> Int {
        return static_cast<Int>( _val );
    };

    const auto get_column = [&_df, cast_as_int, this](
                                const size_t _i ) -> containers::Column<Int> {
        assert_true( discrete_names_ );
        assert_true( _i < discrete_names_->size() );

        const auto& name = discrete_names_->at( _i );

        const auto col = _df.numerical( name );

        const auto range = col | std::views::transform( cast_as_int );

        const auto ptr = std::make_shared<std::vector<Int>>(
            stl::collect::vector<Int>( range ) );

        return containers::Column<Int>( ptr, col.name() );
    };

    const auto get_mapping =
        [this]( const size_t _i ) -> MappingForDf::value_type {
        assert_true( _i < discrete_.size() );
        return discrete_.at( _i );
    };

    const auto make_pair = [get_column,
                            get_mapping]( const size_t _i ) -> Pair {
        return std::make_pair( get_column( _i ), get_mapping( _i ) );
    };

    assert_true( discrete_names_ );

    assert_true( discrete_.size() == discrete_names_->size() );

    const auto make_cols = [this]( const Pair& _p ) {
        return make_mapping_columns_int( _p );
    };

    const auto iota =
        std::views::iota( static_cast<size_t>( 0 ), discrete_.size() );

    const auto range = iota | std::views::transform( make_pair ) |
                       std::views::transform( make_cols );

    const auto all =
        stl::collect::vector<std::vector<containers::Column<Float>>>( range );

    return stl::collect::vector<containers::Column<Float>>(
        all | std::ranges::views::join );
}

// ----------------------------------------------------

std::vector<containers::Column<Float>> Mapping::transform_text(
    const containers::DataFrame& _df ) const
{
    using Pair =
        std::pair<containers::Column<strings::String>, TextMapping::value_type>;

    const auto get_column =
        [&_df, this]( const size_t _i ) -> containers::Column<strings::String> {
        assert_true( text_names_ );
        assert_true( _i < text_names_->size() );
        const auto& name = text_names_->at( _i );
        return _df.text( name );
    };

    const auto get_mapping =
        [this]( const size_t _i ) -> TextMapping::value_type {
        assert_true( _i < text_.size() );
        return text_.at( _i );
    };

    const auto make_pair = [get_column,
                            get_mapping]( const size_t _i ) -> Pair {
        return std::make_pair( get_column( _i ), get_mapping( _i ) );
    };

    assert_true( text_names_ );

    assert_true( text_.size() == text_names_->size() );

    const auto make_cols = [this]( const Pair& _p ) {
        return make_mapping_columns_text( _p );
    };

    const auto iota =
        std::views::iota( static_cast<size_t>( 0 ), text_.size() );

    const auto range = iota | std::views::transform( make_pair ) |
                       std::views::transform( make_cols );

    const auto all =
        stl::collect::vector<std::vector<containers::Column<Float>>>( range );

    return stl::collect::vector<containers::Column<Float>>(
        all | std::ranges::views::join );
}

// ----------------------------------------------------

Poco::JSON::Array::Ptr Mapping::transform_text_mapping(
    const TextMapping& _mapping ) const
{
    // --------------------------------------------------------------

    const auto map_to_object =
        []( const std::map<std::string, std::vector<Float>>& _map ) {
            Poco::JSON::Object::Ptr obj( new Poco::JSON::Object() );

            for ( const auto& [key, value] : _map )
                {
                    obj->set(
                        key, jsonutils::JSON::vector_to_array_ptr( value ) );
                }

            return obj;
        };

    // --------------------------------------------------------------

    auto arr = Poco::JSON::Array::Ptr( new Poco::JSON::Array() );

    for ( const auto& ptr : _mapping )
        {
            assert_true( ptr );
            arr->add( map_to_object( *ptr ) );
        }

    return arr;
}

// ----------------------------------------------------
}  // namespace preprocessors
}  // namespace engine
