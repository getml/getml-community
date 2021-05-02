#include "engine/preprocessors/preprocessors.hpp"

namespace engine
{
namespace preprocessors
{
// ----------------------------------------------------

std::tuple<
    helpers::DataFrame,
    helpers::TableHolder,
    std::shared_ptr<const helpers::VocabularyContainer>>
Mapping::build_prerequisites(
    const containers::DataFrame& _population_df,
    const std::vector<containers::DataFrame>& _peripheral_dfs,
    const helpers::Placeholder& _placeholder,
    const std::vector<std::string>& _peripheral_names ) const
{
    const auto to_immutable =
        []( const containers::DataFrame& _df ) -> helpers::DataFrame {
        return _df.to_immutable<helpers::DataFrame>();
    };

    const auto population = to_immutable( _population_df );

    const auto peripheral = stl::collect::vector<helpers::DataFrame>(
        _peripheral_dfs | std::views::transform( to_immutable ) );

    const auto [vocabulary, word_index_container] =
        handle_text_fields( population, peripheral );

    const auto rownums = std::make_shared<std::vector<size_t>>(
        stl::collect::vector<size_t>( std::views::iota(
            static_cast<size_t>( 0 ), population.nrows() ) ) );

    const auto population_view = helpers::DataFrameView( population, rownums );

    const auto table_holder = helpers::TableHolder(
        _placeholder,
        population_view,
        peripheral,
        _peripheral_names,
        std::nullopt,
        word_index_container );

    return std::make_tuple( population, table_holder, vocabulary );
}

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

std::vector<size_t> Mapping::find_output_ix(
    const std::vector<size_t>& _input_ix,
    const helpers::DataFrame& _output_table,
    const helpers::DataFrame& _input_table ) const
{
    const auto time_stamp_in_range =
        []( const Float _time_stamp_input,
            const Float _upper_time_stamp,
            const Float _time_stamp_output ) -> bool {
        return (
            ( _time_stamp_input <= _time_stamp_output ) &&
            ( std::isnan( _upper_time_stamp ) ||
              _time_stamp_output < _upper_time_stamp ) );
    };

    std::set<size_t> unique;

    for ( const auto ix : _input_ix )
        {
            if ( !_output_table.has( _input_table.join_key( ix ) ) )
                {
                    continue;
                }

            const auto it = _output_table.find( _input_table.join_key( ix ) );

            const auto time_stamp_input = _input_table.time_stamp( ix );

            const auto upper_time_stamp = _input_table.upper_time_stamp( ix );

            for ( const auto ix_out : it->second )
                {
                    assert_true( ix_out >= 0 );
                    assert_true( ix_out < _output_table.nrows() );

                    const bool use_this = time_stamp_in_range(
                        time_stamp_input,
                        upper_time_stamp,
                        _output_table.time_stamp( ix_out ) );

                    if ( use_this )
                        {
                            unique.insert( static_cast<size_t>( ix_out ) );
                        }
                }
        }

    return std::vector<size_t>( unique.begin(), unique.end() );
}

// ----------------------------------------------------

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
Mapping::fit_on_categoricals(
    const helpers::DataFrame& _population,
    const std::vector<helpers::DataFrame>& _main_tables,
    const std::vector<helpers::DataFrame>& _peripheral_tables ) const
{
    const auto col_to_mapping =
        [this, &_population, &_main_tables, &_peripheral_tables](
            const helpers::Column<Int>& _col ) {
            const auto rownum_map = make_rownum_map( _col );
            return make_mapping(
                rownum_map, _population, _main_tables, _peripheral_tables );
        };

    const auto get_colname =
        []( const helpers::Column<Int>& _col ) -> std::string {
        return _col.name_;
    };

    const auto& data_frame =
        _peripheral_tables.size() > 0 ? _peripheral_tables.back() : _population;

    const auto range1 =
        data_frame.categoricals_ | std::views::transform( col_to_mapping );

    const auto range2 =
        data_frame.categoricals_ | std::views::transform( get_colname );

    const auto mappings =
        stl::collect::vector<MappingForDf::value_type>( range1 );

    const auto colnames = std::make_shared<const std::vector<std::string>>(
        stl::collect::vector<std::string>( range2 ) );

    return std::make_pair( mappings, colnames );
}

// ----------------------------------------------------------------------------

std::pair<typename Mapping::MappingForDf, typename Mapping::Colnames>
Mapping::fit_on_discretes(
    const helpers::DataFrame& _population,
    const std::vector<helpers::DataFrame>& _main_tables,
    const std::vector<helpers::DataFrame>& _peripheral_tables ) const
{
    const auto col_to_mapping =
        [this, &_population, &_main_tables, &_peripheral_tables](
            const helpers::Column<Float>& _col ) {
            const auto rownum_map = make_rownum_map( _col );
            return make_mapping(
                rownum_map, _population, _main_tables, _peripheral_tables );
        };

    const auto get_colname =
        []( const helpers::Column<Float>& _col ) -> std::string {
        return _col.name_;
    };

    const auto& data_frame =
        _peripheral_tables.size() > 0 ? _peripheral_tables.back() : _population;

    const auto range1 =
        data_frame.discretes_ | std::views::transform( col_to_mapping );

    const auto range2 =
        data_frame.discretes_ | std::views::transform( get_colname );

    const auto mappings =
        stl::collect::vector<MappingForDf::value_type>( range1 );

    const auto colnames = std::make_shared<const std::vector<std::string>>(
        stl::collect::vector<std::string>( range2 ) );

    return std::make_pair( mappings, colnames );
}

// ----------------------------------------------------

Mapping Mapping::fit_on_table_holder(
    const helpers::DataFrame& _population,
    const helpers::TableHolder& _table_holder,
    const std::vector<helpers::DataFrame>& _main_tables,
    const std::vector<helpers::DataFrame>& _peripheral_tables,
    const size_t _ix ) const
{
    // ----------------------------------------------------

    const auto append =
        []( const std::vector<helpers::DataFrame>& _vec,
            const helpers::DataFrame& _df ) -> std::vector<helpers::DataFrame> {
        auto vec = _vec;
        vec.push_back( _df );
        return vec;
    };

    // ----------------------------------------------------

    assert_true(
        _table_holder.main_tables().size() ==
        _table_holder.peripheral_tables().size() );

    assert_true(
        _table_holder.main_tables().size() ==
        _table_holder.subtables().size() );

    assert_true( _ix < _table_holder.main_tables().size() );

    // ----------------------------------------------------

    const auto main_tables =
        append( _main_tables, _table_holder.main_tables().at( _ix ).df() );

    const auto peripheral_tables = append(
        _peripheral_tables, _table_holder.peripheral_tables().at( _ix ) );

    // ----------------------------------------------------

    auto mapping = *this;

    mapping.prefix_ += std::to_string( _ix + 1 ) + "_";

    mapping.table_name_ = _table_holder.peripheral_tables().at( _ix ).name();

    mapping.submappings_ = mapping.fit_submappings(
        _population,
        _table_holder.subtables().at( _ix ),
        main_tables,
        peripheral_tables );

    std::tie( mapping.categorical_, mapping.categorical_names_ ) =
        fit_on_categoricals( _population, main_tables, peripheral_tables );

    std::tie( mapping.discrete_, mapping.discrete_names_ ) =
        fit_on_discretes( _population, main_tables, peripheral_tables );

    std::tie( mapping.text_, mapping.text_names_ ) =
        fit_on_text( _population, main_tables, peripheral_tables );

    return mapping;

    // ----------------------------------------------------
}

// ----------------------------------------------------------------------------

std::pair<typename Mapping::MappingForDf, typename Mapping::Colnames>
Mapping::fit_on_text(
    const helpers::DataFrame& _population,
    const std::vector<helpers::DataFrame>& _main_tables,
    const std::vector<helpers::DataFrame>& _peripheral_tables ) const
{
    const auto col_to_mapping =
        [this, &_population, &_main_tables, &_peripheral_tables](
            const std::shared_ptr<const textmining::WordIndex>& _word_index ) {
            assert_true( _word_index );
            const auto rownum_map =
                helpers::MappingContainerMaker::make_rownum_map_text(
                    *_word_index );
            return make_mapping(
                rownum_map, _population, _main_tables, _peripheral_tables );
        };

    const auto get_colname =
        []( const helpers::Column<strings::String>& _col ) -> std::string {
        return _col.name_;
    };

    const auto& data_frame =
        _peripheral_tables.size() > 0 ? _peripheral_tables.back() : _population;

    assert_true( data_frame.word_indices_.size() == data_frame.text_.size() );

    const auto range1 =
        data_frame.word_indices_ | std::views::transform( col_to_mapping );

    const auto range2 = data_frame.text_ | std::views::transform( get_colname );

    const auto mappings =
        stl::collect::vector<MappingForDf::value_type>( range1 );

    const auto colnames = std::make_shared<const std::vector<std::string>>(
        stl::collect::vector<std::string>( range2 ) );

    return std::make_pair( mappings, colnames );
}

// ----------------------------------------------------

std::vector<Mapping> Mapping::fit_submappings(
    const helpers::DataFrame& _population,
    const std::optional<helpers::TableHolder>& _table_holder,
    const std::vector<helpers::DataFrame>& _main_tables,
    const std::vector<helpers::DataFrame>& _peripheral_tables ) const
{
    // ----------------------------------------------------

    if ( !_table_holder )
        {
            return {};
        }

    // ----------------------------------------------------

    const auto fit = [this,
                      &_population,
                      &_table_holder,
                      &_main_tables,
                      &_peripheral_tables]( const size_t _ix ) -> Mapping {
        return fit_on_table_holder(
            _population,
            _table_holder.value(),
            _main_tables,
            _peripheral_tables,
            _ix );
    };

    // ----------------------------------------------------

    const auto iota = std::views::iota(
        static_cast<size_t>( 0 ), _table_holder->main_tables().size() );

    return stl::collect::vector<Mapping>( iota | std::views::transform( fit ) );

    // ----------------------------------------------------
}

// ----------------------------------------------------

std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
Mapping::fit_transform(
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<containers::Encoding>& _categories,
    const containers::DataFrame& _population_df,
    const std::vector<containers::DataFrame>& _peripheral_dfs,
    const helpers::Placeholder& _placeholder,
    const std::vector<std::string>& _peripheral_names )
{
    assert_true( _categories );

    const auto [population, table_holder, vocabulary] = build_prerequisites(
        _population_df, _peripheral_dfs, _placeholder, _peripheral_names );

    vocabulary_ = vocabulary;

    prefix_ = "_";

    submappings_ = fit_submappings( population, table_holder, {}, {} );

    std::tie( categorical_, categorical_names_ ) =
        fit_on_categoricals( population, {}, {} );

    std::tie( discrete_, discrete_names_ ) =
        fit_on_discretes( population, {}, {} );

    std::tie( text_, text_names_ ) = fit_on_text( population, {}, {} );

    return transform(
        _cmd,
        _categories,
        _population_df,
        _peripheral_dfs,
        _placeholder,
        _peripheral_names );
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
            that.text_ = extract_mapping_vector( _obj, "text_" );

            that.text_names_ = extract_colnames( _obj, "text_names_" );
        }

    return that;
}

// ----------------------------------------------------

std::pair<
    std::shared_ptr<const helpers::VocabularyContainer>,
    helpers::WordIndexContainer>
Mapping::handle_text_fields(
    const helpers::DataFrame& _population,
    const std::vector<helpers::DataFrame>& _peripheral ) const
{
    const auto vocabulary =
        vocabulary_ ? vocabulary_
                    : std::make_shared<const helpers::VocabularyContainer>(
                          1, 0, _population, _peripheral );

    const auto word_indices =
        helpers::WordIndexContainer( _population, _peripheral, *vocabulary );

    return std::make_pair( vocabulary, word_indices );
}

// ----------------------------------------------------

std::vector<containers::Column<Float>> Mapping::make_mapping_columns_int(
    const std::pair<containers::Column<Int>, MappingForDf::value_type>& _p )
    const
{
    // ----------------------------------------------------

    const auto& col = _p.first;
    const auto& mapping = _p.second;

    assert_true( mapping );

    // ----------------------------------------------------

    const auto map_value =
        [&mapping]( const size_t _weight_num, const Int& _val ) -> Float {
        const auto it = mapping->find( _val );

        if ( it == mapping->end() )
            {
                return 0.0;
            }

        assert_true( _weight_num < it->second.size() );

        return it->second.at( _weight_num );
    };

    // ----------------------------------------------------

    const auto make_mapping_column =
        [this, map_value, &col](
            const size_t _weight_num ) -> containers::Column<Float> {
        const auto get_val =
            std::bind( map_value, _weight_num, std::placeholders::_1 );

        const auto range = col | std::views::transform( get_val );

        const auto ptr = std::make_shared<std::vector<Float>>(
            stl::collect::vector<Float>( range ) );

        const auto name = helpers::MappingContainerMaker::make_colname(
            col.name(), prefix_, aggregation_, _weight_num );

        return containers::Column<Float>( ptr, name );
    };

    // ----------------------------------------------------

    if ( mapping->size() <= 1 )
        {
            return std::vector<containers::Column<Float>>();
        }

    const auto num_weights = mapping->begin()->second.size();

    const auto iota = std::views::iota( static_cast<size_t>( 0 ), num_weights );

    return stl::collect::vector<containers::Column<Float>>(
        iota | std::views::transform( make_mapping_column ) );

    // ----------------------------------------------------
}

// ----------------------------------------------------

std::vector<containers::Column<Float>> Mapping::make_mapping_columns_text(
    const std::tuple<
        std::string,
        std::shared_ptr<const textmining::WordIndex>,
        MappingForDf::value_type>& _t ) const
{
    // ----------------------------------------------------

    const auto& colname = std::get<0>( _t );

    const auto& word_index = std::get<1>( _t );

    const auto& mapping = std::get<2>( _t );

    assert_true( word_index );

    assert_true( mapping );

    // ----------------------------------------------------

    const auto map_word =
        [&mapping]( const size_t _weight_num, const Int _word ) -> Float {
        const auto it = mapping->find( _word );

        if ( it == mapping->end() )
            {
                return NAN;
            }

        assert_true( _weight_num < it->second.size() );

        return it->second.at( _weight_num );
    };

    // ----------------------------------------------------

    const auto map_text_field = [map_word, &word_index](
                                    const size_t _weight_num,
                                    const size_t _i ) -> Float {
        const auto map =
            std::bind( map_word, _weight_num, std::placeholders::_1 );

        const auto words = word_index->range( _i );

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
        [this, map_text_field, &colname, &word_index](
            const size_t _weight_num ) -> containers::Column<Float> {
        const auto get_val =
            std::bind( map_text_field, _weight_num, std::placeholders::_1 );

        const auto iota =
            std::views::iota( static_cast<size_t>( 0 ), word_index->nrows() );

        const auto range = iota | std::views::transform( get_val );

        const auto ptr = std::make_shared<std::vector<Float>>(
            stl::collect::vector<Float>( range ) );

        const auto name = helpers::MappingContainerMaker::make_colname(
            colname, "", aggregation_, _weight_num );

        return containers::Column<Float>( ptr, name );
    };

    // ----------------------------------------------------

    if ( mapping->size() <= 1 )
        {
            return std::vector<containers::Column<Float>>();
        }

    const auto num_weights = mapping->begin()->second.size();

    const auto iota = std::views::iota( static_cast<size_t>( 0 ), num_weights );

    const auto vec = stl::collect::vector<containers::Column<Float>>(
        iota | std::views::transform( make_mapping_column ) );

    return vec;

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

    // TODO
    /*
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
    };*/

    // ----------------------------------------------------

    // TODO
    return {};  // columns_to_sql( mapping_to_sql, text_, text_names_ );
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

    obj->set(
        "text_",
        helpers::MappingContainer::transform_mapping_vec( { text_ } ) );

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
    const std::vector<containers::DataFrame>& _peripheral_dfs,
    const helpers::Placeholder& _placeholder,
    const std::vector<std::string>& _peripheral_names ) const
{
    const auto [population, table_holder, _] = build_prerequisites(
        _population_df, _peripheral_dfs, _placeholder, _peripheral_names );

    auto population_df = _population_df;

    auto peripheral_dfs = _peripheral_dfs;

    transform_peripherals( table_holder, &peripheral_dfs );

    transform_data_frame( population, &population_df );

    return std::make_pair( population_df, peripheral_dfs );
}

// ----------------------------------------------------

void Mapping::transform_peripherals(
    const helpers::TableHolder& _table_holder,
    std::vector<containers::DataFrame>* _peripheral_dfs ) const
{
    // ----------------------------------------------------

    const auto find_peripheral =
        [_peripheral_dfs]( const Mapping& _mapping ) -> containers::DataFrame* {
        for ( auto& df : *_peripheral_dfs )
            {
                if ( df.name() == _mapping.table_name_ )
                    {
                        return &df;
                    }
            }

        throw std::runtime_error(
            "Mapping: Data frame '" + _mapping.table_name_ + "' not found!" );

        return nullptr;
    };

    // ----------------------------------------------------

    assert_true(
        submappings_.size() == _table_holder.peripheral_tables().size() );

    assert_true( submappings_.size() == _table_holder.subtables().size() );

    for ( size_t i = 0; i < submappings_.size(); ++i )
        {
            const auto& mapping = submappings_.at( i );

            const auto subtable = _table_holder.subtables().at( i );

            if ( mapping.submappings_.size() > 0 )
                {
                    assert_true( subtable );
                    mapping.transform_peripherals(
                        subtable.value(), _peripheral_dfs );
                }

            const auto& immutable = _table_holder.peripheral_tables().at( i );

            const auto df = find_peripheral( mapping );

            mapping.transform_data_frame( immutable, df );
        }

    // ----------------------------------------------------
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

void Mapping::transform_data_frame(
    const helpers::DataFrame& _immutable,
    containers::DataFrame* _data_frame ) const
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

    const auto categorical_mappings = transform_categorical( *_data_frame );

    const auto discrete_mappings = transform_discrete( *_data_frame );

    const auto text_mappings = transform_text( _immutable, *_data_frame );

    add_columns( categorical_mappings, _data_frame );

    add_columns( discrete_mappings, _data_frame );

    add_columns( text_mappings, _data_frame );
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
    const helpers::DataFrame& _immutable,
    const containers::DataFrame& _df ) const
{
    // --------------------------------------------------------------

    using Pair =
        std::pair<std::string, std::shared_ptr<const textmining::WordIndex>>;

    using Tuple = std::tuple<
        std::string,
        std::shared_ptr<const textmining::WordIndex>,
        MappingForDf::value_type>;

    assert_true( _immutable.word_indices_.size() == _immutable.text_.size() );

    assert_true( text_names_ );

    assert_true( text_.size() == text_names_->size() );

    // --------------------------------------------------------------

    const auto find_word_index = [&_immutable]( const std::string& _name )
        -> std::shared_ptr<const textmining::WordIndex> {
        for ( size_t i = 0; i < _immutable.text_.size(); ++i )
            {
                if ( _immutable.text_.at( i ).name_ == _name )
                    {
                        return _immutable.word_indices_.at( i );
                    }
            }

        throw std::invalid_argument(
            "Mapping: Column '" + _name + "' not found!" );

        return _immutable.word_indices_.at( 0 );
    };

    // --------------------------------------------------------------

    const auto get_word_index = [this,
                                 find_word_index]( const size_t _i ) -> Pair {
        assert_true( text_names_ );
        assert_true( _i < text_names_->size() );
        const auto& colname = text_names_->at( _i );
        return std::make_pair( colname, find_word_index( colname ) );
    };

    // --------------------------------------------------------------

    const auto get_mapping =
        [this]( const size_t _i ) -> MappingForDf::value_type {
        assert_true( _i < text_.size() );
        return text_.at( _i );
    };

    // --------------------------------------------------------------

    const auto make_tuple = [get_word_index,
                             get_mapping]( const size_t _i ) -> Tuple {
        const auto [colname, word_index] = get_word_index( _i );
        return std::make_tuple( colname, word_index, get_mapping( _i ) );
    };

    // --------------------------------------------------------------

    const auto make_cols = [this]( const Tuple& _t ) {
        return make_mapping_columns_text( _t );
    };

    // --------------------------------------------------------------

    const auto iota =
        std::views::iota( static_cast<size_t>( 0 ), text_.size() );

    const auto range = iota | std::views::transform( make_tuple ) |
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
