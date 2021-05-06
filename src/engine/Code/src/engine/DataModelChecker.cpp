#include "engine/preprocessors/preprocessors.hpp"

namespace engine
{
namespace preprocessors
{
// ----------------------------------------------------------------------------

void DataModelChecker::check(
    const std::shared_ptr<const helpers::Placeholder> _placeholder,
    const std::shared_ptr<const std::vector<std::string>> _peripheral_names,
    const containers::DataFrame& _population,
    const std::vector<containers::DataFrame>& _peripheral,
    const std::vector<std::shared_ptr<featurelearners::AbstractFeatureLearner>>
        _feature_learners,
    const std::shared_ptr<const communication::Logger>& _logger,
    Poco::Net::StreamSocket* _socket )
{
    // --------------------------------------------------------------------------

    const auto is_not_text_field =
        []( const containers::DataFrame& _df ) -> bool {
        return _df.name().find( helpers::Macros::text_field() ) ==
               std::string::npos;
    };

    const auto peripheral = stl::collect::vector<containers::DataFrame>(
        _peripheral | std::views::filter( is_not_text_field ) );

    // --------------------------------------------------------------------------

    assert_true( _peripheral_names );

    if ( _peripheral_names->size() != peripheral.size() )
        {
            throw std::invalid_argument(
                "The number of peripheral tables in the placeholder must "
                "be "
                "equal to the number of peripheral tables passed (" +
                std::to_string( _peripheral_names->size() ) + " vs. " +
                std::to_string( peripheral.size() ) +
                "). This is the point of having placeholders." );
        }

    // --------------------------------------------------------------------------

    communication::Warner warner;

    // --------------------------------------------------------------------------

    check_data_frames( _population, peripheral, _feature_learners, &warner );

    // --------------------------------------------------------------------------

    assert_true( _placeholder );

    if ( _feature_learners.size() > 0 )
        {
            if ( _population.nrows() == 0 )
                {
                    throw std::invalid_argument(
                        "There are no rows in the population table." );
                }

            // The probability of being picked is equal for all rows in the
            // population table.
            const auto prob_pick = std::vector<Float>(
                _population.nrows(),
                1.0 / static_cast<Float>( _population.nrows() ) );

            check_join(
                *_placeholder,
                _peripheral_names,
                _population,
                peripheral,
                prob_pick,
                _feature_learners,
                &warner );

            check_self_joins(
                *_placeholder,
                _population,
                peripheral,
                _feature_learners,
                &warner );
        }

    // --------------------------------------------------------------------------

    if ( _logger )
        {
            for ( const auto& warning : warner.warnings() )
                {
                    _logger->log( warning );
                }
        }

    // --------------------------------------------------------------------------

    warner.send( _socket );

    // --------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DataModelChecker::check_categorical_column(
    const containers::Column<Int>& _col,
    const std::string& _df_name,
    communication::Warner* _warner )
{
    // --------------------------------------------------------------------------

    const auto length = static_cast<Float>( _col.size() );

    // --------------------------------------------------------------------------

    assert_true( _col.size() > 0 );

    const Float num_non_null =
        utils::Aggregations::count_categorical( *_col.data_ptr() );

    const auto share_null = 1.0 - num_non_null / length;

    if ( share_null > 0.9 )
        {
            warn_too_many_nulls(
                false, share_null, _col.name(), _df_name, _warner );
        }

    if ( num_non_null < 0.5 )
        {
            return;
        }

    // --------------------------------------------------------------------------

    const bool is_comparison_only =
        ( _col.unit().find( "comparison only" ) != std::string::npos );

    const Float num_distinct =
        utils::Aggregations::count_distinct( *_col.data_ptr() );

    // --------------------------------------------------------------------------

    if ( num_distinct == 1.0 && !is_comparison_only )
        {
            warn_all_equal( false, _col.name(), _df_name, _warner );
        }

    // --------------------------------------------------------------------------

    if ( num_distinct > 1000.0 && !is_comparison_only )
        {
            warn_too_many_unique(
                num_distinct, _col.name(), _df_name, _warner );
        }

    // --------------------------------------------------------------------------

    const auto unique_share = num_distinct / num_non_null;

    if ( !is_comparison_only && unique_share > 0.25 )
        {
            warn_unique_share_too_high(
                unique_share, _col.name(), _df_name, _warner );
        }

    // --------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DataModelChecker::check_data_frames(
    const containers::DataFrame& _population,
    const std::vector<containers::DataFrame>& _peripheral,
    const std::vector<std::shared_ptr<featurelearners::AbstractFeatureLearner>>
        _feature_learners,
    communication::Warner* _warner )
{
    // --------------------------------------------------------------------------

    const auto [has_multirel, has_multirel_ts] = find_feature_learner(
        _feature_learners,
        featurelearners::AbstractFeatureLearner::MULTIREL_MODEL,
        featurelearners::AbstractFeatureLearner::MULTIREL_TIME_SERIES );

    const auto [has_relmt, has_relmt_ts] = find_feature_learner(
        _feature_learners,
        featurelearners::AbstractFeatureLearner::RELMT_MODEL,
        featurelearners::AbstractFeatureLearner::RELMT_TIME_SERIES );

    // --------------------------------------------------------------------------

    // Too many columns in the population table are only a problem if there is a
    // multirel time series, as the population table is usually not aggregated.
    check_df( _population, has_multirel_ts, _warner );

    for ( const auto& df : _peripheral )
        {
            check_df( df, has_multirel, _warner );
        }

    // --------------------------------------------------------------------------

    if ( has_relmt )
        {
            for ( const auto& df : _peripheral )
                {
                    check_num_columns_relmt( _population, df, _warner );
                }
        }

    // --------------------------------------------------------------------------

    if ( has_relmt_ts )
        {
            check_num_columns_relmt( _population, _population, _warner );
        }

    // --------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DataModelChecker::check_df(
    const containers::DataFrame& _df,
    const bool _check_num_columns,
    communication::Warner* _warner )
{
    // --------------------------------------------------------------------------

    if ( _df.nrows() == 0 )
        {
            warn_is_empty( _df.name(), _warner );
            return;
        }

    // --------------------------------------------------------------------------

    if ( _check_num_columns )
        {
            const auto num_columns =
                _df.num_numericals() + _df.num_categoricals();

            if ( num_columns > 20 )
                {
                    warn_too_many_columns_multirel(
                        num_columns, _df.name(), _warner );
                }
        }

    // --------------------------------------------------------------------------

    for ( size_t i = 0; i < _df.num_categoricals(); ++i )
        {
            check_categorical_column(
                _df.categorical( i ), _df.name(), _warner );
        }

    for ( size_t i = 0; i < _df.num_numericals(); ++i )
        {
            check_float_column( _df.numerical( i ), _df.name(), _warner );
        }

    for ( size_t i = 0; i < _df.num_time_stamps(); ++i )
        {
            check_float_column( _df.time_stamp( i ), _df.name(), _warner );
        }

    // --------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DataModelChecker::check_float_column(
    const containers::Column<Float>& _col,
    const std::string& _df_name,
    communication::Warner* _warner )
{
    // --------------------------------------------------------------------------

    const auto length = static_cast<Float>( _col.size() );

    // --------------------------------------------------------------------------

    assert_true( _col.size() > 0 );

    const Float num_non_null =
        utils::Aggregations::count( _col.begin(), _col.end() );

    const auto share_null = 1.0 - num_non_null / length;

    if ( share_null > 0.9 )
        {
            warn_too_many_nulls(
                true, share_null, _col.name(), _df_name, _warner );
        }

    // --------------------------------------------------------------------------

    const bool is_comparison_only =
        ( _col.unit().find( "comparison only" ) != std::string::npos );

    const auto all_equal = !is_comparison_only && is_all_equal( _col );

    if ( all_equal )
        {
            warn_all_equal( true, _col.name(), _df_name, _warner );
        }

    // --------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DataModelChecker::check_join(
    const helpers::Placeholder& _placeholder,
    const std::shared_ptr<const std::vector<std::string>> _peripheral_names,
    const containers::DataFrame& _population,
    const std::vector<containers::DataFrame>& _peripheral,
    const std::vector<Float>& _prob_pick,
    const std::vector<std::shared_ptr<featurelearners::AbstractFeatureLearner>>&
        _feature_learners,
    communication::Warner* _warner )
{
    // ------------------------------------------------------------------------

    const auto is_prop =
        []( const std::shared_ptr<featurelearners::AbstractFeatureLearner>&
                _fl ) -> bool {
        assert_true( _fl );
        return (
            _fl->type() ==
                featurelearners::AbstractFeatureLearner::FASTPROP_MODEL ||
            _fl->type() ==
                featurelearners::AbstractFeatureLearner::FASTPROP_TIME_SERIES );
    };

    const bool all_propositionalization = std::all_of(
        _feature_learners.begin(), _feature_learners.end(), is_prop );

    // ------------------------------------------------------------------------

    assert_true( _peripheral_names );

    assert_true( _peripheral_names->size() == _peripheral.size() );

    // ------------------------------------------------------------------------

    const auto& joined_tables = _placeholder.joined_tables_;

    const auto& join_keys_used = _placeholder.join_keys_used_;

    const auto& other_join_keys_used = _placeholder.other_join_keys_used_;

    const auto& propositionalization = _placeholder.propositionalization_;

    const auto& time_stamps_used = _placeholder.time_stamps_used_;

    const auto& other_time_stamps_used = _placeholder.other_time_stamps_used_;

    const auto& upper_time_stamps_used = _placeholder.upper_time_stamps_used_;

    // ------------------------------------------------------------------------

    const auto size = joined_tables.size();

    assert_true( join_keys_used.size() == size );

    assert_true( other_join_keys_used.size() == size );

    assert_true( propositionalization.size() == size );

    assert_true( time_stamps_used.size() == size );

    assert_true( other_time_stamps_used.size() == size );

    assert_true( upper_time_stamps_used.size() == size );

    // ------------------------------------------------------------------------

    for ( size_t i = 0; i < size; ++i )
        {
            const auto& name = joined_tables.at( i ).name_;

            const auto it = std::find(
                _peripheral_names->begin(), _peripheral_names->end(), name );

            if ( it == _peripheral_names->end() )
                {
                    throw std::invalid_argument(
                        "No placeholder called '" + name +
                        "' among the peripheral placeholders." );
                }

            const auto dist = std::distance( _peripheral_names->begin(), it );

            const auto
                [is_many_to_one,
                 num_matches,
                 num_expected,
                 num_jk_not_found,
                 prob_pick] =
                    check_matches(
                        join_keys_used.at( i ),
                        other_join_keys_used.at( i ),
                        time_stamps_used.at( i ),
                        other_time_stamps_used.at( i ),
                        upper_time_stamps_used.at( i ),
                        _population,
                        _peripheral.at( dist ),
                        _prob_pick );

            raise_join_warnings(
                all_propositionalization || propositionalization.at( i ),
                is_many_to_one,
                num_matches,
                num_expected,
                num_jk_not_found,
                join_keys_used.at( i ),
                other_join_keys_used.at( i ),
                _population,
                _peripheral.at( dist ),
                _warner );

            check_join(
                joined_tables.at( i ),
                _peripheral_names,
                _peripheral.at( dist ),
                _peripheral,
                prob_pick,
                _feature_learners,
                _warner );
        }

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::tuple<bool, size_t, Float, size_t, std::vector<Float>>
DataModelChecker::check_matches(
    const std::string& _join_key_used,
    const std::string& _other_join_key_used,
    const std::string& _time_stamp_used,
    const std::string& _other_time_stamp_used,
    const std::string& _upper_time_stamp_used,
    const containers::DataFrame& _population_df,
    const containers::DataFrame& _peripheral_df,
    const std::vector<Float>& _prob_pick )
{
    // ------------------------------------------------------------------------

    assert_true( _population_df.nrows() == _prob_pick.size() );

    // ------------------------------------------------------------------------

    const auto jk1 = _population_df.join_key( _join_key_used );

    const auto ptr2 = _peripheral_df.index( _other_join_key_used ).map();

    assert_true( ptr2 );

    const auto& map2 = *ptr2;

    // ------------------------------------------------------------------------

    const auto [ts1, ts2, upper] = find_time_stamps(
        _time_stamp_used,
        _other_time_stamp_used,
        _upper_time_stamp_used,
        _population_df,
        _peripheral_df );

    // ------------------------------------------------------------------------

    bool is_many_to_one = true;

    size_t num_matches = 0;

    size_t num_jk_not_found = 0;

    auto prob_pick = std::vector<Float>( _peripheral_df.nrows() );

    auto local_num_matches = std::vector<Float>( _population_df.nrows() );

    // ------------------------------------------------------------------------

    for ( size_t ix1 = 0; ix1 < jk1.size(); ++ix1 )
        {
            const auto it2 = map2.find( jk1[ix1] );

            if ( it2 == map2.end() )
                {
                    num_jk_not_found++;
                    continue;
                }

            for ( const auto ix2 : it2->second )
                {
                    const bool in_range = is_in_range(
                        ts1 ? ts1->at( ix1 ) : 0.0,
                        ts1 ? ts2->at( ix2 ) : 0.0,
                        upper ? upper->at( ix2 ) : NAN );

                    if ( !in_range )
                        {
                            continue;
                        }

                    ++num_matches;

                    local_num_matches.at( ix1 ) += 1.0;

                    prob_pick.at( ix2 ) += _prob_pick.at( ix1 );

                    if ( local_num_matches.at( ix1 ) > 1.0 )
                        {
                            is_many_to_one = false;
                        }
                }
        }

    // ------------------------------------------------------------------------

    const auto num_expected = std::inner_product(
        local_num_matches.begin(),
        local_num_matches.end(),
        _prob_pick.begin(),
        0.0 );

    // ------------------------------------------------------------------------

    return std::make_tuple(
        is_many_to_one,
        num_matches,
        num_expected,
        num_jk_not_found,
        prob_pick );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DataModelChecker::check_num_columns_relmt(
    const containers::DataFrame& _population,
    const containers::DataFrame& _peripheral,
    communication::Warner* _warner )
{
    const auto num_columns =
        _population.num_numericals() + _population.num_time_stamps() +
        _peripheral.num_numericals() + _peripheral.num_time_stamps();

    if ( num_columns > 40 )
        {
            warn_too_many_columns_relmt(
                num_columns, _population.name(), _peripheral.name(), _warner );
        }
}

// ----------------------------------------------------------------------------

void DataModelChecker::check_self_joins(
    const helpers::Placeholder& _placeholder,
    const containers::DataFrame& _population,
    const std::vector<containers::DataFrame>& _peripheral,
    const std::vector<std::shared_ptr<featurelearners::AbstractFeatureLearner>>
        _feature_learners,
    communication::Warner* _warner )
{
    // ------------------------------------------------------------------------

    const auto old_size = _placeholder.joined_tables_.size();

    // ------------------------------------------------------------------------

    for ( const auto& fl : _feature_learners )
        {
            // ----------------------------------------------------------------

            assert_true( fl );

            if ( !fl->is_time_series() )
                {
                    continue;
                }

            // ----------------------------------------------------------------

            const bool is_propositionalization =
                ( fl->type() == featurelearners::AbstractFeatureLearner::
                                    FASTPROP_TIME_SERIES );

            // ----------------------------------------------------------------

            const auto [new_population, new_peripheral] =
                fl->modify_data_frames( _population, _peripheral );

            // ----------------------------------------------------------------

            const auto new_placeholder = fl->make_placeholder();

            // ----------------------------------------------------------------

            const auto& joined_tables = new_placeholder.joined_tables_;

            const auto& join_keys_used = new_placeholder.join_keys_used_;

            const auto& other_join_keys_used =
                new_placeholder.other_join_keys_used_;

            const auto& time_stamps_used = new_placeholder.time_stamps_used_;

            const auto& other_time_stamps_used =
                new_placeholder.other_time_stamps_used_;

            const auto& upper_time_stamps_used =
                new_placeholder.upper_time_stamps_used_;

            // ----------------------------------------------------------------

            const auto new_size = joined_tables.size();

            assert_true( join_keys_used.size() == new_size );

            assert_true( other_join_keys_used.size() == new_size );

            assert_true( time_stamps_used.size() == new_size );

            assert_true( other_time_stamps_used.size() == new_size );

            assert_true( upper_time_stamps_used.size() == new_size );

            // ----------------------------------------------------------------

            assert_true( new_peripheral.size() == _peripheral.size() + 1 );

            // The probability of being picked is equal for all rows in the
            // population table.
            const auto prob_pick = std::vector<Float>(
                _population.nrows(),
                1.0 / static_cast<Float>( _population.nrows() ) );

            for ( size_t i = old_size; i < new_size; ++i )
                {
                    const auto
                        [is_many_to_one,
                         num_matches,
                         num_expected,
                         num_jk_not_found,
                         _] =
                            check_matches(
                                join_keys_used.at( i ),
                                other_join_keys_used.at( i ),
                                time_stamps_used.at( i ),
                                other_time_stamps_used.at( i ),
                                upper_time_stamps_used.at( i ),
                                new_population,
                                new_peripheral.back(),
                                prob_pick );

                    raise_self_join_warnings(
                        is_propositionalization,
                        is_many_to_one,
                        num_matches,
                        new_population,
                        _warner );
                }

            // ----------------------------------------------------------------
        }

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::pair<bool, bool> DataModelChecker::find_feature_learner(
    const std::vector<std::shared_ptr<featurelearners::AbstractFeatureLearner>>&
        _feature_learners,
    const std::string& _model,
    const std::string& _ts )
{
    const auto is_model_or_ts =
        [_model, _ts]( const std::shared_ptr<
                       const featurelearners::AbstractFeatureLearner>& fl ) {
            assert_true( fl );
            return fl->type() == _model || fl->type() == _ts;
        };

    const auto is_ts =
        [_ts]( const std::shared_ptr<
               const featurelearners::AbstractFeatureLearner>& fl ) {
            assert_true( fl );
            return fl->type() == _ts;
        };

    const bool has_model_or_ts = std::any_of(
        _feature_learners.begin(), _feature_learners.end(), is_model_or_ts );

    const bool has_ts = std::any_of(
        _feature_learners.begin(), _feature_learners.end(), is_ts );

    return std::make_pair( has_model_or_ts, has_ts );
}

// ----------------------------------------------------------------------------

std::tuple<
    std::optional<containers::Column<Float>>,
    std::optional<containers::Column<Float>>,
    std::optional<containers::Column<Float>>>
DataModelChecker::find_time_stamps(
    const std::string& _time_stamp_used,
    const std::string& _other_time_stamp_used,
    const std::string& _upper_time_stamp_used,
    const containers::DataFrame& _population_df,
    const containers::DataFrame& _peripheral_df )
{
    // ------------------------------------------------------------------------

    if ( ( _time_stamp_used == "" ) != ( _other_time_stamp_used == "" ) )
        {
            throw std::invalid_argument(
                "You have to pass both time_stamp_used and "
                "other_time_stamps_used or neither of them." );
        }

    if ( _time_stamp_used == "" && _upper_time_stamp_used != "" )
        {
            throw std::invalid_argument(
                "If you pass no time_stamp_used, then passing an "
                "upper_time_stamp_used makes no sense." );
        }

    // ------------------------------------------------------------------------

    auto ts1 = std::optional<containers::Column<Float>>();

    if ( _time_stamp_used != "" )
        {
            ts1 = _population_df.time_stamp( _time_stamp_used );
        }

    auto ts2 = std::optional<containers::Column<Float>>();

    if ( _other_time_stamp_used != "" )
        {
            ts2 = _peripheral_df.time_stamp( _other_time_stamp_used );
        }

    auto upper = std::optional<containers::Column<Float>>();

    if ( _upper_time_stamp_used != "" )
        {
            upper = _peripheral_df.time_stamp( _upper_time_stamp_used );
        }

    // ------------------------------------------------------------------------

    return std::make_tuple( ts1, ts2, upper );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::tuple<
    std::vector<std::string>,
    std::vector<std::string>,
    std::vector<std::string>>
DataModelChecker::get_time_stamps_used(
    const Poco::JSON::Object& _population_placeholder,
    const size_t _expected_size )
{
    // ------------------------------------------------------------------------

    const auto time_stamps_used_arr =
        _population_placeholder.getArray( "time_stamps_used_" );

    if ( !time_stamps_used_arr )
        {
            throw std::invalid_argument(
                "The placeholder has no array named 'time_stamps_used_'!" );
        }

    const auto other_time_stamps_used_arr =
        _population_placeholder.getArray( "other_time_stamps_used_" );

    if ( !other_time_stamps_used_arr )
        {
            throw std::invalid_argument(
                "The placeholder has no array named "
                "'other_time_stamps_used_'!" );
        }

    const auto upper_time_stamps_used_arr =
        _population_placeholder.getArray( "upper_time_stamps_used_" );

    if ( !upper_time_stamps_used_arr )
        {
            throw std::invalid_argument(
                "The placeholder has no array named "
                "'upper_time_stamps_used_'!" );
        }

    // ------------------------------------------------------------------------

    const auto time_stamps_used =
        JSON::array_to_vector<std::string>( time_stamps_used_arr );

    const auto other_time_stamps_used =
        JSON::array_to_vector<std::string>( other_time_stamps_used_arr );

    const auto upper_time_stamps_used =
        JSON::array_to_vector<std::string>( upper_time_stamps_used_arr );

    // ------------------------------------------------------------------------

    if ( _expected_size != time_stamps_used.size() )
        {
            throw std::invalid_argument(
                "Length of 'joined_tables_' must match length of "
                "'time_stamps_used_'." );
        }

    if ( _expected_size != other_time_stamps_used.size() )
        {
            throw std::invalid_argument(
                "Length of 'joined_tables_' must match length of "
                "'other_time_stamps_used_'." );
        }

    if ( _expected_size != upper_time_stamps_used.size() )
        {
            throw std::invalid_argument(
                "Length of 'joined_tables_' must match length of "
                "'upper_time_stamps_used_'." );
        }

    // ------------------------------------------------------------------------

    return std::make_tuple(
        time_stamps_used, other_time_stamps_used, upper_time_stamps_used );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

bool DataModelChecker::is_all_equal( const containers::Column<Float>& _col )
{
    auto it = std::find_if( _col.begin(), _col.end(), []( Float val ) {
        return !std::isnan( val );
    } );

    if ( it == _col.end() )
        {
            return true;
        }

    Float val = *it;

    for ( ; it != _col.end(); ++it )
        {
            if ( std::isnan( *it ) )
                {
                    continue;
                }

            if ( val != *it )
                {
                    return false;
                }
        }

    return true;
}

// ------------------------------------------------------------------------

std::string DataModelChecker::modify_df_name( const std::string& _df_name )
{
    return helpers::SQLGenerator::make_staging_table_name( _df_name );
}

// ------------------------------------------------------------------------

std::string DataModelChecker::modify_join_key_name(
    const std::string& _jk_name )
{
    const auto names = helpers::Macros::parse_join_key_name( _jk_name );

    std::string modified;

    for ( size_t i = 0; i < names.size(); ++i )
        {
            const bool has_column =
                ( names.at( i ).find( helpers::Macros::column() ) !=
                  std::string::npos );

            const auto name =
                has_column ? helpers::Macros::get_param(
                                 names.at( i ), helpers::Macros::column() )
                           : names.at( i );

            modified += "'" + name + "'";

            if ( i != names.size() - 1 )
                {
                    modified += ", ";
                }
        }

    return modified;
}

// ------------------------------------------------------------------------

void DataModelChecker::raise_join_warnings(
    const bool _is_propositionalization,
    const bool _is_many_to_one,
    const size_t _num_matches,
    const Float _num_expected,
    const size_t _num_jk_not_found,
    const std::string& _join_key_used,
    const std::string& _other_join_key_used,
    const containers::DataFrame& _population_df,
    const containers::DataFrame& _peripheral_df,
    communication::Warner* _warner )
{
    // ------------------------------------------------------------------------

    if ( _num_matches == 0 )
        {
            warn_no_matches(
                _join_key_used,
                _other_join_key_used,
                _population_df,
                _peripheral_df,
                _warner );

            return;
        }

    // ------------------------------------------------------------------------

    if ( _is_many_to_one )
        {
            warn_many_to_one(
                _join_key_used,
                _other_join_key_used,
                _population_df,
                _peripheral_df,
                _warner );
        }

    // ------------------------------------------------------------------------

    if ( !_is_propositionalization && _num_expected > 300.0 )
        {
            warn_too_many_matches(
                _num_matches,
                _join_key_used,
                _other_join_key_used,
                _population_df,
                _peripheral_df,
                _warner );
        }

    // ------------------------------------------------------------------------

    if ( _num_expected > 3000.0 )
        {
            warn_memory(
                _num_matches,
                _join_key_used,
                _other_join_key_used,
                _population_df,
                _peripheral_df,
                _warner );
        }

    // ------------------------------------------------------------------------

    const auto not_found_ratio = static_cast<Float>( _num_jk_not_found ) /
                                 static_cast<Float>( _population_df.nrows() );

    if ( not_found_ratio > 0.0 )
        {
            warn_not_found(
                not_found_ratio,
                _join_key_used,
                _other_join_key_used,
                _population_df,
                _peripheral_df,
                _warner );
        }

    // ------------------------------------------------------------------------
}

// ------------------------------------------------------------------------

void DataModelChecker::raise_self_join_warnings(
    const bool _is_propositionalization,
    const bool _is_many_to_one,
    const size_t _num_matches,
    const containers::DataFrame& _population_df,
    communication::Warner* _warner )
{
    // ------------------------------------------------------------------------

    if ( _num_matches == 0 )
        {
            return;
        }

    // ------------------------------------------------------------------------

    if ( _is_many_to_one )
        {
            warn_self_join_no_matches( _population_df, _warner );
        }

    // ------------------------------------------------------------------------

    const auto avg_num_matches = static_cast<Float>( _num_matches ) /
                                 static_cast<Float>( _population_df.nrows() );

    if ( !_is_propositionalization && avg_num_matches > 300.0 )
        {
            warn_self_join_too_many_matches(
                _num_matches, _population_df, _warner );
        }

    // ------------------------------------------------------------------------

    if ( avg_num_matches > 3000.0 )
        {
            warn_self_join_memory( _num_matches, _population_df, _warner );
        }

    // ------------------------------------------------------------------------
}

// ------------------------------------------------------------------------

void DataModelChecker::warn_all_equal(
    const bool _is_float,
    const std::string& _colname,
    const std::string& _df_name,
    communication::Warner* _warner )
{
    const std::string role = _is_float
                                 ? containers::DataFrame::ROLE_UNUSED_FLOAT
                                 : containers::DataFrame::ROLE_UNUSED_STRING;

    const auto colname = modify_colname( _colname );

    const auto df_name = modify_df_name( _df_name );

    _warner->add(
        column_should_be_unused() + "All non-NULL entries in column '" +
        colname + "' in " + df_name +
        " are equal to each other. You should "
        "consider setting its role to " +
        role +
        " or using it for "
        "comparison only (you can do the latter by setting a unit "
        "that contains 'comparison only')." );
}

// ------------------------------------------------------------------------

void DataModelChecker::warn_is_empty(
    const std::string& _df_name, communication::Warner* _warner )
{
    const auto df_name = modify_df_name( _df_name );
    _warner->add( "It appears that " + df_name + " is empty." );
}

// ------------------------------------------------------------------------

void DataModelChecker::warn_many_to_one(
    const std::string& _join_key_used,
    const std::string& _other_join_key_used,
    const containers::DataFrame& _population_df,
    const containers::DataFrame& _peripheral_df,
    communication::Warner* _warner )
{
    const auto join_key_used = modify_join_key_name( _join_key_used );

    const auto other_join_key_used =
        modify_join_key_name( _other_join_key_used );

    const auto population_name = modify_df_name( _population_df.name() );

    const auto peripheral_name = modify_df_name( _peripheral_df.name() );

    _warner->add(
        data_model_can_be_improved() + "It appears that " + population_name +
        " and " + peripheral_name +
        " are in a many-to-one or one-to-one relationship when "
        "joined over " +
        join_key_used + " and " + other_join_key_used +
        ". If the relationship must always be many-to-one or one-to-one, you "
        "should "
        "mark the relationship as such in the .join(...) method of the "
        "placeholder. If this is just an idiosyncracy of the training set, you "
        "should consider using a different training set." );
}

// ------------------------------------------------------------------------

void DataModelChecker::warn_memory(
    const size_t _num_matches,
    const std::string& _join_key_used,
    const std::string& _other_join_key_used,
    const containers::DataFrame& _population_df,
    const containers::DataFrame& _peripheral_df,
    communication::Warner* _warner )
{
    const auto join_key_used = modify_join_key_name( _join_key_used );

    const auto other_join_key_used =
        modify_join_key_name( _other_join_key_used );

    const auto population_name = modify_df_name( _population_df.name() );

    const auto peripheral_name = modify_df_name( _peripheral_df.name() );

    _warner->add(
        uses_memory() + "There are " + std::to_string( _num_matches ) +
        " matches between " + population_name + " and " + peripheral_name +
        " when joined over " + join_key_used + " and " + other_join_key_used +
        ". This pipeline might use a lot of memory. "
        "You could impose a narrower limit on the scope of "
        "this join by reducing the memory (the period of time until "
        "the feature learner 'forgets' historical data). "
        "You can reduce the memory "
        "by setting the appropriate parameter in the .join(...)-method "
        "of the Placeholder. "
        "Please note that a memory of 0.0 means that "
        "the time series will not forget any past "
        "data." );
}
// ------------------------------------------------------------------------

void DataModelChecker::warn_no_matches(
    const std::string& _join_key_used,
    const std::string& _other_join_key_used,
    const containers::DataFrame& _population_df,
    const containers::DataFrame& _peripheral_df,
    communication::Warner* _warner )
{
    const auto join_key_used = modify_join_key_name( _join_key_used );

    const auto other_join_key_used =
        modify_join_key_name( _other_join_key_used );

    const auto population_name = modify_df_name( _population_df.name() );

    const auto peripheral_name = modify_df_name( _peripheral_df.name() );

    _warner->add(
        data_model_can_be_improved() + "There are no matches between " +
        join_key_used + " in " + population_name + " and " +
        other_join_key_used + " in " + peripheral_name +
        ". You should consider removing this join from your data "
        "model or re-examine your join keys." );
}

// ------------------------------------------------------------------------

void DataModelChecker::warn_not_found(
    const Float _not_found_ratio,
    const std::string& _join_key_used,
    const std::string& _other_join_key_used,
    const containers::DataFrame& _population_df,
    const containers::DataFrame& _peripheral_df,
    communication::Warner* _warner )
{
    const auto join_key_used = modify_join_key_name( _join_key_used );

    const auto other_join_key_used =
        modify_join_key_name( _other_join_key_used );

    const auto population_name = modify_df_name( _population_df.name() );

    const auto peripheral_name = modify_df_name( _peripheral_df.name() );

    _warner->add(
        join_keys_not_found() + "When joining " + population_name + " and " +
        peripheral_name + " over " + join_key_used + " and " +
        other_join_key_used + ", there are no corresponding entries for " +
        std::to_string( _not_found_ratio * 100.0 ) + "% of entries in " +
        join_key_used + " in '" + population_name +
        "'. You might want to double-check your join keys." );
}

// ------------------------------------------------------------------------

void DataModelChecker::warn_self_join_memory(
    const size_t _num_matches,
    const containers::DataFrame& _population_df,
    communication::Warner* _warner )
{
    const auto population_name = modify_df_name( _population_df.name() );

    _warner->add(
        uses_memory() + "The self-join on " + population_name +
        " created by the time series feature learner "
        "has a total of " +
        std::to_string( _num_matches ) +
        " matches.  This might use a lot of memory. "
        "You could impose a narrower limit on the scope of "
        "this join by reducing the memory (the period of time until "
        "the feature learner 'forgets' historical data). "
        "Please note that a memory of 0.0 means that "
        "the time series will not forget any past "
        "data." );
}

// ------------------------------------------------------------------------

void DataModelChecker::warn_self_join_no_matches(
    const containers::DataFrame& _population_df,
    communication::Warner* _warner )
{
    const auto population_name = modify_df_name( _population_df.name() );

    _warner->add(
        data_model_can_be_improved() + "The self-join on " + population_name +
        " created by the time series feature learner has no matches. "
        "You should examine your join keys." );
}

// ------------------------------------------------------------------------

void DataModelChecker::warn_self_join_too_many_matches(
    const size_t _num_matches,
    const containers::DataFrame& _population_df,
    communication::Warner* _warner )
{
    const auto population_name = modify_df_name( _population_df.name() );

    _warner->add(
        might_take_long() + "The self-join on " + population_name +
        " created by the time series feature learner "
        "has a total of " +
        std::to_string( _num_matches ) +
        " matches.  This can take a long time to fit. "
        "There are two possible ways to fix this: \n"
        "1) You could impose a narrower limit on the scope of "
        "this join by reducing the memory (the period of time until "
        "the feature learner 'forgets' historical data). "
        "Please note that a memory of 0.0 means that "
        "the time series will not forget any past "
        "data.\n"
        "2) You could also use FastPropTimeSeries instead." );
}

// ------------------------------------------------------------------------

void DataModelChecker::warn_too_many_columns_multirel(
    const size_t _num_columns,
    const std::string& _df_name,
    communication::Warner* _warner )
{
    const auto df_name = modify_df_name( _df_name );

    _warner->add(
        might_take_long() + df_name + " contains " +
        std::to_string( _num_columns ) +
        " categorical and numerical columns. "
        "Please note that columns created by the preprocessors "
        "are also part of this count. The multirel "
        "algorithm does not scale very well to data frames "
        "with many columns. This pipeline might take a very "
        "long time to fit. You should consider removing some "
        "columns or preprocessors. You could use a column selection "
        "to pick the right columns. "
        "You could also replace "
        "MultirelModel or "
        "MultirelTimeSeries with RelboostModel or "
        "RelboostTimeSeries respectively. The relboost "
        "algorithm has been designed to scale well to data "
        "frames with many columns." );
}

// ------------------------------------------------------------------------

void DataModelChecker::warn_too_many_columns_relmt(
    const size_t _num_columns,
    const std::string& _population_name,
    const std::string& _peripheral_name,
    communication::Warner* _warner )
{
    const auto population_name = modify_df_name( _population_name );

    const auto peripheral_name = modify_df_name( _peripheral_name );

    std::string warning = might_take_long() + population_name;

    if ( population_name != peripheral_name )
        {
            warning += " and " + peripheral_name + " contain " +
                       std::to_string( _num_columns );
        }
    else
        {
            warning += " contains " + std::to_string( _num_columns / 2 );
        }

    warning +=
        " numerical columns and time stamps. "
        "Please note that columns created by the preprocessors "
        "are also part of this count. The relmt "
        "algorithm does not scale very well to data frames "
        "with many columns. This pipeline might take a very "
        "long time to fit. You should consider removing some "
        "columns or preprocessors. You can use a column selection "
        "to pick the right columns. "
        "You could also replace "
        "RelMTModel or "
        "RelMTTimeSeries with RelboostModel or "
        "RelboostTimeSeries respectively. The relboost "
        "algorithm has been designed to scale well to data "
        "frames with many columns.";

    _warner->add( warning );
}

// ------------------------------------------------------------------------

void DataModelChecker::warn_too_many_matches(
    const size_t _num_matches,
    const std::string& _join_key_used,
    const std::string& _other_join_key_used,
    const containers::DataFrame& _population_df,
    const containers::DataFrame& _peripheral_df,
    communication::Warner* _warner )
{
    const auto join_key_used = modify_join_key_name( _join_key_used );

    const auto other_join_key_used =
        modify_join_key_name( _other_join_key_used );

    const auto population_name = modify_df_name( _population_df.name() );

    const auto peripheral_name = modify_df_name( _peripheral_df.name() );

    _warner->add(
        might_take_long() + "There are " + std::to_string( _num_matches ) +
        " matches between " + population_name + " and " + peripheral_name +
        " when joined over " + join_key_used + " and " + other_join_key_used +
        ". This pipeline might take a very long time to fit. There are "
        "multiple ways to fix this: \n"
        "1) You could impose a narrower limit on the scope of "
        "this join by reducing the memory (the period of time until "
        "the feature learner 'forgets' historical data). "
        "You can reduce the memory "
        "by setting the appropriate parameter in the .join(...)-method "
        "of the Placeholder. "
        "Please note that a memory of 0.0 means that "
        "the time series will not forget any past "
        "data.\n"
        "2) You could also set the relationship parameter to "
        "propositionalization, which would force the pipeline to use the "
        "FastProp algorithm for this particular join. You can also do that in "
        "the .join(...)-method of the Placeholder.\n"
        "3) You could also use FastPropModel or FastPropTimeSeries for the "
        "entire pipeline." );
}

// ------------------------------------------------------------------------

void DataModelChecker::warn_too_many_nulls(
    const bool _is_float,
    const Float _share_null,
    const std::string& _colname,
    const std::string& _df_name,
    communication::Warner* _warner )
{
    const std::string role = _is_float
                                 ? containers::DataFrame::ROLE_UNUSED_FLOAT
                                 : containers::DataFrame::ROLE_UNUSED_STRING;

    const auto colname = modify_colname( _colname );

    const auto df_name = modify_df_name( _df_name );

    _warner->add(
        column_should_be_unused() + std::to_string( _share_null * 100.0 ) +
        "% of all entries in column '" + colname + "' in " + df_name +
        " are NULL values. You should "
        "consider setting its role to " +
        role + "." );
}

// ------------------------------------------------------------------------

void DataModelChecker::warn_too_many_unique(
    const Float _num_distinct,
    const std::string& _colname,
    const std::string& _df_name,
    communication::Warner* _warner )
{
    const auto colname = modify_colname( _colname );

    const auto df_name = modify_df_name( _df_name );

    _warner->add(
        might_take_long() +
        "The number of unique entries in column "
        "'" +
        colname + "' in " + df_name + " is " +
        std::to_string( static_cast<int>( _num_distinct ) ) +
        ". This might take a long time to fit. You should "
        "consider setting its role to unused_string or using it "
        "for "
        "comparison only (you can do the latter by setting a unit "
        "that "
        "contains 'comparison only')." );
}

// ------------------------------------------------------------------------

void DataModelChecker::warn_unique_share_too_high(
    const Float _unique_share,
    const std::string& _colname,
    const std::string& _df_name,
    communication::Warner* _warner )
{
    const auto colname = modify_colname( _colname );

    const auto df_name = modify_df_name( _df_name );

    _warner->add(
        column_should_be_unused() +
        "The ratio of unique entries to non-NULL entries in column "
        "'" +
        colname + "' in data frame " + df_name + " is " +
        std::to_string( _unique_share * 100.0 ) +
        "%. You should "
        "consider setting its role to unused_string or using it "
        "for "
        "comparison only (you can do the latter by setting a unit "
        "that "
        "contains 'comparison only')." );
}

// ----------------------------------------------------------------------------
}  // namespace preprocessors
}  // namespace engine
