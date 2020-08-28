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

    assert_true( _peripheral_names );

    if ( _peripheral_names->size() != _peripheral.size() )
        {
            throw std::invalid_argument(
                "The number of peripheral tables in the placeholder must "
                "be "
                "equal to the number of peripheral tables passed (" +
                std::to_string( _peripheral_names->size() ) + " vs. " +
                std::to_string( _peripheral.size() ) +
                "). This is the point of having placeholders." );
        }

    // --------------------------------------------------------------------------

    communication::Warner warner;

    // --------------------------------------------------------------------------

    check_data_frames( _population, _peripheral, _feature_learners, &warner );

    // --------------------------------------------------------------------------

    assert_true( _placeholder );

    if ( _feature_learners.size() > 0 )
        {
            check_join(
                *_placeholder,
                _peripheral_names,
                _population,
                _peripheral,
                &warner );

            check_self_joins(
                *_placeholder,
                _population,
                _peripheral,
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
        utils::ColumnOperators::count_categorical( *_col.data_ptr() );

    const auto share_null = 1.0 - num_non_null / length;

    if ( share_null > 0.9 )
        {
            _warner->add(
                info() + std::to_string( share_null * 100.0 ) +
                "\% of all entries in column '" + _col.name() +
                "' in data frame '" + _df_name +
                "' are NULL values. You should "
                "consider setting its role to unused_string." );
        }

    if ( num_non_null < 0.5 )
        {
            return;
        }

    // --------------------------------------------------------------------------

    const Float num_distinct =
        utils::ColumnOperators::count_distinct( *_col.data_ptr() );

    if ( num_distinct == 1.0 )
        {
            _warner->add(
                column_should_be_unused() + "All non-NULL entries in column '" +
                _col.name() + "' in data frame '" + _df_name +
                "' are equal to each other. You should "
                "consider setting its role to unused_string." );
        }

    // --------------------------------------------------------------------------

    const bool is_comparison_only =
        ( _col.unit().find( "comparison only" ) != std::string::npos );

    if ( num_distinct > 1000.0 && !is_comparison_only )
        {
            _warner->add(
                might_take_long() +
                "The number of unique entries in column "
                "'" +
                _col.name() + "' in data frame '" + _df_name + "' is " +
                std::to_string( static_cast<int>( num_distinct ) ) +
                ". This might take a long time to fit. You should "
                "consider setting its role to unused_string or using it "
                "for "
                "comparison only (you can do the latter by setting a unit "
                "that "
                "contains 'comparison only')." );
        }

    // --------------------------------------------------------------------------

    const auto unique_share = num_distinct / num_non_null;

    if ( !is_comparison_only && unique_share > 0.25 )
        {
            _warner->add(
                column_should_be_unused() +
                "The ratio of unique entries to non-NULL entries in column "
                "'" +
                _col.name() + "' in data frame '" + _df_name + "' is " +
                std::to_string( unique_share * 100.0 ) +
                "\%. You should "
                "consider setting its role to unused_string or using it "
                "for "
                "comparison only (you can do the latter by setting a unit "
                "that "
                "contains 'comparison only')." );
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

    const auto is_multirel = []( const std::shared_ptr<
                                 const featurelearners::AbstractFeatureLearner>&
                                     fl ) {
        assert_true( fl );
        return fl->type() ==
                   featurelearners::AbstractFeatureLearner::MULTIREL_MODEL ||
               fl->type() == featurelearners::AbstractFeatureLearner::
                                 MULTIREL_TIME_SERIES;
    };

    const auto is_multirel_ts =
        []( const std::shared_ptr<
            const featurelearners::AbstractFeatureLearner>& fl ) {
            assert_true( fl );
            return fl->type() == featurelearners::AbstractFeatureLearner::
                                     MULTIREL_TIME_SERIES;
        };

    const bool has_multirel = std::any_of(
        _feature_learners.begin(), _feature_learners.end(), is_multirel );

    const bool has_multirel_ts = std::any_of(
        _feature_learners.begin(), _feature_learners.end(), is_multirel_ts );

    // --------------------------------------------------------------------------

    // Too many columns in the population table are only a problem if there is a
    // multirel time series, as the population table is usually not aggregated.
    check_df( _population, has_multirel_ts, _warner );

    for ( const auto& df : _peripheral )
        {
            check_df( df, has_multirel, _warner );
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
            _warner->add( "Data frame '" + _df.name() + "' is empty." );
            return;
        }

    // --------------------------------------------------------------------------

    if ( _check_num_columns )
        {
            const auto num_columns =
                _df.num_numericals() + _df.num_categoricals();

            if ( num_columns > 20 )
                {
                    _warner->add(
                        might_take_long() + "Data frame '" + _df.name() +
                        "' contains " + std::to_string( num_columns ) +
                        " categorical and numerical columns. "
                        "Please note that columns created by the preprocessors "
                        "are also part of this count. The multirel "
                        "algorithm does not scale very well to data frames "
                        "with many columns. This pipeline might take a very "
                        "long time to fit. You should consider removing some "
                        "columns or preprocessors. You could also replace "
                        "MultirelModel or "
                        "MultirelTimeSeries with RelboostModel or "
                        "RelboostTimeSeries respectively. The relboost "
                        "algorithm has been designed to scale well to data "
                        "frames with many columns." );
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
        utils::ColumnOperators::count( _col.begin(), _col.end() );

    const auto share_null = 1.0 - num_non_null / length;

    if ( share_null > 0.9 )
        {
            _warner->add(
                column_should_be_unused() +
                std::to_string( share_null * 100.0 ) +
                "\% of all entries in column '" + _col.name() +
                "' in data frame '" + _df_name +
                "' are NULL values. You should "
                "consider setting its role to unused_float." );
        }

    // --------------------------------------------------------------------------

    const bool is_comparison_only =
        ( _col.unit().find( "comparison only" ) != std::string::npos );

    const auto all_equal = !is_comparison_only && is_all_equal( _col );

    if ( all_equal )
        {
            _warner->add(
                column_should_be_unused() + "All non-NULL entries in column '" +
                _col.name() + "' in data frame '" + _df_name +
                "' are equal to each other. You should "
                "consider setting its role to unused_float or using it for "
                "comparison only (you can do the latter by setting a unit "
                "that contains 'comparison only')." );
        }

    // --------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DataModelChecker::check_join(
    const helpers::Placeholder& _placeholder,
    const std::shared_ptr<const std::vector<std::string>> _peripheral_names,
    const containers::DataFrame& _population,
    const std::vector<containers::DataFrame>& _peripheral,
    communication::Warner* _warner )
{
    // ------------------------------------------------------------------------

    assert_true( _peripheral_names );

    assert_true( _peripheral_names->size() == _peripheral.size() );

    // ------------------------------------------------------------------------

    const auto& joined_tables = _placeholder.joined_tables_;

    const auto& join_keys_used = _placeholder.join_keys_used_;

    const auto& other_join_keys_used = _placeholder.other_join_keys_used_;

    const auto& time_stamps_used = _placeholder.time_stamps_used_;

    const auto& other_time_stamps_used = _placeholder.other_time_stamps_used_;

    const auto& upper_time_stamps_used = _placeholder.upper_time_stamps_used_;

    // ------------------------------------------------------------------------

    const auto size = joined_tables.size();

    assert_true( join_keys_used.size() == size );

    assert_true( other_join_keys_used.size() == size );

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

            const auto [is_many_to_one, num_matches, num_jk_not_found] =
                check_matches(
                    join_keys_used.at( i ),
                    other_join_keys_used.at( i ),
                    time_stamps_used.at( i ),
                    other_time_stamps_used.at( i ),
                    upper_time_stamps_used.at( i ),
                    _population,
                    _peripheral.at( dist ) );

            raise_join_warnings(
                is_many_to_one,
                num_matches,
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
                _warner );
        }

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::tuple<bool, size_t, size_t> DataModelChecker::check_matches(
    const std::string& _join_key_used,
    const std::string& _other_join_key_used,
    const std::string& _time_stamp_used,
    const std::string& _other_time_stamp_used,
    const std::string& _upper_time_stamp_used,
    const containers::DataFrame& _population_df,
    const containers::DataFrame& _peripheral_df )
{
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

    // ------------------------------------------------------------------------

    for ( size_t ix1 = 0; ix1 < jk1.size(); ++ix1 )
        {
            const auto it2 = map2.find( jk1[ix1] );

            if ( it2 == map2.end() )
                {
                    num_jk_not_found++;
                    continue;
                }

            size_t local_num_matches = 0;

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

                    ++local_num_matches;

                    if ( local_num_matches > 1 )
                        {
                            is_many_to_one = false;
                        }
                }
        }

    // ------------------------------------------------------------------------

    return std::make_tuple( is_many_to_one, num_matches, num_jk_not_found );

    // ------------------------------------------------------------------------
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

            for ( size_t i = old_size; i < new_size; ++i )
                {
                    const auto [is_many_to_one, num_matches, num_jk_not_found] =
                        check_matches(
                            join_keys_used.at( i ),
                            other_join_keys_used.at( i ),
                            time_stamps_used.at( i ),
                            other_time_stamps_used.at( i ),
                            upper_time_stamps_used.at( i ),
                            new_population,
                            new_peripheral.back() );

                    raise_self_join_warnings(
                        is_many_to_one, num_matches, new_population, _warner );
                }

            // ----------------------------------------------------------------
        }

    // ------------------------------------------------------------------------
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

void DataModelChecker::raise_join_warnings(
    const bool _is_many_to_one,
    const size_t _num_matches,
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
            _warner->add(
                data_model_can_be_improved() +
                "There are no matches between '" + _join_key_used + "' in '" +
                _population_df.name() + "' and '" + _other_join_key_used +
                "' in '" + _peripheral_df.name() +
                "'. You should consider removing this join from your data "
                "model or re-examine your join keys." );

            return;
        }

    // ------------------------------------------------------------------------

    if ( _is_many_to_one )
        {
            _warner->add(
                data_model_can_be_improved() + "'" + _population_df.name() +
                "' and '" + _peripheral_df.name() +
                "' are in a many-to-one or one-to-one relationship when "
                "joined "
                "over '" +
                _join_key_used + "' and '" + _other_join_key_used +
                "'. Aggregating over such relationships makes little "
                "sense. "
                "You should consider removing this join from your data "
                "model and directly joining '" +
                _peripheral_df.name() + "' on '" + _population_df.name() +
                "' using the data frame's built-in join method." );
        }

    // ------------------------------------------------------------------------

    const auto avg_num_matches = static_cast<Float>( _num_matches ) /
                                 static_cast<Float>( _population_df.nrows() );

    if ( avg_num_matches > 300.0 )
        {
            _warner->add(
                might_take_long() + "There are " +
                std::to_string( _num_matches ) + " matches between '" +
                _population_df.name() + "' and '" + _peripheral_df.name() +
                "' when joined over '" + _join_key_used + "' and '" +
                _other_join_key_used +
                "'. This pipeline might take a very long time to fit. "
                "You should consider imposing a narrower limit on the scope of "
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

    const auto not_found_ratio = static_cast<Float>( _num_jk_not_found ) /
                                 static_cast<Float>( _population_df.nrows() );

    if ( not_found_ratio > 0.0 )
        {
            _warner->add(
                join_keys_not_found() + "When joining '" +
                _population_df.name() + "' and '" + _peripheral_df.name() +
                "' over '" + _join_key_used + "' and '" + _other_join_key_used +
                "', there are no corresponding entries for " +
                std::to_string( not_found_ratio * 100.0 ) +
                "\% of entries in join key '" + _join_key_used + "' in '" +
                _population_df.name() +
                "'. You might want to double-check your join keys." );
        }

    // ------------------------------------------------------------------------
}

// ------------------------------------------------------------------------

void DataModelChecker::raise_self_join_warnings(
    const bool _is_many_to_one,
    const size_t _num_matches,
    const containers::DataFrame& _population_df,
    communication::Warner* _warner )
{
    // ------------------------------------------------------------------------

    if ( _num_matches == 0 )
        {
            _warner->add(
                data_model_can_be_improved() + "The self-join on '" +
                _population_df.name() +
                "' created by the time series feature learner has no matches. "
                "You should examine your join keys." );

            return;
        }

    // ------------------------------------------------------------------------

    if ( _is_many_to_one )
        {
            _warner->add(
                data_model_can_be_improved() + "The self-join on '" +
                _population_df.name() +
                "' created by the time series feature learner is a "
                "one-to-one relationship. Using a time series feature learner "
                "for such a data set makes little sense. You should consider "
                "using a normal feature learner instead." );
        }

    // ------------------------------------------------------------------------

    const auto avg_num_matches = static_cast<Float>( _num_matches ) /
                                 static_cast<Float>( _population_df.nrows() );

    if ( avg_num_matches > 300.0 )
        {
            _warner->add(
                might_take_long() + "The self-join on '" +
                _population_df.name() +
                "' created by the time series feature learner "
                "has a total of " +
                std::to_string( _num_matches ) +
                " matches.  This can take a long time to fit. "
                "You should consider imposing a narrower limit on the scope of "
                "this join by reducing the memory (the period of time until "
                "the time series feature learner 'forgets' historical data). "
                "You can "
                "do so by setting the appropriate hyperparameter in the "
                "feature learner. Please note that a memory of 0.0 means that "
                "the time series feature learner will not forget any past "
                "data." );
        }

    // ------------------------------------------------------------------------
}

// ------------------------------------------------------------------------
// ----------------------------------------------------------------------------
}  // namespace preprocessors
}  // namespace engine
