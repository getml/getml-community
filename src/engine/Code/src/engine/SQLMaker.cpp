#include "engine/utils/utils.hpp"

namespace engine
{
namespace utils
{
// ----------------------------------------------------------------------------

std::string SQLMaker::make_feature_table(
    const std::string& _main_table,
    const std::vector<std::string>& _autofeatures,
    const std::vector<std::string>& _sql,
    const predictors::PredictorImpl& _predictor_impl )
{
    std::string sql = "DROP TABLE IF EXISTS \"FEATURES\";\n\n";

    sql += "CREATE TABLE \"FEATURES\" AS\n";

    sql += make_select( _main_table, _autofeatures, _predictor_impl );

    const auto main_table =
        helpers::SQLGenerator::get_table_name( _main_table );

    sql += "FROM \"" + main_table + "\" t1\n";

    sql += helpers::SQLGenerator::handle_many_to_one_joins( _main_table, "t1" );

    sql += make_left_joins( _autofeatures );

    sql += "ORDER BY t1.rowid;\n\n";

    return sql;
}

// ----------------------------------------------------------------------------

std::string SQLMaker::make_left_joins(
    const std::vector<std::string>& _autofeatures )
{
    std::string sql;

    for ( const auto table : _autofeatures )
        {
            sql += "LEFT JOIN \"" +
                   helpers::StringReplacer::replace_all(
                       table, "feature", "FEATURE" ) +
                   "\" f_" + table.substr( 8 ) + "\n";

            sql += "ON t1.rowid = f_" + table.substr( 8 ) + ".\"rownum\"\n";
        }

    return sql;
}

// ----------------------------------------------------------------------------

std::string SQLMaker::make_postprocessing(
    const std::vector<std::string>& _sql )
{
    std::string sql;

    for ( const auto feature : _sql )
        {
            const auto pos = feature.find( "\";\n" );

            throw_unless(
                pos != std::string::npos,
                "Could not find end of DROP TABLE IF EXISTS statement." );

            sql += feature.substr( 0, pos ) + "\";\n";
        }

    return sql;
}

// ----------------------------------------------------------------------------

std::string SQLMaker::make_select(
    const std::string& _main_table,
    const std::vector<std::string>& _autofeatures,
    const predictors::PredictorImpl& _predictor_impl )
{
    const auto concat = []( const std::vector<std::string>& _vec1,
                            const std::vector<std::string>& _vec2 ) {
        auto vec = _vec1;
        vec.insert( vec.end(), _vec2.begin(), _vec2.end() );
        return vec;
    };

    std::string sql = "SELECT ";

    const auto numerical = _predictor_impl.numerical_colnames();

    const auto categorical = _predictor_impl.categorical_colnames();

    const auto manual_features = concat( numerical, categorical );

    const auto modified_colnames =
        helpers::Macros::modify_colnames( manual_features );

    for ( size_t i = 0; i < manual_features.size(); ++i )
        {
            const std::string begin = ( i == 0 ? "" : "       " );

            const std::string data_type =
                ( i < numerical.size() ? "REAL" : "TEXT" );

            const bool no_comma =
                ( i == manual_features.size() - 1 &&
                  _autofeatures.size() == 0 );

            const auto end = no_comma ? "\"\n" : "\",\n";

            sql += begin + "CAST( " +
                   helpers::SQLGenerator::edit_colname(
                       manual_features.at( i ), "t1" ) +
                   " AS " + data_type + " ) AS \"manual_feature_" +
                   std::to_string( i + 1 ) + "__" + modified_colnames.at( i ) +
                   end;
        }

    for ( size_t i = 0; i < _autofeatures.size(); ++i )
        {
            const auto end = ( i == _autofeatures.size() - 1 ? "\n" : ",\n" );

            sql += "       CAST( COALESCE( f_" +
                   _autofeatures.at( i ).substr( 8 ) + ".\"" +
                   _autofeatures.at( i ) + "\", 0.0 ) AS REAL ) AS \"" +
                   _autofeatures.at( i ) + "\"" + end;
        }

    return sql;
}

// ----------------------------------------------------------------------------

std::string SQLMaker::make_sql(
    const std::string& _main_table,
    const std::vector<std::string>& _autofeatures,
    const std::vector<std::string>& _sql,
    const predictors::PredictorImpl& _predictor_impl )
{
    std::string sql;

    for ( const auto& feature : _sql )
        {
            sql += feature;
        }

    sql +=
        make_feature_table( _main_table, _autofeatures, _sql, _predictor_impl );

    sql += make_postprocessing( _sql );

    return sql;
}

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace engine

