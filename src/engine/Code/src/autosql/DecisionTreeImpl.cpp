#include "decisiontrees/decisiontrees.hpp"

namespace autosql
{
namespace decisiontrees
{
// ----------------------------------------------------------------------------

std::string DecisionTreeImpl::get_colname(
    const std::string& _feature_num,
    const DataUsed _data_used,
    const AUTOSQL_INT _ix_column_used,
    const bool _equals ) const
{
    std::string colname;

    switch ( _data_used )
        {
            case DataUsed::not_applicable:

                colname = "*";

                break;

            case DataUsed::same_unit_categorical:

                colname = get_colname(
                    _feature_num,
                    std::get<0>( same_units_categorical()[_ix_column_used] )
                        .data_used,
                    std::get<0>( same_units_categorical()[_ix_column_used] )
                        .ix_column_used );

                if ( _equals )
                    {
                        colname.append( " = " );
                    }
                else
                    {
                        colname.append( " != " );
                    }

                colname.append( get_colname(
                    _feature_num,
                    std::get<1>( same_units_categorical()[_ix_column_used] )
                        .data_used,
                    std::get<1>( same_units_categorical()[_ix_column_used] )
                        .ix_column_used ) );

                break;

            case DataUsed::same_unit_discrete:

                colname =
                    get_colname(
                        _feature_num,
                        std::get<1>( same_units_discrete()[_ix_column_used] )
                            .data_used,
                        std::get<1>( same_units_discrete()[_ix_column_used] )
                            .ix_column_used ) +
                    " - " +
                    get_colname(
                        _feature_num,
                        std::get<0>( same_units_discrete()[_ix_column_used] )
                            .data_used,
                        std::get<0>( same_units_discrete()[_ix_column_used] )
                            .ix_column_used );

                break;

            case DataUsed::same_unit_numerical:

                colname =
                    get_colname(
                        _feature_num,
                        std::get<1>( same_units_numerical()[_ix_column_used] )
                            .data_used,
                        std::get<1>( same_units_numerical()[_ix_column_used] )
                            .ix_column_used ) +
                    " - " +
                    get_colname(
                        _feature_num,
                        std::get<0>( same_units_numerical()[_ix_column_used] )
                            .data_used,
                        std::get<0>( same_units_numerical()[_ix_column_used] )
                            .ix_column_used );

                break;

            case DataUsed::x_perip_categorical:

                colname =
                    "t2." + x_perip_categorical_colname( _ix_column_used );

                break;

            case DataUsed::x_perip_numerical:

                colname = "t2." + x_perip_numerical_colname( _ix_column_used );

                break;

            case DataUsed::x_perip_discrete:

                colname = "t2." + x_perip_discrete_colname( _ix_column_used );

                break;

            case DataUsed::x_popul_categorical:

                colname =
                    "t1." + x_popul_categorical_colname( _ix_column_used );

                break;

            case DataUsed::x_popul_numerical:

                colname = "t1." + x_popul_numerical_colname( _ix_column_used );

                break;

            case DataUsed::x_popul_discrete:

                colname = "t1." + x_popul_discrete_colname( _ix_column_used );

                break;

            case DataUsed::x_subfeature:

                colname = "t2.feature_" + _feature_num + "_" +
                          std::to_string( _ix_column_used + 1 );

                break;

            case DataUsed::time_stamps_diff:

                colname = "t1." + time_stamps_popul_name_ + " - t2." +
                          time_stamps_perip_name_;

                break;

            default:

                assert( false && "Unknown DataUsed in get_colname(...)!" );
        }

    return colname;
}

// ----------------------------------------------------------------------------

void DecisionTreeImpl::source_importances(
    const DataUsed _data_used,
    const AUTOSQL_INT _ix_column_used,
    const AUTOSQL_FLOAT _factor,
    std::map<descriptors::SourceImportancesColumn, AUTOSQL_FLOAT>& _map ) const
{
    descriptors::SourceImportancesColumn col;

    // ---------------------------------------------------

    switch ( _data_used )
        {
            case DataUsed::not_applicable:

                col.table_ = peripheral_name_;

                col.column_ = "COUNT";

                break;

            case DataUsed::same_unit_categorical:

                source_importances(
                    std::get<0>( same_units_categorical()[_ix_column_used] )
                        .data_used,
                    std::get<0>( same_units_categorical()[_ix_column_used] )
                        .ix_column_used,
                    _factor * 0.5,
                    _map );

                source_importances(
                    std::get<1>( same_units_categorical()[_ix_column_used] )
                        .data_used,
                    std::get<1>( same_units_categorical()[_ix_column_used] )
                        .ix_column_used,
                    _factor * 0.5,
                    _map );

                return;

            case DataUsed::same_unit_discrete:

                source_importances(
                    std::get<0>( same_units_discrete()[_ix_column_used] )
                        .data_used,
                    std::get<0>( same_units_discrete()[_ix_column_used] )
                        .ix_column_used,
                    _factor * 0.5,
                    _map );

                source_importances(
                    std::get<1>( same_units_discrete()[_ix_column_used] )
                        .data_used,
                    std::get<1>( same_units_discrete()[_ix_column_used] )
                        .ix_column_used,
                    _factor * 0.5,
                    _map );

                return;

            case DataUsed::same_unit_numerical:

                source_importances(
                    std::get<0>( same_units_numerical()[_ix_column_used] )
                        .data_used,
                    std::get<0>( same_units_numerical()[_ix_column_used] )
                        .ix_column_used,
                    _factor * 0.5,
                    _map );

                source_importances(
                    std::get<1>( same_units_numerical()[_ix_column_used] )
                        .data_used,
                    std::get<1>( same_units_numerical()[_ix_column_used] )
                        .ix_column_used,
                    _factor * 0.5,
                    _map );

                return;

            case DataUsed::x_perip_categorical:

                col.table_ = peripheral_name_;

                col.column_ = x_perip_categorical_colname( _ix_column_used );

                break;

            case DataUsed::x_perip_numerical:

                col.table_ = peripheral_name_;

                col.column_ = x_perip_numerical_colname( _ix_column_used );

                break;

            case DataUsed::x_perip_discrete:

                col.table_ = peripheral_name_;

                col.column_ = x_perip_discrete_colname( _ix_column_used );

                break;

            case DataUsed::x_popul_categorical:

                col.table_ = population_name_;

                col.column_ = x_popul_categorical_colname( _ix_column_used );

                break;

            case DataUsed::x_popul_numerical:

                col.table_ = population_name_;

                col.column_ = x_popul_numerical_colname( _ix_column_used );

                break;

            case DataUsed::x_popul_discrete:

                col.table_ = population_name_;

                col.column_ = x_popul_discrete_colname( _ix_column_used );

                break;

            default:

                col.table_ = peripheral_name_;

                col.column_ = time_stamps_perip_name_;

                break;
        }

    // ---------------------------------------------------

    auto it = _map.find( col );

    if ( it == _map.end() )
        {
            _map[col] = _factor;
        }
    else
        {
            it->second += _factor;
        }

    // ---------------------------------------------------
}

// ----------------------------------------------------------------------------
}
}