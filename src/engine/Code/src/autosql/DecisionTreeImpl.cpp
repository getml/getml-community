#include "autosql/decisiontrees/decisiontrees.hpp"

namespace autosql
{
namespace decisiontrees
{
// ----------------------------------------------------------------------------

std::string DecisionTreeImpl::get_colname(
    const std::string& _feature_num,
    const enums::DataUsed _data_used,
    const size_t _ix_column_used,
    const bool _equals ) const
{
    std::string colname;

    switch ( _data_used )
        {
            case enums::DataUsed::not_applicable:

                colname = "*";

                break;

            case enums::DataUsed::same_unit_categorical:

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

            case enums::DataUsed::same_unit_discrete:

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

            case enums::DataUsed::same_unit_numerical:

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

            case enums::DataUsed::x_perip_categorical:

                colname = "t2." + input().categorical_name( _ix_column_used );

                break;

            case enums::DataUsed::x_perip_numerical:

                colname = "t2." + input().numerical_name( _ix_column_used );

                break;

            case enums::DataUsed::x_perip_discrete:

                colname = "t2." + input().discrete_name( _ix_column_used );

                break;

            case enums::DataUsed::x_popul_categorical:

                colname = "t1." + output().categorical_name( _ix_column_used );

                break;

            case enums::DataUsed::x_popul_numerical:

                colname = "t1." + output().numerical_name( _ix_column_used );

                break;

            case enums::DataUsed::x_popul_discrete:

                colname = "t1." + output().discrete_name( _ix_column_used );

                break;

            case enums::DataUsed::x_subfeature:

                colname = "t2.feature_" + _feature_num + "_" +
                          std::to_string( _ix_column_used + 1 );

                break;

            case enums::DataUsed::time_stamps_diff:

                colname = "t1." + output().time_stamps_name() + " - t2." +
                          input().time_stamps_name();

                break;

            case enums::DataUsed::time_stamps_window:

                colname = "t1." + output().time_stamps_name() + " - t2." +
                          input().time_stamps_name();

                break;

            default:

                assert_true(
                    false && "Unknown enums::DataUsed in get_colname(...)!" );
        }

    return colname;
}

// ----------------------------------------------------------------------------
}  // namespace decisiontrees
}  // namespace autosql
