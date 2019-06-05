#include "descriptors/descriptors.hpp"

namespace autosql
{
namespace descriptors
{
// ----------------------------------------------------------------------------

void Scores::from_json_obj( const Poco::JSON::Object& _json_obj )
{
    // -------------------------

    accuracy_.clear();
    auc_.clear();
    cross_entropy_.clear();
    mae_.clear();
    rmse_.clear();
    rsquared_.clear();

    // -------------------------

    prediction_min_.clear();
    prediction_step_size_.clear();

    // -------------------------

    if ( _json_obj.has( "prediction_min_" ) )
        {
            prediction_min_ = JSON::array_to_vector<SQLNET_FLOAT>(
                _json_obj.SQLNET_GET_ARRAY( "prediction_min_" ) );
        }

    if ( _json_obj.has( "prediction_step_size_" ) )
        {
            prediction_step_size_ = JSON::array_to_vector<SQLNET_FLOAT>(
                _json_obj.SQLNET_GET_ARRAY( "prediction_step_size_" ) );
        }

    // -------------------------

    if ( _json_obj.has( "accuracy_" ) )
        {
            accuracy_ = JSON::array_to_vector<SQLNET_FLOAT>(
                _json_obj.SQLNET_GET_ARRAY( "accuracy_" ) );
        }

    if ( _json_obj.has( "auc_" ) )
        {
            auc_ = JSON::array_to_vector<SQLNET_FLOAT>(
                _json_obj.SQLNET_GET_ARRAY( "auc_" ) );
        }

    if ( _json_obj.has( "cross_entropy_" ) )
        {
            cross_entropy_ = JSON::array_to_vector<SQLNET_FLOAT>(
                _json_obj.SQLNET_GET_ARRAY( "cross_entropy_" ) );
        }

    if ( _json_obj.has( "mae_" ) )
        {
            mae_ = JSON::array_to_vector<SQLNET_FLOAT>(
                _json_obj.SQLNET_GET_ARRAY( "mae_" ) );
        }

    if ( _json_obj.has( "rmse_" ) )
        {
            rmse_ = JSON::array_to_vector<SQLNET_FLOAT>(
                _json_obj.SQLNET_GET_ARRAY( "rmse_" ) );
        }

    if ( _json_obj.has( "rsquared_" ) )
        {
            rsquared_ = JSON::array_to_vector<SQLNET_FLOAT>(
                _json_obj.SQLNET_GET_ARRAY( "rsquared_" ) );
        }

    // -------------------------

    if ( !_json_obj.getArray( "accuracy_curves_" ).isNull() )
        {
            accuracy_curves().clear();

            auto arr = _json_obj.SQLNET_GET_ARRAY( "accuracy_curves_" );

            for ( size_t i = 0; i < arr->size(); ++i )
                {
                    accuracy_curves().push_back(
                        JSON::array_to_vector<SQLNET_FLOAT>(
                            arr->getArray( static_cast<unsigned int>( i ) ) ) );
                }
        }

    // -------------------------

    if ( !_json_obj.getArray( "average_targets_" ).isNull() )
        {
            average_targets_.clear();

            auto arr = _json_obj.SQLNET_GET_ARRAY( "average_targets_" );

            for ( size_t i = 0; i < arr->size(); ++i )
                {
                    auto vec = std::vector<std::vector<SQLNET_FLOAT>>( 0 );

                    auto arr2 = arr->getArray( static_cast<unsigned int>( i ) );

                    for ( size_t j = 0; j < arr2->size(); ++j )
                        {
                            vec.push_back( JSON::array_to_vector<SQLNET_FLOAT>(
                                arr2->getArray(
                                    static_cast<unsigned int>( j ) ) ) );
                        }

                    average_targets_.emplace_back( std::move( vec ) );
                }
        }

    // -------------------------

    if ( !_json_obj.getArray( "feature_correlations_" ).isNull() )
        {
            feature_correlations_.clear();

            auto arr = _json_obj.SQLNET_GET_ARRAY( "feature_correlations_" );

            for ( size_t i = 0; i < arr->size(); ++i )
                {
                    feature_correlations_.push_back(
                        JSON::array_to_vector<SQLNET_FLOAT>(
                            arr->getArray( static_cast<unsigned int>( i ) ) ) );
                }
        }

    // -------------------------

    if ( !_json_obj.getArray( "feature_densities_" ).isNull() )
        {
            feature_densities_.clear();

            auto arr = _json_obj.SQLNET_GET_ARRAY( "feature_densities_" );

            for ( size_t i = 0; i < arr->size(); ++i )
                {
                    feature_densities_.push_back(
                        JSON::array_to_vector<SQLNET_INT>(
                            arr->getArray( static_cast<unsigned int>( i ) ) ) );
                }
        }

    // -------------------------

    if ( !_json_obj.getArray( "feature_importances_" ).isNull() )
        {
            feature_importances_.clear();

            auto arr = _json_obj.SQLNET_GET_ARRAY( "feature_importances_" );

            for ( size_t i = 0; i < arr->size(); ++i )
                {
                    feature_importances_.push_back(
                        JSON::array_to_vector<SQLNET_FLOAT>(
                            arr->getArray( static_cast<unsigned int>( i ) ) ) );
                }
        }

    // -------------------------

    if ( !_json_obj.getArray( "fpr_" ).isNull() )
        {
            fpr().clear();

            auto arr = _json_obj.SQLNET_GET_ARRAY( "fpr_" );

            for ( size_t i = 0; i < arr->size(); ++i )
                {
                    fpr().push_back( JSON::array_to_vector<SQLNET_FLOAT>(
                        arr->getArray( static_cast<unsigned int>( i ) ) ) );
                }
        }

    // -------------------------

    if ( !_json_obj.getArray( "labels_" ).isNull() )
        {
            labels_.clear();

            auto arr = _json_obj.SQLNET_GET_ARRAY( "labels_" );

            for ( size_t i = 0; i < arr->size(); ++i )
                {
                    labels_.push_back( JSON::array_to_vector<SQLNET_FLOAT>(
                        arr->getArray( static_cast<unsigned int>( i ) ) ) );
                }
        }

    // -------------------------

    if ( !_json_obj.getArray( "tpr_" ).isNull() )
        {
            tpr().clear();

            auto arr = _json_obj.SQLNET_GET_ARRAY( "tpr_" );

            for ( size_t i = 0; i < arr->size(); ++i )
                {
                    tpr().push_back( JSON::array_to_vector<SQLNET_FLOAT>(
                        arr->getArray( static_cast<unsigned int>( i ) ) ) );
                }
        }

    // -------------------------
}

// ----------------------------------------------------------------------------

Poco::JSON::Object Scores::to_json_obj() const
{
    // -------------------------

    Poco::JSON::Object obj;

    // -------------------------

    obj.set( "prediction_min_", JSON::vector_to_array_ptr( prediction_min_ ) );

    obj.set(
        "prediction_step_size_",
        JSON::vector_to_array_ptr( prediction_step_size_ ) );

    // -------------------------

    obj.set( "accuracy_", JSON::vector_to_array_ptr( accuracy_ ) );

    obj.set( "auc_", JSON::vector_to_array_ptr( auc_ ) );

    obj.set( "cross_entropy_", JSON::vector_to_array_ptr( cross_entropy_ ) );

    obj.set( "mae_", JSON::vector_to_array_ptr( mae_ ) );

    obj.set( "rmse_", JSON::vector_to_array_ptr( rmse_ ) );

    obj.set( "rsquared_", JSON::vector_to_array_ptr( rsquared_ ) );

    // -------------------------

    auto accuracy_curves_arr = Poco::JSON::Array();

    for ( auto& vec : accuracy_curves() )
        {
            accuracy_curves_arr.add( JSON::vector_to_array_ptr( vec ) );
        }

    obj.set( "accuracy_curves_", accuracy_curves_arr );

    // -------------------------

    auto average_targets_arr = Poco::JSON::Array();

    for ( auto& vec : average_targets() )
        {
            average_targets_arr.add( JSON::vector_to_array_ptr( vec ) );
        }

    obj.set( "average_targets_", average_targets_arr );

    // -------------------------

    auto feature_correlations_arr =
        JSON::vector_to_array_ptr( feature_correlations() );

    obj.set( "feature_correlations_", feature_correlations_arr );

    // -------------------------

    auto feature_densities_arr =
        JSON::vector_to_array_ptr( feature_densities() );

    obj.set( "feature_densities_", feature_densities_arr );

    // -------------------------

    auto feature_importances_arr =
        JSON::vector_to_array_ptr( feature_importances() );

    obj.set( "feature_importances_", feature_importances_arr );

    // -------------------------

    auto fpr_arr = Poco::JSON::Array();

    for ( auto& vec : fpr() )
        {
            fpr_arr.add( JSON::vector_to_array_ptr( vec ) );
        }

    obj.set( "fpr_", fpr_arr );

    // -------------------------

    Poco::JSON::Array labels_arr = JSON::vector_to_array( labels() );

    obj.set( "labels_", labels_arr );

    // -------------------------

    auto tpr_arr = Poco::JSON::Array();

    for ( auto& vec : tpr() )
        {
            tpr_arr.add( JSON::vector_to_array_ptr( vec ) );
        }

    obj.set( "tpr_", tpr_arr );

    // -------------------------

    return obj;

    // -------------------------
}

// ----------------------------------------------------------------------------
}  // namespace descriptors
}  // namespace autosql
