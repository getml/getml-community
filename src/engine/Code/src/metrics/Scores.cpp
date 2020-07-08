#include "metrics/metrics.hpp"

namespace metrics
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

    if ( _json_obj.has( "set_used_" ) )
        {
            set_used_ = jsonutils::JSON::get_value<std::string>(
                _json_obj, "set_used_" );
        }

    // -------------------------

    update_1d_vector( _json_obj, "prediction_min_", &prediction_min_ );

    update_1d_vector(
        _json_obj, "prediction_step_size_", &prediction_step_size_ );

    update_1d_vector( _json_obj, "feature_names_", &feature_names_ );

    update_1d_vector( _json_obj, "accuracy_", &accuracy_ );

    update_1d_vector( _json_obj, "auc_", &auc_ );

    update_1d_vector( _json_obj, "cross_entropy_", &cross_entropy_ );

    update_1d_vector( _json_obj, "mae_", &mae_ );

    update_1d_vector( _json_obj, "rmse_", &rmse_ );

    update_1d_vector( _json_obj, "rsquared_", &rsquared_ );

    // -------------------------

    update_2d_vector( _json_obj, "accuracy_curves_", &accuracy_curves() );

    update_2d_vector(
        _json_obj, "feature_correlations_", &feature_correlations() );

    update_2d_vector( _json_obj, "feature_densities_", &feature_densities() );

    update_2d_vector(
        _json_obj, "feature_importances_", &feature_importances() );

    update_2d_vector( _json_obj, "fpr_", &fpr() );

    update_2d_vector( _json_obj, "lift_", &lift() );

    update_2d_vector( _json_obj, "labels_", &labels() );

    update_2d_vector( _json_obj, "precision_", &precision() );

    update_2d_vector( _json_obj, "proportion_", &proportion() );

    update_2d_vector( _json_obj, "tpr_", &tpr() );

    // -------------------------

    update_3d_vector( _json_obj, "average_targets_", &average_targets_ );

    // -------------------------
}

// ----------------------------------------------------------------------------

void Scores::save( const std::string& _fname ) const
{
    std::ofstream fs( _fname, std::ofstream::out );

    Poco::JSON::Stringifier::stringify( to_json_obj(), fs );

    fs.close();
}

// ----------------------------------------------------------------------------

Poco::JSON::Object Scores::to_json_obj() const
{
    // -------------------------

    Poco::JSON::Object obj;

    // -------------------------

    obj.set( "set_used_", set_used_ );

    // -------------------------

    obj.set(
        "prediction_min_",
        jsonutils::JSON::vector_to_array_ptr( prediction_min_ ) );

    obj.set(
        "prediction_step_size_",
        jsonutils::JSON::vector_to_array_ptr( prediction_step_size_ ) );

    // -------------------------

    obj.set(
        "feature_names_",
        jsonutils::JSON::vector_to_array_ptr( feature_names() ) );

    // -------------------------

    obj.set( "accuracy_", jsonutils::JSON::vector_to_array_ptr( accuracy_ ) );

    obj.set( "auc_", jsonutils::JSON::vector_to_array_ptr( auc_ ) );

    obj.set(
        "cross_entropy_",
        jsonutils::JSON::vector_to_array_ptr( cross_entropy_ ) );

    obj.set( "mae_", jsonutils::JSON::vector_to_array_ptr( mae_ ) );

    obj.set( "rmse_", jsonutils::JSON::vector_to_array_ptr( rmse_ ) );

    obj.set( "rsquared_", jsonutils::JSON::vector_to_array_ptr( rsquared_ ) );

    // -------------------------

    obj.set( "accuracy_curves_", to_2d_array( accuracy_curves() ) );

    obj.set( "feature_correlations_", to_2d_array( feature_correlations() ) );

    obj.set( "feature_densities_", to_2d_array( feature_densities() ) );

    obj.set( "feature_importances_", to_2d_array( feature_importances() ) );

    obj.set( "fpr_", to_2d_array( fpr() ) );

    obj.set( "labels_", to_2d_array( labels() ) );

    obj.set( "lift_", to_2d_array( lift() ) );

    obj.set( "precision_", to_2d_array( precision() ) );

    obj.set( "proportion_", to_2d_array( proportion() ) );

    obj.set( "tpr_", to_2d_array( tpr() ) );

    // -------------------------

    obj.set( "average_targets_", to_3d_array( average_targets() ) );

    // -------------------------

    return obj;

    // -------------------------
}

// ----------------------------------------------------------------------------
}  // namespace metrics
