#include "predictors/predictors.hpp"

namespace predictors
{
// -----------------------------------------------------------------------------

GradientBoostingPredictor::GradientBoostingPredictor(
    const Poco::JSON::Object& _hyperparams,
    const std::shared_ptr<const PredictorImpl>& _impl,
    const std::shared_ptr<const std::vector<strings::String>>& _categories )
    : impl_( _impl )
{
    auto hyperparams = _hyperparams;

    hyperparams.set( "delta_t_", 0.0 );
    hyperparams.set( "include_categorical_", false );
    hyperparams.set( "num_subfeatures_", 100 );
    hyperparams.set( "session_name_", "" );
    hyperparams.set( "share_selected_features_", 0.0 );
    hyperparams.set( "use_timestamps_", true );

    model_ = relboost::ensemble::DecisionTreeEnsemble(
        _categories,
        std::make_shared<const relboost::Hyperparameters>( hyperparams ),
        nullptr,
        nullptr,
        nullptr,
        nullptr );
}

// -----------------------------------------------------------------------------

GradientBoostingPredictor::DataFrameType GradientBoostingPredictor::extract_df(
    const std::vector<CIntColumn>& _X_categorical,
    const std::vector<CFloatColumn>& _X_numerical,
    const std::optional<CFloatColumn>& _y ) const
{
    // ------------------------------------------------------------------------

    std::vector<typename DataFrameType::IntColumnType> categoricals;

    for ( size_t i = 0; i < _X_categorical.size(); ++i )
        {
            const auto& col = _X_categorical[i];

            assert_true( col );

            categoricals.push_back( typename DataFrameType::IntColumnType(
                col->data(),
                "categorical_column_" + std::to_string( i ),
                col->size(),
                "" ) );
        }

    std::vector<typename DataFrameType::FloatColumnType> numericals;

    for ( size_t i = 0; i < _X_numerical.size(); ++i )
        {
            const auto& col = _X_numerical[i];

            assert_true( col );

            numericals.push_back( typename DataFrameType::FloatColumnType(
                col->data(),
                "numerical_column_" + std::to_string( i ),
                col->size(),
                "" ) );
        }

    std::vector<typename DataFrameType::FloatColumnType> targets;

    if ( _y )
        {
            const auto& col = *_y;

            assert_true( col );

            targets.push_back( typename DataFrameType::FloatColumnType(
                col->data(), "target_column", col->size(), "" ) );
        }

    // ------------------------------------------------------------------------

    return DataFrameType(
        categoricals, {}, {}, "POPULATION", numericals, targets, {} );

    // ------------------------------------------------------------------------
}

// -----------------------------------------------------------------------------

std::vector<Float> GradientBoostingPredictor::feature_importances(
    const size_t _num_features ) const
{
    // TODO
    return std::vector<Float>( _num_features, 1.0 );
}

// -----------------------------------------------------------------------------

void GradientBoostingPredictor::load( const std::string& _fname )
{
    const auto obj = load_json_obj( _fname );
    model_ =
        relboost::ensemble::DecisionTreeEnsemble( model().categories(), obj );
}

// -----------------------------------------------------------------------------

Poco::JSON::Object GradientBoostingPredictor::load_json_obj(
    const std::string& _fname ) const
{
    std::ifstream input( _fname );

    std::stringstream json;

    std::string line;

    if ( input.is_open() )
        {
            while ( std::getline( input, line ) )
                {
                    json << line;
                }

            input.close();
        }
    else
        {
            throw std::invalid_argument( "File '" + _fname + "' not found!" );
        }

    const auto ptr = Poco::JSON::Parser()
                         .parse( json.str() )
                         .extract<Poco::JSON::Object::Ptr>();

    if ( !ptr )
        {
            throw std::runtime_error( "JSON file did not contain an object!" );
        }

    return *ptr;
}
// -----------------------------------------------------------------------------

std::string GradientBoostingPredictor::fit(
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    const std::vector<CIntColumn>& _X_categorical,
    const std::vector<CFloatColumn>& _X_numerical,
    const CFloatColumn& _y )
{
    const auto df = extract_df( _X_categorical, _X_numerical, _y );

    model().fit( df, {}, _logger );

    return "Fitted " + std::to_string( model().num_features() ) + " trees.";
}

// -----------------------------------------------------------------------------

CFloatColumn GradientBoostingPredictor::predict(
    const std::vector<CIntColumn>& _X_categorical,
    const std::vector<CFloatColumn>& _X_numerical ) const
{
    const auto df = extract_df(
        _X_categorical, _X_numerical, std::optional<CFloatColumn>() );

    auto predictions = model().transform( df, {} );

    assert_true( predictions.size() == 1 );

    assert_true( predictions[0] );

    return CFloatColumn( predictions[0] );
}

// -----------------------------------------------------------------------------

void GradientBoostingPredictor::save( const std::string& _fname ) const
{
    model().save( _fname );
}

// -----------------------------------------------------------------------------
}  // namespace predictors
