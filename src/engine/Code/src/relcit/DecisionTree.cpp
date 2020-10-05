#include "relcit/decisiontrees/decisiontrees.hpp"

namespace relcit
{
namespace decisiontrees
{
// ----------------------------------------------------------------------------

DecisionTree::DecisionTree(
    const std::shared_ptr<const Hyperparameters>& _hyperparameters,
    const std::shared_ptr<lossfunctions::LossFunction>& _loss_function,
    const size_t _peripheral_used,
    const std::shared_ptr<const utils::StandardScaler>& _output_scaler,
    const std::shared_ptr<const utils::StandardScaler>& _input_scaler,
    multithreading::Communicator* _comm )
    : comm_( _comm ),
      hyperparameters_( _hyperparameters ),
      initial_loss_reduction_( 0.0 ),
      input_scaler_( _input_scaler ),
      intercept_( 0.0 ),
      loss_function_( _loss_function ),
      output_scaler_( _output_scaler ),
      peripheral_used_( _peripheral_used ),
      update_rate_( 0.0 )
{
}

// ----------------------------------------------------------------------------

DecisionTree::DecisionTree(
    const std::shared_ptr<const Hyperparameters>& _hyperparameters,
    const std::shared_ptr<lossfunctions::LossFunction>& _loss_function,
    const Poco::JSON::Object& _obj )
    : comm_( nullptr ),
      hyperparameters_( _hyperparameters ),
      initial_loss_reduction_( 0.0 )
{
    from_json_obj( _obj, _loss_function );
}

// ----------------------------------------------------------------------------

std::tuple<Float, Float, std::vector<Float>> DecisionTree::calc_initial_weights(
    const containers::DataFrameView& _output,
    const containers::DataFrame& _input,
    const containers::Subfeatures& _subfeatures,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _end )
{
    const auto ncols = _input.num_numericals() + _input.num_discretes() +
                       _output.num_numericals() + _output.num_discretes() +
                       _subfeatures.size();

    const auto zero_weights = std::vector<Float>( ncols + 1 );

    const auto [_, new_weights] = loss_function().calc_pair(
        enums::Revert::False,
        enums::Update::calc_one,
        hyperparameters().min_num_samples_,
        0.0,
        zero_weights,
        _begin,
        _begin,
        _end,
        _end );

    loss_function().revert_to_commit();

    const auto loss_reduction = loss_function().evaluate_split(
        0.0, zero_weights, new_weights, _begin, _end, _end );

    loss_function().commit( 0.0, zero_weights, new_weights );

    return std::make_tuple(
        loss_reduction,
        std::get<0>( new_weights ),
        std::get<1>( new_weights ) );
}

// ----------------------------------------------------------------------------

void DecisionTree::column_importances(
    utils::ImportanceMaker* _importance_maker ) const
{
    // ------------------------------------------------------------------------

    assert_true( root_ );

    assert_true( input_ );

    assert_true( output_ );

    assert_true( initial_weights_.size() > 0 );

    assert_true(
        initial_weights_.size() >=
        input_->discretes_.size() + input_->numericals_.size() +
            output_->discretes_.size() + output_->numericals_.size() + 1 );

    // ------------------------------------------------------------------------

    const auto sum_abs = []( const Float init, const Float val ) {
        return init + std::abs( val );
    };

    const auto sum_weights = std::accumulate(
        initial_weights_.begin() + 1, initial_weights_.end(), 0.0, sum_abs );

    const auto divide_by_sum = [sum_weights]( const Float val ) {
        return std::abs( val ) / sum_weights;
    };

    auto rescaled_weights = std::vector<Float>( initial_weights_.size() - 1 );

    if ( sum_weights > 0.0 )
        {
            std::transform(
                initial_weights_.begin() + 1,
                initial_weights_.end(),
                rescaled_weights.begin(),
                divide_by_sum );
        }

    // ------------------------------------------------------------------------

    size_t i = 0;

    for ( size_t j = 0; j < input_->discretes_.size(); ++i, ++j )
        {
            _importance_maker->add(
                *input_,
                *output_,
                enums::DataUsed::discrete_input,
                j,
                0,
                initial_loss_reduction_ * rescaled_weights.at( i ) );
        }

    for ( size_t j = 0; j < input_->numericals_.size(); ++i, ++j )
        {
            _importance_maker->add(
                *input_,
                *output_,
                enums::DataUsed::numerical_input,
                j,
                0,
                initial_loss_reduction_ * rescaled_weights.at( i ) );
        }

    for ( size_t j = 0; j < output_->discretes_.size(); ++i, ++j )
        {
            _importance_maker->add(
                *input_,
                *output_,
                enums::DataUsed::discrete_output,
                j,
                0,
                initial_loss_reduction_ * rescaled_weights.at( i ) );
        }

    for ( size_t j = 0; j < output_->numericals_.size(); ++i, ++j )
        {
            _importance_maker->add(
                *input_,
                *output_,
                enums::DataUsed::numerical_output,
                j,
                0,
                initial_loss_reduction_ * rescaled_weights.at( i ) );
        }

    // ------------------------------------------------------------------------

    for ( size_t j = 0; i < rescaled_weights.size(); ++i, ++j )
        {
            _importance_maker->add(
                *input_,
                *output_,
                enums::DataUsed::subfeatures,
                j,
                0,
                initial_loss_reduction_ * rescaled_weights.at( i ) );
        }

    // ------------------------------------------------------------------------

    root_->column_importances( _importance_maker );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DecisionTree::fit(
    const containers::DataFrameView& _output,
    const std::optional<containers::DataFrame>& _input,
    const containers::Subfeatures& _subfeatures,
    const containers::Rescaled& _output_rescaled,
    const containers::Rescaled& _input_rescaled,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _end )
{
    // ------------------------------------------------------------------------
    // Store input and output (we need the column names).

    if ( _input )
        {
            input_.reset( new containers::Placeholder( _input->to_schema() ) );
        }

    output_.reset( new containers::Placeholder( _output.df().to_schema() ) );

    // ------------------------------------------------------------------------

    // TODO: Remove optional
    assert_true( _input );

    std::tie( initial_loss_reduction_, intercept_, initial_weights_ ) =
        calc_initial_weights( _output, *_input, _subfeatures, _begin, _end );

    // ------------------------------------------------------------------------

    debug_log( "Set up and fit root node." );

    root_.reset( new DecisionTreeNode(
        utils::ConditionMaker(
            hyperparameters().delta_t_,
            peripheral_used(),
            input_scaler_,
            output_scaler_ ),
        0,
        hyperparameters_,
        loss_function_,
        initial_weights_,
        &comm() ) );

    root_->fit(
        _output,
        _input,
        _subfeatures,
        _output_rescaled,
        _input_rescaled,
        _begin,
        _end,
        &intercept_ );

    // ------------------------------------------------------------------------
    // Reset the loss function, so that it can be used for the next tree.

    loss_function_->reset();

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DecisionTree::from_json_obj(
    const Poco::JSON::Object& _obj,
    const std::shared_ptr<lossfunctions::LossFunction>& _loss_function )
{
    loss_function_ = aggregations::AggregationParser::parse(
        JSON::get_value<std::string>( _obj, "loss_" ), _loss_function );

    input_.reset(
        new containers::Placeholder( *JSON::get_object( _obj, "input_" ) ) );

    intercept_ = JSON::get_value<Float>( _obj, "intercept_" );

    output_.reset(
        new containers::Placeholder( *JSON::get_object( _obj, "output_" ) ) );

    peripheral_used_ = JSON::get_value<size_t>( _obj, "peripheral_used_" );

    update_rate_ = JSON::get_value<Float>( _obj, "update_rate_" );

    input_scaler_ = std::make_shared<utils::StandardScaler>(
        *JSON::get_object( _obj, "input_scaler_" ) );

    output_scaler_ = std::make_shared<utils::StandardScaler>(
        *JSON::get_object( _obj, "output_scaler_" ) );

    initial_loss_reduction_ =
        JSON::get_value<Float>( _obj, "initial_loss_reduction_" );

    initial_weights_ = JSON::array_to_vector<Float>(
        JSON::get_array( _obj, "initial_weights_" ) );

    root_.reset( new DecisionTreeNode(
        utils::ConditionMaker(
            hyperparameters().delta_t_,
            peripheral_used(),
            input_scaler_,
            output_scaler_ ),
        0,  // _depth
        hyperparameters_,
        loss_function_,
        *JSON::get_object( _obj, "root_" ) ) );
}

// ----------------------------------------------------------------------------

Poco::JSON::Object::Ptr DecisionTree::to_json_obj() const
{
    Poco::JSON::Object::Ptr obj( new Poco::JSON::Object );

    assert_true( root_ );

    assert_true( input_ );

    obj->set( "input_", input().to_json_obj() );

    obj->set( "intercept_", intercept_ );

    obj->set( "loss_", loss_function().type() );

    obj->set( "output_", output().to_json_obj() );

    obj->set( "peripheral_used_", peripheral_used_ );

    obj->set( "root_", root_->to_json_obj() );

    obj->set( "update_rate_", update_rate_ );

    obj->set( "input_scaler_", input_scaler().to_json_obj() );

    obj->set( "output_scaler_", output_scaler().to_json_obj() );

    obj->set( "initial_loss_reduction_", initial_loss_reduction_ );

    obj->set( "initial_weights_", JSON::vector_to_array( initial_weights_ ) );

    return obj;
}

// ----------------------------------------------------------------------------

std::string DecisionTree::to_sql(
    const std::vector<strings::String>& _categories,
    const std::string _feature_num,
    const bool _use_timestamps ) const
{
    // -------------------------------------------------------------------

    std::stringstream sql;

    const std::string tab = "    ";

    // -------------------------------------------------------------------

    sql << "CREATE TABLE \"FEATURE_" << _feature_num << "\" AS" << std::endl;

    // -------------------------------------------------------------------
    // First part of SELECT statement

    sql << "SELECT ";

    sql << loss_function().type() << "( " << std::endl;

    // -------------------------------------------------------------------

    std::vector<std::string> conditions;

    assert_true( root_ );

    root_->to_sql( _categories, _feature_num, "", &conditions );

    // -------------------------------------------------------------------

    if ( conditions.size() > 1 )
        {
            sql << tab << "CASE" << std::endl;

            for ( size_t i = 0; i < conditions.size(); ++i )
                {
                    sql << tab << tab << conditions.at( i ) << std::endl;
                }

            sql << tab << tab << "ELSE NULL" << std::endl
                << tab << "END" << std::endl;
        }
    else
        {
            assert_true( conditions.size() == 1 );

            sql << tab << conditions.at( 0 ).substr( 5 ) << std::endl;
        }

    // -------------------------------------------------------------------
    // Second part of SELECT statement

    sql << ") AS \"feature_" << _feature_num << "\"," << std::endl;

    sql << tab << " t1.rowid AS \"rownum\"" << std::endl;

    // -------------------------------------------------------------------
    // JOIN statement

    sql << "FROM \"" << output().name() << "\" t1" << std::endl;

    sql << "LEFT JOIN \"" << input().name() << "\" t2" << std::endl;

    sql << "ON t1.\"" << output().join_keys_name() << "\" = t2.\""
        << input().join_keys_name() << "\"" << std::endl;

    // -------------------------------------------------------------------
    // WHERE statement

    if ( _use_timestamps && input().num_time_stamps() > 0 &&
         output().num_time_stamps() > 0 )
        {
            sql << "WHERE ";

            sql << "datetime( t2.\"" << input().time_stamps_name()
                << "\" ) <= datetime( t1.\"" << output().time_stamps_name()
                << "\" )" << std::endl;

            if ( input().num_time_stamps() > 1 )
                {
                    sql << "AND ( datetime( t2.\""
                        << input().upper_time_stamps_name()
                        << "\" ) > datetime( t1.\""
                        << output().time_stamps_name()
                        << "\" ) OR datetime( t2.\""
                        << input().upper_time_stamps_name() << "\" ) IS NULL )"
                        << std::endl;
                }
        }

    // -------------------------------------------------------------------
    // GROUP BY statement

    sql << "GROUP BY t1.rowid"
        << ";" << std::endl
        << std::endl
        << std::endl;

    // -------------------------------------------------------------------

    return sql.str();

    // -------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::vector<Float> DecisionTree::transform(
    const containers::DataFrameView& _output,
    const std::optional<containers::DataFrame>& _input,
    const containers::Subfeatures& _subfeatures ) const
{
    // ------------------------------------------------------------------------

    assert_true( root_ );

    assert_true( _input );

    // ------------------------------------------------------------------------

    auto predictions = std::vector<Float>( _output.nrows() );

    const auto output_map = std::make_shared<containers::Rescaled::MapType>(
        _output.nrows(), _output.nrows() );

    const auto input_map = std::make_shared<containers::Rescaled::MapType>(
        _input->nrows(), _input->nrows() );

    // ------------------------------------------------------------------------

    for ( size_t ix_output = 0; ix_output < _output.nrows(); ++ix_output )
        {
            // ------------------------------------------------------------------------

            std::vector<containers::Match> matches;

            assert_true( _input );

            utils::Matchmaker::make_matches(
                _output,
                *_input,
                hyperparameters_->use_timestamps_,
                ix_output,
                &matches );

            // ------------------------------------------------------------------------

            const auto output_rescaled = output_scaler().transform(
                _output,
                std::nullopt,
                output_map,
                matches.begin(),
                matches.end() );

            const auto input_rescaled = input_scaler().transform(
                *_input,
                _subfeatures,
                input_map,
                matches.begin(),
                matches.end() );

            // ------------------------------------------------------------------------

            std::vector<Float> weights( matches.size() );

            for ( size_t i = 0; i < matches.size(); ++i )
                {
                    weights[i] = root_->transform(
                        _output,
                        _input,
                        _subfeatures,
                        output_rescaled,
                        input_rescaled,
                        matches[i] );
                }

            // ------------------------------------------------------------------------

            predictions[ix_output] = loss_function_->transform( weights );

            // ------------------------------------------------------------------------
        }

    // ------------------------------------------------------------------------

    return predictions;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace decisiontrees
}  // namespace relcit
