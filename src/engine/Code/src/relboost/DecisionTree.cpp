#include "relboost/decisiontrees/decisiontrees.hpp"

namespace relboost
{
namespace decisiontrees
{
// ----------------------------------------------------------------------------

DecisionTree::DecisionTree(
    const std::shared_ptr<const std::vector<std::string>>& _encoding,
    const std::shared_ptr<const Hyperparameters>& _hyperparameters,
    const std::shared_ptr<lossfunctions::LossFunction>& _loss_function,
    const size_t _peripheral_used,
    multithreading::Communicator* _comm )
    : comm_( _comm ),
      encoding_( _encoding ),
      hyperparameters_( _hyperparameters ),
      intercept_( 0.0 ),
      loss_function_( _loss_function ),
      peripheral_used_( _peripheral_used ),
      update_rate_( 0.0 )
{
}

// ----------------------------------------------------------------------------

DecisionTree::DecisionTree(
    const std::shared_ptr<const std::vector<std::string>>& _encoding,
    const std::shared_ptr<const Hyperparameters>& _hyperparameters,
    const std::shared_ptr<lossfunctions::LossFunction>& _loss_function,
    const Poco::JSON::Object& _obj )
    : comm_( nullptr ),
      encoding_( _encoding ),
      hyperparameters_( _hyperparameters ),
      loss_function_( aggregations::AggregationParser::parse(
          JSON::get_value<std::string>( _obj, "loss_" ), _loss_function ) )
{
    input_.reset(
        new containers::Schema( *JSON::get_object( _obj, "input_" ) ) );

    intercept_ = JSON::get_value<Float>( _obj, "intercept_" );

    output_.reset(
        new containers::Schema( *JSON::get_object( _obj, "output_" ) ) );

    peripheral_used_ = JSON::get_value<size_t>( _obj, "peripheral_used_" );

    update_rate_ = JSON::get_value<Float>( _obj, "update_rate_" );

    root_.reset( new DecisionTreeNode(
        utils::ConditionMaker(
            encoding_, hyperparameters().delta_t_, peripheral_used() ),
        0,  // _depth
        hyperparameters_,
        loss_function_,
        *JSON::get_object( _obj, "root_" ) ) );
}

// ----------------------------------------------------------------------------

void DecisionTree::fit(
    const containers::DataFrameView& _output,
    const containers::DataFrame& _input,
    const containers::Subfeatures& _subfeatures,
    const std::vector<const containers::Match*>::iterator _begin,
    const std::vector<const containers::Match*>::iterator _end )
{
    // ------------------------------------------------------------------------
    // Store input and output (we need the column names).

    input_.reset( new containers::Schema( _input.to_schema() ) );

    output_.reset( new containers::Schema( _output.df().to_schema() ) );

    // ------------------------------------------------------------------------
    // Set up and fit root node

    debug_log( "Set up and fit root node." );

    assert_true( encoding_ );

    root_.reset( new DecisionTreeNode(
        utils::ConditionMaker(
            encoding_, hyperparameters().delta_t_, peripheral_used() ),
        0,
        hyperparameters_,
        loss_function_,
        0.0,
        &comm() ) );

    root_->fit( _output, _input, _subfeatures, _begin, _end, &intercept_ );

    // ------------------------------------------------------------------------
    // Reset the loss function, so that it can be used for the next tree.

    loss_function_->reset();

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

Poco::JSON::Object::Ptr DecisionTree::to_json_obj() const
{
    Poco::JSON::Object::Ptr obj( new Poco::JSON::Object );

    assert_true( root_ );

    obj->set( "input_", input().to_json_obj() );

    obj->set( "intercept_", intercept_ );

    obj->set( "loss_", loss_function().type() );

    obj->set( "output_", output().to_json_obj() );

    obj->set( "peripheral_used_", peripheral_used_ );

    obj->set( "root_", root_->to_json_obj() );

    obj->set( "update_rate_", update_rate_ );

    return obj;
}

// ----------------------------------------------------------------------------

std::string DecisionTree::to_sql(
    const std::string _feature_num, const bool _use_timestamps ) const
{
    // -------------------------------------------------------------------

    std::stringstream sql;

    // -------------------------------------------------------------------

    sql << "CREATE TABLE FEATURE_" << _feature_num << " AS" << std::endl;

    // -------------------------------------------------------------------
    // First part of SELECT statement

    sql << "SELECT " << intercept() << " + ";

    sql << loss_function().type() << "( " << std::endl;

    sql << "CASE" << std::endl;

    // -------------------------------------------------------------------
    // Conditions for the feature

    std::vector<std::string> conditions;

    assert_true( root_ );

    root_->to_sql( _feature_num, "", &conditions );

    for ( size_t i = 0; i < conditions.size(); ++i )
        {
            sql << "     " << conditions[i] << std::endl;
        }

    sql << "     ELSE NULL" << std::endl;

    // -------------------------------------------------------------------
    // Second part of SELECT statement

    sql << "END" << std::endl
        << ") AS feature_" << _feature_num << "," << std::endl;

    sql << "     t1." << output().join_keys_name() << "," << std::endl;

    sql << "     t1." << output().time_stamps_name() << std::endl;

    // -------------------------------------------------------------------
    // JOIN statement

    sql << "FROM (" << std::endl;

    sql << "     SELECT *," << std::endl;

    sql << "            ROW_NUMBER() OVER ( ORDER BY "
        << output().join_keys_name() << ", " << output().time_stamps_name()
        << " ASC ) AS rownum" << std::endl;

    sql << "     FROM " << output().name() << std::endl;

    sql << ") t1" << std::endl;

    sql << "LEFT JOIN " << input().name() << " t2" << std::endl;

    sql << "ON t1." << output().join_keys_name() << " = t2."
        << input().join_keys_name() << std::endl;

    // -------------------------------------------------------------------
    // WHERE statement

    if ( _use_timestamps )
        {
            sql << "WHERE ";

            sql << "t2." << input().time_stamps_name() << " <= t1."
                << output().time_stamps_name() << std::endl;

            if ( input().num_time_stamps() > 1 )
                {
                    sql << "AND ( t2." << input().upper_time_stamps_name()
                        << " > t1." << output().time_stamps_name() << " OR t2."
                        << input().upper_time_stamps_name() << " IS NULL )"
                        << std::endl;
                }
        }

    // -------------------------------------------------------------------
    // GROUP BY statement

    sql << "GROUP BY t1.rownum," << std::endl;

    sql << "         t1." << output().join_keys_name() << "," << std::endl;

    sql << "         t1." << output().time_stamps_name() << ";" << std::endl
        << std::endl
        << std::endl;

    // -------------------------------------------------------------------

    return sql.str();

    // -------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::vector<Float> DecisionTree::transform(
    const containers::DataFrameView& _output,
    const containers::DataFrame& _input,
    const containers::Subfeatures& _subfeatures ) const
{
    // ------------------------------------------------------------------------

    assert_true( root_ );

    // ------------------------------------------------------------------------

    auto predictions = std::vector<Float>( _output.nrows() );

    // ------------------------------------------------------------------------

    for ( size_t ix_output = 0; ix_output < _output.nrows(); ++ix_output )
        {
            // ------------------------------------------------------------------------
            // Build matches and pointers

            std::vector<containers::Match> matches;

            utils::Matchmaker::make_matches(
                _output,
                _input,
                hyperparameters_->use_timestamps_,
                ix_output,
                &matches );

            // ------------------------------------------------------------------------
            // Calculate weights for each match.

            std::vector<Float> weights( matches.size() );

            for ( size_t i = 0; i < matches.size(); ++i )
                {
                    weights[i] = root_->transform(
                        _output, _input, _subfeatures, matches[i] );
                }

            // ------------------------------------------------------------------------
            // Aggregate weights to predictions.

            predictions[ix_output] = loss_function_->transform( weights );

            // ------------------------------------------------------------------------
        }

    // ------------------------------------------------------------------------
    // Add intercept.

    for ( size_t ix_output = 0; ix_output < predictions.size(); ++ix_output )
        {
            predictions[ix_output] += intercept();
        }

    // ------------------------------------------------------------------------

    return predictions;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace decisiontrees
}  // namespace relboost
