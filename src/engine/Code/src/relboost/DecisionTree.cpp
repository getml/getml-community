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
    const size_t _peripheral_used )
    : encoding_( _encoding ),
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
    : encoding_( _encoding ),
      hyperparameters_( _hyperparameters ),
      loss_function_( aggregations::AggregationParser::parse(
          JSON::get_value<std::string>( _obj, "loss_" ), _loss_function ) )
{
    intercept_ = JSON::get_value<RELBOOST_FLOAT>( _obj, "intercept_" );

    peripheral_used_ = JSON::get_value<size_t>( _obj, "peripheral_used_" );

    update_rate_ = JSON::get_value<RELBOOST_FLOAT>( _obj, "update_rate_" );

    root_.reset( new DecisionTreeNode(
        utils::ConditionMaker( encoding_ ),
        0,  // _depth
        hyperparameters_,
        loss_function_,
        *JSON::get_object( _obj, "root_" ) ) );
}

// ----------------------------------------------------------------------------

void DecisionTree::fit(
    const containers::DataFrame& _output,
    const containers::DataFrame& _input,
    const std::vector<const containers::Match*>::iterator _begin,
    const std::vector<const containers::Match*>::iterator _end )
{
    // ------------------------------------------------------------------------
    // Store input and output (we need the column names).

    input_.reset( new containers::DataFrame( _input ) );

    output_.reset( new containers::DataFrame( _output ) );

    // ------------------------------------------------------------------------
    // Set up and fit root node

    debug_log( "Set up and fit root node." );

    assert( encoding_ );

    root_.reset( new DecisionTreeNode(
        utils::ConditionMaker( encoding_ ),
        0,
        hyperparameters_,
        loss_function_,
        0.0 ) );

    root_->fit( _output, _input, _begin, _end, &intercept_ );

    // ------------------------------------------------------------------------
    // Reset the loss function, so that it can be used for the next tree.

    loss_function_->reset();

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

Poco::JSON::Object DecisionTree::to_json() const
{
    Poco::JSON::Object obj;

    assert( root_ );

    obj.set( "intercept_", intercept_ );

    obj.set( "loss_", loss_function().type() );

    obj.set( "peripheral_used_", peripheral_used_ );

    obj.set( "root_", root_->to_json() );

    obj.set( "update_rate_", update_rate_ );

    return obj;
}

// ----------------------------------------------------------------------------

std::string DecisionTree::to_sql(
    const std::string _feature_num, const bool _use_timestamps ) const
{
    // -------------------------------------------------------------------

    std::stringstream sql;

    // -------------------------------------------------------------------

    /*for ( size_t i = 0; i < subtrees().size(); ++i )
        {
            sql << subtrees()[i].to_sql(
                _feature_num + "_" + std::to_string( i + 1 ), _use_timestamps );
        }*/

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

    assert( root_ );

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

    sql << "     t1." << output().join_keys_[0].colnames_[0] << ","
        << std::endl;

    sql << "     t1." << output().time_stamps_[0].colnames_[0] << std::endl;

    // -------------------------------------------------------------------
    // JOIN statement

    sql << "FROM (" << std::endl;

    sql << "     SELECT *," << std::endl;

    sql << "            ROW_NUMBER() OVER ( ORDER BY "
        << output().join_keys_[0].colnames_[0] << ", "
        << output().time_stamps_[0].colnames_[0] << " ASC ) AS rownum"
        << std::endl;

    sql << "     FROM " << output().name_ << std::endl;

    sql << ") t1" << std::endl;

    sql << "LEFT JOIN " << input().name_ << " t2" << std::endl;

    sql << "ON t1." << output().join_keys_[0].colnames_[0] << " = t2."
        << input().join_keys_[0].colnames_[0] << std::endl;

    // -------------------------------------------------------------------
    // WHERE statement

    if ( _use_timestamps )
        {
            sql << "WHERE ";

            sql << "t2." << input().time_stamps_[0].colnames_[0] << " <= t1."
                << output().time_stamps_[0].colnames_[0] << std::endl;

            if ( input().time_stamps_.size() > 1 )
                {
                    sql << "AND ( t2." << input().time_stamps_[1].colnames_[0]
                        << " > t1." << output().time_stamps_[0].colnames_[0]
                        << " OR t2." << input().time_stamps_[1].colnames_[0]
                        << " IS NULL )" << std::endl;
                }
        }

    // -------------------------------------------------------------------
    // GROUP BY statement

    sql << "GROUP BY t1.rownum," << std::endl;

    sql << "         t1." << output().join_keys_[0].colnames_[0] << ","
        << std::endl;

    sql << "         t1." << output().time_stamps_[0].colnames_[0] << ";"
        << std::endl
        << std::endl
        << std::endl;

    // -------------------------------------------------------------------

    return sql.str();

    // -------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::vector<RELBOOST_FLOAT> DecisionTree::transform(
    const containers::DataFrame& _output,
    const containers::DataFrame& _input ) const
{
    // ------------------------------------------------------------------------

    assert( root_ );

    // ------------------------------------------------------------------------

    auto predictions = std::vector<RELBOOST_FLOAT>( _output.nrows() );

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

            std::vector<RELBOOST_FLOAT> weights( matches.size() );

            for ( size_t i = 0; i < matches.size(); ++i )
                {
                    weights[i] =
                        root_->transform( _output, _input, matches[i] );
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
