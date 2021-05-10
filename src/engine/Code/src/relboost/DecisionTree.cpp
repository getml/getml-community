#include "relboost/decisiontrees/decisiontrees.hpp"

namespace relboost
{
namespace decisiontrees
{
// ----------------------------------------------------------------------------

DecisionTree::DecisionTree(
    const std::shared_ptr<const Hyperparameters>& _hyperparameters,
    const std::shared_ptr<lossfunctions::LossFunction>& _loss_function,
    const size_t _peripheral_used,
    multithreading::Communicator* _comm )
    : comm_( _comm ),
      hyperparameters_( _hyperparameters ),
      intercept_( 0.0 ),
      loss_function_( _loss_function ),
      peripheral_used_( _peripheral_used ),
      update_rate_( 0.0 )
{
}

// ----------------------------------------------------------------------------

DecisionTree::DecisionTree(
    const std::shared_ptr<const Hyperparameters>& _hyperparameters,
    const std::shared_ptr<lossfunctions::LossFunction>& _loss_function,
    const Poco::JSON::Object& _obj )
    : comm_( nullptr ), hyperparameters_( _hyperparameters )
{
    if ( _obj.has( "input_" ) )
        {
            loss_function_ = aggregations::AggregationParser::parse(
                JSON::get_value<std::string>( _obj, "loss_" ), _loss_function );
        }
    else
        {
            loss_function_ = _loss_function;
        }

    if ( _obj.has( "input_" ) )
        {
            input_.reset( new containers::Placeholder(
                *JSON::get_object( _obj, "input_" ) ) );
        }

    intercept_ = JSON::get_value<Float>( _obj, "intercept_" );

    output_.reset(
        new containers::Placeholder( *JSON::get_object( _obj, "output_" ) ) );

    peripheral_used_ = JSON::get_value<size_t>( _obj, "peripheral_used_" );

    update_rate_ = JSON::get_value<Float>( _obj, "update_rate_" );

    root_.reset( new DecisionTreeNode(
        utils::ConditionMaker( hyperparameters().delta_t_, peripheral_used() ),
        0,  // _depth
        hyperparameters_,
        loss_function_,
        *JSON::get_object( _obj, "root_" ) ) );
}

// ----------------------------------------------------------------------------

void DecisionTree::fit(
    const containers::DataFrameView& _output,
    const std::optional<containers::DataFrame>& _input,
    const containers::Subfeatures& _subfeatures,
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
    // Set up and fit root node

    debug_log( "Set up and fit root node." );

    root_.reset( new DecisionTreeNode(
        utils::ConditionMaker( hyperparameters().delta_t_, peripheral_used() ),
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

    if ( input_ )
        {
            obj->set( "input_", input().to_json_obj() );
        }

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
    const std::vector<strings::String>& _categories,
    const helpers::VocabularyTree& _vocabulary,
    const std::string& _feature_prefix,
    const std::string& _feature_num,
    const bool _use_timestamps,
    const std::tuple<bool, bool, bool> _has_subfeatures ) const
{
    // -------------------------------------------------------------------

    std::stringstream sql;

    const std::string tab = "    ";

    // -------------------------------------------------------------------

    sql << "DROP TABLE IF EXISTS \"FEATURE_" << _feature_prefix << _feature_num
        << "\";" << std::endl
        << std::endl;

    // -------------------------------------------------------------------

    sql << "CREATE TABLE \"FEATURE_" << _feature_prefix << _feature_num
        << "\" AS" << std::endl;

    // -------------------------------------------------------------------

    sql << "SELECT ";

    sql << loss_function().type() << "( " << std::endl;

    sql << tab << "CASE" << std::endl;

    // -------------------------------------------------------------------

    std::vector<std::string> conditions;

    assert_true( root_ );

    assert_true( peripheral_used() < _vocabulary.peripheral().size() );

    root_->to_sql(
        _categories,
        _vocabulary.population(),
        _vocabulary.peripheral().at( peripheral_used() ),
        _feature_prefix,
        _feature_num,
        "",
        &conditions );

    for ( size_t i = 0; i < conditions.size(); ++i )
        {
            sql << tab << tab << conditions.at( i ) << std::endl;
        }

    sql << tab << tab << "ELSE NULL" << std::endl;

    // -------------------------------------------------------------------

    sql << tab << "END" << std::endl
        << ") AS \"feature_" << _feature_prefix << _feature_num << "\","
        << std::endl;

    sql << tab << " t1.rowid AS \"rownum\"" << std::endl;

    // -------------------------------------------------------------------

    sql << helpers::SQLGenerator::make_joins(
        output().name(),
        input().name(),
        output().join_keys_name(),
        input().join_keys_name() );

    // -------------------------------------------------------------------

    const auto [has_normal_subfeatures, output_has_prop, input_has_prop] =
        _has_subfeatures;

    if ( has_normal_subfeatures )
        {
            sql << helpers::SQLGenerator::make_subfeature_joins(
                _feature_prefix, peripheral_used_ );
        }

    if ( output_has_prop )
        {
            sql << helpers::SQLGenerator::make_subfeature_joins(
                _feature_prefix,
                peripheral_used_,
                "t1",
                "_PROPOSITIONALIZATION" );
        }

    if ( input_has_prop )
        {
            sql << helpers::SQLGenerator::make_subfeature_joins(
                _feature_prefix,
                peripheral_used_,
                "t2",
                "_PROPOSITIONALIZATION" );
        }

    // -------------------------------------------------------------------

    if ( _use_timestamps && input().num_time_stamps() > 0 &&
         output().num_time_stamps() > 0 )
        {
            sql << "WHERE ";

            const auto upper_ts = input().num_time_stamps() > 1
                                      ? input().upper_time_stamps_name()
                                      : std::string( "" );

            sql << helpers::SQLGenerator::make_time_stamps(
                output().time_stamps_name(),
                input().time_stamps_name(),
                upper_ts,
                "t1",
                "t2",
                "t1" );
        }

    // -------------------------------------------------------------------

    sql << "GROUP BY t1.rowid"
        << ";" << std::endl
        << std::endl
        << std::endl;

    // -------------------------------------------------------------------

    return sql.str();

    // -------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::shared_ptr<std::vector<Float>> DecisionTree::transform(
    const containers::DataFrameView& _output,
    const containers::DataFrame& _input,
    const containers::Subfeatures& _subfeatures ) const
{
    // ------------------------------------------------------------------------

    assert_true( root_ );

    // ------------------------------------------------------------------------

    const auto predictions =
        std::make_shared<std::vector<Float>>( _output.nrows() );

    // ------------------------------------------------------------------------

    for ( size_t ix_output = 0; ix_output < _output.nrows(); ++ix_output )
        {
            // ------------------------------------------------------------------------

            std::vector<containers::Match> matches;

            utils::Matchmaker::make_matches(
                _output,
                _input,
                hyperparameters_->use_timestamps_,
                ix_output,
                &matches );

            // ------------------------------------------------------------------------

            std::vector<Float> weights( matches.size() );

            for ( size_t i = 0; i < matches.size(); ++i )
                {
                    weights[i] = root_->transform(
                        _output, _input, _subfeatures, matches[i] );
                }

            // ------------------------------------------------------------------------

            ( *predictions )[ix_output] = loss_function_->transform( weights );

            // ------------------------------------------------------------------------
        }

    // ------------------------------------------------------------------------

    return predictions;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace decisiontrees
}  // namespace relboost
