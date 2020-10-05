#include "relcit/decisiontrees/decisiontrees.hpp"

namespace relcit
{
namespace decisiontrees
{
// ----------------------------------------------------------------------------

DecisionTreeNode::DecisionTreeNode(
    const utils::ConditionMaker& _condition_maker,
    const Int _depth,
    const std::shared_ptr<const Hyperparameters>& _hyperparameters,
    const std::shared_ptr<lossfunctions::LossFunction>& _loss_function,
    const std::vector<Float> _weights,
    multithreading::Communicator* _comm )
    : comm_( _comm ),
      condition_maker_( _condition_maker ),
      depth_( _depth ),
      hyperparameters_( _hyperparameters ),
      loss_function_( _loss_function ),
      loss_reduction_( NAN ),
      weights_( _weights )
{
}

// ----------------------------------------------------------------------------

DecisionTreeNode::DecisionTreeNode(
    const utils::ConditionMaker& _condition_maker,
    const Int _depth,
    const std::shared_ptr<const Hyperparameters>& _hyperparameters,
    const std::shared_ptr<lossfunctions::LossFunction>& _loss_function,
    const Poco::JSON::Object& _obj )
    : comm_( nullptr ),
      condition_maker_( _condition_maker ),
      depth_( _depth ),
      hyperparameters_( _hyperparameters ),
      loss_function_( _loss_function ),
      loss_reduction_( NAN ),
      weights_(
          JSON::array_to_vector<Float>( JSON::get_array( _obj, "weights_" ) ) )
{
    if ( _obj.has( "input_" ) )
        {
            input_.reset( new containers::Placeholder(
                *JSON::get_object( _obj, "input_" ) ) );
        }

    output_.reset(
        new containers::Placeholder( *JSON::get_object( _obj, "output_" ) ) );

    if ( _obj.has( "child_greater_" ) )
        {
            const auto categories_used =
                std::make_shared<const std::vector<Int>>(
                    JSON::array_to_vector<Int>(
                        JSON::get_array( _obj, "categories_used_" ) ) );

            const auto column = JSON::get_value<size_t>( _obj, "column_" );

            const auto column_input =
                JSON::get_value<size_t>( _obj, "column_input_" );

            const auto critical_value =
                JSON::get_value<Float>( _obj, "critical_value_" );

            const auto data_used = JSON::destringify(
                JSON::get_value<std::string>( _obj, "data_used_" ) );

            // For backwards compatatability.
            if ( _obj.has( "loss_reduction_" ) )
                {
                    loss_reduction_ =
                        JSON::get_value<Float>( _obj, "loss_reduction_" );
                }

            split_ = containers::Split(
                categories_used,
                column,
                column_input,
                critical_value,
                data_used );

            child_greater_.reset( new DecisionTreeNode(
                condition_maker_,
                depth_ + 1,
                hyperparameters_,
                loss_function_,
                *JSON::get_object( _obj, "child_greater_" ) ) );

            child_smaller_.reset( new DecisionTreeNode(
                condition_maker_,
                depth_ + 1,
                hyperparameters_,
                loss_function_,
                *JSON::get_object( _obj, "child_smaller_" ) ) );
        }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::add_candidates(
    const enums::Revert _revert,
    const enums::Update _update,
    const Float _old_intercept,
    const containers::Split& _split,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _last_it,
    const std::vector<containers::Match>::iterator _it,
    const std::vector<containers::Match>::iterator _end,
    std::vector<containers::CandidateSplit>* _candidates )
{
    auto [loss_reduction, new_weights] = loss_function().calc_pair(
        _revert,
        _update,
        hyperparameters().min_num_samples_,
        _old_intercept,
        weights_,
        _begin,
        _last_it,
        _it,
        _end );

    _candidates->push_back( containers::CandidateSplit(
        loss_reduction, _split, std::move( new_weights ) ) );

    if ( _revert == enums::Revert::True )
        {
            loss_function().revert( weights_ );
        }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::assert_aligned(
    const std::vector<containers::CandidateSplit>::iterator _begin,
    const std::vector<containers::CandidateSplit>::iterator _end,
    const std::vector<containers::CandidateSplit>::iterator _it )
{
#ifndef NDEBUG
    const auto num_candidates = std::distance( _begin, _end );
    const auto ix_best = std::distance( _begin, _it );
    const auto partial_loss = _it->partial_loss_;

    auto global_num_candidates = num_candidates;
    auto global_ix_best = ix_best;
    auto global_partial_loss = partial_loss;

    utils::Reducer::reduce(
        multithreading::maximum<decltype( global_num_candidates )>(),
        &global_num_candidates,
        &comm() );

    utils::Reducer::reduce(
        multithreading::maximum<decltype( global_ix_best )>(),
        &global_ix_best,
        &comm() );

    utils::Reducer::reduce(
        multithreading::maximum<decltype( global_partial_loss )>(),
        &global_partial_loss,
        &comm() );

    assert_true( global_num_candidates == num_candidates );
    assert_true( global_ix_best == ix_best );
    assert_true( global_partial_loss == partial_loss );

#endif  /// NDEBUG
}

// ----------------------------------------------------------------------------

Float DecisionTreeNode::calc_prediction(
    const containers::Rescaled& _output_rescaled,
    const containers::Rescaled& _input_rescaled,
    const containers::Match& _match ) const
{
    assert_true( weights_.size() > 0 );
    assert_true(
        weights_.size() ==
        _output_rescaled.ncols() + _input_rescaled.ncols() + 1 );

    Float p = weights_[0];

    size_t i = 1;

    const auto input_row = _input_rescaled.row( _match.ix_input );

    for ( size_t j = 0; j < _input_rescaled.ncols(); ++i, ++j )
        {
            p += input_row[j] * weights_[i];
        }

    const auto output_row = _output_rescaled.row( _match.ix_output );

    for ( size_t j = 0; j < _output_rescaled.ncols(); ++i, ++j )
        {
            p += output_row[j] * weights_[i];
        }

    return p;
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::column_importances(
    utils::ImportanceMaker* _importance_maker ) const
{
    if ( !std::isnan( loss_reduction_ ) )
        {
            assert_true( input_ );
            assert_true( output_ );

            _importance_maker->add(
                *input_,
                *output_,
                split_.data_used_,
                split_.column_,
                split_.column_input_,
                loss_reduction_ );

            assert_true( child_greater_ );
            assert_true( child_smaller_ );

            child_greater_->column_importances( _importance_maker );

            child_smaller_->column_importances( _importance_maker );
        }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::fit(
    const containers::DataFrameView& _output,
    const std::optional<containers::DataFrame>& _input,
    const containers::Subfeatures& _subfeatures,
    const containers::Rescaled& _output_rescaled,
    const containers::Rescaled& _input_rescaled,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _end,
    Float* _intercept )
{
    // ------------------------------------------------------------------------
    // Store input and output (we need the column names).

    assert_true( _input );

    input_.reset( new containers::Placeholder( _input->to_schema() ) );

    output_.reset( new containers::Placeholder( _output.df().to_schema() ) );

    // ------------------------------------------------------------------------

    if ( hyperparameters().max_depth_ >= 0 &&
         depth_ == hyperparameters().max_depth_ )
        {
            debug_log( "Max depth reached." );
            return;
        }

    // ------------------------------------------------------------------------

    auto candidates = try_all(
        *_intercept,
        _output,
        *_input,
        _subfeatures,
        _output_rescaled,
        _input_rescaled,
        _begin,
        _end );

    debug_log( "candidates.size(): " + std::to_string( candidates.size() ) );

    if ( candidates.size() == 0 )
        {
            debug_log( "No candidates." );
            return;
        }

    // ------------------------------------------------------------------------

    const auto cmp = []( const containers::CandidateSplit& c1,
                         const containers::CandidateSplit& c2 ) {
        return c1.partial_loss_ < c2.partial_loss_;
    };

    const auto it =
        std::min_element( candidates.begin(), candidates.end(), cmp );

    // DEBUG ONLY: Makes sure that the candidates and min element are aligned
    // over all threads.
    assert_aligned( candidates.begin(), candidates.end(), it );

    const auto best_split = *it;

    candidates = std::vector<containers::CandidateSplit>();

    // ------------------------------------------------------------------------

    const auto it_split = partition(
        _output, _input, _subfeatures, best_split.split_, _begin, _end );

    const auto loss_reduction = loss_function().evaluate_split(
        *_intercept, weights_, best_split.weights_, _begin, it_split, _end );

    // ------------------------------------------------------------------------

    debug_log( "loss_reduction: " + std::to_string( loss_reduction ) );

    if ( loss_reduction < hyperparameters().gamma_ )
        {
            loss_function().revert_to_commit();
            return;
        }

    split_ = best_split.split_.deep_copy();

    loss_reduction_ = loss_reduction;

    loss_function().commit( *_intercept, weights_, best_split.weights_ );

    *_intercept = std::get<0>( best_split.weights_ );

    // ------------------------------------------------------------------------

    child_greater_.reset( new DecisionTreeNode(
        condition_maker_,
        depth_ + 1,
        hyperparameters_,
        loss_function_,
        std::get<1>( best_split.weights_ ),
        &comm() ) );

    child_smaller_.reset( new DecisionTreeNode(
        condition_maker_,
        depth_ + 1,
        hyperparameters_,
        loss_function_,
        std::get<2>( best_split.weights_ ),
        &comm() ) );

    child_greater_->fit(
        _output,
        _input,
        _subfeatures,
        _output_rescaled,
        _input_rescaled,
        _begin,
        it_split,
        _intercept );

    child_smaller_->fit(
        _output,
        _input,
        _subfeatures,
        _output_rescaled,
        _input_rescaled,
        it_split,
        _end,
        _intercept );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

bool DecisionTreeNode::match_is_greater(
    const containers::DataFrameView& _output,
    const std::optional<containers::DataFrame>& _input,
    const containers::Subfeatures& _subfeatures,
    const containers::Match& _match ) const
{
    // ------------------------------------------------------------------------

    assert_true( child_smaller_ );

    switch ( split_.data_used_ )
        {
            case enums::DataUsed::categorical_input:
                assert_true( _input );
                return utils::Partitioner<enums::DataUsed::categorical_input>::
                    is_greater( split_, *_input, _match );

            case enums::DataUsed::categorical_output:
                return utils::Partitioner<enums::DataUsed::categorical_output>::
                    is_greater( split_, _output, _match );

            case enums::DataUsed::discrete_input:
                assert_true( _input );
                return utils::Partitioner<enums::DataUsed::discrete_input>::
                    is_greater( split_, *_input, _match );

            case enums::DataUsed::discrete_input_is_nan:
                assert_true( _input );
                return utils::Partitioner<
                    enums::DataUsed::discrete_input_is_nan>::
                    is_greater( split_.column_, *_input, _match );

            case enums::DataUsed::discrete_output:
                return utils::Partitioner<enums::DataUsed::discrete_output>::
                    is_greater( split_, _output, _match );

            case enums::DataUsed::discrete_output_is_nan:
                return utils::Partitioner<
                    enums::DataUsed::discrete_output_is_nan>::
                    is_greater( split_.column_, _output, _match );

            case enums::DataUsed::numerical_input:
                assert_true( _input );
                return utils::Partitioner<enums::DataUsed::numerical_input>::
                    is_greater( split_, *_input, _match );

            case enums::DataUsed::numerical_input_is_nan:
                assert_true( _input );
                return utils::Partitioner<
                    enums::DataUsed::numerical_input_is_nan>::
                    is_greater( split_.column_, *_input, _match );

            case enums::DataUsed::numerical_output:
                return utils::Partitioner<enums::DataUsed::numerical_output>::
                    is_greater( split_, _output, _match );

            case enums::DataUsed::numerical_output_is_nan:
                return utils::Partitioner<
                    enums::DataUsed::numerical_output_is_nan>::
                    is_greater( split_.column_, _output, _match );

            case enums::DataUsed::same_units_categorical:
                assert_true( _input );
                return utils::Partitioner<
                    enums::DataUsed::same_units_categorical>::
                    is_greater( split_, *_input, _output, _match );

            case enums::DataUsed::same_units_discrete_ts:
            case enums::DataUsed::same_units_discrete:
                assert_true( _input );
                return utils::Partitioner<
                    enums::DataUsed::same_units_discrete>::
                    is_greater( split_, *_input, _output, _match );

            case enums::DataUsed::same_units_discrete_is_nan:
                assert_true( _input );
                return utils::Partitioner<
                    enums::DataUsed::same_units_discrete_is_nan>::
                    is_greater(
                        split_.column_input_,
                        split_.column_,
                        *_input,
                        _output,
                        _match );

            case enums::DataUsed::same_units_numerical_ts:
            case enums::DataUsed::same_units_numerical:
                assert_true( _input );
                return utils::Partitioner<
                    enums::DataUsed::same_units_numerical>::
                    is_greater( split_, *_input, _output, _match );

            case enums::DataUsed::same_units_numerical_is_nan:
                assert_true( _input );
                return utils::Partitioner<
                    enums::DataUsed::same_units_numerical_is_nan>::
                    is_greater(
                        split_.column_input_,
                        split_.column_,
                        *_input,
                        _output,
                        _match );

            case enums::DataUsed::subfeatures:
                assert_true( _input );
                return utils::Partitioner<enums::DataUsed::subfeatures>::
                    is_greater( split_, _subfeatures, _match );

            case enums::DataUsed::time_stamps_diff:
                assert_true( _input );
                return utils::Partitioner<enums::DataUsed::time_stamps_diff>::
                    is_greater( split_, *_input, _output, _match );

            case enums::DataUsed::time_stamps_window:
                assert_true( _input );
                return utils::Partitioner<enums::DataUsed::time_stamps_window>::
                    is_greater(
                        split_,
                        hyperparameters().delta_t_,
                        *_input,
                        _output,
                        _match );

            default:
                assert_true( false && "Unknown data_used_" );
                return false;
        }
}

// ----------------------------------------------------------------------------

std::vector<containers::Match>::iterator DecisionTreeNode::partition(
    const containers::DataFrameView& _output,
    const std::optional<containers::DataFrame>& _input,
    const containers::Subfeatures& _subfeatures,
    const containers::Split& _split,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _end ) const
{
    switch ( _split.data_used_ )
        {
            case enums::DataUsed::categorical_input:
                assert_true( _input );
                return utils::Partitioner<enums::DataUsed::categorical_input>::
                    partition( _split, *_input, _begin, _end );

            case enums::DataUsed::categorical_output:
                return utils::Partitioner<enums::DataUsed::categorical_output>::
                    partition( _split, _output, _begin, _end );

            case enums::DataUsed::discrete_input:
                assert_true( _input );
                return utils::Partitioner<enums::DataUsed::discrete_input>::
                    partition( _split, *_input, _begin, _end );

            case enums::DataUsed::discrete_input_is_nan:
                assert_true( _input );
                return utils::Partitioner<
                    enums::DataUsed::discrete_input_is_nan>::
                    partition( _split.column_, *_input, _begin, _end );

            case enums::DataUsed::discrete_output:
                return utils::Partitioner<enums::DataUsed::discrete_output>::
                    partition( _split, _output, _begin, _end );

            case enums::DataUsed::discrete_output_is_nan:
                return utils::Partitioner<
                    enums::DataUsed::discrete_output_is_nan>::
                    partition( _split.column_, _output, _begin, _end );

            case enums::DataUsed::numerical_input:
                assert_true( _input );
                return utils::Partitioner<enums::DataUsed::numerical_input>::
                    partition( _split, *_input, _begin, _end );

            case enums::DataUsed::numerical_input_is_nan:
                assert_true( _input );
                return utils::Partitioner<
                    enums::DataUsed::numerical_input_is_nan>::
                    partition( _split.column_, *_input, _begin, _end );

            case enums::DataUsed::numerical_output:
                return utils::Partitioner<enums::DataUsed::numerical_output>::
                    partition( _split, _output, _begin, _end );

            case enums::DataUsed::numerical_output_is_nan:
                return utils::Partitioner<
                    enums::DataUsed::numerical_output_is_nan>::
                    partition( _split.column_, _output, _begin, _end );

            case enums::DataUsed::same_units_categorical:
                assert_true( _input );
                return utils::Partitioner<
                    enums::DataUsed::same_units_categorical>::
                    partition( _split, *_input, _output, _begin, _end );

            case enums::DataUsed::same_units_discrete_ts:
            case enums::DataUsed::same_units_discrete:
                assert_true( _input );
                return utils::Partitioner<
                    enums::DataUsed::same_units_discrete>::
                    partition( _split, *_input, _output, _begin, _end );

            case enums::DataUsed::same_units_discrete_is_nan:
                assert_true( _input );
                return utils::Partitioner<
                    enums::DataUsed::same_units_discrete_is_nan>::
                    partition(
                        _split.column_input_,
                        _split.column_,
                        *_input,
                        _output,
                        _begin,
                        _end );

            case enums::DataUsed::same_units_numerical_ts:
            case enums::DataUsed::same_units_numerical:
                assert_true( _input );
                return utils::Partitioner<
                    enums::DataUsed::same_units_numerical>::
                    partition( _split, *_input, _output, _begin, _end );

            case enums::DataUsed::same_units_numerical_is_nan:
                assert_true( _input );
                return utils::Partitioner<
                    enums::DataUsed::same_units_numerical_is_nan>::
                    partition(
                        _split.column_input_,
                        _split.column_,
                        *_input,
                        _output,
                        _begin,
                        _end );

            case enums::DataUsed::subfeatures:
                assert_true( _input );
                return utils::Partitioner<enums::DataUsed::subfeatures>::
                    partition( _split, _subfeatures, _begin, _end );

            case enums::DataUsed::time_stamps_diff:
                assert_true( _input );
                return utils::Partitioner<enums::DataUsed::time_stamps_diff>::
                    partition( _split, *_input, _output, _begin, _end );

            case enums::DataUsed::time_stamps_window:
                assert_true( _input );
                return utils::Partitioner<enums::DataUsed::time_stamps_window>::
                    partition(
                        _split,
                        hyperparameters().delta_t_,
                        *_input,
                        _output,
                        _begin,
                        _end );

            default:
                assert_true( false && "Unknown data_used_" );
                return _begin;
        }
}

// ----------------------------------------------------------------------------

Poco::JSON::Object::Ptr DecisionTreeNode::to_json_obj() const
{
    Poco::JSON::Object::Ptr obj( new Poco::JSON::Object() );

    if ( input_ )
        {
            obj->set( "input_", input().to_json_obj() );
        }

    obj->set( "output_", output().to_json_obj() );

    obj->set( "weights_", JSON::vector_to_array( weights_ ) );

    if ( child_greater_ )
        {
            assert_true( child_smaller_ );

            obj->set( "column_", split_.column_ );

            obj->set( "column_input_", split_.column_input_ );

            obj->set(
                "categories_used_",
                JSON::vector_to_array( *split_.categories_used_ ) );

            obj->set( "critical_value_", split_.critical_value_ );

            obj->set( "data_used_", JSON::stringify( split_.data_used_ ) );

            obj->set( "child_greater_", child_greater_->to_json_obj() );

            obj->set( "child_smaller_", child_smaller_->to_json_obj() );

            // For backwards compatatability.
            if ( !std::isnan( loss_reduction_ ) )
                {
                    obj->set( "loss_reduction_", loss_reduction_ );
                }
        }

    return obj;
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::to_sql(
    const std::vector<strings::String>& _categories,
    const std::string& _feature_num,
    const std::string& _sql,
    std::vector<std::string>* _conditions ) const
{
    if ( child_greater_ )
        {
            assert_true( child_smaller_ );

            const auto prefix = ( _sql == "" ) ? "WHEN " : " AND ";

            const auto sql_greater =
                _sql + prefix +
                condition_maker_.condition_greater(
                    _categories, input(), output(), split_ );

            child_greater_->to_sql(
                _categories, _feature_num, sql_greater, _conditions );

            const auto sql_smaller =
                _sql + prefix +
                condition_maker_.condition_smaller(
                    _categories, input(), output(), split_ );

            child_smaller_->to_sql(
                _categories, _feature_num, sql_smaller, _conditions );
        }
    else
        {
            const auto condition =
                _sql + " THEN " +
                condition_maker_.make_equation( input(), output(), weights_ );

            _conditions->push_back( condition );
        }
}

// ----------------------------------------------------------------------------

Float DecisionTreeNode::transform(
    const containers::DataFrameView& _output,
    const std::optional<containers::DataFrame>& _input,
    const containers::Subfeatures& _subfeatures,
    const containers::Rescaled& _output_rescaled,
    const containers::Rescaled& _input_rescaled,
    const containers::Match& _match ) const
{
    // ------------------------------------------------------------------------

    assert_true( _input );

    if ( !child_greater_ )
        {
            assert_true( !child_smaller_ );
            return calc_prediction( _output_rescaled, _input_rescaled, _match );
        }

    // ------------------------------------------------------------------------

    const bool is_greater =
        match_is_greater( _output, _input, _subfeatures, _match );

    // ------------------------------------------------------------------------

    if ( is_greater )
        {
            return child_greater_->transform(
                _output,
                _input,
                _subfeatures,
                _output_rescaled,
                _input_rescaled,
                _match );
        }
    else
        {
            return child_smaller_->transform(
                _output,
                _input,
                _subfeatures,
                _output_rescaled,
                _input_rescaled,
                _match );
        }

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::vector<containers::CandidateSplit> DecisionTreeNode::try_all(
    const Float _old_intercept,
    const containers::DataFrameView& _output,
    const containers::DataFrame& _input,
    const containers::Subfeatures& _subfeatures,
    const containers::Rescaled& _output_rescaled,
    const containers::Rescaled& _input_rescaled,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _end )
{
    std::vector<containers::CandidateSplit> candidates;

    std::vector<containers::Match> bins( _begin, _end );

    try_categorical_input(
        _old_intercept,
        _input,
        _output_rescaled,
        _input_rescaled,
        _begin,
        _end,
        &bins,
        &candidates );

    try_discrete_input(
        _old_intercept, _input, _begin, _end, &bins, &candidates );

    try_numerical_input(
        _old_intercept, _input, _begin, _end, &bins, &candidates );

    try_same_units_categorical(
        _old_intercept, _input, _output, _begin, _end, &candidates );

    try_same_units_discrete(
        _old_intercept, _input, _output, _begin, _end, &bins, &candidates );

    try_same_units_numerical(
        _old_intercept, _input, _output, _begin, _end, &bins, &candidates );

    try_time_stamps_window(
        _old_intercept, _input, _output, _begin, _end, &bins, &candidates );

    try_categorical_output(
        _old_intercept,
        _output,
        _output_rescaled,
        _input_rescaled,
        _begin,
        _end,
        &bins,
        &candidates );

    try_discrete_output(
        _old_intercept, _output, _begin, _end, &bins, &candidates );

    try_numerical_output(
        _old_intercept, _output, _begin, _end, &bins, &candidates );

    /*try_subfeatures(
        _old_intercept, _subfeatures, _begin, _end, &bins, &candidates );*/

    return candidates;
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_categorical(
    const enums::Revert _revert,
    const Int _min,
    const std::shared_ptr<const std::vector<Int>> _critical_values,
    const size_t _num_column,
    const Float _old_intercept,
    const enums::DataUsed _data_used,
    const std::vector<size_t>& _indptr,
    std::vector<containers::Match>* _bins,
    std::vector<containers::CandidateSplit>* _candidates )
{
    debug_log( "try_categorical." );

    assert_true( _min >= 0 );

    assert_true( _critical_values );

    auto critical_values_begin = _critical_values->begin();

    for ( size_t i = 0; i < _critical_values->size(); ++i )
        {
            const auto cv = ( *_critical_values )[i];

            const auto update =
                ( i == 0 ? enums::Update::calc_all : enums::Update::calc_diff );

            if ( _revert == enums::Revert::True )
                {
                    critical_values_begin = _critical_values->begin() + i;
                }

            assert_true( cv >= _min );

            assert_true(
                static_cast<size_t>( cv - _min ) < _indptr.size() - 1 );

            const auto split_begin = _bins->begin() + _indptr[cv - _min];

            const auto split_end = _bins->begin() + _indptr[cv - _min + 1];

            add_candidates(
                _revert,
                update,
                _old_intercept,
                containers::Split(
                    _critical_values,
                    critical_values_begin,
                    _critical_values->begin() + i + 1,
                    _num_column,
                    _data_used ),
                _bins->begin(),
                split_begin,
                split_end,
                _bins->end(),
                _candidates );
        }

    loss_function().revert_to_commit();
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_categorical_input(
    const Float _old_intercept,
    const containers::DataFrame& _input,
    const containers::Rescaled& _output_rescaled,
    const containers::Rescaled& _input_rescaled,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _end,
    std::vector<containers::Match>* _bins,
    std::vector<containers::CandidateSplit>* _candidates )
{
    debug_log( "try_categorical_input." );

    for ( size_t j = 0; j < _input.num_categoricals(); ++j )
        {
            // ----------------------------------------------------------------

            if ( _input.categorical_unit( j ).find( "comparison only" ) !=
                 std::string::npos )
                {
                    continue;
                }

            // ----------------------------------------------------------------
            // First, we bin by category.

            const auto is_nan = [j, &_input]( const containers::Match& m ) {
                const auto i = m.ix_input;
                assert_true( i < _input.nrows() );
                return ( _input.categorical( i, j ) >= 0 );
            };

            // Moves all NULL values to the end.
            const auto nan_begin = std::partition( _begin, _end, is_nan );

            const auto get_value = [j, &_input]( const containers::Match& m ) {
                const auto i = m.ix_input;
                assert_true( i < _input.nrows() );
                return _input.categorical( i, j );
            };

            const auto [min, max] =
                utils::MinMaxFinder<decltype( get_value ), Int>::find_min_max(
                    get_value, _begin, nan_begin, &comm() );

            // Note that this bins in ASCENDING order.
            const auto [indptr, critical_values] =
                utils::CategoricalBinner<decltype( get_value )>::bin(
                    min,
                    max,
                    get_value,
                    _begin,
                    nan_begin,
                    _end,
                    _bins,
                    &comm() );

            assert_true( indptr.size() == 0 || critical_values );

            if ( indptr.size() == 0 || critical_values->size() <= 1 )
                {
                    continue;
                }

            // ----------------------------------------------------------------
            // Record the current size of _candidates - we will need it later.

            const auto begin_ix = _candidates->size();

            // ----------------------------------------------------------------
            // Try individual categorical values.

            try_categorical(
                enums::Revert::True,
                min,
                critical_values,
                j,
                _old_intercept,
                enums::DataUsed::categorical_input,
                indptr,
                _bins,
                _candidates );

            // ----------------------------------------------------------------
            // Sort critical values by their associated weights in DESCENDING
            // order.

            const auto sorted_critical_values =
                utils::CriticalValueSorter::sort(
                    min,
                    indptr,
                    _output_rescaled,
                    _input_rescaled,
                    _candidates->begin() + begin_ix,
                    _candidates->end(),
                    _bins,
                    comm_ );

            // ----------------------------------------------------------------
            // Try combined categorical values.

            try_categorical(
                enums::Revert::False,
                min,
                sorted_critical_values,
                j,
                _old_intercept,
                enums::DataUsed::categorical_input,
                indptr,
                _bins,
                _candidates );

            // ----------------------------------------------------------------
        }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_categorical_output(
    const Float _old_intercept,
    const containers::DataFrameView& _output,
    const containers::Rescaled& _output_rescaled,
    const containers::Rescaled& _input_rescaled,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _end,
    std::vector<containers::Match>* _bins,
    std::vector<containers::CandidateSplit>* _candidates )
{
    debug_log( "try_categorical_output." );

    for ( size_t j = 0; j < _output.num_categoricals(); ++j )
        {
            // ----------------------------------------------------------------

            if ( _output.categorical_unit( j ).find( "comparison only" ) !=
                 std::string::npos )
                {
                    continue;
                }

            // ----------------------------------------------------------------
            // First, we bin by category.

            const auto is_nan = [j, &_output]( const containers::Match& m ) {
                const auto i = m.ix_output;
                assert_true( i < _output.nrows() );
                return ( _output.categorical( i, j ) >= 0 );
            };

            // Moves all NULL values to the end.
            const auto nan_begin = std::partition( _begin, _end, is_nan );

            const auto get_value = [j, &_output]( const containers::Match& m ) {
                const auto i = m.ix_output;
                assert_true( i < _output.nrows() );
                return _output.categorical( i, j );
            };

            const auto [min, max] =
                utils::MinMaxFinder<decltype( get_value ), Int>::find_min_max(
                    get_value, _begin, nan_begin, &comm() );

            // Note that this bins in ASCENDING order.
            const auto [indptr, critical_values] =
                utils::CategoricalBinner<decltype( get_value )>::bin(
                    min,
                    max,
                    get_value,
                    _begin,
                    nan_begin,
                    _end,
                    _bins,
                    &comm() );

            assert_true( indptr.size() == 0 || critical_values );

            if ( indptr.size() == 0 || critical_values->size() <= 1 )
                {
                    continue;
                }

            // ----------------------------------------------------------------
            // Record the current size of _candidates - we will need it later.

            const auto begin_ix = _candidates->size();

            // ----------------------------------------------------------------
            // Try individual categorical values.

            try_categorical(
                enums::Revert::True,
                min,
                critical_values,
                j,
                _old_intercept,
                enums::DataUsed::categorical_output,
                indptr,
                _bins,
                _candidates );

            // ----------------------------------------------------------------
            // Sort critical values by their associated weights in DESCENDING
            // order.

            const auto sorted_critical_values =
                utils::CriticalValueSorter::sort(
                    min,
                    indptr,
                    _output_rescaled,
                    _input_rescaled,
                    _candidates->begin() + begin_ix,
                    _candidates->end(),
                    _bins,
                    comm_ );

            // ----------------------------------------------------------------
            // Try combined categorical values.

            try_categorical(
                enums::Revert::False,
                min,
                sorted_critical_values,
                j,
                _old_intercept,
                enums::DataUsed::categorical_output,
                indptr,
                _bins,
                _candidates );

            // ----------------------------------------------------------------
        }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_discrete_input(
    const Float _old_intercept,
    const containers::DataFrame& _input,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _end,
    std::vector<containers::Match>* _bins,
    std::vector<containers::CandidateSplit>* _candidates )
{
    debug_log( "try_discrete_input." );

    for ( size_t j = 0; j < _input.num_discretes(); ++j )
        {
            if ( _input.discrete_unit( j ).find( "comparison only" ) !=
                 std::string::npos )
                {
                    continue;
                }

            // Moves all matches for which the critical value is NAN to the
            // very end.
            const auto nan_begin =
                utils::Partitioner<enums::DataUsed::discrete_input_is_nan>::
                    partition( j, _input, _begin, _end );

            const auto get_value = [j, &_input]( const containers::Match& m ) {
                const auto i = m.ix_input;
                assert_true( i < _input.nrows() );
                return _input.discrete( i, j );
            };

            const auto [min, max] =
                utils::MinMaxFinder<decltype( get_value ), Float>::find_min_max(
                    get_value, _begin, nan_begin, &comm() );

            const auto num_bins_numerical = calc_num_bins( _begin, nan_begin );

            // Note that this bins in DESCENDING order.
            const auto [indptr, step_size] =
                utils::DiscreteBinner<decltype( get_value )>::bin(
                    min,
                    max,
                    get_value,
                    num_bins_numerical,
                    _begin,
                    nan_begin,
                    _end,
                    _bins );

            if ( indptr.size() == 0 )
                {
                    continue;
                }

            try_numerical_or_discrete(
                enums::DataUsed::discrete_input,
                j,
                0,
                _old_intercept,
                max,
                step_size,
                indptr,
                _bins,
                _candidates );
        }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_discrete_output(
    const Float _old_intercept,
    const containers::DataFrameView& _output,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _end,
    std::vector<containers::Match>* _bins,
    std::vector<containers::CandidateSplit>* _candidates )
{
    debug_log( "try_discrete_output." );

    for ( size_t j = 0; j < _output.num_discretes(); ++j )
        {
            if ( _output.discrete_unit( j ).find( "comparison only" ) !=
                 std::string::npos )
                {
                    continue;
                }

            // Moves all matches for which the critical value is NAN to the
            // very end.
            const auto nan_begin =
                utils::Partitioner<enums::DataUsed::discrete_output_is_nan>::
                    partition( j, _output, _begin, _end );

            const auto get_value = [j, &_output]( const containers::Match& m ) {
                const auto i = m.ix_output;
                assert_true( i < _output.nrows() );
                return _output.discrete( i, j );
            };

            const auto [min, max] =
                utils::MinMaxFinder<decltype( get_value ), Float>::find_min_max(
                    get_value, _begin, nan_begin, &comm() );

            const auto num_bins_numerical = calc_num_bins( _begin, nan_begin );

            // Note that this bins in DESCENDING order.
            const auto [indptr, step_size] =
                utils::DiscreteBinner<decltype( get_value )>::bin(
                    min,
                    max,
                    get_value,
                    num_bins_numerical,
                    _begin,
                    nan_begin,
                    _end,
                    _bins );

            if ( indptr.size() == 0 )
                {
                    continue;
                }

            try_numerical_or_discrete(
                enums::DataUsed::discrete_output,
                j,
                0,
                _old_intercept,
                max,
                step_size,
                indptr,
                _bins,
                _candidates );
        }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_numerical_or_discrete(
    const enums::DataUsed _data_used,
    const size_t _col1,
    const size_t _col2,
    const Float _old_intercept,
    const Float _max,
    const Float _step_size,
    const std::vector<size_t>& _indptr,
    std::vector<containers::Match>* _bins,
    std::vector<containers::CandidateSplit>* _candidates )
{
    for ( size_t i = 1; i < _indptr.size(); ++i )
        {
            assert_true( _indptr[i - 1] <= _indptr[i] );
            assert_true( _indptr[i] <= _bins->size() );

            const auto split_begin = _bins->begin() + _indptr[i - 1];

            const auto split_end = _bins->begin() + _indptr[i];

            const auto update =
                ( i == 1 ? enums::Update::calc_all : enums::Update::calc_diff );

            const auto critical_value =
                _max - static_cast<Float>( i ) * _step_size;

            const auto split =
                is_same_units( _data_used )
                    ? containers::Split(
                          _col1, _col2, critical_value, _data_used )
                    : containers::Split( _col1, critical_value, _data_used );

            add_candidates(
                enums::Revert::False,
                update,
                _old_intercept,
                split,
                _bins->begin(),
                split_begin,
                split_end,
                _bins->end(),
                _candidates );
        }

    loss_function().revert_to_commit();
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_numerical_input(
    const Float _old_intercept,
    const containers::DataFrame& _input,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _end,
    std::vector<containers::Match>* _bins,
    std::vector<containers::CandidateSplit>* _candidates )
{
    debug_log( "try_numerical_input." );

    for ( size_t j = 0; j < _input.num_numericals(); ++j )
        {
            if ( _input.numerical_unit( j ).find( "comparison only" ) !=
                 std::string::npos )
                {
                    continue;
                }

            // Moves all matches for which the critical value is NAN to the
            // very end.
            const auto nan_begin =
                utils::Partitioner<enums::DataUsed::numerical_input_is_nan>::
                    partition( j, _input, _begin, _end );

            const auto get_value = [j, &_input]( const containers::Match& m ) {
                const auto i = m.ix_input;
                assert_true( i < _input.nrows() );
                return _input.numerical( i, j );
            };

            const auto [min, max] =
                utils::MinMaxFinder<decltype( get_value ), Float>::find_min_max(
                    get_value, _begin, nan_begin, &comm() );

            const auto num_bins = calc_num_bins( _begin, nan_begin );

            // Note that this bins in DESCENDING order.
            const auto [indptr, step_size] =
                utils::NumericalBinner<decltype( get_value )>::bin(
                    min,
                    max,
                    get_value,
                    num_bins,
                    _begin,
                    nan_begin,
                    _end,
                    _bins );

            if ( indptr.size() == 0 )
                {
                    continue;
                }

            try_numerical_or_discrete(
                enums::DataUsed::numerical_input,
                j,
                0,
                _old_intercept,
                max,
                step_size,
                indptr,
                _bins,
                _candidates );
        }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_numerical_output(
    const Float _old_intercept,
    const containers::DataFrameView& _output,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _end,
    std::vector<containers::Match>* _bins,
    std::vector<containers::CandidateSplit>* _candidates )
{
    debug_log( "try_numerical_output." );

    for ( size_t j = 0; j < _output.num_numericals(); ++j )
        {
            if ( _output.numerical_unit( j ).find( "comparison only" ) !=
                 std::string::npos )
                {
                    continue;
                }

            // Moves all matches for which the critical value is NAN to the
            // very end.
            const auto nan_begin =
                utils::Partitioner<enums::DataUsed::numerical_output_is_nan>::
                    partition( j, _output, _begin, _end );

            const auto get_value = [j, &_output]( const containers::Match& m ) {
                const auto i = m.ix_output;
                assert_true( i < _output.nrows() );
                return _output.numerical( i, j );
            };

            const auto [min, max] =
                utils::MinMaxFinder<decltype( get_value ), Float>::find_min_max(
                    get_value, _begin, nan_begin, &comm() );

            const auto num_bins = calc_num_bins( _begin, nan_begin );

            // Note that this bins in DESCENDING order.
            const auto [indptr, step_size] =
                utils::NumericalBinner<decltype( get_value )>::bin(
                    min,
                    max,
                    get_value,
                    num_bins,
                    _begin,
                    nan_begin,
                    _end,
                    _bins );

            if ( indptr.size() == 0 )
                {
                    continue;
                }

            try_numerical_or_discrete(
                enums::DataUsed::numerical_output,
                j,
                0,
                _old_intercept,
                max,
                step_size,
                indptr,
                _bins,
                _candidates );
        }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_same_units_categorical(
    const Float _old_intercept,
    const containers::DataFrame& _input,
    const containers::DataFrameView& _output,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _end,
    std::vector<containers::CandidateSplit>* _candidates )
{
    debug_log( "try_same_units_categorical." );

    for ( size_t output_col = 0; output_col < _output.num_categoricals();
          ++output_col )
        {
            for ( size_t input_col = 0; input_col < _input.num_categoricals();
                  ++input_col )
                {
                    if ( _output.categorical_unit( output_col ) == "" ||
                         _output.categorical_unit( output_col ) !=
                             _input.categorical_unit( input_col ) )
                        {
                            continue;
                        }

                    const auto partition_function = [input_col,
                                                     output_col,
                                                     &_input,
                                                     &_output](
                                                        containers::Match m ) {
                        assert_true( m.ix_input < _input.nrows() );
                        assert_true( m.ix_output < _output.nrows() );

                        const bool is_same =
                            ( _input.categorical( m.ix_input, input_col ) ==
                              _output.categorical( m.ix_output, output_col ) );

                        return is_same;
                    };

                    const auto it =
                        std::partition( _begin, _end, partition_function );

                    add_candidates(
                        enums::Revert::False,
                        enums::Update::calc_all,
                        _old_intercept,
                        containers::Split( output_col, input_col ),
                        _begin,
                        _begin,
                        it,
                        _end,
                        _candidates );

                    loss_function().revert_to_commit();
                }
        }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_same_units_discrete(
    const Float _old_intercept,
    const containers::DataFrame& _input,
    const containers::DataFrameView& _output,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _end,
    std::vector<containers::Match>* _bins,
    std::vector<containers::CandidateSplit>* _candidates )
{
    debug_log( "try_same_units_discrete." );

    for ( size_t output_col = 0; output_col < _output.num_discretes();
          ++output_col )
        {
            for ( size_t input_col = 0; input_col < _input.num_discretes();
                  ++input_col )
                {
                    if ( _output.discrete_unit( output_col ) == "" ||
                         _output.discrete_unit( output_col ) !=
                             _input.discrete_unit( input_col ) )
                        {
                            continue;
                        }

                    // Moves all matches for which the critical value is NAN to
                    // the very end.
                    const auto nan_begin = utils::Partitioner<
                        enums::DataUsed::same_units_discrete_is_nan>::
                        partition(
                            input_col,
                            output_col,
                            _input,
                            _output,
                            _begin,
                            _end );

                    const auto get_value =
                        [input_col, output_col, &_input, &_output](
                            const containers::Match& m ) {
                            const auto i1 = m.ix_input;
                            const auto i2 = m.ix_output;
                            assert_true( i1 < _input.nrows() );
                            assert_true( i2 < _output.nrows() );
                            return _output.discrete( i2, output_col ) -
                                   _input.discrete( i1, input_col );
                        };

                    const auto [min, max] =
                        utils::MinMaxFinder<decltype( get_value ), Float>::
                            find_min_max(
                                get_value, _begin, nan_begin, &comm() );

                    const auto num_bins = calc_num_bins( _begin, nan_begin );

                    // Note that this bins in DESCENDING order.
                    const auto [indptr, step_size] =
                        utils::DiscreteBinner<decltype( get_value )>::bin(
                            min,
                            max,
                            get_value,
                            num_bins,
                            _begin,
                            nan_begin,
                            _end,
                            _bins );

                    if ( indptr.size() == 0 )
                        {
                            continue;
                        }

                    const bool is_ts =
                        _output.discrete_unit( output_col )
                                .find( "time stamp" ) != std::string::npos &&
                        _output.discrete_name( output_col )
                                .find( "$GETML_ROWID" ) == std::string::npos;

                    const auto data_used =
                        is_ts ? enums::DataUsed::same_units_discrete_ts
                              : enums::DataUsed::same_units_discrete;

                    try_numerical_or_discrete(
                        data_used,
                        output_col,
                        input_col,
                        _old_intercept,
                        max,
                        step_size,
                        indptr,
                        _bins,
                        _candidates );
                }
        }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_same_units_numerical(
    const Float _old_intercept,
    const containers::DataFrame& _input,
    const containers::DataFrameView& _output,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _end,
    std::vector<containers::Match>* _bins,
    std::vector<containers::CandidateSplit>* _candidates )
{
    debug_log( "try_same_units_numerical." );

    for ( size_t output_col = 0; output_col < _output.num_numericals();
          ++output_col )
        {
            for ( size_t input_col = 0; input_col < _input.num_numericals();
                  ++input_col )
                {
                    if ( _output.numerical_unit( output_col ) == "" ||
                         _output.numerical_unit( output_col ) !=
                             _input.numerical_unit( input_col ) )
                        {
                            continue;
                        }

                    // Moves all matches for which the critical value is NAN to
                    // the very end.
                    const auto nan_begin = utils::Partitioner<
                        enums::DataUsed::same_units_numerical_is_nan>::
                        partition(
                            input_col,
                            output_col,
                            _input,
                            _output,
                            _begin,
                            _end );

                    const auto get_value =
                        [input_col, output_col, &_input, &_output](
                            const containers::Match& m ) {
                            const auto i1 = m.ix_input;
                            const auto i2 = m.ix_output;
                            assert_true( i1 < _input.nrows() );
                            assert_true( i2 < _output.nrows() );
                            return _output.numerical( i2, output_col ) -
                                   _input.numerical( i1, input_col );
                        };

                    const auto [min, max] =
                        utils::MinMaxFinder<decltype( get_value ), Float>::
                            find_min_max(
                                get_value, _begin, nan_begin, &comm() );

                    const auto num_bins = calc_num_bins( _begin, nan_begin );

                    // Note that this bins in DESCENDING order.
                    const auto [indptr, step_size] =
                        utils::NumericalBinner<decltype( get_value )>::bin(
                            min,
                            max,
                            get_value,
                            num_bins,
                            _begin,
                            nan_begin,
                            _end,
                            _bins );

                    if ( indptr.size() == 0 )
                        {
                            continue;
                        }

                    const bool is_ts =
                        _output.numerical_unit( output_col )
                                .find( "time stamp" ) != std::string::npos &&
                        _output.numerical_name( output_col )
                                .find( "$GETML_ROWID" ) == std::string::npos;

                    const auto data_used =
                        is_ts ? enums::DataUsed::same_units_numerical_ts
                              : enums::DataUsed::same_units_numerical;

                    try_numerical_or_discrete(
                        data_used,
                        output_col,
                        input_col,
                        _old_intercept,
                        max,
                        step_size,
                        indptr,
                        _bins,
                        _candidates );
                }
        }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_subfeatures(
    const Float _old_intercept,
    const containers::Subfeatures& _subfeatures,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _end,
    std::vector<containers::Match>* _bins,
    std::vector<containers::CandidateSplit>* _candidates )
{
    debug_log( "try_subfeatures." );

    for ( size_t j = 0; j < _subfeatures.size(); ++j )
        {
            assert_true( std::all_of(
                _subfeatures[j].col().begin(),
                _subfeatures[j].col().end(),
                []( const Float val ) {
                    return !std::isnan( val ) && !std::isinf( val );
                } ) );

            const auto get_value =
                [j, &_subfeatures]( const containers::Match& m ) {
                    const auto i = m.ix_input;
                    return _subfeatures[j][i];
                };

            const auto [min, max] =
                utils::MinMaxFinder<decltype( get_value ), Float>::find_min_max(
                    get_value, _begin, _end, &comm() );

            const auto num_bins = calc_num_bins( _begin, _end );

            // Note that this bins in DESCENDING order.
            const auto [indptr, step_size] =
                utils::NumericalBinner<decltype( get_value )>::bin(
                    min, max, get_value, num_bins, _begin, _end, _end, _bins );

            if ( indptr.size() == 0 )
                {
                    continue;
                }

            try_numerical_or_discrete(
                enums::DataUsed::subfeatures,
                j,
                0,
                _old_intercept,
                max,
                step_size,
                indptr,
                _bins,
                _candidates );
        }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_time_stamps_window(
    const Float _old_intercept,
    const containers::DataFrame& _input,
    const containers::DataFrameView& _output,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _end,
    std::vector<containers::Match>* _bins,
    std::vector<containers::CandidateSplit>* _candidates )
{
    // ------------------------------------------------------------------------

    if ( hyperparameters().delta_t_ <= 0.0 )
        {
            return;
        }

    // ------------------------------------------------------------------------

    debug_log( "Time windows." );

    // ------------------------------------------------------------------------

    const auto get_value = [&_input, &_output]( const containers::Match& m ) {
        const auto i1 = m.ix_output;
        const auto i2 = m.ix_input;
        assert_true( i1 < _output.nrows() );
        assert_true( i2 < _input.nrows() );
        return _output.time_stamp( i1 ) - _input.time_stamp( i2 );
    };

    const auto [min, max] =
        utils::MinMaxFinder<decltype( get_value ), Float>::find_min_max(
            get_value, _begin, _end, &comm() );

    if ( max <= min )
        {
            return;
        }

    const auto step_size = hyperparameters().delta_t_;

    const auto num_bins = static_cast<size_t>( ( max - min ) / step_size ) + 1;

    // Be reasonable - avoid memory overflow.
    if ( num_bins > 1000000 )
        {
            return;
        }

    // ------------------------------------------------------------------------

    // Note that this bins in DESCENDING order.
    const auto indptr =
        utils::NumericalBinner<decltype( get_value )>::bin_given_step_size(
            min, max, get_value, step_size, _begin, _end, _end, _bins );

    if ( indptr.size() == 0 )
        {
            return;
        }

    // ------------------------------------------------------------------------

    for ( size_t i = 1; i < indptr.size(); ++i )
        {
            assert_true( indptr[i - 1] <= indptr[i] );
            assert_true( indptr[i] <= _bins->size() );

            const auto split_begin = _bins->begin() + indptr[i - 1];

            const auto split_end = _bins->begin() + indptr[i];

            const auto update =
                ( i == 1 ? enums::Update::calc_all : enums::Update::calc_diff );

            const auto critical_value =
                max - static_cast<Float>( i ) * step_size;

            add_candidates(
                enums::Revert::True,
                update,
                _old_intercept,
                containers::Split(
                    0, critical_value, enums::DataUsed::time_stamps_window ),
                _bins->begin(),
                split_begin,
                split_end,
                _bins->end(),
                _candidates );
        }

    loss_function().revert_to_commit();

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace decisiontrees
}  // namespace relcit
