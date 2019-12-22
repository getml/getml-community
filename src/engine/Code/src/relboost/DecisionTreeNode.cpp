#include "relboost/decisiontrees/decisiontrees.hpp"

namespace relboost
{
namespace decisiontrees
{
// ----------------------------------------------------------------------------

DecisionTreeNode::DecisionTreeNode(
    const utils::ConditionMaker& _condition_maker,
    const Int _depth,
    const std::shared_ptr<const Hyperparameters>& _hyperparameters,
    const std::shared_ptr<lossfunctions::LossFunction>& _loss_function,
    const Float _weight,
    multithreading::Communicator* _comm )
    : comm_( _comm ),
      condition_maker_( _condition_maker ),
      depth_( _depth ),
      hyperparameters_( _hyperparameters ),
      loss_function_( _loss_function ),
      weight_( _weight )
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
      weight_(
          _obj.has( "weight_" ) ? JSON::get_value<Float>( _obj, "weight_" )
                                : NAN )
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
    // -----------------------------------------------------------------
    // Calculate weights.

    auto pairs = loss_function().calc_pairs(
        _revert,
        _update,
        hyperparameters().min_num_samples_,
        _old_intercept,
        weight_,
        _begin,
        _last_it,
        _it,
        _end );

    // -----------------------------------------------------------------
    // Calculate and store loss reduction.

    for ( const auto& [loss_reduction, new_weights] : pairs )
        {
            assert_true( !std::isinf( std::get<0>( new_weights ) ) );
            assert_true( !std::isinf( std::get<1>( new_weights ) ) );
            assert_true( !std::isinf( std::get<2>( new_weights ) ) );

            if ( std::isnan( std::get<0>( new_weights ) ) )
                {
                    continue;
                }

            assert_true(
                !std::isnan( std::get<1>( new_weights ) ) ||
                !std::isnan( std::get<2>( new_weights ) ) );

            _candidates->push_back( containers::CandidateSplit(
                loss_reduction, _split, new_weights ) );
        }

    // -----------------------------------------------------------------
    // Revert, if applicable

    if ( _revert == enums::Revert::True )
        {
            loss_function().revert( weight_ );
        }

    // -----------------------------------------------------------------
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

void DecisionTreeNode::fit(
    const containers::DataFrameView& _output,
    const std::optional<containers::DataFrame>& _input,
    const containers::Subfeatures& _subfeatures,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _end,
    Float* _intercept )
{
    // ------------------------------------------------------------------------
    // Store input and output (we need the column names).

    if ( _input )
        {
            input_.reset( new containers::Placeholder( _input->to_schema() ) );
        }

    output_.reset( new containers::Placeholder( _output.df().to_schema() ) );

    // ------------------------------------------------------------------------
    // If the maximum depth is reached or there are no samples to fit, don't
    // bother fitting the node.

    if ( hyperparameters().max_depth_ > 0 &&
         depth_ == hyperparameters().max_depth_ )
        {
            debug_log( "Max depth reached." );
            return;
        }

    // ------------------------------------------------------------------------
    // Try all possible splits.

    auto candidates =
        try_all( *_intercept, _output, _input, _subfeatures, _begin, _end );

    debug_log( "candidates.size(): " + std::to_string( candidates.size() ) );

    if ( candidates.size() == 0 )
        {
            debug_log( "No candidates." );
            return;
        }

    // ------------------------------------------------------------------------
    // Identify best candidate split (the one with the minimal partial loss)

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
    // So far, we only calculated the partial loss. But we need to evaluate
    // it properly.

    const auto it_split = partition(
        _output, _input, _subfeatures, best_split.split_, _begin, _end );

    const auto loss_reduction = loss_function().evaluate_split(
        *_intercept, weight_, best_split.weights_, _begin, it_split, _end );

    // ------------------------------------------------------------------------
    // If the loss reduction is sufficient, then take this split.

    debug_log( "loss_reduction: " + std::to_string( loss_reduction ) );

    if ( loss_reduction < hyperparameters().gamma_ )
        {
            loss_function().revert_to_commit();
            return;
        }

    split_ = best_split.split_.deep_copy();

    loss_function().commit( *_intercept, weight_, best_split.weights_ );

    *_intercept = std::get<0>( best_split.weights_ );

    // ------------------------------------------------------------------------
    // Set up and fit child nodes.

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
        _output, _input, _subfeatures, _begin, it_split, _intercept );

    child_smaller_->fit(
        _output, _input, _subfeatures, it_split, _end, _intercept );

    // ------------------------------------------------------------------------
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

    if ( !std::isnan( weight_ ) )
        {
            obj->set( "weight_", weight_ );
        }

    if ( child_greater_ )
        {
            assert_true( child_smaller_ );

            obj->set( "column_", split_.column_ );

            obj->set( "column_input_", split_.column_input_ );

            obj->set(
                "categories_used_",
                JSON::vector_to_array_ptr( *split_.categories_used_ ) );

            obj->set( "critical_value_", split_.critical_value_ );

            obj->set( "data_used_", JSON::stringify( split_.data_used_ ) );

            obj->set( "child_greater_", child_greater_->to_json_obj() );

            obj->set( "child_smaller_", child_smaller_->to_json_obj() );
        }

    return obj;
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::to_sql(
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
                condition_maker_.condition_greater( input(), output(), split_ );

            child_greater_->to_sql( _feature_num, sql_greater, _conditions );

            const auto sql_smaller =
                _sql + prefix +
                condition_maker_.condition_smaller( input(), output(), split_ );

            child_smaller_->to_sql( _feature_num, sql_smaller, _conditions );
        }
    else
        {
            const auto weight =
                std::isnan( weight_ ) ? "NULL" : std::to_string( weight_ );

            const auto condition = _sql + " THEN " + weight;

            _conditions->push_back( condition );
        }
}

// ----------------------------------------------------------------------------

Float DecisionTreeNode::transform(
    const containers::DataFrameView& _output,
    const std::optional<containers::DataFrame>& _input,
    const containers::Subfeatures& _subfeatures,
    const containers::Match& _match ) const
{
    // ------------------------------------------------------------------------
    // If the node has no children, then return its weight.

    if ( !child_greater_ )
        {
            return weight_;
        }

    // ------------------------------------------------------------------------
    // Calculate the value used for determining the split.

    assert_true( child_smaller_ );

    bool is_greater = false;

    switch ( split_.data_used_ )
        {
            case enums::DataUsed::categorical_input:
                assert_true( _input );
                is_greater =
                    utils::Partitioner<enums::DataUsed::categorical_input>::
                        is_greater( split_, *_input, _match );
                break;

            case enums::DataUsed::categorical_output:
                is_greater =
                    utils::Partitioner<enums::DataUsed::categorical_output>::
                        is_greater( split_, _output, _match );
                break;

            case enums::DataUsed::discrete_input:
                assert_true( _input );
                is_greater =
                    utils::Partitioner<enums::DataUsed::discrete_input>::
                        is_greater( split_, *_input, _match );
                break;

            case enums::DataUsed::discrete_input_is_nan:
                assert_true( _input );
                is_greater =
                    utils::Partitioner<enums::DataUsed::discrete_input_is_nan>::
                        is_greater( split_.column_, *_input, _match );
                break;

            case enums::DataUsed::discrete_output:
                is_greater =
                    utils::Partitioner<enums::DataUsed::discrete_output>::
                        is_greater( split_, _output, _match );
                break;

            case enums::DataUsed::discrete_output_is_nan:
                is_greater = utils::Partitioner<
                    enums::DataUsed::discrete_output_is_nan>::
                    is_greater( split_.column_, _output, _match );
                break;

            case enums::DataUsed::numerical_input:
                assert_true( _input );
                is_greater =
                    utils::Partitioner<enums::DataUsed::numerical_input>::
                        is_greater( split_, *_input, _match );
                break;

            case enums::DataUsed::numerical_input_is_nan:
                assert_true( _input );
                is_greater = utils::Partitioner<
                    enums::DataUsed::numerical_input_is_nan>::
                    is_greater( split_.column_, *_input, _match );
                break;

            case enums::DataUsed::numerical_output:
                is_greater =
                    utils::Partitioner<enums::DataUsed::numerical_output>::
                        is_greater( split_, _output, _match );
                break;

            case enums::DataUsed::numerical_output_is_nan:
                is_greater = utils::Partitioner<
                    enums::DataUsed::numerical_output_is_nan>::
                    is_greater( split_.column_, _output, _match );
                break;

            case enums::DataUsed::same_units_categorical:
                assert_true( _input );
                is_greater = utils::Partitioner<
                    enums::DataUsed::same_units_categorical>::
                    is_greater( split_, *_input, _output, _match );
                break;

            case enums::DataUsed::same_units_discrete:
                assert_true( _input );
                is_greater =
                    utils::Partitioner<enums::DataUsed::same_units_discrete>::
                        is_greater( split_, *_input, _output, _match );
                break;

            case enums::DataUsed::same_units_discrete_is_nan:
                assert_true( _input );
                is_greater = utils::Partitioner<
                    enums::DataUsed::same_units_discrete_is_nan>::
                    is_greater(
                        split_.column_input_,
                        split_.column_,
                        *_input,
                        _output,
                        _match );
                break;

            case enums::DataUsed::same_units_numerical:
                assert_true( _input );
                is_greater =
                    utils::Partitioner<enums::DataUsed::same_units_numerical>::
                        is_greater( split_, *_input, _output, _match );
                break;

            case enums::DataUsed::same_units_numerical_is_nan:
                assert_true( _input );
                is_greater = utils::Partitioner<
                    enums::DataUsed::same_units_numerical_is_nan>::
                    is_greater(
                        split_.column_input_,
                        split_.column_,
                        *_input,
                        _output,
                        _match );
                break;

            case enums::DataUsed::subfeatures:
                assert_true( _input );
                is_greater = utils::Partitioner<enums::DataUsed::subfeatures>::
                    is_greater( split_, _subfeatures, _match );
                break;

            case enums::DataUsed::time_stamps_diff:
                assert_true( _input );
                is_greater =
                    utils::Partitioner<enums::DataUsed::time_stamps_diff>::
                        is_greater( split_, *_input, _output, _match );
                break;

            case enums::DataUsed::time_stamps_window:
                assert_true( _input );
                is_greater =
                    utils::Partitioner<enums::DataUsed::time_stamps_window>::
                        is_greater(
                            split_,
                            hyperparameters().delta_t_,
                            *_input,
                            _output,
                            _match );
                break;

            default:
                assert_true( false && "Unknown data_used_" );
        }

    // ------------------------------------------------------------------------
    // Based on the value send problem to one of the child nodes.

    if ( is_greater )
        {
            return child_greater_->transform(
                _output, _input, _subfeatures, _match );
        }
    else
        {
            return child_smaller_->transform(
                _output, _input, _subfeatures, _match );
        }

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::vector<containers::CandidateSplit> DecisionTreeNode::try_all(
    const Float _old_intercept,
    const containers::DataFrameView& _output,
    const std::optional<containers::DataFrame>& _input,
    const containers::Subfeatures& _subfeatures,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _end )
{
    std::vector<containers::CandidateSplit> candidates;

    if ( _input )
        {
            try_categorical_input(
                _old_intercept, *_input, _begin, _end, &candidates );

            try_discrete_input(
                _old_intercept, *_input, _begin, _end, &candidates );

            try_numerical_input(
                _old_intercept, *_input, _begin, _end, &candidates );

            try_subfeatures(
                _old_intercept, _subfeatures, _begin, _end, &candidates );

            try_same_units_categorical(
                _old_intercept, *_input, _output, _begin, _end, &candidates );

            try_same_units_discrete(
                _old_intercept, *_input, _output, _begin, _end, &candidates );

            try_same_units_numerical(
                _old_intercept, *_input, _output, _begin, _end, &candidates );

            try_time_stamps_diff(
                _old_intercept, *_input, _output, _begin, _end, &candidates );
        }

    try_categorical_output(
        _old_intercept, _output, _begin, _end, &candidates );

    try_discrete_output( _old_intercept, _output, _begin, _end, &candidates );

    try_numerical_output( _old_intercept, _output, _begin, _end, &candidates );

    return candidates;
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_categorical(
    const enums::Revert _revert,
    const std::shared_ptr<const std::vector<Int>> _critical_values,
    const size_t num_column,
    const Float _old_intercept,
    const enums::DataUsed _data_used,
    const containers::CategoryIndex& _category_index,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _end,
    std::vector<containers::CandidateSplit>* _candidates )
{
    debug_log( "try_categorical." );

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

            add_candidates(
                _revert,
                update,
                _old_intercept,
                containers::Split(
                    _critical_values,
                    critical_values_begin,
                    _critical_values->begin() + i + 1,
                    num_column,
                    _data_used ),
                _begin,
                _category_index.begin( cv ),
                _category_index.end( cv ),
                _end,
                _candidates );
        }

    loss_function().revert_to_commit();
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_categorical_input(
    const Float _old_intercept,
    const containers::DataFrame& _input,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _end,
    std::vector<containers::CandidateSplit>* _candidates )
{
    debug_log( "try_categorical_input." );

    for ( size_t j = 0; j < _input.num_categoricals(); ++j )
        {
            // ----------------------------------------------------------------
            // Record the current size of _candidates - we will need it later.

            const auto begin_ix = _candidates->size();

            // ----------------------------------------------------------------
            // Sort the matches by their categorical value.

            utils::Sorter<enums::DataUsed::categorical_input>::sort(
                j, _input, _begin, _end );

            // ----------------------------------------------------------------
            // Identify all unique categorical values.

            // Used as placeholder.
            const auto output = containers::DataFrameView(
                _input, std::shared_ptr<const std::vector<size_t>>() );

            const auto critical_values =
                utils::CriticalValues::calc_categorical(
                    enums::DataUsed::categorical_input,
                    j,
                    _input,
                    output,
                    _begin,
                    _end,
                    &comm() );

            if ( critical_values->size() <= 1 )
                {
                    continue;
                }

            // ----------------------------------------------------------------
            // Build an index over the categories, so we can find them faster.

            auto category_index = containers::CategoryIndex( _begin, _end );

            category_index.build_indptr<enums::DataUsed::categorical_input>(
                _input, j, *critical_values.get() );

            // ----------------------------------------------------------------
            // Try individual categorical values.

            try_categorical(
                enums::Revert::True,
                critical_values,
                j,
                _old_intercept,
                enums::DataUsed::categorical_input,
                category_index,
                _begin,
                _end,
                _candidates );

            // ----------------------------------------------------------------
            // Sort critical values by their associated weights in DESCENDING
            // order.

            const auto sorted_critical_values =
                utils::CriticalValueSorter::sort(
                    _candidates->begin() + begin_ix, _candidates->end() );

            // ----------------------------------------------------------------
            // Try combined categorical values.

            try_categorical(
                enums::Revert::False,
                sorted_critical_values,
                j,
                _old_intercept,
                enums::DataUsed::categorical_input,
                category_index,
                _begin,
                _end,
                _candidates );

            // ----------------------------------------------------------------
        }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_categorical_output(
    const Float _old_intercept,
    const containers::DataFrameView& _output,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _end,
    std::vector<containers::CandidateSplit>* _candidates )
{
    debug_log( "try_categorical_output." );

    for ( size_t j = 0; j < _output.num_categoricals(); ++j )
        {
            // ----------------------------------------------------------------
            // Record the current size of _candidates - we will need it later.

            const auto begin_ix = _candidates->size();

            // ----------------------------------------------------------------
            // Sort the matches by their categorical value.

            utils::Sorter<enums::DataUsed::categorical_output>::sort(
                j, _output, _begin, _end );

            // ----------------------------------------------------------------
            // Identify all unique categorical values.

            const auto critical_values =
                utils::CriticalValues::calc_categorical(
                    enums::DataUsed::categorical_output,
                    j,
                    _output.df(),  // just as a placeholder
                    _output,
                    _begin,
                    _end,
                    &comm() );

            if ( critical_values->size() <= 1 )
                {
                    continue;
                }

            // ----------------------------------------------------------------
            // Build an index over the categories, so we can find them faster.

            auto category_index = containers::CategoryIndex( _begin, _end );

            category_index.build_indptr<enums::DataUsed::categorical_output>(
                _output, j, *critical_values.get() );

            // ----------------------------------------------------------------
            // Try individual categorical values.

            try_categorical(
                enums::Revert::True,
                critical_values,
                j,
                _old_intercept,
                enums::DataUsed::categorical_output,
                category_index,
                _begin,
                _end,
                _candidates );

            // ----------------------------------------------------------------
            // Sort critical values by their associated weights in DESCENDING
            // order.

            const auto sorted_critical_values =
                utils::CriticalValueSorter::sort(
                    _candidates->begin() + begin_ix, _candidates->end() );

            // ----------------------------------------------------------------
            // Try combined categorical values.

            try_categorical(
                enums::Revert::False,
                sorted_critical_values,
                j,
                _old_intercept,
                enums::DataUsed::categorical_output,
                category_index,
                _begin,
                _end,
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
    std::vector<containers::CandidateSplit>* _candidates )
{
    debug_log( "try_discrete_input." );

    for ( size_t j = 0; j < _input.num_discretes(); ++j )
        {
            // Moves all matches for which the critical value is NAN to the
            // very end.
            const auto nan_begin =
                utils::Partitioner<enums::DataUsed::discrete_input_is_nan>::
                    partition( j, _input, _begin, _end );

            // Note that this sorts in DESCENDING order.
            utils::Sorter<enums::DataUsed::discrete_input>::sort(
                j, _input, _begin, nan_begin );

            // Used as placeholder.
            const auto output = containers::DataFrameView(
                _input, std::shared_ptr<const std::vector<size_t>>() );

            const auto critical_values = utils::CriticalValues::calc_discrete(
                enums::DataUsed::discrete_input,
                j,
                _input,
                output,
                _begin,
                nan_begin,
                &comm() );

            if ( critical_values.size() == 0 ||
                 critical_values.front() == critical_values.back() )
                {
                    continue;
                }

            auto it = _begin;

            auto last_it = _begin;

            for ( auto cv = critical_values.begin();
                  cv != critical_values.end();
                  ++cv )
                {
                    it = utils::Finder<enums::DataUsed::discrete_input>::
                        next_split( *cv, j, _input, it, nan_begin );

                    const auto update =
                        ( cv == critical_values.begin()
                              ? enums::Update::calc_all
                              : enums::Update::calc_diff );

                    add_candidates(
                        enums::Revert::False,
                        update,
                        _old_intercept,
                        containers::Split(
                            j, *cv, enums::DataUsed::discrete_input ),
                        _begin,
                        last_it,
                        it,
                        _end,
                        _candidates );

                    last_it = it;
                }

            add_candidates(
                enums::Revert::False,
                enums::Update::calc_diff,
                _old_intercept,
                containers::Split(
                    j, 0.0, enums::DataUsed::discrete_input_is_nan ),
                _begin,
                last_it,
                nan_begin,
                _end,
                _candidates );

            loss_function().revert_to_commit();
        }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_discrete_output(
    const Float _old_intercept,
    const containers::DataFrameView& _output,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _end,
    std::vector<containers::CandidateSplit>* _candidates )
{
    debug_log( "try_discrete_output." );

    for ( size_t j = 0; j < _output.num_discretes(); ++j )
        {
            // Moves all matches for which the critical value is NAN to the
            // very end.
            const auto nan_begin =
                utils::Partitioner<enums::DataUsed::discrete_output_is_nan>::
                    partition( j, _output, _begin, _end );

            // Note that this sorts in DESCENDING order.
            utils::Sorter<enums::DataUsed::discrete_output>::sort(
                j, _output, _begin, nan_begin );

            const auto critical_values = utils::CriticalValues::calc_discrete(
                enums::DataUsed::discrete_output,
                j,
                _output.df(),  // just as a placeholder
                _output,
                _begin,
                nan_begin,
                &comm() );

            if ( critical_values.size() == 0 ||
                 critical_values.front() == critical_values.back() )
                {
                    continue;
                }

            auto it = _begin;

            auto last_it = _begin;

            for ( auto cv = critical_values.begin();
                  cv != critical_values.end();
                  ++cv )
                {
                    it = utils::Finder<enums::DataUsed::discrete_output>::
                        next_split( *cv, j, _output, it, nan_begin );

                    const auto update =
                        ( cv == critical_values.begin()
                              ? enums::Update::calc_all
                              : enums::Update::calc_diff );

                    add_candidates(
                        enums::Revert::False,
                        update,
                        _old_intercept,
                        containers::Split(
                            j, *cv, enums::DataUsed::discrete_output ),
                        _begin,
                        last_it,
                        it,
                        _end,
                        _candidates );

                    last_it = it;
                }

            add_candidates(
                enums::Revert::False,
                enums::Update::calc_diff,
                _old_intercept,
                containers::Split(
                    j, 0.0, enums::DataUsed::discrete_output_is_nan ),
                _begin,
                last_it,
                nan_begin,
                _end,
                _candidates );

            loss_function().revert_to_commit();
        }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_numerical_input(
    const Float _old_intercept,
    const containers::DataFrame& _input,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _end,
    std::vector<containers::CandidateSplit>* _candidates )
{
    debug_log( "try_numerical_input." );

    for ( size_t j = 0; j < _input.num_numericals(); ++j )
        {
            // Moves all matches for which the critical value is NAN to the
            // very end.
            const auto nan_begin =
                utils::Partitioner<enums::DataUsed::numerical_input_is_nan>::
                    partition( j, _input, _begin, _end );

            // Note that this sorts in DESCENDING order.
            utils::Sorter<enums::DataUsed::numerical_input>::sort(
                j, _input, _begin, nan_begin );

            // Used as placeholder.
            const auto output = containers::DataFrameView(
                _input, std::shared_ptr<const std::vector<size_t>>() );

            const auto critical_values = utils::CriticalValues::calc_numerical(
                enums::DataUsed::numerical_input,
                j,
                _input,
                output,
                _begin,
                nan_begin,
                &comm() );

            if ( critical_values.size() == 0 ||
                 critical_values.front() == critical_values.back() )
                {
                    continue;
                }

            auto it = _begin;

            auto last_it = _begin;

            for ( auto cv = critical_values.begin();
                  cv != critical_values.end();
                  ++cv )
                {
                    it = utils::Finder<enums::DataUsed::numerical_input>::
                        next_split( *cv, j, _input, it, nan_begin );

                    const auto update =
                        ( cv == critical_values.begin()
                              ? enums::Update::calc_all
                              : enums::Update::calc_diff );

                    add_candidates(
                        enums::Revert::False,
                        update,
                        _old_intercept,
                        containers::Split(
                            j, *cv, enums::DataUsed::numerical_input ),
                        _begin,
                        last_it,
                        it,
                        _end,
                        _candidates );

                    last_it = it;
                }

            add_candidates(
                enums::Revert::False,
                enums::Update::calc_diff,
                _old_intercept,
                containers::Split(
                    j, 0.0, enums::DataUsed::numerical_input_is_nan ),
                _begin,
                last_it,
                nan_begin,
                _end,
                _candidates );

            loss_function().revert_to_commit();
        }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_numerical_output(
    const Float _old_intercept,
    const containers::DataFrameView& _output,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _end,
    std::vector<containers::CandidateSplit>* _candidates )
{
    debug_log( "try_numerical_output." );

    for ( size_t j = 0; j < _output.num_numericals(); ++j )
        {
            // Moves all matches for which the critical value is NAN to the
            // very end.
            const auto nan_begin =
                utils::Partitioner<enums::DataUsed::numerical_output_is_nan>::
                    partition( j, _output, _begin, _end );

            // Note that this sorts in DESCENDING order.
            utils::Sorter<enums::DataUsed::numerical_output>::sort(
                j, _output, _begin, nan_begin );

            const auto critical_values = utils::CriticalValues::calc_numerical(
                enums::DataUsed::numerical_output,
                j,
                _output.df(),  // just as a placeholder
                _output,
                _begin,
                nan_begin,
                &comm() );

            if ( critical_values.size() == 0 ||
                 critical_values.front() == critical_values.back() )
                {
                    continue;
                }

            debug_log(
                "critical_values.size(): " +
                std::to_string( critical_values.size() ) );

            auto it = _begin;

            auto last_it = _begin;

            for ( auto cv = critical_values.begin();
                  cv != critical_values.end();
                  ++cv )
                {
                    debug_log( "cv: " + std::to_string( *cv ) );

                    it = utils::Finder<enums::DataUsed::numerical_output>::
                        next_split( *cv, j, _output, it, nan_begin );

                    const auto update =
                        ( cv == critical_values.begin()
                              ? enums::Update::calc_all
                              : enums::Update::calc_diff );

                    add_candidates(
                        enums::Revert::False,
                        update,
                        _old_intercept,
                        containers::Split(
                            j, *cv, enums::DataUsed::numerical_output ),
                        _begin,
                        last_it,
                        it,
                        _end,
                        _candidates );

                    last_it = it;
                }

            add_candidates(
                enums::Revert::False,
                enums::Update::calc_diff,
                _old_intercept,
                containers::Split(
                    j, 0.0, enums::DataUsed::numerical_output_is_nan ),
                _begin,
                last_it,
                nan_begin,
                _end,
                _candidates );

            loss_function().revert_to_commit();
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

                    // Note that this sorts in DESCENDING order.
                    utils::Sorter<enums::DataUsed::same_units_discrete>::sort(
                        input_col,
                        output_col,
                        _input,
                        _output,
                        _begin,
                        nan_begin );

                    const auto critical_values =
                        utils::CriticalValues::calc_discrete(
                            enums::DataUsed::same_units_discrete,
                            input_col,
                            output_col,
                            _input,
                            _output,
                            _begin,
                            nan_begin,
                            &comm() );

                    if ( critical_values.size() == 0 ||
                         critical_values.front() == critical_values.back() )
                        {
                            continue;
                        }

                    auto it = _begin;

                    auto last_it = _begin;

                    for ( auto cv = critical_values.begin();
                          cv != critical_values.end();
                          ++cv )
                        {
                            debug_log( "cv: " + std::to_string( *cv ) );

                            it = utils::Finder<
                                enums::DataUsed::same_units_discrete>::
                                next_split(
                                    *cv,
                                    input_col,
                                    output_col,
                                    _input,
                                    _output,
                                    it,
                                    nan_begin );

                            const auto update =
                                ( cv == critical_values.begin()
                                      ? enums::Update::calc_all
                                      : enums::Update::calc_diff );

                            add_candidates(
                                enums::Revert::False,
                                update,
                                _old_intercept,
                                containers::Split(
                                    output_col,
                                    input_col,
                                    *cv,
                                    enums::DataUsed::same_units_discrete ),
                                _begin,
                                last_it,
                                it,
                                _end,
                                _candidates );

                            last_it = it;
                        }

                    add_candidates(
                        enums::Revert::False,
                        enums::Update::calc_diff,
                        _old_intercept,
                        containers::Split(
                            output_col,
                            input_col,
                            0.0,
                            enums::DataUsed::same_units_discrete_is_nan ),
                        _begin,
                        last_it,
                        nan_begin,
                        _end,
                        _candidates );

                    loss_function().revert_to_commit();
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

                    // Note that this sorts in DESCENDING order.
                    utils::Sorter<enums::DataUsed::same_units_numerical>::sort(
                        input_col,
                        output_col,
                        _input,
                        _output,
                        _begin,
                        nan_begin );

                    const auto critical_values =
                        utils::CriticalValues::calc_numerical(
                            enums::DataUsed::same_units_numerical,
                            input_col,
                            output_col,
                            _input,
                            _output,
                            _begin,
                            nan_begin,
                            &comm() );

                    if ( critical_values.size() == 0 ||
                         critical_values.front() == critical_values.back() )
                        {
                            continue;
                        }

                    auto it = _begin;

                    auto last_it = _begin;

                    for ( auto cv = critical_values.begin();
                          cv != critical_values.end();
                          ++cv )
                        {
                            debug_log( "cv: " + std::to_string( *cv ) );

                            it = utils::Finder<
                                enums::DataUsed::same_units_numerical>::
                                next_split(
                                    *cv,
                                    input_col,
                                    output_col,
                                    _input,
                                    _output,
                                    it,
                                    nan_begin );

                            const auto update =
                                ( cv == critical_values.begin()
                                      ? enums::Update::calc_all
                                      : enums::Update::calc_diff );

                            add_candidates(
                                enums::Revert::False,
                                update,
                                _old_intercept,
                                containers::Split(
                                    output_col,
                                    input_col,
                                    *cv,
                                    enums::DataUsed::same_units_numerical ),
                                _begin,
                                last_it,
                                it,
                                _end,
                                _candidates );

                            last_it = it;
                        }

                    add_candidates(
                        enums::Revert::False,
                        enums::Update::calc_diff,
                        _old_intercept,
                        containers::Split(
                            output_col,
                            input_col,
                            0.0,
                            enums::DataUsed::same_units_numerical_is_nan ),
                        _begin,
                        last_it,
                        nan_begin,
                        _end,
                        _candidates );

                    loss_function().revert_to_commit();
                }
        }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_subfeatures(
    const Float _old_intercept,
    const containers::Subfeatures& _subfeatures,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _end,
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

            // Note that this sorts in DESCENDING order.
            utils::Sorter<enums::DataUsed::subfeatures>::sort(
                j, _subfeatures, _begin, _end );

            const auto critical_values =
                utils::CriticalValues::calc_subfeatures(
                    j, _subfeatures, _begin, _end, &comm() );

            if ( critical_values.size() == 0 ||
                 critical_values.front() == critical_values.back() )
                {
                    continue;
                }

            assert_true( std::all_of(
                critical_values.begin(),
                critical_values.end(),
                []( const Float cv ) {
                    return !std::isnan( cv ) && !std::isinf( cv );
                } ) );

            auto it = _begin;

            auto last_it = _begin;

            for ( auto cv = critical_values.begin();
                  cv != critical_values.end();
                  ++cv )
                {
                    debug_log( "cv:" + std::to_string( *cv ) );

                    it =
                        utils::Finder<enums::DataUsed::subfeatures>::next_split(
                            *cv, j, _subfeatures, it, _end );

                    const auto update =
                        ( cv == critical_values.begin()
                              ? enums::Update::calc_all
                              : enums::Update::calc_diff );

                    add_candidates(
                        enums::Revert::False,
                        update,
                        _old_intercept,
                        containers::Split(
                            j, *cv, enums::DataUsed::subfeatures ),
                        _begin,
                        last_it,
                        it,
                        _end,
                        _candidates );

                    last_it = it;
                }

            loss_function().revert_to_commit();
        }
}
// ----------------------------------------------------------------------------

void DecisionTreeNode::try_time_stamps_diff(
    const Float _old_intercept,
    const containers::DataFrame& _input,
    const containers::DataFrameView& _output,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _end,
    std::vector<containers::CandidateSplit>* _candidates )
{
    debug_log( "Time stamps diff." );

    // Note that this sorts in DESCENDING order.
    utils::Sorter<enums::DataUsed::time_stamps_diff>::sort(
        _input, _output, _begin, _end );

    const auto critical_values = utils::CriticalValues::calc_numerical(
        enums::DataUsed::time_stamps_diff,
        0,
        _input,
        _output,
        _begin,
        _end,
        &comm() );

    if ( critical_values.size() == 0 ||
         critical_values.front() == critical_values.back() )
        {
            return;
        }

    debug_log(
        "critical_values.size(): " + std::to_string( critical_values.size() ) );

    auto it = _begin;

    auto last_it = _begin;

    for ( auto cv = critical_values.begin(); cv != critical_values.end(); ++cv )
        {
            debug_log( "cv: " + std::to_string( *cv ) );

            it = utils::Finder<enums::DataUsed::time_stamps_diff>::next_split(
                *cv, _input, _output, it, _end );

            const auto update =
                ( cv == critical_values.begin() ? enums::Update::calc_all
                                                : enums::Update::calc_diff );

            add_candidates(
                enums::Revert::False,
                update,
                _old_intercept,
                containers::Split( 0, *cv, enums::DataUsed::time_stamps_diff ),
                _begin,
                last_it,
                it,
                _end,
                _candidates );

            last_it = it;
        }

    loss_function().revert_to_commit();

    if ( hyperparameters().delta_t_ > 0.0 )
        {
            try_window(
                _old_intercept, _input, _output, _begin, _end, _candidates );
        }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_window(
    const Float _old_intercept,
    const containers::DataFrame& _input,
    const containers::DataFrameView& _output,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _end,
    std::vector<containers::CandidateSplit>* _candidates )
{
    // ------------------------------------------------------------------------

    debug_log( "Time windows." );

#ifndef NDEBUG

    // Should already be sorted by time stamps.
    const bool is_sorted = std::is_sorted(
        _begin,
        _end,
        [&_input, &_output](
            const containers::Match m1, const containers::Match m2 ) {
            assert_true( m1.ix_input < _input.nrows() );
            assert_true( m2.ix_input < _input.nrows() );

            assert_true( m1.ix_output < _output.nrows() );
            assert_true( m2.ix_output < _output.nrows() );

            return ( _output.time_stamp( m1.ix_output ) -
                     _input.time_stamp( m1.ix_input ) ) >
                   ( _output.time_stamp( m2.ix_output ) -
                     _input.time_stamp( m2.ix_input ) );
        } );

    assert_true( is_sorted );

#endif  // NDEBUG

    // ------------------------------------------------------------------------

    const auto critical_values = utils::CriticalValues::calc_time_window(
        hyperparameters().delta_t_, _input, _output, _begin, _end, &comm() );

    if ( critical_values.size() == 0 ||
         critical_values.front() == critical_values.back() )
        {
            return;
        }

    auto it = _begin;

    auto last_it = _begin;

    for ( auto cv = critical_values.begin(); cv != critical_values.end(); ++cv )
        {
            debug_log( "cv: " + std::to_string( *cv ) );

            it = utils::Finder<enums::DataUsed::time_stamps_diff>::next_split(
                *cv, _input, _output, it, _end );

            const auto update =
                ( cv == critical_values.begin() ? enums::Update::calc_all
                                                : enums::Update::calc_diff );

            add_candidates(
                enums::Revert::True,
                update,
                _old_intercept,
                containers::Split(
                    0, *cv, enums::DataUsed::time_stamps_window ),
                _begin,
                last_it,
                it,
                _end,
                _candidates );

            last_it = it;
        }

    loss_function().revert_to_commit();

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace decisiontrees
}  // namespace relboost
