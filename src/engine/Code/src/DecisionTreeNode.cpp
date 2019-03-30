#include "decisiontrees/decisiontrees.hpp"

namespace relboost
{
namespace decisiontrees
{
// ----------------------------------------------------------------------------

void DecisionTreeNode::add_candidates(
    const enums::Revert _revert,
    const enums::Update _update,
    const RELBOOST_FLOAT _old_intercept,
    const containers::Split& _split,
    const std::vector<const containers::Match*>::iterator _begin,
    const std::vector<const containers::Match*>::iterator _last_it,
    const std::vector<const containers::Match*>::iterator _it,
    const std::vector<const containers::Match*>::iterator _end,
    std::vector<containers::CandidateSplit>* _candidates )
{
    // -----------------------------------------------------------------
    // Calculate weights.

    auto all_new_weights = loss_function().calc_weights(
        _revert, _update, weight_, _begin, _last_it, _it, _end );

    // -----------------------------------------------------------------
    // Calculate and store loss reduction.

    for ( const auto& new_weights : all_new_weights )
        {
            assert( !std::isinf( std::get<0>( new_weights ) ) );
            assert( !std::isinf( std::get<1>( new_weights ) ) );
            assert( !std::isinf( std::get<2>( new_weights ) ) );

            if ( std::isnan( std::get<0>( new_weights ) ) )
                {
                    continue;
                }

            assert(
                !std::isnan( std::get<1>( new_weights ) ) ||
                !std::isnan( std::get<2>( new_weights ) ) );

            auto loss_reduction = loss_function().evaluate_split(
                _old_intercept, weight_, new_weights );

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

void DecisionTreeNode::fit(
    const containers::DataFrame& _output,
    const containers::DataFrame& _input,
    const std::vector<const containers::Match*>::iterator _begin,
    const std::vector<const containers::Match*>::iterator _end,
    RELBOOST_FLOAT* _intercept )
{
    // ------------------------------------------------------------------------
    // Store input and output (we need the column names).

    input_.reset( new containers::DataFrame( _input ) );

    output_.reset( new containers::DataFrame( _output ) );

    // ------------------------------------------------------------------------
    // If the maximum depth is reached or there are no samples to fit, don't
    // bother fitting the node.

    if ( hyperparameters().max_depth_ > 0 &&
         depth_ > hyperparameters().max_depth_ )
        {
            debug_log( "Max depth reached." );
            return;
        }

    if ( std::distance( _begin, _end ) <= 0 )
        {
            debug_log( "No samples left." );
            return;
        }

    // ------------------------------------------------------------------------
    // Try all possible splits.

    const auto candidates =
        try_all( *_intercept, _output, _input, _begin, _end );

    debug_log( "candidates.size(): " + std::to_string( candidates.size() ) );

    if ( candidates.size() == 0 )
        {
            debug_log( "No candidates." );
            return;
        }

    // ------------------------------------------------------------------------
    // Identify best candidate split (the one with the maximum loss reduction)

    const auto cmp = []( const containers::CandidateSplit& c1,
                         const containers::CandidateSplit& c2 ) {
        return c1.loss_reduction_ < c2.loss_reduction_;
    };

    const auto best_split =
        *std::max_element( candidates.begin(), candidates.end(), cmp );

    // ------------------------------------------------------------------------
    // If the loss reduction is sufficient, then take this split.

    debug_log(
        "best_split.loss_reduction_: " +
        std::to_string( best_split.loss_reduction_ ) );

    if ( best_split.loss_reduction_ < hyperparameters().gamma_ )
        {
            return;
        }

    split_ = best_split.split_.deep_copy();

    const auto it_split = partition( _output, _input, _begin, _end );

    loss_function().commit(
        *_intercept, weight_, best_split.weights_, _begin, it_split, _end );

    *_intercept = std::get<0>( best_split.weights_ );

    // ------------------------------------------------------------------------
    // Set up and fit child nodes.

    child_greater_.reset( new DecisionTreeNode(
        condition_maker_,
        depth_ + 1,
        hyperparameters_,
        loss_function_,
        std::get<1>( best_split.weights_ ) ) );

    child_smaller_.reset( new DecisionTreeNode(
        condition_maker_,
        depth_ + 1,
        hyperparameters_,
        loss_function_,
        std::get<2>( best_split.weights_ ) ) );

    child_greater_->fit( _output, _input, _begin, it_split, _intercept );

    child_smaller_->fit( _output, _input, it_split, _end, _intercept );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::vector<const containers::Match*>::iterator DecisionTreeNode::partition(
    const containers::DataFrame& _output,
    const containers::DataFrame& _input,
    const std::vector<const containers::Match*>::iterator _begin,
    const std::vector<const containers::Match*>::iterator _end )
{
    switch ( split_.data_used_ )
        {
            case enums::DataUsed::categorical_input:
                return utils::Partitioner<enums::DataUsed::categorical_input>::
                    partition( split_, _input, _begin, _end );

            case enums::DataUsed::categorical_output:
                return utils::Partitioner<enums::DataUsed::categorical_output>::
                    partition( split_, _output, _begin, _end );

            case enums::DataUsed::discrete_input:
                return utils::Partitioner<enums::DataUsed::discrete_input>::
                    partition( split_, _input, _begin, _end );

            case enums::DataUsed::discrete_input_is_nan:
                return utils::Partitioner<
                    enums::DataUsed::discrete_input_is_nan>::
                    partition( split_.column_, _input, _begin, _end );

            case enums::DataUsed::discrete_output:
                return utils::Partitioner<enums::DataUsed::discrete_output>::
                    partition( split_, _output, _begin, _end );

            case enums::DataUsed::discrete_output_is_nan:
                return utils::Partitioner<
                    enums::DataUsed::discrete_output_is_nan>::
                    partition( split_.column_, _output, _begin, _end );

            case enums::DataUsed::numerical_input:
                return utils::Partitioner<enums::DataUsed::numerical_input>::
                    partition( split_, _input, _begin, _end );

            case enums::DataUsed::numerical_input_is_nan:
                return utils::Partitioner<
                    enums::DataUsed::numerical_input_is_nan>::
                    partition( split_.column_, _input, _begin, _end );

            case enums::DataUsed::numerical_output:
                return utils::Partitioner<enums::DataUsed::numerical_output>::
                    partition( split_, _output, _begin, _end );

            case enums::DataUsed::numerical_output_is_nan:
                return utils::Partitioner<
                    enums::DataUsed::numerical_output_is_nan>::
                    partition( split_.column_, _output, _begin, _end );

            case enums::DataUsed::same_units_categorical:
                return utils::Partitioner<
                    enums::DataUsed::same_units_categorical>::
                    partition( split_, _input, _output, _begin, _end );

            case enums::DataUsed::same_units_discrete:
                return utils::Partitioner<
                    enums::DataUsed::same_units_discrete>::
                    partition( split_, _input, _output, _begin, _end );

            case enums::DataUsed::same_units_discrete_is_nan:
                return utils::Partitioner<
                    enums::DataUsed::same_units_discrete_is_nan>::
                    partition(
                        split_.column_input_,
                        split_.column_,
                        _input,
                        _output,
                        _begin,
                        _end );

            case enums::DataUsed::same_units_numerical:
                return utils::Partitioner<
                    enums::DataUsed::same_units_numerical>::
                    partition( split_, _input, _output, _begin, _end );

            case enums::DataUsed::same_units_numerical_is_nan:
                return utils::Partitioner<
                    enums::DataUsed::same_units_numerical_is_nan>::
                    partition(
                        split_.column_input_,
                        split_.column_,
                        _input,
                        _output,
                        _begin,
                        _end );

            case enums::DataUsed::time_stamps_diff:
                return utils::Partitioner<enums::DataUsed::time_stamps_diff>::
                    partition( split_, _input, _output, _begin, _end );

            default:
                assert( false && "Unknown data_used_" );
                return _begin;
        }
}

// ----------------------------------------------------------------------------

Poco::JSON::Object DecisionTreeNode::to_json() const
{
    Poco::JSON::Object obj;

    if ( !std::isnan( weight_ ) )
        {
            obj.set( "weight_", weight_ );
        }

    if ( child_greater_ )
        {
            assert( child_smaller_ );

            obj.set( "column_", split_.column_ );

            obj.set( "column_input_", split_.column_input_ );

            obj.set(
                "categories_used_",
                JSON::vector_to_array( *split_.categories_used_ ) );

            obj.set( "critical_value_", split_.critical_value_ );

            obj.set( "data_used_", JSON::stringify( split_.data_used_ ) );

            obj.set( "child_greater_", child_greater_->to_json() );

            obj.set( "child_smaller_", child_smaller_->to_json() );
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
            assert( child_smaller_ );

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

RELBOOST_FLOAT DecisionTreeNode::transform(
    const containers::DataFrame& _output,
    const containers::DataFrame& _input,
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

    assert( child_smaller_ );

    bool is_greater = false;

    switch ( split_.data_used_ )
        {
            case enums::DataUsed::categorical_input:
                is_greater =
                    utils::Partitioner<enums::DataUsed::categorical_input>::
                        is_greater( split_, _input, _match );
                break;

            case enums::DataUsed::categorical_output:
                is_greater =
                    utils::Partitioner<enums::DataUsed::categorical_output>::
                        is_greater( split_, _output, _match );
                break;

            case enums::DataUsed::discrete_input:
                is_greater =
                    utils::Partitioner<enums::DataUsed::discrete_input>::
                        is_greater( split_, _input, _match );
                break;

            case enums::DataUsed::discrete_input_is_nan:
                is_greater =
                    utils::Partitioner<enums::DataUsed::discrete_input_is_nan>::
                        is_greater( split_.column_, _input, _match );
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
                is_greater =
                    utils::Partitioner<enums::DataUsed::numerical_input>::
                        is_greater( split_, _input, _match );
                break;

            case enums::DataUsed::numerical_input_is_nan:
                is_greater = utils::Partitioner<
                    enums::DataUsed::numerical_input_is_nan>::
                    is_greater( split_.column_, _input, _match );
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
                is_greater = utils::Partitioner<
                    enums::DataUsed::same_units_categorical>::
                    is_greater( split_, _input, _output, _match );
                break;

            case enums::DataUsed::same_units_discrete:
                is_greater =
                    utils::Partitioner<enums::DataUsed::same_units_discrete>::
                        is_greater( split_, _input, _output, _match );
                break;

            case enums::DataUsed::same_units_discrete_is_nan:
                is_greater = utils::Partitioner<
                    enums::DataUsed::same_units_discrete_is_nan>::
                    is_greater(
                        split_.column_input_,
                        split_.column_,
                        _input,
                        _output,
                        _match );
                break;

            case enums::DataUsed::same_units_numerical:
                is_greater =
                    utils::Partitioner<enums::DataUsed::same_units_numerical>::
                        is_greater( split_, _input, _output, _match );
                break;

            case enums::DataUsed::same_units_numerical_is_nan:
                is_greater = utils::Partitioner<
                    enums::DataUsed::same_units_numerical_is_nan>::
                    is_greater(
                        split_.column_input_,
                        split_.column_,
                        _input,
                        _output,
                        _match );
                break;

            case enums::DataUsed::time_stamps_diff:
                is_greater =
                    utils::Partitioner<enums::DataUsed::time_stamps_diff>::
                        is_greater( split_, _input, _output, _match );
                break;

            default:
                assert( false && "Unknown data_used_" );
        }

    // ------------------------------------------------------------------------
    // Based on the value send problem to one of the child nodes.

    if ( is_greater )
        {
            return child_greater_->transform( _output, _input, _match );
        }
    else
        {
            return child_smaller_->transform( _output, _input, _match );
        }

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::vector<containers::CandidateSplit> DecisionTreeNode::try_all(
    const RELBOOST_FLOAT _old_intercept,
    const containers::DataFrame& _output,
    const containers::DataFrame& _input,
    const std::vector<const containers::Match*>::iterator _begin,
    const std::vector<const containers::Match*>::iterator _end )
{
    std::vector<containers::CandidateSplit> candidates;

    try_categorical_input( _old_intercept, _input, _begin, _end, &candidates );

    try_categorical_output(
        _old_intercept, _output, _begin, _end, &candidates );

    try_discrete_input( _old_intercept, _input, _begin, _end, &candidates );

    try_discrete_output( _old_intercept, _output, _begin, _end, &candidates );

    try_numerical_input( _old_intercept, _input, _begin, _end, &candidates );

    try_numerical_output( _old_intercept, _output, _begin, _end, &candidates );

    try_same_units_categorical(
        _old_intercept, _input, _output, _begin, _end, &candidates );

    try_same_units_discrete(
        _old_intercept, _input, _output, _begin, _end, &candidates );

    try_same_units_numerical(
        _old_intercept, _input, _output, _begin, _end, &candidates );

    try_time_stamps_diff(
        _old_intercept, _input, _output, _begin, _end, &candidates );

    return candidates;
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_categorical(
    const enums::Revert _revert,
    const std::shared_ptr<const std::vector<RELBOOST_INT>> _critical_values,
    const size_t num_column,
    const RELBOOST_FLOAT _old_intercept,
    const enums::DataUsed _data_used,
    const containers::CategoryIndex& _category_index,
    const containers::DataFrame& _input,
    const std::vector<const containers::Match*>::iterator _begin,
    const std::vector<const containers::Match*>::iterator _end,
    std::vector<containers::CandidateSplit>* _candidates )
{
    auto critical_values_begin = _critical_values->begin();

    for ( size_t i = 0; i < _critical_values->size(); ++i )
        {
            const auto cv = ( *_critical_values )[i];

            const auto update =
                ( cv == ( *_critical_values )[0] ? enums::Update::calc_all
                                                 : enums::Update::calc_diff );

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
    const RELBOOST_FLOAT _old_intercept,
    const containers::DataFrame& _input,
    const std::vector<const containers::Match*>::iterator _begin,
    const std::vector<const containers::Match*>::iterator _end,
    std::vector<containers::CandidateSplit>* _candidates )
{
    for ( size_t j = 0; j < _input.categorical_.colnames_.size(); ++j )
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

            const auto critical_values =
                utils::CriticalValues::calc_categorical(
                    enums::DataUsed::categorical_input,
                    j,
                    _input,
                    _input,  // just as a placeholder
                    _begin,
                    _end );

            if ( critical_values->size() <= 1 )
                {
                    continue;
                }

            // ----------------------------------------------------------------
            // Build an index over the categories, so we can find them faster.

            auto category_index = containers::CategoryIndex( _begin, _end );

            category_index.build_indptr<enums::DataUsed::categorical_input>(
                _input.categorical_, j, *critical_values.get() );

            // ----------------------------------------------------------------
            // Try individual categorical values.

            try_categorical(
                enums::Revert::True,
                critical_values,
                j,
                _old_intercept,
                enums::DataUsed::categorical_input,
                category_index,
                _input,
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
                _input,
                _begin,
                _end,
                _candidates );

            // ----------------------------------------------------------------
        }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_categorical_output(
    const RELBOOST_FLOAT _old_intercept,
    const containers::DataFrame& _output,
    const std::vector<const containers::Match*>::iterator _begin,
    const std::vector<const containers::Match*>::iterator _end,
    std::vector<containers::CandidateSplit>* _candidates )
{
    for ( size_t j = 0; j < _output.categorical_.colnames_.size(); ++j )
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
                    _output,  // just as a placeholder
                    _output,
                    _begin,
                    _end );

            if ( critical_values->size() <= 1 )
                {
                    continue;
                }

            // ----------------------------------------------------------------
            // Build an index over the categories, so we can find them faster.

            auto category_index = containers::CategoryIndex( _begin, _end );

            category_index.build_indptr<enums::DataUsed::categorical_output>(
                _output.categorical_, j, *critical_values.get() );

            // ----------------------------------------------------------------
            // Try individual categorical values.

            try_categorical(
                enums::Revert::True,
                critical_values,
                j,
                _old_intercept,
                enums::DataUsed::categorical_output,
                category_index,
                _output,
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
                _output,
                _begin,
                _end,
                _candidates );

            // ----------------------------------------------------------------
        }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_discrete_input(
    const RELBOOST_FLOAT _old_intercept,
    const containers::DataFrame& _input,
    const std::vector<const containers::Match*>::iterator _begin,
    const std::vector<const containers::Match*>::iterator _end,
    std::vector<containers::CandidateSplit>* _candidates )
{
    for ( size_t j = 0; j < _input.discrete_.colnames_.size(); ++j )
        {
            // Moves all matches for which the critical value is NAN to the
            // very end.
            const auto nan_begin =
                utils::Partitioner<enums::DataUsed::discrete_input_is_nan>::
                    partition( j, _input, _begin, _end );

            // Note that this sorts in DESCENDING order.
            utils::Sorter<enums::DataUsed::discrete_input>::sort(
                j, _input, _begin, nan_begin );

            const auto critical_values = utils::CriticalValues::calc_discrete(
                enums::DataUsed::discrete_input,
                j,
                _input,
                _input,  // just as a placeholder
                _begin,
                nan_begin );

            if ( critical_values.size() == 0 ||
                 critical_values.front() == critical_values.back() )
                {
                    continue;
                }

            auto it = _begin;

            auto last_it = _begin;

            for ( auto& cv : critical_values )
                {
                    it = utils::Finder<enums::DataUsed::discrete_input>::
                        next_split( cv, j, _input, it, nan_begin );

                    const auto update =
                        ( cv == critical_values[0] ? enums::Update::calc_all
                                                   : enums::Update::calc_diff );

                    add_candidates(
                        enums::Revert::False,
                        update,
                        _old_intercept,
                        containers::Split(
                            j, cv, enums::DataUsed::discrete_input ),
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
    const RELBOOST_FLOAT _old_intercept,
    const containers::DataFrame& _output,
    const std::vector<const containers::Match*>::iterator _begin,
    const std::vector<const containers::Match*>::iterator _end,
    std::vector<containers::CandidateSplit>* _candidates )
{
    for ( size_t j = 0; j < _output.discrete_.colnames_.size(); ++j )
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
                _output,
                _output,  // just as a placeholder
                _begin,
                nan_begin );

            if ( critical_values.size() == 0 ||
                 critical_values.front() == critical_values.back() )
                {
                    continue;
                }

            auto it = _begin;

            auto last_it = _begin;

            for ( auto& cv : critical_values )
                {
                    it = utils::Finder<enums::DataUsed::discrete_output>::
                        next_split( cv, j, _output, it, nan_begin );

                    const auto update =
                        ( cv == critical_values[0] ? enums::Update::calc_all
                                                   : enums::Update::calc_diff );

                    add_candidates(
                        enums::Revert::False,
                        update,
                        _old_intercept,
                        containers::Split(
                            j, cv, enums::DataUsed::discrete_output ),
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
    const RELBOOST_FLOAT _old_intercept,
    const containers::DataFrame& _input,
    const std::vector<const containers::Match*>::iterator _begin,
    const std::vector<const containers::Match*>::iterator _end,
    std::vector<containers::CandidateSplit>* _candidates )
{
    for ( size_t j = 0; j < _input.numerical_.colnames_.size(); ++j )
        {
            // Moves all matches for which the critical value is NAN to the
            // very end.
            const auto nan_begin =
                utils::Partitioner<enums::DataUsed::numerical_input_is_nan>::
                    partition( j, _input, _begin, _end );

            // Note that this sorts in DESCENDING order.
            utils::Sorter<enums::DataUsed::numerical_input>::sort(
                j, _input, _begin, nan_begin );

            const auto critical_values = utils::CriticalValues::calc_numerical(
                enums::DataUsed::numerical_input,
                j,
                _input,
                _input,  // just as a placeholder
                _begin,
                nan_begin );

            if ( critical_values.size() == 0 ||
                 critical_values.front() == critical_values.back() )
                {
                    continue;
                }

            auto it = _begin;

            auto last_it = _begin;

            for ( auto& cv : critical_values )
                {
                    it = utils::Finder<enums::DataUsed::numerical_input>::
                        next_split( cv, j, _input, it, nan_begin );

                    const auto update =
                        ( cv == critical_values[0] ? enums::Update::calc_all
                                                   : enums::Update::calc_diff );

                    add_candidates(
                        enums::Revert::False,
                        update,
                        _old_intercept,
                        containers::Split(
                            j, cv, enums::DataUsed::numerical_input ),
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
    const RELBOOST_FLOAT _old_intercept,
    const containers::DataFrame& _output,
    const std::vector<const containers::Match*>::iterator _begin,
    const std::vector<const containers::Match*>::iterator _end,
    std::vector<containers::CandidateSplit>* _candidates )
{
    for ( size_t j = 0; j < _output.numerical_.colnames_.size(); ++j )
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
                _output,  // just as a placeholder
                _output,
                _begin,
                nan_begin );

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

            for ( auto& cv : critical_values )
                {
                    debug_log( "cv: " + std::to_string( cv ) );

                    it = utils::Finder<enums::DataUsed::numerical_output>::
                        next_split( cv, j, _output, it, nan_begin );

                    const auto update =
                        ( cv == critical_values[0] ? enums::Update::calc_all
                                                   : enums::Update::calc_diff );

                    add_candidates(
                        enums::Revert::False,
                        update,
                        _old_intercept,
                        containers::Split(
                            j, cv, enums::DataUsed::numerical_output ),
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
    const RELBOOST_FLOAT _old_intercept,
    const containers::DataFrame& _input,
    const containers::DataFrame& _output,
    const std::vector<const containers::Match*>::iterator _begin,
    const std::vector<const containers::Match*>::iterator _end,
    std::vector<containers::CandidateSplit>* _candidates )
{
    assert(
        _input.categorical_.units_.size() ==
        _input.categorical_.colnames_.size() );

    assert(
        _output.categorical_.units_.size() ==
        _output.categorical_.colnames_.size() );

    for ( size_t output_col = 0;
          output_col < _output.categorical_.units_.size();
          ++output_col )
        {
            for ( size_t input_col = 0;
                  input_col < _input.categorical_.units_.size();
                  ++input_col )
                {
                    if ( _output.categorical_.units_[output_col] == "" ||
                         _output.categorical_.units_[output_col] !=
                             _output.categorical_.units_[input_col] )
                        {
                            continue;
                        }

                    const auto partition_function =
                        [input_col, output_col, &_input, &_output](
                            const containers::Match* m ) {
                            assert( m->ix_input < _input.nrows() );
                            assert( m->ix_output < _output.nrows() );

                            const bool is_same =
                                ( _input.categorical_(
                                      m->ix_input, input_col ) ==
                                  _output.categorical_(
                                      m->ix_output, output_col ) );

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
    const RELBOOST_FLOAT _old_intercept,
    const containers::DataFrame& _input,
    const containers::DataFrame& _output,
    const std::vector<const containers::Match*>::iterator _begin,
    const std::vector<const containers::Match*>::iterator _end,
    std::vector<containers::CandidateSplit>* _candidates )
{
    assert(
        _input.discrete_.units_.size() == _input.discrete_.colnames_.size() );

    assert(
        _output.discrete_.units_.size() == _output.discrete_.colnames_.size() );

    for ( size_t output_col = 0; output_col < _output.discrete_.units_.size();
          ++output_col )
        {
            for ( size_t input_col = 0;
                  input_col < _input.discrete_.units_.size();
                  ++input_col )
                {
                    if ( _output.discrete_.units_[output_col] == "" ||
                         _output.discrete_.units_[output_col] !=
                             _output.discrete_.units_[input_col] )
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
                            nan_begin );

                    if ( critical_values.size() == 0 ||
                         critical_values.front() == critical_values.back() )
                        {
                            continue;
                        }

                    auto it = _begin;

                    auto last_it = _begin;

                    for ( auto& cv : critical_values )
                        {
                            debug_log( "cv: " + std::to_string( cv ) );

                            it = utils::Finder<
                                enums::DataUsed::same_units_discrete>::
                                next_split(
                                    cv,
                                    input_col,
                                    output_col,
                                    _input,
                                    _output,
                                    it,
                                    nan_begin );

                            const auto update =
                                ( cv == critical_values[0]
                                      ? enums::Update::calc_all
                                      : enums::Update::calc_diff );

                            add_candidates(
                                enums::Revert::False,
                                update,
                                _old_intercept,
                                containers::Split(
                                    output_col,
                                    input_col,
                                    cv,
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
    const RELBOOST_FLOAT _old_intercept,
    const containers::DataFrame& _input,
    const containers::DataFrame& _output,
    const std::vector<const containers::Match*>::iterator _begin,
    const std::vector<const containers::Match*>::iterator _end,
    std::vector<containers::CandidateSplit>* _candidates )
{
    assert(
        _input.numerical_.units_.size() == _input.numerical_.colnames_.size() );

    assert(
        _output.numerical_.units_.size() ==
        _output.numerical_.colnames_.size() );

    for ( size_t output_col = 0; output_col < _output.numerical_.units_.size();
          ++output_col )
        {
            for ( size_t input_col = 0;
                  input_col < _input.numerical_.units_.size();
                  ++input_col )
                {
                    if ( _output.numerical_.units_[output_col] == "" ||
                         _output.numerical_.units_[output_col] !=
                             _output.numerical_.units_[input_col] )
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
                            nan_begin );

                    if ( critical_values.size() == 0 ||
                         critical_values.front() == critical_values.back() )
                        {
                            continue;
                        }

                    auto it = _begin;

                    auto last_it = _begin;

                    for ( auto& cv : critical_values )
                        {
                            debug_log( "cv: " + std::to_string( cv ) );

                            it = utils::Finder<
                                enums::DataUsed::same_units_numerical>::
                                next_split(
                                    cv,
                                    input_col,
                                    output_col,
                                    _input,
                                    _output,
                                    it,
                                    nan_begin );

                            const auto update =
                                ( cv == critical_values[0]
                                      ? enums::Update::calc_all
                                      : enums::Update::calc_diff );

                            add_candidates(
                                enums::Revert::False,
                                update,
                                _old_intercept,
                                containers::Split(
                                    output_col,
                                    input_col,
                                    cv,
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

void DecisionTreeNode::try_time_stamps_diff(
    const RELBOOST_FLOAT _old_intercept,
    const containers::DataFrame& _input,
    const containers::DataFrame& _output,
    const std::vector<const containers::Match*>::iterator _begin,
    const std::vector<const containers::Match*>::iterator _end,
    std::vector<containers::CandidateSplit>* _candidates )
{
    debug_log(
        "Numerical population, column '" + _output.numerical_.colnames_[j] +
        "'." );

    // Note that this sorts in DESCENDING order.
    utils::Sorter<enums::DataUsed::time_stamps_diff>::sort(
        _input, _output, _begin, _end );

    const auto critical_values = utils::CriticalValues::calc_numerical(
        enums::DataUsed::time_stamps_diff, 0, _input, _output, _begin, _end );

    if ( critical_values.size() == 0 ||
         critical_values.front() == critical_values.back() )
        {
            return;
        }

    debug_log(
        "critical_values.size(): " + std::to_string( critical_values.size() ) );

    auto it = _begin;

    auto last_it = _begin;

    for ( auto& cv : critical_values )
        {
            debug_log( "cv: " + std::to_string( cv ) );

            it = utils::Finder<enums::DataUsed::time_stamps_diff>::next_split(
                cv, _input, _output, it, _end );

            const auto update =
                ( cv == critical_values[0] ? enums::Update::calc_all
                                           : enums::Update::calc_diff );

            add_candidates(
                enums::Revert::False,
                update,
                _old_intercept,
                containers::Split( 0, cv, enums::DataUsed::time_stamps_diff ),
                _begin,
                last_it,
                it,
                _end,
                _candidates );

            last_it = it;
        }

    loss_function().revert_to_commit();
}

// ----------------------------------------------------------------------------
}  // namespace decisiontrees
}  // namespace relboost