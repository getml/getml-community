#include "relboost/ensemble/ensemble.hpp"

namespace relboost
{
namespace ensemble
{
// ----------------------------------------------------------------------------

RELBOOST_FLOAT DecisionTreeEnsemble::calc_loss_reduction(
    const decisiontrees::DecisionTree &_decision_tree,
    const std::vector<RELBOOST_FLOAT> &_yhat_old,
    const std::vector<RELBOOST_FLOAT> &_predictions ) const
{
    assert( _yhat_old.size() == targets().size() );
    assert( _predictions.size() == targets().size() );

    std::vector<RELBOOST_FLOAT> yhat_new( _yhat_old.size() );

    const auto update_rate = _decision_tree.update_rate();

    const auto update_function = [update_rate](
                                     const RELBOOST_FLOAT y_old,
                                     const RELBOOST_FLOAT y_new ) {
        return y_old + y_new * update_rate;
    };

    std::transform(
        _yhat_old.begin(),
        _yhat_old.end(),
        _predictions.begin(),
        yhat_new.begin(),
        update_function );

    return loss_function_->evaluate_tree( yhat_new );
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::fit(
    const containers::DataFrame &_population,
    const std::vector<containers::DataFrame> &_peripheral )
{
    // ------------------------------------------------------------------------
    // Prepare targets.

    if ( _population.target_.colnames_.size() != 1 )
        {
            throw std::runtime_error(
                "The population table needs to define exactly one target!" );
        }

    targets().resize( _population.nrows() );

    std::copy(
        _population.target_.data_,
        _population.target_.data_ + _population.nrows(),
        targets().begin() );

    // ------------------------------------------------------------------------
    // Calculate initial prediction.

    initial_prediction() = std::accumulate(
        targets().begin(), targets().end(), 0.0, std::plus<RELBOOST_FLOAT>() );

    initial_prediction() /= static_cast<RELBOOST_FLOAT>( targets().size() );

    // ------------------------------------------------------------------------
    // Calculate gradient.

    auto yhat_old = std::make_shared<std::vector<RELBOOST_FLOAT>>(
        _population.nrows(), initial_prediction() );

    loss_function().calc_gradients( yhat_old );

    // ------------------------------------------------------------------------
    // Build table holder.

    const auto table_holder = TableHolder(
        placeholder(), _population, _peripheral, peripheral_names() );

    // ------------------------------------------------------------------------
    // Do the actual fitting.

    for ( size_t i = 0; i < hyperparameters().n_iter_; ++i )
        {
            trees().push_back(
                fit_new_tree( loss_function_, table_holder, yhat_old.get() ) );

            loss_function().calc_gradients( yhat_old );

            loss_function().commit();
        }

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

decisiontrees::DecisionTree DecisionTreeEnsemble::fit_new_tree(
    const std::shared_ptr<lossfunctions::LossFunction> &_loss_function,
    const TableHolder &_table_holder,
    std::vector<RELBOOST_FLOAT> *_yhat_old )
{
    // ------------------------------------------------------------------------

    assert(
        _table_holder.main_tables_.size() ==
        _table_holder.peripheral_tables_.size() );

    assert( _table_holder.main_tables_.size() > 0 );

    // ------------------------------------------------------------------------
    // Create new sample weights.

    const auto sample_weights =
        sampler().make_sample_weights( _table_holder.main_tables_[0].nrows() );

    // ------------------------------------------------------------------------
    // Recalculate the sums.

    loss_function().calc_sums( sample_weights );

    loss_function().commit();

    // ------------------------------------------------------------------------

    std::vector<decisiontrees::DecisionTree> candidates;

    std::vector<RELBOOST_FLOAT> loss;

    std::vector<std::vector<RELBOOST_FLOAT>> predictions;

    for ( size_t ix_table_used = 0;
          ix_table_used < _table_holder.main_tables_.size();
          ++ix_table_used )
        {
            // ------------------------------------------------------------------------

            const auto &output_table =
                _table_holder.main_tables_[ix_table_used];

            const auto &input_table =
                _table_holder.peripheral_tables_[ix_table_used];

            // ------------------------------------------------------------------------

            assert( output_table.nrows() == sample_weights->size() );

            // ------------------------------------------------------------------------
            // Matches can potentially use a lot of memory - better to create
            // them anew when needed.

            const auto matches = utils::Matchmaker::make_matches(
                output_table,
                input_table,
                sample_weights,
                hyperparameters().use_timestamps_ );

            auto matches_ptr = utils::Matchmaker::make_pointers( matches );

            assert( matches.size() == matches_ptr.size() );

            debug_log(
                "Number of matches: " + std::to_string( matches.size() ) );

            // ------------------------------------------------------------------------
            // Build aggregations.

            std::vector<std::shared_ptr<lossfunctions::LossFunction>>
                aggregations;

            aggregations.push_back( std::make_shared<aggregations::Avg>(
                _loss_function, matches_ptr, input_table, output_table ) );

            aggregations.push_back( std::make_shared<aggregations::Sum>(
                _loss_function, input_table, output_table ) );

            // ------------------------------------------------------------------------
            // Iterate through aggregations.

            for ( auto &agg : aggregations )
                {
                    candidates.push_back( decisiontrees::DecisionTree(
                        impl().encoding_,
                        impl().hyperparameters_,
                        agg,
                        ix_table_used ) );

                    candidates.back().fit(
                        output_table,
                        input_table,
                        matches_ptr.begin(),
                        matches_ptr.end() );

                    const auto new_predictions = candidates.back().transform(
                        output_table, input_table );

                    candidates.back().calc_update_rate(
                        *_yhat_old, new_predictions );

                    auto loss_reduction = calc_loss_reduction(
                        candidates.back(), *_yhat_old, new_predictions );

                    loss.push_back( loss_reduction );

                    predictions.emplace_back( std::move( new_predictions ) );
                }

            // ------------------------------------------------------------------------
        }

    // ------------------------------------------------------------------------
    // Find best candidate

    assert( loss.size() == candidates.size() );
    assert( predictions.size() == candidates.size() );

    const auto it = std::min_element( loss.begin(), loss.end() );

    const auto dist = std::distance( loss.begin(), it );

    const auto &best_predictions = predictions[dist];

    // ------------------------------------------------------------------------
    // Update _yhat_old.

    update_predictions(
        candidates[dist].update_rate(), best_predictions, _yhat_old );

    // ------------------------------------------------------------------------

    return candidates[dist];
}

// ----------------------------------------------------------------------------

std::vector<RELBOOST_FLOAT> DecisionTreeEnsemble::generate_predictions(
    const decisiontrees::DecisionTree &_decision_tree,
    const TableHolder &_table_holder ) const
{
    const auto peripheral_used = _decision_tree.peripheral_used();

    return _decision_tree.transform(
        _table_holder.main_tables_[peripheral_used],
        _table_holder.peripheral_tables_[peripheral_used] );
}

// ----------------------------------------------------------------------------

DecisionTreeEnsemble &DecisionTreeEnsemble::operator=(
    const DecisionTreeEnsemble &_other )
{
    debug_log( "DecisionTreeEnsemble: Copy assignment constructor..." );

    DecisionTreeEnsemble temp( _other );

    *this = std::move( temp );

    return *this;
}

// ----------------------------------------------------------------------------

DecisionTreeEnsemble &DecisionTreeEnsemble::operator=(
    DecisionTreeEnsemble &&_other ) noexcept
{
    debug_log( "DecisionTreeEnsemble: Move assignment constructor..." );

    if ( this == &_other )
        {
            return *this;
        }

    impl_ = std::move( _other.impl_ );

    targets_ = _other.targets_;

    loss_function_ = lossfunctions::LossFunctionParser::parse(
        _other.loss_function().type(), impl().hyperparameters_, targets_ );

    return *this;
}

// ----------------------------------------------------------------------------

std::vector<RELBOOST_FLOAT> DecisionTreeEnsemble::predict(
    const containers::DataFrame &_population,
    const std::vector<containers::DataFrame> &_peripheral ) const
{
    // ------------------------------------------------------------------------

    if ( trees().size() == 0 )
        {
            throw std::runtime_error( "Relboost model has not been fitted!" );
        }

    // ------------------------------------------------------------------------
    // Build table holder.

    const auto table_holder = TableHolder(
        placeholder(), _population, _peripheral, peripheral_names() );

    // ------------------------------------------------------------------------
    // Init yhat.

    std::vector<RELBOOST_FLOAT> yhat( _population.nrows() );

    std::fill( yhat.begin(), yhat.end(), initial_prediction() );

    // ------------------------------------------------------------------------
    // Generate prediction.

    for ( const auto &decision_tree : trees() )
        {
            const auto predictions =
                generate_predictions( decision_tree, table_holder );

            update_predictions(
                decision_tree.update_rate(), predictions, &yhat );
        }

    // ------------------------------------------------------------------------

    return yhat;
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::save( const std::string &_fname ) const
{
    std::ofstream fs( _fname, std::ofstream::out );

    Poco::JSON::Stringifier::stringify( to_json(), fs );

    fs.close();
}

// ----------------------------------------------------------------------------

std::vector<RELBOOST_FLOAT> DecisionTreeEnsemble::transform(
    const containers::DataFrame &_population,
    const std::vector<containers::DataFrame> &_peripheral ) const
{
    // ------------------------------------------------------------------------

    if ( trees().size() == 0 )
        {
            throw std::runtime_error( "Relboost model has not been fitted!" );
        }

    // ------------------------------------------------------------------------
    // Build table holder.

    const auto table_holder = TableHolder(
        placeholder(), _population, _peripheral, peripheral_names() );

    // ------------------------------------------------------------------------
    // Init features.

    const auto nrows = static_cast<uint64_t>( _population.nrows() );

    const auto ncols = static_cast<uint64_t>( trees().size() );

    std::vector<RELBOOST_FLOAT> features( nrows * ncols );

    // ------------------------------------------------------------------------
    // Generate new features.

    for ( uint64_t j = 0; j < ncols; ++j )
        {
            const auto predictions =
                generate_predictions( trees()[j], table_holder );

            assert( predictions.size() == _population.nrows() );

            for ( uint64_t i = 0; i < nrows; ++i )
                {
                    features[i * ncols + j] = predictions[i];
                }
        }

    // ------------------------------------------------------------------------

    return features;
}

// ----------------------------------------------------------------------------

Poco::JSON::Object DecisionTreeEnsemble::to_json() const
{
    // ------------------------------------------------------------------------

    Poco::JSON::Object obj;

    // ------------------------------------------------------------------------

    obj.set( "hyperparameters_", hyperparameters().to_json_obj() );

    obj.set( "initial_prediction_", initial_prediction() );

    // ------------------------------------------------------------------------

    Poco::JSON::Array arr;

    for ( auto &tree : trees() )
        {
            arr.add( tree.to_json() );
        }

    obj.set( "trees_", arr );

    // ------------------------------------------------------------------------

    return obj;
}

// ----------------------------------------------------------------------------

std::string DecisionTreeEnsemble::to_sql() const
{
    std::stringstream sql;

    for ( size_t i = 0; i < trees().size(); ++i )
        {
            sql << trees()[i].to_sql(
                std::to_string( i + 1 ), hyperparameters().use_timestamps_ );
        }

    return sql.str();
}

// ----------------------------------------------------------------------------
}  // namespace ensemble
}  // namespace relboost
