#include "decisiontrees/decisiontrees.hpp"

namespace autosql
{
namespace decisiontrees
{
// ------------------------------------------------------------------------

void TablePreparer::append_join_key_and_index(
    const size_t _i,
    const Placeholder &_placeholder_population,
    containers::DataFrameView &_population_table_raw,
    TableHolder &_table_holder )
{
    const std::string &join_key_name =
        _placeholder_population.join_keys_used[_i];

    const auto it = std::find_if(
        _population_table_raw.df().join_keys().begin(),
        _population_table_raw.df().join_keys().end(),
        [join_key_name]( const containers::Matrix<AUTOSQL_INT> &jk ) {
            return jk.colname( 0 ) == join_key_name;
        } );

    if ( it == _population_table_raw.df().join_keys().end() )
        {
            throw std::invalid_argument(
                "DataFrame '" + _population_table_raw.df().name() +
                "' has no join key named '" + join_key_name + "'!" );
        }

    assert(
        _population_table_raw.df().join_keys().size() ==
        _population_table_raw.df().indices().size() );

    auto &index = _population_table_raw.df().index(
        std::distance( _population_table_raw.df().join_keys().begin(), it ) );

    _table_holder.main_table.df().join_keys().push_back( *it );

    _table_holder.main_table.df().indices().push_back( index );

    assert(
        _table_holder.main_table.df().join_keys().size() ==
        _table_holder.main_table.df().indices().size() );
}

// ------------------------------------------------------------------------

void TablePreparer::append_time_stamps(
    const size_t _i,
    const Placeholder &_placeholder_population,
    containers::DataFrameView &_population_table_raw,
    TableHolder &_table_holder )
{
    const std::string &time_stamps_name =
        _placeholder_population.time_stamps_used[_i];

    const auto it = std::find_if(
        _population_table_raw.df().time_stamps_all().begin(),
        _population_table_raw.df().time_stamps_all().end(),
        [time_stamps_name]( const containers::Matrix<AUTOSQL_FLOAT> &ts ) {
            return ts.colname( 0 ) == time_stamps_name;
        } );

    if ( it == _population_table_raw.df().time_stamps_all().end() )
        {
            throw std::invalid_argument(
                "DataFrame '" + _population_table_raw.df().name() +
                "' has no set of time stamps named '" + time_stamps_name +
                "'!" );
        }

    _table_holder.main_table.df().time_stamps_all().push_back( *it );
}

// ------------------------------------------------------------------------

void TablePreparer::check_plausibility(
    const Placeholder &_placeholder_population,
    const std::vector<std::string> &_placeholder_peripheral,
    const std::vector<containers::DataFrame> &_peripheral_tables_raw,
    const containers::DataFrameView &_population_table_raw )
{
    if ( _peripheral_tables_raw.size() != _placeholder_peripheral.size() )
        {
            throw std::invalid_argument(
                "There must be exactly one peripheral table for every "
                "peripheral placeholder (this is the point of having "
                "placeholders...)!" );
        }

    if ( _placeholder_population.joined_tables.size() == 0 )
        {
            throw std::invalid_argument(
                "It appears you have not joined anything to the population "
                "placeholder!" );
        }

    if ( _placeholder_population.joined_tables.size() !=
         _placeholder_population.join_keys_used.size() )
        {
            throw std::invalid_argument(
                "Number of joined tables in placeholders provided does not "
                "match number of join keys in placeholders!" );
        }

    if ( _placeholder_population.joined_tables.size() !=
         _placeholder_population.other_join_keys_used.size() )
        {
            throw std::invalid_argument(
                "Number of joined tables in placeholders provided does not "
                "match number of other join keys used in placeholders!" );
        }

    if ( _placeholder_population.joined_tables.size() !=
         _placeholder_population.other_time_stamps_used.size() )
        {
            throw std::invalid_argument(
                "Number of joined tables in placeholders provided does not "
                "match number of other time stamps used in placeholders!" );
        }

    if ( _placeholder_population.joined_tables.size() !=
         _placeholder_population.time_stamps_used.size() )
        {
            throw std::invalid_argument(
                "Number of joined tables in placeholders provided does not "
                "match number of time stamps used in placeholders!" );
        }

    if ( _placeholder_population.joined_tables.size() !=
         _placeholder_population.upper_time_stamps_used.size() )
        {
            throw std::invalid_argument(
                "Number of joined tables in placeholders provided does not "
                "match number of upper time stamps used in placeholders!" );
        }

    _population_table_raw.df().check_plausibility();

    for ( auto &per : _peripheral_tables_raw )
        {
            per.check_plausibility();
        }
}

// ------------------------------------------------------------------------

AUTOSQL_INT TablePreparer::identify_peripheral(
    const size_t _i,
    const Placeholder &_placeholder_population,
    const std::vector<std::string> &_placeholder_peripheral )
{
    const std::string &table_name =
        _placeholder_population.joined_tables[_i].name;

    auto it = find_if(
        _placeholder_peripheral.begin(),
        _placeholder_peripheral.end(),
        [table_name]( const std::string &name ) {
            return name == table_name;
        } );

    if ( it == _placeholder_peripheral.end() )
        {
            throw std::invalid_argument(
                "'" + table_name + "' not among placeholder tables!" );
        }

    return static_cast<AUTOSQL_INT>(
        std::distance( _placeholder_peripheral.begin(), it ) );
}

// ------------------------------------------------------------------------

void TablePreparer::prepare_children(
    const Placeholder &_placeholder_population,
    const std::vector<std::string> &_placeholder_peripheral,
    std::vector<containers::DataFrame> &_peripheral_tables_raw,
    TableHolder &_table_holder )
{
    assert(
        _table_holder.peripheral_tables.size() ==
        _table_holder.subtables.size() );

    assert(
        _table_holder.peripheral_tables.size() ==
        _placeholder_population.joined_tables.size() );

    for ( size_t i = 0; i < _table_holder.peripheral_tables.size(); ++i )
        {
            const auto &placeholder = _placeholder_population.joined_tables[i];

            if ( placeholder.joined_tables.size() > 0 )
                {
                    _table_holder.subtables[i].reset( new TableHolder );

                    auto view = containers::DataFrameView(
                        _table_holder.peripheral_tables[i] );

                    *_table_holder.subtables[i] = TablePreparer::prepare_tables(
                        placeholder,
                        _placeholder_peripheral,
                        _peripheral_tables_raw,
                        view );
                }
        }
}

// ------------------------------------------------------------------------

TableHolder TablePreparer::prepare_tables(
    const Placeholder &_placeholder_population,
    const std::vector<std::string> &_placeholder_peripheral,
    std::vector<containers::DataFrame> &_peripheral_tables_raw,
    containers::DataFrameView &_population_table_raw )
{
    // ----------------------------------------------------------------------

    TableHolder table_holder( _placeholder_population.joined_tables.size() );

    // ----------------------------------------------------------------------

    debug_message( "Preparing tables..." );

    // ----------------------------------------------------------------------

    check_plausibility(
        _placeholder_population,
        _placeholder_peripheral,
        _peripheral_tables_raw,
        _population_table_raw );

    // ----------------------------------------------------------------------
    // Copy data from the raw population table, rename and then clear the join
    // keys. Recall that for the main table, we will never actually
    // use the sample containers, so we don't really have to think about
    // them too much.

    table_holder.main_table = _population_table_raw;

    table_holder.main_table.df().name() = _placeholder_population.name;

    table_holder.main_table.df().join_keys().clear();

    table_holder.main_table.df().indices().clear();

    table_holder.main_table.df().time_stamps_all().clear();

    // ----------------------------------------------------------------------

    for ( size_t i = 0; i < _placeholder_population.joined_tables.size(); ++i )
        {
            // --------------------------------------------------------------
            // Identify the correct peripheral table to use

            const auto dist = identify_peripheral(
                i, _placeholder_population, _placeholder_peripheral );

            table_holder.peripheral_tables[i] = _peripheral_tables_raw[dist];

            table_holder.peripheral_tables[i].name() =
                _placeholder_population.joined_tables[i].name;

            // ---------------------------------------------------------------
            // Set join_key_used_ in the peripheral table, so we know which
            // join key to use in the future

            set_join_key_used(
                i,
                dist,
                _placeholder_population,
                _peripheral_tables_raw,
                table_holder );

            // ---------------------------------------------------------------
            // Set time_stamps_used_ in the peripheral table, so we know which
            // set of time stamps to use in the future

            set_time_stamps_used(
                i,
                dist,
                _placeholder_population,
                _peripheral_tables_raw,
                table_holder );

            // ---------------------------------------------------------------
            // Set upper_time_stamps_ in the peripheral table, so we know which
            // set of time stamps to use for the upper bound, if any

            set_upper_time_stamps(
                i,
                dist,
                _placeholder_population,
                _peripheral_tables_raw,
                table_holder );

            // ---------------------------------------------------------------
            // Append the correct join key to the main table

            append_join_key_and_index(
                i,
                _placeholder_population,
                _population_table_raw,
                table_holder );

            // ---------------------------------------------------------------
            // Append the correct set of time stamps to the main table

            append_time_stamps(
                i,
                _placeholder_population,
                _population_table_raw,
                table_holder );

            // ---------------------------------------------------------------
        }

    // ----------------------------------------------------------------------
    // We set the join key used to -1. If anyone ever tries to get the
    // sample containers maps from the population table, there will be an
    // assertion failure, unless a proper join key is set.

    table_holder.main_table.df().set_join_key_used( -1 );

    table_holder.main_table.df().set_time_stamps_used( -1 );

    // ----------------------------------------------------------------------
    // Now, prepare the children of the placeholder, if there are any

    prepare_children(
        _placeholder_population,
        _placeholder_peripheral,
        _peripheral_tables_raw,
        table_holder );

    // ----------------------------------------------------------------------

    return table_holder;

    // ----------------------------------------------------------------------
}

// ------------------------------------------------------------------------

void TablePreparer::set_join_key_used(
    const AUTOSQL_SIZE _i,
    const AUTOSQL_INT _dist,
    const Placeholder &_placeholder_population,
    const std::vector<containers::DataFrame> &_peripheral_tables_raw,
    TableHolder &_table_holder )
{
    const std::string &other_join_key_name =
        _placeholder_population.other_join_keys_used[_i];

    const auto it = std::find_if(
        _table_holder.peripheral_tables[_i].join_keys().begin(),
        _table_holder.peripheral_tables[_i].join_keys().end(),
        [other_join_key_name]( containers::Matrix<AUTOSQL_INT> &jk ) {
            return jk.colname( 0 ) == other_join_key_name;
        } );

    if ( it == _table_holder.peripheral_tables[_i].join_keys().end() )
        {
            throw std::invalid_argument(
                "DataFrame '" + _peripheral_tables_raw[_dist].name() +
                "' has no join key named '" + other_join_key_name + "'!" );
        }

    const auto dist = std::distance(
        _table_holder.peripheral_tables[_i].join_keys().begin(), it );

    _table_holder.peripheral_tables[_i].set_join_key_used( dist );
}

// ------------------------------------------------------------------------

void TablePreparer::set_time_stamps_used(
    const size_t _i,
    const AUTOSQL_INT _dist,
    const Placeholder &_placeholder_population,
    const std::vector<containers::DataFrame> &_peripheral_tables_raw,
    TableHolder &_table_holder )
{
    const std::string &other_time_stamps_name =
        _placeholder_population.other_time_stamps_used[_i];

    const auto it = std::find_if(
        _table_holder.peripheral_tables[_i].time_stamps_all().begin(),
        _table_holder.peripheral_tables[_i].time_stamps_all().end(),
        [other_time_stamps_name]( containers::Matrix<AUTOSQL_FLOAT> &ts ) {
            return ts.colname( 0 ) == other_time_stamps_name;
        } );

    if ( it == _table_holder.peripheral_tables[_i].time_stamps_all().end() )
        {
            throw std::invalid_argument(
                "DataFrame '" + _peripheral_tables_raw[_dist].name() +
                "' has no set of time stamps named '" + other_time_stamps_name +
                "'!" );
        }

    const auto dist = std::distance(
        _table_holder.peripheral_tables[_i].time_stamps_all().begin(), it );

    _table_holder.peripheral_tables[_i].set_time_stamps_used( dist );
}

// ------------------------------------------------------------------------

void TablePreparer::set_upper_time_stamps(
    const size_t _i,
    const AUTOSQL_INT _dist,
    const Placeholder &_placeholder_population,
    const std::vector<containers::DataFrame> &_peripheral_tables_raw,
    TableHolder &_table_holder )
{
    const std::string &upper_time_stamps_name =
        _placeholder_population.upper_time_stamps_used[_i];

    if ( upper_time_stamps_name == "" )
        {
            _table_holder.peripheral_tables[_i].set_upper_time_stamps( -1 );
            return;
        }

    const auto it = std::find_if(
        _table_holder.peripheral_tables[_i].time_stamps_all().begin(),
        _table_holder.peripheral_tables[_i].time_stamps_all().end(),
        [upper_time_stamps_name]( containers::Matrix<AUTOSQL_FLOAT> &ts ) {
            return ts.colname( 0 ) == upper_time_stamps_name;
        } );

    if ( it == _table_holder.peripheral_tables[_i].time_stamps_all().end() )
        {
            throw std::invalid_argument(
                "DataFrame '" + _peripheral_tables_raw[_dist].name() +
                "' has no set of time stamps named '" + upper_time_stamps_name +
                "'!" );
        }

    const auto dist = std::distance(
        _table_holder.peripheral_tables[_i].time_stamps_all().begin(), it );

    _table_holder.peripheral_tables[_i].set_upper_time_stamps( dist );
}

// ------------------------------------------------------------------------
}  // namespace decisiontrees
}  // namespace autosql
