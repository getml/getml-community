#ifndef ENGINE_CONTAINERS_MACROS_HPP_
#define ENGINE_CONTAINERS_MACROS_HPP_

namespace engine
{
namespace containers
{
// -------------------------------------------------------------------------

class Macros
{
   public:
    /// Generates the name of a table.
    static std::string make_table_name(
        const std::string& _join_key,
        const std::string& _other_join_key,
        const std::string& _time_stamp,
        const std::string& _other_time_stamp,
        const std::string& _upper_time_stamp,
        const std::string& _name,
        const std::string& _joined_to );

    /// Removes macros from a vector of column names.
    static std::vector<std::string> modify_colnames(
        const std::vector<std::string>& _names );

    /// Removes macros from column importances.
    static helpers::ImportanceMaker modify_column_importances(
        const helpers::ImportanceMaker& _importance_maker );

    /// Removes macros from generated SQL code.
    static std::string modify_sql( const std::string& _sql );

    /// Splits the name of a joined tabled into all tables that are part of the
    /// join.
    static std::vector<std::string> split_joined_name(
        const std::string& _joined_name );

    /// Extracts all relevant parameters from the output of split_joined_name.
    /// Effectively reverses make_table_name(...).
    static std::tuple<
        std::string,
        std::string,
        std::string,
        std::string,
        std::string,
        std::string,
        std::string>
    parse_table_name( const std::string& _splitted );

   private:
    static std::string get_param(
        const std::string& _splitted, const std::string& _key );

    static std::string make_left_join( const std::string& _splitted );

    static std::string make_subquery( const std::string& _joined_name );

    static std::string remove_many_to_one(
        const std::string& _query,
        const std::string& _key1,
        const std::string& _key2 );

    static std::string remove_time_diff( const std::string& _from_colname );

    /// Replaces all instances of macros from a query or a colunm name.
    static std::string replace( const std::string& _query );

    static std::pair<std::string, std::string> parse_table_colname(
        const std::string& _table, const std::string& _colname );

   public:
    static std::string close_bracket() { return "$GETML_CLOSE_BRACKET"; }

    static std::string column() { return "$GETML_JOIN_PARAM_COLUMN"; }

    static std::string delimiter() { return "$GETML_JOIN_PARAM_DELIMITER"; }

    static std::string end() { return "$GETML_JOIN_PARAM_END"; }

    static std::string generated_ts() { return "$GETML_GENERATED_TS"; }

    static std::string hour() { return "$GETML_HOUR"; }

    static std::string join_key() { return "$GETML_JOIN_PARAM_JOIN_KEY"; }

    static std::string join_param() { return "$GETML_JOIN_PARAM"; }

    static std::string joined_to() { return "$GETML_JOIN_PARAM_JOINED_TO"; }

    static std::string lower_ts() { return "$GETML_LOWER_TS"; }

    static std::string minute() { return "$GETML_MINUTE"; }

    static std::string month() { return "$GETML_MONTH"; }

    static std::string name() { return "$GETML_JOIN_PARAM_NAME"; }

    static std::string no_join_key() { return "$GETML_NO_JOIN_KEY"; }

    static std::string peripheral() { return "$GETML_PERIPHERAL"; }

    static std::string other_join_key()
    {
        return "$GETML_JOIN_PARAM_OTHER_JOIN_KEY";
    }

    static std::string other_time_stamp()
    {
        return "$GETML_JOIN_PARAM_OTHER_TIME_STAMP";
    }

    static std::string remove_char() { return "$GETML_REMOVE_CHAR"; }

    static std::string rowid() { return "$GETML_ROWID"; }

    static std::string rowid_comparison_only()
    {
        return "$GETML_ROWID, comparison only";
    }

    static std::string self_join_key() { return "$GETML_SELF_JOIN_KEY"; }

    static std::string table() { return "$GETML_JOIN_PARAM_TABLE"; }

    static std::string time_stamp() { return "$GETML_JOIN_PARAM_TIME_STAMP"; }

    static std::string upper_time_stamp()
    {
        return "$GETML_JOIN_PARAM_UPPER_TIME_STAMP";
    }

    static std::string upper_ts() { return "$GETML_UPPER_TS"; }

    static std::string weekday() { return "$GETML_WEEKDAY"; }

    static std::string year() { return "$GETML_YEAR"; }
};

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace engine
#endif  //
