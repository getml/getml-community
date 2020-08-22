#ifndef HELPERS_COLUMNDESCRIPTION_HPP_
#define HELPERS_COLUMNDESCRIPTION_HPP_

namespace helpers
{
// -------------------------------------------------------------------------

struct ColumnDescription
{
    static constexpr const char* PERIPHERAL = "[PERIPHERAL]";
    static constexpr const char* POPULATION = "[POPULATION]";

    ColumnDescription(
        const std::string& _marker,
        const std::string& _table,
        const std::string& _name )
        : marker_( _marker ), name_( _name ), table_( _table )
    {
    }

    ColumnDescription( const Poco::JSON::Object& _obj )
        : ColumnDescription(
              jsonutils::JSON::get_value<std::string>( _obj, "marker_" ),
              jsonutils::JSON::get_value<std::string>( _obj, "table_" ),
              jsonutils::JSON::get_value<std::string>( _obj, "name_" ) )
    {
    }

    ~ColumnDescription() = default;

    // ---------------------------------------------------------------------

    /// Generates the full name from the description.
    std::string name() const { return marker_ + " " + table_ + "." + name_; }

    /// Equal to operator
    bool operator==( const ColumnDescription& _other ) const
    {
        return marker_ == _other.marker_ && name_ == _other.name_ &&
               table_ == _other.table_;
    }

    /// Less than operator
    bool operator<( const ColumnDescription& _other ) const
    {
        if ( marker_ != _other.marker_ )
            {
                return ( marker_ < _other.marker_ );
            }

        if ( table_ != _other.table_ )
            {
                return ( table_ < _other.table_ );
            }

        if ( name_ != _other.name_ )
            {
                return ( name_ < _other.name_ );
            }

        return false;
    }

    /// Expresses the column description as a JSON object.
    Poco::JSON::Object::Ptr to_json_obj() const
    {
        auto obj = Poco::JSON::Object::Ptr( new Poco::JSON::Object() );
        obj->set( "marker_", marker_ );
        obj->set( "name_", name_ );
        obj->set( "table_", table_ );
        return obj;
    }

    // ---------------------------------------------------------------------

    /// [POPULATION] or [PERIPHERAL]
    const std::string marker_;

    /// The name of the column.
    const std::string name_;

    /// The name of the table.
    const std::string table_;
};

// -------------------------------------------------------------------------
}  // namespace helpers

#endif  // HELPERS_COLUMNDESCRIPTION_HPP_
