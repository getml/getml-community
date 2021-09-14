#ifndef DATABASE_GOUTILSTYPEINFERRER_HPP_
#define DATABASE_GOUTILSTYPEINFERRER_HPP_

namespace database
{
// -----------------------------------------------------------------------------

struct GoutilsTypeInferrer
{
    typedef goutils::Helpers::RecordType::element_type::value_type FieldType;

    static io::Datatype to_datatype( const FieldType& _type_name )
    {
        const auto type_name = std::string( _type_name.get() );

        if ( type_name == "double_precision" )
            {
                return io::Datatype::double_precision;
            }

        if ( type_name == "integer" )
            {
                return io::Datatype::integer;
            }

        if ( type_name == "string" )
            {
                return io::Datatype::string;
            }

        return io::Datatype::unknown;
    }
};

// ----------------------------------------------------------------------------

}  // namespace database

#endif  // DATABASE_GOUTILSTYPEINFERRER_HPP_
