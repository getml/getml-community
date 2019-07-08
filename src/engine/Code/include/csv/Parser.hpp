#ifndef CSV_PARSER_HPP_
#define CSV_PARSER_HPP_

namespace csv
{
// ----------------------------------------------------------------------------

class Parser
{
   public:
    // -------------------------------

    /// Transforms a string to a double.
    static Float to_double( const std::string& _str )
    {
        const auto trimmed = trim( _str );

        if ( trimmed.find_first_not_of( "0123456789.e-+" ) !=
             std::string::npos )
            {
                throw std::runtime_error(
                    "'" + _str + "' could not be converted to double!" );
            }

        return std::stod( trimmed );
    }

    // -------------------------------

    /// Transforms a string to an integer.
    static Int to_int( const std::string& _str )
    {
        const auto trimmed = trim( _str );

        const auto val = std::stoi( trimmed );

        if ( std::to_string( val ) != trimmed )
            {
                throw std::runtime_error(
                    "'" + _str + "' could not be converted to integer!" );
            }

        return val;
    }

    // -------------------------------

    /// Transforms a string to a time stamp.
    static Float to_time_stamp(
        const std::string& _str, const std::vector<std::string>& _time_formats )
    {
        int utc = Poco::DateTimeFormatter::UTC;

        const auto trimmed = trim( _str );

        for ( const auto& fmt : _time_formats )
            {
                const auto date_time =
                    Poco::DateTimeParser::parse( fmt, trimmed, utc );

                //  return static_cast<Float>( date_time.utcTime() ) / ...;

                return static_cast<Float>( date_time.timestamp().epochTime() ) /
                       86400.0;
            }

        throw std::runtime_error(
            "'" + _str + "' could not be converted to a time stamp!" );
    }

    // -------------------------------

    /// Removes all whitespaces at the beginning and end of the string.
    static std::string trim( const std::string& _str )
    {
        const auto pos = _str.find_first_not_of( "\t\v\f\r " );
        const auto len = _str.find_last_not_of( "\t\v\f\r " ) - pos + 1;
        return _str.substr( pos, len );
    }

    // -------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace csv

#endif  // CSV_PARSER_HPP_
