;
#ifndef IO_PARSER_HPP_
#define IO_PARSER_HPP_

namespace io
{
// ----------------------------------------------------------------------------

class Parser
{
   public:
    // -------------------------------

    /// Removes the quotechars.
    static std::string remove_quotechars(
        const std::string& _str, const char _quotechar )
    {
        auto field = _str;

        field.erase(
            std::remove( field.begin(), field.end(), _quotechar ),
            field.end() );

        return field;
    }

    // -------------------------------

    /// Transforms a string to a double.
    static std::pair<Float, bool> to_double( const std::string& _str )
    {
        try
            {
                const auto trimmed = trim( _str );

                if ( trimmed.find_first_not_of( "0123456789.e-+" ) !=
                     std::string::npos )
                    {
                        return std::pair<Float, bool>( 0.0, false );
                    }

                return std::pair<Float, bool>( std::stod( trimmed ), true );
            }
        catch ( std::exception& e )
            {
                return std::pair<Float, bool>( 0.0, false );
            }
    }

    // -------------------------------

    /// Transforms a string to an integer.
    static std::pair<Int, bool> to_int( const std::string& _str )
    {
        try
            {
                const auto trimmed = trim( _str );

                const auto val = std::stoi( trimmed );

                if ( std::to_string( val ) != trimmed )
                    {
                        return std::pair<Int, bool>( 0, false );
                    }

                return std::pair<Int, bool>( val, true );
            }
        catch ( std::exception& e )
            {
                return std::pair<Int, bool>( 0, false );
            }
    }

    // -------------------------------

    /// Transforms a string to a time stamp.
    static std::pair<Float, bool> to_time_stamp(
        const std::string& _str, const std::vector<std::string>& _time_formats )
    {
        const auto trimmed = trim( _str );

        int utc = Poco::DateTimeFormatter::UTC;

        for ( const auto& fmt : _time_formats )
            {
                Poco::DateTime date_time;

                const auto success = Poco::DateTimeParser::tryParse(
                    fmt, trimmed, date_time, utc );

                const auto time_stamp = date_time.timestamp();

                if ( !success || Poco::DateTimeFormatter::format(
                                     time_stamp, fmt ) != trimmed )
                    {
                        continue;
                    }

                return std::pair<Float, bool>(
                    static_cast<Float>( time_stamp.epochMicroseconds() ) /
                        1.0e6,
                    true );
            }

        return std::pair<Float, bool>( 0.0, false );
    }

    // -------------------------------

    /// Removes all whitespaces at the beginning and end of the string.
    static std::string trim( const std::string& _str )
    {
        const auto pos = _str.find_first_not_of( "\t\v\f\r\n " );

        if ( pos == std::string::npos )
            {
                return "";
            }

        const auto len = _str.find_last_not_of( "\t\v\f\r\n " ) - pos + 1;

        return _str.substr( pos, len );
    }

    // -------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace io

#endif  // IO_PARSER_HPP_
