#ifndef ENGINE_UTILS_TIME_HPP_
#define ENGINE_UTILS_TIME_HPP_

namespace engine
{
namespace utils
{
// ----------------------------------------------------------------------------

class Time
{
   public:
    /// Extracts the day of the month out of a time stamp.
    static Float day( const Float _val )
    {
        if ( std::isnan( _val ) || std::isinf( _val ) )
            {
                return static_cast<Float>( NAN );
            }
        return static_cast<Float>( to_time_stamp( _val ).day() );
    };

    /// Extracts the hour of the day out of a time stamp.
    static Float hour( const Float _val )
    {
        if ( std::isnan( _val ) || std::isinf( _val ) )
            {
                return static_cast<Float>( NAN );
            }
        return static_cast<Float>( to_time_stamp( _val ).hour() );
    };

    /// Extracts the minute of the hour out of a time stamp.
    static Float minute( const Float _val )
    {
        if ( std::isnan( _val ) || std::isinf( _val ) )
            {
                return static_cast<Float>( NAN );
            }
        return static_cast<Float>( to_time_stamp( _val ).minute() );
    };

    /// Extracts the month of the year out of a time stamp.
    static Float month( const Float _val )
    {
        if ( std::isnan( _val ) || std::isinf( _val ) )
            {
                return static_cast<Float>( NAN );
            }
        return static_cast<Float>( to_time_stamp( _val ).month() );
    };

    /// Extracts the second of the minute out of a time stamp.
    static Float second( const Float _val )
    {
        if ( std::isnan( _val ) || std::isinf( _val ) )
            {
                return static_cast<Float>( NAN );
            }
        return static_cast<Float>( to_time_stamp( _val ).second() );
    };

    /// Extracts the day of the week out of a time stamp.
    static Float weekday( const Float _val )
    {
        if ( std::isnan( _val ) || std::isinf( _val ) )
            {
                return static_cast<Float>( NAN );
            }
        return static_cast<Float>( to_time_stamp( _val ).dayOfWeek() );
    };

    /// Extracts the year out of a time stamp.
    static Float year( const Float _val )
    {
        if ( std::isnan( _val ) || std::isinf( _val ) )
            {
                return static_cast<Float>( NAN );
            }
        return static_cast<Float>( to_time_stamp( _val ).year() );
    };

    /// Extracts the day of the year out of a time stamp.
    static Float yearday( const Float _val )
    {
        if ( std::isnan( _val ) || std::isinf( _val ) )
            {
                return static_cast<Float>( NAN );
            }
        return static_cast<Float>( to_time_stamp( _val ).dayOfYear() );
    };

   private:
    /// Transforms a Float into a Poco::DateTime object.
    static Poco::DateTime to_time_stamp( const Float _val )
    {
        const auto seconds_since_epoch = static_cast<std::time_t>( _val );

        return Poco::DateTime(
            Poco::Timestamp::fromEpochTime( seconds_since_epoch ) );
    }
};

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace engine

#endif  // ENGINE_UTILS_TIME_HPP_
