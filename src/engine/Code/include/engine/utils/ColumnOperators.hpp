#ifndef ENGINE_UTILS_COLUMNOPERATORS_HPP_
#define ENGINE_UTILS_COLUMNOPERATORS_HPP_

namespace engine
{
namespace utils
{
// ------------------------------------------------------------------------

class ColumnOperators
{
    // ------------------------------------------------------------------------

   public:
    /// Throws an exception if not all values are the same.
    template <class IteratorType>
    static Float assert_equal( IteratorType _begin, IteratorType _end )
    {
        if ( std::distance( _begin, _end ) <= 0 )
            {
                throw std::runtime_error( "Column cannot be of length 0." );
            }

        const auto assert_equal = []( const Float init, const Float val ) {
            if ( init != val )
                {
                    throw std::runtime_error(
                        "Values not equal: " + std::to_string( init ) +
                        " vs. " + std::to_string( val ) + "." );
                }

            return init;
        };

        return num_agg( _begin, _end, assert_equal, *_begin );
    }

    /// Takes the average of all non-null entries.
    template <class IteratorType>
    static Float avg( IteratorType _begin, IteratorType _end )
    {
        const auto numerator = sum( _begin, _end );

        const auto divisor = count( _begin, _end );

        return numerator / divisor;
    }

    /// Counts all non-null entries.
    template <class IteratorType>
    static Float count( IteratorType _begin, IteratorType _end )
    {
        const auto count = []( const Float init, const Float val ) {
            if ( std::isnan( val ) )
                {
                    return init;
                }
            else
                {
                    return init + 1.0;
                }
        };

        return num_agg( _begin, _end, count, 0.0 );
    }

    /// Counts the non-null number of entries.
    static Float count_categorical( const std::vector<std::string>& _vec )
    {
        Float result = 0.0;

        for ( const auto& str : _vec )
            {
                if ( str == "" || str == "nan" || str == "NaN" || str == "NA" ||
                     str == "NULL" )
                    {
                        continue;
                    }

                ++result;
            }

        return result;
    }

    /// Counts the non-null distinct number of entries.
    static Float count_distinct( const std::vector<std::string>& _vec )
    {
        auto set = std::unordered_set<std::string>();

        for ( const auto& str : _vec )
            {
                if ( str == "" || str == "nan" || str == "NaN" || str == "NA" ||
                     str == "NULL" )
                    {
                        continue;
                    }

                set.insert( str );
            }

        return static_cast<Float>( set.size() );
    }

    /// Finds the maximum of all non-null entries.
    template <class IteratorType>
    static Float max( IteratorType _begin, IteratorType _end )
    {
        const auto max = []( const Float init, const Float val ) {
            return ( ( val > init || std::isnan( init ) ) ? val : init );
        };

        return num_agg( _begin, _end, max, NAN );
    }

    template <class IteratorType>
    static Float median( IteratorType _begin, IteratorType _end )
    {
        if ( std::distance( _begin, _end ) <= 0 )
            {
                throw std::runtime_error( "Column cannot be of length 0." );
            }

        auto values = std::vector<Float>( _begin, _end );

        std::sort( values.begin(), values.end() );

        if ( values.size() % 2 == 0 )
            {
                return ( values[( values.size() / 2 ) - 1] +
                         values[values.size() / 2] ) /
                       2.0;
            }
        else
            {
                return values[values.size() / 2];
            }
    }

    /// Finds the minimum of all non-null entries.
    template <class IteratorType>
    static Float min( IteratorType _begin, IteratorType _end )
    {
        const auto min = []( const Float init, const Float val ) {
            return ( ( val < init || std::isnan( init ) ) ? val : init );
        };

        return num_agg( _begin, _end, min, NAN );
    }

    /// Takes the standard deviation of all non-null entries.
    template <class IteratorType>
    static Float stddev( IteratorType _begin, IteratorType _end )
    {
        return std::sqrt( var( _begin, _end ) );
    }

    /// Takes the sum of all non-null entries.
    template <class IteratorType>
    static Float sum( IteratorType _begin, IteratorType _end )
    {
        const auto sum = []( const Float init, const Float val ) {
            if ( std::isnan( val ) )
                {
                    return init;
                }
            else
                {
                    return init + val;
                }
        };

        return num_agg( _begin, _end, sum, 0.0 );
    }

    /// Takes the variance of all non-null entries.
    template <class IteratorType>
    static Float var( IteratorType _begin, IteratorType _end )
    {
        const auto mean = avg( _begin, _end );

        const auto n = count( _begin, _end );

        const auto var = [mean, n]( const Float init, const Float val ) {
            if ( std::isnan( val ) )
                {
                    return init;
                }
            else
                {
                    const auto diff = val - mean;
                    return init + diff * diff / n;
                }
        };

        return num_agg( _begin, _end, var, 0.0 );
    }

    // ------------------------------------------------------------------------

   private:
    /// Undertakes a numerical aggregation based on the template class
    /// Aggregation.
    template <class Aggregation, class IteratorType>
    static Float num_agg(
        IteratorType _begin,
        IteratorType _end,
        const Aggregation& _agg,
        const Float _init )
    {
        if ( std::distance( _begin, _end ) <= 0 )
            {
                throw std::runtime_error( "Column cannot be of length 0." );
            }

        return std::accumulate( _begin, _end, _init, _agg );
    }

    // ------------------------------------------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace utils
}  // namespace engine

#endif  // ENGINE_UTILS_COLUMNOPERATORS_HPP_
