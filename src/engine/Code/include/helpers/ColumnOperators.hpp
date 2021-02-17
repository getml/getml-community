#ifndef HELPERS_COLUMNOPERATORS_HPP_
#define HELPERS_COLUMNOPERATORS_HPP_

namespace helpers
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
                return 0.0;
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
        const auto divisor = count( _begin, _end );

        if ( divisor == 0.0 )
            {
                return NAN;
            }

        const auto numerator = sum( _begin, _end );

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
                if ( NullChecker::is_null( str ) )
                    {
                        continue;
                    }

                ++result;
            }

        return result;
    }

    /// Counts the non-null number of entries.
    static Float count_categorical( const std::vector<Int>& _vec )
    {
        Float result = 0.0;

        for ( const auto& val : _vec )
            {
                if ( NullChecker::is_null( val ) )
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
        return count_distinct( _vec.begin(), _vec.end() );
    }

    /// Counts the non-null distinct number of entries.
    static Float count_distinct( const std::vector<Int>& _vec )
    {
        return count_distinct( _vec.begin(), _vec.end() );
    }

    /// Counts the distinct number of entries.
    template <class IteratorType>
    static Float count_distinct( IteratorType _begin, IteratorType _end )
    {
        using ConstValueType = typename std::remove_reference<decltype( *_begin )>::type;

        using ValueType = typename std::remove_const<ConstValueType>::type;

        auto set = std::unordered_set<ValueType>();

        for ( auto it = _begin; it != _end; ++it )
            {
                if ( NullChecker::is_null( *it ) )
                    {
                        continue;
                    }

                set.insert( *it );
            }

        return static_cast<Float>( set.size() );
    }

    /// Implements the FIRST aggregation. Assumes that the iterator points to a
    /// set of pairs, the first signifying the element over which we want to
    /// sort and the second signifying the value.
    template <class IteratorType>
    static Float first( IteratorType _begin, IteratorType _end )
    {
        if ( std::distance( _begin, _end ) <= 0 )
            {
                return NAN;
            }

        using Pair = std::pair<Float, Float>;

        const auto ts_is_smaller = []( const Pair& p1,
                                       const Pair& p2 ) -> Float {
            return p1.first < p2.first;
        };

        const auto p = *std::ranges::min_element( _begin, _end, ts_is_smaller );

        return p.second;
    }

    /// Implements the LAST aggregation. Assumes that the iterator points to a
    /// set of pairs, the first signifying the element over which we want to
    /// sort and the second signifying the value.
    template <class IteratorType>
    static Float last( IteratorType _begin, IteratorType _end )
    {
        if ( std::distance( _begin, _end ) <= 0 )
            {
                return NAN;
            }

        using Pair = std::pair<Float, Float>;

        const auto ts_is_smaller = []( const Pair& p1,
                                       const Pair& p2 ) -> Float {
            return p1.first < p2.first;
        };

        const auto p = *std::ranges::max_element( _begin, _end, ts_is_smaller );

        return p.second;
    }

    /// Finds the maximum of all non-null entries.
    template <class IteratorType>
    static Float maximum( IteratorType _begin, IteratorType _end )
    {
        const auto max_op = []( const Float init, const Float val ) {
            return ( ( val > init || std::isnan( init ) ) ? val : init );
        };

        return num_agg( _begin, _end, max_op, NAN );
    }

    template <class IteratorType>
    static Float median( IteratorType _begin, IteratorType _end )
    {
        if ( std::distance( _begin, _end ) <= 0 )
            {
                return NAN;
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
    static Float minimum( IteratorType _begin, IteratorType _end )
    {
        const auto min_op = []( const Float init, const Float val ) {
            return ( ( val < init || std::isnan( init ) ) ? val : init );
        };

        return num_agg( _begin, _end, min_op, NAN );
    }

    /// Takes the skewness of all non-null entries.
    template <class IteratorType>
    static Float skew( IteratorType _begin, IteratorType _end )
    {
        const auto n = count( _begin, _end );

        if ( n == 0.0 )
            {
                return NAN;
            }

        const auto mean = avg( _begin, _end );

        const auto std = stddev( _begin, _end );

        const auto skewness = [mean, std, n](
                                  const Float init, const Float val ) {
            if ( NullChecker::is_null( val ) )
                {
                    return init;
                }
            else
                {
                    const auto diff = ( val - mean ) / std;
                    return init + diff * diff * diff / n;
                }
        };

        return num_agg( _begin, _end, skewness, 0.0 );
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
            if ( NullChecker::is_null( val ) )
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
        const auto n = count( _begin, _end );

        if ( n == 0.0 )
            {
                return NAN;
            }

        const auto mean = avg( _begin, _end );

        const auto variance = [mean, n]( const Float init, const Float val ) {
            if ( NullChecker::is_null( val ) )
                {
                    return init;
                }
            else
                {
                    const auto diff = val - mean;
                    return init + diff * diff / n;
                }
        };

        return num_agg( _begin, _end, variance, 0.0 );
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
        return std::accumulate( _begin, _end, _init, _agg );
    }

    // ------------------------------------------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace helpers

#endif  // HELPERS_COLUMNOPERATORS_HPP_
