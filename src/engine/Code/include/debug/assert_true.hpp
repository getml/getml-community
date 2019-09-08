#ifndef DEBUG_ASSERT_TRUE_HPP_
#define DEBUG_ASSERT_TRUE_HPP_

// ----------------------------------------------------------------------------
// If THROWASSERT is defined, then throw an exception,
// otherwise do nothing.

// NDEBUG implies no assertions.
#ifdef NDEBUG

#define assert_true( EX )

#else

#ifdef THROWASSERT
#define assert_true( EX ) \
    (void)( ( EX ) || ( debug::Assert::throw_exception( #EX, __FILE__, __LINE__ ), 0 ) )
#else
#define assert_true( EX ) assert( EX )
#endif  // THROWASSERT

#endif

// ----------------------------------------------------------------------------

#endif  // DEBUG_ASSERT_TRUE_HPP_
