#ifndef DEBUG_ASSERT_TRUE_HPP_
#define DEBUG_ASSERT_TRUE_HPP_

// ----------------------------------------------------------------------------

// NDEBUG implies no assertions.
#ifdef NDEBUG

#define assert_true( EX )

#else

#define assert_true( EX ) \
    (void)( ( EX ) || ( debug::Assert::throw_exception( #EX, __FILE__, __LINE__ ), 0 ) )

#endif

// ----------------------------------------------------------------------------

#endif  // DEBUG_ASSERT_TRUE_HPP_
