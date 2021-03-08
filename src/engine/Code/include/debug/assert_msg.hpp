#ifndef DEBUG_ASSERT_MSG_HPP_
#define DEBUG_ASSERT_MSG_HPP_

// ----------------------------------------------------------------------------

// NDEBUG implies no assertions.
#ifdef NDEBUG

#define assert_msg( EX, CUSTOM_MSG )

#else

#define assert_msg( EX, CUSTOM_MSG ) \
    (void)( ( EX ) || ( debug::Assert::throw_exception( #EX, __FILE__, __LINE__, CUSTOM_MSG ), 0 ) )

#endif

// ----------------------------------------------------------------------------

#endif  // DEBUG_ASSERT_MSG_HPP_
