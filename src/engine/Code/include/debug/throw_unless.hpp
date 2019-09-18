#ifndef DEBUG_THROW_UNLESS_HPP_
#define DEBUG_THROW_UNLESS_HPP_

// ----------------------------------------------------------------------------

// Throw an exception unless condition is true.
#define throw_unless( _condition, _msg ) \
    (void)( ( _condition ) || ( debug::Assert::throw_exception( _msg ), 0 ) )

// ----------------------------------------------------------------------------

#endif  // DEBUG_ASSERT_TRUE_HPP
