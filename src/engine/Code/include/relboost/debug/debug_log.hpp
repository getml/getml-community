#ifndef RELBOOST_DEBUG_DEBUG_LOG_HPP_
#define RELBOOST_DEBUG_DEBUG_LOG_HPP_

// ----------------------------------------------------------------------------
// NDEBUG implies NDEBUGLOG

#ifdef NDEBUG
#ifndef NDEBUGLOG
#define NDEBUGLOG
#endif  // NDEBUGLOG
#endif  // NDEBUG

// ----------------------------------------------------------------------------
// If NDEBUGLOG is defined, then do nothing,
// otherwise print debug_log using the Debugger.

#ifndef debug_log

#ifndef NDEBUGLOG
#define debug_log( _msg ) relboost::debug::Debugger::log( _msg )
#else  // NDEBUGLOG
#define debug_log( _msg )
#endif  // NDEBUGLOG

#endif

// ----------------------------------------------------------------------------

#endif  // RELBOOST_DEBUG_DEBUG_LOG_HPP_