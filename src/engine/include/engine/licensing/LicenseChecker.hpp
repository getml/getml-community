#ifndef ENGINE_LICENSECHECKER_HPP_
#define ENGINE_LICENSECHECKER_HPP_

// ----------------------------------------------------------------------------

#if (defined(_WIN32) || defined(_WIN64))
#define _WINSOCKAPI_  // stops windows.h including winsock.h
#include <windows.h>
#include <winsock2.h>

// ----------------------------------------------------------------------------
#include <openssl/ossl_typ.h>
#endif

#include <Poco/Crypto/RSACipherImpl.h>
#include <Poco/Net/HTTPSClientSession.h>
#include <Poco/StreamCopier.h>
#include <Poco/URI.h>

// ----------------------------------------------------------------------------

#include <chrono>
#include <sstream>
#include <string>
#include <thread>

// ----------------------------------------------------------------------------

#include "multithreading/multithreading.hpp"

// ----------------------------------------------------------------------------

#include "engine/Int.hpp"
#include "engine/ULong.hpp"
#include "engine/communication/communication.hpp"
#include "engine/config/config.hpp"
#include "engine/containers/containers.hpp"
#include "engine/crypto/crypto.hpp"

// ------------------------------------------------------------------------

#include "engine/licensing/Token.hpp"

// ----------------------------------------------------------------------------

namespace engine {
namespace licensing {

class LicenseChecker {
 public:
  LicenseChecker(const fct::Ref<const communication::Logger> _logger,
                 const fct::Ref<const communication::Monitor> _monitor,
                 const config::Options& _options)
      : logger_(_logger),
        monitor_(_monitor),
        options_(_options),
        read_write_lock_(fct::Ref<multithreading::ReadWriteLock>::make()),
        token_(new Token()) {}

  ~LicenseChecker(){};

  // ------------------------------------------------------------------------

 public:
  /// Makes sure that the size of the raw data used does not exceed the
  /// memory limit.
  void check_mem_size(
      const std::map<std::string, containers::DataFrame>& _data_frames,
      const ULong _new_size = 0) const;

  /// Makes sure that this is an enterprise user.
  void check_enterprise() const;

  /// Receives a token from the license server
  void receive_token(const std::string& _caller_id);

  // ------------------------------------------------------------------------

 public:
  /// Whether there is a valid token.
  bool has_active_token() const {
    multithreading::ReadLock read_lock(read_write_lock_);
    return token_ && token_->currently_active();
  }

  /// Checks whether is an enterprise version.
  bool is_enterprise() const {
    multithreading::ReadLock read_lock(read_write_lock_);
    return token_ && (token_->function_set_id_ == "enterprise");
  }

  /// Trivial accessor
  Token token() const {
    multithreading::ReadLock read_lock(read_write_lock_);
    assert_true(token_);
    return *token_;
  }

  // ------------------------------------------------------------------------

 private:
  /// Calculates the memory size of the data frames.
  ULong calc_mem_size(
      const std::map<std::string, containers::DataFrame>& _data_frames) const;

  // ------------------------------------------------------------------------

 private:
  /// Encrypts a message using a one-way encryption algorithm.
  std::string encrypt(const std::string& _msg);

  /// Returns the operating system we are running on.
  std::string os() const;

  /// Sends a POST request to the license server (via the getML monitor).
  std::pair<std::string, bool> send(const Poco::JSON::Object& _request);

  // ------------------------------------------------------------------------

 private:
  /// For logging the licensing process.
  const fct::Ref<const communication::Logger> logger_;

  /// The user management and licensing process is handled by the monitor.
  const fct::Ref<const communication::Monitor> monitor_;

  /// Contains information on the port of the license checker process
  const config::Options options_;

  /// For protecting the token.
  const fct::Ref<multithreading::ReadWriteLock> read_write_lock_;

  /// The token containing information on the memory limit
  std::unique_ptr<const Token> token_;

  // ------------------------------------------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace licensing
}  // namespace engine

#endif  // ENGINE_LICENSECHECKER_HPP_
