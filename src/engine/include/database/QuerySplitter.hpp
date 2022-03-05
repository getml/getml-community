#ifndef DATABASE_QUERYSPLITTER_HPP_
#define DATABASE_QUERYSPLITTER_HPP_

#include <string>
#include <vector>

namespace database {

class QuerySplitter {
 public:
  /// Splits the query into its components. Also handles the DELIMITER syntax.
  static std::vector<std::string> split_queries(
      const std::string& _queries_str);

 private:
  /// Trims the resulting queries and removes empty strings.
  static std::vector<std::string> sanitize(
      const std::vector<std::string>& _splitted);
};
}  // namespace database

#endif  // DATABASE_QUERYSPLITTER_HPP_
