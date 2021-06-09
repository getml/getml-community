#ifndef HELPERS_SUBROLEPARSER_HPP_
#define HELPERS_SUBROLEPARSER_HPP_

namespace helpers
{
// ----------------------------------------------------------------------------

struct SubroleParser
{
    static constexpr const char* COMPARISON_ONLY = "comparison only";
    static constexpr const char* EMAIL = "email";
    static constexpr const char* EMAIL_ONLY = "email only";
    static constexpr const char* EXCLUDE_FASTPROP = "exclude fastprop";
    static constexpr const char* EXCLUDE_FEATURE_LEARNERS =
        "exclude feature learners";
    static constexpr const char* EXCLUDE_IMPUTATION = "exclude imputation";
    static constexpr const char* EXCLUDE_MAPPING = "exclude mapping";
    static constexpr const char* EXCLUDE_MULTIREL = "exclude multirel";
    static constexpr const char* EXCLUDE_PREDICTORS = "exclude predictors";
    static constexpr const char* EXCLUDE_PREPROCESSORS =
        "exclude preprocessors";
    static constexpr const char* EXCLUDE_RELBOOST = "exclude relboost";
    static constexpr const char* EXCLUDE_RELMT = "exclude relmt";
    static constexpr const char* EXCLUDE_SEASONAL = "exclude seasonal";
    static constexpr const char* EXCLUDE_TEXT_FIELD_SPLITTER =
        "exclude text field splitter";
    static constexpr const char* SUBSTRING = "substring";
    static constexpr const char* SUBSTRING_ONLY = "substring only";

    /// Whether the column returns contains any of the _targets.
    static bool contains_any(
        const std::vector<std::string>& _column,
        const std::vector<Subrole>& _targets );

    /// Whether the column returns contains any of the _targets.
    static bool contains_any(
        const std::vector<Subrole>& _column,
        const std::vector<Subrole>& _targets );

    /// Parses a single subrole
    static Subrole parse( const std::string& _str );

    /// Parses a vector of subroles.
    static std::vector<Subrole> parse( const std::vector<std::string>& _vec );
};

// ----------------------------------------------------------------------------
}  // namespace helpers

#endif  // HELPERS_SUBROLEPARSER_HPP_
