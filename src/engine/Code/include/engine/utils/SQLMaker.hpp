
#ifndef ENGINE_UTILS_SQLMAKER_HPP_
#define ENGINE_UTILS_SQLMAKER_HPP_

namespace engine
{
namespace utils
{
// ----------------------------------------------------------------------------

class SQLMaker
{
   public:
    /// Transpiles the features in SQLite3 code. This
    /// is supposed to replicate the .transform(...) method
    /// of a pipeline.
    static std::string make_sql(
        const std::string& _main_table,
        const std::vector<std::string>& _autofeatures,
        const std::vector<std::string>& _sql,
        const std::vector<std::string>& _targets,
        const predictors::PredictorImpl& _predictor_impl );

   private:
    /// Generates the table that contains all the features.
    static std::string make_feature_table(
        const std::string& _main_table,
        const std::vector<std::string>& _autofeatures,
        const std::vector<std::string>& _sql,
        const std::vector<std::string>& _targets,
        const predictors::PredictorImpl& _predictor_impl );

    /// Generates the SQL code needed to join the feature tables.
    static std::string make_left_joins(
        const std::vector<std::string>& _autofeatures );

    /// Generates the SQL code needed to impute the features and drop the
    /// feature tables.
    static std::string make_postprocessing(
        const std::vector<std::string>& _sql );

    /// Generates the select statement for the feature table.
    static std::string make_select(
        const std::string& _main_table,
        const std::vector<std::string>& _autofeatures,
        const std::vector<std::string>& _targets,
        const predictors::PredictorImpl& _predictor_impl );
};

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace engine

#endif  // ENGINE_UTILS_SQLMAKER_HPP_

