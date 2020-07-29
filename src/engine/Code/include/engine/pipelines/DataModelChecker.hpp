#ifndef ENGINE_PIPELINES_DATAMODELCHECKER_HPP_
#define ENGINE_PIPELINES_DATAMODELCHECKER_HPP_

// ----------------------------------------------------------------------------

namespace engine
{
namespace pipelines
{
// ----------------------------------------------------------------------------

class DataModelChecker
{
   public:
    /// Generates warnings, if there are obvious issues in the data model.
    static void check(
        const std::shared_ptr<Poco::JSON::Object> _population_placeholder,
        const std::shared_ptr<std::vector<std::string>> _peripheral_names,
        const containers::DataFrame& _population,
        const std::vector<containers::DataFrame>& _peripheral,
        const std::vector<
            std::shared_ptr<featurelearners::AbstractFeatureLearner>>
            _feature_learners,
        const std::shared_ptr<const communication::Logger>& _logger,
        Poco::Net::StreamSocket* _socket );

   private:
    /// Checks the plausibility of a categorical column.
    static void check_categorical_column(
        const containers::Column<Int>& _col,
        const std::string& _df_name,
        communication::Warner* _warner );

    /// Checks the validity of the data frames.
    static void check_data_frames(
        const containers::DataFrame& _population,
        const std::vector<containers::DataFrame>& _peripheral,
        const std::vector<
            std::shared_ptr<featurelearners::AbstractFeatureLearner>>
            _feature_learners,
        communication::Warner* _warner );

    /// Checks all of the columns in the data frame make sense.
    static void check_df(
        const containers::DataFrame& _df,
        const bool _check_num_columns,
        communication::Warner* _warner );

    /// Checks the plausibility of a float column.
    static void check_float_column(
        const containers::Column<Float>& _col,
        const std::string& _df_name,
        communication::Warner* _warner );

    /// Recursively checks the plausibility of the joins.
    static void check_join(
        const Poco::JSON::Object& _population_placeholder,
        const std::vector<std::string>& _peripheral_names,
        const containers::DataFrame& _population,
        const std::vector<containers::DataFrame>& _peripheral,
        communication::Warner* _warner );

    /// Raises a warning if there is something wrong with the matches.
    static std::tuple<bool, size_t, size_t> check_matches(
        const std::string& _join_key_used,
        const std::string& _other_join_key_used,
        const std::string& _time_stamp_used,
        const std::string& _other_time_stamp_used,
        const std::string& _upper_time_stamp_used,
        const containers::DataFrame& _population_df,
        const containers::DataFrame& _peripheral_df );

    /// Checks the self joins for the time series models.
    static void check_self_joins(
        const Poco::JSON::Object& _population_placeholder,
        const containers::DataFrame& _population,
        const std::vector<containers::DataFrame>& _peripheral,
        const std::vector<
            std::shared_ptr<featurelearners::AbstractFeatureLearner>>
            _feature_learners,
        communication::Warner* _warner );

    /// Finds the time stamps, if necessary.
    static std::tuple<
        std::optional<containers::Column<Float>>,
        std::optional<containers::Column<Float>>,
        std::optional<containers::Column<Float>>>
    find_time_stamps(
        const std::string& _time_stamp_used,
        const std::string& _other_time_stamp_used,
        const std::string& _upper_time_stamp_used,
        const containers::DataFrame& _population_df,
        const containers::DataFrame& _peripheral_df );

    /// Extracts the join keys from the population placeholder.
    static std::pair<std::vector<std::string>, std::vector<std::string>>
    get_join_keys_used(
        const Poco::JSON::Object& _population_placeholder,
        const size_t _expected_size );

    /// Extracts the time stamps from the population placeholder.
    static std::tuple<
        std::vector<std::string>,
        std::vector<std::string>,
        std::vector<std::string>>
    get_time_stamps_used(
        const Poco::JSON::Object& _population_placeholder,
        const size_t _expected_size );

    /// Checks whether all non-NULL elements in _col are equal to each other
    static bool is_all_equal( const containers::Column<Float>& _col );

    /// Returns a modified version of the placeholder, the population and
    /// peripheral tables.
    static std::tuple<
        Poco::JSON::Object,
        containers::DataFrame,
        std::vector<containers::DataFrame>>
    modify(
        const Poco::JSON::Object& _population_placeholder,
        const containers::DataFrame& _population,
        const std::vector<containers::DataFrame>& _peripheral,
        const std::vector<
            std::shared_ptr<featurelearners::AbstractFeatureLearner>>
            _feature_learners );

    /// Adds warning messages related to the joins.
    static void raise_join_warnings(
        const bool _is_many_to_one,
        const size_t _num_matches,
        const size_t _num_jk_not_found,
        const std::string& _join_key_used,
        const std::string& _other_join_key_used,
        const containers::DataFrame& _population_df,
        const containers::DataFrame& _peripheral_df,
        communication::Warner* _warner );

    /// Adds warning messages related to the joins.
    static void raise_self_join_warnings(
        const bool _is_many_to_one,
        const size_t _num_matches,
        const containers::DataFrame& _population_df,
        communication::Warner* _warner );

    // -------------------------------------------------------------------------

   private:
    /// Standard header for a column that should be unused.
    static std::string column_should_be_unused()
    {
        return warning() + "[COLUMN SHOULD BE UNUSED]: ";
    }

    /// Standard header for an ill-defined data model.
    static std::string data_model_can_be_improved()
    {
        return warning() + "[DATA MODEL CAN BE IMPROVED]: ";
    }

    /// Standard header for an info message.
    static std::string info() { return "INFO "; }

    /// Checks whether ts1 lies between ts2 and upper.
    static bool is_in_range(
        const Float ts1, const Float ts2, const Float upper )
    {
        return ts2 <= ts1 && ( std::isnan( upper ) || ts1 < upper );
    }

    /// Standard header for when some join keys where not found.
    static std::string join_keys_not_found()
    {
        return info() + "[JOIN KEYS NOT FOUND]: ";
    }

    /// Standard header for something that might take long.
    static std::string might_take_long()
    {
        return info() + "[MIGHT TAKE LONG]: ";
    }

    /// Standard header for a warning message.
    static std::string warning() { return "WARNING "; }

    // -------------------------------------------------------------------------
};

//  ----------------------------------------------------------------------------
}  // namespace pipelines
}  // namespace engine

// ----------------------------------------------------------------------------
#endif  // ENGINE_PIPELINES_DATAMODELCHECKER_HPP_
