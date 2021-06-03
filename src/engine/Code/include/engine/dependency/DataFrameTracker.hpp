#ifndef ENGINE_DEPENDENCY_DATAFRAMETRACKER_HPP_
#define ENGINE_DEPENDENCY_DATAFRAMETRACKER_HPP_

namespace engine
{
namespace dependency
{
// -------------------------------------------------------------------------

class DataFrameTracker
{
   public:
    DataFrameTracker( const std::shared_ptr<
                      std::map<std::string, engine::containers::DataFrame>>&
                          _data_frames )
        : data_frames_( _data_frames )
    {
    }

    ~DataFrameTracker() = default;

   public:
    /// Adds a new element to be tracked.
    void add( const containers::DataFrame& _df );

    /// Removes all elements.
    void clear();

    /// Generates a build history from the dependencies returned by
    /// the pipeline and the df_fingerprints.
    Poco::JSON::Object::Ptr make_build_history(
        const std::vector<Poco::JSON::Object::Ptr>& _dependencies,
        const std::vector<Poco::JSON::Object::Ptr>& _df_fingerprints ) const;

    /// Retrieves a deep copy of an element from the tracker, if a data
    /// frame containing this build_history exists.
    std::optional<containers::DataFrame> retrieve(
        const Poco::JSON::Object::Ptr _build_history ) const;

   public:
    /// Generates the build history and retrieves the data frame.
    std::optional<containers::DataFrame> retrieve(
        const Poco::JSON::Object& _obj,
        const Poco::JSON::Object& _cmd,
        const std::map<std::string, containers::DataFrame>& _data_frames,
        const std::vector<Poco::JSON::Object::Ptr>& _dependencies ) const
    {
        // TODO
        return std::nullopt;

        /*const auto df_fingerprints =
            containers::DataFrameExtractor::extract_df_fingerprints(
                _obj, _cmd, _data_frames );
        const auto build_history =
            make_build_history( _dependencies, df_fingerprints );
        return retrieve( build_history );*/
    }

   private:
    /// Removes all data frames that are no longer accessable.
    void clean_up();

    /// Returns the data frame designated by the hash, if such a data frame
    /// exists.
    std::optional<containers::DataFrame> get_df( const size_t _b_hash ) const;

   private:
    /// The underlying data frames.
    const std::shared_ptr<std::map<std::string, engine::containers::DataFrame>>
        data_frames_;

    /// A map keeping track of the names of the data frame and when they were
    /// last changed.
    std::map<size_t, std::pair<std::string, std::string>> pairs_;
};

// -------------------------------------------------------------------------
}  // namespace dependency
}  // namespace engine

#endif  // ENGINE_DEPENDENCY_DATAFRAMETRACKER_HPP_

