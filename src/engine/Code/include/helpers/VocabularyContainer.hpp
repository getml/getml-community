#ifndef HELPERS_VOCABULARYCONTAINER_HPP_
#define HELPERS_VOCABULARYCONTAINER_HPP_

namespace helpers
{
// -------------------------------------------------------------------------

class VocabularyContainer
{
   public:
    typedef std::vector<std::shared_ptr<const std::vector<strings::String>>>
        VocabForDf;

   public:
    VocabularyContainer(
        size_t _min_df,
        size_t _max_size,
        const DataFrame& _population,
        const std::vector<DataFrame>& _peripheral );

    VocabularyContainer(
        const VocabForDf& _population,
        const std::vector<VocabForDf>& _peripheral );

    VocabularyContainer( const Poco::JSON::Object& _obj );

    ~VocabularyContainer();

    /// Transforms the VocabularyContainer into a JSON object.
    Poco::JSON::Object::Ptr to_json_obj() const;

   public:
    /// Trivial (const) accessor
    const std::vector<VocabForDf>& peripheral() const { return peripheral_; }

    /// Trivial (const) accessor
    const VocabForDf& population() const { return population_; }

   private:
    /// The vocabulary for the peripheral tables.
    std::vector<VocabForDf> peripheral_;

    /// The vocabulary for the population table.
    VocabForDf population_;
};

// -------------------------------------------------------------------------
}  // namespace helpers

#endif  // HELPERS_VOCABULARYCONTAINER_HPP_

