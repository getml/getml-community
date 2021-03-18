#ifndef HELPERS_MAPPINGCONTAINER_HPP_
#define HELPERS_MAPPINGCONTAINER_HPP_

namespace helpers
{
// -------------------------------------------------------------------------

struct MappingContainer
{
    typedef std::vector<
        std::shared_ptr<const std::map<Int, std::vector<Float>>>>
        MappingForDf;

    MappingContainer(
        const std::vector<MappingForDf>& _categorical,
        const std::vector<MappingForDf>& _discrete,
        const std::vector<std::shared_ptr<const MappingContainer>>&
            _subcontainers,
        const std::vector<MappingForDf>& _text );

    MappingContainer( const Poco::JSON::Object& _obj );

    ~MappingContainer();

    /// Extracts a vector containing mappings from the object.
    static std::vector<MappingForDf> extract_mapping_vector(
        const Poco::JSON::Object& _obj, const std::string& _key );

    /// Extracts the subcontainers from an object.
    static std::vector<std::shared_ptr<const MappingContainer>>
    extract_subcontainers( const Poco::JSON::Object& _obj );

    /// Transforms the VocabularyContainer into a JSON object.
    Poco::JSON::Object::Ptr to_json_obj() const;

    /// The vocabulary for the categorical columns.
    const std::vector<MappingForDf> categorical_;

    /// The vocabulary for the discrete columns.
    const std::vector<MappingForDf> discrete_;

    /// Containers for any and all existing subtables.
    const std::vector<std::shared_ptr<const MappingContainer>> subcontainers_;

    /// The vocabulary for the text columns.
    const std::vector<MappingForDf> text_;
};

// -------------------------------------------------------------------------
}  // namespace helpers

#endif  // HELPERS_MAPPINGCONTAINER_HPP_
