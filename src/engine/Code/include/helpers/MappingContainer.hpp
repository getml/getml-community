#ifndef HELPERS_MAPPINGCONTAINER_HPP_
#define HELPERS_MAPPINGCONTAINER_HPP_

namespace helpers
{
// -------------------------------------------------------------------------

struct MappingContainer
{
    typedef std::shared_ptr<const std::vector<std::string>> Colnames;

    typedef std::vector<
        std::shared_ptr<const std::map<Int, std::vector<Float>>>>
        MappingForDf;

    MappingContainer(
        const std::vector<MappingForDf>& _categorical,
        const std::vector<Colnames>& _categorical_names,
        const std::vector<MappingForDf>& _discrete,
        const std::vector<Colnames>& _discrete_names,
        const std::vector<std::shared_ptr<const MappingContainer>>&
            _subcontainers,
        const std::vector<MappingForDf>& _text,
        const std::vector<Colnames>& _text_names );

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

    /// Expresses the mapping as SQL
    std::vector<std::string> to_sql(
        const std::shared_ptr<const std::vector<strings::String>>& _categories,
        const std::string& _feature_prefix,
        const size_t _offset ) const;

    /// The vocabulary for the categorical columns.
    const std::vector<MappingForDf> categorical_;

    /// The names of the categorical columns.
    const std::vector<Colnames> categorical_names_;

    /// The vocabulary for the discrete columns.
    const std::vector<MappingForDf> discrete_;

    /// The names of the discrete columns.
    const std::vector<Colnames> discrete_names_;

    /// Containers for any and all existing subtables.
    const std::vector<std::shared_ptr<const MappingContainer>> subcontainers_;

    /// The vocabulary for the text columns.
    const std::vector<MappingForDf> text_;

    /// The names of the text columns.
    const std::vector<Colnames> text_names_;
};

// -------------------------------------------------------------------------
}  // namespace helpers

#endif  // HELPERS_MAPPINGCONTAINER_HPP_
