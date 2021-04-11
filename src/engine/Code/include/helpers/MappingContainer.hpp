#ifndef HELPERS_MAPPINGCONTAINER_HPP_
#define HELPERS_MAPPINGCONTAINER_HPP_

namespace helpers
{
// -------------------------------------------------------------------------

class MappingContainer
{
   public:
    typedef std::shared_ptr<const std::vector<std::string>> Colnames;

    typedef std::vector<
        std::shared_ptr<const std::map<Int, std::vector<Float>>>>
        MappingForDf;

    typedef std::map<std::string, std::vector<std::string>> ColnameMap;

    typedef std::shared_ptr<const std::vector<std::string>> TableNames;

    MappingContainer(
        const std::vector<MappingForDf>& _categorical,
        const std::vector<Colnames>& _categorical_names,
        const std::vector<MappingForDf>& _discrete,
        const std::vector<Colnames>& _discrete_names,
        const std::vector<std::shared_ptr<const MappingContainer>>&
            _subcontainers,
        const TableNames& _table_names,
        const std::vector<MappingForDf>& _text,
        const std::vector<Colnames>& _text_names );

    MappingContainer( const Poco::JSON::Object& _obj );

    ~MappingContainer();

   public:
    /// Transforms the VocabularyContainer into a JSON object.
    Poco::JSON::Object::Ptr to_json_obj() const;

    /// Expresses the mapping as SQL
    std::pair<std::vector<std::string>, ColnameMap> to_sql(
        const std::shared_ptr<const std::vector<strings::String>>& _categories,
        const std::string& _feature_prefix ) const;

   private:
    /// Checks the lengths of the mappings.
    void check_lengths() const;

    /// Extracts a vector containing mappings from the object.
    static std::vector<MappingForDf> extract_mapping_vector(
        const Poco::JSON::Object& _obj, const std::string& _key );

    /// Extracts the subcontainers from an object.
    static std::vector<std::shared_ptr<const MappingContainer>>
    extract_subcontainers( const Poco::JSON::Object& _obj );

   public:
    /// Trivial (const) accessor.
    const std::vector<MappingForDf>& categorical() const
    {
        return categorical_;
    }

    /// Trivial (const) accessor.
    const std::vector<MappingForDf>& discrete() const { return discrete_; }

    /// Trivial (const) accessor.
    const std::vector<std::shared_ptr<const MappingContainer>>& subcontainers()
        const
    {
        return subcontainers_;
    }

    /// Trivial (const) accessor.
    const std::vector<MappingForDf>& text() const { return text_; }

   private:
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

    /// The names of the table the mappings apply to.
    const TableNames table_names_;

    /// The vocabulary for the text columns.
    const std::vector<MappingForDf> text_;

    /// The names of the text columns.
    const std::vector<Colnames> text_names_;
};

// -------------------------------------------------------------------------
}  // namespace helpers

#endif  // HELPERS_MAPPINGCONTAINER_HPP_
