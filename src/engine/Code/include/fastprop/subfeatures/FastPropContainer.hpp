#ifndef FASTPROP_SUBFEATURES_FASTPROPCONTAINER_HPP_
#define FASTPROP_SUBFEATURES_FASTPROPCONTAINER_HPP_

// ----------------------------------------------------------------------------

namespace fastprop
{
namespace subfeatures
{
// ------------------------------------------------------------------------

class FastPropContainer
{
   public:
    typedef std::vector<std::shared_ptr<const FastPropContainer>> Subcontainers;

    FastPropContainer(
        const std::shared_ptr<const algorithm::FastProp>& _fast_prop,
        const std::shared_ptr<const Subcontainers>& _subcontainers );

    FastPropContainer( const Poco::JSON::Object& _obj );

    ~FastPropContainer();

   public:
    /// Trivial (const) accessor.
    const algorithm::FastProp& fast_prop() const
    {
        assert_true( fast_prop_ );
        return *fast_prop_;
    }

    /// Whether there is a fast_prop contained in the container.
    bool has_fast_prop() const { return ( fast_prop_ && true ); }

    /// Returns the number of peripheral tables
    size_t size() const
    {
        assert_true( subcontainers_ );
        return subcontainers_->size();
    }

    /// Trivial accessor
    std::shared_ptr<const FastPropContainer> subcontainers( size_t _i ) const
    {
        assert_true( subcontainers_ );
        assert_true( _i < subcontainers_->size() );
        return subcontainers_->at( _i );
    }

    /// Expresses the containers as a JSON object.
    Poco::JSON::Object::Ptr to_json_obj() const;

    /// Expresses the features to in SQL code.
    void to_sql(
        const std::shared_ptr<const std::vector<strings::String>>& _categories,
        const helpers::VocabularyTree& _vocabulary,
        const std::shared_ptr<const helpers::SQLDialectGenerator>&
            _sql_dialect_generator,
        const std::string& _feature_prefix,
        const bool _subfeatures,
        std::vector<std::string>* _sql ) const;

   private:
    /// Parses the subcontainers.
    std::shared_ptr<Subcontainers> parse_subcontainers(
        const Poco::JSON::Object& _obj );

   private:
    /// The algorithm used to generate new features.
    const std::shared_ptr<const algorithm::FastProp> fast_prop_;

    /// The FastPropContainer used for any subtrees.
    const std::shared_ptr<const Subcontainers> subcontainers_;
};

// ------------------------------------------------------------------------
}  // namespace subfeatures
}  // namespace fastprop

#endif  // FASTPROP_SUBFEATURES_FASTPROPCONTAINER_HPP_

