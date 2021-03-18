#ifndef HELPERS_MAPPEDCONTAINER_HPP_
#define HELPERS_MAPPEDCONTAINER_HPP_

namespace helpers
{
// -------------------------------------------------------------------------

class MappedContainer
{
   public:
    typedef std::vector<Column<Float>> MappedColumns;

    MappedContainer(
        const std::vector<MappedColumns>& _categorical,
        const std::vector<MappedColumns>& _discrete,
        const std::vector<std::shared_ptr<const MappedContainer>>&
            _subcontainers,
        const std::vector<MappedColumns>& _text )
        : categorical_( _categorical ),
          discrete_( _discrete ),
          subcontainers_( _subcontainers ),
          text_( _text )
    {
        assert_msg(
            _categorical.size() == _subcontainers.size(),
            "_categorical.size(): " + std::to_string( _categorical.size() ) +
                ", _subcontainers.size(): " +
                std::to_string( _subcontainers.size() ) );

        assert_msg(
            _categorical.size() == _discrete.size(),
            "_categorical.size(): " + std::to_string( _categorical.size() ) +
                ", _discrete.size(): " + std::to_string( _discrete.size() ) );

        assert_msg(
            _categorical.size() == _text.size(),
            "_categorical.size(): " + std::to_string( _categorical.size() ) +
                ", _text.size(): " + std::to_string( _text.size() ) );
    }

    ~MappedContainer() = default;

    // Returns all mapped columns.
    MappedColumns mapped( size_t _i ) const
    {
        assert_true( _i < categorical_.size() );

        MappedColumns m;

        for ( const auto& col : categorical_.at( _i ) )
            {
                m.push_back( col );
            }

        for ( const auto& col : text_.at( _i ) )
            {
                m.push_back( col );
            }

        assert_true(
            m.size() == categorical_.at( _i ).size() + text_.at( _i ).size() );

        return m;
    }

    /// Returns the number of peripheral tables
    size_t size() const { return subcontainers_.size(); }

    /// Trivial accessor
    const std::shared_ptr<const MappedContainer>& subcontainers(
        size_t _i ) const
    {
        assert_true( _i < subcontainers_.size() );
        return subcontainers_.at( _i );
    }

   private:
    /// The transformed categorical columns for the data frame.
    const std::vector<MappedColumns> categorical_;

    /// The transformed discrete columns for the data frame.
    const std::vector<MappedColumns> discrete_;

    /// Containers for any and all existing subtables.
    const std::vector<std::shared_ptr<const MappedContainer>> subcontainers_;

    /// The transformed text columns for the data frame.
    const std::vector<MappedColumns> text_;
};

// -------------------------------------------------------------------------
}  // namespace helpers

#endif  // HELPERS_MAPPEDCONTAINER_HPP_
