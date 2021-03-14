#ifndef HELPERS_MAPPEDCONTAINER_HPP_
#define HELPERS_MAPPEDCONTAINER_HPP_

namespace helpers
{
// -------------------------------------------------------------------------

struct MappedContainer
{
    typedef std::vector<Column<Float>> MappedColumns;

    MappedContainer(
        const std::vector<MappedColumns>& _categorical,
        const std::vector<std::shared_ptr<const MappedContainer>>&
            _subcontainers )
        : categorical_( _categorical ), subcontainers_( _subcontainers )
    {
        assert_msg(
            _categorical.size() == _subcontainers.size(),
            "_categorical.size(): " + std::to_string( _categorical.size() ) +
                ", _subcontainers.size(): " +
                std::to_string( _subcontainers.size() ) );
    }

    ~MappedContainer() = default;

    /// The transformed categorical columns for the data frame.
    const std::vector<MappedColumns> categorical_;

    /// Containers for any and all existing subtables.
    const std::vector<std::shared_ptr<const MappedContainer>> subcontainers_;
};

// -------------------------------------------------------------------------
}  // namespace helpers

#endif  // HELPERS_MAPPEDCONTAINER_HPP_
