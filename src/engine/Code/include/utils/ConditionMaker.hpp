#ifndef RELBOOST_UTILS_CONDITIONMAKER_HPP_
#define RELBOOST_UTILS_CONDITIONMAKER_HPP_

// ----------------------------------------------------------------------------

namespace relboost
{
namespace utils
{
// ------------------------------------------------------------------------

class ConditionMaker
{
   public:
    ConditionMaker(
        const std::shared_ptr<const std::vector<std::string>>& _encoding )
        : encoding_( _encoding )
    {
        assert( encoding_ );
    }

    ~ConditionMaker() = default;

    /// Identifies matches between population table and peripheral tables.
    std::string condition_greater(
        const containers::DataFrame& _input,
        const containers::DataFrame& _output,
        const containers::Split& _split ) const;

    std::string condition_smaller(
        const containers::DataFrame& _input,
        const containers::DataFrame& _output,
        const containers::Split& _split ) const;

   private:
    /// Trivial accessor.
    std::string encoding( size_t _i ) const
    {
        assert( encoding_ );

        if ( _i < encoding_->size() )
            {
                return ( *encoding_ )[_i];
            }
        else
            {
                assert( false && "Encoding out of range!" );
                return "";
            }
    }

   private:
    /// Encoding for the categorical data, maps integers to underlying category.
    const std::shared_ptr<const std::vector<std::string>> encoding_;
};

// ------------------------------------------------------------------------
}  // namespace utils
}  // namespace relboost

// ----------------------------------------------------------------------------

#endif  // RELBOOST_UTILS_CONDITIONMAKER_HPP_