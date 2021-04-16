#ifndef HELPERS_VOCABULARYTREE_HPP_
#define HELPERS_VOCABULARYTREE_HPP_

namespace helpers
{
// -------------------------------------------------------------------------

class VocabularyTree
{
   public:
    typedef std::vector<std::shared_ptr<const std::vector<strings::String>>>
        VocabForDf;

   public:
    VocabularyTree(
        const VocabForDf& _population,
        const std::vector<VocabForDf>& _peripheral,
        const Placeholder& _placeholder,
        const std::vector<std::string>& _peripheral_names )
        : peripheral_( parse_peripheral(
              _peripheral, _placeholder, _peripheral_names ) ),
          population_( _population ),
          subtrees_(
              parse_subtrees( _peripheral, _placeholder, _peripheral_names ) )
    {
        assert_true( peripheral_.size() == subtrees_.size() );
    }

    ~VocabularyTree() {}

   public:
    /// Trivial (const) accessor
    const std::vector<VocabForDf>& peripheral() const { return peripheral_; }

    /// Trivial (const) accessor
    const VocabForDf& population() const { return population_; }

    /// Trivial (const) accessor
    const std::vector<std::optional<VocabularyTree>>& subtrees() const
    {
        return subtrees_;
    }

   private:
    /// Identifies the index for the associated peripheral table.
    static VocabForDf find_peripheral(
        const std::vector<VocabForDf>& _peripheral,
        const Placeholder& _placeholder,
        const std::vector<std::string>& _peripheral_names )
    {
        const auto it = std::find(
            _peripheral_names.begin(),
            _peripheral_names.end(),
            _placeholder.name_ );

        if ( it == _peripheral_names.end() )
            {
                throw std::invalid_argument(
                    "Peripheral table named '" + _placeholder.name_ +
                    "' not found!" );
            }

        const auto ix = std::distance( _peripheral_names.begin(), it );

        assert_true( ix >= 0 );

        assert_true( static_cast<size_t>( ix ) < _peripheral.size() );

        return _peripheral.at( ix );
    }

    /// Parses the vocabulary for the peripheral tables.
    static std::vector<VocabForDf> parse_peripheral(
        const std::vector<VocabForDf>& _peripheral,
        const Placeholder& _placeholder,
        const std::vector<std::string>& _peripheral_names )
    {
        const auto extract_peripheral = std::bind(
            find_peripheral,
            _peripheral,
            std::placeholders::_1,
            _peripheral_names );

        const auto range = _placeholder.joined_tables_ |
                           std::ranges::views::transform( extract_peripheral );

        return stl::make::vector<VocabForDf>( range );
    }

    /// Parses the subtrees.
    static std::vector<std::optional<VocabularyTree>> parse_subtrees(
        const std::vector<VocabForDf>& _peripheral,
        const Placeholder& _placeholder,
        const std::vector<std::string>& _peripheral_names )
    {
        const auto extract_peripheral = std::bind(
            find_peripheral,
            _peripheral,
            std::placeholders::_1,
            _peripheral_names );

        const auto make_subtree =
            [&_peripheral, &_peripheral_names, extract_peripheral](
                const Placeholder& _p ) -> std::optional<VocabularyTree> {
            if ( _p.joined_tables_.size() == 0 )
                {
                    return std::nullopt;
                }

            const auto new_population = extract_peripheral( _p );

            return VocabularyTree(
                new_population, _peripheral, _p, _peripheral_names );
        };

        const auto range = _placeholder.joined_tables_ |
                           std::ranges::views::transform( make_subtree );

        return stl::make::vector<std::optional<VocabularyTree>>( range );
    }

   private:
    /// The vocabulary for the peripheral tables.
    const std::vector<VocabForDf> peripheral_;

    /// The vocabulary for the population table.
    const VocabForDf population_;

    /// Vocabulary used for any subholders.
    const std::vector<std::optional<VocabularyTree>> subtrees_;
};

// -------------------------------------------------------------------------
}  // namespace helpers

#endif  // HELPERS_VOCABULARYTREE_HPP_

