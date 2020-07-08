#ifndef RELBOOST_CONTAINERS_SPLIT_HPP_
#define RELBOOST_CONTAINERS_SPLIT_HPP_

namespace relboost
{
namespace containers
{
// -------------------------------------------------------------------------

struct Split
{
    /// Empty constructor.
    Split()
        : categories_used_( std::make_shared<std::vector<Int>>( 0 ) ),
          categories_used_begin_( categories_used_->cbegin() ),
          categories_used_end_( categories_used_->cend() ),
          column_( 0 ),
          column_input_( 0 ),
          critical_value_( 0.0 ),
          data_used_( enums::DataUsed::time_stamps_diff )
    {
    }

    /// Constructor for splits on categorical values.
    Split(
        const std::shared_ptr<const std::vector<Int>>& _categories_used,
        const std::vector<Int>::const_iterator _categories_used_begin,
        const std::vector<Int>::const_iterator _categories_used_end,
        const size_t _column,
        const enums::DataUsed _data_used )
        : categories_used_( _categories_used ),
          categories_used_begin_( _categories_used_begin ),
          categories_used_end_( _categories_used_end ),
          column_( _column ),
          column_input_( 0 ),
          critical_value_( 0.0 ),
          data_used_( _data_used )
    {
        assert_true(
            _data_used == enums::DataUsed::categorical_input ||
            _data_used == enums::DataUsed::categorical_output );
    }

    /// Constructor for splits on numerical values.
    Split(
        const size_t _column,
        const Float _critical_value,
        const enums::DataUsed _data_used )
        : categories_used_( std::make_shared<std::vector<Int>>( 0 ) ),
          categories_used_begin_( categories_used_->cbegin() ),
          categories_used_end_( categories_used_->cend() ),
          column_( _column ),
          column_input_( 0 ),
          critical_value_( _critical_value ),
          data_used_( _data_used )
    {
        assert_true(
            _data_used == enums::DataUsed::discrete_input ||
            _data_used == enums::DataUsed::discrete_input_is_nan ||
            _data_used == enums::DataUsed::discrete_output ||
            _data_used == enums::DataUsed::discrete_output_is_nan ||
            _data_used == enums::DataUsed::numerical_input ||
            _data_used == enums::DataUsed::numerical_input_is_nan ||
            _data_used == enums::DataUsed::numerical_output ||
            _data_used == enums::DataUsed::numerical_output_is_nan ||
            _data_used == enums::DataUsed::subfeatures ||
            _data_used == enums::DataUsed::time_stamps_diff ||
            _data_used == enums::DataUsed::time_stamps_window );

        assert_true(
            _data_used != enums::DataUsed::time_stamps_diff || _column == 0 );

        assert_true(
            _data_used != enums::DataUsed::time_stamps_window || _column == 0 );
    }

    /// Constructor for splits on same units (categorical).
    Split( const size_t _column, const size_t _column_input )
        : categories_used_( std::make_shared<std::vector<Int>>( 0 ) ),
          categories_used_begin_( categories_used_->cbegin() ),
          categories_used_end_( categories_used_->cend() ),
          column_( _column ),
          column_input_( _column_input ),
          critical_value_( 0.0 ),
          data_used_( enums::DataUsed::same_units_categorical )
    {
    }

    /// Constructor for splits on same units (discrete or numerical).
    Split(
        const size_t _column,
        const size_t _column_input,
        const Float _critical_value,
        const enums::DataUsed _data_used )
        : categories_used_( std::make_shared<std::vector<Int>>( 0 ) ),
          categories_used_begin_( categories_used_->cbegin() ),
          categories_used_end_( categories_used_->cend() ),
          column_( _column ),
          column_input_( _column_input ),
          critical_value_( _critical_value ),
          data_used_( _data_used )
    {
        assert_true(
            _data_used == enums::DataUsed::same_units_discrete ||
            _data_used == enums::DataUsed::same_units_discrete_is_nan ||
            _data_used == enums::DataUsed::same_units_discrete_ts ||
            _data_used == enums::DataUsed::same_units_numerical ||
            _data_used == enums::DataUsed::same_units_numerical_is_nan ||
            _data_used == enums::DataUsed::same_units_numerical_ts );
    }

    /// Constructor for deep copies and reconstruction from JSON.
    Split(
        const std::shared_ptr<const std::vector<Int>>& _categories_used,
        const size_t _column,
        const size_t _column_input,
        const Float _critical_value,
        const enums::DataUsed _data_used )
        : categories_used_( _categories_used ),
          categories_used_begin_( _categories_used->begin() ),
          categories_used_end_( _categories_used->end() ),
          column_( _column ),
          column_input_( _column_input ),
          critical_value_( _critical_value ),
          data_used_( _data_used )
    {
    }

    ~Split() = default;

    // ------------------------------------------------------------------------

    /// Returns a deep copy of the Split.
    Split deep_copy() const
    {
        auto sorted = std::make_shared<std::vector<Int>>(
            categories_used_begin_, categories_used_end_ );

        std::sort( sorted->begin(), sorted->end() );

        return Split(
            sorted, column_, column_input_, critical_value_, data_used_ );
    }

    // ------------------------------------------------------------------------

    // Categories used for the node - for categorical values.
    std::shared_ptr<const std::vector<Int>> categories_used_;

    // Iterator pointing to the beginning of the categories used.
    std::vector<Int>::const_iterator categories_used_begin_;

    // Iterator pointing to the end of the categories used.
    std::vector<Int>::const_iterator categories_used_end_;

    // Columns used
    size_t column_;

    // Columns used in the input table for same units.
    size_t column_input_;

    // Critical value
    Float critical_value_;

    // The data used for this split.
    enums::DataUsed data_used_;

    // ------------------------------------------------------------------------
};

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace relboost

#endif  // RELBOOST_CONTAINERS_SPLIT_HPP_
