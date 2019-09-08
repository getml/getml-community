#ifndef RELBOOST_UTILS_SORTER_HPP_
#define RELBOOST_UTILS_SORTER_HPP_

// ----------------------------------------------------------------------------

namespace relboost
{
namespace utils
{
// ----------------------------------------------------------------------------
// This uses template specialization to implement different sorting functions
// for the different enumerations of DataUsed.

template <enums::DataUsed>
struct Sorter
{
};

// ----------------------------------------------------------------------------

template <>
struct Sorter<enums::DataUsed::categorical_input>
{
    static void sort(
        const size_t _num_column,
        const containers::DataFrame& _df,
        const std::vector<const containers::Match*>::iterator _begin,
        const std::vector<const containers::Match*>::iterator _end )
    {
        assert_true( _end >= _begin );

        assert_true( _num_column < _df.num_categoricals() );

        // Note that we are sorting in ASCENDING order!

        std::sort(
            _begin,
            _end,
            [_num_column, &_df](
                const containers::Match* m1, const containers::Match* m2 ) {
                assert_true( m1->ix_input < _df.nrows() );
                assert_true( m2->ix_input < _df.nrows() );

                return _df.categorical( m1->ix_input, _num_column ) <
                       _df.categorical( m2->ix_input, _num_column );
            } );
    }
};

// ----------------------------------------------------------------------------

template <>
struct Sorter<enums::DataUsed::categorical_output>
{
    static void sort(
        const size_t _num_column,
        const containers::DataFrameView& _df,
        const std::vector<const containers::Match*>::iterator _begin,
        const std::vector<const containers::Match*>::iterator _end )
    {
        assert_true( _end >= _begin );

        assert_true( _num_column < _df.num_categoricals() );

        // Note that we are sorting in ASCENDING order!

        std::sort(
            _begin,
            _end,
            [_num_column, &_df](
                const containers::Match* m1, const containers::Match* m2 ) {
                assert_true( m1->ix_output < _df.nrows() );
                assert_true( m2->ix_output < _df.nrows() );

                return _df.categorical( m1->ix_output, _num_column ) <
                       _df.categorical( m2->ix_output, _num_column );
            } );
    }
};

// ----------------------------------------------------------------------------

template <>
struct Sorter<enums::DataUsed::discrete_input>
{
    static void sort(
        const size_t _num_column,
        const containers::DataFrame& _df,
        const std::vector<const containers::Match*>::iterator _begin,
        const std::vector<const containers::Match*>::iterator _end )
    {
        assert_true( _end >= _begin );

        assert_true( _num_column < _df.num_discretes() );

        // Note that we are sorting in DESCENDING order!

        std::sort(
            _begin,
            _end,
            [_num_column, &_df](
                const containers::Match* m1, const containers::Match* m2 ) {
                assert_true( m1->ix_input < _df.nrows() );
                assert_true( m2->ix_input < _df.nrows() );

                return _df.discrete( m1->ix_input, _num_column ) >
                       _df.discrete( m2->ix_input, _num_column );
            } );
    }
};

// ----------------------------------------------------------------------------

template <>
struct Sorter<enums::DataUsed::discrete_output>
{
    static void sort(
        const size_t _num_column,
        const containers::DataFrameView& _df,
        const std::vector<const containers::Match*>::iterator _begin,
        const std::vector<const containers::Match*>::iterator _end )
    {
        assert_true( _end >= _begin );

        assert_true( _num_column < _df.num_discretes() );

        // Note that we are sorting in DESCENDING order!

        std::sort(
            _begin,
            _end,
            [_num_column, &_df](
                const containers::Match* m1, const containers::Match* m2 ) {
                assert_true( m1->ix_output < _df.nrows() );
                assert_true( m2->ix_output < _df.nrows() );

                return _df.discrete( m1->ix_output, _num_column ) >
                       _df.discrete( m2->ix_output, _num_column );
            } );
    }
};

// ----------------------------------------------------------------------------

template <>
struct Sorter<enums::DataUsed::numerical_input>
{
    static void sort(
        const size_t _num_column,
        const containers::DataFrame& _df,
        const std::vector<const containers::Match*>::iterator _begin,
        const std::vector<const containers::Match*>::iterator _end )
    {
        assert_true( _end >= _begin );

        assert_true( _num_column < _df.num_numericals() );

        // Note that we are sorting in DESCENDING order!

        std::sort(
            _begin,
            _end,
            [_num_column, &_df](
                const containers::Match* m1, const containers::Match* m2 ) {
                assert_true( m1->ix_input < _df.nrows() );
                assert_true( m2->ix_input < _df.nrows() );

                return _df.numerical( m1->ix_input, _num_column ) >
                       _df.numerical( m2->ix_input, _num_column );
            } );
    }
};

// ----------------------------------------------------------------------------

template <>
struct Sorter<enums::DataUsed::numerical_output>
{
    static void sort(
        const size_t _num_column,
        const containers::DataFrameView& _df,
        const std::vector<const containers::Match*>::iterator _begin,
        const std::vector<const containers::Match*>::iterator _end )
    {
        assert_true( _end >= _begin );

        assert_true( _num_column < _df.num_numericals() );

        // Note that we are sorting in DESCENDING order!

        std::sort(
            _begin,
            _end,
            [_num_column, &_df](
                const containers::Match* m1, const containers::Match* m2 ) {
                assert_true( m1->ix_output < _df.nrows() );
                assert_true( m2->ix_output < _df.nrows() );

                return _df.numerical( m1->ix_output, _num_column ) >
                       _df.numerical( m2->ix_output, _num_column );
            } );
    }
};

// ----------------------------------------------------------------------------

template <>
struct Sorter<enums::DataUsed::same_units_discrete>
{
    static void sort(
        const size_t _input_col,
        const size_t _output_col,
        const containers::DataFrame& _input,
        const containers::DataFrameView& _output,
        const std::vector<const containers::Match*>::iterator _begin,
        const std::vector<const containers::Match*>::iterator _end )
    {
        // Note that we are sorting in DESCENDING order!

        assert_true( _end >= _begin );

        assert_true( _input_col < _input.num_discretes() );
        assert_true( _output_col < _output.num_discretes() );

        std::sort(
            _begin,
            _end,
            [_input_col, _output_col, &_input, &_output](
                const containers::Match* m1, const containers::Match* m2 ) {
                assert_true( m1->ix_input < _input.nrows() );
                assert_true( m2->ix_input < _input.nrows() );

                assert_true( m1->ix_output < _output.nrows() );
                assert_true( m2->ix_output < _output.nrows() );

                const auto diff1 =
                    _output.discrete( m1->ix_output, _output_col ) -
                    _input.discrete( m1->ix_input, _input_col );

                const auto diff2 =
                    _output.discrete( m2->ix_output, _output_col ) -
                    _input.discrete( m2->ix_input, _input_col );

                return diff1 > diff2;
            } );
    }
};

// ----------------------------------------------------------------------------

template <>
struct Sorter<enums::DataUsed::same_units_numerical>
{
    static void sort(
        const size_t _input_col,
        const size_t _output_col,
        const containers::DataFrame& _input,
        const containers::DataFrameView& _output,
        const std::vector<const containers::Match*>::iterator _begin,
        const std::vector<const containers::Match*>::iterator _end )
    {
        assert_true( _end >= _begin );

        assert_true( _input_col < _input.num_numericals() );
        assert_true( _output_col < _output.num_numericals() );

        // Note that we are sorting in DESCENDING order!

        std::sort(
            _begin,
            _end,
            [_input_col, _output_col, &_input, &_output](
                const containers::Match* m1, const containers::Match* m2 ) {
                assert_true( m1->ix_input < _input.nrows() );
                assert_true( m2->ix_input < _input.nrows() );

                assert_true( m1->ix_output < _output.nrows() );
                assert_true( m2->ix_output < _output.nrows() );

                const auto diff1 =
                    _output.numerical( m1->ix_output, _output_col ) -
                    _input.numerical( m1->ix_input, _input_col );

                const auto diff2 =
                    _output.numerical( m2->ix_output, _output_col ) -
                    _input.numerical( m2->ix_input, _input_col );

                return diff1 > diff2;
            } );
    }
};

// ----------------------------------------------------------------------------

template <>
struct Sorter<enums::DataUsed::time_stamps_diff>
{
    static void sort(
        const containers::DataFrame& _input,
        const containers::DataFrameView& _output,
        const std::vector<const containers::Match*>::iterator _begin,
        const std::vector<const containers::Match*>::iterator _end )
    {
        assert_true( _end >= _begin );

        // Note that we are sorting in DESCENDING order!

        std::sort(
            _begin,
            _end,
            [&_input, &_output](
                const containers::Match* m1, const containers::Match* m2 ) {
                assert_true( m1->ix_input < _input.nrows() );
                assert_true( m2->ix_input < _input.nrows() );

                assert_true( m1->ix_output < _output.nrows() );
                assert_true( m2->ix_output < _output.nrows() );

                return ( _output.time_stamp( m1->ix_output ) -
                         _input.time_stamp( m1->ix_input ) ) >
                       ( _output.time_stamp( m2->ix_output ) -
                         _input.time_stamp( m2->ix_input ) );
            } );
    }
};

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace relboost

// ----------------------------------------------------------------------------

#endif  // RELBOOST_UTILS_SORTER_HPP_