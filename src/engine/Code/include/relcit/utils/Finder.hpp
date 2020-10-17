#ifndef RELBOOSTXX_UTILS_FINDER_HPP_
#define RELBOOSTXX_UTILS_FINDER_HPP_

// ----------------------------------------------------------------------------

namespace relcit
{
namespace utils
{
// ----------------------------------------------------------------------------
// This uses template specialization to implement different functions
// for finding splits (for numerical and discrete only as categorical variables
// have the CategoryIndex that performs the same task.)

template <enums::DataUsed>
struct Finder
{
};

// ----------------------------------------------------------------------------

template <>
struct Finder<enums::DataUsed::discrete_input>
{
    static std::vector<containers::Match>::iterator next_split(
        const Float _cv,
        const size_t _num_column,
        const containers::DataFrame& _input,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _end )
    {
        const auto smaller_than_cv =
            [&_input, _num_column, _cv]( containers::Match m ) {
                assert_true( m.ix_input < _input.nrows() );
                return _input.discrete( m.ix_input, _num_column ) <= _cv;
            };

        return std::find_if( _begin, _end, smaller_than_cv );
    }
};

// ----------------------------------------------------------------------------

template <>
struct Finder<enums::DataUsed::discrete_output>
{
    static std::vector<containers::Match>::iterator next_split(
        const Float _cv,
        const size_t _num_column,
        const containers::DataFrameView& _output,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _end )
    {
        const auto smaller_than_cv =
            [&_output, _num_column, _cv]( containers::Match m ) {
                assert_true( m.ix_output < _output.nrows() );
                return _output.discrete( m.ix_output, _num_column ) <= _cv;
            };

        return std::find_if( _begin, _end, smaller_than_cv );
    }
};

// ----------------------------------------------------------------------------

template <>
struct Finder<enums::DataUsed::numerical_input>
{
    static std::vector<containers::Match>::iterator next_split(
        const Float _cv,
        const size_t _num_column,
        const containers::DataFrame& _input,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _end )
    {
        const auto smaller_than_cv =
            [&_input, _num_column, _cv]( containers::Match m ) {
                assert_true( m.ix_input < _input.nrows() );
                return _input.numerical( m.ix_input, _num_column ) <= _cv;
            };

        return std::find_if( _begin, _end, smaller_than_cv );
    }
};

// ----------------------------------------------------------------------------

template <>
struct Finder<enums::DataUsed::numerical_output>
{
    static std::vector<containers::Match>::iterator next_split(
        const Float _cv,
        const size_t _num_column,
        const containers::DataFrameView& _output,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _end )
    {
        const auto smaller_than_cv =
            [&_output, _num_column, _cv]( containers::Match m ) {
                assert_true( m.ix_output < _output.nrows() );
                return _output.numerical( m.ix_output, _num_column ) <= _cv;
            };

        return std::find_if( _begin, _end, smaller_than_cv );
    }
};

// ----------------------------------------------------------------------------

template <>
struct Finder<enums::DataUsed::same_units_discrete>
{
    static std::vector<containers::Match>::iterator next_split(
        const Float _cv,
        const size_t _input_col,
        const size_t _output_col,
        const containers::DataFrame& _input,
        const containers::DataFrameView& _output,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _end )
    {
        auto smaller_than_cv =
            [&_input, &_output, _input_col, _output_col, _cv](
                containers::Match m ) {
                assert_true( m.ix_input < _input.nrows() );
                assert_true( m.ix_output < _output.nrows() );

                const auto diff = _output.discrete( m.ix_output, _output_col ) -
                                  _input.discrete( m.ix_input, _input_col );

                return ( diff <= _cv );
            };

        return std::find_if( _begin, _end, smaller_than_cv );
    }
};

// ----------------------------------------------------------------------------

template <>
struct Finder<enums::DataUsed::same_units_numerical>
{
    static std::vector<containers::Match>::iterator next_split(
        const Float _cv,
        const size_t _input_col,
        const size_t _output_col,
        const containers::DataFrame& _input,
        const containers::DataFrameView& _output,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _end )
    {
        auto smaller_than_cv = [&_input,
                                &_output,
                                _input_col,
                                _output_col,
                                _cv]( containers::Match m ) {
            assert_true( m.ix_input < _input.nrows() );
            assert_true( m.ix_output < _output.nrows() );

            const auto diff = _output.numerical( m.ix_output, _output_col ) -
                              _input.numerical( m.ix_input, _input_col );

            return ( diff <= _cv );
        };

        return std::find_if( _begin, _end, smaller_than_cv );
    }
};

// ----------------------------------------------------------------------------

template <>
struct Finder<enums::DataUsed::subfeatures>
{
    static std::vector<containers::Match>::iterator next_split(
        const Float _cv,
        const size_t _num_column,
        const containers::Subfeatures& _subfeatures,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _end )
    {
        assert_true( _num_column < _subfeatures.size() );

        const auto& subfeature = _subfeatures[_num_column];

        const auto smaller_than_cv = [&subfeature, _cv]( containers::Match m ) {
            return subfeature[m.ix_input] <= _cv;
        };

        return std::find_if( _begin, _end, smaller_than_cv );
    }
};

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace relcit

// ----------------------------------------------------------------------------

#endif  // RELBOOSTXX_UTILS_FINDER_HPP_
