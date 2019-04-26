#ifndef RELBOOST_UTILS_FINDER_HPP_
#define RELBOOST_UTILS_FINDER_HPP_

// ----------------------------------------------------------------------------

namespace relboost
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
    static std::vector<const containers::Match*>::iterator next_split(
        const RELBOOST_FLOAT _cv,
        const size_t _num_column,
        const containers::DataFrame& _input,
        const std::vector<const containers::Match*>::iterator _begin,
        const std::vector<const containers::Match*>::iterator _end )
    {
        const auto smaller_than_cv =
            [&_input, _num_column, _cv]( const containers::Match* m ) {
                assert( m->ix_input < _input.nrows() );
                return _input.discrete( m->ix_input, _num_column ) <= _cv;
            };

        return std::find_if( _begin, _end, smaller_than_cv );
    }
};

// ----------------------------------------------------------------------------

template <>
struct Finder<enums::DataUsed::discrete_output>
{
    static std::vector<const containers::Match*>::iterator next_split(
        const RELBOOST_FLOAT _cv,
        const size_t _num_column,
        const containers::DataFrame& _output,
        const std::vector<const containers::Match*>::iterator _begin,
        const std::vector<const containers::Match*>::iterator _end )
    {
        const auto smaller_than_cv =
            [&_output, _num_column, _cv]( const containers::Match* m ) {
                assert( m->ix_output < _output.nrows() );
                return _output.discrete( m->ix_output, _num_column ) <= _cv;
            };

        return std::find_if( _begin, _end, smaller_than_cv );
    }
};

// ----------------------------------------------------------------------------

template <>
struct Finder<enums::DataUsed::numerical_input>
{
    static std::vector<const containers::Match*>::iterator next_split(
        const RELBOOST_FLOAT _cv,
        const size_t _num_column,
        const containers::DataFrame& _input,
        const std::vector<const containers::Match*>::iterator _begin,
        const std::vector<const containers::Match*>::iterator _end )
    {
        const auto smaller_than_cv =
            [&_input, _num_column, _cv]( const containers::Match* m ) {
                assert( m->ix_input < _input.nrows() );
                return _input.numerical( m->ix_input, _num_column ) <= _cv;
            };

        return std::find_if( _begin, _end, smaller_than_cv );
    }
};

// ----------------------------------------------------------------------------

template <>
struct Finder<enums::DataUsed::numerical_output>
{
    static std::vector<const containers::Match*>::iterator next_split(
        const RELBOOST_FLOAT _cv,
        const size_t _num_column,
        const containers::DataFrame& _output,
        const std::vector<const containers::Match*>::iterator _begin,
        const std::vector<const containers::Match*>::iterator _end )
    {
        const auto smaller_than_cv =
            [&_output, _num_column, _cv]( const containers::Match* m ) {
                assert( m->ix_output < _output.nrows() );
                return _output.numerical( m->ix_output, _num_column ) <= _cv;
            };

        return std::find_if( _begin, _end, smaller_than_cv );
    }
};

// ----------------------------------------------------------------------------

template <>
struct Finder<enums::DataUsed::same_units_discrete>
{
    static std::vector<const containers::Match*>::iterator next_split(
        const RELBOOST_FLOAT _cv,
        const size_t _input_col,
        const size_t _output_col,
        const containers::DataFrame& _input,
        const containers::DataFrame& _output,
        const std::vector<const containers::Match*>::iterator _begin,
        const std::vector<const containers::Match*>::iterator _end )
    {
        auto smaller_than_cv = [&_input,
                                &_output,
                                _input_col,
                                _output_col,
                                _cv]( const containers::Match* m ) {
            assert( m->ix_input < _input.nrows() );
            assert( m->ix_output < _output.nrows() );

            const auto diff = _output.discrete( m->ix_output, _output_col ) -
                              _input.discrete( m->ix_input, _input_col );

            return ( diff <= _cv );
        };

        return std::find_if( _begin, _end, smaller_than_cv );
    }
};

// ----------------------------------------------------------------------------

template <>
struct Finder<enums::DataUsed::same_units_numerical>
{
    static std::vector<const containers::Match*>::iterator next_split(
        const RELBOOST_FLOAT _cv,
        const size_t _input_col,
        const size_t _output_col,
        const containers::DataFrame& _input,
        const containers::DataFrame& _output,
        const std::vector<const containers::Match*>::iterator _begin,
        const std::vector<const containers::Match*>::iterator _end )
    {
        auto smaller_than_cv = [&_input,
                                &_output,
                                _input_col,
                                _output_col,
                                _cv]( const containers::Match* m ) {
            assert( m->ix_input < _input.nrows() );
            assert( m->ix_output < _output.nrows() );

            const auto diff = _output.numerical( m->ix_output, _output_col ) -
                              _input.numerical( m->ix_input, _input_col );

            return ( diff <= _cv );
        };

        return std::find_if( _begin, _end, smaller_than_cv );
    }
};

// ----------------------------------------------------------------------------

template <>
struct Finder<enums::DataUsed::time_stamps_diff>
{
    static std::vector<const containers::Match*>::iterator next_split(
        const RELBOOST_FLOAT _cv,
        const containers::DataFrame& _input,
        const containers::DataFrame& _output,
        const std::vector<const containers::Match*>::iterator _begin,
        const std::vector<const containers::Match*>::iterator _end )
    {
        auto smaller_than_cv =
            [&_input, &_output, _cv]( const containers::Match* m ) {
                assert( m->ix_input < _input.nrows() );
                assert( m->ix_output < _output.nrows() );

                const auto diff = _output.time_stamp( m->ix_output ) -
                                  _input.time_stamp( m->ix_input );

                return ( diff <= _cv );
            };

        return std::find_if( _begin, _end, smaller_than_cv );
    }
};

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace relboost

// ----------------------------------------------------------------------------

#endif  // RELBOOST_UTILS_FINDER_HPP_