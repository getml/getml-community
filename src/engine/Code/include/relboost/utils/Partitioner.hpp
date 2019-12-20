#ifndef RELBOOST_UTILS_PARTITIONER_HPP_
#define RELBOOST_UTILS_PARTITIONER_HPP_

// ----------------------------------------------------------------------------

namespace relboost
{
namespace utils
{
// ----------------------------------------------------------------------------
// This uses template specialization to implement different partition functions
// for the different enumerations of DataUsed.

template <enums::DataUsed>
struct Partitioner
{
};

// ----------------------------------------------------------------------------

template <>
struct Partitioner<enums::DataUsed::categorical_input>
{
    // --------------------------------------------------------------------

    static std::vector<containers::Match>::iterator partition(
        const containers::Split& _split,
        const containers::DataFrame& _input,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _end )
    {
        const auto partition_function = [&_split,
                                         &_input]( containers::Match m ) {
            return is_greater( _split, _input, m );
        };

        return std::partition( _begin, _end, partition_function );
    }

    // --------------------------------------------------------------------

    static bool is_greater(
        const containers::Split& _split,
        const containers::DataFrame& _input,
        const containers::Match& _match )
    {
        const auto i = _match.ix_input;
        const auto j = _split.column_;

        assert_true( i < _input.nrows() );
        assert_true( j < _input.num_categoricals() );

        const auto it = std::find(
            _split.categories_used_begin_,
            _split.categories_used_end_,
            _input.categorical( i, j ) );

        return it != _split.categories_used_end_;
    }

    // --------------------------------------------------------------------
};

// ----------------------------------------------------------------------------

template <>
struct Partitioner<enums::DataUsed::categorical_output>
{
    // --------------------------------------------------------------------

    static std::vector<containers::Match>::iterator partition(
        const containers::Split& _split,
        const containers::DataFrameView& _output,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _end )
    {
        const auto partition_function = [_split,
                                         &_output]( containers::Match m ) {
            return is_greater( _split, _output, m );
        };

        return std::partition( _begin, _end, partition_function );
    }

    // --------------------------------------------------------------------

    static bool is_greater(
        const containers::Split& _split,
        const containers::DataFrameView& _output,
        const containers::Match& _match )
    {
        const auto i = _match.ix_output;
        const auto j = _split.column_;

        assert_true( i < _output.nrows() );
        assert_true( j < _output.num_categoricals() );

        const auto it = std::find(
            _split.categories_used_begin_,
            _split.categories_used_end_,
            _output.categorical( i, j ) );

        return it != _split.categories_used_end_;
    }

    // --------------------------------------------------------------------
};

// ----------------------------------------------------------------------------

template <>
struct Partitioner<enums::DataUsed::discrete_input>
{
    // --------------------------------------------------------------------

    static std::vector<containers::Match>::iterator partition(
        const containers::Split& _split,
        const containers::DataFrame& _input,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _end )
    {
        const auto partition_function = [&_split,
                                         &_input]( containers::Match m ) {
            return is_greater( _split, _input, m );
        };

        return std::partition( _begin, _end, partition_function );
    }

    // --------------------------------------------------------------------

    static bool is_greater(
        const containers::Split& _split,
        const containers::DataFrame& _input,
        const containers::Match& _match )
    {
        const auto i = _match.ix_input;
        const auto j = _split.column_;

        assert_true( i < _input.nrows() );
        assert_true( j < _input.num_discretes() );

        return _input.discrete( i, j ) > _split.critical_value_;
    }

    // --------------------------------------------------------------------
};

// ----------------------------------------------------------------------------

template <>
struct Partitioner<enums::DataUsed::discrete_input_is_nan>
{
    static std::vector<containers::Match>::iterator partition(
        const size_t _num_column,
        const containers::DataFrame& _input,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _end )
    {
        assert_true( _end >= _begin );

        return std::partition(
            _begin, _end, [_num_column, &_input]( containers::Match m ) {
                return is_greater( _num_column, _input, m );
            } );
    }

    // --------------------------------------------------------------------

    static bool is_greater(
        const size_t _num_column,
        const containers::DataFrame& _input,
        const containers::Match& _match )
    {
        const auto i = _match.ix_input;

        assert_true( i < _input.nrows() );
        assert_true( _num_column < _input.num_discretes() );

        return !std::isnan( _input.discrete( _match.ix_input, _num_column ) );
    }

    // --------------------------------------------------------------------
};

// ----------------------------------------------------------------------------

template <>
struct Partitioner<enums::DataUsed::discrete_output>
{
    // --------------------------------------------------------------------

    static std::vector<containers::Match>::iterator partition(
        const containers::Split& _split,
        const containers::DataFrameView& _output,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _end )
    {
        const auto partition_function = [_split,
                                         &_output]( containers::Match m ) {
            return is_greater( _split, _output, m );
        };

        return std::partition( _begin, _end, partition_function );
    }

    // --------------------------------------------------------------------

    static bool is_greater(
        const containers::Split& _split,
        const containers::DataFrameView& _output,
        const containers::Match& _match )
    {
        const auto i = _match.ix_output;
        const auto j = _split.column_;

        assert_true( i < _output.nrows() );
        assert_true( j < _output.num_discretes() );

        return ( _output.discrete( i, j ) > _split.critical_value_ );
    }

    // --------------------------------------------------------------------
};

// ----------------------------------------------------------------------------

template <>
struct Partitioner<enums::DataUsed::discrete_output_is_nan>
{
    static std::vector<containers::Match>::iterator partition(
        const size_t _num_column,
        const containers::DataFrameView& _output,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _end )
    {
        assert_true( _end >= _begin );

        return std::partition(
            _begin, _end, [_num_column, &_output]( containers::Match m ) {
                return is_greater( _num_column, _output, m );
            } );
    }

    // --------------------------------------------------------------------

    static bool is_greater(
        const size_t _num_column,
        const containers::DataFrameView& _output,
        const containers::Match& _match )
    {
        const auto i = _match.ix_output;

        assert_true( i < _output.nrows() );
        assert_true( _num_column < _output.num_discretes() );

        return !std::isnan( _output.discrete( _match.ix_output, _num_column ) );
    }

    // --------------------------------------------------------------------
};

// ----------------------------------------------------------------------------

template <>
struct Partitioner<enums::DataUsed::numerical_input>
{
    // --------------------------------------------------------------------

    static std::vector<containers::Match>::iterator partition(
        const containers::Split& _split,
        const containers::DataFrame& _input,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _end )
    {
        const auto partition_function = [&_split,
                                         &_input]( containers::Match m ) {
            return is_greater( _split, _input, m );
        };

        return std::partition( _begin, _end, partition_function );
    }

    // --------------------------------------------------------------------

    static bool is_greater(
        const containers::Split& _split,
        const containers::DataFrame& _input,
        const containers::Match& _match )
    {
        const auto i = _match.ix_input;
        const auto j = _split.column_;

        assert_true( i < _input.nrows() );
        assert_true( j < _input.num_numericals() );

        return _input.numerical( i, j ) > _split.critical_value_;
    }

    // --------------------------------------------------------------------
};

// ----------------------------------------------------------------------------

template <>
struct Partitioner<enums::DataUsed::numerical_input_is_nan>
{
    static std::vector<containers::Match>::iterator partition(
        const size_t _num_column,
        const containers::DataFrame& _input,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _end )
    {
        assert_true( _end >= _begin );

        return std::partition(
            _begin, _end, [_num_column, &_input]( containers::Match m ) {
                return is_greater( _num_column, _input, m );
            } );
    }

    // --------------------------------------------------------------------

    static bool is_greater(
        const size_t _num_column,
        const containers::DataFrame& _input,
        const containers::Match& _match )
    {
        const auto i = _match.ix_input;

        assert_true( i < _input.nrows() );
        assert_true( _num_column < _input.num_numericals() );

        return !std::isnan( _input.numerical( _match.ix_input, _num_column ) );
    }

    // --------------------------------------------------------------------
};

// ----------------------------------------------------------------------------

template <>
struct Partitioner<enums::DataUsed::numerical_output>
{
    // --------------------------------------------------------------------

    static std::vector<containers::Match>::iterator partition(
        const containers::Split& _split,
        const containers::DataFrameView& _output,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _end )
    {
        const auto partition_function = [_split,
                                         &_output]( containers::Match m ) {
            return is_greater( _split, _output, m );
        };

        return std::partition( _begin, _end, partition_function );
    }

    // --------------------------------------------------------------------

    static bool is_greater(
        const containers::Split& _split,
        const containers::DataFrameView& _output,
        const containers::Match& _match )
    {
        const auto i = _match.ix_output;
        const auto j = _split.column_;

        assert_true( i < _output.nrows() );
        assert_true( j < _output.num_numericals() );

        return ( _output.numerical( i, j ) > _split.critical_value_ );
    }

    // --------------------------------------------------------------------
};

// ----------------------------------------------------------------------------

template <>
struct Partitioner<enums::DataUsed::numerical_output_is_nan>
{
    static std::vector<containers::Match>::iterator partition(
        const size_t _num_column,
        const containers::DataFrameView& _output,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _end )
    {
        assert_true( _end >= _begin );

        return std::partition(
            _begin, _end, [_num_column, &_output]( containers::Match m ) {
                return is_greater( _num_column, _output, m );
            } );
    }

    // --------------------------------------------------------------------

    static bool is_greater(
        const size_t _num_column,
        const containers::DataFrameView& _output,
        const containers::Match& _match )
    {
        const auto i = _match.ix_output;

        assert_true( i < _output.nrows() );
        assert_true( _num_column < _output.num_numericals() );

        return !std::isnan(
            _output.numerical( _match.ix_output, _num_column ) );
    }

    // --------------------------------------------------------------------
};

// ----------------------------------------------------------------------------

template <>
struct Partitioner<enums::DataUsed::same_units_categorical>
{
    // --------------------------------------------------------------------

    static std::vector<containers::Match>::iterator partition(
        const containers::Split& _split,
        const containers::DataFrame& _input,
        const containers::DataFrameView& _output,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _end )
    {
        const auto partition_function =
            [_split, &_input, &_output]( containers::Match m ) {
                return is_greater( _split, _input, _output, m );
            };

        return std::partition( _begin, _end, partition_function );
    }

    // --------------------------------------------------------------------

    static bool is_greater(
        const containers::Split& _split,
        const containers::DataFrame& _input,
        const containers::DataFrameView& _output,
        const containers::Match& _match )
    {
        assert_true( _match.ix_input < _input.nrows() );
        assert_true( _match.ix_output < _output.nrows() );

        assert_true( _split.column_input_ < _input.num_categoricals() );
        assert_true( _split.column_ < _output.num_categoricals() );

        const bool is_same =
            ( _input.categorical( _match.ix_input, _split.column_input_ ) ==
              _output.categorical( _match.ix_output, _split.column_ ) );

        return is_same;
    }

    // --------------------------------------------------------------------
};

// ----------------------------------------------------------------------------

template <>
struct Partitioner<enums::DataUsed::same_units_discrete>
{
    // --------------------------------------------------------------------

    static std::vector<containers::Match>::iterator partition(
        const containers::Split& _split,
        const containers::DataFrame& _input,
        const containers::DataFrameView& _output,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _end )
    {
        const auto partition_function =
            [&_split, &_input, &_output]( containers::Match m ) {
                return is_greater( _split, _input, _output, m );
            };

        return std::partition( _begin, _end, partition_function );
    }

    // --------------------------------------------------------------------

    static bool is_greater(
        const containers::Split& _split,
        const containers::DataFrame& _input,
        const containers::DataFrameView& _output,
        const containers::Match& _match )
    {
        assert_true( _match.ix_input < _input.nrows() );
        assert_true( _match.ix_output < _output.nrows() );

        assert_true( _split.column_input_ < _input.num_discretes() );
        assert_true( _split.column_ < _output.num_discretes() );

        const auto diff =
            _output.discrete( _match.ix_output, _split.column_ ) -
            _input.discrete( _match.ix_input, _split.column_input_ );

        return ( diff > _split.critical_value_ );
    }

    // --------------------------------------------------------------------
};

// ----------------------------------------------------------------------------

template <>
struct Partitioner<enums::DataUsed::same_units_discrete_is_nan>
{
    // --------------------------------------------------------------------

    static std::vector<containers::Match>::iterator partition(
        const size_t _input_col,
        const size_t _output_col,
        const containers::DataFrame& _input,
        const containers::DataFrameView& _output,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _end )
    {
        const auto partition_function = [_input_col,
                                         _output_col,
                                         &_input,
                                         &_output]( containers::Match m ) {
            return is_greater( _input_col, _output_col, _input, _output, m );
        };

        return std::partition( _begin, _end, partition_function );
    }

    // --------------------------------------------------------------------

    static bool is_greater(
        const size_t _input_col,
        const size_t _output_col,
        const containers::DataFrame& _input,
        const containers::DataFrameView& _output,
        const containers::Match& _match )
    {
        assert_true( _match.ix_input < _input.nrows() );
        assert_true( _match.ix_output < _output.nrows() );

        assert_true( _input_col < _input.num_discretes() );
        assert_true( _output_col < _output.num_discretes() );

        const auto val1 = _input.discrete( _match.ix_input, _input_col );
        const auto val2 = _output.discrete( _match.ix_output, _output_col );

        return !std::isnan( val1 ) && !std::isnan( val2 );
    }

    // --------------------------------------------------------------------
};

// ----------------------------------------------------------------------------

template <>
struct Partitioner<enums::DataUsed::same_units_numerical>
{
    // --------------------------------------------------------------------

    static std::vector<containers::Match>::iterator partition(
        const containers::Split& _split,
        const containers::DataFrame& _input,
        const containers::DataFrameView& _output,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _end )
    {
        const auto partition_function =
            [&_split, &_input, &_output]( containers::Match m ) {
                return is_greater( _split, _input, _output, m );
            };

        return std::partition( _begin, _end, partition_function );
    }

    // --------------------------------------------------------------------

    static bool is_greater(
        const containers::Split& _split,
        const containers::DataFrame& _input,
        const containers::DataFrameView& _output,
        const containers::Match& _match )
    {
        assert_true( _match.ix_input < _input.nrows() );
        assert_true( _match.ix_output < _output.nrows() );

        assert_true( _split.column_input_ < _input.num_numericals() );
        assert_true( _split.column_ < _output.num_numericals() );

        const auto diff =
            _output.numerical( _match.ix_output, _split.column_ ) -
            _input.numerical( _match.ix_input, _split.column_input_ );

        return ( diff > _split.critical_value_ );
    }

    // --------------------------------------------------------------------
};

// ----------------------------------------------------------------------------

template <>
struct Partitioner<enums::DataUsed::same_units_numerical_is_nan>
{
    // --------------------------------------------------------------------

    static std::vector<containers::Match>::iterator partition(
        const size_t _input_col,
        const size_t _output_col,
        const containers::DataFrame& _input,
        const containers::DataFrameView& _output,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _end )
    {
        const auto partition_function = [_input_col,
                                         _output_col,
                                         &_input,
                                         &_output]( containers::Match m ) {
            return is_greater( _input_col, _output_col, _input, _output, m );
        };

        return std::partition( _begin, _end, partition_function );
    }

    // --------------------------------------------------------------------

    static bool is_greater(
        const size_t _input_col,
        const size_t _output_col,
        const containers::DataFrame& _input,
        const containers::DataFrameView& _output,
        const containers::Match& _match )
    {
        assert_true( _match.ix_input < _input.nrows() );
        assert_true( _match.ix_output < _output.nrows() );

        assert_true( _input_col < _input.num_numericals() );
        assert_true( _output_col < _output.num_numericals() );

        const auto val1 = _input.numerical( _match.ix_input, _input_col );
        const auto val2 = _output.numerical( _match.ix_output, _output_col );

        return !std::isnan( val1 ) && !std::isnan( val2 );
    }

    // --------------------------------------------------------------------
};

// ----------------------------------------------------------------------------

template <>
struct Partitioner<enums::DataUsed::subfeatures>
{
    // --------------------------------------------------------------------

    static std::vector<containers::Match>::iterator partition(
        const containers::Split& _split,
        const containers::Subfeatures& _subfeatures,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _end )
    {
        const auto partition_function = [&_split,
                                         &_subfeatures]( containers::Match m ) {
            return is_greater( _split, _subfeatures, m );
        };

        return std::partition( _begin, _end, partition_function );
    }

    // --------------------------------------------------------------------

    static bool is_greater(
        const containers::Split& _split,
        const containers::Subfeatures& _subfeatures,
        const containers::Match& _match )
    {
        const auto i = _match.ix_input;
        const auto j = _split.column_;

        assert_true( j < _subfeatures.size() );

        return _subfeatures[j][i] > _split.critical_value_;
    }

    // --------------------------------------------------------------------
};

// ----------------------------------------------------------------------------

template <>
struct Partitioner<enums::DataUsed::time_stamps_diff>
{
    // --------------------------------------------------------------------

    static std::vector<containers::Match>::iterator partition(
        const containers::Split& _split,
        const containers::DataFrame& _input,
        const containers::DataFrameView& _output,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _end )
    {
        const auto partition_function =
            [_split, &_input, &_output]( containers::Match m ) {
                return is_greater( _split, _input, _output, m );
            };

        return std::partition( _begin, _end, partition_function );
    }

    // --------------------------------------------------------------------

    static bool is_greater(
        const containers::Split& _split,
        const containers::DataFrame& _input,
        const containers::DataFrameView& _output,
        const containers::Match& _match )
    {
        const auto in = _match.ix_input;
        const auto out = _match.ix_output;

        assert_true( in < _input.nrows() );
        assert_true( out < _output.nrows() );

        return (
            _output.time_stamp( out ) - _input.time_stamp( in ) >
            _split.critical_value_ );
    }

    // --------------------------------------------------------------------
};

// ----------------------------------------------------------------------------

template <>
struct Partitioner<enums::DataUsed::time_stamps_window>
{
    // --------------------------------------------------------------------

    static std::vector<containers::Match>::iterator partition(
        const containers::Split& _split,
        const Float _lag,
        const containers::DataFrame& _input,
        const containers::DataFrameView& _output,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _end )
    {
        const auto partition_function =
            [_split, _lag, &_input, &_output]( containers::Match m ) {
                return is_greater( _split, _lag, _input, _output, m );
            };

        return std::partition( _begin, _end, partition_function );
    }

    // --------------------------------------------------------------------

    static bool is_greater(
        const containers::Split& _split,
        const Float _lag,
        const containers::DataFrame& _input,
        const containers::DataFrameView& _output,
        const containers::Match& _match )
    {
        const auto in = _match.ix_input;
        const auto out = _match.ix_output;

        assert_true( in < _input.nrows() );
        assert_true( out < _output.nrows() );

        const auto diff = _output.time_stamp( out ) - _input.time_stamp( in );

        return (
            ( diff > _split.critical_value_ ) &&
            ( diff <= _split.critical_value_ + _lag ) );
    }

    // --------------------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace relboost

// ----------------------------------------------------------------------------

#endif  // RELBOOST_UTILS_PARTITIONER_HPP_
