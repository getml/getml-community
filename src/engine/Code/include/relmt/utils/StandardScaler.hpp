
#ifndef RELMT_UTILS_STANDARDSCALER_HPP_
#define RELMT_UTILS_STANDARDSCALER_HPP_

// ----------------------------------------------------------------------------

namespace relmt
{
namespace utils
{
// ------------------------------------------------------------------------

class StandardScaler
{
   public:
    StandardScaler() {}

    StandardScaler( const Poco::JSON::Object& _obj )
    {
        means_ =
            JSON::array_to_vector<Float>( JSON::get_array( _obj, "means_" ) );

        inverse_stddev_ = JSON::array_to_vector<Float>(
            JSON::get_array( _obj, "inverse_stddev_" ) );
    }

    ~StandardScaler() = default;

   public:
    /// Rescales all numerical columns, discrete columns and subfeatures to zero
    /// mean and unit standard deviation.
    template <class DataFrameType>
    containers::Rescaled fit_transform(
        const DataFrameType& _df,
        const std::optional<containers::Subfeatures>& _subfeatures,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _end );

    /// Rescales all numerical columns, discrete columns and subfeatures to zero
    /// mean and unit standard deviation, but uses the means and standard
    /// deviations from the training set.
    template <class DataFrameType>
    containers::Rescaled transform(
        const DataFrameType& _df,
        const std::optional<containers::Subfeatures>& _subfeatures,
        const std::shared_ptr<containers::Rescaled::MapType>& _rows_map,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _end ) const;

   public:
    /// Trivial (const) accessor
    const std::vector<Float>& inverse_stddev() const { return inverse_stddev_; }

    /// Trivial (const) accessor
    const std::vector<Float>& means() const { return means_; }

    /// Expresses the StandardScaler as a JSON object.
    Poco::JSON::Object::Ptr to_json_obj() const
    {
        auto obj = Poco::JSON::Object::Ptr( new Poco::JSON::Object() );
        obj->set( "means_", JSON::vector_to_array( means_ ) );
        obj->set( "inverse_stddev_", JSON::vector_to_array( inverse_stddev_ ) );
        return obj;
    }

   private:
    /// Helper function that allows to reduce duplicate code in calc_mean and
    /// calc_variance.
    template <class ColumnType, class OpType>
    Float apply_operator(
        const ColumnType& _col,
        const OpType& _op,
        const std::vector<size_t>& _unique_indices ) const;

    /// Rescales all numerical columns, discrete columns and subfeatures to zero
    /// mean and unit standard deviation given the means and inverse standard
    /// deviations.
    template <class DataFrameType>
    containers::Rescaled calc_rescaled(
        const std::vector<Float>& _means,
        const std::vector<Float>& _inverse_stddev,
        const DataFrameType& _df,
        const std::optional<containers::Subfeatures>& _subfeatures,
        const std::shared_ptr<const containers::Rescaled::MapType>& _rows_map,
        const std::vector<size_t>& _unique_indices ) const;

    /// Calculates the map and unique indices.
    template <class DataFrameType>
    std::pair<
        std::shared_ptr<const containers::Rescaled::MapType>,
        std::vector<size_t>>
    calc_rows_map(
        const DataFrameType& _df,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _end,
        const std::shared_ptr<containers::Rescaled::MapType> _rows_map =
            nullptr ) const;

    /// Calculates the mean of a single column or subfeature.
    template <class ColumnType>
    Float calc_mean(
        const ColumnType& _col,
        const std::vector<size_t>& _unique_indices ) const;

    /// Calculates the means for each column and subfeature.
    template <class DataFrameType>
    std::vector<Float> calc_means(
        const DataFrameType& _df,
        const std::optional<containers::Subfeatures>& _subfeatures,
        const std::vector<size_t>& _unique_indices ) const;

    /// Calculates the means for each column and subfeature.
    template <class DataFrameType>
    std::vector<Float> calc_inverse_stddev(
        const std::vector<Float>& _means,
        const DataFrameType& _df,
        const std::optional<containers::Subfeatures>& _subfeatures,
        const std::vector<size_t>& _unique_indices ) const;

    /// Calculates the variance of a column.
    template <class ColumnType>
    Float calc_variance(
        const ColumnType& _col,
        const Float _mean,
        const std::vector<size_t>& _unique_indices ) const;

   private:
    /// The means taken from the training set.
    std::vector<Float> means_;

    /// The inverse standard deviations taken from the training set.
    std::vector<Float> inverse_stddev_;
};

// ------------------------------------------------------------------------
// ------------------------------------------------------------------------

template <class ColumnType, class OpType>
Float StandardScaler::apply_operator(
    const ColumnType& _col,
    const OpType& _op,
    const std::vector<size_t>& _unique_indices ) const
{
    if ( _unique_indices.size() == 0 )
        {
            return 0.0;
        }

    Float sum = 0.0;

    for ( auto ix : _unique_indices )
        {
            sum = _op( sum, _col[ix] );
        }

    return sum / static_cast<Float>( _unique_indices.size() );
}

// ------------------------------------------------------------------------

template <class ColumnType>
Float StandardScaler::calc_mean(
    const ColumnType& _col, const std::vector<size_t>& _unique_indices ) const
{
    const auto op = []( Float init, Float val ) {
        if ( std::isinf( val ) || std::isnan( val ) )
            {
                return init;
            }
        return init + val;
    };

    return apply_operator( _col, op, _unique_indices );
}

// ----------------------------------------------------------------------------

template <class DataFrameType>
std::vector<Float> StandardScaler::calc_means(
    const DataFrameType& _df,
    const std::optional<containers::Subfeatures>& _subfeatures,
    const std::vector<size_t>& _unique_indices ) const
{
    std::vector<Float> means;

    for ( size_t j = 0; j < _df.num_discretes(); ++j )
        {
            means.push_back(
                calc_mean( _df.discrete_col( j ), _unique_indices ) );
        }

    for ( size_t j = 0; j < _df.num_numericals(); ++j )
        {
            means.push_back(
                calc_mean( _df.numerical_col( j ), _unique_indices ) );
        }

    if ( _subfeatures )
        {
            for ( size_t j = 0; j < _subfeatures->size(); ++j )
                {
                    means.push_back(
                        calc_mean( _subfeatures->at( j ), _unique_indices ) );
                }
        }

    return means;
}

// ----------------------------------------------------------------------------

template <class DataFrameType>
std::vector<Float> StandardScaler::calc_inverse_stddev(
    const std::vector<Float>& _means,
    const DataFrameType& _df,
    const std::optional<containers::Subfeatures>& _subfeatures,
    const std::vector<size_t>& _unique_indices ) const
{
    std::vector<Float> inverse_stddev( _means.size() );

    size_t i = 0;

    for ( size_t j = 0; j < _df.num_discretes(); ++i, ++j )
        {
            assert_true( i < _means.size() );
            inverse_stddev.at( i ) = calc_variance(
                _df.discrete_col( j ), _means.at( i ), _unique_indices );
        }

    for ( size_t j = 0; j < _df.num_numericals(); ++i, ++j )
        {
            assert_true( i < _means.size() );
            inverse_stddev.at( i ) = calc_variance(
                _df.numerical_col( j ), _means.at( i ), _unique_indices );
        }

    if ( _subfeatures )
        {
            for ( size_t j = 0; j < _subfeatures->size(); ++i, ++j )
                {
                    inverse_stddev.at( i ) = calc_variance(
                        _subfeatures->at( j ),
                        _means.at( i ),
                        _unique_indices );
                }
        }

    const auto sqrt_invert = []( Float val ) {
        if ( val == 0.0 )
            {
                return val;
            }
        return 1.0 / std::sqrt( val );
    };

    std::transform(
        inverse_stddev.begin(),
        inverse_stddev.end(),
        inverse_stddev.begin(),
        sqrt_invert );

    return inverse_stddev;
}

// ----------------------------------------------------------------------------

template <class DataFrameType>
containers::Rescaled StandardScaler::calc_rescaled(
    const std::vector<Float>& _means,
    const std::vector<Float>& _inverse_stddev,
    const DataFrameType& _df,
    const std::optional<containers::Subfeatures>& _subfeatures,
    const std::shared_ptr<const containers::Rescaled::MapType>& _rows_map,
    const std::vector<size_t>& _unique_indices ) const
{
    assert_true( _rows_map );

    const auto map_size = _unique_indices.size();

    const auto nrows = _df.nrows();

    const auto ncols =
        ( _subfeatures )
            ? _df.num_discretes() + _df.num_numericals() + _subfeatures->size()
            : _df.num_discretes() + _df.num_numericals();

    const auto data = std::make_shared<std::vector<Float>>( map_size * ncols );

    const auto rescale = [&_means, &_inverse_stddev]( Float val, size_t k ) {
        if ( std::isinf( val ) || std::isnan( val ) )
            {
                return 0.0;
            }
        return ( val - _means.at( k ) ) * _inverse_stddev.at( k );
    };

    size_t k = 0;

    for ( size_t j = 0; j < _df.num_discretes(); ++j, ++k )
        {
            assert_true( k < _means.size() );

            for ( size_t i = 0; i < map_size; ++i )
                {
                    const auto ix = _unique_indices.at( i );

                    data->at( i * ncols + k ) =
                        rescale( _df.discrete( ix, j ), k );
                }
        }

    for ( size_t j = 0; j < _df.num_numericals(); ++j, ++k )
        {
            assert_true( k < _means.size() );

            for ( size_t i = 0; i < map_size; ++i )
                {
                    const auto ix = _unique_indices.at( i );

                    data->at( i * ncols + k ) =
                        rescale( _df.numerical( ix, j ), k );
                }
        }

    if ( _subfeatures )
        {
            for ( size_t j = 0; j < _subfeatures->size(); ++j, ++k )
                {
                    assert_true( k < _means.size() );

                    for ( size_t i = 0; i < map_size; ++i )
                        {
                            const auto ix = _unique_indices.at( i );

                            data->at( i * ncols + k ) =
                                rescale( _subfeatures->at( j )[ix], k );
                        }
                }
        }

    return containers::Rescaled( data, nrows, ncols, _rows_map );
}

// ------------------------------------------------------------------------

template <class DataFrameType>
std::pair<
    std::shared_ptr<const containers::Rescaled::MapType>,
    std::vector<size_t>>
StandardScaler::calc_rows_map(
    const DataFrameType& _df,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _end,
    const std::shared_ptr<containers::Rescaled::MapType> _rows_map ) const
{
    std::set<size_t> unique_indices;

    assert_true( _end >= _begin );

    if constexpr ( std::is_same<DataFrameType, containers::DataFrameView>() )
        {
            for ( auto it = _begin; it != _end; ++it )
                {
                    unique_indices.insert( it->ix_output );
                }
        }

    if constexpr ( std::is_same<DataFrameType, containers::DataFrame>() )
        {
            for ( auto it = _begin; it != _end; ++it )
                {
                    unique_indices.insert( it->ix_input );
                }
        }

    const auto rows_map = _rows_map
                              ? _rows_map
                              : std::make_shared<containers::Rescaled::MapType>(
                                    _df.nrows(), _df.nrows() );

    assert_true( rows_map->size() == _df.nrows() );

    size_t i = 0;

    for ( auto ix : unique_indices )
        {
            ( *rows_map )[ix] = i++;
        }

    return std::make_pair(
        rows_map,
        std::vector<size_t>( unique_indices.begin(), unique_indices.end() ) );
}

// ------------------------------------------------------------------------

template <class ColumnType>
Float StandardScaler::calc_variance(
    const ColumnType& _col,
    const Float _mean,
    const std::vector<size_t>& _unique_indices ) const
{
    const auto op = [_mean]( Float init, Float val ) {
        if ( std::isinf( val ) || std::isnan( val ) )
            {
                return init;
            }
        return init + ( val - _mean ) * ( val - _mean );
    };

    return apply_operator( _col, op, _unique_indices );
}

// ------------------------------------------------------------------------

template <class DataFrameType>
containers::Rescaled StandardScaler::fit_transform(
    const DataFrameType& _df,
    const std::optional<containers::Subfeatures>& _subfeatures,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _end )
{
    const auto [rows_map, unique_indices] = calc_rows_map( _df, _begin, _end );

    means_ = calc_means( _df, _subfeatures, unique_indices );

    inverse_stddev_ =
        calc_inverse_stddev( means_, _df, _subfeatures, unique_indices );

    return calc_rescaled(
        means_, inverse_stddev_, _df, _subfeatures, rows_map, unique_indices );
}

// ----------------------------------------------------------------------------

template <class DataFrameType>
containers::Rescaled StandardScaler::transform(
    const DataFrameType& _df,
    const std::optional<containers::Subfeatures>& _subfeatures,
    const std::shared_ptr<containers::Rescaled::MapType>& _rows_map,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _end ) const
{
    assert_true( means_.size() == inverse_stddev_.size() );

    const auto [rows_map, unique_indices] =
        calc_rows_map( _df, _begin, _end, _rows_map );

    return calc_rescaled(
        means_, inverse_stddev_, _df, _subfeatures, rows_map, unique_indices );
}

// ------------------------------------------------------------------------
}  // namespace utils
}  // namespace relmt

#endif  // RELMT_UTILS_STANDARDSCALER_HPP_

