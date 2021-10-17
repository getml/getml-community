#ifndef ENGINE_CONTAINERS_ARRAYMAKER_HPP_
#define ENGINE_CONTAINERS_ARRAYMAKER_HPP_

namespace engine
{
namespace containers
{
// -------------------------------------------------------------------------

class ArrayMaker
{
   public:
    /// The maximum size for the the chunks.
    constexpr static size_t MAX_CHUNKSIZE = 100000;

    /// Generates a boolean array.
    template <class IteratorType>
    static std::shared_ptr<arrow::ChunkedArray> make_boolean_array(
        const IteratorType _begin, const IteratorType _end );

    /// Generates a float array.
    template <class IteratorType>
    static std::shared_ptr<arrow::ChunkedArray> make_float_array(
        const IteratorType _begin, const IteratorType _end );

    /// Generates a string array.
    template <class IteratorType>
    static std::shared_ptr<arrow::ChunkedArray> make_string_array(
        const IteratorType _begin, const IteratorType _end );

    /// Generates a time stamp array.
    template <class IteratorType>
    static std::shared_ptr<arrow::ChunkedArray> make_time_stamp_array(
        const IteratorType _begin, const IteratorType _end );

   private:
    /// Generate the chunks for the chunked array.
    template <class IteratorType, class AppendFunctionType, class BuilderType>
    static std::vector<std::shared_ptr<arrow::Array>> make_chunks(
        const IteratorType _begin,
        const IteratorType _end,
        const AppendFunctionType _append_function,
        BuilderType* _builder );
};

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

template <class IteratorType>
std::shared_ptr<arrow::ChunkedArray> ArrayMaker::make_boolean_array(
    const IteratorType _begin, const IteratorType _end )
{
    const auto append_function = []( const bool _val,
                                     arrow::BooleanBuilder* _builder ) {
#ifdef NDEBUG
        _builder->UnsafeAppend( _val );
#else
        _builder->Append( _val );
#endif
    };

    arrow::BooleanBuilder builder;

    auto chunks = make_chunks( _begin, _end, append_function, &builder );

    return std::make_shared<arrow::ChunkedArray>( std::move( chunks ) );
}

// -------------------------------------------------------------------------

template <class IteratorType, class AppendFunctionType, class BuilderType>
std::vector<std::shared_ptr<arrow::Array>> ArrayMaker::make_chunks(
    const IteratorType _begin,
    const IteratorType _end,
    const AppendFunctionType _append_function,
    BuilderType* _builder )
{
    _builder->Resize( MAX_CHUNKSIZE );

    std::vector<std::shared_ptr<arrow::Array>> chunks;

    for ( auto it = _begin; it != _end; )
        {
            for ( size_t i = 0; i < MAX_CHUNKSIZE && it != _end; ++i, ++it )
                {
                    _append_function( *it, _builder );
                }

            std::shared_ptr<arrow::Array> array;

            const auto status = _builder->Finish( &array );

            if ( !status.ok() )
                {
                    throw std::runtime_error(
                        "Could not create boolean array: " + status.message() );
                }

            chunks.emplace_back( std::move( array ) );

            _builder->Reset();
        }

    return chunks;
}

// ----------------------------------------------------------------------------

template <class IteratorType>
std::shared_ptr<arrow::ChunkedArray> ArrayMaker::make_float_array(
    const IteratorType _begin, const IteratorType _end )
{
    const auto append_function = []( const Float _val,
                                     arrow::DoubleBuilder* _builder ) {
        if ( helpers::NullChecker::is_null( _val ) )
            {
                _builder->AppendNull();
            }
        else
            {
#ifdef NDEBUG
                _builder->UnsafeAppend( _val );
#else
                _builder->Append( _val );
#endif
            }
    };

    arrow::DoubleBuilder builder;

    auto chunks = make_chunks( _begin, _end, append_function, &builder );

    return std::make_shared<arrow::ChunkedArray>( std::move( chunks ) );
}

// -------------------------------------------------------------------------

template <class IteratorType>
std::shared_ptr<arrow::ChunkedArray> ArrayMaker::make_string_array(
    const IteratorType _begin, const IteratorType _end )
{
    const auto append_function = []( const auto& _val,
                                     arrow::StringBuilder* _builder ) {
        if ( helpers::NullChecker::is_null( _val ) )
            {
                _builder->AppendNull();
            }
        else
            {
                // For some reason, UnsafeAppend
                // doesn't work for strings.
                _builder->Append( _val );
            }
    };

    arrow::StringBuilder builder;

    auto chunks = make_chunks( _begin, _end, append_function, &builder );

    return std::make_shared<arrow::ChunkedArray>( std::move( chunks ) );
}

// -------------------------------------------------------------------------

template <class IteratorType>
std::shared_ptr<arrow::ChunkedArray> ArrayMaker::make_time_stamp_array(
    IteratorType _begin, IteratorType _end )
{
    const auto append_function = []( const Float _val,
                                     arrow::TimestampBuilder* _builder ) {
        if ( helpers::NullChecker::is_null( _val ) )
            {
                _builder->AppendNull();
            }
        else
            {
#ifdef NDEBUG
                _builder->UnsafeAppend(
                    static_cast<std::int64_t>( _val * 1.0e+09 ) );
#else
                _builder->Append( static_cast<std::int64_t>( _val * 1.0e+09 ) );
#endif
            }
    };

    arrow::TimestampBuilder builder(
        arrow::timestamp( arrow::TimeUnit::NANO ),
        arrow::default_memory_pool() );

    auto chunks = make_chunks( _begin, _end, append_function, &builder );

    return std::make_shared<arrow::ChunkedArray>( std::move( chunks ) );
}

// ----------------------------------------------------------------------------
}  // namespace containers
}  // namespace engine

#endif  // ENGINE_CONTAINERS_CATEGORICALFEATURES_HPP_
