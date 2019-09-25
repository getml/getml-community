
// ---------------------------------------------------------------------------

template <class T>
std::vector<T> make_categorical_column( size_t _length, std::mt19937& _rng )
{
    std::vector<T> vec( _length );

    std::uniform_real_distribution<> dis( 1.0, 11.0 );

    for ( auto& val : vec )
        {
            val = static_cast<T>( dis( _rng ) );
        }

    return vec;
}

// -----------------------------------------------------------------------------
