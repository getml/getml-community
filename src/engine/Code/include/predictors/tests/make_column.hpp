
// ---------------------------------------------------------------------------

template <class T>
std::shared_ptr<std::vector<T>> make_column(
    size_t _length, std::mt19937* _rng )
{
    auto vec = std::make_shared<std::vector<T>>( _length );

    std::uniform_real_distribution<> dis( 0.0, 500.0 );

    for ( auto& val : *vec )
        {
            val = dis( *_rng );
        }

    return vec;
}

// -----------------------------------------------------------------------------
