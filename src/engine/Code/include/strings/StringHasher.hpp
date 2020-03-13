namespace strings
{
// ----------------------------------------------------------------------------

struct StringHasher
{
    std::size_t operator()( const String& _str ) const { return _str.hash(); }
};

// ----------------------------------------------------------------------------
}  // namespace strings
