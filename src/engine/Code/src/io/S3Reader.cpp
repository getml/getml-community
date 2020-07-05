#if ( defined( _WIN32 ) || defined( _WIN64 ) )
    // S3 is not supported on windows
#else

#include "io/io.hpp"

namespace io
{
// ----------------------------------------------------------------------------

std::vector<std::string> S3Reader::next_line()
{
    // ------------------------------------------------------------------------
    // Usually the calling function should make sure that we haven't reached
    // the end of file. But just to be sure, we do it again.

    if ( eof() )
        {
            return std::vector<std::string>();
        }

    // ------------------------------------------------------------------------

    assert_true( records_ );

    const auto begin = records_->begin() + current_row_ * ncols_;

    assert_true( begin + ncols_ <= records_->end() );

    auto result = std::vector<std::string>( ncols_ );

    for ( size_t i = 0; i < ncols_; ++i )
        {
            auto& ptr = *( begin + i );

            assert_true( ptr );

            result[i] = ptr.get();

            ptr.reset();
        }

    ++current_row_;

    // ------------------------------------------------------------------------

    return result;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace io

#endif
