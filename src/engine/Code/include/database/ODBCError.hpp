#ifndef DATABASE_ODBCERROR_HPP_
#define DATABASE_ODBCERROR_HPP_

namespace database
{
// ----------------------------------------------------------------------------

class ODBCError
{
   public:
    /// Checks whether an error occurs and throws if necessary.
    static void check(
        const SQLRETURN _ret,
        const std::string& _activity,
        const SQLHANDLE _handle,
        const SQLSMALLINT _type )
    {
        if ( _ret != SQL_SUCCESS && _ret != SQL_SUCCESS_WITH_INFO )
            {
                throw_error( _ret, _activity, _handle, _type );
            }
    }

   private:
    /// Returns the string corresponding to the return code.
    static std::string interpret_return_code( const SQLRETURN _ret )
    {
        switch ( _ret )
            {
                case SQL_SUCCESS:
                    return "SQL_SUCCESS";

                case SQL_SUCCESS_WITH_INFO:
                    return "SQL_SUCCESS_WITH_INFO";

                case SQL_ERROR:
                    return "SQL_ERROR";

                case SQL_INVALID_HANDLE:
                    return "SQL_INVALID_HANDLE";

                case SQL_NO_DATA:
                    return "SQL_NO_DATA";

                case SQL_NEED_DATA:
                    return "SQL_NEED_DATA";

                case SQL_STILL_EXECUTING:
                    return "SQL_STILL_EXECUTING";

                default:
                    return "unknown return code";
            }
    }

    /// Throws an error
    static void throw_error(
        const SQLRETURN _ret,
        const std::string& _activity,
        const SQLHANDLE _handle,
        const SQLSMALLINT _type )
    {
        std::string err_msg =
            "The ODBC driver reported the following error when trying to call ";

        err_msg += _activity;

        err_msg += ": ";

        err_msg += "Return code ";

        err_msg += std::to_string( _ret );

        err_msg += " (";

        err_msg += interpret_return_code( _ret );

        err_msg += "). ";

        for ( SQLINTEGER i = 1; true; ++i )
            {
                SQLINTEGER native_error = 0;
                SQLCHAR state[7];
                SQLCHAR diag_rec[256];
                SQLSMALLINT diag_rec_length = 0;

                const auto ret = SQLGetDiagRec(
                    _type,
                    _handle,
                    i,
                    state,
                    &native_error,
                    diag_rec,
                    sizeof( diag_rec ),
                    &diag_rec_length );

                if ( SQL_SUCCEEDED( ret ) )
                    {
                        err_msg += std::to_string( i ) + ":" +
                                   std::to_string( native_error ) +
                                   reinterpret_cast<const char*>( diag_rec ) +
                                   "; ";
                    }

                if ( ret != SQL_SUCCESS )
                    {
                        break;
                    }
            }

        throw std::runtime_error( err_msg );
    }
};

// ----------------------------------------------------------------------------
}  // namespace database

#endif  // DATABASE_ODBCERROR_HPP_
