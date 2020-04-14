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
                throw_error( _activity, _handle, _type );
            }
    }

   private:
    /// Throws an error
    static void throw_error(
        const std::string& _activity,
        const SQLHANDLE _handle,
        const SQLSMALLINT _type )
    {
        std::string err_msg =
            "The ODBC driver reported the following error when trying to call ";

        err_msg += _activity;

        err_msg += ": ";

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
