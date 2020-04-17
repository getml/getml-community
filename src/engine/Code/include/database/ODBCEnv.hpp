#ifndef DATABASE_ODBCENV_HPP_
#define DATABASE_ODBCENV_HPP_

namespace database
{
// ----------------------------------------------------------------------------

struct ODBCEnv
{
    ODBCEnv() : handle_( SQL_NULL_HENV )
    {
        auto ret = SQLAllocHandle( SQL_HANDLE_ENV, SQL_NULL_HANDLE, &handle_ );

        ODBCError::check(
            ret, "SQLAllocHandle(SQL_HANDLE_ENV)", handle_, SQL_HANDLE_ENV );

        ret = SQLSetEnvAttr(
            handle_, SQL_ATTR_ODBC_VERSION, (SQLPOINTER*)SQL_OV_ODBC3, 0 );

        ODBCError::check(
            ret, "SQLSetEnvAttr(SQL_HANDLE_ENV)", handle_, SQL_HANDLE_ENV );
    }

    ~ODBCEnv()
    {
        if ( handle_ != SQL_NULL_HENV )
            {
                SQLFreeHandle( SQL_HANDLE_ENV, handle_ );
            }
    }

    ODBCEnv( const ODBCEnv& _other ) = delete;

    ODBCEnv( ODBCEnv&& _other ) noexcept
    {
        handle_ = std::move( _other.handle_ );
    }

    ODBCEnv& operator=( const ODBCEnv& _other ) = delete;

    ODBCEnv& operator=( ODBCEnv&& _other ) noexcept
    {
        if ( this == &_other )
            {
                return *this;
            }

        handle_ = std::move( _other.handle_ );
    }

    SQLHENV handle_;
};

// ----------------------------------------------------------------------------
}  // namespace database

#endif  // DATABASE_ODBCENV_HPP_

