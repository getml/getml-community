#ifndef DATABASE_ODBCSTMT_HPP_
#define DATABASE_ODBCSTMT_HPP_

namespace database
{
// ----------------------------------------------------------------------------

struct ODBCStmt
{
    ODBCStmt( const ODBCConn& _conn, const std::string& _query = "" )
        : handle_( SQL_NULL_HSTMT )
    {
        auto ret = SQLAllocHandle( SQL_HANDLE_STMT, _conn.handle_, &handle_ );

        ODBCError::check(
            ret, "SQLAllocHandle(SQL_HANDLE_STMT)", handle_, SQL_HANDLE_STMT );

        if ( _query != "" )
            {
                auto query = to_ptr( _query );

                ret = SQLExecDirect( handle_, query.get(), SQL_NTS );

                ODBCError::check(
                    ret, "SQLExecDirect()", handle_, SQL_HANDLE_STMT );
            }
    }

    ~ODBCStmt()
    {
        if ( handle_ != SQL_NULL_HSTMT )
            {
                SQLFreeHandle( SQL_HANDLE_STMT, handle_ );
            }
    }

    ODBCStmt( const ODBCStmt& _other ) = delete;

    ODBCStmt( ODBCStmt&& _other ) noexcept
    {
        handle_ = std::move( _other.handle_ );
    }

    ODBCStmt& operator=( const ODBCStmt& _other ) = delete;

    ODBCStmt& operator=( ODBCStmt&& _other ) noexcept
    {
        if ( this == &_other )
            {
                return *this;
            }

        handle_ = std::move( _other.handle_ );

        return *this;
    }

    std::unique_ptr<SQLCHAR[]> to_ptr( const std::string& _str ) const
    {
        auto ptr = std::make_unique<SQLCHAR[]>( _str.size() + 1 );

        std::copy(
            reinterpret_cast<const SQLCHAR*>( _str.c_str() ),
            reinterpret_cast<const SQLCHAR*>( _str.c_str() ) + _str.size(),
            ptr.get() );

        return ptr;
    }

    SQLHSTMT handle_;
};

// ----------------------------------------------------------------------------
}  // namespace database

#endif  // DATABASE_ODBCSTMT_HPP_
