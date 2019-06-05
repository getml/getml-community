#ifndef AUTOSQL_CONTAINER_CONTAINER_HPP_
#define AUTOSQL_CONTAINER_CONTAINER_HPP_

namespace autosql
{
namespace containers
{
// -------------------------------------------------------------------------

template <class T>
class Container
{
   public:
    Container( SQLNET_INT _nrows, SQLNET_INT _ncols )
    {
        init( _nrows, _ncols );
    }

    // -------------------------------

    /// Trivial getter
    inline std::vector<SQLNET_INT>& batches()
    {
        return batches_.get()[0];
    }

    /// Trivial getter
    inline std::shared_ptr<std::vector<std::string> >& colnames()
    {
        return colnames_;
    }

    /// Trivial getter
    inline std::string& colname( SQLNET_INT _i )
    {
        return colnames_.get()[0][_i];
    }

    /// Trivial getter
    inline T*& data() { return data_ptr_; }

    /// Trivial getter
    inline std::string& name() const { return *( name_ ); }

    /// Trivial getter
    inline SQLNET_INT ncols() const { return ncols_; }

    /// Trivial getter
    inline SQLNET_INT nrows() const { return nrows_; }

    /// Trivial getter
    inline const SQLNET_SIZE num_batches()
    {
        return batches().size() - 1;
    }

    /// Trivial setter
    inline void set_colnames( std::vector<std::string>& _colnames )
    {
        if ( static_cast<SQLNET_INT>( _colnames.size() ) != ncols_ )
            {
                throw std::invalid_argument(
                    "Number of colnames provided does not match number of "
                    "columns!" );
            }

        colnames_.get()[0] = _colnames;
    }

    /// Trivial setter
    inline void set_units( std::vector<std::string>& _units )
    {
        if ( static_cast<SQLNET_INT>( _units.size() ) != ncols_ )
            {
                throw std::invalid_argument(
                    "Number of units provided does not match number of "
                    "columns!" );
            }

        units_.get()[0] = _units;
    }

    /// Trivial getter
    inline std::string type() const { return type_; }

    /// Trivial getter
    inline std::string& unit( SQLNET_INT _i )
    {
        assert(
            static_cast<SQLNET_INT>( units_->size() ) > _i && _i >= 0 );
        return units_.get()[0][_i];
    }

    /// Trivial getter
    inline std::shared_ptr<std::vector<std::string> >& units()
    {
        return units_;
    }

    // -------------------------------

   protected:
    /// Initializes the containers object
    void init( SQLNET_INT _nrows, SQLNET_INT _ncols );

    // -------------------------------

    /// Batches contain information on how data was loaded
    /// into the containers, so the original order can be reconstructed
    std::shared_ptr<std::vector<SQLNET_INT> > batches_;

    /// Names of the columns
    std::shared_ptr<std::vector<std::string> > colnames_;

    /// The actual data, unless containers is simply virtual
    std::shared_ptr<std::vector<T> > data_;

    /// Pointer to the actual data. This is used by all member
    /// functions, in order to account for virtual containers
    T* data_ptr_;

    /// Name of this containers
    std::shared_ptr<std::string> name_;

    /// Number of columns
    SQLNET_INT ncols_;

    /// Number of columns - unsigned long version
    SQLNET_UNSIGNED_LONG ncols_long_;

    /// Number of rows
    SQLNET_INT nrows_;

    /// Number of rows - unsigned long version
    SQLNET_UNSIGNED_LONG nrows_long_;

    /// Units of the columns
    std::shared_ptr<std::vector<std::string> > units_;

    /// Type of this containers (since it is a base class)
    std::string type_;
};

// -------------------------------------------------------------------------

template <class T>
void Container<T>::init( SQLNET_INT _nrows, SQLNET_INT _ncols )
{
    nrows_ = _nrows;

    nrows_long_ = static_cast<SQLNET_UNSIGNED_LONG>( _nrows );

    ncols_ = _ncols;

    ncols_long_ = static_cast<SQLNET_UNSIGNED_LONG>( _ncols );

    batches_.reset( new std::vector<SQLNET_INT>( 0 ) );

    batches() = {0, nrows_};

    name_.reset( new std::string( "" ) );

    colnames_.reset( new std::vector<std::string>( _ncols, "" ) );

    units_.reset( new std::vector<std::string>( _ncols, "" ) );
}

// -------------------------------------------------------------------------
}
}

#endif // AUTOSQL_CONTAINER_CONTAINER_HPP_ 
