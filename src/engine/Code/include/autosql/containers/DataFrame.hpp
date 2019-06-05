#ifndef AUTOSQL_CONTAINER_DATAFRAME_HPP_
#define AUTOSQL_CONTAINER_DATAFRAME_HPP_

namespace autosql
{
namespace containers
{
// -------------------------------------------------------------------------

class DataFrame
{
   public:
    DataFrame()
        : categories_( std::make_shared<containers::Encoding>() ),
          indices_( std::vector<std::shared_ptr<SQLNET_INDEX>>( 0 ) ),
          join_keys_encoding_( std::make_shared<containers::Encoding>() ),
          join_key_used_( -1 ),
          time_stamps_used_( -1 ),
          upper_time_stamps_( -1 )
    {
    }

    DataFrame(
        const std::shared_ptr<containers::Encoding> &_categories,
        const std::shared_ptr<containers::Encoding> &_join_keys_encoding )
        : categories_( _categories ),
          indices_( std::vector<std::shared_ptr<SQLNET_INDEX>>( 0 ) ),
          join_keys_encoding_( _join_keys_encoding ),
          join_key_used_( -1 ),
          time_stamps_used_( -1 ),
          upper_time_stamps_( -1 )
    {
    }

    ~DataFrame() = default;

    // -------------------------------

    /// Appends another data frame to this data frame.
    void append( DataFrame &_other );

    /// Deletes all data in the DataFrame object
    void clear();

    /// Makes sure that the data contained in the DataFrame is plausible
    /// and consistent.
    void check_plausibility() const;

    /// Builds indices_, which serve the role of
    /// an "index" over the join keys
    void create_indices();

    /// Setter for a float_matrix
    void float_matrix(
        containers::Matrix<SQLNET_FLOAT> &_mat,
        const std::string &_role,
        const std::string _name,
        const SQLNET_SIZE _num );

    /// Getter for a float_matrix
    containers::Matrix<SQLNET_FLOAT> &float_matrix(
        const std::string &_role, const SQLNET_SIZE _num );

    /// Returns the encodings as a property tree
    Poco::JSON::Object get_colnames();

    /// Returns the content of the data frame in a format that is compatible
    /// with the DataTables.js server-side processing API.
    Poco::JSON::Object get_content(
        const std::int32_t _draw,
        const std::int32_t _start,
        const std::int32_t _length ) const;

    /// Setter for an int_matrix (either join keys or categorical)
    void int_matrix(
        containers::Matrix<SQLNET_INT> &_mat,
        const std::string _role,
        const std::string _name,
        const SQLNET_SIZE _num );

    /// Getter for an int_matrix (either join keys or categorical)
    containers::Matrix<SQLNET_INT> &int_matrix(
        const std::string &_role, const SQLNET_SIZE _num );

    /// Loads the data from the hard-disk into the engine
    void load( const std::string &_path );

#ifdef SQLNET_MULTINODE_MPI

    /// MPI version only: Non-root companion to load()
    void load_non_root();

#endif  // SQLNET_MULTINODE_MPI

    /// Returns number of bytes occupied by the data
    SQLNET_UNSIGNED_LONG nbytes();

    /// Saves the data on the engine
    void save( const std::string &_path );

#ifdef SQLNET_MULTINODE_MPI

    /// MPI version only: Non-root companion to save()
    void save_non_root();

#endif  // SQLNET_MULTINODE_MPI

    /// Extracts the data frame as a Poco::JSON::Object the monitor process can
    /// understand
    Poco::JSON::Object to_monitor( const std::string _name );

    // -------------------------------

    /// Trivial accessor
    inline containers::Matrix<SQLNET_INT> &categorical()
    {
        return categorical_;
    }

    /// Trivial accessor
    inline containers::Matrix<SQLNET_INT> const &categorical() const
    {
        return categorical_;
    }

    /// Trivial accessor
    inline const containers::Encoding &categories() const
    {
        return *categories_.get();
    }

    /// Trivial accessor
    inline std::string const &category( const SQLNET_INT _i ) const
    {
        assert( _i >= 0 );
        assert( static_cast<SQLNET_SIZE>( _i ) < categories().size() );

        return categories()[_i];
    }

    /// Trivial accessor
    inline containers::Matrix<SQLNET_FLOAT> &discrete() { return discrete_; }

    /// Trivial accessor
    inline containers::Matrix<SQLNET_FLOAT> const &discrete() const
    {
        return discrete_;
    }

    /// Returns the index signified by index _i
    template <class T>
    std::shared_ptr<SQLNET_INDEX> &index( T _i )
    {
        assert( indices_.size() == join_keys_.size() );
        assert( join_keys_.size() > 0 );
        assert( _i >= 0 );
        assert( static_cast<SQLNET_SIZE>( _i ) < indices_.size() );
        assert( indices_[_i] );

        return indices_[_i];
    }

    /// Returns the index signified by index _i
    template <class T>
    const std::shared_ptr<const SQLNET_INDEX> index( T _i ) const
    {
        assert( indices_.size() == join_keys_.size() );
        assert( join_keys_.size() > 0 );
        assert( _i >= 0 );
        assert( static_cast<SQLNET_SIZE>( _i ) < indices_.size() );
        assert( indices_[_i] );

        return indices_[_i];
    }

    /// Returns the index signified by index join_key_used_
    std::shared_ptr<SQLNET_INDEX> &index()
    {
        assert( join_key_used_ >= 0 );
        assert( static_cast<SQLNET_SIZE>( join_key_used_ ) < indices_.size() );

        return index( join_key_used_ );
    }

    /// Returns the index signified by index join_key_used_
    const std::shared_ptr<const SQLNET_INDEX> index() const
    {
        assert( join_key_used_ >= 0 );
        assert( static_cast<SQLNET_SIZE>( join_key_used_ ) < indices_.size() );

        return index( join_key_used_ );
    }

    /// Trivial accessor
    std::vector<std::shared_ptr<SQLNET_INDEX>> &indices() { return indices_; }

    /// Trivial accessor
    const std::vector<std::shared_ptr<SQLNET_INDEX>> &indices() const
    {
        return indices_;
    }

    /// Returns the join key signified by index _i
    template <class T>
    containers::Matrix<SQLNET_INT> &join_key( const T _i )
    {
        assert( join_keys_.size() > 0 );
        assert( _i >= 0 );
        assert( _i < static_cast<T>( join_keys_.size() ) );

        return join_keys_[_i];
    }

    /// Returns the join key signified by index _i
    template <class T>
    containers::Matrix<SQLNET_INT> const &join_key( const T _i ) const
    {
        assert( join_keys_.size() > 0 );
        assert( _i >= 0 );
        assert( _i < static_cast<T>( join_keys_.size() ) );

        return join_keys_[_i];
    }

    /// Returns the join key signified by index join_key_used_
    containers::Matrix<SQLNET_INT> &join_key()
    {
        assert( join_keys_.size() > 0 );
        assert( join_key_used_ >= 0 );
        assert( join_key_used_ < static_cast<SQLNET_INT>( join_keys_.size() ) );

        return join_key( join_key_used_ );
    }

    /// Returns the join key signified by index join_key_used_
    containers::Matrix<SQLNET_INT> const &join_key() const
    {
        assert( join_keys_.size() > 0 );
        assert( join_key_used_ >= 0 );
        assert( join_key_used_ < static_cast<SQLNET_INT>( join_keys_.size() ) );

        return join_key( join_key_used_ );
    }

    /// Trivial accessor
    std::vector<containers::Matrix<SQLNET_INT>> &join_keys()
    {
        return join_keys_;
    }

    /// Trivial accessor
    std::vector<containers::Matrix<SQLNET_INT>> const &join_keys() const
    {
        return join_keys_;
    }

    /// Primitive abstraction for member join_keys_encoding_
    inline const containers::Encoding &join_keys_encoding() const
    {
        return *join_keys_encoding_.get();
    }

    /// Primitive abstraction for member name_
    std::string &name() { return name_; }

    /// Primitive abstraction for member name_
    const std::string &name() const { return name_; }

    /// Get the number of rows
    const SQLNET_INT nrows() const
    {
        assert( categorical().nrows() == discrete().nrows() );
        assert( categorical().nrows() == numerical().nrows() );
        assert( categorical().nrows() == targets().nrows() );

        return categorical().nrows();
    }

    /// Returns number of join keys
    size_t const num_join_keys() const { return join_keys_.size(); }

    /// Returns number of the time stamps
    size_t const num_time_stamps() const { return time_stamps_.size(); }

    /// Primitive abstraction for member numerical_
    containers::Matrix<SQLNET_FLOAT> &numerical() { return numerical_; }

    /// Primitive abstraction for member numerical_
    containers::Matrix<SQLNET_FLOAT> const &numerical() const
    {
        return numerical_;
    }

    /// Primitive setter
    void set_categories(
        const std::shared_ptr<containers::Encoding> &_categories )
    {
        categories_ = _categories;
    }

    /// Primitive setter
    void set_join_keys_encoding(
        const std::shared_ptr<containers::Encoding> &_join_keys_encoding )
    {
        join_keys_encoding_ = _join_keys_encoding;
    }

    /// Setter for member join_key_used_
    template <class T>
    void set_join_key_used( T _join_key_used )
    {
        join_key_used_ = static_cast<SQLNET_INT>( _join_key_used );
    }

    /// Setter for member time_stamps_used_
    template <class T>
    void set_time_stamps_used( T _time_stamps_used )
    {
        time_stamps_used_ = static_cast<SQLNET_INT>( _time_stamps_used );
    }

    /// Setter for member upper_time_stamps_
    template <class T>
    void set_upper_time_stamps( T _upper_time_stamps )
    {
        upper_time_stamps_ = static_cast<SQLNET_INT>( _upper_time_stamps );
    }

    /// Trivial accessor
    containers::Matrix<SQLNET_FLOAT> &targets() { return targets_; }

    /// Trivial accessor
    containers::Matrix<SQLNET_FLOAT> const &targets() const { return targets_; }

    /// Returns the time stamps signified by index _i
    template <class T>
    containers::Matrix<SQLNET_FLOAT> &time_stamps( const T _i )
    {
        assert( time_stamps_.size() > 0 );

        assert( _i >= 0 );

        assert( _i < static_cast<T>( time_stamps_.size() ) );

        return time_stamps_[_i];
    }

    /// Returns the time stamps signified by index _i
    template <class T>
    containers::Matrix<SQLNET_FLOAT> const &time_stamps( const T _i ) const
    {
        assert( time_stamps_.size() > 0 );

        assert( _i >= 0 );

        assert( _i < static_cast<T>( time_stamps_.size() ) );

        return time_stamps_[_i];
    }

    /// Returns the time stamps signified by index time_stamps_used_
    containers::Matrix<SQLNET_FLOAT> &time_stamps()
    {
        assert( time_stamps_.size() > 0 );

        assert( time_stamps_used_ >= 0 );

        assert(
            time_stamps_used_ <
            static_cast<SQLNET_INT>( time_stamps_.size() ) );

        return time_stamps( time_stamps_used_ );
    }

    /// Returns the time stamps signified by index time_stamps_used_
    const containers::Matrix<SQLNET_FLOAT> &time_stamps() const
    {
        assert( time_stamps_.size() > 0 );

        assert( time_stamps_used_ >= 0 );

        assert(
            time_stamps_used_ <
            static_cast<SQLNET_INT>( time_stamps_.size() ) );

        return time_stamps( time_stamps_used_ );
    }

    /// Trivial accessor
    std::vector<containers::Matrix<SQLNET_FLOAT>> &time_stamps_all()
    {
        return time_stamps_;
    }

    /// Trivial accessor
    std::vector<containers::Matrix<SQLNET_FLOAT>> const &time_stamps_all() const
    {
        return time_stamps_;
    }

    /// Returns the time stamps signified by index upper_time_stamps_
    containers::Matrix<SQLNET_FLOAT> *const upper_time_stamps()
    {
        if ( upper_time_stamps_ < 0 )
            {
                return nullptr;
            }

        assert( time_stamps_.size() > 0 );

        assert(
            upper_time_stamps_ <
            static_cast<SQLNET_INT>( time_stamps_.size() ) );

        return &time_stamps( upper_time_stamps_ );
    }

    /// Returns the time stamps signified by index time_stamps_used_
    const containers::Matrix<SQLNET_FLOAT> *const upper_time_stamps() const
    {
        if ( upper_time_stamps_ < 0 )
            {
                return nullptr;
            }

        assert( time_stamps_.size() > 0 );

        assert(
            upper_time_stamps_ <
            static_cast<SQLNET_INT>( time_stamps_.size() ) );

        return &time_stamps( upper_time_stamps_ );
    }

    // -------------------------------

   private:
    /// Loads the join keys
    void load_join_keys( const std::string &_path );

    /// Loads the time stamps
    void load_time_stamps( const std::string &_path );

    /// Transforms a float to a time stamp
    std::string to_time_stamp( const SQLNET_FLOAT &_time_stamp_float ) const;

    // -------------------------------

   private:
    /// Categorical data
    containers::Matrix<SQLNET_INT> categorical_;

    /// Maps integers to names of categories
    std::shared_ptr<containers::Encoding> categories_;

    /// Discrete data
    containers::Matrix<SQLNET_FLOAT> discrete_;

    /// Performs the role of an "index" over the join keys
    std::vector<std::shared_ptr<SQLNET_INDEX>> indices_;

    /// Join keys - note that their might be several
    std::vector<containers::Matrix<SQLNET_INT>> join_keys_;

    /// Maps integers to names of join keys
    std::shared_ptr<containers::Encoding> join_keys_encoding_;

    /// Peripheral tables use only one particular join key -
    /// which one is signified by join_key_used_
    SQLNET_INT join_key_used_;

    /// Name of the data frame
    std::string name_;

    /// Numerical data
    containers::Matrix<SQLNET_FLOAT> numerical_;

    /// Targets - only exists for population tables
    containers::Matrix<SQLNET_FLOAT> targets_;

    /// Time stamps
    std::vector<containers::Matrix<SQLNET_FLOAT>> time_stamps_;

    /// Peripheral tables use only one particular set of time stamps -
    /// which one is signified by time_stamps_used_
    SQLNET_INT time_stamps_used_;

    /// Peripheral tables use only one particular set of upper time stamps -
    /// which one is signified by upper_time_stamps_
    SQLNET_INT upper_time_stamps_;
};

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace autosql

#endif  // AUTOSQL_CONTAINER_DATAFRAME_HPP_
