#ifndef ENGINE_CONTAINERS_DATAFRAME_HPP_
#define ENGINE_CONTAINERS_DATAFRAME_HPP_

namespace engine
{
namespace containers
{
// -------------------------------------------------------------------------

class DataFrame
{
   public:
    DataFrame()
        : categories_( std::make_shared<Encoding>() ),
          indices_( std::vector<std::shared_ptr<ENGINE_INDEX>>( 0 ) ),
          join_keys_encoding_( std::make_shared<Encoding>() )
    {
    }

    DataFrame(
        const std::shared_ptr<Encoding> &_categories,
        const std::shared_ptr<Encoding> &_join_keys_encoding )
        : categories_( _categories ),
          indices_( std::vector<std::shared_ptr<ENGINE_INDEX>>( 0 ) ),
          join_keys_encoding_( _join_keys_encoding )
    {
    }

    ~DataFrame() = default;

    // -------------------------------

    /// Setter for a float_matrix
    void add_float_matrix(
        const Matrix<ENGINE_FLOAT> &_mat,
        const std::string &_role,
        const std::string _name,
        const size_t _num );

    /// Setter for an int_matrix
    void add_int_matrix(
        const Matrix<ENGINE_INT> &_mat,
        const std::string _role,
        const std::string _name,
        const size_t _num );

    /// Appends another data frame to this data frame.
    void append( const DataFrame &_other );

    /// Makes sure that the data contained in the DataFrame is plausible
    /// and consistent.
    void check_plausibility() const;

    /// Builds indices_, which serve the role of
    /// an "index" over the join keys
    void create_indices();

    /// Getter for a float_matrix
    const Matrix<ENGINE_FLOAT> &float_matrix(
        const std::string &_role, const size_t _num ) const;

    /// Returns the encodings as a property tree
    Poco::JSON::Object get_colnames();

    /// Returns the content of the data frame in a format that is compatible
    /// with the DataTables.js server-side processing API.
    Poco::JSON::Object get_content(
        const std::int32_t _draw,
        const std::int32_t _start,
        const std::int32_t _length ) const;

    /// Getter for an int_matrix (either join keys or categorical)
    const Matrix<ENGINE_INT> &int_matrix(
        const std::string &_role, const size_t _num ) const;

    /// Loads the data from the hard-disk into the engine
    void load( const std::string &_path );

    /// Returns number of bytes occupied by the data
    ENGINE_UNSIGNED_LONG nbytes() const;

    /// Saves the data on the engine
    void save( const std::string &_path );

    /// Extracts the data frame as a Poco::JSON::Object the monitor process can
    /// understand
    Poco::JSON::Object to_monitor( const std::string _name );

    // -------------------------------

    /// Trivial accessor
    template <class T>
    const Matrix<ENGINE_INT> &categorical( const T _i ) const
    {
        assert( categoricals_.size() > 0 );
        assert( _i >= 0 );
        assert( _i < static_cast<T>( categoricals_.size() ) );

        return categoricals_[_i];
    }

    /// Trivial accessor
    const Encoding &categories() const { return *categories_.get(); }

    /// Trivial accessor
    std::string const &category( const size_t _i ) const
    {
        assert( _i < categories().size() );

        return categories()[_i];
    }

    /// Trivial accessor
    template <class T>
    const Matrix<ENGINE_FLOAT> &discrete( const T _i ) const
    {
        assert( _i >= 0 );
        assert( _i < static_cast<T>( discretes_.size() ) );

        return discretes_[_i];
    }

    /// Returns the index signified by index _i
    template <class T>
    std::shared_ptr<ENGINE_INDEX> &index( T _i )
    {
        assert( indices_.size() == join_keys_.size() );
        assert( join_keys_.size() > 0 );
        assert( _i >= 0 );
        assert( static_cast<size_t>( _i ) < indices_.size() );
        assert( indices_[_i] );

        return indices_[_i];
    }

    /// Returns the index signified by index _i
    template <class T>
    const std::shared_ptr<const ENGINE_INDEX> index( T _i ) const
    {
        assert( indices_.size() == join_keys_.size() );
        assert( join_keys_.size() > 0 );
        assert( _i >= 0 );
        assert( static_cast<size_t>( _i ) < indices_.size() );
        assert( indices_[_i] );

        return indices_[_i];
    }

    /// Trivial accessor
    std::vector<std::shared_ptr<ENGINE_INDEX>> &indices() { return indices_; }

    /// Trivial accessor
    const std::vector<std::shared_ptr<ENGINE_INDEX>> &indices() const
    {
        return indices_;
    }

    /// Returns the join key signified by index _i
    template <class T>
    const Matrix<ENGINE_INT> &join_key( const T _i ) const
    {
        assert( join_keys_.size() > 0 );
        assert( _i >= 0 );
        assert( _i < static_cast<T>( join_keys_.size() ) );

        return join_keys_[_i];
    }

    /// Trivial accessor
    const std::vector<Matrix<ENGINE_INT>> &join_keys() const
    {
        return join_keys_;
    }

    /// Primitive abstraction for member join_keys_encoding_
    const Encoding &join_keys_encoding() const
    {
        return *join_keys_encoding_.get();
    }

    /// Primitive abstraction for member name_
    std::string &name() { return name_; }

    /// Primitive abstraction for member name_
    const std::string &name() const { return name_; }

    /// Get the number of rows
    const size_t nrows() const
    {
        assert( num_join_keys() > 0 );

        return join_keys_[0].nrows();
    }

    /// Returns number of categorical columns.
    size_t const num_categoricals() const { return categoricals_.size(); }

    /// Returns number of discrete columns.
    size_t const num_discretes() const { return discretes_.size(); }

    /// Returns number of join keys.
    size_t const num_join_keys() const { return join_keys_.size(); }

    /// Returns number of numerical columns.
    size_t const num_numericals() const { return numericals_.size(); }

    /// Returns number of target columns.
    size_t const num_targets() const { return targets_.size(); }

    /// Returns number of the time stamps.
    size_t const num_time_stamps() const { return time_stamps_.size(); }

    /// Trivial accessor
    template <class T>
    const Matrix<ENGINE_FLOAT> &numerical( const T _i ) const
    {
        assert( numericals_.size() > 0 );
        assert( _i >= 0 );
        assert( _i < static_cast<T>( numericals_.size() ) );

        return numericals_[_i];
    }

    /// Primitive setter
    void set_categories( const std::shared_ptr<Encoding> &_categories )
    {
        categories_ = _categories;
    }

    /// Primitive setter
    void set_join_keys_encoding(
        const std::shared_ptr<Encoding> &_join_keys_encoding )
    {
        join_keys_encoding_ = _join_keys_encoding;
    }

    /// Trivial accessor
    template <class T>
    const Matrix<ENGINE_FLOAT> &target( const T _i ) const
    {
        assert( targets_.size() > 0 );
        assert( _i >= 0 );
        assert( _i < static_cast<T>( targets_.size() ) );

        return targets_[_i];
    }

    /// Returns the time stamps signified by index _i
    template <class T>
    Matrix<ENGINE_FLOAT> const &time_stamp( const T _i ) const
    {
        assert( time_stamps_.size() > 0 );
        assert( _i >= 0 );
        assert( _i < static_cast<T>( time_stamps_.size() ) );

        return time_stamps_[_i];
    }

    /// Trivial accessor
    const std::vector<Matrix<ENGINE_FLOAT>> &time_stamps() const
    {
        return time_stamps_;
    }

    // -------------------------------

   private:
    /// Adds a categorical column.
    void add_categorical(
        const Matrix<ENGINE_INT> &_mat,
        const std::string _name,
        const size_t _num );

    /// Adds a discrete column.
    void add_discrete(
        const Matrix<ENGINE_FLOAT> &_mat,
        const std::string _name,
        const size_t _num );

    /// Adds a join key column.
    void add_join_key(
        const Matrix<ENGINE_INT> &_mat,
        const std::string _name,
        const size_t _num );

    /// Adds a numerical column.
    void add_numerical(
        const Matrix<ENGINE_FLOAT> &_mat,
        const std::string _name,
        const size_t _num );

    /// Adds a target column.
    void add_target(
        const Matrix<ENGINE_FLOAT> &_mat,
        const std::string _name,
        const size_t _num );

    /// Adds a time stamp column.
    void add_time_stamp(
        const Matrix<ENGINE_FLOAT> &_mat,
        const std::string _name,
        const size_t _num );

    /// Calculate the number of bytes.
    template <class T>
    ENGINE_UNSIGNED_LONG calc_nbytes(
        const std::vector<Matrix<T>> &_columns ) const;

    /// Returns the colnames of a vector of columns
    template <class T>
    Poco::JSON::Array get_colnames(
        const std::vector<Matrix<T>> &_columns ) const;

    /// Returns the units of a vector of columns
    template <class T>
    Poco::JSON::Array get_units( const std::vector<Matrix<T>> &_columns ) const;

    /// Loads columns.
    template <class T>
    std::vector<Matrix<T>> load_matrices(
        const std::string &_path, const std::string &_prefix ) const;

    template <class T>
    void save_matrices(
        const std::vector<Matrix<T>> &_matrices,
        const std::string &_path,
        const std::string &_prefix ) const;

    /// Transforms a float to a time stamp
    std::string to_time_stamp( const ENGINE_FLOAT &_time_stamp_float ) const;

    // -------------------------------

   private:
    /// Categorical data
    std::vector<Matrix<ENGINE_INT>> categoricals_;

    /// Maps integers to names of categories
    std::shared_ptr<Encoding> categories_;

    /// Discrete data
    std::vector<Matrix<ENGINE_FLOAT>> discretes_;

    /// Performs the role of an "index" over the join keys
    std::vector<std::shared_ptr<ENGINE_INDEX>> indices_;

    /// Join keys - note that their might be several
    std::vector<Matrix<ENGINE_INT>> join_keys_;

    /// Maps integers to names of join keys
    std::shared_ptr<Encoding> join_keys_encoding_;

    /// Name of the data frame
    std::string name_;

    /// Numerical data
    std::vector<Matrix<ENGINE_FLOAT>> numericals_;

    /// Targets - only exists for population tables
    std::vector<Matrix<ENGINE_FLOAT>> targets_;

    /// Time stamps
    std::vector<Matrix<ENGINE_FLOAT>> time_stamps_;
};

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace engine

// -------------------------------------------------------------------------
// -------------------------------------------------------------------------

namespace engine
{
namespace containers
{
// -------------------------------------------------------------------------

template <class T>
ENGINE_UNSIGNED_LONG DataFrame::calc_nbytes(
    const std::vector<Matrix<T>> &_columns ) const
{
    return std::accumulate(
        _columns.begin(),
        _columns.end(),
        static_cast<ENGINE_UNSIGNED_LONG>( 0 ),
        []( ENGINE_UNSIGNED_LONG &init, const Matrix<T> &mat ) {
            return init + mat.nbytes();
        } );
}

// -------------------------------------------------------------------------

template <class T>
Poco::JSON::Array DataFrame::get_colnames(
    const std::vector<Matrix<T>> &_columns ) const
{
    std::vector<std::string> colnames;

    std::for_each(
        _columns.begin(), _columns.end(), [&colnames]( const Matrix<T> &mat ) {
            colnames.push_back( mat.colname( 0 ) );
        } );

    return JSON::vector_to_array( colnames );
}

// -------------------------------------------------------------------------

template <class T>
Poco::JSON::Array DataFrame::get_units(
    const std::vector<Matrix<T>> &_columns ) const
{
    std::vector<std::string> units;

    std::for_each(
        _columns.begin(), _columns.end(), [&units]( const Matrix<T> &mat ) {
            units.push_back( mat.unit( 0 ) );
        } );

    return JSON::vector_to_array( units );
}

// ----------------------------------------------------------------------------

template <class T>
std::vector<Matrix<T>> DataFrame::load_matrices(
    const std::string &_path, const std::string &_prefix ) const
{
    std::vector<Matrix<T>> matrices;

    for ( size_t i = 0; true; ++i )
        {
            std::string fname = _path + _prefix + std::to_string( i );

            if ( !Poco::File( fname ).exists() )
                {
                    break;
                }

            Matrix<T> mat;

            mat.load( fname );

            mat.name() = name();

            matrices.push_back( mat );
        }

    return matrices;
}

// ----------------------------------------------------------------------------

template <class T>
void DataFrame::save_matrices(
    const std::vector<Matrix<T>> &_matrices,
    const std::string &_path,
    const std::string &_prefix ) const
{
    for ( size_t i = 0; i < _matrices.size(); ++i )
        {
            _matrices[i].save( _path + _prefix + std::to_string( i ) );
        }
}

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace engine

// -------------------------------------------------------------------------

#endif  // ENGINE_CONTAINERS_DATAFRAME_HPP_
