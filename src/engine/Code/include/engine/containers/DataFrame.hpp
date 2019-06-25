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
          join_keys_encoding_( std::make_shared<Encoding>() )
    {
    }

    DataFrame(
        const std::shared_ptr<Encoding> &_categories,
        const std::shared_ptr<Encoding> &_join_keys_encoding )
        : categories_( _categories ), join_keys_encoding_( _join_keys_encoding )
    {
    }

    ~DataFrame() = default;

    // -------------------------------

    /// Setter for a float_matrix
    void add_float_column(
        const Matrix<Float> &_mat,
        const std::string &_role,
        const size_t _num );

    /// Setter for an int_matrix
    void add_int_column(
        const Matrix<Int> &_mat, const std::string _role, const size_t _num );

    /// Appends another data frame to this data frame.
    void append( const DataFrame &_other );

    /// Makes sure that the data contained in the DataFrame is plausible
    /// and consistent.
    void check_plausibility() const;

    /// Builds indices_, which serve the role of
    /// an "index" over the join keys
    void create_indices();

    /// Getter for a float_matrix
    const Matrix<Float> &float_matrix(
        const std::string &_role, const size_t _num ) const;

    /// Builds a dataframe from a database connector.
    void from_db(
        const std::shared_ptr<database::Connector> _connector,
        const std::string &_tname,
        const std::vector<std::string> &_categoricals,
        const std::vector<std::string> &_discretes,
        const std::vector<std::string> &_join_keys,
        const std::vector<std::string> &_numericals,
        const std::vector<std::string> &_targets,
        const std::vector<std::string> &_time_stamps );

    /// Builds a dataframe from a JSON Object.
    void from_json(
        const Poco::JSON::Object &_obj,
        const std::vector<std::string> _time_formats,
        const std::vector<std::string> &_categoricals,
        const std::vector<std::string> &_discretes,
        const std::vector<std::string> &_join_keys,
        const std::vector<std::string> &_numericals,
        const std::vector<std::string> &_targets,
        const std::vector<std::string> &_time_stamps );

    /// Returns the encodings as a property tree
    Poco::JSON::Object get_colnames();

    /// Returns the content of the data frame in a format that is compatible
    /// with the DataTables.js server-side processing API.
    Poco::JSON::Object get_content(
        const std::int32_t _draw,
        const std::int32_t _start,
        const std::int32_t _length ) const;

    /// Getter for an int_matrix (either join keys or categorical)
    const Matrix<Int> &int_matrix(
        const std::string &_role, const size_t _num ) const;

    /// Loads the data from the hard-disk into the engine
    void load( const std::string &_path );

    /// Returns number of bytes occupied by the data
    ULong nbytes() const;

    /// Saves the data on the engine
    void save( const std::string &_path );

    /// Extracts the data frame as a Poco::JSON::Object the monitor process can
    /// understand
    Poco::JSON::Object to_monitor( const std::string _name );

    // -------------------------------

    /// Trivial accessor
    template <
        typename T,
        typename std::enable_if<!std::is_same<T, std::string>::value, int>::
            type = 0>
    const Matrix<Int> &categorical( const T _i ) const
    {
        assert( categoricals_.size() > 0 );
        assert( _i >= 0 );
        assert( _i < static_cast<T>( categoricals_.size() ) );

        return categoricals_[_i];
    }

    /// Trivial accessor
    const Matrix<Int> &categorical( const std::string &_name ) const
    {
        for ( size_t i = 0; i < num_categoricals(); ++i )
            {
                if ( categorical( i ).colname( 0 ) == _name )
                    {
                        return categorical( i );
                    }
            }

        throw std::invalid_argument(
            "Data frame '" + name_ +
            "' contains no categorical column named '" + _name + "'!" );
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
    template <
        typename T,
        typename std::enable_if<!std::is_same<T, std::string>::value, int>::
            type = 0>
    const Matrix<Float> &discrete( const T _i ) const
    {
        assert( _i >= 0 );
        assert( _i < static_cast<T>( discretes_.size() ) );

        return discretes_[_i];
    }

    /// Trivial accessor
    const Matrix<Float> &discrete( const std::string &_name ) const
    {
        for ( size_t i = 0; i < num_discretes(); ++i )
            {
                if ( discrete( i ).colname( 0 ) == _name )
                    {
                        return discrete( i );
                    }
            }

        throw std::invalid_argument(
            "Data frame '" + name_ + "' contains no discrete column named '" +
            _name + "'!" );
    }

    /// Returns the index signified by index _i
    template <class T>
    DataFrameIndex &index( T _i )
    {
        assert( indices_.size() == join_keys_.size() );
        assert( join_keys_.size() > 0 );
        assert( _i >= 0 );
        assert( static_cast<size_t>( _i ) < indices_.size() );

        return indices_[_i];
    }

    /// Returns the index signified by index _i
    template <class T>
    const DataFrameIndex index( T _i ) const
    {
        assert( indices_.size() == join_keys_.size() );
        assert( join_keys_.size() > 0 );
        assert( _i >= 0 );
        assert( static_cast<size_t>( _i ) < indices_.size() );

        return indices_[_i];
    }

    /// Trivial accessor
    std::vector<DataFrameIndex> &indices() { return indices_; }

    /// Trivial accessor
    const std::vector<DataFrameIndex> &indices() const { return indices_; }

    /// Returns the join key signified by index _i
    template <
        typename T,
        typename std::enable_if<!std::is_same<T, std::string>::value, int>::
            type = 0>
    const Matrix<Int> &join_key( const T _i ) const
    {
        assert( join_keys_.size() > 0 );
        assert( _i >= 0 );
        assert( _i < static_cast<T>( join_keys_.size() ) );

        return join_keys_[_i];
    }

    /// Trivial accessor
    const Matrix<Int> &join_key( const std::string &_name ) const
    {
        for ( size_t i = 0; i < num_join_keys(); ++i )
            {
                if ( join_key( i ).colname( 0 ) == _name )
                    {
                        return join_key( i );
                    }
            }

        throw std::invalid_argument(
            "Data frame '" + name_ + "' contains no join key named '" + _name +
            "'!" );
    }

    /// Trivial accessor
    const std::vector<Matrix<Int>> &join_keys() const { return join_keys_; }

    /// Primitive abstraction for member join_keys_encoding_
    const Encoding &join_keys_encoding() const
    {
        return *join_keys_encoding_.get();
    }

    /// Returns the maps underlying the indices.
    const std::vector<std::shared_ptr<Index>> maps() const
    {
        std::vector<std::shared_ptr<Index>> maps;
        for ( const auto &ix : indices_ ) maps.push_back( ix.map() );
        return maps;
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
    template <
        typename T,
        typename std::enable_if<!std::is_same<T, std::string>::value, int>::
            type = 0>
    const Matrix<Float> &numerical( const T _i ) const
    {
        assert( numericals_.size() > 0 );
        assert( _i >= 0 );
        assert( _i < static_cast<T>( numericals_.size() ) );

        return numericals_[_i];
    }

    /// Trivial accessor
    const Matrix<Float> &numerical( const std::string &_name ) const
    {
        for ( size_t i = 0; i < num_numericals(); ++i )
            {
                if ( numerical( i ).colname( 0 ) == _name )
                    {
                        return numerical( i );
                    }
            }

        throw std::invalid_argument(
            "Data frame '" + name_ + "' contains no numerical column named '" +
            _name + "'!" );
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
    template <
        typename T,
        typename std::enable_if<!std::is_same<T, std::string>::value, int>::
            type = 0>
    const Matrix<Float> &target( const T _i ) const
    {
        assert( targets_.size() > 0 );
        assert( _i >= 0 );
        assert( _i < static_cast<T>( targets_.size() ) );

        return targets_[_i];
    }

    /// Trivial accessor
    const Matrix<Float> &target( const std::string &_name ) const
    {
        for ( size_t i = 0; i < num_targets(); ++i )
            {
                if ( target( i ).colname( 0 ) == _name )
                    {
                        return target( i );
                    }
            }

        throw std::invalid_argument(
            "Data frame '" + name_ + "' contains no target column named '" +
            _name + "'!" );
    }

    /// Trivial accessor
    template <
        typename T,
        typename std::enable_if<!std::is_same<T, std::string>::value, int>::
            type = 0>
    Matrix<Float> const &time_stamp( const T _i ) const
    {
        assert( time_stamps_.size() > 0 );
        assert( _i >= 0 );
        assert( _i < static_cast<T>( time_stamps_.size() ) );

        return time_stamps_[_i];
    }

    /// Trivial accessor
    const Matrix<Float> &time_stamp( const std::string &_name ) const
    {
        for ( size_t i = 0; i < num_time_stamps(); ++i )
            {
                if ( time_stamp( i ).colname( 0 ) == _name )
                    {
                        return time_stamp( i );
                    }
            }

        throw std::invalid_argument(
            "Data frame '" + name_ + "' contains no time stamp named '" +
            _name + "'!" );
    }

    /// Trivial accessor
    const std::vector<Matrix<Float>> &time_stamps() const
    {
        return time_stamps_;
    }

    // -------------------------------

   private:
    /// Adds a categorical column.
    void add_categorical( const Matrix<Int> &_mat, const size_t _num );

    /// Adds a discrete column.
    void add_discrete( const Matrix<Float> &_mat, const size_t _num );

    /// Adds a vector of float vectors.
    void add_float_vectors(
        const std::vector<std::string> &_names,
        const std::vector<std::shared_ptr<std::vector<Float>>> &_vectors,
        const std::string &_role );

    /// Adds a vector of integer vectors.
    void add_int_vectors(
        const std::vector<std::string> &_names,
        const std::vector<std::shared_ptr<std::vector<Int>>> &_vectors,
        const std::string &_role );

    /// Adds a join key column.
    void add_join_key( const Matrix<Int> &_mat, const size_t _num );

    /// Adds a numerical column.
    void add_numerical( const Matrix<Float> &_mat, const size_t _num );

    /// Adds a target column.
    void add_target( const Matrix<Float> &_mat, const size_t _num );

    /// Adds a time stamp column.
    void add_time_stamp( const Matrix<Float> &_mat, const size_t _num );

    /// Calculate the number of bytes.
    template <class T>
    ULong calc_nbytes( const std::vector<Matrix<T>> &_columns ) const;

    /// Concatenate a set of colnames.
    std::vector<std::string> concat_colnames(
        const std::vector<std::string> &_categorical_names,
        const std::vector<std::string> &_discrete_names,
        const std::vector<std::string> &_join_key_names,
        const std::vector<std::string> &_numerical_names,
        const std::vector<std::string> &_target_names,
        const std::vector<std::string> &_time_stamp_names ) const;

    /// Parses int columns.
    void from_json(
        const Poco::JSON::Object &_obj,
        const std::vector<std::string> &_names,
        const std::string &_type,
        Encoding *_encoding );

    /// Parses float columns.
    void from_json(
        const Poco::JSON::Object &_obj,
        const std::vector<std::string> &_names,
        const std::string &_type );

    /// Parses time stamp columns.
    void from_json(
        const Poco::JSON::Object &_obj,
        const std::vector<std::string> &_names,
        const std::vector<std::string> &_time_formats );

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

    /// Creates a vector of vectors of type T.
    template <class T>
    std::vector<std::shared_ptr<std::vector<T>>> make_vectors(
        const size_t _size ) const;

    /// Saves all matrices.
    template <class T>
    void save_matrices(
        const std::vector<Matrix<T>> &_matrices,
        const std::string &_path,
        const std::string &_prefix ) const;

    /// Transforms a float to a time stamp
    std::string to_time_stamp( const Float &_time_stamp_float ) const;

    // -------------------------------

   private:
    /// Categorical data
    std::vector<Matrix<Int>> categoricals_;

    /// Maps integers to names of categories
    std::shared_ptr<Encoding> categories_;

    /// Discrete data
    std::vector<Matrix<Float>> discretes_;

    /// Performs the role of an "index" over the join keys
    std::vector<DataFrameIndex> indices_;

    /// Join keys - note that their might be several
    std::vector<Matrix<Int>> join_keys_;

    /// Maps integers to names of join keys
    std::shared_ptr<Encoding> join_keys_encoding_;

    /// Name of the data frame
    std::string name_;

    /// Numerical data
    std::vector<Matrix<Float>> numericals_;

    /// Targets - only exists for population tables
    std::vector<Matrix<Float>> targets_;

    /// Time stamps
    std::vector<Matrix<Float>> time_stamps_;
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
ULong DataFrame::calc_nbytes( const std::vector<Matrix<T>> &_columns ) const
{
    return std::accumulate(
        _columns.begin(),
        _columns.end(),
        static_cast<ULong>( 0 ),
        []( ULong &init, const Matrix<T> &mat ) {
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

            matrices.push_back( mat );
        }

    return matrices;
}

// ----------------------------------------------------------------------------

template <class T>
std::vector<std::shared_ptr<std::vector<T>>> DataFrame::make_vectors(
    const size_t _size ) const
{
    auto vectors = std::vector<std::shared_ptr<std::vector<T>>>( _size );

    for ( auto &vec : vectors ) vec = std::make_shared<std::vector<T>>( 0 );

    return vectors;
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
