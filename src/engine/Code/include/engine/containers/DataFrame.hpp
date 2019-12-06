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
        const std::string &_name,
        const std::shared_ptr<Encoding> &_categories,
        const std::shared_ptr<Encoding> &_join_keys_encoding )
        : categories_( _categories ),
          join_keys_encoding_( _join_keys_encoding ),
          name_( _name )
    {
    }

    ~DataFrame() = default;

    // -------------------------------

    /// Setter for a float_column
    void add_float_column(
        const Column<Float> &_col, const std::string &_role );

    /// Setter for an int_column
    void add_int_column( const Column<Int> &_col, const std::string _role );

    /// Setter for a string_column
    void add_string_column( const Column<strings::String> &_col );

    /// Appends another data frame to this data frame.
    void append( const DataFrame &_other );

    /// Makes sure that the data contained in the DataFrame is plausible
    /// and consistent.
    void check_plausibility() const;

    /// Builds indices_, which serve the role of
    /// an "index" over the join keys
    void create_indices();

    /// Getter for a float_column.
    const Column<Float> &float_column(
        const std::string &_role, const size_t _num ) const;

    /// Getter for a float column.
    const Column<Float> &float_column(
        const std::string &_name, const std::string &_role ) const;

    /// Builds a dataframe from one or several CSV files.
    void from_csv(
        const std::vector<std::string> &_fnames,
        const std::string &_quotechar,
        const std::string &_sep,
        const std::vector<std::string> &_time_formats,
        const std::vector<std::string> &_categorical_names,
        const std::vector<std::string> &_discrete_names,
        const std::vector<std::string> &_join_key_names,
        const std::vector<std::string> &_numerical_names,
        const std::vector<std::string> &_target_names,
        const std::vector<std::string> &_time_stamp_names,
        const std::vector<std::string> &_undefined_float_names,
        const std::vector<std::string> &_undefined_integer_names,
        const std::vector<std::string> &_undefined_string_names );

    /// Builds a dataframe from a table in the data base.
    void from_db(
        const std::shared_ptr<database::Connector> _connector,
        const std::string &_tname,
        const std::vector<std::string> &_categorical_names,
        const std::vector<std::string> &_discrete_names,
        const std::vector<std::string> &_join_key_names,
        const std::vector<std::string> &_numerical_names,
        const std::vector<std::string> &_target_names,
        const std::vector<std::string> &_time_stamp_names,
        const std::vector<std::string> &_undefined_float_names,
        const std::vector<std::string> &_undefined_integer_names,
        const std::vector<std::string> &_undefined_string_names );

    /// Builds a dataframe from a query.
    void from_query(
        const std::shared_ptr<database::Connector> _connector,
        const std::string &_query,
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
        const std::vector<std::string> &_time_stamps,
        const std::vector<std::string> &_undefined_float_names,
        const std::vector<std::string> &_undefined_integer_names,
        const std::vector<std::string> &_undefined_string_names );

    /// Returns the encodings as a property tree
    Poco::JSON::Object get_colnames();

    /// Returns the content of the data frame in a format that is compatible
    /// with the DataTables.js server-side processing API.
    Poco::JSON::Object get_content(
        const std::int32_t _draw,
        const std::int32_t _start,
        const std::int32_t _length ) const;

    /// Getter for an int_column (either join keys or categorical)
    const Column<Int> &int_column(
        const std::string &_role, const size_t _num ) const;

    /// Getter for an int_column.
    const Column<Int> &int_column(
        const std::string &_name, const std::string &_role ) const;

    /// Loads the data from the hard-disk into the engine
    void load( const std::string &_path );

    /// Returns number of bytes occupied by the data
    ULong nbytes() const;

    /// Get the number of rows or return 0, if the DataFrame contains no
    /// columns.
    const size_t nrows() const;

    /// Removes a column.
    bool remove_column( const std::string &_name );

    /// Saves the data on the engine
    void save( const std::string &_path );

    /// Extracts the data frame as a Poco::JSON::Object the monitor process can
    /// understand
    Poco::JSON::Object to_monitor() const;

    /// Transforms a float to a time stamp
    std::string to_time_stamp( const Float &_time_stamp_float ) const;

    /// Selects all rows for which the corresponding entry in _condition is
    /// true.
    void where( const std::vector<bool> &_condition );

    // -------------------------------

    /// Trivial accessor
    template <
        typename T,
        typename std::enable_if<!std::is_same<T, std::string>::value, int>::
            type = 0>
    const Column<Int> &categorical( const T _i ) const
    {
        assert_true( categoricals_.size() > 0 );
        assert_true( _i >= 0 );
        assert_true( _i < static_cast<T>( categoricals_.size() ) );

        return categoricals_[_i];
    }

    /// Trivial accessor
    const Column<Int> &categorical( const std::string &_name ) const
    {
        for ( size_t i = 0; i < num_categoricals(); ++i )
            {
                if ( categorical( i ).name() == _name )
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
    std::string category( const size_t _i ) const
    {
        assert_true( _i < categories().size() );

        return categories()[_i].str();
    }

    /// Trivial accessor
    template <
        typename T,
        typename std::enable_if<!std::is_same<T, std::string>::value, int>::
            type = 0>
    const Column<Float> &discrete( const T _i ) const
    {
        assert_true( _i >= 0 );
        assert_true( _i < static_cast<T>( discretes_.size() ) );

        return discretes_[_i];
    }

    /// Trivial accessor
    const Column<Float> &discrete( const std::string &_name ) const
    {
        for ( size_t i = 0; i < num_discretes(); ++i )
            {
                if ( discrete( i ).name() == _name )
                    {
                        return discrete( i );
                    }
            }

        throw std::invalid_argument(
            "Data frame '" + name_ + "' contains no discrete column named '" +
            _name + "'!" );
    }

    /// Whether the DataFrame has any column named _name.
    bool has( const std::string &_name ) const
    {
        return has_categorical( _name ) || has_discrete( _name ) ||
               has_join_key( _name ) || has_numerical( _name ) ||
               has_target( _name ) || has_time_stamp( _name ) ||
               has_undefined_float( _name ) || has_undefined_integer( _name ) ||
               has_undefined_string( _name );
    }

    /// Whether the DataFrame has a categorical column named _name.
    bool has_categorical( const std::string &_name ) const
    {
        for ( size_t i = 0; i < num_categoricals(); ++i )
            {
                if ( categorical( i ).name() == _name )
                    {
                        return true;
                    }
            }

        return false;
    }

    /// Whether the DataFrame has a discrete column named _name.
    bool has_discrete( const std::string &_name ) const
    {
        for ( size_t i = 0; i < num_discretes(); ++i )
            {
                if ( discrete( i ).name() == _name )
                    {
                        return true;
                    }
            }

        return false;
    }

    /// Whether the DataFrame has a join_key named _name.
    bool has_join_key( const std::string &_name ) const
    {
        for ( size_t i = 0; i < num_join_keys(); ++i )
            {
                if ( join_key( i ).name() == _name )
                    {
                        return true;
                    }
            }

        return false;
    }

    /// Whether the DataFrame has a numerical column named _name.
    bool has_numerical( const std::string &_name ) const
    {
        for ( size_t i = 0; i < num_numericals(); ++i )
            {
                if ( numerical( i ).name() == _name )
                    {
                        return true;
                    }
            }

        return false;
    }

    /// Whether the DataFrame has a target column of named _name.
    bool has_target( const std::string &_name ) const
    {
        for ( size_t i = 0; i < num_targets(); ++i )
            {
                if ( target( i ).name() == _name )
                    {
                        return true;
                    }
            }

        return false;
    }

    /// Whether the DataFrame has a time_stamp column of named _name.
    bool has_time_stamp( const std::string &_name ) const
    {
        for ( size_t i = 0; i < num_time_stamps(); ++i )
            {
                if ( time_stamp( i ).name() == _name )
                    {
                        return true;
                    }
            }

        return false;
    }

    /// Whether the DataFrame has an undefined float column name _name.
    bool has_undefined_float( const std::string &_name ) const
    {
        for ( size_t i = 0; i < num_undefined_floats(); ++i )
            {
                if ( undefined_float( i ).name() == _name )
                    {
                        return true;
                    }
            }

        return false;
    }

    /// Whether the DataFrame has an undefined int column named _name.
    bool has_undefined_integer( const std::string &_name ) const
    {
        for ( size_t i = 0; i < num_undefined_integers(); ++i )
            {
                if ( undefined_integer( i ).name() == _name )
                    {
                        return true;
                    }
            }

        return false;
    }

    /// Whether the DataFrame has an undefined string column named _name.
    bool has_undefined_string( const std::string &_name ) const
    {
        for ( size_t i = 0; i < num_undefined_strings(); ++i )
            {
                if ( undefined_string( i ).name() == _name )
                    {
                        return true;
                    }
            }

        return false;
    }

    /// Returns the index signified by index _i
    template <class T>
    DataFrameIndex &index( T _i )
    {
        assert_true( indices_.size() == join_keys_.size() );
        assert_true( join_keys_.size() > 0 );
        assert_true( _i >= 0 );
        assert_true( static_cast<size_t>( _i ) < indices_.size() );

        return indices_[_i];
    }

    /// Returns the index signified by index _i
    template <class T>
    const DataFrameIndex index( T _i ) const
    {
        assert_true( indices_.size() == join_keys_.size() );
        assert_true( join_keys_.size() > 0 );
        assert_true( _i >= 0 );
        assert_true( static_cast<size_t>( _i ) < indices_.size() );

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
    const Column<Int> &join_key( const T _i ) const
    {
        assert_true( join_keys_.size() > 0 );
        assert_true( _i >= 0 );
        assert_true( _i < static_cast<T>( join_keys_.size() ) );

        return join_keys_[_i];
    }

    /// Trivial accessor
    const Column<Int> &join_key( const std::string &_name ) const
    {
        for ( size_t i = 0; i < num_join_keys(); ++i )
            {
                if ( join_key( i ).name() == _name )
                    {
                        return join_key( i );
                    }
            }

        throw std::invalid_argument(
            "Data frame '" + name_ + "' contains no join key named '" + _name +
            "'!" );
    }

    /// Trivial accessor
    const std::vector<Column<Int>> &join_keys() const { return join_keys_; }

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
    const std::string &name() const { return name_; }

    /// Get the number of columns.
    const size_t ncols() const
    {
        return undefined_floats_.size() + undefined_integers_.size() +
               undefined_strings_.size() + join_keys_.size() +
               time_stamps_.size() + categoricals_.size() + discretes_.size() +
               numericals_.size() + targets_.size();
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

    /// Returns number of undefined float columns.
    size_t const num_undefined_floats() const
    {
        return undefined_floats_.size();
    }

    /// Returns number of undefined integer columns.
    size_t const num_undefined_integers() const
    {
        return undefined_integers_.size();
    }

    /// Returns number of undefined string columns.
    size_t const num_undefined_strings() const
    {
        return undefined_strings_.size();
    }

    /// Trivial accessor
    template <
        typename T,
        typename std::enable_if<!std::is_same<T, std::string>::value, int>::
            type = 0>
    const Column<Float> &numerical( const T _i ) const
    {
        assert_true( numericals_.size() > 0 );
        assert_true( _i >= 0 );
        assert_true( _i < static_cast<T>( numericals_.size() ) );

        return numericals_[_i];
    }

    /// Trivial accessor
    const Column<Float> &numerical( const std::string &_name ) const
    {
        for ( size_t i = 0; i < num_numericals(); ++i )
            {
                if ( numerical( i ).name() == _name )
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

    /// Primitive setter
    void set_name( const std::string &_name ) { name_ = _name; }

    /// Trivial accessor
    template <
        typename T,
        typename std::enable_if<!std::is_same<T, std::string>::value, int>::
            type = 0>
    const Column<Float> &target( const T _i ) const
    {
        assert_true( targets_.size() > 0 );
        assert_true( _i >= 0 );
        assert_true( _i < static_cast<T>( targets_.size() ) );

        return targets_[_i];
    }

    /// Trivial accessor
    const Column<Float> &target( const std::string &_name ) const
    {
        for ( size_t i = 0; i < num_targets(); ++i )
            {
                if ( target( i ).name() == _name )
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
    Column<Float> const &time_stamp( const T _i ) const
    {
        assert_true( time_stamps_.size() > 0 );
        assert_true( _i >= 0 );
        assert_true( _i < static_cast<T>( time_stamps_.size() ) );

        return time_stamps_[_i];
    }

    /// Trivial accessor
    const Column<Float> &time_stamp( const std::string &_name ) const
    {
        for ( size_t i = 0; i < num_time_stamps(); ++i )
            {
                if ( time_stamp( i ).name() == _name )
                    {
                        return time_stamp( i );
                    }
            }

        throw std::invalid_argument(
            "Data frame '" + name_ + "' contains no time stamp named '" +
            _name + "'!" );
    }

    /// Trivial accessor
    const std::vector<Column<Float>> &time_stamps() const
    {
        return time_stamps_;
    }

    /// Trivial accessor
    template <
        typename T,
        typename std::enable_if<!std::is_same<T, std::string>::value, int>::
            type = 0>
    const Column<Float> &undefined_float( const T _i ) const
    {
        assert_true( undefined_floats_.size() > 0 );
        assert_true( _i >= 0 );
        assert_true( _i < static_cast<T>( undefined_floats_.size() ) );

        return undefined_floats_[_i];
    }

    /// Trivial accessor
    const Column<Float> &undefined_float( const std::string &_name ) const
    {
        for ( size_t i = 0; i < num_undefined_floats(); ++i )
            {
                if ( undefined_float( i ).name() == _name )
                    {
                        return undefined_float( i );
                    }
            }

        throw std::invalid_argument(
            "Data frame '" + name_ +
            "' contains no undefined float column named '" + _name + "'!" );
    }

    /// Trivial accessor
    template <
        typename T,
        typename std::enable_if<!std::is_same<T, std::string>::value, int>::
            type = 0>
    const Column<Int> &undefined_integer( const T _i ) const
    {
        assert_true( undefined_integers_.size() > 0 );
        assert_true( _i >= 0 );
        assert_true( _i < static_cast<T>( undefined_integers_.size() ) );

        return undefined_integers_[_i];
    }

    /// Trivial accessor
    const Column<Int> &undefined_integer( const std::string &_name ) const
    {
        for ( size_t i = 0; i < num_undefined_integers(); ++i )
            {
                if ( undefined_integer( i ).name() == _name )
                    {
                        return undefined_integer( i );
                    }
            }

        throw std::invalid_argument(
            "Data frame '" + name_ +
            "' contains no undefined integer column named '" + _name + "'!" );
    }

    /// Trivial accessor
    template <
        typename T,
        typename std::enable_if<!std::is_same<T, std::string>::value, int>::
            type = 0>
    const Column<strings::String> &undefined_string( const T _i ) const
    {
        assert_true( undefined_strings_.size() > 0 );
        assert_true( _i >= 0 );
        assert_true( _i < static_cast<T>( undefined_strings_.size() ) );

        return undefined_strings_[_i];
    }

    /// Trivial accessor
    const Column<strings::String> &undefined_string(
        const std::string &_name ) const
    {
        for ( size_t i = 0; i < num_undefined_strings(); ++i )
            {
                if ( undefined_string( i ).name() == _name )
                    {
                        return undefined_string( i );
                    }
            }

        throw std::invalid_argument(
            "Data frame '" + name_ +
            "' contains no undefined string column named '" + _name + "'!" );
    }

    // -------------------------------

   private:
    /// Adds a column to _columns.
    template <class ColType>
    void add_column( const ColType &_col, std::vector<ColType> *_columns );

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

    /// Adds a vector of string vectors.
    void add_string_vectors(
        const std::vector<std::string> &_names,
        const std::vector<std::shared_ptr<std::vector<strings::String>>>
            &_vectors );

    /// Calculate the number of bytes.
    template <class T>
    ULong calc_nbytes( const std::vector<Column<T>> &_columns ) const;

    /// Concatenate a set of colnames.
    std::vector<std::string> concat_colnames(
        const std::vector<std::string> &_categorical_names,
        const std::vector<std::string> &_discrete_names,
        const std::vector<std::string> &_join_key_names,
        const std::vector<std::string> &_numerical_names,
        const std::vector<std::string> &_target_names,
        const std::vector<std::string> &_time_stamp_names,
        const std::vector<std::string> &_undefined_float_names,
        const std::vector<std::string> &_undefined_integer_names,
        const std::vector<std::string> &_undefined_string_names ) const;

    /// Builds a dataframe from a CSV file.
    void from_csv(
        const std::string &_fname,
        const std::string &_quotechar,
        const std::string &_sep,
        const std::vector<std::string> &_time_formats,
        const std::vector<std::string> &_categorical_names,
        const std::vector<std::string> &_discrete_names,
        const std::vector<std::string> &_join_key_names,
        const std::vector<std::string> &_numerical_names,
        const std::vector<std::string> &_target_names,
        const std::vector<std::string> &_time_stamp_names,
        const std::vector<std::string> &_undefined_float_names,
        const std::vector<std::string> &_undefined_integer_names,
        const std::vector<std::string> &_undefined_string_names );

    /// Parses int columns.
    void from_json(
        const Poco::JSON::Object &_obj,
        const std::vector<std::string> &_names,
        const std::string &_role,
        Encoding *_encoding );

    /// Parses float columns.
    void from_json(
        const Poco::JSON::Object &_obj,
        const std::vector<std::string> &_names,
        const std::string &_role );

    /// Parses time stamp columns.
    void from_json(
        const Poco::JSON::Object &_obj,
        const std::vector<std::string> &_names,
        const std::vector<std::string> &_time_formats );

    /// Returns the colnames of a vector of columns
    template <class T>
    Poco::JSON::Array get_colnames(
        const std::vector<Column<T>> &_columns ) const;

    /// Returns the units of a vector of columns
    template <class T>
    Poco::JSON::Array get_units( const std::vector<Column<T>> &_columns ) const;

    /// Loads columns.
    template <class T>
    std::vector<Column<T>> load_matrices(
        const std::string &_path, const std::string &_prefix ) const;

    /// Creates a vector of vectors of type T.
    template <class T>
    std::vector<std::shared_ptr<std::vector<T>>> make_vectors(
        const size_t _size ) const;

    /// Returns the colnames of a vector of columns
    template <class T>
    bool rm_col(
        const std::string &_name,
        std::vector<Column<T>> *_columns,
        std::vector<DataFrameIndex> *_indices = nullptr ) const;

    /// Saves all matrices.
    template <class T>
    void save_matrices(
        const std::vector<Column<T>> &_matrices,
        const std::string &_path,
        const std::string &_prefix ) const;

   private:
    /// Custom string conversions (produces more beautiful numbers than
    /// std::to_string)
    std::string to_string( const Float &_val ) const
    {
        std::ostringstream stream;
        stream << _val;
        return stream.str();
    }

    // -------------------------------

   private:
    /// Categorical data
    std::vector<Column<Int>> categoricals_;

    /// Maps integers to names of categories
    std::shared_ptr<Encoding> categories_;

    /// Discrete data
    std::vector<Column<Float>> discretes_;

    /// Performs the role of an "index" over the join keys
    std::vector<DataFrameIndex> indices_;

    /// Join keys - note that there might be several
    std::vector<Column<Int>> join_keys_;

    /// Maps integers to names of join keys
    std::shared_ptr<Encoding> join_keys_encoding_;

    /// Name of the data frame
    std::string name_;

    /// Numerical data
    std::vector<Column<Float>> numericals_;

    /// "Undefined" floats - undefined means that
    /// no explicit role has been set yet.
    std::vector<Column<Float>> undefined_floats_;

    /// "Undefined" integers - undefined means that
    /// no explicit role has been set yet.
    std::vector<Column<Int>> undefined_integers_;

    /// "Undefined" strings - undefined means that
    /// no explicit role has been set yet.
    std::vector<Column<strings::String>> undefined_strings_;

    /// Targets - only exists for population tables
    std::vector<Column<Float>> targets_;

    /// Time stamps
    std::vector<Column<Float>> time_stamps_;
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
// ----------------------------------------------------------------------------

template <class ColType>
void DataFrame::add_column(
    const ColType &_col, std::vector<ColType> *_columns )
{
    if ( _col.nrows() != nrows() && ncols() != 0 )
        {
            throw std::runtime_error(
                "Column '" + _col.name() + "' is of length " +
                std::to_string( _col.nrows() ) + ", expected " +
                std::to_string( nrows() ) + "." );
        }

    remove_column( _col.name() );

    _columns->push_back( _col );
}

// -------------------------------------------------------------------------

template <class T>
ULong DataFrame::calc_nbytes( const std::vector<Column<T>> &_columns ) const
{
    return std::accumulate(
        _columns.begin(),
        _columns.end(),
        static_cast<ULong>( 0 ),
        []( ULong &init, const Column<T> &col ) {
            return init + col.nbytes();
        } );
}

// -------------------------------------------------------------------------

template <class T>
Poco::JSON::Array DataFrame::get_colnames(
    const std::vector<Column<T>> &_columns ) const
{
    std::vector<std::string> colnames;

    std::for_each(
        _columns.begin(), _columns.end(), [&colnames]( const Column<T> &col ) {
            colnames.push_back( col.name() );
        } );

    return JSON::vector_to_array( colnames );
}

// -------------------------------------------------------------------------

template <class T>
Poco::JSON::Array DataFrame::get_units(
    const std::vector<Column<T>> &_columns ) const
{
    std::vector<std::string> units;

    std::for_each(
        _columns.begin(), _columns.end(), [&units]( const Column<T> &col ) {
            units.push_back( col.unit() );
        } );

    return JSON::vector_to_array( units );
}

// ----------------------------------------------------------------------------

template <class T>
std::vector<Column<T>> DataFrame::load_matrices(
    const std::string &_path, const std::string &_prefix ) const
{
    std::vector<Column<T>> matrices;

    for ( size_t i = 0; true; ++i )
        {
            std::string fname = _path + _prefix + std::to_string( i );

            if ( !Poco::File( fname ).exists() )
                {
                    break;
                }

            Column<T> col;

            col.load( fname );

            matrices.push_back( col );
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
bool DataFrame::rm_col(
    const std::string &_name,
    std::vector<Column<T>> *_columns,
    std::vector<DataFrameIndex> *_indices ) const
{
    const auto has_name = [_name]( const Column<T> &_col ) {
        return _col.name() == _name;
    };

    const auto it =
        std::find_if( _columns->begin(), _columns->end(), has_name );

    if ( it == _columns->end() )
        {
            return false;
        }

    if ( _indices )
        {
            assert_true( _indices->size() == _columns->size() );

            const auto dist = std::distance( _columns->begin(), it );

            _indices->erase( _indices->begin() + dist );
        }

    _columns->erase( it );

    return true;
}

// ----------------------------------------------------------------------------

template <class T>
void DataFrame::save_matrices(
    const std::vector<Column<T>> &_matrices,
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
