#ifndef ENGINE_FEATUREENGINEERERS_FEATUREENGINEERER_HPP_
#define ENGINE_FEATUREENGINEERERS_FEATUREENGINEERER_HPP_

// ----------------------------------------------------------------------------

namespace engine
{
namespace featureengineerers
{
// ----------------------------------------------------------------------------

template <class FeatureEngineererType>
class FeatureEngineerer : public AbstractFeatureEngineerer
{
    // --------------------------------------------------------

   public:
    FeatureEngineerer(
        const std::shared_ptr<const std::vector<strings::String>>& _categories,
        const Poco::JSON::Object& _cmd,
        const std::shared_ptr<const Poco::JSON::Object>& _placeholder,
        const std::shared_ptr<const std::vector<std::string>>& _peripheral,
        const std::vector<Poco::JSON::Object::Ptr>& _dependencies )
        : categories_( _categories ),
          cmd_( _cmd ),
          dependencies_( _dependencies ),
          placeholder_( _placeholder ),
          peripheral_( _peripheral )
    {
    }

    /// Destructor.
    ~FeatureEngineerer() = default;

    // --------------------------------------------------------

   public:
    /// Returns the fingerprint of the feature engineerer (necessary to build
    /// the dependency graphs).
    Poco::JSON::Object::Ptr fingerprint() const final;

    /// Fits the model.
    void fit(
        const Poco::JSON::Object& _cmd,
        const std::shared_ptr<const monitoring::Logger>& _logger,
        const std::map<std::string, containers::DataFrame>& _data_frames,
        Poco::Net::StreamSocket* _socket ) final;

    /// Generate features.
    containers::Features transform(
        const Poco::JSON::Object& _cmd,
        const std::shared_ptr<const monitoring::Logger>& _logger,
        const std::map<std::string, containers::DataFrame>& _data_frames,
        Poco::Net::StreamSocket* _socket ) const final;

    // --------------------------------------------------------

   public:
    /// Creates a deep copy.
    std::shared_ptr<AbstractFeatureEngineerer> clone() const final
    {
        return std::make_shared<FeatureEngineerer<FeatureEngineererType>>(
            *this );
    }

    /// Whether the feature engineerer is used for classification.
    bool is_classification() const final
    {
        const auto loss_function =
            JSON::get_value<std::string>( cmd_, "loss_function_" );
        return ( loss_function != "SquareLoss" );
    }

    /// Loads the feature engineerer from a file designated by _fname.
    void load( const std::string& _fname ) final
    {
        const auto obj = load_json_obj( _fname );
        feature_engineerer_ =
            std::make_optional<FeatureEngineererType>( categories_, obj );
    }

    /// Returns the number of features in the feature engineerer.
    size_t num_features() const final
    {
        return feature_engineerer().num_features();
    }

    /// Whether the feature engineerer is for the premium version only.
    bool premium_only() const final
    {
        return FeatureEngineererType::premium_only_;
    }

    /// Saves the feature engineerer in JSON format, if applicable
    void save( const std::string& _fname ) const final
    {
        feature_engineerer().save( _fname );
    }

    /// Selects the features according to the index given.
    void select_features( const std::vector<size_t>& _index ) final
    {
        feature_engineerer().select_features( _index );
    }

    /// Whether the feature engineerer supports multiple targets.
    bool supports_multiple_targets() const final
    {
        return FeatureEngineererType::supports_multiple_targets_;
    }

    /// Return model as a JSON Object.
    Poco::JSON::Object to_json_obj( const bool _schema_only ) const final
    {
        return feature_engineerer().to_json_obj( _schema_only );
    }

    /// Returns model as a JSON Object in a form that the monitor can
    /// understand.
    Poco::JSON::Object to_monitor( const std::string& _name ) const final
    {
        return feature_engineerer().to_monitor( _name );
    }

    /// Return feature engineerer as SQL code.
    std::vector<std::string> to_sql(
        const size_t _offset, const bool _subfeatures ) const final
    {
        return feature_engineerer().to_sql( "", _offset, _subfeatures );
    }

    // --------------------------------------------------------

   private:
    /// Extract a data frame of type FeatureEngineererType::DataFrameType from
    /// an engine::containers::DataFrame.
    template <typename DataFrameType>
    DataFrameType extract_df(
        const std::string& _name,
        const std::map<std::string, containers::DataFrame>& _data_frames )
        const;

    /// Extract a data frame of type FeatureEngineererType::DataFrameType from
    /// an engine::containers::DataFrame using the pre-stored schema.
    template <typename DataFrameType, typename SchemaType>
    DataFrameType extract_df_by_colnames(
        const std::string& _name,
        const SchemaType& _schema,
        const std::map<std::string, containers::DataFrame>& _data_frames )
        const;

    /// Initializes the feature engineerer.
    std::optional<FeatureEngineererType> make_feature_engineerer(
        const Poco::JSON::Object& _cmd ) const;

    /// Helper function for loading a json object.
    Poco::JSON::Object load_json_obj( const std::string& _fname ) const;

    // --------------------------------------------------------

   private:
    /// Trivial accessor.
    FeatureEngineererType& feature_engineerer()
    {
        if ( !feature_engineerer_ )
            {
                throw std::invalid_argument(
                    "Feature engineering algorithm has not been fitted!" );
            }

        return *feature_engineerer_;
    }

    /// Trivial accessor.
    const FeatureEngineererType& feature_engineerer() const
    {
        if ( !feature_engineerer_ )
            {
                throw std::invalid_argument(
                    "Feature engineering algorithm has not been fitted!" );
            }

        return *feature_engineerer_;
    }

    // --------------------------------------------------------

   private:
    /// The categories used for the mapping.
    std::shared_ptr<const std::vector<strings::String>> categories_;

    /// The command used to create the feature engineerer.
    Poco::JSON::Object cmd_;

    /// The dependencies used to build the fingerprint.
    std::vector<Poco::JSON::Object::Ptr> dependencies_;

    /// The underlying feature engineering algorithm.
    std::optional<FeatureEngineererType> feature_engineerer_;

    /// The placeholder describing the data schema.
    std::shared_ptr<const Poco::JSON::Object> placeholder_;

    /// The names of the peripheral tables
    std::shared_ptr<const std::vector<std::string>> peripheral_;

    // --------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace featureengineerers
}  // namespace engine

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

namespace engine
{
namespace featureengineerers
{
// ----------------------------------------------------------------------------

template <typename FeatureEngineererType>
template <typename DataFrameType>
DataFrameType FeatureEngineerer<FeatureEngineererType>::extract_df(
    const std::string& _name,
    const std::map<std::string, containers::DataFrame>& _data_frames ) const
{
    // ------------------------------------------------------------------------

    const auto df = utils::Getter::get( _name, _data_frames );

    // ------------------------------------------------------------------------

    std::vector<typename DataFrameType::IntColumnType> categoricals;

    for ( size_t i = 0; i < df.num_categoricals(); ++i )
        {
            const auto& mat = df.categorical( i );

            categoricals.push_back( typename DataFrameType::IntColumnType(
                mat.data(), mat.name(), mat.nrows(), mat.unit() ) );
        }

    // ------------------------------------------------------------------------

    std::vector<typename DataFrameType::IntColumnType> join_keys;

    for ( size_t i = 0; i < df.num_join_keys(); ++i )
        {
            const auto& mat = df.join_key( i );

            join_keys.push_back( typename DataFrameType::IntColumnType(
                mat.data(), mat.name(), mat.nrows(), mat.unit() ) );
        }

    // ------------------------------------------------------------------------
    // The numerical/discrete binning strategy exists, but
    // the user does not have to think about it. Instead, this will make
    // the decision for him/her.

    std::vector<typename DataFrameType::FloatColumnType> discretes;

    std::vector<typename DataFrameType::FloatColumnType> numericals;

    const auto is_int = []( const Float val ) {
        return std::isnan( val ) || val == std::round( val );
    };

    for ( size_t i = 0; i < df.num_numericals(); ++i )
        {
            const auto& mat = df.numerical( i );

            const bool is_discrete =
                std::all_of( mat.begin(), mat.end(), is_int );

            if ( is_discrete )
                {
                    discretes.push_back(
                        typename DataFrameType::FloatColumnType(
                            mat.data(), mat.name(), mat.nrows(), mat.unit() ) );
                }
            else
                {
                    numericals.push_back(
                        typename DataFrameType::FloatColumnType(
                            mat.data(), mat.name(), mat.nrows(), mat.unit() ) );
                }
        }

    // ------------------------------------------------------------------------

    std::vector<typename DataFrameType::FloatColumnType> targets;

    for ( size_t i = 0; i < df.num_targets(); ++i )
        {
            const auto& mat = df.target( i );

            targets.push_back( typename DataFrameType::FloatColumnType(
                mat.data(), mat.name(), mat.nrows(), mat.unit() ) );
        }

    // ------------------------------------------------------------------------

    std::vector<typename DataFrameType::FloatColumnType> time_stamps;

    for ( size_t i = 0; i < df.num_time_stamps(); ++i )
        {
            const auto& mat = df.time_stamp( i );

            time_stamps.push_back( typename DataFrameType::FloatColumnType(
                mat.data(), mat.name(), mat.nrows(), mat.unit() ) );
        }

    // ------------------------------------------------------------------------

    return DataFrameType(
        categoricals,
        discretes,
        df.maps(),
        join_keys,
        _name,
        numericals,
        targets,
        time_stamps );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

template <typename FeatureEngineererType>
template <typename DataFrameType, typename SchemaType>
DataFrameType FeatureEngineerer<FeatureEngineererType>::extract_df_by_colnames(
    const std::string& _name,
    const SchemaType& _schema,
    const std::map<std::string, containers::DataFrame>& _data_frames ) const
{
    // ------------------------------------------------------------------------

    const auto df = utils::Getter::get( _name, _data_frames );

    // ------------------------------------------------------------------------

    try
        {
            // ------------------------------------------------------------------------

            std::vector<typename DataFrameType::IntColumnType> categoricals;

            for ( size_t i = 0; i < _schema.num_categoricals(); ++i )
                {
                    const auto& name = _schema.categorical_name( i );

                    const auto& mat = df.categorical( name );

                    categoricals.push_back(
                        typename DataFrameType::IntColumnType(
                            mat.data(), name, mat.nrows(), mat.unit() ) );
                }

            // ------------------------------------------------------------------------

            std::vector<typename DataFrameType::FloatColumnType> discretes;

            for ( size_t i = 0; i < _schema.num_discretes(); ++i )
                {
                    const auto& name = _schema.discrete_name( i );

                    // Note that discrete columns actually do not exist
                    // in the DataFrame - they are taken from numerical
                    // instead.
                    const auto& mat = df.numerical( name );

                    discretes.push_back(
                        typename DataFrameType::FloatColumnType(
                            mat.data(), name, mat.nrows(), mat.unit() ) );
                }

            // ------------------------------------------------------------------------

            std::vector<typename DataFrameType::IntColumnType> join_keys;

            for ( size_t i = 0; i < _schema.num_join_keys(); ++i )
                {
                    const auto& name = _schema.join_keys_name( i );

                    const auto& mat = df.join_key( name );

                    join_keys.push_back( typename DataFrameType::IntColumnType(
                        mat.data(), name, mat.nrows(), mat.unit() ) );
                }

            // ------------------------------------------------------------------------

            std::vector<typename DataFrameType::FloatColumnType> numericals;

            for ( size_t i = 0; i < _schema.num_numericals(); ++i )
                {
                    const auto& name = _schema.numerical_name( i );

                    const auto& mat = df.numerical( name );

                    numericals.push_back(
                        typename DataFrameType::FloatColumnType(
                            mat.data(), name, mat.nrows(), mat.unit() ) );
                }

            // ------------------------------------------------------------------------

            std::vector<typename DataFrameType::FloatColumnType> targets;

            for ( size_t i = 0; i < _schema.num_targets(); ++i )
                {
                    const auto& name = _schema.target_name( i );

                    if ( df.has_target( name ) )
                        {
                            const auto& mat = df.target( name );

                            targets.push_back(
                                typename DataFrameType::FloatColumnType(
                                    mat.data(),
                                    name,
                                    mat.nrows(),
                                    mat.unit() ) );
                        }
                }

            // ------------------------------------------------------------------------

            std::vector<typename DataFrameType::FloatColumnType> time_stamps;

            for ( size_t i = 0; i < _schema.num_time_stamps(); ++i )
                {
                    const auto& name = _schema.time_stamps_name( i );

                    const auto& mat = df.time_stamp( name );

                    time_stamps.push_back(
                        typename DataFrameType::FloatColumnType(
                            mat.data(), name, mat.nrows(), mat.unit() ) );
                }

            // ------------------------------------------------------------------------

            return DataFrameType(
                categoricals,
                discretes,
                df.maps(),
                join_keys,
                _name,
                numericals,
                targets,
                time_stamps );

            // ------------------------------------------------------------------------
        }
    catch ( std::exception& e )
        {
            throw std::invalid_argument(
                std::string( e.what() ) +
                " Is it possible that your peripheral tables are in the wrong "
                "order?" );
        }

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

template <typename FeatureEngineererType>
Poco::JSON::Object::Ptr FeatureEngineerer<FeatureEngineererType>::fingerprint()
    const
{
    auto obj = Poco::JSON::Object::Ptr( new Poco::JSON::Object() );

    obj->set( "cmd_", cmd_ );
    obj->set( "dependencies_", JSON::vector_to_array_ptr( dependencies_ ) );

    return obj;
}

// ----------------------------------------------------------------------------

template <typename FeatureEngineererType>
void FeatureEngineerer<FeatureEngineererType>::fit(
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<const monitoring::Logger>& _logger,
    const std::map<std::string, containers::DataFrame>& _data_frames,
    Poco::Net::StreamSocket* _socket )
{
    // ------------------------------------------------
    // Extract the peripheral tables.

    const auto peripheral_names = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "peripheral_names_" ) );

    std::vector<typename FeatureEngineererType::DataFrameType>
        peripheral_tables = {};

    for ( auto& name : peripheral_names )
        {
            const auto df =
                extract_df<typename FeatureEngineererType::DataFrameType>(
                    name, _data_frames );

            peripheral_tables.push_back( df );
        }

    // ------------------------------------------------
    // Extract the population table.

    const auto population_name =
        JSON::get_value<std::string>( _cmd, "population_name_" );

    const auto population_table =
        extract_df<typename FeatureEngineererType::DataFrameType>(
            population_name, _data_frames );

    const auto population_df =
        utils::Getter::get( population_name, _data_frames );

    // ------------------------------------------------
    // Fit the feature engineerer.

    auto new_feature_engineerer = make_feature_engineerer( cmd_ );

    new_feature_engineerer->fit( population_table, peripheral_tables, _logger );

    // ------------------------------------------------
    // Fitting ran through without any problems - let's store the result.

    feature_engineerer_ = std::move( new_feature_engineerer );

    // ------------------------------------------------
}

// ------------------------------------------------------------------------

template <typename FeatureEngineererType>
Poco::JSON::Object FeatureEngineerer<FeatureEngineererType>::load_json_obj(
    const std::string& _fname ) const
{
    std::ifstream input( _fname );

    std::stringstream json;

    std::string line;

    if ( input.is_open() )
        {
            while ( std::getline( input, line ) )
                {
                    json << line;
                }

            input.close();
        }
    else
        {
            throw std::invalid_argument( "File '" + _fname + "' not found!" );
        }

    const auto ptr = Poco::JSON::Parser()
                         .parse( json.str() )
                         .extract<Poco::JSON::Object::Ptr>();

    if ( !ptr )
        {
            throw std::runtime_error( "JSON file did not contain an object!" );
        }

    return *ptr;
}

// ----------------------------------------------------------------------------

template <typename FeatureEngineererType>
std::optional<FeatureEngineererType>
FeatureEngineerer<FeatureEngineererType>::make_feature_engineerer(
    const Poco::JSON::Object& _cmd ) const
{
    const auto hyperparameters =
        std::make_shared<typename FeatureEngineererType::HypType>( _cmd );

    const auto placeholder =
        std::make_shared<const typename FeatureEngineererType::PlaceholderType>(
            *placeholder_ );

    // TODO
    auto population_schema = std::shared_ptr<
        const typename FeatureEngineererType::PlaceholderType>();

    /*if ( _cmd.has( "population_schema_" ) )
        {
            population_schema =
                std::make_shared<const multirel::containers::Placeholder>(
                    *JSON::get_object( _cmd, "population_schema_" ) );
        }*/

    auto peripheral_schema = std::shared_ptr<
        const std::vector<typename FeatureEngineererType::PlaceholderType>>();

    // TODO
    /*if ( _cmd.has( "peripheral_schema_" ) )
        {
            std::vector<multirel::containers::Placeholder> peripheral;

            const auto peripheral_arr =
                *JSON::get_array( _cmd, "peripheral_schema_" );

            for ( size_t i = 0; i < peripheral_arr.size(); ++i )
                {
                    const auto ptr = peripheral_arr.getObject(
                        static_cast<unsigned int>( i ) );

                    if ( !ptr )
                        {
                            throw std::invalid_argument(
                                "peripheral_schema_, element " +
                                std::to_string( i ) +
                                " is not an Object!" );
                        }

                    peripheral.push_back(
                        multirel::containers::Placeholder( *ptr ) );
                }

            peripheral_schema = std::make_shared<
                const std::vector<multirel::containers::Placeholder>>(
                peripheral );
        }*/

    return std::make_optional<FeatureEngineererType>(
        categories_,
        hyperparameters,
        peripheral_,
        placeholder,
        peripheral_schema,
        population_schema );
}

// ----------------------------------------------------------------------------

template <typename FeatureEngineererType>
containers::Features FeatureEngineerer<FeatureEngineererType>::transform(
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<const monitoring::Logger>& _logger,
    const std::map<std::string, containers::DataFrame>& _data_frames,
    Poco::Net::StreamSocket* _socket ) const
{
    // -------------------------------------------------------------------------
    // Extract the peripheral tables

    const auto peripheral_schema = feature_engineerer().peripheral_schema();

    const auto peripheral_names = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "peripheral_names_" ) );

    if ( peripheral_schema.size() != peripheral_names.size() )
        {
            throw std::invalid_argument(
                "Expected " + std::to_string( peripheral_schema.size() ) +
                " peripheral tables, got " +
                std::to_string( peripheral_names.size() ) + "." );
        }

    std::vector<typename FeatureEngineererType::DataFrameType>
        peripheral_tables = {};

    for ( size_t i = 0; i < peripheral_names.size(); ++i )
        {
            const auto df = extract_df_by_colnames<
                typename FeatureEngineererType::DataFrameType>(
                peripheral_names[i], peripheral_schema[i], _data_frames );

            peripheral_tables.push_back( df );
        }

    // -------------------------------------------------------------------------
    // Extract the population table

    const auto population_schema = feature_engineerer().population_schema();

    const auto population_name =
        JSON::get_value<std::string>( _cmd, "population_name_" );

    const auto population_table =
        extract_df_by_colnames<typename FeatureEngineererType::DataFrameType>(
            population_name, population_schema, _data_frames );

    // -------------------------------------------------------------------------
    // Generate the features.

    return feature_engineerer().transform(
        population_table, peripheral_tables, _logger );

    // -------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------
}  // namespace featureengineerers
}  // namespace engine

// ----------------------------------------------------------------------------

// ----------------------------------------------------------------------------

#endif  // ENGINE_FEATUREENGINEERERS_FEATUREENGINEERER_HPP_

