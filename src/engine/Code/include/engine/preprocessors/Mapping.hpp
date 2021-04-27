#ifndef ENGINE_PREPROCESSORS_MAPPING_HPP_
#define ENGINE_PREPROCESSORS_MAPPING_HPP_

namespace engine
{
namespace preprocessors
{
// ----------------------------------------------------

class Mapping : public Preprocessor
{
   public:
    typedef typename helpers::MappingContainer::Colnames Colnames;
    typedef typename helpers::MappingContainer::MappingForDf MappingForDf;
    typedef std::vector<
        std::shared_ptr<const std::map<std::string, std::vector<Float>>>>
        TextMapping;

   public:
    Mapping() : min_freq_( 0 ) {}

    Mapping(
        const Poco::JSON::Object& _obj,
        const std::vector<Poco::JSON::Object::Ptr>& _dependencies )
        : min_freq_( 0 )
    {
        *this = from_json_obj( _obj );
        dependencies_ = _dependencies;
    }

    ~Mapping() = default;

   public:
    /// Returns the fingerprint of the preprocessor (necessary to build
    /// the dependency graphs).
    Poco::JSON::Object::Ptr fingerprint() const final;

    /// Identifies which features should be extracted from which time stamps.
    std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
    fit_transform(
        const Poco::JSON::Object& _cmd,
        const std::shared_ptr<containers::Encoding>& _categories,
        const containers::DataFrame& _population_df,
        const std::vector<containers::DataFrame>& _peripheral_dfs ) final;

    /// Expresses the Seasonal preprocessor as a JSON object.
    Poco::JSON::Object::Ptr to_json_obj() const final;

    /// Transforms the data frames by adding the desired time series
    /// transformations.
    std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
    transform(
        const Poco::JSON::Object& _cmd,
        const std::shared_ptr<const containers::Encoding> _categories,
        const containers::DataFrame& _population_df,
        const std::vector<containers::DataFrame>& _peripheral_dfs ) const final;

    /// Generates the mapping tables.
    std::vector<std::string> to_sql(
        const std::shared_ptr<const std::vector<strings::String>>& _categories )
        const final;

   public:
    /// Trivial (const) accessor.
    const std::vector<std::string>& aggregation() const { return aggregation_; }

    /// Trivial (const) accessor.
    const std::vector<helpers::MappingAggregation>& aggregation_enums() const
    {
        return aggregation_enums_;
    }

    /// Creates a deep copy.
    std::shared_ptr<Preprocessor> clone() const final
    {
        return std::make_shared<Mapping>( *this );
    }

    /// Returns the type of the preprocessor.
    std::string type() const final { return Preprocessor::MAPPING; }

    /// Trivial (const) accessor.
    size_t min_freq() const { return min_freq_; }

   private:
    /// Transforms the mappings for the categorical columns to SQL.
    std::vector<std::string> categorical_columns_to_sql(
        const std::shared_ptr<const std::vector<strings::String>>& _categories )
        const;

    /// Transforms the mappings for the discrete columns to SQL.
    std::vector<std::string> discrete_columns_to_sql() const;

    /// Transforms the mappings for the text columns to SQL.
    std::vector<std::string> text_columns_to_sql() const;

    /// Extracts the text mapping.
    typename Mapping::TextMapping extract_text_mapping(
        const Poco::JSON::Object& _obj, const std::string& _key ) const;

    /// Calculates the mappings for categorical columns.
    std::pair<MappingForDf, Colnames> fit_on_categoricals(
        const containers::DataFrame& _data_frame ) const;

    /// Calculates the mappings for discrete columns.
    std::pair<MappingForDf, Colnames> fit_on_discretes(
        const containers::DataFrame& _data_frame ) const;

    /// Calculates the mappings for text columns.
    std::pair<typename Mapping::TextMapping, typename Mapping::Colnames>
    fit_on_text( const containers::DataFrame& _data_frame ) const;

    /// Parses a JSON object.
    Mapping from_json_obj( const Poco::JSON::Object& _obj ) const;

    /// Generates the mapping columns.
    std::vector<containers::Column<Float>> make_mapping_columns_int(
        const std::pair<containers::Column<Int>, MappingForDf::value_type>& _p )
        const;

    /// Generates the mapping columns for text fields.
    std::vector<containers::Column<Float>> make_mapping_columns_text(
        const std::pair<
            containers::Column<strings::String>,
            TextMapping::value_type>& _p ) const;

    /// Generates the rownum map for the text columns.
    std::map<strings::String, std::vector<size_t>> make_rownum_map_text(
        const helpers::Column<strings::String>& _col ) const;

    /// Transforms the categorical columns in the DataFrame.
    std::vector<containers::Column<Float>> transform_categorical(
        const containers::DataFrame& _df ) const;

    /// Transforms the discrete columns in the DataFrame.
    std::vector<containers::Column<Float>> transform_discrete(
        const containers::DataFrame& _df ) const;

    /// Transforms the text columns in the DataFrame.
    std::vector<containers::Column<Float>> transform_text(
        const containers::DataFrame& _df ) const;

    /// Transforms the text mapping into a JSON Array.
    Poco::JSON::Array::Ptr transform_text_mapping(
        const TextMapping& _mapping ) const;

   private:
    /// Calculates the aggregated targets.
    template <class KeyType>
    std::pair<KeyType, std::vector<Float>> calc_agg_targets(
        const containers::DataFrame& _data_frame,
        const std::pair<KeyType, std::vector<size_t>>& _input ) const
    {
        // -----------------------------------------------------------------------

        const auto& rownums = _input.second;

        // -----------------------------------------------------------------------

        const auto get_target =
            [&_data_frame]( const size_t _i ) -> containers::Column<Float> {
            return _data_frame.target( _i );
        };

        // -----------------------------------------------------------------------

        const auto calc_aggs =
            [this, &rownums]( const containers::Column<Float>& _target_col )
            -> std::vector<Float> {
            const auto get_value = [&_target_col]( const size_t _i ) -> Float {
                return _target_col[_i];
            };

            const auto value_range =
                rownums | std::views::transform( get_value );

            const auto aggregate =
                [value_range](
                    const helpers::MappingAggregation& _agg ) -> Float {
                return helpers::MappingContainerMaker::aggregate(
                    value_range.begin(), value_range.end(), _agg );
            };

            const auto aggregated_range =
                aggregation_enums_ | std::views::transform( aggregate );

            return stl::make::vector<Float>( aggregated_range );
        };

        // -----------------------------------------------------------------------

        const auto iota = std::views::iota(
            static_cast<size_t>( 0 ), _data_frame.num_targets() );

        const auto range = iota | std::views::transform( get_target ) |
                           std::views::transform( calc_aggs );

        const auto values = stl::make::vector<std::vector<Float>>( range );

        const auto second =
            stl::make::vector<Float>( values | std::views::join );

        // -----------------------------------------------------------------------

        return std::make_pair( _input.first, second );

        // -----------------------------------------------------------------------
    }

    /// Transforms a set of columns to SQL.
    template <class MappingToSqlType, class MappingType>
    std::vector<std::string> columns_to_sql(
        const MappingToSqlType& _mapping_to_sql,
        const MappingType& _mappings,
        const Colnames& _colnames ) const
    {
        // -----------------------------------------------------------------------

        assert_true( _colnames );

        assert_true( _mappings.size() == _colnames->size() );

        // -----------------------------------------------------------------------

        const auto all_weights_to_sql =
            [_mapping_to_sql,
             &_mappings]( const size_t _i ) -> std::vector<std::string> {
            const auto& mapping = _mappings.at( _i );

            assert_true( mapping );

            if ( mapping->size() <= 1 )
                {
                    return {};
                }

            const auto num_weights = mapping->begin()->second.size();

            const auto to_sql =
                std::bind( _mapping_to_sql, _i, std::placeholders::_1 );

            const auto iota =
                std::views::iota( static_cast<size_t>( 0 ), num_weights );

            return stl::make::vector<std::string>(
                iota | std::views::transform( to_sql ) );
        };

        // -----------------------------------------------------------------------

        const auto iota =
            std::views::iota( static_cast<size_t>( 0 ), _mappings.size() );

        const auto all = stl::make::vector<std::vector<std::string>>(
            iota | std::views::transform( all_weights_to_sql ) );

        return stl::make::vector<std::string>( all | std::views::join );
    }

    /// Generates the mapping for a rownum map.
    template <class KeyType>
    auto make_mapping(
        const std::map<KeyType, std::vector<size_t>>& _rownum_map,
        const containers::DataFrame& _data_frame ) const
    {
        using RownumPair = std::pair<KeyType, std::vector<size_t>>;

        const auto greater_than_min_freq =
            [this]( const RownumPair& _input ) -> bool {
            return _input.second.size() >= min_freq_;
        };

        const auto calc_agg = [this, &_data_frame]( const RownumPair& _pair )
            -> std::pair<KeyType, std::vector<Float>> {
            return calc_agg_targets( _data_frame, _pair );
        };

        const auto key_to_string =
            []( const std::pair<strings::String, std::vector<Float>>& _pair )
            -> std::pair<std::string, std::vector<Float>> {
            return std::make_pair( _pair.first.str(), _pair.second );
        };

        constexpr bool is_text = std::is_same<KeyType, strings::String>();

        if constexpr ( is_text )
            {
                auto range = _rownum_map |
                             std::views::filter( greater_than_min_freq ) |
                             std::views::transform( calc_agg ) |
                             std::views::transform( key_to_string );

                return std::make_shared<
                    const std::map<std::string, std::vector<Float>>>(
                    range.begin(), range.end() );
            }

        if constexpr ( !is_text )
            {
                auto range = _rownum_map |
                             std::views::filter( greater_than_min_freq ) |
                             std::views::transform( calc_agg );

                return std::make_shared<
                    const std::map<KeyType, std::vector<Float>>>(
                    range.begin(), range.end() );
            }
    }

    /// Generates a rownum map for discrete columns.
    template <class T>
    auto make_rownum_map( const containers::Column<T>& _col ) const
    {
        const auto col = helpers::Column<T>( _col.data_ptr(), _col.name(), "" );

        if constexpr ( std::is_same<T, Int>() )
            {
                return helpers::MappingContainerMaker::
                    make_rownum_map_categorical( col );
            }

        if constexpr ( std::is_same<T, Float>() )
            {
                return helpers::MappingContainerMaker::make_rownum_map_discrete(
                    col );
            }

        if constexpr ( std::is_same<T, strings::String>() )
            {
                return make_rownum_map_text( col );
            }
    }

   private:
    /// The aggregations to use, in string form.
    std::vector<std::string> aggregation_;

    /// The aggregations to use, as enums.
    std::vector<helpers::MappingAggregation> aggregation_enums_;

    /// The vocabulary for the categorical columns.
    MappingForDf categorical_;

    /// The names of the categorical columns.
    Colnames categorical_names_;

    /// The dependencies inserted into the the preprocessor.
    std::vector<Poco::JSON::Object::Ptr> dependencies_;

    /// The vocabulary for the discrete columns.
    MappingForDf discrete_;

    /// The names of the discrete columns.
    Colnames discrete_names_;

    /// The minimum number of targets required for a category to be included.
    size_t min_freq_;

    /// The vocabulary for the text columns.
    TextMapping text_;

    /// The names of the text columns.
    Colnames text_names_;
};

// ----------------------------------------------------
}  // namespace preprocessors
}  // namespace engine

#endif  // ENGINE_PREPROCESSORS_MAPPING_HPP_

