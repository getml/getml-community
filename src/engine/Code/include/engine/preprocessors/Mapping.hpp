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
    static constexpr const char* AVG = "AVG";
    static constexpr const char* AVG_TIME_BETWEEN = "AVG TIME BETWEEN";
    static constexpr const char* COUNT = "COUNT";
    static constexpr const char* COUNT_ABOVE_MEAN = "COUNT ABOVE MEAN";
    static constexpr const char* COUNT_BELOW_MEAN = "COUNT BELOW MEAN";
    static constexpr const char* COUNT_DISTINCT = "COUNT DISTINCT";
    static constexpr const char* COUNT_DISTINCT_OVER_COUNT =
        "COUNT DISTINCT OVER COUNT";
    static constexpr const char* COUNT_MINUS_COUNT_DISTINCT =
        "COUNT MINUS COUNT DISTINCT";
    static constexpr const char* KURTOSIS = "KURTOSIS";
    static constexpr const char* MAX = "MAX";
    static constexpr const char* MEDIAN = "MEDIAN";
    static constexpr const char* MIN = "MIN";
    static constexpr const char* MODE = "MODE";
    static constexpr const char* NUM_MAX = "NUM MAX";
    static constexpr const char* NUM_MIN = "NUM MIN";
    static constexpr const char* Q1 = "Q1";
    static constexpr const char* Q5 = "Q5";
    static constexpr const char* Q10 = "Q10";
    static constexpr const char* Q25 = "Q25";
    static constexpr const char* Q75 = "Q75";
    static constexpr const char* Q90 = "Q90";
    static constexpr const char* Q95 = "Q95";
    static constexpr const char* Q99 = "Q99";
    static constexpr const char* SKEW = "SKEW";
    static constexpr const char* SUM = "SUM";
    static constexpr const char* STDDEV = "STDDEV";
    static constexpr const char* VAR = "VAR";
    static constexpr const char* VARIATION_COEFFICIENT =
        "VARIATION COEFFICIENT";

   public:
    typedef std::shared_ptr<const std::vector<std::string>> Colnames;
    typedef std::vector<
        std::shared_ptr<const std::map<Int, std::vector<Float>>>>
        MappingForDf;
    typedef typename MappingForDf::value_type PtrType;
    typedef typename PtrType::element_type Map;
    typedef std::pair<Int, std::vector<size_t>> RownumPair;
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
        const std::vector<containers::DataFrame>& _peripheral_dfs,
        const helpers::Placeholder& _placeholder,
        const std::vector<std::string>& _peripheral_names ) final;

    /// Expresses the Seasonal preprocessor as a JSON object.
    Poco::JSON::Object::Ptr to_json_obj() const final;

    /// Transforms the data frames by adding the desired time series
    /// transformations.
    std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
    transform(
        const Poco::JSON::Object& _cmd,
        const std::shared_ptr<const containers::Encoding> _categories,
        const containers::DataFrame& _population_df,
        const std::vector<containers::DataFrame>& _peripheral_dfs,
        const helpers::Placeholder& _placeholder,
        const std::vector<std::string>& _peripheral_names ) const final;

    /// Generates the mapping tables.
    std::vector<std::string> to_sql(
        const std::shared_ptr<const std::vector<strings::String>>& _categories )
        const final;

   public:
    /// Trivial (const) accessor.
    const std::vector<std::string>& aggregation() const { return aggregation_; }

    /// Trivial (const) accessor.
    const std::vector<MappingAggregation>& aggregation_enums() const
    {
        return aggregation_enums_;
    }

    /// Creates a deep copy.
    std::shared_ptr<Preprocessor> clone(
        const std::optional<std::vector<Poco::JSON::Object::Ptr>>&
            _dependencies = std::nullopt ) const final
    {
        const auto c = std::make_shared<Mapping>( *this );
        if ( _dependencies )
            {
                c->dependencies_ = *_dependencies;
            }
        return c;
    }

    /// Returns the type of the preprocessor.
    std::string type() const final { return Preprocessor::MAPPING; }

    /// Trivial (const) accessor.
    size_t min_freq() const { return min_freq_; }

   private:
    /// Aggregates the range.
    template <class IteratorType>
    Float aggregate(
        const IteratorType _begin,
        const IteratorType _end,
        const MappingAggregation _aggregation ) const
    {
        switch ( _aggregation )
            {
                case MappingAggregation::avg:
                    return helpers::Aggregations::avg( _begin, _end );

                case MappingAggregation::count:
                    return helpers::Aggregations::count( _begin, _end );

                case MappingAggregation::count_above_mean:
                    return helpers::Aggregations::count_above_mean(
                        _begin, _end );

                case MappingAggregation::count_below_mean:
                    return helpers::Aggregations::count_below_mean(
                        _begin, _end );

                case MappingAggregation::count_distinct:
                    return helpers::Aggregations::count_distinct(
                        _begin, _end );

                case MappingAggregation::count_distinct_over_count:
                    return helpers::Aggregations::count_distinct_over_count(
                        _begin, _end );

                case MappingAggregation::count_minus_count_distinct:
                    return helpers::Aggregations::count( _begin, _end ) -
                           helpers::Aggregations::count_distinct(
                               _begin, _end );

                case MappingAggregation::kurtosis:
                    return helpers::Aggregations::kurtosis( _begin, _end );

                case MappingAggregation::max:
                    return helpers::Aggregations::maximum( _begin, _end );

                case MappingAggregation::median:
                    return helpers::Aggregations::median( _begin, _end );

                case MappingAggregation::min:
                    return helpers::Aggregations::minimum( _begin, _end );

                case MappingAggregation::mode:
                    return helpers::Aggregations::mode<Float>( _begin, _end );

                case MappingAggregation::num_max:
                    return helpers::Aggregations::num_max( _begin, _end );

                case MappingAggregation::num_min:
                    return helpers::Aggregations::num_min( _begin, _end );

                case MappingAggregation::q1:
                    return helpers::Aggregations::quantile(
                        0.01, _begin, _end );

                case MappingAggregation::q5:
                    return helpers::Aggregations::quantile(
                        0.05, _begin, _end );

                case MappingAggregation::q10:
                    return helpers::Aggregations::quantile( 0.1, _begin, _end );

                case MappingAggregation::q25:
                    return helpers::Aggregations::quantile(
                        0.25, _begin, _end );

                case MappingAggregation::q75:
                    return helpers::Aggregations::quantile(
                        0.75, _begin, _end );

                case MappingAggregation::q90:
                    return helpers::Aggregations::quantile(
                        0.90, _begin, _end );

                case MappingAggregation::q95:
                    return helpers::Aggregations::quantile(
                        0.95, _begin, _end );

                case MappingAggregation::q99:
                    return helpers::Aggregations::quantile(
                        0.99, _begin, _end );

                case MappingAggregation::skew:
                    return helpers::Aggregations::skew( _begin, _end );

                case MappingAggregation::stddev:
                    return helpers::Aggregations::stddev( _begin, _end );

                case MappingAggregation::sum:
                    return helpers::Aggregations::sum( _begin, _end );

                case MappingAggregation::var:
                    return helpers::Aggregations::var( _begin, _end );

                case MappingAggregation::variation_coefficient:
                    return helpers::Aggregations::variation_coefficient(
                        _begin, _end );

                default:
                    assert_true( false && "Unknown aggregation" );
                    return 0.0;
            }
    }

    /// Builds some of the objects required for fitting or transforming the
    /// mapping.
    std::tuple<
        helpers::DataFrame,
        std::optional<helpers::TableHolder>,
        std::shared_ptr<const helpers::VocabularyContainer>>
    build_prerequisites(
        const containers::DataFrame& _population_df,
        const std::vector<containers::DataFrame>& _peripheral_dfs,
        const helpers::Placeholder& _placeholder,
        const std::vector<std::string>& _peripheral_names,
        const bool _targets ) const;

    /// Calculates the aggregated targets.
    std::pair<Int, std::vector<Float>> calc_agg_targets(
        const helpers::DataFrame& _population,
        const std::pair<Int, std::vector<size_t>>& _input ) const;

    /// Transforms the mappings for the categorical columns to SQL.
    std::vector<std::string> categorical_columns_to_sql(
        const std::shared_ptr<const std::vector<strings::String>>& _categories )
        const;

    /// Transforms a mapping for a categorical or text column to SQL.
    std::string categorical_or_text_column_to_sql(
        const std::shared_ptr<const std::vector<strings::String>>& _categories,
        const std::string& _name,
        const PtrType& _ptr,
        const size_t _weight_num,
        const bool _is_text ) const;

    /// Transforms the mappings for a discrete columns to SQL.
    std::string discrete_column_to_sql(
        const std::string& _name,
        const PtrType& _ptr,
        const size_t _weight_num ) const;

    /// Transforms the mappings for the discrete columns to SQL.
    std::vector<std::string> discrete_columns_to_sql() const;

    /// Transforms the mappings for the text columns to SQL.
    std::vector<std::string> text_columns_to_sql() const;

    /// Extracts a mapping from JSON.
    MappingForDf extract_mapping(
        const Poco::JSON::Object& _obj, const std::string& _key ) const;

    /// Extracts the schemata from the peripheral and population tables.
    std::pair<
        std::shared_ptr<const containers::Schema>,
        std::shared_ptr<const std::vector<containers::Schema>>>
    extract_schemata(
        const containers::DataFrame& _population_df,
        const std::vector<containers::DataFrame>& _peripheral_dfs ) const;

    /// Extracts the text mapping.
    typename Mapping::TextMapping extract_text_mapping(
        const Poco::JSON::Object& _obj, const std::string& _key ) const;

    /// Identifies the correct indicies in the output table.
    std::vector<size_t> find_output_ix(
        const std::vector<size_t>& _input_ix,
        const helpers::DataFrame& _output_table,
        const helpers::DataFrame& _input_table ) const;

    /// Calculates the mappings for categorical columns.
    std::pair<MappingForDf, Colnames> fit_on_categoricals(
        const helpers::DataFrame& _population,
        const std::vector<helpers::DataFrame>& _main_tables,
        const std::vector<helpers::DataFrame>& _peripheral_tables ) const;

    /// Calculates the mappings for discrete columns.
    std::pair<MappingForDf, Colnames> fit_on_discretes(
        const helpers::DataFrame& _population,
        const std::vector<helpers::DataFrame>& _main_tables,
        const std::vector<helpers::DataFrame>& _peripheral_tables ) const;

    /// Fits a new submapping on table holder.
    Mapping fit_on_table_holder(
        const helpers::DataFrame& _population,
        const helpers::TableHolder& _table_holder,
        const std::vector<helpers::DataFrame>& _main_tables,
        const std::vector<helpers::DataFrame>& _peripheral_tables,
        const size_t _ix ) const;

    /// Calculates the mappings for text columns.
    std::pair<MappingForDf, Colnames> fit_on_text(
        const helpers::DataFrame& _population,
        const std::vector<helpers::DataFrame>& _main_tables,
        const std::vector<helpers::DataFrame>& _peripheral_tables ) const;

    /// Fits the submappings on the subtables of a table holder.
    std::vector<Mapping> fit_submappings(
        const helpers::DataFrame& _population,
        const std::optional<helpers::TableHolder>& _table_holder,
        const std::vector<helpers::DataFrame>& _main_tables,
        const std::vector<helpers::DataFrame>& _peripheral_tables ) const;

    /// Parses a JSON object.
    Mapping from_json_obj( const Poco::JSON::Object& _obj ) const;

    /// Creates indices on the text fields.
    std::pair<
        std::shared_ptr<const helpers::VocabularyContainer>,
        helpers::WordIndexContainer>
    handle_text_fields(
        const helpers::DataFrame& _population,
        const std::vector<helpers::DataFrame>& _peripheral ) const;

    /// Generates the name of the mapping column.
    std::string make_colname(
        const std::string& _name, const size_t _weight_num ) const;

    /// Generates the mapping for a rownum map.
    std::shared_ptr<const std::map<Int, std::vector<Float>>> make_mapping(
        const std::map<Int, std::vector<size_t>>& _rownum_map,
        const helpers::DataFrame& _population,
        const std::vector<helpers::DataFrame>& _main_tables,
        const std::vector<helpers::DataFrame>& _peripheral_tables ) const;

    /// Generates the mapping columns.
    std::vector<containers::Column<Float>> make_mapping_columns_int(
        const std::pair<containers::Column<Int>, MappingForDf::value_type>& _p )
        const;

    /// Generates the mapping columns for text fields.
    std::vector<containers::Column<Float>> make_mapping_columns_text(
        const std::tuple<
            std::string,
            std::shared_ptr<const textmining::WordIndex>,
            MappingForDf::value_type>& _t ) const;

    /// Generates the pairs needed generate the create table statement.
    std::vector<std::pair<Int, Float>> make_pairs(
        const Map& _m, const size_t _weight_num ) const;

    /// Generates the rownum map for the categorical columns.
    std::map<Int, std::vector<size_t>> make_rownum_map_categorical(
        const helpers::Column<Int>& _col ) const;

    /// Generates the rownum map for the discrete columns.
    std::map<Int, std::vector<size_t>> make_rownum_map_discrete(
        const helpers::Column<Float>& _col ) const;

    /// Generates the rownum map for the text columns.
    std::map<Int, std::vector<size_t>> make_rownum_map_text(
        const textmining::WordIndex& _word_index ) const;

    /// Generates the table header for the resulting SQL code.
    std::string make_table_header(
        const std::string& _name, const bool _key_is_num ) const;

    /// Identifies the correct rownums to use by parsing through the main and
    /// peripheral tables.
    RownumPair match_rownums(
        const std::vector<helpers::DataFrame>& _main_tables,
        const std::vector<helpers::DataFrame>& _peripheral_tables,
        const RownumPair& _input ) const;

    /// Parses the aggregation string to a proper enum.
    MappingAggregation parse_aggregation( const std::string& _str ) const;

    /// Transforms the categorical columns in the DataFrame.
    std::vector<containers::Column<Float>> transform_categorical(
        const containers::DataFrame& _df ) const;

    /// Adds the columns produced by this mapping to the data frame.
    void transform_data_frame(
        const helpers::DataFrame& _immutable,
        containers::DataFrame* _data_frame ) const;

    /// Transforms the discrete columns in the DataFrame.
    std::vector<containers::Column<Float>> transform_discrete(
        const containers::DataFrame& _df ) const;

    /// Transforms the peripheral data frames.
    void transform_peripherals(
        const helpers::TableHolder& _table_holder,
        std::vector<containers::DataFrame>* _peripheral_dfs ) const;

    /// Transform the mapping into a Poco array.
    Poco::JSON::Array::Ptr transform_mapping(
        const MappingForDf& _mapping ) const;

    /// Transforms the text columns in the DataFrame.
    std::vector<containers::Column<Float>> transform_text(
        const helpers::DataFrame& _immutable,
        const containers::DataFrame& _df ) const;

    /// Transforms the text mapping into a JSON Array.
    Poco::JSON::Array::Ptr transform_text_mapping(
        const TextMapping& _mapping ) const;

   private:
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

            const auto iota = stl::iota<size_t>( 0, num_weights );

            return stl::collect::vector<std::string>(
                iota | std::views::transform( to_sql ) );
        };

        // -----------------------------------------------------------------------

        const auto iota = stl::iota<size_t>( 0, _mappings.size() );

        const auto all = stl::collect::vector<std::vector<std::string>>(
            iota | std::views::transform( all_weights_to_sql ) );

        return stl::join( all );
    }

   private:
    /// The aggregations to use, in string form.
    std::vector<std::string> aggregation_;

    /// The aggregations to use, as enums.
    std::vector<MappingAggregation> aggregation_enums_;

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

    /// The schema of the peripheral data frames
    std::shared_ptr<const std::vector<containers::Schema>> peripheral_schema_;

    /// The schema of the population data frame
    std::shared_ptr<const containers::Schema> population_schema_;

    /// The prefix to insert into the generated mapping.
    std::string prefix_;

    /// Any relational mappings that might exist.
    std::vector<Mapping> submappings_;

    /// The name of the table for which the mappings are created
    std::string table_name_;

    /// The vocabulary for the text columns.
    MappingForDf text_;

    /// The names of the text columns.
    Colnames text_names_;

    /// The vocabulary used for the text fields.
    std::shared_ptr<const helpers::VocabularyContainer> vocabulary_;
};

// ----------------------------------------------------
}  // namespace preprocessors
}  // namespace engine

#endif  // ENGINE_PREPROCESSORS_MAPPING_HPP_
