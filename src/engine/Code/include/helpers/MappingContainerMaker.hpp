#ifndef HELPERS_MAPPINGCONTAINERMAKER_HPP_
#define HELPERS_MAPPINGCONTAINERMAKER_HPP_

namespace helpers
{
// -------------------------------------------------------------------------

class MappingContainerMaker
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
    typedef MappingContainer::Colnames Colnames;
    typedef MappedContainer::MappedColumns MappedColumns;
    typedef MappingContainer::MappingForDf MappingForDf;
    typedef std::pair<Int, std::vector<size_t>> RownumPair;
    typedef std::pair<Int, std::vector<Float>> ValuePair;

   public:
    /// Fits the mapping container.
    static std::shared_ptr<const MappingContainer> fit(
        const std::shared_ptr<const std::vector<std::string>>& _aggregation,
        const std::vector<MappingAggregation>& _aggregation_enums,
        const size_t _min_freq,
        const Placeholder& _placeholder,
        const DataFrame& _population,
        const std::vector<DataFrame>& _peripheral,
        const std::vector<std::string>& _peripheral_names,
        const WordIndexContainer& _word_indices,
        const std::shared_ptr<const logging::AbstractLogger>& _logger );

    /// Infers the number of targets on which the mapping was fitted.
    static size_t infer_num_targets( const MappingForDf& _mapping );

    /// Generates the column name for the mapping.
    static std::string make_colname(
        const std::string& _name,
        const std::string& _feature_postfix,
        const std::vector<std::string>& _aggregation,
        const size_t _weight_num );

    /// Returns a map of all the rownums associated with a categorical value.
    static std::map<Int, std::vector<size_t>> make_rownum_map_categorical(
        const Column<Int>& _col );

    /// Returns a map of all the rownums associated with a discrete value.
    static std::map<Int, std::vector<size_t>> make_rownum_map_discrete(
        const Column<Float>& _col );

    /// Returns the correct enum for the string.
    static MappingAggregation parse_aggregation( const std::string& _str );

    /// Transform categorical columns by mapping them onto
    /// the corresponding weights.
    static std::optional<const MappedContainer> transform(
        const std::shared_ptr<const MappingContainer>& _mapping,
        const Placeholder& _placeholder,
        const DataFrame& _population,
        const std::vector<DataFrame>& _peripheral,
        const std::vector<std::string>& _peripheral_names,
        const std::optional<WordIndexContainer>& _word_indices,
        const std::shared_ptr<const logging::AbstractLogger>& _logger );

   private:
    /// Calculates the aggregated targets.
    static std::pair<Int, std::vector<Float>> calc_agg_targets(
        const std::vector<MappingAggregation>& _aggregation_enums,
        const DataFrame& _data_frame,
        const RownumPair& _input );

    /// Counts the total number of mappable columns.
    static size_t count_mappable_columns( const TableHolder& _table_holder );

    /// Finds the correspondings rownums to the input indices.
    static std::vector<size_t> find_output_ix(
        const std::vector<size_t>& _input_ix,
        const DataFrame& _output_table,
        const DataFrame& _input_table );

    /// Generates the mapping for a categorical column.
    static MappingForDf fit_on_categoricals(
        const std::vector<MappingAggregation>& _aggregation_enums,
        const size_t _min_freq,
        const std::vector<DataFrame>& _main_tables,
        const std::vector<DataFrame>& _peripheral_tables,
        logging::ProgressLogger* _progress_logger );

    /// Generates the mapping for a discrete column.
    static MappingForDf fit_on_discretes(
        const std::vector<MappingAggregation>& _aggregation_enums,
        const size_t _min_freq,
        const std::vector<DataFrame>& _main_tables,
        const std::vector<DataFrame>& _peripheral_tables,
        logging::ProgressLogger* _progress_logger );

    /// Generates the mapping for a text column.
    static MappingForDf fit_on_text(
        const std::vector<MappingAggregation>& _aggregation_enums,
        const size_t _min_freq,
        const std::vector<DataFrame>& _main_tables,
        const std::vector<DataFrame>& _peripheral_tables,
        logging::ProgressLogger* _progress_logger );

    /// Fits a new mapping on the table holder.
    static std::shared_ptr<const MappingContainer> fit_on_table_holder(
        const std::shared_ptr<const std::vector<std::string>>& _aggregation,
        const std::vector<MappingAggregation>& _aggregation_enums,
        const size_t _min_freq,
        const TableHolder& _table_holder,
        const std::vector<DataFrame>& _main_tables,
        const std::vector<DataFrame>& _peripheral_tables,
        logging::ProgressLogger* _progress_logger );

    /// Generates a new mapping based on the rownum_map for a particular column.
    static std::shared_ptr<const std::map<Int, std::vector<Float>>>
    make_mapping(
        const std::vector<MappingAggregation>& _aggregation_enums,
        const size_t _min_freq,
        const std::map<Int, std::vector<size_t>>& _rownum_map,
        const std::vector<DataFrame>& _main_tables,
        const std::vector<DataFrame>& _peripheral_tables );

    /// Generates a lambda function to match the rownums.
    static std::function<RownumPair( const RownumPair& )> make_match_rownums(
        const std::vector<DataFrame>& _main_tables,
        const std::vector<DataFrame>& _peripheral_tables );

    /// Returns a map of all the rownums associated with a word.
    static std::map<Int, std::vector<size_t>> make_rownum_map_text(
        const textmining::WordIndex& _word_index );

    /// Maps the categories on their corresponding weights.
    static MappedColumns transform_categorical(
        const MappingForDf& _mapping,
        const std::vector<Column<Int>>& _categorical,
        const std::string& _feature_postfix,
        const std::vector<std::string>& _aggregation,
        logging::ProgressLogger* _progress_logger );

    /// Applies the mapping to a categorical column.
    static Column<Float> transform_categorical_column(
        const MappingForDf& _mapping,
        const std::vector<Column<Int>>& _categorical,
        const std::string& _feature_postfix,
        const std::vector<std::string>& _aggregation,
        const size_t _colnum,
        const size_t _weight_num );

    /// Maps the discrete values on their corresponding weights.
    static MappedColumns transform_discrete(
        const MappingForDf& _mapping,
        const std::vector<Column<Float>>& _discrete,
        const std::string& _feature_postfix,
        const std::vector<std::string>& _aggregation,
        logging::ProgressLogger* _progress_logger );

    /// Applies the mapping to a discrete column.
    static Column<Float> transform_discrete_column(
        const MappingForDf& _mapping,
        const std::vector<Column<Float>>& _discrete,
        const std::string& _feature_postfix,
        const std::vector<std::string>& _aggregation,
        const size_t _colnum,
        const size_t _weight_num );

    /// Transforms a table holder to get the extra columns.
    static std::shared_ptr<const MappedContainer> transform_table_holder(
        const std::shared_ptr<const MappingContainer>& _mapping,
        const TableHolder& _table_holder,
        const std::string& _feature_postfix,
        logging::ProgressLogger* _progress_logger );

    /// Maps the text fields on their corresponding weights.
    static MappedColumns transform_text(
        const MappingForDf& _mapping,
        const std::vector<Column<strings::String>>& _text,
        const typename DataFrame::WordIndices& _word_indices,
        const std::string& _feature_postfix,
        const std::vector<std::string>& _aggregation,
        logging::ProgressLogger* _progress_logger );

    /// Applies the mapping to a text column.
    static Column<Float> transform_text_column(
        const MappingForDf& _mapping,
        const std::vector<Column<strings::String>>& _text,
        const typename DataFrame::WordIndices& _word_indices,
        const std::string& _feature_postfix,
        const std::vector<std::string>& _aggregation,
        const size_t _colnum,
        const size_t _weight_num );

   public:
    /// Aggregates the range.
    template <class IteratorType>
    static Float aggregate(
        const IteratorType _begin,
        const IteratorType _end,
        const MappingAggregation _aggregation )
    {
        switch ( _aggregation )
            {
                case MappingAggregation::avg:
                    return Aggregations::avg( _begin, _end );

                case MappingAggregation::count:
                    return Aggregations::count( _begin, _end );

                case MappingAggregation::count_above_mean:
                    return Aggregations::count_above_mean( _begin, _end );

                case MappingAggregation::count_below_mean:
                    return Aggregations::count_below_mean( _begin, _end );

                case MappingAggregation::count_distinct:
                    return Aggregations::count_distinct( _begin, _end );

                case MappingAggregation::count_distinct_over_count:
                    return Aggregations::count_distinct_over_count(
                        _begin, _end );

                case MappingAggregation::count_minus_count_distinct:
                    return Aggregations::count( _begin, _end ) -
                           Aggregations::count_distinct( _begin, _end );

                case MappingAggregation::kurtosis:
                    return Aggregations::kurtosis( _begin, _end );

                case MappingAggregation::max:
                    return Aggregations::maximum( _begin, _end );

                case MappingAggregation::median:
                    return Aggregations::median( _begin, _end );

                case MappingAggregation::min:
                    return Aggregations::minimum( _begin, _end );

                case MappingAggregation::mode:
                    return Aggregations::mode<Float>( _begin, _end );

                case MappingAggregation::num_max:
                    return Aggregations::num_max( _begin, _end );

                case MappingAggregation::num_min:
                    return Aggregations::num_min( _begin, _end );

                case MappingAggregation::q1:
                    return Aggregations::quantile( 0.01, _begin, _end );

                case MappingAggregation::q5:
                    return Aggregations::quantile( 0.05, _begin, _end );

                case MappingAggregation::q10:
                    return Aggregations::quantile( 0.1, _begin, _end );

                case MappingAggregation::q25:
                    return Aggregations::quantile( 0.25, _begin, _end );

                case MappingAggregation::q75:
                    return Aggregations::quantile( 0.75, _begin, _end );

                case MappingAggregation::q90:
                    return Aggregations::quantile( 0.90, _begin, _end );

                case MappingAggregation::q95:
                    return Aggregations::quantile( 0.95, _begin, _end );

                case MappingAggregation::q99:
                    return Aggregations::quantile( 0.99, _begin, _end );

                case MappingAggregation::skew:
                    return Aggregations::skew( _begin, _end );

                case MappingAggregation::stddev:
                    return Aggregations::stddev( _begin, _end );

                case MappingAggregation::sum:
                    return Aggregations::sum( _begin, _end );

                case MappingAggregation::var:
                    return Aggregations::var( _begin, _end );

                case MappingAggregation::variation_coefficient:
                    return Aggregations::variation_coefficient( _begin, _end );

                default:
                    assert_true( false && "Unknown aggregation" );
                    return 0.0;
            }
    }

    /// Infers the aggregation and the target number from the weight num.
    static std::pair<std::string, size_t> infer_aggregation_target_num(
        const std::vector<std::string>& _aggregation, const size_t _weight_num )
    {
        const auto agg_num = _weight_num % _aggregation.size();
        const auto target_num = _weight_num / _aggregation.size();
        return std::make_pair( _aggregation.at( agg_num ), target_num );
    }

    /// Generates a function that applies a mapping to column.
    template <class MapToWeightFunc>
    static auto make_transform_col(
        const MapToWeightFunc _map_to_weight, const MappingForDf& _mapping )
    {
        return [_map_to_weight, _mapping](
                   const size_t _colnum ) -> std::vector<Column<Float>> {
            assert_true( _mapping.at( _colnum ) );

            if ( _mapping.at( _colnum )->size() <= 1 )
                {
                    return {};
                }

            const auto num_weights =
                _mapping.at( _colnum )->begin()->second.size();

            const auto iota =
                std::views::iota( static_cast<size_t>( 0 ), num_weights );

            const auto map =
                std::bind( _map_to_weight, _colnum, std::placeholders::_1 );

            return stl::make::vector<Column<Float>>(
                iota | std::views::transform( map ) );
        };
    }

   private:
    /// Extracts the colnames of a list of columns.
    template <class T>
    static Colnames extract_colnames( const std::vector<Column<T>>& _columns )
    {
        const auto get_name = []( const Column<T>& _col ) {
            return _col.name_;
        };
        const auto range = _columns | std::ranges::views::transform( get_name );
        return std::make_shared<std::vector<std::string>>(
            stl::make::vector<std::string>( range ) );
    }
};

// -------------------------------------------------------------------------
}  // namespace helpers

#endif  // HELPERS_MAPPINGCONTAINERMAKER_HPP_
