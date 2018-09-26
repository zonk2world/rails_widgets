# frozen_string_literal: true

module Widgets
  module Keywords
    class SearchVolume < Chart
      class << self
        def options(context, opts)
          chart_options(context, opts).deep_merge(
            legend: legend_options(context, opts),
            plotOptions: {
              column: column_options(context, opts)
            },
            tooltip: {
              formatter: tooltip_formatter(total: 'Total Search Volume')
            },
            xAxis: x_axis_options(context, opts),
            yAxis: y_axis_options(context, opts).deep_merge(
              labels: {
                formatter: Chart::JsFunction.init(opts, 'function() { return this.isFirst ? "0" : this.value; }')
              },
              min: 0,
              title: {
                text: I18n.t('charts.search_volume.axis')
              }
            )
          )
        end

        def dashboard_widget_filters
          [
            %w[date_range],
            %w[only_conversion_event_ids keyword_tag_ids],
            %w[extended_date_range]
          ]
        end

        def required_param_names
          %w[site from to]
        end

        def permitted_param_names
          required_param_names + %w[keyword_tag_ids keyword_tag_ids_logics only_conversion_event_ids search_string keyword_id report_id annotation_kind_ids location_ids competitor_tag_ids]
        end

        def data(opts)
          country_code = opts[:site].country_info&.iso_alpha2&.downcase

          date_range = (opts[:from]..opts[:to]).map { |d| d.beginning_of_month }.uniq
          opts[:dates] = date_range
          search_volume_data = []
          new_opts = opts.merge(site_id: (opts[:site].source_site || opts[:site]).id)
          date_range.each do |date|
            if opts[:keyword_tag_ids].present?
              opts[:sort_col] = 'group' unless %w[group keywords search_volume].include?(opts[:sort_col])
              opts[:sort_dir] = 'asc' unless opts[:sort_dir] == 'desc'

              query = "
                SELECT MIN(G.tag_name) AS group,
                    G.tag_id AS group_id,
                    MAX(G.keywords) AS keywords,
                    MAX(G.search_volume) AS search_volume,
                    COUNT(*) OVER() AS total_count
                FROM (SELECT
                        MIN(T.tag_name) AS tag_name,
                        T.tag_id,
                        SUM(ADV.search_volume) AS search_volume,
                        COUNT(DISTINCT(T.keyword_id::text || ':' || T.location_id::text)) AS keywords
                    FROM (#{CoreExt::Accessors::KeywordTag.table_query(new_opts)}) T
                        LEFT JOIN (#{AdwordsExt::Accessors::KeywordStatistics.join_table_query(new_opts.merge(date: date, country_code: country_code))}) ADV
                            ON ADV.keyword_id = T.keyword_id
                    GROUP BY T.tag_id) G
                GROUP BY G.tag_id
                ORDER BY \"#{opts[:sort_col]}\" #{opts[:sort_dir]} NULLS LAST"

            else
              opts[:sort_col] = 'keyword_name' unless %w[keyword_name location_name search_volume].include?(opts[:sort_col])
              sort_col = opts[:sort_col]
              opts[:sort_dir] = 'asc' unless opts[:sort_dir] == 'desc'

              query = "
                SELECT
                    MIN(G.keyword_name) AS keyword_name,
                    MIN(G.mysql_keyword_id) AS mysql_keyword_id,
                    G.keyword_id,
                    G.location_id,
                    MIN(G.location_name) AS location_name,
                    MAX(G.search_volume) AS search_volume,
                    COUNT(*) OVER() AS total_count
                FROM (SELECT
                        K.keyword_name,
                        K.mysql_keyword_id,
                        K.keyword_id,
                        K.location_id,
                        GN.location_name,
                        ADV.search_volume
                    FROM (#{CoreExt::Accessors::Keyword.table_query(new_opts)}) K
                        LEFT JOIN (#{GeonamesExt::Accessors::Name.join_table_query(new_opts)}) GN USING (location_id)
                        LEFT JOIN (#{AdwordsExt::Accessors::KeywordStatistics.join_table_query(new_opts.merge(date: date, country_code: country_code))}) ADV
                            ON ADV.keyword_id = K.keyword_id) G
                GROUP BY G.keyword_id, G.location_id
                ORDER BY \"#{sort_col}\" #{opts[:sort_dir]} NULLS LAST"
            end

            sql = query.match?(/:\w+/) ? ::Core::Models::ReadOnly.send(:sanitize_sql_array, [query, new_opts]) : query

            rows = ::Core::Models::ReadOnly.connection.select_all(sql)
            search_volume_data << rows.inject(0) do |sum, r|
              r.symbolize_keys!
              sum + r[:search_volume].to_i
            end
          end

          data = {
            categories: date_range.map { |date| [date, date] },
            series: [{
              type: 'column',
              name: 'Keywords Search Volume',
              data: search_volume_data,
              color: 'green'
            }]
          }
          data
        end
      end
    end
  end
end
