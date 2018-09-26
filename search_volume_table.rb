# frozen_string_literal: true

module Widgets
  module Keywords
    class SearchVolumeTable < Table
      class << self
        def options(_context, _opts)
          { grouped_filter: true, no_description: true }
        end

        def dashboard_widget_filters
          [
            %w[date_range],
            %w[granularity search_engine grouped],
            %w[buckets only_conversion_event_ids keyword_tag_ids],
            %w[reduced_date_range],
            %w[export_all_rows],
            %w[site_of_competitors]
          ]
        end

        def dashboard_widget_sorting_columns
          %w[keyword_name group search_volume]
        end

        def required_param_names
          %w[site search_engine from to]
        end

        def permitted_param_names
          required_param_names + %w[limit offset sort_col sort_dir grouped granularity search_string keyword_tag_ids keyword_tag_ids_logics only_conversion_event_ids location_ids]
        end

        def data(opts)
          country_code = opts[:site].country_info&.iso_alpha2&.downcase

          date_range = (opts[:from]..opts[:to]).map { |d| d.beginning_of_month }.uniq
          opts[:dates] = date_range
          search_volume_data = []
          rows = []
          new_opts = opts.merge(site_id: (opts[:site].source_site || opts[:site]).id, search_engine_id: opts[:search_engine].id)
          date_range.each do |date|
            if opts[:grouped].present?
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
                ORDER BY \"#{opts[:sort_col]}\" #{opts[:sort_dir]} NULLS LAST
                #{opts[:limit].to_i.positive? ? 'LIMIT :limit OFFSET :offset' : ''}"

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
                ORDER BY \"#{sort_col}\" #{opts[:sort_dir]} NULLS LAST
                #{opts[:limit].to_i.positive? ? 'LIMIT :limit OFFSET :offset' : ''}"
            end

            sql = query.match?(/:\w+/) ? ::Core::Models::ReadOnly.send(:sanitize_sql_array, [query, new_opts]) : query

            rows = ::Core::Models::ReadOnly.connection.select_all(sql)
            rows.each do |r|
              r.symbolize_keys!
              search_volume_data << { search_volume: (search_volume = r[:search_volume].to_i).zero? ? '-' : search_volume }
            end
          end

          total_count = 0

          page_ids = []

          rows.each_with_index do |r, index|
            r.symbolize_keys!

            if opts[:grouped].present?
              r[:group] = 'Ungrouped' if r[:group].nil?
              r[:group_id] = r[:group_id].to_i
              r[:keywords] = r[:keywords].to_i
            end

            r[:search_volume] = (search_volume = r[:search_volume].to_i).zero? ? '-' : search_volume
            search_vol_arr = search_volume_data.each_slice(rows.size).to_a
            search_array = []
            date_range.each_with_index { |_date, i| search_array << search_vol_arr[i][index] }
            r[:search_volume_data] = search_array
            total_count = r[:total_count].to_i unless total_count.positive?
            r.delete(:total_count)
          end

          append_pages(page_ids.compact.uniq, [opts[:site].id], rows)

          format_response(rows, total_count, opts).tap do |response|
            response[:dates] = date_range.map { |date| [date.to_s, date.to_s] }
            response[:dates_count] = date_range.count
          end
        end
      end
    end
  end
end
