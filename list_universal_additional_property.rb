# frozen_string_literal: true

module Widgets
  module Keywords
    class ListUniversalAdditionalProperty < Table
      class << self
        def options(_context, _opts)
          { no_description: true, ranked_filter: true }
        end

        def dashboard_widget_filters
          [
            %w[date_range],
            %w[search_engine],
            %w[only_conversion_event_ids keyword_tag_ids],
            %w[ranked],
            %w[limit],
            %w[serp_property_ids]
          ]
        end

        def dashboard_widget_sorting_columns
          %w[keyword_name rank]
        end

        def required_param_names
          %w[site search_engine from to]
        end

        def permitted_param_names
          required_param_names + %w[limit offset sort_col sort_dir search_string keyword_tag_ids keyword_tag_ids_logics only_conversion_event_ids location_ids ranked serp_property_ids]
        end

        def data(opts)
          from  = Ranking::Accessors::Rankings.last_ranking_date(site_id: opts[:site].id, search_engine_id: opts[:search_engine].id, date: opts[:from])
          to    = Ranking::Accessors::Rankings.last_ranking_date(site_id: opts[:site].id, search_engine_id: opts[:search_engine].id, date: opts[:to])
          new_opts = opts.merge(site_id: opts[:site].id, search_engine_id: opts[:search_engine].id)
          if opts[:site].combined_settings[:enable_serp_properties] && opts[:site].global_key != 'a141db604b19'
            new_opts[:serp_properties_date] = to
          end

          conditions = ['_PSAP.site_id = :site_id', '_PSAP.search_engine_id = :search_engine_id', '_PSAP.date = :to']
          join_type = 'LEFT JOIN'
          subconditions = []
          if new_opts[:ranked].present?
            subconditions = ['_PSAP.site_ranked_in_answerbox = true', '_PSAP.site_ranked_in_knowledge_panel = true', '_PSAP.site_ranked_in_actions = true', '_PSAP.site_ranked_in_reviews = true']
            join_type = 'JOIN'
          end

          query = "
              SELECT
                  K.keyword_name,
                  K.mysql_keyword_id,
                  K.keyword_id,
                  K.location_id,
                  GN.location_name,
                  _PSAP.answerbox_website,
                  _PSAP.knowledge_panel_website,
                  _PSAP.knowledge_panel_actions,
                  _PSAP.knowledge_panel_reviews,
                  R.rank,
                  R.search_engine_id,
                 COUNT(*) OVER() AS total_count
              FROM (#{CoreExt::Accessors::Keyword.table_query(new_opts)}) K
              LEFT JOIN (#{GeonamesExt::Accessors::Name.join_table_query(new_opts)}) GN USING (location_id)
              LEFT JOIN (#{Ranking::Accessors::Rankings.join_table_query(new_opts.merge(date: to))}) R USING (keyword_id, location_id)
              #{join_type} (
                          SELECT _PSAP.*
                          FROM #{Ranking::Models::ParsedSerpAdditionalProperty.table_name} _PSAP
                          WHERE #{conditions.join(' AND ')} #{" AND (#{subconditions.join(' OR ')})" if subconditions.present?}
                        ) _PSAP USING(keyword_id, location_id)
              #{opts[:limit].to_i.positive? ? 'LIMIT :limit OFFSET :offset' : ''}"

          sql = query.match?(/:\w+/) ? ::Core::Models::ReadOnly.send(:sanitize_sql_array, [query, new_opts]) : query

          rows = ::Core::Models::ReadOnly.connection.select_all(sql)

          total_count = 0
          rows.each do |r|
            r.symbolize_keys!

            r[:answerbox_website] = r[:answerbox_website].present? ? r[:answerbox_website] : "-"
            r[:knowledge_panel_website] = r[:knowledge_panel_website].present? ? r[:knowledge_panel_website] : "-"
            r[:knowledge_panel_actions] = r[:knowledge_panel_actions].present? ? r[:knowledge_panel_actions].gsub(/{|}/,'').split(",").join("\n") : "-"
            r[:knowledge_panel_reviews] = r[:knowledge_panel_reviews].present? ? r[:knowledge_panel_reviews].gsub(/{|}/,'').split(",").join("\n") : "-"
            r[:rank] = (rank = r[:rank].to_i; rank > 50 || rank == 0) ? '50+' : rank

            total_count = r[:total_count].to_i unless total_count.positive?
            r.delete(:total_count)
          end

          format_response(rows, total_count, opts)
        end
      end
    end
  end
end
