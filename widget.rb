# frozen_string_literal: true

module Widgets
  class Widget
    include ::Cache::Accessor

    class QuotaExceededError < StandardError; end

    class << self
      def data_source_url
        "/#{to_s.underscore}"
      end

      def additional_templates
        []
      end

      def dashboard_widget_filters
        []
      end

      def required_param_names
        []
      end

      def permitted_param_names
        required_param_names
      end

      def resource_controller
        nil
      end

      def competitor_sites(opts)
        competitors = opts[:site].competitors

        competitor_tag_ids = []

        if opts[:keyword_tag_ids].present?
          query = 'SELECT competitor_tag_id FROM core.competitor_tags_keyword_tags WHERE keyword_tag_id IN (:keyword_tag_ids)'
          sql = query.match?(/:\w+/) ? ::Core::Models::ReadOnly.send(:sanitize_sql_array, [query, opts]) : query
          rows = ::Core::Models::ReadOnly.connection.select_all(sql)
          competitor_tag_ids += rows.map { |r| r['competitor_tag_id'].to_i }
        end

        if opts[:page_tag_ids].present?
          query = 'SELECT competitor_tag_id FROM core.competitor_tags_page_tags WHERE page_tag_id IN (:page_tag_ids)'
          sql = query.match?(/:\w+/) ? ::Core::Models::ReadOnly.send(:sanitize_sql_array, [query, opts]) : query
          rows = ::Core::Models::ReadOnly.connection.select_all(sql)
          competitor_tag_ids += rows.map { |r| r['competitor_tag_id'].to_i }
        end

        opts[:competitor_tag_ids] = competitor_tag_ids unless competitor_tag_ids.empty?

        if opts[:competitor_tag_ids].present?
          selected_site_ids = Core::Models::CompetitorTag.site_ids(opts[:competitor_tag_ids])
          competitors.select! { |c| selected_site_ids.include?(c.id) }
        end

        competitors
      end

      private

      def daily_quota_exceeded(context_id, quota)
        date = Date.today
        key = "quota:#{to_s.underscore.tr('/', ':')}:#{context_id}:#{date}"

        usage = GinzaUtil.redis.get(key).to_i

        return true if usage.present? && quota.present? && usage >= quota.to_i

        usage += 1

        GinzaUtil.redis.set(key, usage)
        GinzaUtil.redis.expireat(key, date.to_time.to_i + 2 * 24 * 60 * 60)

        false
      end

      def fetch_rows(sql, context, opts)
        if context.is_a?(Site) && context.combined_settings[:cache_site_widgets] ||
           context.is_a?(Account) && context.combined_settings[:cache_account_widgets]
          with_cache(context.id, Digest::MD5.hexdigest(sql), opts) do
            ::Core::Models::ReadOnly.connection.select_all(sql)
          end
        else
          ::Core::Models::ReadOnly.connection.select_all(sql)
        end
      end

      public

      def widget_class(obj)
        if obj.is_a?(Class)
          obj
        elsif obj.is_a?(String)
          begin
            "::Widgets::#{obj.camelize}".constantize
          rescue NameError
            nil
          end
        else
          raise 'Unknown attribute for widget class detection'
        end
      end

      def widget_superclass(obj)
        return nil unless obj

        widget_class = widget_class(obj)
        return nil unless widget_class

        if widget_class.ancestors.include?(::Widgets::Table)
          ::Widgets::Table
        elsif widget_class.ancestors.include?(::Widgets::Chart)
          ::Widgets::Chart
        elsif widget_class.ancestors.include?(::Widgets::Custom)
          ::Widgets::Custom
        elsif widget_class.ancestors.include?(::Widgets::Static)
          ::Widgets::Static
        else
          raise 'Unknown widget superclass'
        end
      end
    end
  end
end
