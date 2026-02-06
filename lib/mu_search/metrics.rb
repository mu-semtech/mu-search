require 'prometheus/client'
require 'prometheus/client/formats/text'

module MuSearch
  module Metrics
    class << self
      ES_STATS_CACHE_TTL = 15

      def setup
        @registry = Prometheus::Client.registry

        # HTTP Request Metrics
        @http_requests_total = @registry.counter(
          :musearch_http_requests_total,
          docstring: 'Total HTTP requests',
          labels: [:endpoint, :method, :status]
        )

        @http_request_duration_seconds = @registry.histogram(
          :musearch_http_request_duration_seconds,
          docstring: 'HTTP request duration in seconds',
          labels: [:endpoint],
          buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]
        )

        # Index Metrics
        @indexes_total = @registry.gauge(
          :musearch_indexes_total,
          docstring: 'Total number of indexes by type and status',
          labels: [:type, :status]
        )

        @indexes_documents_total = @registry.gauge(
          :musearch_indexes_documents_total,
          docstring: 'Total document count by index type',
          labels: [:type]
        )

        @indexes_eager_total = @registry.gauge(
          :musearch_indexes_eager_total,
          docstring: 'Total number of eager indexes'
        )

        # Queue Metrics
        @update_queue_size = @registry.gauge(
          :musearch_update_queue_size,
          docstring: 'Current size of update queue'
        )

        @delta_queue_size = @registry.gauge(
          :musearch_delta_queue_size,
          docstring: 'Current size of delta queue'
        )

        @deltas_received_total = @registry.counter(
          :musearch_deltas_received_total,
          docstring: 'Total deltas received'
        )

        @updates_processed_total = @registry.counter(
          :musearch_updates_processed_total,
          docstring: 'Total updates processed'
        )

        # Elasticsearch Metrics
        @elasticsearch_up = @registry.gauge(
          :musearch_elasticsearch_up,
          docstring: 'Elasticsearch availability (1=up, 0=down)'
        )

        @elasticsearch_cluster_status = @registry.gauge(
          :musearch_elasticsearch_cluster_status,
          docstring: 'Elasticsearch cluster health (2=green, 1=yellow, 0=red)'
        )

        @elasticsearch_read_only = @registry.gauge(
          :musearch_elasticsearch_read_only,
          docstring: 'Elasticsearch read-only status due to disk watermark (1=read-only, 0=normal, -1=unknown)'
        )

        @elasticsearch_pool_available = @registry.gauge(
          :musearch_elasticsearch_pool_available,
          docstring: 'Available Elasticsearch connections in pool'
        )

        @elasticsearch_pool_size = @registry.gauge(
          :musearch_elasticsearch_pool_size,
          docstring: 'Total Elasticsearch connection pool size'
        )

        # SPARQL Metrics
        @sparql_up = @registry.gauge(
          :musearch_sparql_up,
          docstring: 'SPARQL endpoint availability (1=up, 0=down)'
        )

        @sparql_pool_available = @registry.gauge(
          :musearch_sparql_pool_available,
          docstring: 'Available SPARQL connections in pool'
        )

        @sparql_pool_size = @registry.gauge(
          :musearch_sparql_pool_size,
          docstring: 'Total SPARQL connection pool size'
        )

        # Thread Health
        @update_handler_threads = @registry.gauge(
          :musearch_update_handler_threads,
          docstring: 'Number of active update handler threads'
        )

        # Info
        @info = @registry.gauge(
          :musearch_info,
          docstring: 'Mu-search service info',
          labels: [:version]
        )
        @info.set(1, labels: { version: musearch_version })

        @es_stats_cache = nil
        @es_stats_cache_time = nil
        @es_health_cache = nil
        @es_read_only_cache = nil
        @sparql_up_cache = nil

        Mu::log.info("METRICS") { "Prometheus metrics initialized" }
      end

      def record_request(endpoint:, method:, status:, duration:)
        return unless @registry

        normalized_endpoint = normalize_endpoint(endpoint)
        @http_requests_total.increment(labels: { endpoint: normalized_endpoint, method: method, status: status.to_s })
        @http_request_duration_seconds.observe(duration, labels: { endpoint: normalized_endpoint })
      end

      def increment_deltas_received
        return unless @registry
        @deltas_received_total.increment
      end

      def increment_updates_processed
        return unless @registry
        @updates_processed_total.increment
      end

      def collect_metrics(index_manager:, delta_handler:, elasticsearch:)
        return unless @registry

        collect_index_metrics(index_manager, elasticsearch)
        collect_queue_metrics(index_manager, delta_handler)
        collect_connection_pool_metrics(elasticsearch)
        collect_thread_metrics(delta_handler)
      end

      def render
        return "" unless @registry
        Prometheus::Client::Formats::Text.marshal(@registry)
      end

      private

      def musearch_version
        ENV.fetch('MU_SEARCH_VERSION', 'unknown')
      end

      def normalize_endpoint(path)
        # Preserve known static endpoints
        return path if %w[/health /metrics /update /indexes].include?(path)

        # Only normalize UUIDs - keep type names for useful per-type metrics
        path.gsub(%r{/[a-f0-9-]{36}(/|$)}, '/:uuid\1')
      end

      def collect_index_metrics(index_manager, elasticsearch)
        status_counts = Hash.new { |h, k| h[k] = Hash.new(0) }
        doc_counts = Hash.new(0)
        eager_count = 0

        es_stats = get_cached_es_stats(elasticsearch)

        index_manager.indexes.each do |type_name, indexes_by_group|
          indexes_by_group.each_value do |index|
            status_counts[type_name][index.status.to_s] += 1
            eager_count += 1 if index.is_eager_index

            doc_count = es_stats.dig('indices', index.name, 'total', 'docs', 'count') || 0
            doc_counts[type_name] += doc_count
          end
        end

        status_counts.each do |type_name, statuses|
          statuses.each do |status, count|
            @indexes_total.set(count, labels: { type: type_name, status: status })
          end
        end

        doc_counts.each do |type_name, count|
          @indexes_documents_total.set(count, labels: { type: type_name })
        end

        @indexes_eager_total.set(eager_count)
      end

      def collect_queue_metrics(index_manager, delta_handler)
        update_handler = delta_handler.instance_variable_get(:@update_handler)
        if update_handler&.respond_to?(:queue)
          @update_queue_size.set(update_handler.queue.length)
        end

        if delta_handler.respond_to?(:queue)
          @delta_queue_size.set(delta_handler.queue.size)
        end
      end

      def collect_connection_pool_metrics(elasticsearch)
        # Elasticsearch pool
        begin
          es_pool = MuSearch::ElasticConnectionPool.instance
          @elasticsearch_pool_available.set(es_pool.available)
          @elasticsearch_pool_size.set(es_pool.size)

          es_health = get_cached_es_health(elasticsearch)
          if es_health
            @elasticsearch_up.set(1)
            status_value = case es_health['status']
                           when 'green' then 2
                           when 'yellow' then 1
                           else 0
                           end
            @elasticsearch_cluster_status.set(status_value)
          else
            @elasticsearch_up.set(0)
            @elasticsearch_cluster_status.set(0)
          end

          @elasticsearch_read_only.set(get_cached_es_read_only)
        rescue => e
          Mu::log.warn("METRICS") { "Failed to collect ES metrics: #{e.message}" }
          @elasticsearch_up.set(0)
        end

        # SPARQL pool
        begin
          sparql_pool = MuSearch::SPARQL::ConnectionPool.instance
          @sparql_pool_available.set(sparql_pool.available)
          @sparql_pool_size.set(sparql_pool.size)

          @sparql_up.set(get_cached_sparql_up)
        rescue => e
          Mu::log.warn("METRICS") { "Failed to collect SPARQL metrics: #{e.message}" }
          @sparql_up.set(0)
        end
      end

      def collect_thread_metrics(delta_handler)
        update_handler = delta_handler.instance_variable_get(:@update_handler)
        if update_handler&.respond_to?(:runners)
          alive_count = update_handler.runners.count(&:alive?)
          @update_handler_threads.set(alive_count)
        end
      end

      def get_cached_es_stats(elasticsearch)
        now = Time.now
        if @es_stats_cache.nil? || @es_stats_cache_time.nil? || (now - @es_stats_cache_time) > ES_STATS_CACHE_TTL
          @es_stats_cache = elasticsearch.index_stats rescue {}
          @es_stats_cache_time = now
        end
        @es_stats_cache
      end

      def get_cached_es_health(elasticsearch)
        @es_health_cache ||= {}
        now = Time.now
        if @es_health_cache[:data].nil? || @es_health_cache[:time].nil? || (now - @es_health_cache[:time]) > ES_STATS_CACHE_TTL
          @es_health_cache[:data] = MuSearch::ElasticConnectionPool.with_client do |client|
            client.cluster.health
          end rescue nil
          @es_health_cache[:time] = now
        end
        @es_health_cache[:data]
      end

      def get_cached_es_read_only
        now = Time.now
        @es_read_only_cache ||= {}
        if @es_read_only_cache[:data].nil? || @es_read_only_cache[:time].nil? || (now - @es_read_only_cache[:time]) > ES_STATS_CACHE_TTL
          @es_read_only_cache[:data] = check_elasticsearch_read_only
          @es_read_only_cache[:time] = now
        end
        @es_read_only_cache[:data]
      end

      def check_elasticsearch_read_only
        MuSearch::ElasticConnectionPool.with_client do |client|
          response = client.indices.get_settings(
            index: '_all',
            filter_path: '**.index.blocks.read_only_allow_delete'
          )
          response.empty? ? 0 : 1
        end
      rescue => e
        Mu::log.warn("METRICS") { "Failed to check ES read-only status: #{e.message}" }
        -1
      end

      def get_cached_sparql_up
        now = Time.now
        @sparql_up_cache ||= {}
        if @sparql_up_cache[:data].nil? || @sparql_up_cache[:time].nil? || (now - @sparql_up_cache[:time]) > ES_STATS_CACHE_TTL
          @sparql_up_cache[:data] = MuSearch::SPARQL::ConnectionPool.up? ? 1 : 0
          @sparql_up_cache[:time] = now
        end
        @sparql_up_cache[:data]
      end
    end
  end
end
