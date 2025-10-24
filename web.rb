require 'webrick'

require_relative 'lib/logger.rb'
require_relative 'lib/sparql-client.rb'
require_relative 'lib/mu_search/sparql.rb'
require_relative 'lib/mu_search/authorization_utils.rb'
require_relative 'lib/mu_search/delta_handler.rb'
require_relative 'lib/mu_search/automatic_update_handler.rb'
require_relative 'lib/mu_search/invalidating_update_handler.rb'
require_relative 'lib/mu_search/config_parser.rb'
require_relative 'lib/mu_search/document_builder.rb'
require_relative 'lib/mu_search/index_builder.rb'
require_relative 'lib/mu_search/search_index.rb'
require_relative 'lib/mu_search/index_manager.rb'
require_relative 'lib/mu_search/elastic.rb'
require_relative 'lib/mu_search/tika.rb'
require_relative 'framework/elastic_query_builder.rb'
require_relative 'framework/jsonapi.rb'

##
# WEBrick setup
##
max_uri_length = ENV["MAX_REQUEST_URI_LENGTH"].to_i > 0 ? ENV["MAX_REQUEST_URI_LENGTH"].to_i : 10240
Mu::log.info("SETUP") { "Set WEBrick MAX_URI_LENGTH to #{max_uri_length}" }
WEBrick::HTTPRequest.send(:remove_const, :MAX_URI_LENGTH) if defined?(WEBrick::HTTPRequest::MAX_URI_LENGTH)
WEBrick::HTTPRequest.const_set("MAX_URI_LENGTH", max_uri_length)
max_header_length = ENV["MAX_REQUEST_HEADER_LENGTH"].to_i > 0 ? ENV["MAX_REQUEST_HEADER_LENGTH"].to_i : 1024000
Mu::log.info("SETUP") { "Set WEBrick MAX_HEADER_LENGTH to #{max_header_length}" }
WEBrick::HTTPRequest.send(:remove_const, :MAX_HEADER_LENGTH) if defined?(WEBrick::HTTPRequest::MAX_HEADER_LENGTH)
WEBrick::HTTPRequest.const_set("MAX_HEADER_LENGTH", max_header_length)
max_yaml_size = ENV["MAX_YAML_SIZE"].to_i > 0 ? ENV["MAX_YAML_SIZE"].to_i : 20_000_000
Psych::Parser.code_point_limit= max_yaml_size

Mu::log.formatter = proc do |severity, datetime, progname, msg|
  "#{severity} [\##{$$}] #{progname} -- #{msg}\n"
end

before do
  request.path_info.chomp!('/')
  content_type 'application/vnd.api+json'
end

##
# Setup index manager based on configuration
##
def setup_index_manager(elasticsearch, config)
  search_configuration = config.select do |key|
    [:type_definitions, :default_index_settings,
     :persist_indexes, :eager_indexing_groups, :number_of_threads,
     :batch_size, :max_batches, :attachment_path_base,
     :ignored_allowed_groups].include? key
  end

  MuSearch::IndexManager.new(
    logger: Mu::log,
    elasticsearch: elasticsearch,
    search_configuration: search_configuration)
end

##
# Setup delta handling based on configuration
##
def setup_delta_handling(index_manager, elasticsearch, config)
  if config[:automatic_index_updates]
    search_configuration = config.select do |key|
      [:type_definitions, :number_of_threads, :update_wait_interval_minutes,
       :attachment_path_base].include? key
    end
    handler = MuSearch::AutomaticUpdateHandler.new(
      logger: Mu::log,
      index_manager: index_manager,
      elasticsearch: elasticsearch,
      search_configuration: search_configuration)
  else
    search_configuration = config.select do |key|
      [:type_definitions, :number_of_threads, :update_wait_interval_minutes].include? key
    end
    handler = MuSearch::InvalidatingUpdateHandler.new(
      logger: Mu::log,
      index_manager: index_manager,
      search_configuration: search_configuration)
  end

  delta_handler = MuSearch::DeltaHandler.new(
    logger: Mu::log,
    update_handler: handler,
    search_configuration: { type_definitions: config[:type_definitions] })
  delta_handler
end

##
# Configures the system and makes sure everything is up.
##
configure do
  set :protection, :except => [:json_csrf]
  set :dev, (ENV['RACK_ENV'] == 'development')

  configuration = MuSearch::ConfigParser.parse('/config/config.json')
  set configuration

  number_of_threads = configuration[:number_of_threads]
  MuSearch::Tika::ConnectionPool.setup(size: number_of_threads)

  elasticsearch = MuSearch::Elastic.new(size: number_of_threads)
  set :elasticsearch, elasticsearch

  MuSearch::SPARQL::ConnectionPool.setup(size: number_of_threads)

  until elasticsearch.up?
    Mu::log.info("SETUP") { "...waiting for elasticsearch..." }
    sleep 1
  end

  until MuSearch::SPARQL::ConnectionPool.up?
    Mu::log.info("SETUP") { "...waiting for SPARQL endpoint..." }
    sleep 1
  end

  index_manager = setup_index_manager elasticsearch, configuration
  set :index_manager, index_manager
  delta_handler = setup_delta_handling index_manager, elasticsearch, configuration
  set :delta_handler, delta_handler
end

###
# API ENDPOINTS
###

# Processes an update from the delta system.
# See MuSearch::DeltaHandler and MuSearch::UpdateHandler for more info
post "/update" do
  settings.delta_handler.handle_deltas @json_body
  { message: "Thanks for all the updates." }.to_json
end

# Performs a search in Elasticsearch
#
# Before the search query is performed, it makes search the required
# search indexes are created and up-to-date.
#
# The search is only performed on indexes the user has access to
# based on the provided allowed groups header.
# If none is provided, the allowed groups are determined by executing
# a query on the triplestore.
#
# See README for more information about the filter syntax.
get "/:path/search" do |path|
  begin
    allowed_groups = get_allowed_groups_with_fallback
    Mu::log.debug("AUTHORIZATION") { "Search request received allowed groups #{allowed_groups}" }
  rescue StandardError => e
    Mu::log.error("AUTHORIZATION") { e.full_message }
    error("Unable to determine authorization groups", 401)
  end

  elasticsearch = settings.elasticsearch
  index_manager = settings.index_manager
  type_def = settings.type_definitions.values.find { |type_def| type_def["on_path"] == path }

  begin
    raise ArgumentError, "No search configuration found for path #{path}" if type_def.nil?

    indexes = index_manager.fetch_indexes(type_def["type"], allowed_groups)

    search_configuration = {
      common_terms_cutoff_frequency: settings.common_terms_cutoff_frequency
    }
    query_builder = ElasticQueryBuilder.new(
      logger: Mu::log,
      type_definition: type_def,
      filter: params["filter"],
      page: params["page"],
      sort: params["sort"],
      count: params["count"],
      highlight: params["highlight"],
      collapse_uuids: params["collapse_uuids"],
      search_configuration: search_configuration)

    if indexes.length == 0
      Mu::log.info("SEARCH") { "No indexes found to search in. Returning empty result" }
      format_search_results(type_def["type"], 0, query_builder.page_number, query_builder.page_size, []).to_json
    else
      search_query = query_builder.build_search_query

      while indexes.any? { |index| index.status == :updating }
        Mu::log.info("SEARCH") { "Waiting for indexes to be up-to-date..." }
        sleep 0.5
      end
      Mu::log.debug("SEARCH") { "All indexes are up to date" }

      index_names = indexes.map { |index| index.name }
      search_results = elasticsearch.search_documents indexes: index_names, query: search_query
      count =
        if query_builder.collapse_uuids
          search_results.dig("aggregations", "type_count", "value")
        elsif query_builder.use_exact_count
          search_results.dig("hits", "total", "value")
        else
          count_query = query_builder.build_count_query
          elasticsearch.count_documents indexes: index_names, query: count_query
        end
      Mu::log.debug("SEARCH") { "Found #{count} results" }
      format_search_results(type_def["type"], count, query_builder.page_number, query_builder.page_size, search_results).to_json
    end
  rescue ArgumentError => e
    error(e.message, 400)
  rescue StandardError => e
    Mu::log.error("SEARCH") { e.full_message }
    error(e.inspect, 500)
  end
end

# Execute a search query by passing a raw Elasticsearch Query DSL as request body
#
# The search is only performed on indexes the user has access to
# based on the provided allowed groups header.
# If none is provided, the allowed groups are determined by executing
# a query on the triplestore.
#
# This endpoint must be used with caution and explicitly enabled in the search config!
if settings.enable_raw_dsl_endpoint
  post "/:path/search" do |path|
    begin
      allowed_groups = get_allowed_groups_with_fallback
      Mu::log.debug("AUTHORIZATION") { "Search request received allowed groups #{allowed_groups}" }
    rescue StandardError => e
      Mu::log.error("AUTHORIZATION") { e.full_message }
      error("Unable to determine authorization groups", 401)
    end

    elasticsearch = settings.elasticsearch
    index_manager = settings.index_manager
    type_def = settings.type_definitions.values.find { |type_def| type_def["on_path"] == path }

    @json_body["size"] ||= 10
    @json_body["from"] ||= 0
    page_size = @json_body["size"]
    page_number = @json_body["from"] / page_size

    begin
      raise ArgumentError, "No search configuration found for path #{path}" if type_def.nil?

      indexes = index_manager.fetch_indexes(type_def["type"], allowed_groups)

      if indexes.length == 0
        Mu::log.info("SEARCH") { "No indexes found to search in. Returning empty result" }
        format_search_results(type_def["type"], 0, page_number, page_size, []).to_json
      else
        search_query = @json_body
        index_names = indexes.map { |index| index.name }
        search_results = elasticsearch.search_documents indexes: index_names, query: search_query
        count_query = search_query.select { |key, _| key != "from" and key != "size" and key != "sort" }
        count = elasticsearch.count_documents indexes: index_names, query: count_query
        Mu::log.debug("SEARCH") { "Found #{count} results" }
        format_search_results(type_def["type"], count, page_number, page_size, search_results).to_json
      end
    rescue ArgumentError => e
      error(e.message, 400)
    rescue StandardError => e
      Mu::log.error("SEARCH") { e.full_message }
      error(e.inspect, 500)
    end
  end
end

# Updates the indexes for the given :path.
# If an authorization header is provided, only the authorized indexes are updated.
# Otherwise, all indexes for the path are updated.
#
# Use _all as path to update all index types
#
# Note:
# - the search index is only marked as invalid in memory.
#   The index is not removed from Elasticsearch nor the triplestore.
#   Hence, on restart of mu-search, the index will be considered valid again.
# - an invalidated index will be updated before executing a search query on it.
post "/:path/index" do |path|
  begin
    allowed_groups = get_allowed_groups
    Mu::log.debug("AUTHORIZATION") { "Index update request received allowed groups #{allowed_groups || 'none'}" }
  rescue StandardError => e
    Mu::log.error("AUTHORIZATION") { e.full_message }
    error("Unable to determine authorization groups", 401)
  end

  index_type = path == "_all" ? nil : path
  index_manager = settings.index_manager
  indexes = index_manager.fetch_indexes index_type, allowed_groups, force_update: true

  data = indexes.map do |index|
    {
      type: "indexes",
      id: index.name,
      attributes: {
        uri: index.uri,
        status: index.status,
        'allowed-groups' => index.allowed_groups
      }
    }
  end

  { data: data }.to_json
end

# Invalidates the indexes for the given :path.
# If an authorization header is provided, only the authorized
# indexes are invalidated.
# Otherwise, all indexes for the path are invalidated.
#
# Use _all as path to invalidate all index types
#
# Note:
# - the search index is only marked as invalid in memory.
#   The index is not removed from Elasticsearch nor the triplestore.
#   Hence, on restart of mu-search, the index will be considered valid again.
# - an invalidated index will be updated before executing a search query on it.
post "/:path/invalidate" do |path|
  begin
    allowed_groups = get_allowed_groups
    Mu::log.debug("AUTHORIZATION") { "Index invalidation request received allowed groups #{allowed_groups || 'none'}" }
  rescue StandardError => e
    Mu::log.error("AUTHORIZATION") { e.full_message }
    error("Unable to determine authorization groups", 401)
  end

  index_type = path == "_all" ? nil : path
  index_manager = settings.index_manager
  indexes = index_manager.invalidate_indexes index_type, allowed_groups

  data = indexes.map do |index|
    {
      type: "indexes",
      id: index.name,
      attributes: {
        uri: index.uri,
        status: index.status,
        'allowed-groups' => index.allowed_groups
      }
    }
  end

  { data: data }.to_json
end

# Removes the indexes for the given :path.
# If an authorization header is provided, only the authorized
# indexes are removed.
# Otherwise, all indexes for the path are removed.
#
# Use _all as path to remove all index types
#
# Note: a removed index will be recreated before executing a search query on it.
delete "/:path" do |path|
  begin
    allowed_groups = get_allowed_groups
    Mu::log.debug("AUTHORIZATION") { "Index delete request received allowed groups #{allowed_groups || 'none'}" }
  rescue StandardError => e
    Mu::log.error("AUTHORIZATION") { e.full_message }
    error("Unable to determine authorization groups", 401)
  end

  index_type = path == "_all" ? nil : path
  index_manager = settings.index_manager
  indexes = index_manager.remove_indexes index_type, allowed_groups

  data = indexes.map do |index|
    {
      type: "indexes",
      id: index.name,
      attributes: {
        uri: index.uri,
        status: index.status,
        'allowed-groups' => index.allowed_groups
      }
    }
  end

  { data: data }.to_json
end

# Health report
# TODO Make this more descriptive - status of all indexes?
get "/health" do
  settings.index_manager.indexes.inspect
  { status: "up" }.to_json
end

get "/indexes" do
  # grab index info from index_manager
  # indexes are kept as indexes[type][serialized_allowed_groups] = actual_index_config (class SearchIndex)
  # so we just extract actual_index_config with the line below
  index_info = settings.index_manager.indexes.values.flatten.map{ |x| x.values }.flatten
  elastic_stats = settings.elasticsearch.index_stats
  response = []
  index_info.each do |index|
    response << {
      uri: index.uri,
      name: index.name,
      type: index.type_name,
      is_eager_index: index.is_eager_index,
      allowed_groups: index.allowed_groups,
      status: index.status,
      document_count: elastic_stats.dig('indices', index.name, 'total', 'docs', 'count'),
      exists_in_elasticsearch: elastic_stats.dig('indices', index.name).nil? ? false : true
    }
  end
  response.to_json
end
