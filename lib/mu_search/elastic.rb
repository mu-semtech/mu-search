require 'elasticsearch'
require 'faraday/typhoeus'
require 'connection_pool'

# monkeypatch "authentic product check"" in client
module ElasticsearchMonkeyPatch
  private

  def verify_elasticsearch(*args, &block)
    while not @verified do
      sleep 1
      begin
        response = @transport.perform_request(*args, &block)
        response.headers['x-elastic-product'] = 'Elasticsearch'
        @verified = true
      rescue StandardError => e
        Mu::log.info("SETUP") { "no reaction from elastic, retrying..." }
        next
      end
    end
    response
  end
end

Elasticsearch::Client.prepend(ElasticsearchMonkeyPatch)

# A wrapper around elasticsearch client for backwards compatiblity
# see https://rubydoc.info/gems/elasticsearch-api/Elasticsearch
# and https://www.elastic.co/guide/en/elasticsearch/client/ruby-api/current/examples.html
# for docs on the client api
##
module MuSearch
  class ElasticWrapper
    # Sets up the ElasticSearch connection pool
    def initialize(size:)
      MuSearch::ElasticConnectionPool.setup(size: size)
      @logger = Mu::log
    end

    # Checks whether or not ElasticSearch is up
    #
    # Executes a health check and accepts either "green" or "yellow".
    def up?
      Mu::log.info("SETUP") { "Checking if Elasticsearch is up..." }
      MuSearch::ElasticConnectionPool.with_client do |es_client|
        begin
          health = es_client.cluster.health
          Mu::log.info("SETUP") { "Elasticsearch cluster health: #{health["status"]}" }
          health["status"] == "yellow" or health["status"] == "green"
        rescue
          false
        end
      end
    end

    # Checks whether or not the supplied index exists.
    #   - index: string name of the index
    #
    # Executes a HEAD request. If that succeeds we can assume the index
    # exists.
    def index_exists?(index)
      MuSearch::ElasticConnectionPool.with_client do |es_client|
        begin
          es_client.indices.exists?(index: index)
        rescue StandardError => e
          @logger.warn("ELASTICSEARCH") { "Error while checking if index #{index} exists. Assuming it doesn't." }
          @logger.warn("ELASTICSEARCH") { e.full_message }
          false
        end
      end
    end

    # Creates an index in Elasticsearch
    #   - index: Index to be created
    #   - mappings: Optional pre-defined document mappings for the index,
    #     JSON object passed directly to Elasticsearch.
    #   - settings: Optional JSON object passed directly to Elasticsearch
    def create_index(index, mappings = nil, settings = nil)
      MuSearch::ElasticConnectionPool.with_client do |es_client|
        begin
          es_client.indices.create(index: index, body: { settings: settings, mappings: mappings})
        rescue Elastic::Transport::Transport::Errors::BadRequest => e
          error_message = e.message
          if error_message.include?("resource_already_exists_exception")
            @logger.warn("ELASTICSEARCH") {"Failed to create index #{index}, because it already exists" }
          else
            @logger.error("ELASTICSEARCH") {"Failed to create index #{index}, error: #{error_message}" }
            raise e
          end
        rescue StandardError => e
          @logger.error("Failed to create index #{index}\n  Error: #{e.full_message}")
          raise e
        end
      end
    end

    # Fetch index statistics
    def index_stats
      MuSearch::ElasticConnectionPool.with_client do |es_client|
        es_client.indices.stats
      end
    end

    # Deletes an index from ElasticSearch
    #   - index: Name of the index to be removed
    #
    # Returns true when the index existed and is succesfully deleted.
    # Otherwise false.
    # Throws an error if the index exists but fails to be deleted.
    def delete_index(index)
      MuSearch::ElasticConnectionPool.with_client do |es_client|
        begin
          es_client.indices.delete(index: index)
          @logger.debug("ELASTICSEARCH") { "Successfully deleted index #{index}" }
          true
        rescue Elastic::Transport::Transport::Errors::NotFound => e
          @logger.debug("ELASTICSEARCH") { "Index #{index} doesn't exist and cannot be deleted." }
          false
        rescue StandardError => e
          @logger.error("ELASTICSEARCH") { "Failed to delete index #{index}. Error: #{e.message}" }
          raise "Failed to delete index #{index}: #{e.message}"
        end
      end
    end

    # Refreshes an ElasticSearch index, making documents available for
    # search.
    #   - index: Name of the index which will be refreshed.
    #
    # Returns whether the refresh succeeded
    #
    # When we store documents in ElasticSearch, they are not necessarily
    # available immediately. It requires a refresh of the index. This
    # operation happens once every second. When we build an index to
    # query it immediately, we should ensure to refresh the index before
    # querying.
    def refresh_index(index)
      MuSearch::ElasticConnectionPool.with_client do |es_client|
        begin
          es_client.indices.refresh(index: index)
          @logger.debug("ELASTICSEARCH") { "Successfully refreshed index #{index}" }
          true
        rescue Elastic::Transport::Transport::Errors::NotFound => e
          @logger.warn("ELASTICSEARCH") { "Index #{index} does not exist, cannot refresh." }
          false
        rescue StandardError => e
          @logger.error("ELASTICSEARCH") { "Failed to refresh index #{index}. Error: #{e.full_message}" }
          false
        end
      end
    end

    # Clear a given index by deleting all documents in the Elasticsearch index
    #   - index: Index name to clear
    # Note: this operation does not delete the index in Elasticsearch
    def clear_index(index)
      MuSearch::ElasticConnectionPool.with_client do |es_client|
        begin
          # The `query: { match_all: {} }` deletes all documents in the index.
          es_client.delete_by_query(index: index, body: { query: { match_all: {} } })
          @logger.debug("ELASTICSEARCH") { "Successfully cleared all documents from index #{index}" }
          true
        rescue Elastic::Transport::Transport::Errors::NotFound => e
          @logger.warn("ELASTICSEARCH") { "Index #{index} does not exist, cannot clear documents." }
          false
        rescue StandardError => e
          @logger.error("ELASTICSEARCH") { "Failed to clear documents in index #{index}. Error: #{e.message}" }
          raise e
        end
      end
    end

    # Gets a single document from an index by its ElasticSearch id.
    # Returns nil if the document cannot be found.
    #   - index: Index to retrieve the document from
    #   - id: ElasticSearch ID of the document
    def get_document(index, id)
      MuSearch::ElasticConnectionPool.with_client do |es_client|
        begin
          es_client.get(index: index, id: id)
        rescue Elastic::Transport::Transport::Errors::NotFound => e
          @logger.debug("ELASTICSEARCH") { "Document #{id} not found in index #{index}" }
          nil
        rescue StandardError => e
          @logger.error("ELASTICSEARCH") { "Failed to get document #{id} from index #{index}.\n Error: #{e.full_message}" }
          raise e
        end
      end
    end

    # Inserts a new document in an Elasticsearch index
    #   - index: Index to store the document in.
    #   - id: Elasticsearch identifier to store the document under.
    #   - document: document contents to index (as a ruby json object)
    # Returns the inserted document
    # Raises an error on failure.
    def insert_document(index, id, document)
      MuSearch::ElasticConnectionPool.with_client do |es_client|
        begin
          body = es_client.index(index: index, id: id, body: document)
          @logger.debug("ELASTICSEARCH") { "Inserted document #{id} in index #{index}" }
          body
        rescue StandardError => e
          @logger.error("ELASTICSEARCH") { "Failed to insert document #{id} in index #{index}.\n  Error: #{e.full_message}" }
          raise e
        end
      end
    end

    # Partially updates an existing document in Elasticsearch index
    #   - index: Index to update the document in
    #   - id: ElasticSearch identifier of the document
    #   - document: New document contents
    # Returns the updated document or nil if the document cannot be found.
    # Otherwise, raises an error.
    def update_document(index, id, document)
      MuSearch::ElasticConnectionPool.with_client do |es_client|
        begin
          body = es_client.update(index: index, id: id, body: {doc: document})
          @logger.debug("ELASTICSEARCH") { "Updated document #{id} in index #{index}" }
          body
        rescue Elastic::Transport::Transport::Errors::NotFound => e
          @logger.info("ELASTICSEARCH") { "Cannot update document #{id} in index #{index} because it doesn't exist" }
          nil
        rescue StandardError => e
          @logger.error("ELASTICSEARCH") { "Failed to update document #{id} in index #{index}.\n Error: #{e.full_message}" }
          raise e
        end
      end
    end

    # Indexes the full document, replacing any existing document with the same id.
    # Creates the document if it doesn't exist yet.
    # - index: index to store document in
    # - id: elastic identifier to store the document under
    # - document: document contents (as a ruby json object)
    def upsert_document(index, id, document)
      insert_document index, id, document
    end

    # Deletes a document from an Elasticsearch index
    #   - index: Index to remove the document from
    #   - id: ElasticSearch identifier of the document
    # Returns true when the document existed and is succesfully deleted.
    # Otherwise false.
    # Throws an error if the document exists but fails to be deleted.
    def delete_document(index, id)
      MuSearch::ElasticConnectionPool.with_client do |es_client|
        begin
          es_client.delete(index: index, id: id)
          @logger.debug("ELASTICSEARCH") { "Successfully deleted document #{id} in index #{index}" }
          true
        rescue Elastic::Transport::Transport::Errors::NotFound => e
          @logger.debug("ELASTICSEARCH") { "Document #{id} doesn't exist in index #{index} and cannot be deleted." }
          false
        rescue StandardError => e
          @logger.error("ELASTICSEARCH") { "Failed to dele document #{id} in index #{index}.\n Error: #{e.full_message}" }
          raise e
        end
      end
    end

    # Searches for documents in the given indexes
    #   - indexes: Array of indexes to be searched
    #   - query: Elasticsearch query JSON object in ruby format
    def search_documents(indexes:, query: nil)
      MuSearch::ElasticConnectionPool.with_client do |es_client|
        begin
          @logger.debug("SEARCH") { "Searching Elasticsearch index(es) #{indexes} with query #{query}" }
          es_client.search(index: indexes, body: query)
        rescue Elastic::Transport::Transport::Errors::BadRequest => e
          raise ArgumentError, "Invalid search query #{query}"
        rescue StandardError => e
          @logger.error("SEARCH") { "Searching documents in index(es) #{indexes} failed.\n Error: #{e.full_message}" }
          raise e
        end
      end
    end

    # Counts search results for documents in the given indexex
    #   - indexes: Array of indexes to be searched
    #   - query: Elasticsearch query JSON object in ruby format
    def count_documents(indexes:, query: nil)
      MuSearch::ElasticConnectionPool.with_client do |es_client|
        begin
          @logger.debug("SEARCH") { "Count search results in index(es) #{indexes} for body #{query.inspect}" }
          response = es_client.count(index: indexes, body: query)
          response["count"]
        rescue Elastic::Transport::Transport::Errors::BadRequest => e
          @logger.error("SEARCH") { "Counting search results in index(es) #{indexes} failed.\n Error: #{e.full_message}" }
          raise ArgumentError, "Invalid count query #{query}"
        rescue StandardError => e
          @logger.error("SEARCH") { "Counting search results in index(es) #{indexes} failed.\n Error: #{e.full_message}" }
          raise e
        end
      end
    end
  end

  class ElasticConnectionPool
    @instance = nil

    def self.setup(size: 4)
      @instance = ::ConnectionPool.new(size: size, timeout: 3) do
        Elasticsearch::Client.new(host: 'elasticsearch', port: 9200)
      end
      Mu::log.info("SETUP") { "Setup Elasticsearch connection pool with #{@instance.size} connections." }
    end

    def self.instance
      if @instance
        @instance
      else
        raise "Elasticsearch connection pool not yet initialized. Please call MuSearch::ElasticConnectionPool.setup() first"
      end
    end

    def self.with_client
      instance.with do |client|
        Mu::log.info("ELASTICSEARCH") { "Claim Elasticsearch connection from pool. #{@instance.available}/#{@instance.size} connections are still available." }
        yield client
      end
    end
  end
end
