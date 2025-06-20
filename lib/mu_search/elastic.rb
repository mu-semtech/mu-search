require 'elasticsearch'
require 'faraday/typhoeus'

# monkeypatch "authentic product check"" in client
module Elasticsearch
  class Client
    alias original_verify_with_version_or_header verify_with_version_or_header

    def verify_with_version_or_header(...)
      original_verify_with_version_or_header(...)
    rescue Elasticsearch::UnsupportedProductError
      # silenty ignore this error
    end
  end
end

# A wrapper around elasticsearch client for backwards compatiblity
# see https://rubydoc.info/gems/elasticsearch-api/Elasticsearch
# and https://www.elastic.co/guide/en/elasticsearch/client/ruby-api/current/examples.html
# for docs on the client api
##
module MuSearch
  class Elastic
    attr_reader :client
    # Sets up the ElasticSearch instance
    def initialize(host: "localhost", port: 9200, logger:)
      @logger = logger
      @client = Elasticsearch::Client.new(host: host, port: port)
    end

    # Checks whether or not ElasticSearch is up
    #
    # Executes a health check and accepts either "green" or "yellow".
    def up?
      begin
        health = client.cluster.health
        health["status"] == "yellow" or health["status"] == "green"
      rescue
        false
      end
    end

    # Checks whether or not the supplied index exists.
    #   - index: string name of the index
    #
    # Executes a HEAD request. If that succeeds we can assume the index
    # exists.
    def index_exists?(index)
      begin
        client.indices.exists?(index: index)
      rescue StandardError => e
        @logger.warn("ELASTICSEARCH") { "Error while checking if index #{index} exists. Assuming it doesn't." }
        @logger.warn("ELASTICSEARCH") { e.full_message }
        false
      end
    end

    # Creates an index in Elasticsearch
    #   - index: Index to be created
    #   - mappings: Optional pre-defined document mappings for the index,
    #     JSON object passed directly to Elasticsearch.
    #   - settings: Optional JSON object passed directly to Elasticsearch
    def create_index(index, mappings = nil, settings = nil)
      begin
        client.indices.create(index: index, body: { settings: settings, mappings: mappings})
      rescue Elasticsearch::Transport::Transport::Errors::BadRequest => e
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
    # Fetch index statistics
    def index_stats
      client.indices.stats
    end

    # Deletes an index from ElasticSearch
    #   - index: Name of the index to be removed
    #
    # Returns true when the index existed and is succesfully deleted.
    # Otherwise false.
    # Throws an error if the index exists but fails to be deleted.
    def delete_index(index)
      begin
        client.indices.delete(index: index)
        @logger.debug("ELASTICSEARCH") { "Successfully deleted index #{index}" }
        true
      rescue Elasticsearch::Transport::Transport::Errors::NotFound => e
        @logger.debug("ELASTICSEARCH") { "Index #{index} doesn't exist and cannot be deleted." }
        false
      rescue StandardError => e
        @logger.error("ELASTICSEARCH") { "Failed to delete index #{index}. Error: #{e.message}" }
        raise "Failed to delete index #{index}: #{e.message}"
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
      begin
        client.indices.refresh(index: index)
        @logger.debug("ELASTICSEARCH") { "Successfully refreshed index #{index}" }
        true
      rescue Elasticsearch::Transport::Transport::Errors::NotFound => e
        @logger.warn("ELASTICSEARCH") { "Index #{index} does not exist, cannot refresh." }
        false
      rescue StandardError => e
        @logger.error("ELASTICSEARCH") { "Failed to refresh index #{index}. Error: #{e.full_message}" }
        false
      end
    end

    # Clear a given index by deleting all documents in the Elasticsearch index
    #   - index: Index name to clear
    # Note: this operation does not delete the index in Elasticsearch
    def clear_index(index)
      begin
        # The `query: { match_all: {} }` deletes all documents in the index.
        client.delete_by_query(index: index, body: { query: { match_all: {} } })
        @logger.debug("ELASTICSEARCH") { "Successfully cleared all documents from index #{index}" }
        true
      rescue Elasticsearch::Transport::Transport::Errors::NotFound => e
        @logger.warn("ELASTICSEARCH") { "Index #{index} does not exist, cannot clear documents." }
        false
      rescue StandardError => e
        @logger.error("ELASTICSEARCH") { "Failed to clear documents in index #{index}. Error: #{e.message}" }
        raise e
      end
    end

    # Gets a single document from an index by its ElasticSearch id.
    # Returns nil if the document cannot be found.
    #   - index: Index to retrieve the document from
    #   - id: ElasticSearch ID of the document
    def get_document(index, id)
      begin
        client.get(index: index, id: id)
      rescue Elasticsearch::Transport::Transport::Errors::NotFound => e
        @logger.debug("ELASTICSEARCH") { "Document #{id} not found in index #{index}" }
        nil
      rescue StandardError => e
        @logger.error("ELASTICSEARCH") { "Failed to get document #{id} from index #{index}.\n Error: #{e.full_message}" }
        raise e
      end
    end

    # Inserts a new document in an Elasticsearch index
    #   - index: Index to store the document in.
    #   - id: Elasticsearch identifier to store the document under.
    #   - document: document contents to index (as a ruby json object)
    # Returns the inserted document
    # Raises an error on failure.
    def insert_document(index, id, document)
      begin
        body = client.index(index: index, id: id, body: document)
        @logger.debug("ELASTICSEARCH") { "Inserted document #{id} in index #{index}" }
        body
      rescue StandardError => e
        @logger.error("ELASTICSEARCH") { "Failed to insert document #{id} in index #{index}.\n  Error: #{e.full_message}" }
        raise e
      end
    end

    # Partially updates an existing document in Elasticsearch index
    #   - index: Index to update the document in
    #   - id: ElasticSearch identifier of the document
    #   - document: New document contents
    # Returns the updated document or nil if the document cannot be found.
    # Otherwise, raises an error.
    def update_document(index, id, document)
      begin
        body = client.update(index: index, id: id, body: {doc: document})
        @logger.debug("ELASTICSEARCH") { "Updated document #{id} in index #{index}" }
        body
      rescue Elasticsearch::Transport::Transport::Errors::NotFound => e
        @logger.info("ELASTICSEARCH") { "Cannot update document #{id} in index #{index} because it doesn't exist" }
        nil
      rescue StandardError => e
        @logger.error("ELASTICSEARCH") { "Failed to update document #{id} in index #{index}.\n Error: #{e.full_message}" }
        raise e
      end
    end

    # Updates the document with the given id in the given index.
    # Inserts the document if it doesn't exist yet
    # - index: index to store document in
    # - id: elastic identifier to store the document under
    # - document: document contents (as a ruby json object)
    def upsert_document(index, id, document)
      @logger.debug("ELASTICSEARCH") { "Trying to update document with id #{id}" }
      updated_document = update_document index, id, document
      if updated_document.nil?
        @logger.debug("ELASTICSEARCH") { "Document #{id} does not exist yet, trying to insert new document" }
        insert_document index, id, document
      else
        updated_document
      end
    end

    # Deletes a document from an Elasticsearch index
    #   - index: Index to remove the document from
    #   - id: ElasticSearch identifier of the document
    # Returns true when the document existed and is succesfully deleted.
    # Otherwise false.
    # Throws an error if the document exists but fails to be deleted.
    def delete_document(index, id)
      begin
        client.delete(index: index, id: id)
        @logger.debug("ELASTICSEARCH") { "Successfully deleted document #{id} in index #{index}" }
        true
      rescue Elasticsearch::Transport::Transport::Errors::NotFound => e
        @logger.debug("ELASTICSEARCH") { "Document #{id} doesn't exist in index #{index} and cannot be deleted." }
        false
      rescue StandardError => e
        @logger.error("ELASTICSEARCH") { "Failed to dele document #{id} in index #{index}.\n Error: #{e.full_message}" }
        raise e
      end
    end

    # Searches for documents in the given indexes
    #   - indexes: Array of indexes to be searched
    #   - query: Elasticsearch query JSON object in ruby format
    def search_documents(indexes:, query: nil)
      begin
        @logger.debug("SEARCH") { "Searching Elasticsearch index(es) #{indexes} with body #{req_body}" }
        client.search(index: indexes, body: query)
      rescue Elasticsearch::Transport::Transport::Errors::BadRequest => e
        raise ArgumentError, "Invalid search query #{query}"
      rescue StandardError => e
        @logger.error("SEARCH") { "Searching documents in index(es) #{indexes} failed.\n Error: #{e.full_message}" }
        raise e
      end
    end

    # Counts search results for documents in the given indexex
    #   - indexes: Array of indexes to be searched
    #   - query: Elasticsearch query JSON object in ruby format
    def count_documents(indexes:, query: nil)
      begin
        @logger.debug("SEARCH") { "Count search results in index(es) #{indexes} for body #{req_body}" }
        puts query.inspect
        response = client.count(index: indexes, body: query)
        response["count"]
      rescue Elasticsearch::Transport::Transport::Errors::BadRequest => e
        @logger.error("SEARCH") { "Counting search results in index(es) #{indexes} failed.\n Error: #{e.full_message}" }
        raise ArgumentError, "Invalid count query #{query}"
      rescue StandardError => e
        @logger.error("SEARCH") { "Counting search results in index(es) #{indexes} failed.\n Error: #{e.full_message}" }
        raise e
      end
    end
  end
end
