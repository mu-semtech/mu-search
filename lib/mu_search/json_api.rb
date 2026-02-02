module MuSearch
  # Provides JSONAPI formatting for Elasticsearch search results.
  #
  # Designed for use as a Sinatra helpers module:
  #
  #   helpers MuSearch::JsonApi
  #
  # Route handlers call the instance method format_search_results directly,
  # which has access to the Sinatra request context.
  #
  # The underlying logic lives in module_function methods (build_response,
  # extract_hits, etc.) that are pure and testable without Sinatra.
  module JsonApi
    # Sinatra helper method - call from route handlers.
    # Extracts request context from Sinatra and delegates to build_response.
    def format_search_results(type, count, page, size, results)
      JsonApi.build_response(
        type: type,
        count: count,
        page: page,
        size: size,
        results: results,
        request_path: request.path,
        query_string: request.query_string
      )
    end

    module_function

    # Builds a JSONAPI-formatted response hash from Elasticsearch results.
    #
    # @param type [String] Type of the searched resource
    # @param count [Integer] Total number of available results
    # @param page [Integer] Current page number (0-indexed)
    # @param size [Integer] Number of results per page
    # @param results [Hash, Array] Elasticsearch response hash, or empty array for no results
    # @param request_path [String] The request path (e.g. "/documents/search")
    # @param query_string [String] The raw query string from the request
    # @return [Hash] Response with :count, :data, and :links keys
    def build_response(type:, count:, page:, size:, results:,
                       request_path:, query_string:)
      hits = extract_hits(results)

      {
        count: count,
        data: format_hits(hits, type),
        links: build_pagination_links(
          request_path: request_path,
          query_string: query_string,
          page: page,
          size: size,
          count: count
        )
      }
    end

    # Extracts the hits array from an Elasticsearch response.
    # Returns an empty array if results is not a valid ES response hash.
    def extract_hits(results)
      return [] unless results.is_a?(Hash)
      results.dig("hits", "hits") || []
    end

    # Formats individual ES hits into JSONAPI data entries.
    def format_hits(hits, type)
      hits.map do |hit|
        uuid = hit.dig("_source", "uuid") || hit["_id"]
        {
          type: type,
          id: uuid,
          attributes: hit["_source"].merge({ uri: hit["_id"] }),
          highlight: hit["highlight"]
        }
      end
    end

    # Builds pagination links for the JSONAPI response.
    def build_pagination_links(request_path:, query_string:, page:, size:, count:)
      last_page = count / size
      base_uri = build_base_uri(request_path, query_string)

      {
        self:  page_uri(base_uri, page, size),
        first: page_uri(base_uri, 0, size),
        last:  page_uri(base_uri, last_page, size),
        prev:  page_uri(base_uri, [page - 1, 0].max, size),
        next:  page_uri(base_uri, [page + 1, last_page].min, size)
      }
    end

    # Builds a URI string for a specific page of results.
    def page_uri(base_uri, page, size)
      parts = [base_uri]
      parts << "page[number]=#{page}" unless page == 0
      parts << "page[size]=#{size}"
      join_uri_parts(*parts)
    end

    # Strips existing page parameters from the query string
    # and combines it with the path to form the base URI.
    def build_base_uri(request_path, query_string)
      cleaned = query_string.gsub(/&?page\[(number|size)\]=[0-9]+/, '')
      cleaned = cleaned.sub(/\A&/, '')
      request_path + '?' + cleaned
    end

    # Joins URI parts with '&', skipping empty strings.
    def join_uri_parts(*parts)
      parts.reject(&:empty?).join('&')
    end
  end
end
