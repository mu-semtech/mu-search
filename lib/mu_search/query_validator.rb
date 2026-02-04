module MuSearch
  # Validates raw Elasticsearch Query DSL input from untrusted sources.
  #
  # Designed for use as a Sinatra helpers module:
  #
  #   helpers MuSearch::QueryValidator
  #
  # Prevents abuse of Elasticsearch features like Terms Lookup
  # that could allow cross-index data access, bypassing authorization.
  module QueryValidator
    # Validates a raw query body and halts with 400 if it contains
    # disallowed properties.
    def validate_raw_query!(query)
      if QueryValidator.contains_index_key?(query)
        halt 400, { "errors" => [{ "title" => "Request body must not contain an 'index' property" }] }.to_json
      end
    end

    module_function

    def contains_index_key?(obj)
      case obj
      when Hash
        return true if obj.key?("index")
        obj.values.any? { |v| contains_index_key?(v) }
      when Array
        obj.any? { |v| contains_index_key?(v) }
      else
        false
      end
    end
  end
end
