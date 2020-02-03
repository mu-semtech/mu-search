require 'set'

module MuSearch
  ##
  # the delta handler is a service for that parses deltas and triggers
  # the necessary updates on the search indexes.
  # Assumes that it is safe to remove objects for which the type was removed
  # updates documents for deltas that match the configured property paths
  class DeltaHandler

    ##
    # creates a delta handler
    #
    # raises an error if an invalid search config is provided
    def initialize(auto_index_updates: , logger:, search_configuration: )
      unless (search_configuration.is_a?(Hash) and search_configuration.has_key?("types"))
        raise ArgumentError.new("invalid search configuration")
      end
      @logger = logger
      @type_to_config_map = map_type_to_config(search_configuration)
      @property_to_config_map = map_property_to_config(search_configuration)
      @auto_index_updates = auto_index_updates
    end

    ##
    # determines which indexes are impacted by the delta and returns the configuration for those indexes
    def applicable_indexes_for_delta(subject:, predicate:, object:)
      predicate_value = predicate["value"]
      if predicate_value == RDF.type.to_s
        @type_to_config_map[object["value"]]
      else
        @property_to_config_map[predicate_value] + @property_to_config_map["^${predicate_value}"]
      end
    end

    ##
    # For an index config determines which root subjects are linked to the delta and returns those
    def find_subjects_for_delta(triple, config, addition = true)
      predicate = triple.dig("predicate", "value")
      # NOTE: current logic assumes rdf:type is never part of the property path
      if predicate == RDF.type.to_s
        [triple.dig("subject", "value")]
      else
        find_subjects_for_property(triple, config, addition)
      end
    end

    ##
    # queries the triplestore to find a subject related to the delta and path
    # this is a utility function called from fetch_subjects_for_property
    def query_for_path_and_delta_addition(index, delta, config, inverse)
        path = config[:rdf_properties].take(index+1) # path up to the added triple
        path_to_delta = MuSearch::SPARQL::make_predicate_string(path)
        properties_after_delta = path.slice(index+1, path.size)
        path_after_delta = properties_after_delta.size > 1 ? MuSearch::SPARQL::make_predicate_string(properties_after_delta) : false
        object = delta.dig("object", "type") == "uri" ? sparql_escape_uri(delta.dig("object", "value")) : delta.dig("object", "value").sparql_escape
        true_s = inverse ? object : delta.dig("subject", "value")

      rdf_types_turtle = sparql_escape_uri(config.dig(:index,"rdf_type"))
        sparql_query = %(
            SELECT ?s WHERE {
            ?s a #{rdf_types_turtle} .
            #{path_to_delta ? "?s #{path_to_delta} #{sparql_escape_uri(true_s)}." : "" }
            #{sparql_escape_uri(delta.dig("subject", "value"))} #{sparql_escape_uri(delta.dig("predicate","value"))} #{object}.
            #{path_after_delta ? "#{object} #{path_after_delta} ?foo." : "" }
            }
        )
        @logger.debug sparql_query
        query_result = MuSearch::SPARQL::direct_query(sparql_query)
    end

        ##
    # queries the triplestore to find a subject related to the delta and path
    # this is a utility function called from fetch_subjects_for_property
    def query_for_path_and_delta_removal(index, delta, config, inverse)
        path = config[:rdf_properties].take(index+1) # path up to the added triple
        path_to_delta = MuSearch::SPARQL::make_predicate_string(path)
        rdf_types_turtle = sparql_escape_uri(type)
        object = delta.dig("object", "type") == "uri" ? sparql_escape_uri(delta.dig("object", "value")) : delta.dig("object", "value").sparql_escape
        true_s = inverse ? object : delta.dig("subject", "value")

        sparql_query = %(
            SELECT ?s WHERE {
            ?s a #{rdf_types_turtle} .
            #{path_to_delta ? "?s #{path_to_delta} #{sparql_escape_uri(true_s)}." : "" }
            }
        )
        @logger.debug sparql_query
        query_result = MuSearch::SPARQL::direct_query(sparql_query)
    end

    ##
    # finds the property path related to the delta and fetches related subjects from the RDF store
    # TODO: this needs some for of cache
    def find_subjects_for_property(delta, config, addition)
      predicate = delta.dig("predicate", "value")
      subjects = []
      config[:rdf_properties].each_with_index do |property,index|
        if [predicate, "^#{predicate}"].include?(property)
          if index != config[:rdf_properties].length - 1 and delta.dig("object" , "type") != "uri"
            @logger.debug "discarding path because object is not a uri, but #{delta.dig("object", "type")}"
            # if we are not at the end of the path and the object is a literal
            return []
          else
            inverse = predicate != property
            query_result = addition ? query_for_path_and_delta_addition(index, delta, config, inverse) : query_for_path_and_delta_removal(index, delta, config, inverse)
            if query_result
              query_result.each {|result| subjects << result["s"]}
            end
          end
        end
      end
      subjects
    end

    ##
    # parses the body of a delta provided, assumes the 0.0.1 format as defined TODO
    # returns a set of documents to update
    def parse_deltas(deltas)
      docs_to_update = Hash.new { |hash, key| hash[key] = Set.new } # default value is a set
      docs_to_delete = Hash.new { |hash, key| hash[key] = Set.new }
      deltas.each do |delta|
        unless delta.is_a?(Hash)
          @logger.error "received delta does not seem to be in 0.0.1 format, mu-search requires delta format v0.0.1 "
        end
        delta["inserts"].uniq.each do |triple|
          applicable_indexes_for_delta({ subject: triple["subject"], predicate: triple["predicate"], object: triple["object"] }).each do |config|
            @logger.debug "matched config #{config.inspect}"
            find_subjects_for_delta(triple,config).each do |subject|
              @logger.debug "found subject #{subject}"
              docs_to_update[subject].add(config)
            end
          end
        end

        delta["deletes"].each do |triple|
          # for deletes the delete of a type triggers the delete of the document,
          # all other changes are considered an update
          applicable_indexes_for_delta({ subject: triple["subject"], predicate: triple["predicate"], object: triple["object"] }).each do |config|
            if triple.dig("predicate","value") == RDF.type.to_s
              subject = triple.dig("subject","value")
              docs_to_delete[subject].add(config)
            else
              find_subjects_for_delta(triple,config, false).each do |subject|
              docs_to_update[subject].add(config)
              end
            end
          end
        end
      end
      [docs_to_update, docs_to_delete]
    end

    ##
    # parses the search configuration and returns a map that links rdf types to their related indexes
    def map_type_to_config(search_configuration)
      index_config = search_configuration["types"]
      type_map = Hash.new{ |hash, key| hash[key] = Set.new } # has a set as default value for each key
      index_config.each do |config|
        # we assume composity types combine existing indexes.
        # so we only need "real" indexes when parsing deltas
        unless config.has_key?("composite_types")
          type_map[config["rdf_type"]].add(config)
        end
      end
      type_map
    end

    ##
    # parses the search configuration and returns a map that links rdf predicates to their related indexes
    def map_property_to_config(search_configuration)
      index_config = search_configuration["types"]
      property_map = Hash.new{ |hash, key| hash[key] = Set.new } # has a set as default value for each key
      # we assume composity types combine existing indexes.
      # so we only need "real" indexes when parsing deltas
      index_config.reject{ |config| config.has_key?("composite_tyes")}.each do |config|
        config["properties"].each do |key, value|
          if key == "data"
            value = value["via"]
          end
          if value.kind_of?(Array)
            value.each do |property|
              property_map[property].add({ index: config, rdf_properties: value })
            end
          else
            property_map[value].add({ index: config, rdf_properties: [ value ] })
          end
        end
      end
      property_map
    end
  end
end
