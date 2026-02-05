require 'set'
require 'pp'

module MuSearch
  ##
  # the delta handler is a service that parses deltas and triggers
  # the necessary updates via the (index) update handler.
  # Assumes that it is safe to remove objects for which the type was removed
  # updates documents for deltas that match the configured property paths
  # NOTE: in theory the handler has a pretty good idea what has changed
  #       it may be possible to have finer grained updates on es documents than we currently have
  class DeltaHandler
    MU_UUID_PREDICATE = "http://mu.semte.ch/vocabularies/core/uuid".freeze
    RDF_TYPE_PREDICATE = RDF.type.to_s.freeze
    DEFAULT_DELTA_BATCH_SIZE = 100

    ##
    # creates a delta handler
    #
    # raises an error if an invalid search config is provided
    def initialize(logger:, search_configuration:, update_handler:)
      @logger = logger
      @type_definitions = search_configuration[:type_definitions]
      @update_handler = update_handler
      @delta_batch_size = search_configuration[:delta_batch_size] || DEFAULT_DELTA_BATCH_SIZE
      # FIFO queue of deltas
      @queue = []
      @mutex = Mutex.new
      setup_runner
    end

    # Setup a runner per thread to handle updates
    def setup_runner
      @runner = Thread.new(abort_on_exception: true) do
        @logger.info("DELTA") { "Runner ready for duty" }
        loop do
          triple = delta = resource_configs = nil
          begin
            @mutex.synchronize do
              if @queue.length > 0
                delta = @queue.shift
              end
            end
            if delta
              triples = delta[:triples]
              resource_configs = delta[:resource_configs]
              handle_queue_entry(triples, resource_configs)
            end
          rescue StandardError => e
            @logger.error("DELTA") { "Failed processing delta #{delta.pretty_inspect}" }
            @logger.error("DELTA") { e.full_message }
          end
          sleep 0.05
        end
      end
    end

    ##
    # Parses the given delta and adds it to the queue to trigger the update of affected documents
    # Assumes delta format v0.0.1
    def handle_deltas(deltas)
      @logger.debug("DELTA") { "Received delta update #{deltas.pretty_inspect}" }
      if deltas.is_a?(Array)
        @logger.debug("DELTA") { "Delta contains #{deltas.length} changesets" }
        triples = []
        deltas.each do |changeset|
          triples.concat(changeset["inserts"].map { |triple| triple.merge({ "is_addition" => true }) })
          triples.concat(changeset["deletes"].map { |triple| triple.merge({ "is_addition" => false }) })
        end
        # Filter out mu:uuid triples - they match all type configs but don't
        # carry useful information for subject discovery
        triples.reject! { |triple| triple["predicate"]["value"] == MU_UUID_PREDICATE }
        return if triples.empty?
        find_config_and_queue_delta(triples)
      else
        @logger.error("DELTA") { "Received delta does not seem to be in v0.0.1 format. Mu-search currently only supports delta format v0.0.1 " }
        @logger.error("DELTA") { deltas.pretty_inspect }
      end
    end


    private

    ##
    # Find the affected indexes for a given changeset and add it to the queue
    #
    def find_config_and_queue_delta(triples)
      @logger.debug("DELTA") { "Handling delta: #{triples.inspect}" }
      search_configs = Set.new
      triples.each do |triple|
        search_configs.merge(applicable_index_configurations_for_triple(triple))
      end
      type_names = search_configs.map(&:name)
      @logger.debug("DELTA") { "Delta affects #{type_names.length} search indexes: #{type_names.join(', ')}" }

      @mutex.synchronize do
        @queue << { triples: triples, resource_configs: search_configs }
      end
    end

    ##
    # Processes a queued delta entry using the batched VALUES pipeline:
    #   Phase 1: collect known subjects directly from rdf:type triples (no SPARQL)
    #   Phase 2: group remaining triples by query shape
    #   Phase 3: execute batched VALUES queries per group
    #   Phase 4: trigger updates for all discovered subjects
    def handle_queue_entry(triples, resource_configs)
      resource_configs.each do |config|
        subjects = Set.new

        # Phase 1: collect known subjects from rdf:type triples
        known_subjects = collect_known_subjects(triples, config)
        subjects.merge(known_subjects)

        # Phase 2: group remaining triples by query shape
        groups = group_triples_by_query_shape(triples, config, known_subjects)

        # Phase 3: batch execute queries
        groups.each do |shape_key, group_triples|
          group_triples.each_slice(@delta_batch_size) do |batch|
            batch_subjects = execute_values_query(batch, config, shape_key)
            subjects.merge(batch_subjects)
          end
        end

        # Phase 4: trigger updates
        if subjects.size > 0
          type_name = config.name
          @logger.debug("DELTA") { "Found #{subjects.length} subjects for resource config '#{type_name}' that needs to be updated." }
          subjects.each { |subject| @update_handler.add_update(subject, type_name) }
        end
      end
    end

    ##
    # Find index configs that are impacted by the given triple,
    # i.e. the object is an rdf:Class that is configured as search index
    #      or the predicate is included in one of the property (paths) of a search index.
    # Returns a set of impacted search configs.
    # Each config contains keys :type_name, :rdf_types, :rdf_properties
    def applicable_index_configurations_for_triple(triple)
      predicate = triple["predicate"]["value"]
      if predicate == RDF_TYPE_PREDICATE
        rdf_type = triple["object"]["value"]
        @type_definitions.select { |name, definition| definition.matches_type?(rdf_type) }.values
      else
        @type_definitions.select { |name, definition| definition.matches_property?(predicate) }.values
      end
    end

    ##
    # Returns a Set of subject URIs from rdf:type triples whose object
    # matches one of the config's related_rdf_types.
    # These subjects are already known without any SPARQL round-trip.
    def collect_known_subjects(triples, config)
      known = Set.new
      rdf_types = config.related_rdf_types
      triples.each do |triple|
        if triple["predicate"]["value"] == RDF_TYPE_PREDICATE && rdf_types.include?(triple["object"]["value"])
          known.add(triple["subject"]["value"])
        end
      end
      known
    end

    ##
    # Groups non-type/non-uuid triples by their query shape.
    # A shape key is [path, position, is_inverse, is_addition].
    # Skips position-0 non-inverse matches where the subject is already in known_subjects
    # (those were already identified in Phase 1).
    # Returns Hash{ shape_key => Array<triple> }.
    def group_triples_by_query_shape(triples, config, known_subjects)
      groups = Hash.new { |h, k| h[k] = [] }

      triples.each do |triple|
        predicate = triple["predicate"]["value"]
        object_type = triple["object"]["type"]
        is_addition = triple["is_addition"]

        # rdf:type triples are handled by collect_known_subjects
        # mu:uuid triples match all configs and don't help with subject discovery
        next if predicate == RDF_TYPE_PREDICATE || predicate == MU_UUID_PREDICATE

        matching_paths = config.full_property_paths_for(predicate)
        matching_paths.each do |path|
          path.each_with_index do |property, i|
            next unless predicate_matches_property?(predicate, property)

            inverse = is_inverse?(property)

            # Discard literal objects on non-terminal positions (can't traverse further)
            if (i < path.length - 1) && !inverse && (object_type != "uri")
              @logger.debug("DELTA") { "Discarding path because object is not a URI, but #{object_type}" }
              next
            end

            # Skip position-0 non-inverse matches where subject is already known from rdf:type
            if i == 0 && !inverse && known_subjects.include?(triple["subject"]["value"])
              next
            end

            shape_key = [path, i, inverse, is_addition]
            groups[shape_key] << triple
          end
        end
      end

      groups
    end

    ##
    # Formats a triple's object as a SPARQL term.
    # Handles URIs, language-tagged literals, datatyped literals, and plain literals.
    def format_object_term(triple_object)
      object_value = triple_object["value"]
      object_type = triple_object["type"]
      object_datatype = triple_object["datatype"]
      object_language = triple_object["xml:lang"]

      if object_type == "uri"
        Mu::sparql_escape_uri(object_value)
      elsif object_language
        %(#{object_value.sparql_escape}@#{object_language})
      elsif object_datatype
        %(#{object_value.sparql_escape}^^#{Mu::sparql_escape_uri(object_datatype)})
      else
        %(#{object_value.sparql_escape})
      end
    end

    ##
    # Formats each triple in a batch into a VALUES row (array of SPARQL terms).
    # The number and meaning of terms depend on the case:
    #   Deletion:                    [target_subject]
    #   Addition, non-inverse:       [target_subject, object_term]
    #   Addition, inverse:           [target_subject, triple_sub_term]
    def build_values_rows(batch, position, is_inverse, is_addition)
      batch.map do |triple|
        subject_value = triple["subject"]["value"]
        object_value = triple["object"]["value"]

        if !is_addition
          target_subject_term = is_inverse ? Mu::sparql_escape_uri(object_value) : Mu::sparql_escape_uri(subject_value)
          [target_subject_term]
        elsif !is_inverse
          target_subject_term = Mu::sparql_escape_uri(subject_value)
          object_term = format_object_term(triple["object"])
          [target_subject_term, object_term]
        else
          target_subject_term = Mu::sparql_escape_uri(object_value)
          triple_sub_term = Mu::sparql_escape_uri(subject_value)
          [target_subject_term, triple_sub_term]
        end
      end
    end

    ##
    # Builds a SPARQL SELECT query using a VALUES clause for batched subject discovery.
    # The query structure mirrors the original per-triple query but binds multiple
    # triples at once via VALUES.
    def build_values_sparql(rows, rdf_type_terms, path, position, is_inverse, is_addition)
      property_path_to_target = path.take(position)
      property_path_from_target = path.drop(position + 1)

      # Determine VALUES variable names based on the case
      if !is_addition
        vars = position == 0 ? "(?s)" : "(?target_sub)"
      elsif position == 0
        vars = is_inverse ? "(?s ?triple_sub)" : "(?s ?obj)"
      else
        vars = is_inverse ? "(?target_sub ?triple_sub)" : "(?target_sub ?obj)"
      end

      formatted_rows = rows.map { |row| "(#{row.join(' ')})" }.join(" ")

      sparql = "SELECT DISTINCT ?s WHERE {\n"
      sparql += "\t VALUES ?type { #{rdf_type_terms.join(' ')} } . \n"
      sparql += "\t VALUES #{vars} { #{formatted_rows} } \n"
      sparql += "\t ?s a ?type. \n"
      if position > 0
        path_to_target_term = MuSearch::SPARQL::make_predicate_string(property_path_to_target)
        sparql += "\t ?s #{path_to_target_term} ?target_sub . \n"
      end

      if is_addition
        # Determine the predicate URI (strip ^ for inverse properties)
        property = path[position]
        predicate_uri = is_inverse ? property[1..] : property
        predicate_term = Mu::sparql_escape_uri(predicate_uri)

        # Triple check pattern
        if position == 0
          if is_inverse
            sparql += "\t ?triple_sub #{predicate_term} ?s . \n"
          else
            sparql += "\t ?s #{predicate_term} ?obj . \n"
          end
        else
          if is_inverse
            sparql += "\t ?triple_sub #{predicate_term} ?target_sub . \n"
          else
            sparql += "\t ?target_sub #{predicate_term} ?obj . \n"
          end
        end

        # Path from target to end of property chain
        if property_path_from_target.length > 0
          path_from_target_term = MuSearch::SPARQL::make_predicate_string(property_path_from_target)
          if is_inverse
            sparql += "\t ?triple_sub #{path_from_target_term} ?foo. \n"
          else
            sparql += "\t ?obj #{path_from_target_term} ?foo. \n"
          end
        end
      end

      sparql += "}"
      sparql
    end

    ##
    # Executes a batched VALUES query for a group of triples sharing the same shape.
    # Returns an array of subject URI strings.
    def execute_values_query(batch, config, shape_key)
      path, position, is_inverse, is_addition = shape_key
      rdf_type_terms = config.related_rdf_types.map { |t| Mu::sparql_escape_uri(t) }

      rows = build_values_rows(batch, position, is_inverse, is_addition)
      sparql = build_values_sparql(rows, rdf_type_terms, path, position, is_inverse, is_addition)

      MuSearch::SPARQL::ConnectionPool.sudo_query(sparql).map { |result| result["s"].to_s }
    end

    # checks if a predicate or its inverse equals the property
    def predicate_matches_property?(predicate, property)
      [predicate, "^#{predicate}"].include?(property)
    end

    # check if the property is inverse
    def is_inverse?(property)
      property.start_with? "^"
    end
  end
end
