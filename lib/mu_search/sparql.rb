module MuSearch
  module SPARQL
    ##
    # Verifies whether or not the SPARQL endpoint is up.
    def self.up?
      begin
        sudo_query "ASK { ?s ?p ?o }", 1
      rescue StandardError => e
        false
      end
    end

    ##
    # provides a client with the given access rights
    # or the regular client with access rights from the end user if allowed_groups is empty
    def self.authorized_client allowed_groups
      if allowed_groups && allowed_groups.length > 0
        allowed_groups_s = allowed_groups.select { |group| group }.to_json
        sparql_options = { headers: { 'mu-auth-allowed-groups': allowed_groups_s } }
        ::SPARQL::Client.new(ENV['MU_SPARQL_ENDPOINT'], sparql_options)
      else
        SinatraTemplate::SPARQL::Client.new(ENV['MU_SPARQL_ENDPOINT'])
      end
    end

    ##
    # provides a client with sudo access
    def self.sudo_client
      sparql_options = { headers: { 'mu-auth-sudo': 'true' } }
      SinatraTemplate::SPARQL::Client.new(ENV['MU_SPARQL_ENDPOINT'], sparql_options)
    end

    ##
    # perform a query with access to all data
    def self.sudo_query(query_string, retries = 6)
      begin
        sudo_client.query query_string
      rescue StandardError => e
        next_retries = retries - 1
        if next_retries == 0
          raise e
        else
          log.warn "Could not execute raw query (attempt #{6 - next_retries}): #{query_string}"
          timeout = (6 - next_retries) ** 2
          sleep timeout
          sudo_query query_string, next_retries
        end
      end
    end

    ##
    # perform an update with access to all data
    def self.sudo_update(query_string, retries = 6)
      begin
        sudo_client.update query_string
      rescue StandardError => e
        next_retries = retries - 1
        if next_retries == 0
          raise e
        else
          log.warn "Could not execute raw query (attempt #{6 - next_retries}): #{query_string}"
          timeout = (6 - next_retries) ** 2
          sleep timeout
          sudo_update query_string, next_retries
        end
      end
    end

    # Converts the given predicate to an escaped predicate used in a SPARQL query.
    #
    # The string may start with a ^ sign to indicate inverse.
    # If that exists, we need to interpolate the URI.
    #
    #   - predicate: Predicate to be escaped.
    def self.predicate_string_term(predicate)
      if predicate.start_with? "^"
        "^#{sparql_escape_uri(predicate.slice(1,predicate.length))}"
      else
        sparql_escape_uri(predicate)
      end
    end

    # Converts the SPARQL predicate definition from the config into a
    # triple path.
    #
    # The configuration in the configuration file may contain an inverse
    # (using ^) and/or a list (using the array notation).  These need to
    # be converted into query paths so we can correctly fetch the
    # contents.
    #
    #   - predicate: Predicate definition as supplied in the config file.
    #     Either a string or an array.
    #
    # TODO: I believe the construction with the query paths leads to
    # incorrect invalidation when delta's arrive. Perhaps we should store
    # the relevant URIs in the stored document so we can invalidate it
    # correctly when new content arrives.
    def self.make_predicate_string(predicate)
      if predicate.is_a? String
        predicate_string_term(predicate)
      else
        predicate.map { |pred| predicate_string_term pred }.join("/")
      end
    end
  end
end
