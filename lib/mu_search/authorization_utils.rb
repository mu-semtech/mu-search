module MuSearch
  # Utilities for working with mu-authorization groups.
  #
  # Designed for use as a Sinatra helpers module:
  #
  #   helpers MuSearch::AuthorizationUtils
  #
  # Route handlers get access to get_allowed_groups and
  # get_allowed_groups_with_fallback as instance methods.
  #
  # The pure functions (serialize/sort) are available as module methods
  # for use outside of Sinatra context, e.g.:
  #
  #   MuSearch::AuthorizationUtils.serialize_authorization_groups(groups)
  module AuthorizationUtils
    # Get the allowed groups from an incoming HTTP request.
    # Returns nil if they are not set.
    def get_allowed_groups
      allowed_groups_s = request.env["HTTP_MU_AUTH_ALLOWED_GROUPS"]
      if allowed_groups_s.nil? || allowed_groups_s.empty?
        nil
      else
        allowed_groups = JSON.parse(allowed_groups_s)
        AuthorizationUtils.sort_authorization_groups(allowed_groups)
      end
    end

    # Get the allowed groups from an incoming HTTP request
    # or calculate them by executing a query to the database
    # if they're not set yet.
    def get_allowed_groups_with_fallback
      allowed_groups = get_allowed_groups
      if allowed_groups.nil?
        # TODO: this isn't very clean and relies on ruby-template internals
        # - Send simple query to mu-auth
        Mu::query("ASK {?s ?p ?o}")
        # - Parse allowed groups from mu-ruby-template internals
        allowed_groups = JSON.parse(RequestStore.store[:mu_auth_allowed_groups])
        AuthorizationUtils.sort_authorization_groups(allowed_groups)
      else
        allowed_groups
      end
    end

    # Authorize the current request and return allowed groups.
    # Halts with a 401 error if authorization groups cannot be determined.
    #
    # @param with_fallback [Boolean] if true, falls back to querying
    #   the triplestore when no authorization header is present
    # @return [Array, nil] the allowed groups, or nil if no header
    #   is present and with_fallback is false
    def authorize!(with_fallback: false)
      groups = with_fallback ? get_allowed_groups_with_fallback : get_allowed_groups
      Mu::log.debug("AUTHORIZATION") { "Received allowed groups #{groups || 'none'}" }
      groups
    rescue StandardError => e
      Mu::log.error("AUTHORIZATION") { e.full_message }
      error("Unable to determine authorization groups", 401)
    end

    module_function

    # Returns a string representation for an authorization group.
    # E.g. { "name": "department", "variables": ["legal", "europe"] }
    #      will be serialized to "departmentlegaleurope"
    def serialize_authorization_group(group)
      group["name"] + group["variables"].join("")
    end

    # Returns a string representation for a list of authorization groups.
    # E.g. [
    #        { "name": "public", "variables": [] },
    #        { "name": "department", "variables": ["legal", "europe"] }
    #      ]
    #      will be serialized to "departmentlegaleurope#public"
    def serialize_authorization_groups(groups)
      groups.map { |group| serialize_authorization_group(group) }.sort.join("#")
    end

    # Sorts a given list of authorization groups.
    # E.g. [
    #        { "name": "public", "variables": [] },
    #        { "name": "admin", "variables": [] },
    #        { "name": "department", "variables": ["legal"] },
    #        { "name": "department", "variables": ["finance"] }
    #      ]
    # will become
    #      [
    #        { "name": "admin", "variables": [] },
    #        { "name": "department", "variables": ["finance"] },
    #        { "name": "department", "variables": ["legal"] },
    #        { "name": "public", "variables": [] }
    #      ]
    # Note: the list of variables in an authorization group
    #       is already ordered and should not be sorted alphabetically
    def sort_authorization_groups(groups)
      groups.sort_by { |group| serialize_authorization_group(group) }
    end
  end
end
