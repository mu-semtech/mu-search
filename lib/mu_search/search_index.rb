require 'concurrent'

module MuSearch
  class SearchIndex
    attr_reader :uri, :name, :type_name, :allowed_groups, :used_groups, :mutex, :status
    attr_accessor :is_eager_index
    def initialize(uri:, name:, type_name:, is_eager_index:, allowed_groups:, used_groups:)
      @uri = uri
      @name = name
      @is_eager_index = is_eager_index
      @type_name = type_name
      @allowed_groups = allowed_groups
      @used_groups = used_groups

      @status = :valid  # possible values: :valid, :invalid, :updating
      @mutex = Mutex.new
      @ready_event = Concurrent::Event.new
      @ready_event.set  # initially ready (not updating)
    end

    def status=(new_status)
      @status = new_status
      if new_status == :updating
        @ready_event.reset
      else
        @ready_event.set
      end
    end

    # Blocks until the index is no longer in :updating state.
    # Returns true if the index is ready, false if the timeout expired.
    def wait_until_ready(timeout: 60)
      @ready_event.wait(timeout)
    end

    def eager_index?
      @is_eager_index
    end

    def to_json(*args)
      {
        uri: uri,
        id: name,
        type: type_name,
        is_eager_index: @is_eager_index,
        allowed_groups: allowed_groups,
        status: status
      }.to_json(*args)
    end
  end
end
