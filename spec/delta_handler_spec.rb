require 'json'
require 'set'
require 'logger'

# Framework stubs (normally provided by semtech/mu-jruby-template Docker image)
module RDF
  class URI
    def initialize(uri)
      @uri = uri
    end

    def to_s
      @uri
    end
  end

  def self.type
    @type_uri ||= URI.new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
  end
end

module Mu
  def self.sparql_escape_uri(uri)
    "<#{uri}>"
  end

  def self.log
    @log ||= ::Logger.new($stdout, level: ::Logger::WARN)
  end
end

class String
  unless method_defined?(:sparql_escape)
    def sparql_escape
      %("""#{self}""")
    end
  end
end

# Minimal SPARQL module stubs (avoids requiring connection_pool gem)
module MuSearch
  module SPARQL
    def self.predicate_string_term(predicate)
      if predicate.start_with?("^")
        "^<#{predicate[1..]}>"
      else
        "<#{predicate}>"
      end
    end

    def self.make_predicate_string(predicate)
      if predicate.is_a?(String)
        predicate_string_term(predicate)
      else
        predicate.map { |pred| predicate_string_term(pred) }.join("/")
      end
    end

    class ConnectionPool
      def self.sudo_query(query_string)
        []
      end
    end
  end
end

require_relative '../lib/mu_search/delta_handler'
require_relative '../lib/mu_search/index_definition'

RSpec.describe MuSearch::DeltaHandler do
  # Test configuration: a "session" type with various property path shapes
  let(:json_config) do
    JSON.parse(<<~JSON)
      [
        {
          "type": "session",
          "on_path": "sessions",
          "rdf_type": "http://example.org/Session",
          "properties": {
            "title": "http://example.org/title",
            "author_name": ["http://example.org/author", "http://example.org/name"],
            "parent": "^http://example.org/hasChild"
          }
        }
      ]
    JSON
  end

  let(:type_definitions) { Hash[MuSearch::IndexDefinition.from_json_config(json_config)] }
  let(:logger) { double('Logger', debug: nil, info: nil, warn: nil, error: nil) }
  let(:update_handler) { double('UpdateHandler', add_update: nil) }

  # Build the handler but suppress the background runner thread
  let(:handler) do
    h = MuSearch::DeltaHandler.allocate
    h.instance_variable_set(:@logger, logger)
    h.instance_variable_set(:@type_definitions, type_definitions)
    h.instance_variable_set(:@update_handler, update_handler)
    h.instance_variable_set(:@delta_batch_size, 100)
    h.instance_variable_set(:@queue, [])
    h.instance_variable_set(:@mutex, Mutex.new)
    h
  end

  let(:session_config) { type_definitions["session"] }

  # Helper to build triple hashes
  def make_triple(subject:, predicate:, object_value:, object_type: "uri", is_addition: true, datatype: nil, language: nil)
    obj = { "value" => object_value, "type" => object_type }
    obj["datatype"] = datatype if datatype
    obj["xml:lang"] = language if language
    {
      "subject" => { "value" => subject, "type" => "uri" },
      "predicate" => { "value" => predicate, "type" => "uri" },
      "object" => obj,
      "is_addition" => is_addition
    }
  end

  describe '#collect_known_subjects' do
    it 'returns subjects from rdf:type triples matching the config' do
      triples = [
        make_triple(
          subject: "http://example.org/s1",
          predicate: "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
          object_value: "http://example.org/Session"
        )
      ]
      result = handler.send(:collect_known_subjects, triples, session_config)
      expect(result).to eq(Set.new(["http://example.org/s1"]))
    end

    it 'ignores rdf:type triples with non-matching types' do
      triples = [
        make_triple(
          subject: "http://example.org/s1",
          predicate: "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
          object_value: "http://example.org/OtherType"
        )
      ]
      result = handler.send(:collect_known_subjects, triples, session_config)
      expect(result).to be_empty
    end

    it 'deduplicates subjects' do
      triples = [
        make_triple(
          subject: "http://example.org/s1",
          predicate: "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
          object_value: "http://example.org/Session"
        ),
        make_triple(
          subject: "http://example.org/s1",
          predicate: "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
          object_value: "http://example.org/Session",
          is_addition: false
        )
      ]
      result = handler.send(:collect_known_subjects, triples, session_config)
      expect(result.size).to eq(1)
    end
  end

  describe '#group_triples_by_query_shape' do
    let(:known_subjects) { Set.new(["http://example.org/s1"]) }

    it 'groups mu:uuid triples like regular properties' do
      triples = [
        make_triple(
          subject: "http://example.org/s1",
          predicate: "http://mu.semte.ch/vocabularies/core/uuid",
          object_value: "some-uuid",
          object_type: "literal"
        )
      ]
      groups = handler.send(:group_triples_by_query_shape, triples, session_config, Set.new)
      expect(groups).not_to be_empty
    end

    it 'skips mu:uuid triples when subject is already known' do
      triples = [
        make_triple(
          subject: "http://example.org/s1",
          predicate: "http://mu.semte.ch/vocabularies/core/uuid",
          object_value: "some-uuid",
          object_type: "literal"
        )
      ]
      groups = handler.send(:group_triples_by_query_shape, triples, session_config, known_subjects)
      expect(groups).to be_empty
    end

    it 'filters out rdf:type triples' do
      triples = [
        make_triple(
          subject: "http://example.org/s1",
          predicate: "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
          object_value: "http://example.org/Session"
        )
      ]
      groups = handler.send(:group_triples_by_query_shape, triples, session_config, Set.new)
      expect(groups).to be_empty
    end

    it 'skips position-0 non-inverse matches for known subjects' do
      triples = [
        make_triple(
          subject: "http://example.org/s1",
          predicate: "http://example.org/title",
          object_value: "Hello",
          object_type: "literal"
        )
      ]
      groups = handler.send(:group_triples_by_query_shape, triples, session_config, known_subjects)
      expect(groups).to be_empty
    end

    it 'keeps position-0 non-inverse matches for unknown subjects' do
      triples = [
        make_triple(
          subject: "http://example.org/s2",
          predicate: "http://example.org/title",
          object_value: "Hello",
          object_type: "literal"
        )
      ]
      groups = handler.send(:group_triples_by_query_shape, triples, session_config, known_subjects)
      expect(groups.size).to eq(1)
      key = groups.keys.first
      expect(key[1]).to eq(0) # position 0
      expect(key[2]).to eq(false) # not inverse
    end

    it 'does NOT skip inverse position-0 even if subject is known' do
      triples = [
        make_triple(
          subject: "http://example.org/child1",
          predicate: "http://example.org/hasChild",
          object_value: "http://example.org/s1"
        )
      ]
      groups = handler.send(:group_triples_by_query_shape, triples, session_config, known_subjects)
      expect(groups.size).to eq(1)
      key = groups.keys.first
      expect(key[2]).to eq(true) # is_inverse
    end

    it 'groups triples sharing the same shape key' do
      triples = [
        make_triple(
          subject: "http://example.org/s2",
          predicate: "http://example.org/title",
          object_value: "Title A",
          object_type: "literal"
        ),
        make_triple(
          subject: "http://example.org/s3",
          predicate: "http://example.org/title",
          object_value: "Title B",
          object_type: "literal"
        )
      ]
      groups = handler.send(:group_triples_by_query_shape, triples, session_config, Set.new)
      expect(groups.size).to eq(1)
      expect(groups.values.first.size).to eq(2)
    end

    it 'discards literal objects on non-terminal positions' do
      triples = [
        make_triple(
          subject: "http://example.org/s2",
          predicate: "http://example.org/author",
          object_value: "a literal",
          object_type: "literal"
        )
      ]
      groups = handler.send(:group_triples_by_query_shape, triples, session_config, Set.new)
      # position 0 in the author_name path should be discarded (non-terminal, literal object)
      # but the same predicate at position 0 in its own single-step path is fine only if it's terminal
      # Actually, http://example.org/author is at position 0 in ["http://example.org/author", "http://example.org/name"]
      # which is non-terminal, so it should be discarded for that path.
      # It should NOT match any other path since there's no single-predicate "author" property.
      expect(groups).to be_empty
    end

    it 'creates separate groups for addition and deletion of same shape' do
      triples = [
        make_triple(
          subject: "http://example.org/s2",
          predicate: "http://example.org/title",
          object_value: "Old",
          object_type: "literal",
          is_addition: false
        ),
        make_triple(
          subject: "http://example.org/s2",
          predicate: "http://example.org/title",
          object_value: "New",
          object_type: "literal",
          is_addition: true
        )
      ]
      groups = handler.send(:group_triples_by_query_shape, triples, session_config, Set.new)
      expect(groups.size).to eq(2)
    end
  end

  describe '#format_object_term' do
    it 'formats a URI object' do
      obj = { "value" => "http://example.org/thing", "type" => "uri" }
      expect(handler.send(:format_object_term, obj)).to eq("<http://example.org/thing>")
    end

    it 'formats a language-tagged literal' do
      obj = { "value" => "hello", "type" => "literal", "xml:lang" => "en" }
      expect(handler.send(:format_object_term, obj)).to eq(%("""hello"""@en))
    end

    it 'formats a datatyped literal' do
      obj = { "value" => "42", "type" => "literal", "datatype" => "http://www.w3.org/2001/XMLSchema#integer" }
      expect(handler.send(:format_object_term, obj)).to eq(%("""42"""^^<http://www.w3.org/2001/XMLSchema#integer>))
    end

    it 'formats a plain literal' do
      obj = { "value" => "plain text", "type" => "literal" }
      expect(handler.send(:format_object_term, obj)).to eq(%("""plain text"""))
    end
  end

  describe '#build_values_sparql' do
    let(:rdf_type_terms) { ["<http://example.org/Session>"] }

    context 'deletion at position 0' do
      it 'uses VALUES (?s) without triple check' do
        rows = [["<http://example.org/s1>"]]
        path = ["http://example.org/title"]
        sparql = handler.send(:build_values_sparql, rows, rdf_type_terms, path, 0, false, false)

        expect(sparql).to include("VALUES (?s)")
        expect(sparql).to include("(<http://example.org/s1>)")
        expect(sparql).to include("?s a ?type")
        expect(sparql).not_to include("?target_sub")
        # No triple check for deletions
        expect(sparql).not_to include("<http://example.org/title>")
      end
    end

    context 'deletion at position > 0' do
      it 'uses VALUES (?target_sub) with path_to_target' do
        rows = [["<http://example.org/author1>"]]
        path = ["http://example.org/author", "http://example.org/name"]
        sparql = handler.send(:build_values_sparql, rows, rdf_type_terms, path, 1, false, false)

        expect(sparql).to include("VALUES (?target_sub)")
        expect(sparql).to include("?s <http://example.org/author> ?target_sub")
        expect(sparql).not_to include("<http://example.org/name>")
      end
    end

    context 'addition at position 0, non-inverse' do
      it 'uses VALUES (?s ?obj) with triple check' do
        rows = [["<http://example.org/s1>", %("Hello")]]
        path = ["http://example.org/title"]
        sparql = handler.send(:build_values_sparql, rows, rdf_type_terms, path, 0, false, true)

        expect(sparql).to include("VALUES (?s ?obj)")
        expect(sparql).to include("?s <http://example.org/title> ?obj")
      end
    end

    context 'addition at position 0, inverse' do
      it 'uses VALUES (?s ?triple_sub) with inverse triple check' do
        rows = [["<http://example.org/parent1>", "<http://example.org/child1>"]]
        path = ["^http://example.org/hasChild"]
        sparql = handler.send(:build_values_sparql, rows, rdf_type_terms, path, 0, true, true)

        expect(sparql).to include("VALUES (?s ?triple_sub)")
        expect(sparql).to include("?triple_sub <http://example.org/hasChild> ?s")
      end
    end

    context 'addition at position > 0, non-inverse' do
      it 'uses VALUES (?target_sub ?obj) with path_to_target and triple check' do
        rows = [["<http://example.org/author1>", %("John")]]
        path = ["http://example.org/author", "http://example.org/name"]
        sparql = handler.send(:build_values_sparql, rows, rdf_type_terms, path, 1, false, true)

        expect(sparql).to include("VALUES (?target_sub ?obj)")
        expect(sparql).to include("?s <http://example.org/author> ?target_sub")
        expect(sparql).to include("?target_sub <http://example.org/name> ?obj")
      end
    end

    context 'addition at position > 0, inverse' do
      # Use a hypothetical path for this test
      it 'uses VALUES (?target_sub ?triple_sub) with path_to_target and inverse triple check' do
        rows = [["<http://example.org/target1>", "<http://example.org/sub1>"]]
        path = ["http://example.org/link", "^http://example.org/ref"]
        sparql = handler.send(:build_values_sparql, rows, rdf_type_terms, path, 1, true, true)

        expect(sparql).to include("VALUES (?target_sub ?triple_sub)")
        expect(sparql).to include("?s <http://example.org/link> ?target_sub")
        expect(sparql).to include("?triple_sub <http://example.org/ref> ?target_sub")
      end
    end

    context 'addition with path_from_target' do
      it 'includes path_from_target for non-inverse' do
        rows = [["<http://example.org/s1>", "<http://example.org/author1>"]]
        path = ["http://example.org/author", "http://example.org/name"]
        sparql = handler.send(:build_values_sparql, rows, rdf_type_terms, path, 0, false, true)

        expect(sparql).to include("?obj <http://example.org/name> ?foo")
      end

      it 'includes path_from_target via ?triple_sub for inverse' do
        rows = [["<http://example.org/obj1>", "<http://example.org/sub1>"]]
        path = ["^http://example.org/hasChild", "http://example.org/name"]
        sparql = handler.send(:build_values_sparql, rows, rdf_type_terms, path, 0, true, true)

        expect(sparql).to include("?triple_sub <http://example.org/name> ?foo")
      end
    end

    it 'formats multiple rows in VALUES clause' do
      rows = [["<http://example.org/s1>"], ["<http://example.org/s2>"]]
      path = ["http://example.org/title"]
      sparql = handler.send(:build_values_sparql, rows, rdf_type_terms, path, 0, false, false)

      expect(sparql).to include("(<http://example.org/s1>) (<http://example.org/s2>)")
    end
  end

  describe '#build_values_rows' do
    it 'returns single-element rows for deletions' do
      triples = [
        make_triple(subject: "http://example.org/s1", predicate: "http://example.org/title",
                    object_value: "Hello", object_type: "literal", is_addition: false)
      ]
      rows = handler.send(:build_values_rows, triples, 0, false, false)
      expect(rows).to eq([["<http://example.org/s1>"]])
    end

    it 'returns [target_subject, object_term] for non-inverse additions' do
      triples = [
        make_triple(subject: "http://example.org/s1", predicate: "http://example.org/title",
                    object_value: "Hello", object_type: "literal", is_addition: true)
      ]
      rows = handler.send(:build_values_rows, triples, 0, false, true)
      expect(rows).to eq([["<http://example.org/s1>", %("""Hello""")]])
    end

    it 'returns [target_subject, triple_sub] for inverse additions' do
      triples = [
        make_triple(subject: "http://example.org/child1", predicate: "http://example.org/hasChild",
                    object_value: "http://example.org/parent1", is_addition: true)
      ]
      rows = handler.send(:build_values_rows, triples, 0, true, true)
      # For inverse: target_subject = object_value, triple_sub = subject_value
      expect(rows).to eq([["<http://example.org/parent1>", "<http://example.org/child1>"]])
    end

    it 'uses object for target_subject in inverse deletions' do
      triples = [
        make_triple(subject: "http://example.org/child1", predicate: "http://example.org/hasChild",
                    object_value: "http://example.org/parent1", is_addition: false)
      ]
      rows = handler.send(:build_values_rows, triples, 0, true, false)
      expect(rows).to eq([["<http://example.org/parent1>"]])
    end
  end

  describe '#handle_queue_entry' do
    it 'identifies subjects from rdf:type triples without SPARQL' do
      triples = [
        make_triple(
          subject: "http://example.org/s1",
          predicate: "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
          object_value: "http://example.org/Session"
        )
      ]

      # No SPARQL queries should be made
      expect(MuSearch::SPARQL::ConnectionPool).not_to receive(:sudo_query)
      expect(update_handler).to receive(:add_update).with("http://example.org/s1", "session")

      handler.send(:handle_queue_entry, triples, [session_config])
    end

    it 'uses batched VALUES query for non-type property triples' do
      triples = [
        make_triple(
          subject: "http://example.org/s2",
          predicate: "http://example.org/title",
          object_value: "Test",
          object_type: "literal"
        )
      ]

      mock_result = double('result')
      allow(mock_result).to receive(:[]).with("s").and_return(RDF::URI.new("http://example.org/s2"))
      expect(MuSearch::SPARQL::ConnectionPool).to receive(:sudo_query).once.and_return([mock_result])
      expect(update_handler).to receive(:add_update).with("http://example.org/s2", "session")

      handler.send(:handle_queue_entry, triples, [session_config])
    end

    it 'skips SPARQL for position-0 properties when subject is known from rdf:type' do
      triples = [
        make_triple(
          subject: "http://example.org/s1",
          predicate: "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
          object_value: "http://example.org/Session"
        ),
        make_triple(
          subject: "http://example.org/s1",
          predicate: "http://example.org/title",
          object_value: "Test",
          object_type: "literal"
        )
      ]

      # The title triple at position 0 should be skipped since s1 is already known
      expect(MuSearch::SPARQL::ConnectionPool).not_to receive(:sudo_query)
      expect(update_handler).to receive(:add_update).with("http://example.org/s1", "session")

      handler.send(:handle_queue_entry, triples, [session_config])
    end

    it 'still queries for position > 0 properties even when subject is known' do
      triples = [
        make_triple(
          subject: "http://example.org/s1",
          predicate: "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
          object_value: "http://example.org/Session"
        ),
        make_triple(
          subject: "http://example.org/s1",
          predicate: "http://example.org/name",
          object_value: "John",
          object_type: "literal"
        )
      ]

      # name is at position 1 in ["author", "name"] path, so it should still query
      mock_result = double('result')
      allow(mock_result).to receive(:[]).with("s").and_return(RDF::URI.new("http://example.org/s1"))
      expect(MuSearch::SPARQL::ConnectionPool).to receive(:sudo_query).once.and_return([mock_result])
      expect(update_handler).to receive(:add_update).with("http://example.org/s1", "session")

      handler.send(:handle_queue_entry, triples, [session_config])
    end

    it 'chunks large groups according to delta_batch_size' do
      handler.instance_variable_set(:@delta_batch_size, 2)

      triples = (1..5).map do |i|
        make_triple(
          subject: "http://example.org/s#{i}",
          predicate: "http://example.org/title",
          object_value: "Title #{i}",
          object_type: "literal"
        )
      end

      # 5 triples / batch_size 2 = 3 queries (2 + 2 + 1)
      expect(MuSearch::SPARQL::ConnectionPool).to receive(:sudo_query).exactly(3).times.and_return([])

      handler.send(:handle_queue_entry, triples, [session_config])
    end

    it 'does not call update_handler when no subjects found' do
      triples = [
        make_triple(
          subject: "http://example.org/s1",
          predicate: "http://example.org/unknownProp",
          object_value: "data",
          object_type: "literal"
        )
      ]

      expect(update_handler).not_to receive(:add_update)

      handler.send(:handle_queue_entry, triples, [session_config])
    end
  end

  describe '#handle_deltas' do
    it 'queues mu:uuid triples so updates are triggered when uuid is added separately' do
      deltas = [
        {
          "inserts" => [
            {
              "subject" => { "value" => "http://example.org/s1", "type" => "uri" },
              "predicate" => { "value" => "http://mu.semte.ch/vocabularies/core/uuid", "type" => "uri" },
              "object" => { "value" => "some-uuid", "type" => "literal" }
            }
          ],
          "deletes" => []
        }
      ]

      handler.handle_deltas(deltas)
      queue = handler.instance_variable_get(:@queue)
      expect(queue).not_to be_empty
    end

    it 'queues non-uuid triples with their affected configs' do
      deltas = [
        {
          "inserts" => [
            {
              "subject" => { "value" => "http://example.org/s1", "type" => "uri" },
              "predicate" => { "value" => "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "type" => "uri" },
              "object" => { "value" => "http://example.org/Session", "type" => "uri" }
            }
          ],
          "deletes" => []
        }
      ]

      handler.handle_deltas(deltas)
      queue = handler.instance_variable_get(:@queue)
      expect(queue.size).to eq(1)
      expect(queue.first[:triples].size).to eq(1)
      expect(queue.first[:resource_configs].map(&:name)).to include("session")
    end
  end
end
