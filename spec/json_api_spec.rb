require_relative '../lib/mu_search/json_api'

RSpec.describe MuSearch::JsonApi do
  describe '.extract_hits' do
    it 'returns hits from an Elasticsearch response' do
      results = { "hits" => { "hits" => [{ "_id" => "1" }, { "_id" => "2" }] } }
      expect(described_class.extract_hits(results)).to eq([{ "_id" => "1" }, { "_id" => "2" }])
    end

    it 'returns an empty array when results is an empty array' do
      expect(described_class.extract_hits([])).to eq([])
    end

    it 'returns an empty array when results is an empty hash' do
      expect(described_class.extract_hits({})).to eq([])
    end

    it 'returns an empty array when hits key is missing' do
      expect(described_class.extract_hits({ "aggregations" => {} })).to eq([])
    end

    it 'returns an empty array when inner hits is nil' do
      expect(described_class.extract_hits({ "hits" => {} })).to eq([])
    end
  end

  describe '.format_hits' do
    it 'uses uuid from _source as id when available' do
      hits = [
        { "_id" => "http://example.org/1", "_source" => { "uuid" => "abc-123", "title" => "Test" } }
      ]
      result = described_class.format_hits(hits, "documents")
      expect(result.first[:id]).to eq("abc-123")
    end

    it 'falls back to _id when uuid is not in _source' do
      hits = [
        { "_id" => "http://example.org/1", "_source" => { "title" => "Test" } }
      ]
      result = described_class.format_hits(hits, "documents")
      expect(result.first[:id]).to eq("http://example.org/1")
    end

    it 'includes the type on each entry' do
      hits = [
        { "_id" => "http://example.org/1", "_source" => { "uuid" => "abc" } }
      ]
      result = described_class.format_hits(hits, "documents")
      expect(result.first[:type]).to eq("documents")
    end

    it 'merges uri into attributes from _id' do
      hits = [
        { "_id" => "http://example.org/1", "_source" => { "uuid" => "abc", "title" => "Test" } }
      ]
      result = described_class.format_hits(hits, "documents")
      expect(result.first[:attributes]).to eq({
        "uuid" => "abc", "title" => "Test", uri: "http://example.org/1"
      })
    end

    it 'includes highlight when present' do
      hits = [
        {
          "_id" => "1",
          "_source" => { "uuid" => "abc" },
          "highlight" => { "title" => ["<em>match</em>"] }
        }
      ]
      result = described_class.format_hits(hits, "documents")
      expect(result.first[:highlight]).to eq({ "title" => ["<em>match</em>"] })
    end

    it 'sets highlight to nil when not present' do
      hits = [{ "_id" => "1", "_source" => { "uuid" => "abc" } }]
      result = described_class.format_hits(hits, "documents")
      expect(result.first[:highlight]).to be_nil
    end

    it 'formats multiple hits' do
      hits = [
        { "_id" => "uri1", "_source" => { "uuid" => "a" } },
        { "_id" => "uri2", "_source" => { "uuid" => "b" } }
      ]
      result = described_class.format_hits(hits, "documents")
      expect(result.length).to eq(2)
      expect(result.map { |r| r[:id] }).to eq(["a", "b"])
    end

    it 'returns an empty array for no hits' do
      expect(described_class.format_hits([], "documents")).to eq([])
    end
  end

  describe '.build_base_uri' do
    it 'strips page[number] from query string' do
      result = described_class.build_base_uri("/search", "filter[name]=foo&page[number]=2")
      expect(result).to eq("/search?filter[name]=foo")
    end

    it 'strips page[size] from query string' do
      result = described_class.build_base_uri("/search", "filter[name]=foo&page[size]=20")
      expect(result).to eq("/search?filter[name]=foo")
    end

    it 'strips both page params' do
      result = described_class.build_base_uri("/search", "page[number]=3&page[size]=10&filter[name]=foo")
      expect(result).to eq("/search?filter[name]=foo")
    end

    it 'preserves non-page query params' do
      result = described_class.build_base_uri("/search", "filter[title]=test&sort[name]=asc")
      expect(result).to eq("/search?filter[title]=test&sort[name]=asc")
    end

    it 'handles empty query string' do
      result = described_class.build_base_uri("/search", "")
      expect(result).to eq("/search?")
    end
  end

  describe '.join_uri_parts' do
    it 'joins non-empty parts with &' do
      result = described_class.join_uri_parts("/search?q=test", "page[number]=2", "page[size]=10")
      expect(result).to eq("/search?q=test&page[number]=2&page[size]=10")
    end

    it 'skips empty strings' do
      result = described_class.join_uri_parts("/search?q=test", "", "page[size]=10")
      expect(result).to eq("/search?q=test&page[size]=10")
    end

    it 'returns empty string when all parts are empty' do
      result = described_class.join_uri_parts("", "")
      expect(result).to eq("")
    end

    it 'returns the single part when only one is non-empty' do
      result = described_class.join_uri_parts("", "/search?", "")
      expect(result).to eq("/search?")
    end
  end

  describe '.page_uri' do
    let(:base) { "/search?filter[name]=foo" }

    it 'omits page[number] for page 0' do
      result = described_class.page_uri(base, 0, 10)
      expect(result).to eq("/search?filter[name]=foo&page[size]=10")
    end

    it 'includes page[number] for non-zero page' do
      result = described_class.page_uri(base, 3, 10)
      expect(result).to eq("/search?filter[name]=foo&page[number]=3&page[size]=10")
    end

    it 'always includes page[size]' do
      result = described_class.page_uri(base, 0, 20)
      expect(result).to include("page[size]=20")
    end
  end

  describe '.build_pagination_links' do
    let(:base_args) do
      {
        request_path: "/docs/search",
        query_string: "filter[title]=test",
        size: 10,
        count: 50
      }
    end

    it 'generates correct links for the first page' do
      links = described_class.build_pagination_links(**base_args, page: 0)
      expect(links[:self]).to eq("/docs/search?filter[title]=test&page[size]=10")
      expect(links[:first]).to eq("/docs/search?filter[title]=test&page[size]=10")
      expect(links[:prev]).to eq("/docs/search?filter[title]=test&page[size]=10")
      expect(links[:next]).to eq("/docs/search?filter[title]=test&page[number]=1&page[size]=10")
      expect(links[:last]).to eq("/docs/search?filter[title]=test&page[number]=5&page[size]=10")
    end

    it 'generates correct links for a middle page' do
      links = described_class.build_pagination_links(**base_args, page: 2)
      expect(links[:self]).to eq("/docs/search?filter[title]=test&page[number]=2&page[size]=10")
      expect(links[:prev]).to eq("/docs/search?filter[title]=test&page[number]=1&page[size]=10")
      expect(links[:next]).to eq("/docs/search?filter[title]=test&page[number]=3&page[size]=10")
    end

    it 'generates correct links for the last page' do
      links = described_class.build_pagination_links(**base_args, page: 5)
      expect(links[:self]).to eq("/docs/search?filter[title]=test&page[number]=5&page[size]=10")
      expect(links[:next]).to eq("/docs/search?filter[title]=test&page[number]=5&page[size]=10")
      expect(links[:last]).to eq("/docs/search?filter[title]=test&page[number]=5&page[size]=10")
    end

    it 'clamps prev to 0 on the first page' do
      links = described_class.build_pagination_links(**base_args, page: 0)
      expect(links[:prev]).to eq(links[:first])
    end

    it 'clamps next to last_page on the last page' do
      links = described_class.build_pagination_links(**base_args, page: 5)
      expect(links[:next]).to eq(links[:last])
    end

    it 'always includes page[size] in links' do
      links = described_class.build_pagination_links(**base_args, page: 0)
      links.each_value { |link| expect(link).to include("page[size]=10") }
    end
  end

  describe '.build_response' do
    let(:es_response) do
      {
        "hits" => {
          "hits" => [
            {
              "_id" => "http://example.org/resource/1",
              "_source" => { "uuid" => "uuid-1", "title" => "First" }
            },
            {
              "_id" => "http://example.org/resource/2",
              "_source" => { "uuid" => "uuid-2", "title" => "Second" },
              "highlight" => { "title" => ["<em>Second</em>"] }
            }
          ]
        }
      }
    end

    let(:base_args) do
      {
        type: "documents",
        request_path: "/documents/search",
        query_string: "filter[title]=test"
      }
    end

    it 'returns a hash with count, data, and links' do
      result = described_class.build_response(**base_args, count: 2, page: 0, size: 10, results: es_response)
      expect(result).to have_key(:count)
      expect(result).to have_key(:data)
      expect(result).to have_key(:links)
    end

    it 'sets count from the provided value' do
      result = described_class.build_response(**base_args, count: 42, page: 0, size: 10, results: es_response)
      expect(result[:count]).to eq(42)
    end

    it 'formats ES hits into data entries' do
      result = described_class.build_response(**base_args, count: 2, page: 0, size: 10, results: es_response)
      expect(result[:data].length).to eq(2)
      expect(result[:data].first[:id]).to eq("uuid-1")
      expect(result[:data].last[:highlight]).to eq({ "title" => ["<em>Second</em>"] })
    end

    it 'handles empty results passed as an empty array' do
      result = described_class.build_response(**base_args, count: 0, page: 0, size: 10, results: [])
      expect(result[:count]).to eq(0)
      expect(result[:data]).to eq([])
    end

    it 'handles empty results passed as an ES response with no hits' do
      empty = { "hits" => { "hits" => [] } }
      result = described_class.build_response(**base_args, count: 0, page: 0, size: 10, results: empty)
      expect(result[:count]).to eq(0)
      expect(result[:data]).to eq([])
    end

    it 'includes pagination links' do
      result = described_class.build_response(**base_args, count: 50, page: 2, size: 10, results: es_response)
      expect(result[:links][:self]).to include("page[number]=2")
      expect(result[:links][:first]).not_to include("page[number]")
      expect(result[:links][:last]).to include("page[number]=5")
    end
  end

  describe '#format_search_results' do
    let(:helper) do
      obj = Object.new
      obj.extend(described_class)

      request = double('request', path: '/docs/search', query_string: 'filter[q]=ruby')
      allow(obj).to receive(:request).and_return(request)
      obj
    end

    let(:es_response) do
      {
        "hits" => {
          "hits" => [
            { "_id" => "uri1", "_source" => { "uuid" => "a", "title" => "Ruby" } }
          ]
        }
      }
    end

    it 'delegates to build_response with request context' do
      result = helper.format_search_results("documents", 1, 0, 10, es_response)
      expect(result[:count]).to eq(1)
      expect(result[:data].first[:id]).to eq("a")
    end

    it 'uses request path and query string for links' do
      result = helper.format_search_results("documents", 50, 2, 10, es_response)
      expect(result[:links][:self]).to include("/docs/search?")
      expect(result[:links][:self]).to include("filter[q]=ruby")
      expect(result[:links][:self]).to include("page[number]=2")
    end

    it 'handles empty results' do
      result = helper.format_search_results("documents", 0, 0, 10, [])
      expect(result[:data]).to eq([])
    end
  end
end
