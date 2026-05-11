require_relative '../lib/mu_search/document_builder.rb'

RSpec.describe MuSearch::DocumentBuilder do
  let(:logger) { double('Logger', debug: nil, warn: nil, info: nil) }
  let(:sparql_client) { double('SparqlClient') }
  let(:attachment_path_base) { '/tmp/attachments' }

  subject(:builder) do
    described_class.new(
      logger: logger,
      sparql_client: sparql_client,
      attachment_path_base: attachment_path_base
    )
  end

  describe '#smart_merge' do
    context 'when one value is nil' do
      it 'returns the non-nil value when a_val is nil' do
        result = builder.send(:smart_merge, { key: nil }, { key: 'value' })
        expect(result).to eq({ key: 'value' })
      end

      it 'returns the non-nil value when b_val is nil' do
        result = builder.send(:smart_merge, { key: 'value' }, { key: nil })
        expect(result).to eq({ key: 'value' })
      end
    end

    context 'when both values are arrays' do
      it 'concatenates and removes duplicates' do
        result = builder.send(:smart_merge, { key: [1, 2] }, { key: [2, 3] })
        expect(result).to eq({ key: [1, 2, 3] })
      end

      it 'handles empty arrays' do
        result = builder.send(:smart_merge, { key: [] }, { key: [1, 2] })
        expect(result).to eq({ key: [1, 2] })
      end
    end

    context 'when one value is array and other is simple value' do
      it 'adds simple value to array when a_val is array' do
        result = builder.send(:smart_merge, { key: [1, 2] }, { key: 3 })
        expect(result).to eq({ key: [1, 2, 3] })
      end

      it 'adds simple value to array when b_val is array' do
        result = builder.send(:smart_merge, { key: 'hello' }, { key: ['world'] })
        expect(result).to eq({ key: ['world', 'hello'] })
      end

      it 'handles string values' do
        result = builder.send(:smart_merge, { key: ['a'] }, { key: 'b' })
        expect(result).to eq({ key: ['a', 'b'] })
      end

      it 'handles integer values' do
        result = builder.send(:smart_merge, { key: [1] }, { key: 2 })
        expect(result).to eq({ key: [1, 2] })
      end

      it 'handles float values' do
        result = builder.send(:smart_merge, { key: [1.5] }, { key: 2.5 })
        expect(result).to eq({ key: [1.5, 2.5] })
      end
    end

    context 'when both values are hashes' do
      it 'recursively merges hashes' do
        hash_a = { key: { nested: 'a', other: 'x' } }
        hash_b = { key: { nested: 'b', another: 'y' } }
        result = builder.send(:smart_merge, hash_a, hash_b)
        expect(result).to eq({ key: { nested: ['a', 'b'], other: 'x', another: 'y' } })
      end
    end

    context 'when both values are simple values' do
      it 'creates array with both values' do
        result = builder.send(:smart_merge, { key: 'a' }, { key: 'b' })
        expect(result).to eq({ key: ['a', 'b'] })
      end

      it 'removes duplicates' do
        result = builder.send(:smart_merge, { key: 'same' }, { key: 'same' })
        expect(result).to eq({ key: ['same'] })
      end

      it 'handles mixed simple types' do
        result = builder.send(:smart_merge, { key: 'text' }, { key: 42 })
        expect(result).to eq({ key: ['text', 42] })
      end
    end

    context 'when values cannot be merged' do
      it 'raises error for incompatible types' do
        expect {
          builder.send(:smart_merge, { key: { nested: 'hash' } }, { key: 'string' })
        }.to raise_error(/smart_merge: Invalid combo/)
      end

      it 'includes problematic values in error message' do
        expect {
          builder.send(:smart_merge, { key: Class.new }, { key: 'string' })
        }.to raise_error(/can not be merged/)
      end
    end

    context 'with complex nested structures' do
      it 'handles deeply nested merging' do
        doc_a = {
          title: 'Document A',
          tags: ['ruby'],
          metadata: {
            author: 'Alice',
            categories: ['tech']
          }
        }

        doc_b = {
          title: 'Document B',
          tags: ['programming'],
          metadata: {
            author: 'Bob',
            categories: ['software'],
            year: 2023
          }
        }

        result = builder.send(:smart_merge, doc_a, doc_b)

        expect(result).to eq({
          title: ['Document A', 'Document B'],
          tags: ['ruby', 'programming'],
          metadata: {
            author: ['Alice', 'Bob'],
            categories: ['tech', 'software'],
            year: 2023
          }
        })
      end
    end
  end
end
