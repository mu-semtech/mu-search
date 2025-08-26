require 'faraday'
require 'faraday/retry'
require 'faraday/typhoeus'

module MuSearch
  module Tika
    class Client
      def initialize(host: "tika", port: 9998, logger:)
        @logger = logger
        @base_url = "http://#{host}:#{port.to_s}/"
        retry_options = {
          max: 6,
          interval: 1,
          interval_randomness: 0.5,
          backoff_factor: 2,
          retry_statuses: [429, 503], # Too many requests, Service unavailable
          exceptions: Faraday::Retry::Middleware::DEFAULT_EXCEPTIONS + [Faraday::ConnectionFailed],
          retry_block: -> (env:, retry_count:, will_retry_in:, options:, exception:) {
            @logger.info("TIKA") { "Failed to run request #{env.method.upcase} #{env.url} (retry #{retry_count + 1}). Request will be retried." }
          },
          exhausted_retries_block: -> (env:, options:, exception:) {
            @logger.warn("TIKA") { "Failed to run request #{env.method.upcase} #{env.url}. Max number of retries reached." }
            raise exception unless exception.nil?
          }
        }
        @connection = Faraday.new(@base_url) do |faraday|
          faraday.request :retry, retry_options
          faraday.adapter :typhoeus
        end
      end

      def extract_text(file_path, blob)
        mime_type = determine_mime_type file_path, blob

        @logger.debug("TIKA") { "Extract text of #{file_path} using Tika" }
        response = @connection.put(
          "tika",
          blob,
          accept: "text/plain",
          content_type: mime_type,
          "X-Tika-OCRskipOcr": "true"
        )

        if response.success?
          text = response.body
          @logger.debug("TIKA") { "Text extraction of #{file_path} using Tika succeeded." }
          text
        elsif response.status == 422 # HTTPUnprocessableEntity
          @logger.warn("TIKA") { "Tika returned [#{response.status} #{response.reason_phrase}] to extract text for file #{file_path}. The file may be encrypted. Check the Tika logs for additional info." }
          nil
        else
          @logger.error("TIKA") { "Failed to extract text for file #{file_path}.\nResponse: #{response.status} #{response.reason_phrase}\n#{response.body}" }
          raise "Tika returned [#{response.status} #{response.reason_phrase}] to extract text for file #{file_path}. Check the Tika logs for additional info."
        end
      end

      private

      def determine_mime_type(file_path, blob)
        @logger.debug("TIKA") { "Determine mimetype of #{file_path} using Tika" }
        response = @connection.put(
          "detect/stream",
          blob,
          content_disposition: "attachment; filename=#{File.basename(file_path)}"
        )

        if response.success?
          mime_type = response.body
          @logger.debug("TIKA") { "Mimetype of #{file_path}: #{mime_type}" }
          mime_type
        else
          @logger.warn("TIKA") { "Unable to determine mimetype of #{file_path}. Tika returned [#{response.status} #{response.reason_phrase}]." }
          nil
        end
      end

    end
  end
end
