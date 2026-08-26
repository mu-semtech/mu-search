require 'concurrent-ruby'

class BoundedConcurrencyMiddleware
  def initialize(app, max_threads: 20, max_queue: 10)
    @app = app

    @pool = Concurrent::ThreadPoolExecutor.new(
      min_threads: 0,
      max_threads: max_threads,
      max_queue: max_queue,       # requests allowed to wait once all threads are busy
      fallback_policy: :abort     # raise Concurrent::RejectedExecutionError when full
    )
    Mu::log.info("SETUP") { "Setup request connection pool with #{max_threads} connections and allowing a queue of #{max_queue}." }
  end

  def call(env)
    future = Concurrent::Future.new(executor: @pool) { @app.call(env) }
    future.execute

    # Blocks the calling (accept/dispatch) thread until the pooled worker finishes.
    # This is fine — it's exactly the blocking that would've happened anyway,
    # just now bounded instead of unbounded.
    future.value!
  rescue Concurrent::RejectedExecutionError
    service_unavailable
  rescue Concurrent::Java::Java::JavaLangError => e
    # Optional extra safety net if you're still hitting native ThreadError
    # from elsewhere despite bounding this pool (belt-and-suspenders).
    raise e unless e.message =~ /unable to create new native thread/i
    service_unavailable
  end

  private

  def service_unavailable
    [
      503,
      { 'Content-Type' => 'text/plain', 'Retry-After' => '1' },
      ["Service temporarily unavailable\n"]
    ]
  end
end