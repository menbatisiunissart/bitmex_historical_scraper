from limits import storage, strategies, RateLimitItemPerMinute
memory_storage = storage.MemoryStorage()
moving_window = strategies.MovingWindowRateLimiter(memory_storage)
rate = RateLimitItemPerMinute(25, 1)

def get_limiter(identifier):
    limiter = moving_window.hit(rate, identifier)
    return limiter

