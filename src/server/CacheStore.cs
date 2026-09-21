using System.Collections.Concurrent;
using server.Interfaces;

namespace server;

public sealed class CacheStore : ICacheStore
{
    private readonly ConcurrentDictionary<string, string> _dictionary = new();

    public Task SetAsync(string key, string value)
    {
        if (string.IsNullOrEmpty(key))
        {
            throw new ArgumentNullException(nameof(key));
        }

        if (string.IsNullOrEmpty(value))
        {
            throw new ArgumentNullException(nameof(value));
        }

        _dictionary[key] = value;
        return Task.CompletedTask;
    }

    public Task<string> GetAsync(string key)
    {
        if (string.IsNullOrEmpty(key))
        {
            throw new ArgumentNullException(nameof(key));
        }

        return Task.FromResult(
            _dictionary.TryGetValue(key, out var value)
                ? value
                : string.Empty);
    }

    public Task RemoveAsync(string key)
    {
        if (string.IsNullOrEmpty(key))
        {
            throw new ArgumentNullException(nameof(key));
        }

        if (!_dictionary.TryRemove(key, out _))
        {
            throw new KeyNotFoundException();
        }

        return Task.CompletedTask;
    }
}
