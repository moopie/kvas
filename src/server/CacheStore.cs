namespace server;

public class CacheStore
{
    private readonly SemaphoreSlim _semaphore = new(1, 1);
    
    private readonly Dictionary<string, string> _dictionary = new();

    public async Task SetAsync(string key, string value)
    {
        if (string.IsNullOrEmpty(key))
        {
            throw new ArgumentNullException(nameof(key));
        }

        if (string.IsNullOrEmpty(value))
        {
            throw new ArgumentNullException(nameof(value));
        }

        await _semaphore.WaitAsync();
        try
        {
            _dictionary[key] = value;
        }
        finally
        {
            _semaphore.Release();
        }
    }

    public async Task<string> GetAsync(string key)
    {
        if (string.IsNullOrEmpty(key))
        {
            throw new ArgumentNullException(nameof(key));
        }

        if (_dictionary.TryGetValue(key, out var value))
        {
            return await Task.FromResult(value);
        }

        return await Task.FromResult(string.Empty);
    }

    public async Task RemoveAsync(string key)
    {
        if (string.IsNullOrEmpty(key))
        {
            throw new ArgumentNullException(nameof(key));
        }

        if (!_dictionary.ContainsKey(key))
        {
            throw new KeyNotFoundException();
        }

        await _semaphore.WaitAsync();
        try
        {
            _dictionary.Remove(key);
        }
        finally
        {
            _semaphore.Release();
        }
    }
}
