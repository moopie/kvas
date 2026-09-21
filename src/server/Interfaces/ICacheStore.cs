namespace server.Interfaces;

public interface ICacheStore
{
    Task SetAsync(string key, string value);
    Task<string> GetAsync(string key);
    Task RemoveAsync(string key);
}
