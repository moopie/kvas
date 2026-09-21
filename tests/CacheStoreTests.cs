using server;

namespace tests;

public class CacheStoreTests
{
    [Fact]
    public async Task SetAsync_WithExistingKey_OverwritesValue()
    {
        var store = new CacheStore();

        await store.SetAsync("key", "first");
        await store.SetAsync("key", "second");

        Assert.Equal("second", await store.GetAsync("key"));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public async Task SetAsync_WithInvalidKey_ThrowsArgumentNullException(string? key)
    {
        var store = new CacheStore();

        await Assert.ThrowsAsync<ArgumentNullException>(() => store.SetAsync(key!, "value"));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public async Task SetAsync_WithInvalidValue_ThrowsArgumentNullException(string? value)
    {
        var store = new CacheStore();

        await Assert.ThrowsAsync<ArgumentNullException>(() => store.SetAsync("key", value!));
    }
    
    [Fact]
    public async Task SetAsync_DeleteAsync()
    {
        var store = new CacheStore();

        await store.SetAsync("key", "first");
        await store.RemoveAsync("key");
        
        Assert.Equal(string.Empty, await store.GetAsync("key"));
    }

    [Fact]
    public async Task GetAsync_WithExistingKey_ReturnsValue()
    {
        var store = new CacheStore();
        await store.SetAsync("key", "value");

        var value = await store.GetAsync("key");

        Assert.Equal("value", value);
    }

    [Fact]
    public async Task GetAsync_WithMissingKey_ReturnsEmptyString()
    {
        var store = new CacheStore();

        var value = await store.GetAsync("missing");

        Assert.Equal(string.Empty, value);
    }

    [Fact]
    public async Task SetGetAndRemoveAsync_ManageCachedValue()
    {
        var store = new CacheStore();

        await store.SetAsync("key", "value");

        Assert.Equal("value", await store.GetAsync("key"));

        await store.RemoveAsync("key");

        Assert.Equal(string.Empty, await store.GetAsync("key"));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public async Task GetAsync_WithInvalidKey_ThrowsArgumentNullException(string? key)
    {
        var store = new CacheStore();

        await Assert.ThrowsAsync<ArgumentNullException>(() => store.GetAsync(key!));
    }
}
