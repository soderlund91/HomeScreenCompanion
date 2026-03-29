using MediaBrowser.Model.Serialization;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;

namespace HomeScreenCompanion
{
    public class TagCacheManager
    {
        public static TagCacheManager Instance { get; } = new TagCacheManager();

        private Dictionary<string, HashSet<string>> _cache = new Dictionary<string, HashSet<string>>();
        private readonly object _lock = new object();
        private string _cacheFilePath;
        private IJsonSerializer _jsonSerializer;

        private TagCacheManager() { }

        public void Initialize(string dataPath, IJsonSerializer jsonSerializer)
        {
            _cacheFilePath = Path.Combine(dataPath, "homescreencompanion_cache.json");
            _jsonSerializer = jsonSerializer;
            Load();
        }

        public void AddToCache(string providerId, string tag)
        {
            lock (_lock)
            {
                if (!_cache.ContainsKey(providerId))
                {
                    _cache[providerId] = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                }
                _cache[providerId].Add(tag);
            }
        }

        public void RemoveTagFromAllEntries(string tagName)
        {
            lock (_lock)
            {
                foreach (var key in _cache.Keys)
                    _cache[key].RemoveWhere(t => string.Equals(t, tagName, StringComparison.OrdinalIgnoreCase));
            }
        }

        public void ClearCache()
        {
            lock (_lock)
            {
                _cache.Clear();
            }
        }

        public List<string> GetTagsForIds(Dictionary<string, string> providerIds)
        {
            var tagsToApply = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            lock (_lock)
            {
                var imdbKey = providerIds.Keys.FirstOrDefault(k => k.Equals("imdb", StringComparison.OrdinalIgnoreCase));
                if (imdbKey != null && _cache.TryGetValue($"imdb_{providerIds[imdbKey]}", out var imdbTags))
                    foreach (var tag in imdbTags) tagsToApply.Add(tag);

                var tmdbKey = providerIds.Keys.FirstOrDefault(k => k.Equals("tmdb", StringComparison.OrdinalIgnoreCase));
                if (tmdbKey != null && _cache.TryGetValue($"tmdb_{providerIds[tmdbKey]}", out var tmdbTags))
                    foreach (var tag in tmdbTags) tagsToApply.Add(tag);
            }
            return tagsToApply.ToList();
        }

        public void Save()
        {
            if (_jsonSerializer == null) return;
            lock (_lock)
            {
                try
                {
                    _jsonSerializer.SerializeToFile(_cache, _cacheFilePath);
                }
                catch { }
            }
        }

        private void Load()
        {
            if (_jsonSerializer == null) return;
            lock (_lock)
            {
                try
                {
                    if (File.Exists(_cacheFilePath))
                    {
                        var loaded = _jsonSerializer.DeserializeFromFile<Dictionary<string, List<string>>>(_cacheFilePath);
                        if (loaded != null)
                        {
                            _cache = new Dictionary<string, HashSet<string>>();
                            foreach (var kvp in loaded)
                                _cache[kvp.Key] = new HashSet<string>(kvp.Value, StringComparer.OrdinalIgnoreCase);
                        }
                    }
                }
                catch { }
            }
        }
    }
}