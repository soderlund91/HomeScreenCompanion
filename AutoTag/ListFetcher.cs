using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MediaBrowser.Common.Net;
using MediaBrowser.Model.Serialization;

namespace HomeScreenCompanion
{
    public class ListFetcher
    {
        private readonly IHttpClient _httpClient;
        private readonly IJsonSerializer _jsonSerializer;

        public ListFetcher(IHttpClient httpClient, IJsonSerializer jsonSerializer)
        {
            _httpClient = httpClient;
            _jsonSerializer = jsonSerializer;
        }

        public async Task<List<ExternalItemDto>> FetchItems(string url, int limit, string traktClientId, string mdbApiKey, CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(url)) return new List<ExternalItemDto>();

            if (url.Contains("mdblist.com"))
            {
                return await FetchMdblist(url, mdbApiKey, cancellationToken);
            }
            else
            {
                return await FetchTrakt(url, traktClientId, limit, cancellationToken);
            }
        }

        private async Task<List<ExternalItemDto>> FetchMdblist(string listUrl, string apiKey, CancellationToken cancellationToken)
        {
            var cleanUrl = listUrl.Trim().TrimEnd('/');
            if (!cleanUrl.EndsWith("/json")) cleanUrl += "/json";
            var apiUrl = $"{cleanUrl}?apikey={apiKey}&_={DateTimeOffset.UtcNow.ToUnixTimeSeconds()}";
            try
            {
                using (var stream = await _httpClient.Get(new HttpRequestOptions { Url = apiUrl, CancellationToken = cancellationToken }))
                {
                    var result = _jsonSerializer.DeserializeFromStream<List<MdbListItem>>(stream);
                    if (result != null)
                    {
                        return result
                            .Where(x => !string.IsNullOrEmpty(x.imdb_id))
                            .Select(x => new ExternalItemDto
                            {
                                Name = x.title,
                                Imdb = x.imdb_id,
                                Tmdb = null
                            })
                            .ToList();
                    }
                }
            }
            catch { }
            return new List<ExternalItemDto>();
        }

        private async Task<List<ExternalItemDto>> FetchTrakt(string rawUrl, string clientId, int limit, CancellationToken cancellationToken)
        {
            string path = rawUrl.Trim();

            if (Uri.TryCreate(path, UriKind.Absolute, out var uri))
            {
                path = uri.AbsolutePath;
            }
            else
            {
                path = path.Replace("https://trakt.tv", "")
                           .Replace("https://api.trakt.tv", "")
                           .Replace("https://app.trakt.tv", "")
                           .Trim();
            }

            if (path.Contains("?")) path = path.Split('?')[0];
            path = path.Trim();

            if (path.Contains("/users/") && path.Contains("/lists/") && !path.EndsWith("/items"))
            {
                path = path.TrimEnd('/') + "/items";
            }

            if (!path.StartsWith("/")) path = "/" + path;

            var timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            var options = new HttpRequestOptions { Url = $"https://api.trakt.tv{path}?limit={limit}&_={timestamp}", CancellationToken = cancellationToken };

            options.RequestHeaders.Add("trakt-api-version", "2");
            options.RequestHeaders.Add("trakt-api-key", clientId);
            options.UserAgent = "HomeScreenCompanionPlugin/1.0";
            options.RequestHeaders.Add("Accept", "application/json");

            return await FetchTraktRobust(options);
        }

        private async Task<List<ExternalItemDto>> FetchTraktRobust(HttpRequestOptions options)
        {
            try
            {
                using (var stream = await _httpClient.Get(options))
                using (var reader = new StreamReader(stream))
                {
                    string json = await reader.ReadToEndAsync();
                    var list = new List<ExternalItemDto>();

                    try
                    {
                        var wrappedList = _jsonSerializer.DeserializeFromString<List<TraktBaseObject>>(json);
                        if (wrappedList != null && wrappedList.Any(x => x.movie != null || x.show != null))
                        {
                            foreach (var item in wrappedList)
                            {
                                string? title = item.movie?.title ?? item.show?.title;
                                string? imdb = item.movie?.ids?.imdb ?? item.show?.ids?.imdb;

                                if (!string.IsNullOrEmpty(title) && !string.IsNullOrEmpty(imdb))
                                    list.Add(new ExternalItemDto { Name = title, Imdb = imdb, Tmdb = null });
                            }
                            if (list.Count > 0) return list;
                        }
                    }
                    catch { }

                    try
                    {
                        var flatList = _jsonSerializer.DeserializeFromString<List<TraktMovie>>(json);
                        if (flatList != null)
                        {
                            foreach (var item in flatList)
                            {
                                if (!string.IsNullOrEmpty(item.title) && !string.IsNullOrEmpty(item.ids?.imdb))
                                    list.Add(new ExternalItemDto { Name = item.title, Imdb = item.ids?.imdb, Tmdb = null });
                            }
                            if (list.Count > 0) return list;
                        }
                    }
                    catch { }

                    return list;
                }
            }
            catch { return new List<ExternalItemDto>(); }
        }
    }
}