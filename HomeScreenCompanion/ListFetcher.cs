using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
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

        private static readonly System.Net.Http.HttpClient _netHttpClient = new System.Net.Http.HttpClient();


        private const string AiSystemPrompt =
            "You are a movie and TV show recommendation assistant. " +
            "Respond ONLY with a valid JSON array. No explanation, no markdown, no code fences. " +
            "Each item must have these fields: " +
            "\"title\" (string, required), " +
            "\"year\" (integer or null), " +
            "\"imdb_id\" (string starting with \"tt\" if known, otherwise null), " +
            "\"type\" (\"movie\" or \"show\"). " +
            "Return exactly the items requested. Do not add any commentary. " +
            "Example: [{\"title\":\"Inception\",\"year\":2010,\"imdb_id\":\"tt1375666\",\"type\":\"movie\"}]";

        public ListFetcher(IHttpClient httpClient, IJsonSerializer jsonSerializer)
        {
            _httpClient = httpClient;
            _jsonSerializer = jsonSerializer;
        }

        public async Task<List<ExternalItemDto>> FetchItems(string url, int limit, string traktClientId, string mdbApiKey, string tmdbApiKey, CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(url)) return new List<ExternalItemDto>();

            if (url.Contains("mdblist.com"))
                return await FetchMdblist(url, mdbApiKey, limit, cancellationToken);
            if (url.Contains("themoviedb.org"))
                return await FetchTmdb(url, tmdbApiKey, limit, cancellationToken);

            return await FetchTrakt(url, traktClientId, limit, cancellationToken);
        }

        // Builds a TMDB API URL, appending api_key for short keys.
        // Bearer tokens are added as a header via BuildTmdbRequest instead.
        private static string BuildTmdbUrl(string endpoint, string apiKey)
        {
            if (apiKey.Length > 80) // Bearer JWT — no api_key in URL
                return $"https://api.themoviedb.org{endpoint}";
            var sep = endpoint.Contains('?') ? "&" : "?";
            return $"https://api.themoviedb.org{endpoint}{sep}api_key={apiKey}";
        }

        private static System.Net.Http.HttpRequestMessage BuildTmdbRequest(string endpoint, string apiKey)
        {
            var url = BuildTmdbUrl(endpoint, apiKey);
            var req = new System.Net.Http.HttpRequestMessage(System.Net.Http.HttpMethod.Get, url);
            if (apiKey.Length > 80)
                req.Headers.TryAddWithoutValidation("Authorization", $"Bearer {apiKey}");
            req.Headers.TryAddWithoutValidation("Accept", "application/json");
            return req;
        }

        private async Task<T> GetTmdbAsync<T>(string endpoint, string apiKey, CancellationToken cancellationToken) where T : class
        {
            using var cts = System.Threading.CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(TimeSpan.FromSeconds(10));
            using var req = BuildTmdbRequest(endpoint, apiKey);
            var resp = await _netHttpClient.SendAsync(req, cts.Token);
            if (!resp.IsSuccessStatusCode) return null;
            var json = await resp.Content.ReadAsStringAsync();
            return _jsonSerializer.DeserializeFromString<T>(json);
        }

        // Maps TMDB browse page URL paths to their API endpoints and implied media type.
        // Returns (apiPath, impliedMediaType) where impliedMediaType is null for mixed/trending.
        private static (string apiPath, string impliedMediaType) ResolveTmdbPageEndpoint(string urlPath)
        {
            var p = urlPath.Trim('/').ToLowerInvariant();
            if (p == "movie/now-playing")  return ("/3/movie/now_playing", "movie");
            if (p == "movie/popular")      return ("/3/movie/popular",     "movie");
            if (p == "movie/top-rated")    return ("/3/movie/top_rated",   "movie");
            if (p == "movie/upcoming")     return ("/3/movie/upcoming",    "movie");
            if (p == "tv/top-rated")       return ("/3/tv/top_rated",      "tv");
            if (p == "tv/popular")         return ("/3/tv/popular",        "tv");
            if (p == "tv/on-the-air")      return ("/3/tv/on_the_air",     "tv");
            if (p == "tv/airing-today")    return ("/3/tv/airing_today",   "tv");
            if (p.StartsWith("trending"))  return ("/3/trending/all/week", null);
            return (null, null);
        }

        private async Task<List<ExternalItemDto>> FetchTmdb(string rawUrl, string apiKey, int limit, CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(apiKey)) return new List<ExternalItemDto>();

            if (!Uri.TryCreate(rawUrl.Trim(), UriKind.Absolute, out var uri))
                return new List<ExternalItemDto>();

            var urlPath = uri.AbsolutePath.TrimEnd('/');
            var (builtInApi, impliedMediaType) = ResolveTmdbPageEndpoint(urlPath);

            if (builtInApi != null)
            {
                // TMDB built-in browse page (now-playing, popular, etc.) — paginated results
                return await FetchTmdbPaged(builtInApi, impliedMediaType, apiKey, limit, cancellationToken);
            }

            var segs = urlPath.Split(new[] { '/' }, StringSplitOptions.RemoveEmptyEntries);

            // Award category: /award/{id}-slug/category/{id}-slug
            if (segs.Length >= 4
                && string.Equals(segs[0], "award", StringComparison.OrdinalIgnoreCase)
                && string.Equals(segs[2], "category", StringComparison.OrdinalIgnoreCase))
            {
                var awardId = ExtractLeadingNumber(segs[1]);
                var categoryId = ExtractLeadingNumber(segs[3]);
                if (awardId != null && categoryId != null)
                    return await FetchTmdbPaged($"/3/award/{awardId}/category/{categoryId}", null, apiKey, limit, cancellationToken);
            }

            // Collection: /collection/{id}-slug
            if (segs.Length >= 2 && string.Equals(segs[0], "collection", StringComparison.OrdinalIgnoreCase))
            {
                var collectionId = ExtractLeadingNumber(segs[1]);
                if (collectionId != null)
                    return await FetchTmdbCollection(collectionId, apiKey, limit, cancellationToken);
            }

            // Keyword: /keyword/{id}-slug or /keyword/{id}-slug/tv
            if (segs.Length >= 2 && string.Equals(segs[0], "keyword", StringComparison.OrdinalIgnoreCase))
            {
                var keywordId = ExtractLeadingNumber(segs[1]);
                var mt = segs.Length >= 3 && string.Equals(segs[2], "tv", StringComparison.OrdinalIgnoreCase) ? "tv" : "movie";
                if (keywordId != null)
                    return await FetchTmdbPaged($"/3/discover/{mt}?with_keywords={keywordId}", mt, apiKey, limit, cancellationToken);
            }

            // Company: /company/{id}-slug or /company/{id}-slug/tv
            if (segs.Length >= 2 && string.Equals(segs[0], "company", StringComparison.OrdinalIgnoreCase))
            {
                var companyId = ExtractLeadingNumber(segs[1]);
                var mt = segs.Length >= 3 && string.Equals(segs[2], "tv", StringComparison.OrdinalIgnoreCase) ? "tv" : "movie";
                if (companyId != null)
                    return await FetchTmdbPaged($"/3/discover/{mt}?with_companies={companyId}", mt, apiKey, limit, cancellationToken);
            }

            // Network: /network/{id}-slug (Usually implies TV)
            if (segs.Length >= 2 && string.Equals(segs[0], "network", StringComparison.OrdinalIgnoreCase))
            {
                var networkId = ExtractLeadingNumber(segs[1]);
                if (networkId != null)
                    return await FetchTmdbPaged($"/3/discover/tv?with_networks={networkId}", "tv", apiKey, limit, cancellationToken);
            }

            // Similar / Recommendations: /movie/{id}-slug/similar or /tv/{id}-slug/recommendations
            if (segs.Length >= 3 && 
                (string.Equals(segs[0], "movie", StringComparison.OrdinalIgnoreCase) || string.Equals(segs[0], "tv", StringComparison.OrdinalIgnoreCase)) &&
                (string.Equals(segs[2], "similar", StringComparison.OrdinalIgnoreCase) || string.Equals(segs[2], "recommendations", StringComparison.OrdinalIgnoreCase)))
            {
                var mediaType = segs[0].ToLowerInvariant();
                var id = ExtractLeadingNumber(segs[1]);
                var action = segs[2].ToLowerInvariant();
                if (id != null)
                    return await FetchTmdbPaged($"/3/{mediaType}/{id}/{action}", mediaType, apiKey, limit, cancellationToken);
            }

            // Fall back: user-created list with numeric ID, e.g. /list/12345
            string listId = null;
            for (int i = 0; i < segs.Length; i++)
            {
                if (string.Equals(segs[i], "list", StringComparison.OrdinalIgnoreCase) && i + 1 < segs.Length)
                {
                    listId = ExtractLeadingNumber(segs[i + 1]);
                    break;
                }
            }
            if (string.IsNullOrEmpty(listId)) return new List<ExternalItemDto>();

            TmdbListResponse listResponse;
            try { listResponse = await GetTmdbAsync<TmdbListResponse>($"/3/list/{listId}", apiKey, cancellationToken); }
            catch { return new List<ExternalItemDto>(); }
            if (listResponse == null) return new List<ExternalItemDto>();

            if (listResponse.items == null || listResponse.items.Count == 0)
                return new List<ExternalItemDto>();

            var items = listResponse.items;
            if (limit > 0 && limit < 10000 && items.Count > limit)
                items = items.Take(limit).ToList();

            return await ResolveTmdbImdbIds(items.Select(i => (i.id, i.media_type ?? "movie", i.title ?? i.name)).ToList(), apiKey, cancellationToken);
        }

        private async Task<List<ExternalItemDto>> FetchTmdbPaged(string apiPath, string impliedMediaType, string apiKey, int limit, CancellationToken cancellationToken)
        {
            var all = new List<(int id, string mediaType, string name)>();
            int page = 1;

            while (true)
            {
                TmdbPagedResponse resp;
                var pageSep = apiPath.Contains('?') ? "&" : "?";
                try { resp = await GetTmdbAsync<TmdbPagedResponse>($"{apiPath}{pageSep}page={page}", apiKey, cancellationToken); }
                catch { break; }

                if (resp?.results == null || resp.results.Count == 0) break;

                foreach (var item in resp.results)
                {
                    var mt = !string.IsNullOrEmpty(item.media_type) ? item.media_type : (impliedMediaType ?? "movie");
                    all.Add((item.id, mt, item.title ?? item.name));
                }

                if (page >= resp.total_pages) break;
                if (limit > 0 && limit < 10000 && all.Count >= limit) break;
                page++;
            }

            if (limit > 0 && limit < 10000 && all.Count > limit)
                all = all.Take(limit).ToList();

            return await ResolveTmdbImdbIds(all, apiKey, cancellationToken);
        }

        // Extracts the leading numeric ID from a TMDB URL slug like "17-best-sound" → "17"
        private static string ExtractLeadingNumber(string segment)
        {
            if (string.IsNullOrEmpty(segment)) return null;
            var dashIdx = segment.IndexOf('-');
            var numPart = dashIdx > 0 ? segment.Substring(0, dashIdx) : segment;
            return int.TryParse(numPart, out _) ? numPart : null;
        }

        private async Task<List<ExternalItemDto>> FetchTmdbCollection(string collectionId, string apiKey, int limit, CancellationToken cancellationToken)
        {
            TmdbCollectionResponse resp;
            try { resp = await GetTmdbAsync<TmdbCollectionResponse>($"/3/collection/{collectionId}", apiKey, cancellationToken); }
            catch { return new List<ExternalItemDto>(); }
            if (resp?.parts == null || resp.parts.Count == 0) return new List<ExternalItemDto>();

            var all = resp.parts
                .Select(i => (i.id, i.media_type ?? "movie", i.title ?? i.name))
                .ToList();
            if (limit > 0 && limit < 10000 && all.Count > limit)
                all = all.Take(limit).ToList();

            return await ResolveTmdbImdbIds(all, apiKey, cancellationToken);
        }

        private async Task<List<ExternalItemDto>> ResolveTmdbImdbIds(List<(int id, string mediaType, string name)> items, string apiKey, CancellationToken cancellationToken)
        {
            // All external_ids fetched concurrently via _netHttpClient (connection-pooled).
            // Reduce concurrency to avoid hitting TMDBs 50 req/s limit in quick bursts.
            var semaphore = new System.Threading.SemaphoreSlim(5, 5);
            var tasks = items.Select(async item =>
            {
                var (id, mediaType, name) = item;
                var endpoint = string.Equals(mediaType, "tv", StringComparison.OrdinalIgnoreCase)
                    ? $"/3/tv/{id}/external_ids"
                    : $"/3/movie/{id}/external_ids";
                await semaphore.WaitAsync(cancellationToken);
                try
                {
                    var extIds = await GetTmdbAsync<TmdbExternalIds>(endpoint, apiKey, cancellationToken);
                    if (!string.IsNullOrEmpty(extIds?.imdb_id))
                        return new ExternalItemDto { Name = name, Imdb = extIds.imdb_id };
                }
                catch { }
                finally { semaphore.Release(); }
                return null;
            });

            var resolved = await System.Threading.Tasks.Task.WhenAll(tasks);
            return resolved.Where(x => x != null).ToList();
        }

        private async Task<string> FetchTmdbExternalIds(int tmdbId, string mediaType, string apiKey, CancellationToken cancellationToken)
        {
            var endpoint = string.Equals(mediaType, "tv", StringComparison.OrdinalIgnoreCase)
                ? $"/3/tv/{tmdbId}/external_ids"
                : $"/3/movie/{tmdbId}/external_ids";
            try
            {
                var extIds = await GetTmdbAsync<TmdbExternalIds>(endpoint, apiKey, cancellationToken);
                return extIds?.imdb_id;
            }
            catch { return null; }
        }

        private async Task<List<ExternalItemDto>> FetchMdblist(string listUrl, string apiKey, int limit, CancellationToken cancellationToken)
        {
            if (!string.IsNullOrWhiteSpace(apiKey))
                return await FetchMdblistApi(listUrl, apiKey, limit, cancellationToken);
            else
                return await FetchMdblistLegacy(listUrl, limit, cancellationToken);
        }

        // Uses api.mdblist.com — works for both public and private lists
        private async Task<List<ExternalItemDto>> FetchMdblistApi(string listUrl, string apiKey, int limit, CancellationToken cancellationToken)
        {
            var apiBaseUrl = BuildMdblistApiUrl(listUrl);
            const int pageSize = 1000;
            var all = new List<ExternalItemDto>();
            int offset = 0;

            while (true)
            {
                var apiUrl = $"{apiBaseUrl}?apikey={apiKey}&limit={pageSize}&offset={offset}";
                try
                {
                    using (var stream = await _httpClient.Get(new HttpRequestOptions { Url = apiUrl, CancellationToken = cancellationToken }))
                    {
                        var result = _jsonSerializer.DeserializeFromStream<MdbListResponse>(stream);
                        if (result == null) break;

                        var movies = result.movies ?? new List<MdbListItem>();
                        var shows = result.shows ?? new List<MdbListItem>();
                        int pageCount = movies.Count + shows.Count;
                        if (pageCount == 0) break;

                        var page = movies.Concat(shows)
                            .Where(x => !string.IsNullOrEmpty(x.imdb_id))
                            .Select(x => new ExternalItemDto { Name = x.title, Imdb = x.imdb_id, Tmdb = null });
                        all.AddRange(page);

                        if (pageCount < pageSize) break;
                        if (limit < 10000 && all.Count >= limit) break;
                        offset += pageSize;
                    }
                }
                catch { break; }
            }

            return all;
        }

        // Legacy fallback: mdblist.com/slug/json — public lists only, no API key needed
        private async Task<List<ExternalItemDto>> FetchMdblistLegacy(string listUrl, int limit, CancellationToken cancellationToken)
        {
            var cleanUrl = listUrl.Trim().TrimEnd('/');
            if (!cleanUrl.EndsWith("/json")) cleanUrl += "/json";

            const int pageSize = 1000;
            var all = new List<ExternalItemDto>();
            int offset = 0;

            while (true)
            {
                var apiUrl = $"{cleanUrl}?limit={pageSize}&offset={offset}&_={DateTimeOffset.UtcNow.ToUnixTimeSeconds()}";
                try
                {
                    using (var stream = await _httpClient.Get(new HttpRequestOptions { Url = apiUrl, CancellationToken = cancellationToken }))
                    {
                        var result = _jsonSerializer.DeserializeFromStream<List<MdbListItem>>(stream);
                        if (result == null || result.Count == 0) break;

                        var page = result
                            .Where(x => !string.IsNullOrEmpty(x.imdb_id))
                            .Select(x => new ExternalItemDto { Name = x.title, Imdb = x.imdb_id, Tmdb = null });
                        all.AddRange(page);

                        if (result.Count < pageSize) break;
                        if (limit < 10000 && all.Count >= limit) break;
                        offset += pageSize;
                    }
                }
                catch { break; }
            }

            return all;
        }

        private static string BuildMdblistApiUrl(string listUrl)
        {
            var cleaned = listUrl.Trim().TrimEnd('/');

            // Already a correct API items URL
            if (cleaned.Contains("api.mdblist.com") && cleaned.EndsWith("/items"))
                return cleaned;

            if (Uri.TryCreate(cleaned, UriKind.Absolute, out var uri))
            {
                var path = uri.AbsolutePath.TrimEnd('/');
                if (path.EndsWith("/json")) path = path.Substring(0, path.Length - 5);
                if (path.EndsWith("/items")) path = path.Substring(0, path.Length - 6);
                return $"https://api.mdblist.com{path}/items";
            }

            // Fallback: treat as path fragment
            var fallback = cleaned
                .Replace("https://mdblist.com", "")
                .Replace("https://www.mdblist.com", "")
                .TrimEnd('/');
            if (fallback.EndsWith("/json")) fallback = fallback.Substring(0, fallback.Length - 5);
            return $"https://api.mdblist.com{fallback}/items";
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

        public async Task<List<AiListItem>> FetchAiList(
            string provider,
            string prompt,
            string openAiApiKey,
            string geminiApiKey,
            string recentlyWatchedContext,
            int limit,
            CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(prompt))
                return new List<AiListItem>();

            var userMessage = string.IsNullOrWhiteSpace(recentlyWatchedContext)
                ? $"{prompt}\n\nReturn up to {limit} items."
                : $"{recentlyWatchedContext}\n\n{prompt}\n\nReturn up to {limit} items.";

            try
            {
                string rawJson;
                if (string.Equals(provider, "Gemini", StringComparison.OrdinalIgnoreCase))
                {
                    if (string.IsNullOrWhiteSpace(geminiApiKey))
                        throw new InvalidOperationException("Gemini API key is not configured.");
                    rawJson = await CallGemini(geminiApiKey, userMessage, cancellationToken);
                }
                else
                {
                    if (string.IsNullOrWhiteSpace(openAiApiKey))
                        throw new InvalidOperationException("OpenAI API key is not configured.");
                    rawJson = await CallOpenAi(openAiApiKey, userMessage, cancellationToken);
                }

                var cleaned = CleanAiJsonOutput(rawJson);
                var items = _jsonSerializer.DeserializeFromString<List<AiListItem>>(cleaned);
                return items ?? new List<AiListItem>();
            }
            catch
            {
                return new List<AiListItem>();
            }
        }

        private async Task<string> CallOpenAi(string apiKey, string userMessage, CancellationToken cancellationToken)
        {
            var requestBody = $"{{" +
                $"\"model\":\"gpt-4o-mini\"," +
                $"\"messages\":[" +
                $"{{\"role\":\"system\",\"content\":{EscapeJsonString(AiSystemPrompt)}}}," +
                $"{{\"role\":\"user\",\"content\":{EscapeJsonString(userMessage)}}}" +
                $"]}}";

            var request = new System.Net.Http.HttpRequestMessage(System.Net.Http.HttpMethod.Post, "https://api.openai.com/v1/chat/completions");
            request.Headers.Add("Authorization", $"Bearer {apiKey}");
            request.Content = new System.Net.Http.StringContent(requestBody, Encoding.UTF8, "application/json");

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(TimeSpan.FromSeconds(30));

            var response = await _netHttpClient.SendAsync(request, cts.Token);
            var responseBody = await response.Content.ReadAsStringAsync();

            if (!response.IsSuccessStatusCode)
                throw new InvalidOperationException($"OpenAI API error {(int)response.StatusCode}: {responseBody}");

            var parsed = _jsonSerializer.DeserializeFromString<OpenAiResponse>(responseBody);
            return parsed?.choices?.FirstOrDefault()?.message?.content ?? "";
        }

        private async Task<string> CallGemini(string apiKey, string userMessage, CancellationToken cancellationToken)
        {
            var url = $"https://generativelanguage.googleapis.com/v1beta/models/gemini-3-flash-preview:generateContent?key={apiKey}";
            var requestBody = $"{{" +
                $"\"systemInstruction\":{{\"parts\":[{{\"text\":{EscapeJsonString(AiSystemPrompt)}}}]}}," +
                $"\"contents\":[{{\"role\":\"user\",\"parts\":[{{\"text\":{EscapeJsonString(userMessage)}}}]}}]" +
                $"}}";

            var request = new System.Net.Http.HttpRequestMessage(System.Net.Http.HttpMethod.Post, url);
            request.Content = new System.Net.Http.StringContent(requestBody, Encoding.UTF8, "application/json");

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(TimeSpan.FromSeconds(30));

            var response = await _netHttpClient.SendAsync(request, cts.Token);
            var responseBody = await response.Content.ReadAsStringAsync();

            if (!response.IsSuccessStatusCode)
                throw new InvalidOperationException($"Gemini API error {(int)response.StatusCode}: {responseBody}");

            var parsed = _jsonSerializer.DeserializeFromString<GeminiResponse>(responseBody);
            return parsed?.candidates?.FirstOrDefault()?.content?.parts?.FirstOrDefault()?.text ?? "";
        }

        private static string CleanAiJsonOutput(string raw)
        {
            var trimmed = raw.Trim();
            if (trimmed.StartsWith("```"))
            {
                var firstNewline = trimmed.IndexOf('\n');
                var lastFence = trimmed.LastIndexOf("```");
                if (firstNewline > 0 && lastFence > firstNewline)
                    trimmed = trimmed.Substring(firstNewline + 1, lastFence - firstNewline - 1).Trim();
            }
            return trimmed;
        }

        private static string EscapeJsonString(string value)
        {
            var sb = new StringBuilder("\"");
            foreach (var c in value)
            {
                switch (c)
                {
                    case '"': sb.Append("\\\""); break;
                    case '\\': sb.Append("\\\\"); break;
                    case '\n': sb.Append("\\n"); break;
                    case '\r': sb.Append("\\r"); break;
                    case '\t': sb.Append("\\t"); break;
                    default: sb.Append(c); break;
                }
            }
            sb.Append('"');
            return sb.ToString();
        }
    }
}