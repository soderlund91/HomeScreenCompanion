using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MediaBrowser.Common.Net;
using HttpRequestOptions = MediaBrowser.Common.Net.HttpRequestOptions;
using MediaBrowser.Model.Serialization;

namespace HomeScreenCompanion
{
    public class ListFetcher
    {
        private readonly IHttpClient _httpClient;
        private readonly IJsonSerializer _jsonSerializer;

        private static readonly System.Net.Http.HttpClient _netHttpClient = new System.Net.Http.HttpClient();


        public ListFetcher(IHttpClient httpClient, IJsonSerializer jsonSerializer)
        {
            _httpClient = httpClient;
            _jsonSerializer = jsonSerializer;
        }

        // At most this many list entries are read from a list.
        internal const int MaxScan = 1000;

        /// <summary>Why the last fetch returned nothing (e.g. "HTTP 403 Forbidden"); null when it did not fail.</summary>
        public string? LastError { get; private set; }

        // A failed request as the log should show it: the HTTP status when there is one.
        private static string Describe(Exception ex)
        {
            try
            {
                var code = ex.GetType().GetProperty("StatusCode")?.GetValue(ex);
                if (code != null) return $"HTTP {Convert.ToInt32(code)} {code}";
            }
            catch { }
            var msg = ex.GetBaseException().Message ?? ex.Message;
            return msg.Length > 200 ? msg.Substring(0, 200) + "…" : msg;
        }

        // LastError of an AI that answered without titles: an empty result, not a failure.
        internal const string EmptyAnswer = "the answer held no titles";

        /// <summary>True when LastError says the source could not be read (HTTP or network error,
        /// missing API key, unreadable URL, AI error), rather than that it was empty.</summary>
        internal static bool IsFailure(string? lastError) =>
            !string.IsNullOrEmpty(lastError) && lastError != EmptyAnswer;

        /// <summary>True when the list does not exist (HTTP 404 / 410): not a passing hiccup, so in a
        /// group with other URLs it is left out instead of keeping the whole group as it was.</summary>
        internal static bool IsGone(string? lastError) =>
            !string.IsNullOrEmpty(lastError) && (lastError!.Contains("HTTP 404") || lastError.Contains("HTTP 410"));

        // A list read part-way (a later page failed) is not used: it would drop every title after
        // that page. It counts as a failed fetch, so "Preserve tags on empty result" keeps them.
        private List<ExternalItemDto> PageFailed(List<ExternalItemDto> all, int pageNumber, Exception ex, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            LastError = pageNumber > 1 ? $"page {pageNumber} could not be read: {Describe(ex)}" : Describe(ex);
            all.Clear();
            return all;
        }

        // With countsTowardLimit, limit is a number of hits instead of list entries: the list is read
        // (in order, up to MaxScan entries) until that many distinct IMDb ids count, and the entries up
        // to and including the last hit are returned. Without it the first limit entries are returned.
        public async Task<List<ExternalItemDto>> FetchItems(string url, int limit, string traktClientId, string mdbApiKey, string tmdbApiKey, CancellationToken cancellationToken,
            Func<ExternalItemDto, bool>? countsTowardLimit = null)
        {
            // Set a global maximum of 1000 items to prevent massive fetches, unless a specific smaller limit is requested
            if (limit <= 0 || limit > MaxScan)
            {
                limit = MaxScan;
            }

            LastError = null;
            if (string.IsNullOrWhiteSpace(url)) return new List<ExternalItemDto>();

            var hits = countsTowardLimit != null && limit < MaxScan ? new HitCounter(limit, countsTowardLimit) : null;
            int fetchLimit = hits != null ? MaxScan : limit;

            List<ExternalItemDto> all;
            if (url.Contains("mdblist.com"))
            {
                all = await FetchMdblist(url, mdbApiKey, fetchLimit, hits, cancellationToken);
            }
            else if (url.Contains("themoviedb.org"))
            {
                all = await FetchTmdb(url, tmdbApiKey, fetchLimit, hits, cancellationToken);
            }
            else if (url.IndexOf("trakt.tv", StringComparison.OrdinalIgnoreCase) >= 0
                || !Uri.TryCreate(url.Trim(), UriKind.Absolute, out var other) || (other.Scheme != "http" && other.Scheme != "https"))
            {
                // A trakt.tv URL, or a bare Trakt API path ("users/x/lists/y").
                all = await FetchTrakt(url, traktClientId, fetchLimit, cancellationToken);
            }
            else
            {
                LastError = "not a Trakt, MDBList or TMDb list URL";
                return new List<ExternalItemDto>();
            }
            all = Distinct(all);
            return hits != null ? hits.Cut(all) : all;
        }

        // The same title twice in one list (TMDb's pages overlap when the list changes while it is
        // read): only its first entry counts.
        private static List<ExternalItemDto> Distinct(List<ExternalItemDto> all)
        {
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            return all.Where(i =>
            {
                var key = !string.IsNullOrEmpty(i?.Imdb) ? "imdb:" + i!.Imdb : !string.IsNullOrEmpty(i?.Tmdb) ? "tmdb:" + i!.Tmdb : null;
                return key == null || seen.Add(key);
            }).ToList();
        }

        // Counts the distinct IMDb ids that count toward the limit, as the entries are read.
        internal class HitCounter
        {
            private readonly int _limit;
            private readonly Func<ExternalItemDto, bool> _counts;
            private readonly HashSet<string> _hits = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            private readonly HashSet<string> _misses = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            internal HitCounter(int limit, Func<ExternalItemDto, bool> counts) { _limit = limit; _counts = counts; }

            internal bool Full => _hits.Count >= _limit;

            // Reads one entry; true once the limit is reached.
            internal bool See(ExternalItemDto item)
            {
                if (Full) return true;
                var id = item?.Imdb;
                if (string.IsNullOrEmpty(id) || _hits.Contains(id!) || _misses.Contains(id!)) return false;
                if (_counts(item!)) _hits.Add(id!); else _misses.Add(id!);
                return Full;
            }

            // The entries up to and including the one that reached the limit.
            internal List<ExternalItemDto> Cut(List<ExternalItemDto> all)
            {
                // Entries not seen yet (a last page read without stopping) are checked here.
                var seen = new HitCounter(_limit, i => _hits.Contains(i.Imdb) || (!_misses.Contains(i.Imdb) && _counts(i)));
                for (int i = 0; i < all.Count; i++)
                    if (seen.See(all[i])) return all.Take(i + 1).ToList();
                return all;
            }
        }

        private async Task<List<ExternalItemDto>> FetchMdblist(string listUrl, string apiKey, int limit, HitCounter? hits, CancellationToken cancellationToken)
        {
            if (IsMdblistOfficialList(listUrl) || string.IsNullOrWhiteSpace(apiKey))
                return await FetchMdblistLegacy(listUrl, limit, hits, cancellationToken);
            else
                return await FetchMdblistApi(listUrl, apiKey, limit, hits, cancellationToken);
        }

        // Official lists (mdblist.com/lists/official/...) have no api.mdblist.com endpoint, only the public /json one
        private static bool IsMdblistOfficialList(string url) =>
            url.IndexOf("/lists/official/", StringComparison.OrdinalIgnoreCase) >= 0;

        // Uses api.mdblist.com — works for both public and private lists
        private async Task<List<ExternalItemDto>> FetchMdblistApi(string listUrl, string apiKey, int limit, HitCounter? hits, CancellationToken cancellationToken)
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

                        var page = movies.Select(x => (x, "movie")).Concat(shows.Select(x => (x, "tv")))
                            .Where(p => !string.IsNullOrEmpty(p.x.imdb_id))
                            .Select(p => new ExternalItemDto { Name = p.x.title!, Imdb = p.x.imdb_id!, Tmdb = null, MediaType = p.Item2 });
                        all.AddRange(page);

                        if (pageCount < pageSize) break;
                        if (limit < 10000 && all.Count >= limit) break;
                        if (hits != null && all.Any(hits.See)) break;
                        offset += pageSize;
                    }
                }
                catch (Exception ex) { return PageFailed(all, offset / pageSize + 1, ex, cancellationToken); }
            }

            return all;
        }

        // Legacy fallback: mdblist.com/slug/json — public lists only, no API key needed
        private async Task<List<ExternalItemDto>> FetchMdblistLegacy(string listUrl, int limit, HitCounter? hits, CancellationToken cancellationToken)
        {
            var cleanUrl = listUrl.Trim();
            int queryIndex = cleanUrl.IndexOfAny(new[] { '?', '#' });
            if (queryIndex >= 0) cleanUrl = cleanUrl.Substring(0, queryIndex);
            cleanUrl = cleanUrl.TrimEnd('/');
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
                            .Select(x => new ExternalItemDto { Name = x.title!, Imdb = x.imdb_id!, Tmdb = null, MediaType = MdbMediaType(x.mediatype) });
                        all.AddRange(page);

                        if (result.Count < pageSize) break;
                        if (limit < 10000 && all.Count >= limit) break;
                        if (hits != null && all.Any(hits.See)) break;
                        offset += pageSize;
                    }
                }
                catch (Exception ex) { return PageFailed(all, offset / pageSize + 1, ex, cancellationToken); }
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

        private class TmdbApiTarget
        {
            public string? ListId { get; set; }
            public string? Endpoint { get; set; }
            public string? MediaType { get; set; }
            public string FullUrl { get; set; } = null!;
        }

        private TmdbApiTarget? ExtractTmdbApiTarget(string url)
        {
            try
            {
                var uri = new Uri(url);

                if (uri.Host.Equals("api.themoviedb.org", StringComparison.OrdinalIgnoreCase))
                {
                    string? mediaType = null;
                    if (uri.AbsolutePath.Contains("/movie/")) mediaType = "movie";
                    else if (uri.AbsolutePath.Contains("/tv/")) mediaType = "tv";
                    else if (uri.AbsolutePath.Contains("/collection/")) mediaType = "movie";   // a collection's parts are films
                    return new TmdbApiTarget { FullUrl = url, MediaType = mediaType };
                }

                var segments = uri.Segments.Select(s => s.TrimEnd('/')).Where(s => !string.IsNullOrEmpty(s)).ToList();

                if (segments.Count >= 2 && segments[0] == "list")
                {
                    return new TmdbApiTarget { ListId = segments[1] };
                }

                // A collection page (themoviedb.org/collection/10-star-wars-collection): its films, in TMDb's order.
                if (segments.Count >= 2 && segments[0] == "collection")
                {
                    var id = new string(segments[1].TakeWhile(char.IsDigit).ToArray());
                    if (id.Length > 0)
                        return new TmdbApiTarget { FullUrl = $"https://api.themoviedb.org/3/collection/{id}", MediaType = "movie" };
                }

                if (segments.Count >= 1 && (segments[0] == "movie" || segments[0] == "tv"))
                {
                    string type = segments[0];
                    string category = segments.Count > 1 ? segments[1].Replace("-", "_") : "popular";

                    var validCategories = new[] { "popular", "top_rated", "now_playing", "upcoming", "on_the_air", "airing_today" };
                    if (validCategories.Contains(category))
                    {
                        return new TmdbApiTarget { Endpoint = $"{type}/{category}", MediaType = type };
                    }
                }
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            return null;
        }

        private async Task<List<ExternalItemDto>> FetchTmdb(string url, string apiKey, int limit, HitCounter? hits, CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(apiKey)) { LastError = "no TMDb API key in Settings"; return new List<ExternalItemDto>(); }

            var target = ExtractTmdbApiTarget(url);
            if (target == null) { LastError = "this TMDb URL is not a list or category HSC can read"; return new List<ExternalItemDto>(); }

            var all = new List<ExternalItemDto>();
            int page = 1;
            bool isToken = apiKey.Length > 100 || apiKey.Contains(".");
            bool useV4 = !string.IsNullOrEmpty(target.ListId);
            bool useV3 = true;

            while (true)
            {
                TmdbListResponse? result = null;

                if (useV4 && !string.IsNullOrEmpty(target.ListId))
                {
                    var apiUrl = $"https://api.themoviedb.org/4/list/{target.ListId}?page={page}";
                    if (!isToken) apiUrl += $"&api_key={apiKey}";

                    try
                    {
                        var options = new HttpRequestOptions { Url = apiUrl, CancellationToken = cancellationToken };
                        if (isToken)
                        {
                            options.RequestHeaders.Add("Authorization", $"Bearer {apiKey}");
                        }
                        using (var stream = await _httpClient.Get(options))
                        {
                            result = _jsonSerializer.DeserializeFromStream<TmdbListResponse>(stream);
                            useV3 = false;
                        }
                    }
                    catch
                    {
                        useV4 = false;
                    }
                }

                if (useV3 && result == null)
                {
                    string apiUrl;
                    if (!string.IsNullOrEmpty(target.FullUrl))
                    {
                        apiUrl = target.FullUrl;
                        apiUrl += apiUrl.Contains("?") ? $"&page={page}" : $"?page={page}";
                    }
                    else
                    {
                        apiUrl = string.IsNullOrEmpty(target.Endpoint) 
                            ? $"https://api.themoviedb.org/3/list/{target.ListId}?page={page}"
                            : $"https://api.themoviedb.org/3/{target.Endpoint}?page={page}";
                    }
                        
                    if (!isToken) apiUrl += $"&api_key={apiKey}";

                    try
                    {
                        var options = new HttpRequestOptions { Url = apiUrl, CancellationToken = cancellationToken };
                        if (isToken)
                        {
                            options.RequestHeaders.Add("Authorization", $"Bearer {apiKey}");
                        }
                        using (var stream = await _httpClient.Get(options))
                        {
                            result = _jsonSerializer.DeserializeFromStream<TmdbListResponse>(stream);
                        }
                    }
                    catch (Exception ex)
                    {
                        return PageFailed(all, page, ex, cancellationToken);
                    }
                }

                if (result == null) break;

                var items = (result.items != null && result.items.Count > 0) ? result.items
                    : (result.results != null && result.results.Count > 0) ? result.results
                    : result.parts;
                if (items == null || items.Count == 0) break;

                // A page's IMDb ids are looked up side by side (one request per title), then read in list order.
                var imdbIds = await Task.WhenAll(items.Select(item => TmdbImdbId(item, target, apiKey, isToken, cancellationToken)));
                for (int k = 0; k < items.Count; k++)
                {
                    var item = items[k];
                    var dto = new ExternalItemDto
                    {
                        Name = (item.title ?? item.name)!,
                        Tmdb = item.id.ToString(),
                        Imdb = imdbIds[k]!,
                        MediaType = target.MediaType ?? (item.media_type == "tv" ? "tv" : "movie")
                    };

                    all.Add(dto);
                    if (limit > 0 && all.Count >= limit) return all.Take(limit).ToList();
                    if (hits != null && hits.See(dto)) return all;
                }

                if (page >= result.total_pages) break;
                page++;
            }

            return all;
        }

        private async Task<string?> TmdbImdbId(TmdbListItem item, TmdbApiTarget target, string apiKey, bool isToken, CancellationToken cancellationToken)
        {
            try
            {
                var type = target.MediaType ?? (item.media_type == "tv" ? "tv" : "movie");
                var extUrl = $"https://api.themoviedb.org/3/{type}/{item.id}/external_ids";
                if (!isToken) extUrl += $"?api_key={apiKey}";

                var extOptions = new HttpRequestOptions { Url = extUrl, CancellationToken = cancellationToken };
                if (isToken)
                {
                    extOptions.RequestHeaders.Add("Authorization", $"Bearer {apiKey}");
                }

                using (var extStream = await _httpClient.Get(extOptions))
                {
                    var extIds = _jsonSerializer.DeserializeFromStream<TmdbExternalIds>(extStream);
                    return extIds != null && !string.IsNullOrEmpty(extIds.imdb_id) ? extIds.imdb_id : null;
                }
            }
            catch { return null; }
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

        private async Task<List<ExternalItemDto>> FetchTraktRobust(MediaBrowser.Common.Net.HttpRequestOptions options)
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
                                    list.Add(new ExternalItemDto { Name = title!, Imdb = imdb!, Tmdb = null, MediaType = item.movie != null ? "movie" : "tv" });
                            }
                            if (list.Count > 0) return list;
                        }
                    }
                    catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }

                    try
                    {
                        var flatList = _jsonSerializer.DeserializeFromString<List<TraktMovie>>(json);
                        if (flatList != null)
                        {
                            foreach (var item in flatList)
                            {
                                if (!string.IsNullOrEmpty(item.title) && !string.IsNullOrEmpty(item.ids?.imdb))
                                    list.Add(new ExternalItemDto { Name = item.title!, Imdb = item.ids?.imdb!, Tmdb = null });
                            }
                            if (list.Count > 0) return list;
                        }
                    }
                    catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }

                    return list;
                }
            }
            catch (Exception ex)
            {
                LastError = Describe(ex);
                return new List<ExternalItemDto>();
            }
        }

        public async Task<List<AiListItem>> FetchAiList(
            string provider,
            string prompt,
            string openAiApiKey,
            string openAiModel,
            string geminiApiKey,
            string geminiModel,
            string claudeApiKey,
            string claudeModel,
            string ollamaBaseUrl,
            string ollamaModel,
            string systemPrompt,
            string recentlyWatchedContext,
            int limit,
            CancellationToken cancellationToken)
        {
            LastError = null;
            if (string.IsNullOrWhiteSpace(prompt))
            {
                LastError = "the prompt is empty";
                return new List<AiListItem>();
            }

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
                    rawJson = await CallGemini(geminiApiKey, geminiModel ?? "gemini-2.0-flash", systemPrompt, userMessage, cancellationToken);
                }
                else if (string.Equals(provider, "Claude", StringComparison.OrdinalIgnoreCase))
                {
                    if (string.IsNullOrWhiteSpace(claudeApiKey))
                        throw new InvalidOperationException("Claude API key is not configured.");
                    rawJson = await CallClaude(claudeApiKey, claudeModel ?? "claude-haiku-4-5-20251001", systemPrompt, userMessage, limit, cancellationToken);
                }
                else if (string.Equals(provider, "Ollama", StringComparison.OrdinalIgnoreCase))
                {
                    if (string.IsNullOrWhiteSpace(ollamaBaseUrl))
                        throw new InvalidOperationException("Ollama base URL is not configured.");
                    if (string.IsNullOrWhiteSpace(ollamaModel))
                        throw new InvalidOperationException("Ollama model is not configured.");
                    rawJson = await CallOllama(ollamaBaseUrl, ollamaModel, systemPrompt, userMessage, cancellationToken);
                }
                else
                {
                    if (string.IsNullOrWhiteSpace(openAiApiKey))
                        throw new InvalidOperationException("OpenAI API key is not configured.");
                    rawJson = await CallOpenAi(openAiApiKey, openAiModel ?? "gpt-4o-mini", systemPrompt, userMessage, cancellationToken);
                }

                var cleaned = CleanAiJsonOutput(rawJson);
                var items = _jsonSerializer.DeserializeFromString<List<AiListItem>>(cleaned);
                if (items == null || items.Count == 0) LastError = EmptyAnswer;
                return items ?? new List<AiListItem>();
            }
            catch (Exception ex)
            {
                cancellationToken.ThrowIfCancellationRequested();
                LastError = Describe(ex);
                return new List<AiListItem>();
            }
        }

        private async Task<string> CallOpenAi(string apiKey, string model, string systemPrompt, string userMessage, CancellationToken cancellationToken)
        {
            var requestBody = $"{{" +
                $"\"model\":{EscapeJsonString(model)}," +
                $"\"messages\":[" +
                $"{{\"role\":\"system\",\"content\":{EscapeJsonString(systemPrompt)}}}," +
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

        private async Task<string> CallGemini(string apiKey, string model, string systemPrompt, string userMessage, CancellationToken cancellationToken)
        {
            var url = $"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={apiKey}";
            var requestBody = $"{{" +
                $"\"systemInstruction\":{{\"parts\":[{{\"text\":{EscapeJsonString(systemPrompt)}}}]}}," +
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

        private async Task<string> CallClaude(string apiKey, string model, string systemPrompt, string userMessage, int count, CancellationToken cancellationToken)
        {
            // About 30 tokens per title in the JSON answer; a cut-off answer cannot be read at all.
            int maxTokens = Math.Min(16000, 1024 + Math.Max(0, count) * 60);
            var requestBody = $"{{" +
                $"\"model\":{EscapeJsonString(model)}," +
                $"\"max_tokens\":{maxTokens}," +
                $"\"system\":{EscapeJsonString(systemPrompt)}," +
                $"\"messages\":[{{\"role\":\"user\",\"content\":{EscapeJsonString(userMessage)}}}]" +
                $"}}";

            var request = new System.Net.Http.HttpRequestMessage(System.Net.Http.HttpMethod.Post, "https://api.anthropic.com/v1/messages");
            request.Headers.Add("x-api-key", apiKey);
            request.Headers.Add("anthropic-version", "2023-06-01");
            request.Content = new System.Net.Http.StringContent(requestBody, Encoding.UTF8, "application/json");

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(TimeSpan.FromSeconds(30));

            var response = await _netHttpClient.SendAsync(request, cts.Token);
            var responseBody = await response.Content.ReadAsStringAsync();

            if (!response.IsSuccessStatusCode)
                throw new InvalidOperationException($"Claude API error {(int)response.StatusCode}: {responseBody}");

            var parsed = _jsonSerializer.DeserializeFromString<ClaudeResponse>(responseBody);
            return parsed?.content?.FirstOrDefault(c => c.type == "text")?.text ?? "";
        }

        private async Task<string> CallOllama(string baseUrl, string model, string systemPrompt, string userMessage, CancellationToken cancellationToken)
        {
            var url = $"{baseUrl.TrimEnd('/')}/api/chat";
            var requestBody = $"{{" +
                $"\"model\":{EscapeJsonString(model)}," +
                $"\"messages\":[" +
                $"{{\"role\":\"system\",\"content\":{EscapeJsonString(systemPrompt)}}}," +
                $"{{\"role\":\"user\",\"content\":{EscapeJsonString(userMessage)}}}" +
                $"]," +
                $"\"stream\":false" +
                $"}}";

            var request = new System.Net.Http.HttpRequestMessage(System.Net.Http.HttpMethod.Post, url);
            request.Content = new System.Net.Http.StringContent(requestBody, Encoding.UTF8, "application/json");

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(TimeSpan.FromSeconds(120));

            var response = await _netHttpClient.SendAsync(request, cts.Token);
            var responseBody = await response.Content.ReadAsStringAsync();

            if (!response.IsSuccessStatusCode)
                throw new InvalidOperationException($"Ollama API error {(int)response.StatusCode}: {responseBody}");

            var parsed = _jsonSerializer.DeserializeFromString<OllamaResponse>(responseBody);
            return parsed?.message?.content ?? "";
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

        // MDBList's mediatype ("movie" / "show") as Sonarr / Radarr's "movie" / "tv".
        private static string? MdbMediaType(string? mediatype)
        {
            if (string.Equals(mediatype, "movie", StringComparison.OrdinalIgnoreCase)) return "movie";
            if (string.Equals(mediatype, "show", StringComparison.OrdinalIgnoreCase)) return "tv";
            return null;
        }

        internal static string EscapeJsonString(string value)
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
                    default:
                        if (c < 0x20) sb.Append("\\u").Append(((int)c).ToString("x4"));
                        else sb.Append(c);
                        break;
                }
            }
            sb.Append('"');
            return sb.ToString();
        }
    }
}