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

        // How many list entries are read from a list, per provider (Settings: TraktMaxItems,
        // MdblistMaxItems, TmdbMaxItems; AiMaxItems for the AI). Trakt and MDBList send a whole list
        // in one or a few requests, so they read it all by default; TMDb needs a request per title,
        // so it and the AI stop at 1000 unless Settings say otherwise. A JSON list (JsonMaxItems) can be
        // any site and may need a TMDb request per title, so it stops at 1000 too.
        internal const int DefaultMaxScan = 1000;
        internal const int DefaultTraktMax = 0, DefaultMdblistMax = 0, DefaultTmdbMax = DefaultMaxScan, DefaultAiMax = DefaultMaxScan,
            DefaultJsonMax = DefaultMaxScan;

        /// <summary>Which provider reads a list URL: "Mdblist", "Tmdb", "Trakt" (a trakt.tv URL or a bare
        /// Trakt API path such as "users/x/lists/y") or "Json" (any other http(s) URL that returns JSON).</summary>
        internal static string ListProvider(string url)
        {
            url = url ?? "";
            if (url.Contains("mdblist.com")) return "Mdblist";
            if (url.Contains("themoviedb.org")) return "Tmdb";
            if (url.IndexOf("trakt.tv", StringComparison.OrdinalIgnoreCase) >= 0
                || !Uri.TryCreate(url.Trim(), UriKind.Absolute, out var other) || (other.Scheme != "http" && other.Scheme != "https"))
                return "Trakt";
            return "Json";
        }

        // The cap Settings give a provider: 0 = no cap (the whole list), below 0 = its default.
        // A config saved before the setting gets the default from PluginConfiguration.
        internal const int NoCap = int.MaxValue;
        internal static int Cap(int configured, int fallback) => configured > 0 ? configured : configured == 0 ? NoCap : Cap(fallback, 0);

        internal static int CapForUrl(string url, PluginConfiguration? config)
        {
            switch (ListProvider(url))
            {
                case "Mdblist": return Cap(config?.MdblistMaxItems ?? DefaultMdblistMax, DefaultMdblistMax);
                case "Tmdb": return Cap(config?.TmdbMaxItems ?? DefaultTmdbMax, DefaultTmdbMax);
                case "Json": return Cap(config?.JsonMaxItems ?? DefaultJsonMax, DefaultJsonMax);
                default: return Cap(config?.TraktMaxItems ?? DefaultTraktMax, DefaultTraktMax);
            }
        }

        /// <summary>Why the last fetch returned nothing (e.g. "HTTP 403 Forbidden"); null when it did not fail.</summary>
        public string? LastError { get; private set; }

        /// <summary>Something the run log should say about a list that was read (a warning, not a failure):
        /// e.g. JSON-list entries left out because they could not be told apart. Null when there is none.</summary>
        public string? LastNote { get; private set; }

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
        // (in order, up to the provider's cap) until that many distinct IMDb ids count, and the entries
        // up to and including the last hit are returned. Without it the first limit entries are returned.
        public async Task<List<ExternalItemDto>> FetchItems(string url, int limit, string traktClientId, string mdbApiKey, string tmdbApiKey, CancellationToken cancellationToken,
            Func<ExternalItemDto, bool>? countsTowardLimit = null)
        {
            LastError = null;
            LastNote = null;
            if (string.IsNullOrWhiteSpace(url)) return new List<ExternalItemDto>();

            // No more than the provider's cap is read, however high (or unset) Max items is.
            int cap = CapForUrl(url, Plugin.Instance?.Configuration);
            if (limit <= 0 || limit > cap) limit = cap;

            var hits = countsTowardLimit != null && limit < cap ? new HitCounter(limit, countsTowardLimit) : null;
            int fetchLimit = hits != null ? cap : limit;

            List<ExternalItemDto> all;
            switch (ListProvider(url))
            {
                case "Mdblist":
                    all = await FetchMdblist(url, mdbApiKey, fetchLimit, hits, cancellationToken);
                    break;
                case "Tmdb":
                    all = await FetchTmdb(url, tmdbApiKey, fetchLimit, hits, cancellationToken);
                    break;
                case "Trakt":
                    // A trakt.tv URL, or a bare Trakt API path ("users/x/lists/y").
                    all = await FetchTrakt(url, traktClientId, fetchLimit, cancellationToken);
                    break;
                default:
                    all = await FetchJsonList(url, tmdbApiKey, fetchLimit, hits, cancellationToken);
                    break;
            }
            all = Distinct(all);
            // MDBList is read 1000 entries a page, so its last page can go past the cap.
            if (all.Count > fetchLimit) all = all.Take(fetchLimit).ToList();
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

        private static ExternalItemDto FromMdblist(MdbListItem x, string? type) => new ExternalItemDto
        {
            Name = x.title!, Imdb = x.imdb_id!, Tmdb = null,
            Type = type ?? (x.mediatype == "show" ? "show" : x.mediatype == "movie" ? "movie" : null),
            Year = x.release_year > 0 ? x.release_year : null
        };

        // Official lists (mdblist.com/lists/official/...) have no api.mdblist.com endpoint, only the public /json one
        private static bool IsMdblistOfficialList(string url) =>
            url.IndexOf("/lists/official/", StringComparison.OrdinalIgnoreCase) >= 0;

        // Uses api.mdblist.com — works for both public and private lists
        private async Task<List<ExternalItemDto>> FetchMdblistApi(string listUrl, string apiKey, int limit, HitCounter? hits, CancellationToken cancellationToken)
        {
            var apiBaseUrl = BuildMdblistApiUrl(listUrl);
            int pageSize = Math.Min(1000, Math.Max(1, limit));
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

                        var page = movies.Select(x => FromMdblist(x, "movie")).Concat(shows.Select(x => FromMdblist(x, "show")))
                            .Where(x => !string.IsNullOrEmpty(x.Imdb));
                        all.AddRange(page);

                        if (pageCount < pageSize) break;
                        if (all.Count >= limit) break;
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

            int pageSize = Math.Min(1000, Math.Max(1, limit));
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
                            .Select(x => FromMdblist(x, null));
                        all.AddRange(page);

                        if (result.Count < pageSize) break;
                        if (all.Count >= limit) break;
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
                    // "/3/discover/tv" and "/3/discover/movie" end in the type rather than carrying it mid-path.
                    var apiPath = uri.AbsolutePath.TrimEnd('/');
                    if (apiPath.Contains("/movie/") || apiPath.EndsWith("/movie", StringComparison.OrdinalIgnoreCase)) mediaType = "movie";
                    else if (apiPath.Contains("/tv/") || apiPath.EndsWith("/tv", StringComparison.OrdinalIgnoreCase)) mediaType = "tv";
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
                    var mediaType = target.MediaType ?? item.media_type;
                    var date = item.release_date ?? item.first_air_date;
                    var dto = new ExternalItemDto
                    {
                        Name = (item.title ?? item.name)!,
                        Tmdb = item.id.ToString(),
                        Imdb = imdbIds[k]!,
                        Type = mediaType == "tv" ? "show" : mediaType == "movie" ? "movie" : null,
                        Year = date != null && date.Length >= 4 && int.TryParse(date.Substring(0, 4), out var y) ? y : (int?)null
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

        // A plain JSON list from any other site: a top-level array, or an object with the list in
        // items / results / movies / shows / data. Each entry gives an IMDb id and/or a TMDb id ("id"
        // counts as the TMDb id unless it is an IMDb id). Entries with a TMDb id only get their IMDb
        // id from TMDb when there is a TMDb key; a TMDb id without a type is tried as a movie first.
        private async Task<List<ExternalItemDto>> FetchJsonList(string url, string tmdbApiKey, int limit, HitCounter? hits, CancellationToken cancellationToken)
        {
            string body;
            try
            {
                var options = new HttpRequestOptions { Url = url.Trim(), CancellationToken = cancellationToken };
                options.RequestHeaders.Add("Accept", "application/json");
                using (var stream = await _httpClient.Get(options))
                using (var reader = new StreamReader(stream))
                    body = await reader.ReadToEndAsync();
            }
            catch (Exception ex)
            {
                cancellationToken.ThrowIfCancellationRequested();
                LastError = Describe(ex);
                return new List<ExternalItemDto>();
            }

            var entries = new List<(JsonListItem Item, string? Type)>();
            var text = (body ?? "").TrimStart('﻿', ' ', '\t', '\r', '\n');
            try
            {
                if (text.StartsWith("["))
                {
                    foreach (var i in _jsonSerializer.DeserializeFromString<List<JsonListItem>>(text) ?? new List<JsonListItem>())
                        entries.Add((i, null));
                }
                else if (text.StartsWith("{"))
                {
                    var w = _jsonSerializer.DeserializeFromString<JsonListWrapper>(text);
                    var lists = new[] { (w?.items, (string?)null), (w?.results, null), (w?.data, null), (w?.movies, "movie"), (w?.shows, "show") };
                    // movies and shows are read together (MDBList's shape); the others on their own.
                    var first = lists.Take(3).FirstOrDefault(l => l.Item1 != null && l.Item1.Count > 0);
                    if (first.Item1 != null) entries.AddRange(first.Item1.Select(i => (i, first.Item2)));
                    else foreach (var l in lists.Skip(3)) if (l.Item1 != null) entries.AddRange(l.Item1.Select(i => (i, l.Item2)));
                    if (entries.Count == 0 && lists.All(l => l.Item1 == null))
                    {
                        LastError = "the JSON has no list in it (a top-level array, or items, results, movies, shows or data)";
                        return new List<ExternalItemDto>();
                    }
                }
                else
                {
                    LastError = "the URL did not return JSON";
                    return new List<ExternalItemDto>();
                }
            }
            catch (Exception ex)
            {
                PluginLog.Swallowed(ex);
                LastError = "the URL did not return a JSON list HSC can read";
                return new List<ExternalItemDto>();
            }

            var all = new List<ExternalItemDto>();
            foreach (var (item, listType) in entries)
            {
                if (item == null) continue;
                var dto = FromJsonListItem(item, listType);
                if (dto != null) all.Add(dto);
            }
            if (all.Count == 0)
            {
                if (entries.Count > 0) LastError = "the JSON list has no IMDb or TMDb ids";
                return all;
            }
            if (all.Count > limit) all = all.Take(limit).ToList();

            bool hasKey = !string.IsNullOrWhiteSpace(tmdbApiKey);
            if (!hasKey)
            {
                if (all.All(i => string.IsNullOrEmpty(i.Imdb)))
                {
                    LastError = "the JSON list gives TMDb ids only, and these need a TMDb API key in Settings";
                    return new List<ExternalItemDto>();
                }
                // Some entries have a TMDb id only: said in the run log instead of left out without a word.
                int tmdbOnly = all.Count(i => string.IsNullOrEmpty(i.Imdb));
                if (tmdbOnly > 0)
                    LastNote = $"{RunLog.Plural(tmdbOnly, "entry", "entries")} with a TMDb id only left out: they need a TMDb API key in Settings";
                return all;
            }

            // The IMDb ids of TMDb-only entries are asked 20 at a time, in list order, so a Max that
            // counts hits stops asking once it has them.
            int ambiguous = 0;
            var result = new List<ExternalItemDto>();
            try
            {
                for (int start = 0; start < all.Count; start += 20)
                {
                    var chunk = all.Skip(start).Take(20).ToList();
                    var told = await Task.WhenAll(chunk.Where(i => string.IsNullOrEmpty(i.Imdb) && !string.IsNullOrEmpty(i.Tmdb))
                        .Select(i => FillImdbFromTmdb(i, tmdbApiKey, cancellationToken)));
                    ambiguous += told.Count(ok => !ok);
                    foreach (var dto in chunk)
                    {
                        result.Add(dto);
                        if (hits != null && hits.See(dto)) return result;
                    }
                }
                return result;
            }
            finally
            {
                if (ambiguous > 0)
                    LastNote = $"{RunLog.Plural(ambiguous, "entry", "entries")} with a TMDb id only and no type left out: TMDb has a movie and a show with that id, "
                        + "and the list's title and year did not tell which one it means. Give the entries a type (\"type\": \"movie\" or \"show\")";
            }
        }

        private static ExternalItemDto? FromJsonListItem(JsonListItem i, string? listType)
        {
            string? Clean(string? s) { s = s?.Trim(); return string.IsNullOrEmpty(s) || s == "0" || s == "null" ? null : s; }
            bool IsImdb(string? s) => s != null && s.StartsWith("tt", StringComparison.OrdinalIgnoreCase) && s.Length > 2 && s.Skip(2).All(char.IsDigit);
            bool IsNumber(string? s) => s != null && s.All(char.IsDigit);

            var id = Clean(i.id);
            var imdb = new[] { i.imdb_id, i.imdb, i.imdbId, i.ids?.imdb }.Select(Clean).FirstOrDefault(IsImdb)
                ?? (IsImdb(id) ? id : null);
            var tmdb = new[] { i.tmdb_id, i.tmdb, i.tmdbId, i.ids?.tmdb }.Select(Clean).FirstOrDefault(IsNumber)
                ?? (IsNumber(id) ? id : null);
            if (imdb == null && tmdb == null) return null;

            int? year = null;
            foreach (var s in new[] { i.year, i.release_year, i.release_date, i.first_air_date })
            {
                var v = Clean(s);
                if (v != null && v.Length >= 4 && int.TryParse(v.Substring(0, 4), out var y) && y > 1800) { year = y; break; }
            }

            string? type = listType;
            var t = (Clean(i.media_type) ?? Clean(i.type))?.ToLowerInvariant();
            if (t == "movie" || t == "movies" || t == "film") type = "movie";
            else if (t == "show" || t == "shows" || t == "tv" || t == "series" || t == "tvshow" || t == "tv_show") type = "show";

            return new ExternalItemDto
            {
                Name = (Clean(i.title) ?? Clean(i.name) ?? imdb ?? ("TMDb " + tmdb))!,
                Imdb = imdb!,
                Tmdb = tmdb,
                Type = type,
                Year = year
            };
        }

        // The IMDb id of an entry with a TMDb id only. With a type: that movie's or show's. Without one:
        // TMDb numbers movies and shows apart, so one id can be a film and an unrelated show. Both are
        // asked; when only one exists it is taken, when both do the one whose title (2 points) and year
        // (1 point) agree better with the list's own is taken. A tie (also: the list gives neither) is
        // left out, so a list of shows never tags the film that has the same number.
        // False when the entry was left out for that reason (said in LastNote); true otherwise.
        private async Task<bool> FillImdbFromTmdb(ExternalItemDto dto, string apiKey, CancellationToken cancellationToken)
        {
            var key = apiKey.Trim();
            if (dto.Type == "show" || dto.Type == "movie")
            {
                try
                {
                    var ids = await MissingTitles.TmdbGet<TmdbExternalIds>($"{(dto.Type == "show" ? "tv" : "movie")}/{dto.Tmdb}/external_ids", key, _httpClient, _jsonSerializer, cancellationToken);
                    if (!string.IsNullOrEmpty(ids?.imdb_id)) dto.Imdb = ids!.imdb_id!;
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
                catch { }
                return true;
            }

            async Task<TmdbTitleWithIds?> Ask(string kind)
            {
                try { return await MissingTitles.TmdbGet<TmdbTitleWithIds>($"{kind}/{dto.Tmdb}?append_to_response=external_ids", key, _httpClient, _jsonSerializer, cancellationToken); }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
                catch { return null; }   // not found (404) or not answered: as if there were none
            }
            var movie = await Ask("movie");
            var show = await Ask("tv");
            if (movie == null && show == null) return true;   // TMDb knows neither: the entry stays without an IMDb id

            // The list gave a title of its own (else Name is the "TMDb <id>" stand-in, see FromJsonListItem).
            bool hasTitle = !string.Equals(dto.Name, "TMDb " + dto.Tmdb, StringComparison.Ordinal);
            string Norm(string? s) => new string((s ?? "").Where(char.IsLetterOrDigit).Select(char.ToLowerInvariant).ToArray());
            int Score(TmdbTitleWithIds? x, bool isShow)
            {
                if (x == null) return -1;
                int score = 0;
                if (hasTitle)
                {
                    var listTitle = Norm(dto.Name);
                    var names = isShow ? new[] { x.name, x.original_name } : new[] { x.title, x.original_title };
                    if (listTitle.Length > 0 && names.Any(n => Norm(n) == listTitle)) score += 2;
                }
                var date = isShow ? x.first_air_date : x.release_date;
                if (dto.Year != null && date != null && date.Length >= 4 && int.TryParse(date.Substring(0, 4), out var y) && y == dto.Year) score += 1;
                return score;
            }
            int movieScore = Score(movie, false), showScore = Score(show, true);
            TmdbTitleWithIds? pick;
            bool pickShow;
            if (movie == null) { pick = show; pickShow = true; }
            else if (show == null) { pick = movie; pickShow = false; }
            else if (movieScore > showScore) { pick = movie; pickShow = false; }
            else if (showScore > movieScore) { pick = show; pickShow = true; }
            else return false;   // cannot be told apart: left out

            if (!string.IsNullOrEmpty(pick!.external_ids?.imdb_id)) dto.Imdb = pick.external_ids!.imdb_id!;
            dto.Type = pickShow ? "show" : "movie";
            return true;
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
            // One request for the whole list; with no cap (Settings 0) a limit no list reaches.
            int traktLimit = Math.Min(limit, 100000);
            var options = new HttpRequestOptions { Url = $"https://api.trakt.tv{path}?limit={traktLimit}&_={timestamp}", CancellationToken = cancellationToken };

            options.RequestHeaders.Add("trakt-api-version", "2");
            options.RequestHeaders.Add("trakt-api-key", clientId);
            options.UserAgent = "HomeScreenCompanionPlugin/1.0";
            options.RequestHeaders.Add("Accept", "application/json");

            // A flat answer (/movies/popular, /shows/popular) does not say what its entries are.
            return await FetchTraktRobust(options, path.IndexOf("/shows", StringComparison.OrdinalIgnoreCase) >= 0 ? "show" : "movie");
        }

        private async Task<List<ExternalItemDto>> FetchTraktRobust(MediaBrowser.Common.Net.HttpRequestOptions options, string flatType)
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

                                int? tmdb = item.movie?.ids?.tmdb ?? item.show?.ids?.tmdb;
                                int year = item.movie?.year ?? item.show?.year ?? 0;

                                if (!string.IsNullOrEmpty(title) && !string.IsNullOrEmpty(imdb))
                                    list.Add(new ExternalItemDto { Name = title!, Imdb = imdb!, Tmdb = tmdb?.ToString(),
                                        Type = item.movie != null ? "movie" : "show", Year = year > 0 ? year : (int?)null });
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
                                    list.Add(new ExternalItemDto { Name = item.title!, Imdb = item.ids?.imdb!, Tmdb = item.ids?.tmdb?.ToString(),
                                        Type = flatType, Year = item.year > 0 ? item.year : (int?)null });
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
            IList<AiConnection>? aiConnections,
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
                else if (AiConnection.IsCompat(provider))
                {
                    // A removed connection must not fall through to OpenAI below.
                    var conn = AiConnection.Find(aiConnections, provider)
                        ?? throw new InvalidOperationException("AI connection not found");
                    if (string.IsNullOrWhiteSpace(conn.BaseUrl))
                        throw new InvalidOperationException($"{conn.Name}: base URL is not configured.");
                    if (string.IsNullOrWhiteSpace(conn.Model))
                        throw new InvalidOperationException($"{conn.Name}: model is not configured.");
                    rawJson = await CallOpenAi(conn.BaseUrl.Trim().TrimEnd('/') + "/chat/completions", conn.ApiKey, conn.Model.Trim(),
                        systemPrompt, userMessage, true, string.IsNullOrWhiteSpace(conn.Name) ? "AI" : conn.Name, cancellationToken, timeoutSeconds: 120);
                }
                else
                {
                    if (string.IsNullOrWhiteSpace(openAiApiKey))
                        throw new InvalidOperationException("OpenAI API key is not configured.");
                    rawJson = await CallOpenAi("https://api.openai.com/v1/chat/completions", openAiApiKey, openAiModel ?? "gpt-4o-mini", systemPrompt, userMessage, false, "OpenAI", cancellationToken);
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

        // OpenAI's chat API, also spoken by the OpenAI-compatible services. jsonMode asks for a JSON
        // object (response_format json_object); a service that rejects that is asked again without it.
        // timeoutSeconds: OpenAI-compatible services are often local models, so they get longer
        // (also when asked again without JSON mode).
        private async Task<string> CallOpenAi(string url, string apiKey, string model, string systemPrompt, string userMessage, bool jsonMode, string name, CancellationToken cancellationToken, int timeoutSeconds = 30)
        {
            // JSON mode only allows an object: the system prompt's array goes inside one, said in
            // the system prompt itself so the two instructions do not disagree.
            var system = jsonMode
                ? systemPrompt + "\n\nThis answer must be a JSON object: put the JSON array described above in it as \"items\", like {\"items\": [ ... ]}, and nothing else."
                : systemPrompt;
            var requestBody = $"{{" +
                $"\"model\":{EscapeJsonString(model)}," +
                $"\"messages\":[" +
                $"{{\"role\":\"system\",\"content\":{EscapeJsonString(system)}}}," +
                $"{{\"role\":\"user\",\"content\":{EscapeJsonString(userMessage)}}}" +
                $"]" + (jsonMode ? ",\"response_format\":{\"type\":\"json_object\"}" : "") + "}";

            var request = new System.Net.Http.HttpRequestMessage(System.Net.Http.HttpMethod.Post, url);
            if (!string.IsNullOrWhiteSpace(apiKey))
                request.Headers.Add("Authorization", $"Bearer {apiKey.Trim()}");
            request.Content = new System.Net.Http.StringContent(requestBody, Encoding.UTF8, "application/json");

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(TimeSpan.FromSeconds(timeoutSeconds));

            var response = await _netHttpClient.SendAsync(request, cts.Token);
            var responseBody = await response.Content.ReadAsStringAsync();

            // A service without JSON mode says so about response_format / json_object: asked again
            // without it (any other 400 is a real error and shown as one).
            if (jsonMode && ((int)response.StatusCode == 400 || (int)response.StatusCode == 422)
                && (responseBody.IndexOf("response_format", StringComparison.OrdinalIgnoreCase) >= 0
                    || responseBody.IndexOf("json_object", StringComparison.OrdinalIgnoreCase) >= 0
                    || responseBody.IndexOf("json mode", StringComparison.OrdinalIgnoreCase) >= 0))
                return await CallOpenAi(url, apiKey, model, systemPrompt, userMessage, false, name, cancellationToken, timeoutSeconds);

            if (!response.IsSuccessStatusCode)
                throw new InvalidOperationException($"{name} API error {(int)response.StatusCode}: {responseBody}");

            var parsed = _jsonSerializer.DeserializeFromString<OpenAiResponse>(responseBody);
            return parsed?.choices?.FirstOrDefault()?.message?.content ?? "";
        }

        private async Task<string> CallGemini(string apiKey, string model, string systemPrompt, string userMessage, CancellationToken cancellationToken)
        {
            var url = $"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent";
            var requestBody = $"{{" +
                $"\"systemInstruction\":{{\"parts\":[{{\"text\":{EscapeJsonString(systemPrompt)}}}]}}," +
                $"\"contents\":[{{\"role\":\"user\",\"parts\":[{{\"text\":{EscapeJsonString(userMessage)}}}]}}]" +
                $"}}";

            var request = new System.Net.Http.HttpRequestMessage(System.Net.Http.HttpMethod.Post, url);
            // The key in a header, not in the URL (a proxy may log URLs).
            request.Headers.Add("x-goog-api-key", apiKey);
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
            int maxTokens = (int)Math.Min(16000L, 1024L + Math.Max(0, count) * 60L);
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
            var trimmed = (raw ?? "").Trim();
            if (trimmed.StartsWith("```"))
            {
                var firstNewline = trimmed.IndexOf('\n');
                var lastFence = trimmed.LastIndexOf("```");
                if (firstNewline > 0 && lastFence > firstNewline)
                    trimmed = trimmed.Substring(firstNewline + 1, lastFence - firstNewline - 1).Trim();
            }
            // The array inside a JSON object ({"items":[…]}) or inside text around it: the first
            // complete array of objects or strings (so "[5] picks: [ … ]" gives the list, not "[5]").
            if (!trimmed.StartsWith("["))
            {
                var found = FirstItemArray(trimmed);
                if (found != null) trimmed = found;
            }
            return trimmed;
        }

        // The first [ … ] in text whose first element is an object or a string, up to its own
        // closing bracket (brackets inside strings and nested arrays are skipped). Null: none.
        private static string? FirstItemArray(string text)
        {
            for (int start = text.IndexOf('['); start >= 0; start = text.IndexOf('[', start + 1))
            {
                int i = start + 1;
                while (i < text.Length && char.IsWhiteSpace(text[i])) i++;
                if (i >= text.Length) continue;
                if (text[i] == ']') return text.Substring(start, i - start + 1);   // [] — no picks: an empty list, not a failure
                if (text[i] != '{' && text[i] != '"') continue;
                int depth = 0, end = -1;
                bool inString = false, escaped = false;
                for (int j = start; j < text.Length && end < 0; j++)
                {
                    char c = text[j];
                    if (inString)
                    {
                        if (escaped) escaped = false;
                        else if (c == '\\') escaped = true;
                        else if (c == '"') inString = false;
                        continue;
                    }
                    if (c == '"') inString = true;
                    else if (c == '[' || c == '{') depth++;
                    else if ((c == ']' || c == '}') && --depth == 0) end = c == ']' ? j : int.MaxValue;
                }
                if (end >= 0 && end != int.MaxValue) return text.Substring(start, end - start + 1);
                if (end < 0) return null;   // not closed: a cut-off answer
            }
            return null;
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