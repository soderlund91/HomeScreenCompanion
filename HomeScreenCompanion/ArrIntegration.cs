using MediaBrowser.Controller.Net;
using MediaBrowser.Model.Logging;
using MediaBrowser.Model.Serialization;
using MediaBrowser.Model.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace HomeScreenCompanion
{
    // Sonarr / Radarr: titles on a list that are not in the library show whether they are already
    // in Sonarr (shows) or Radarr (movies), with an Add button. Add posts to /api/v3/series or /api/v3/movie.

    [Route("/HomeScreenCompanion/Arr/Test", "POST")]
    [Authenticated(Roles = "Admin")]
    public class ArrTestRequest : IReturn<ArrTestResponse>
    {
        public string Kind { get; set; } = "";   // "sonarr" | "radarr"
        public string Url { get; set; } = "";
        public string ApiKey { get; set; } = "";
    }

    public class ArrTestResponse
    {
        public bool Success { get; set; }
        public string Message { get; set; } = "";
        public string Version { get; set; } = "";
    }

    [Route("/HomeScreenCompanion/Arr/Status", "POST")]
    [Authenticated(Roles = "Admin")]
    public class ArrStatusRequest : IReturn<ArrStatusResponse>
    {
        public List<ArrStatusItem> Items { get; set; } = new List<ArrStatusItem>();
    }

    public class ArrStatusItem
    {
        public string Imdb { get; set; } = "";
        public string Tmdb { get; set; } = "";
        public string MediaType { get; set; } = "";  // "movie" | "tv" | "" (unknown)
        public bool InSonarr { get; set; }
        public bool InRadarr { get; set; }
    }

    public class ArrStatusResponse
    {
        public bool SonarrConfigured { get; set; }
        public bool RadarrConfigured { get; set; }
        public List<ArrStatusItem> Items { get; set; } = new List<ArrStatusItem>();
    }

    [Route("/HomeScreenCompanion/Arr/Options", "GET")]
    [Authenticated(Roles = "Admin")]
    public class ArrOptionsRequest : IReturn<ArrOptionsResponse>
    {
        public string Kind { get; set; } = "";
    }

    public class ArrOptionsResponse
    {
        public bool Success { get; set; }
        public string Message { get; set; } = "";
        public List<string> RootFolders { get; set; } = new List<string>();
        public List<ArrProfile> QualityProfiles { get; set; } = new List<ArrProfile>();
        public string LastRootFolderPath { get; set; } = "";
    }

    public class ArrProfile
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    [Route("/HomeScreenCompanion/Arr/Add", "POST")]
    [Authenticated(Roles = "Admin")]
    public class ArrAddRequest : IReturn<ArrAddResponse>
    {
        public string Kind { get; set; } = "";
        public string SourceId { get; set; } = "";   // IMDb id, or a TMDb id (Radarr)
        public string Title { get; set; } = "";
        public int QualityProfileId { get; set; }
        public string RootFolderPath { get; set; } = "";
        public string MinimumAvailability { get; set; } = "announced";
        public bool Monitored { get; set; } = true;
    }

    public class ArrAddResponse
    {
        public bool Success { get; set; }
        public bool AlreadyIn { get; set; }
        public string Message { get; set; } = "";
    }

    public class ArrService : IService
    {
        private static readonly HttpClient Http = new HttpClient { Timeout = TimeSpan.FromMinutes(5) };
        private const int MaxConcurrentSonarrLookups = 3;
        private static readonly TimeSpan CacheFor = TimeSpan.FromMinutes(2);

        // Full Sonarr / Radarr library lists, kept briefly so a popup and its refresh after an
        // Add don't each download thousands of entries.
        private static readonly object CacheLock = new object();
        private static readonly Dictionary<string, (DateTime At, object List)> LibraryCache = new Dictionary<string, (DateTime, object)>();
        private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, int?> ImdbTvdbCache = new System.Collections.Concurrent.ConcurrentDictionary<string, int?>();

        private readonly IJsonSerializer _json;
        private readonly ILogger _logger;

        public ArrService(IJsonSerializer jsonSerializer, ILogManager logManager)
        {
            _json = jsonSerializer;
            _logger = logManager.GetLogger("HomeScreenCompanion");
        }

        private static PluginConfiguration Config => Plugin.Instance!.Configuration;

        private static (string Url, string Key) Settings(string kind)
        {
            var c = Config;
            return kind == "sonarr"
                ? ((c.SonarrUrl ?? "").Trim().TrimEnd('/'), (c.SonarrApiKey ?? "").Trim())
                : ((c.RadarrUrl ?? "").Trim().TrimEnd('/'), (c.RadarrApiKey ?? "").Trim());
        }

        private static string Name(string kind) => kind == "sonarr" ? "Sonarr" : "Radarr";

        private static async Task<(int Status, string Body)> Send(HttpMethod method, string baseUrl, string apiKey, string path, string? jsonBody = null)
        {
            var req = new HttpRequestMessage(method, baseUrl + path);
            req.Headers.Add("X-Api-Key", apiKey);
            if (jsonBody != null) req.Content = new StringContent(jsonBody, Encoding.UTF8, "application/json");
            using (var res = await Http.SendAsync(req).ConfigureAwait(false))
            {
                var body = await res.Content.ReadAsStringAsync().ConfigureAwait(false);
                return ((int)res.StatusCode, body);
            }
        }

        private async Task<T?> GetJson<T>(string baseUrl, string apiKey, string path) where T : class
        {
            var (status, body) = await Send(HttpMethod.Get, baseUrl, apiKey, path).ConfigureAwait(false);
            if (status < 200 || status > 299) throw new InvalidOperationException("HTTP " + status);
            return _json.DeserializeFromString<T>(body);
        }

        // ---- Test connection (Settings) ----

        public async Task<object> Post(ArrTestRequest request)
        {
            var kind = (request.Kind ?? "").ToLowerInvariant();
            var url = (request.Url ?? "").Trim().TrimEnd('/');
            var key = (request.ApiKey ?? "").Trim();
            if (url.Length == 0 || key.Length == 0)
                return new ArrTestResponse { Success = false, Message = "Connection failed: URL and API key are required." };
            try
            {
                var status = await GetJson<ArrSystemStatus>(url, key, "/api/v3/system/status").ConfigureAwait(false);
                var version = status?.version ?? "";
                return new ArrTestResponse { Success = true, Version = version, Message = $"Connection successful! {Name(kind)} version: {(version.Length > 0 ? version : "unknown")}" };
            }
            catch (Exception ex)
            {
                return new ArrTestResponse { Success = false, Message = "Connection failed: " + ex.Message };
            }
        }

        // ---- Library lists ----

        private async Task<List<ArrLibraryEntry>?> LibraryList(string kind, string baseUrl, string apiKey)
        {
            var cacheKey = kind + "|" + baseUrl + "|" + apiKey;
            lock (CacheLock)
            {
                if (LibraryCache.TryGetValue(cacheKey, out var hit) && DateTime.UtcNow - hit.At < CacheFor)
                    return (List<ArrLibraryEntry>)hit.List;
            }
            try
            {
                var list = await GetJson<List<ArrLibraryEntry>>(baseUrl, apiKey, kind == "sonarr" ? "/api/v3/series" : "/api/v3/movie").ConfigureAwait(false);
                if (list == null) return null;
                lock (CacheLock) LibraryCache[cacheKey] = (DateTime.UtcNow, list);
                return list;
            }
            catch (Exception ex)
            {
                _logger.Debug($"Arr: could not read the {Name(kind)} library: {ex.Message}");
                return null;
            }
        }

        private static void ForgetLibrary(string kind)
        {
            lock (CacheLock)
            {
                foreach (var k in LibraryCache.Keys.Where(k => k.StartsWith(kind + "|", StringComparison.Ordinal)).ToList())
                    LibraryCache.Remove(k);
            }
        }

        private async Task<int?> SonarrTvdbId(string imdb, string baseUrl, string apiKey)
        {
            var cacheKey = baseUrl + "|" + imdb;
            if (ImdbTvdbCache.TryGetValue(cacheKey, out var cached)) return cached;
            int? tvdb = null;
            try
            {
                var results = await GetJson<List<ArrLibraryEntry>>(baseUrl, apiKey, "/api/v3/series/lookup?term=imdb:" + Uri.EscapeDataString(imdb)).ConfigureAwait(false);
                var first = results?.FirstOrDefault();
                if (first != null && first.tvdbId > 0) tvdb = first.tvdbId;
            }
            catch (Exception ex)
            {
                _logger.Debug($"Arr: Sonarr lookup failed for {imdb}: {ex.Message}");
            }
            ImdbTvdbCache[cacheKey] = tvdb;
            return tvdb;
        }

        // ---- Status of titles not in the library ----

        public async Task<object> Post(ArrStatusRequest request)
        {
            var (sUrl, sKey) = Settings("sonarr");
            var (rUrl, rKey) = Settings("radarr");
            var response = new ArrStatusResponse
            {
                SonarrConfigured = sUrl.Length > 0 && sKey.Length > 0,
                RadarrConfigured = rUrl.Length > 0 && rKey.Length > 0
            };
            var items = request.Items ?? new List<ArrStatusItem>();
            if (!response.SonarrConfigured && !response.RadarrConfigured)
            {
                response.Items = items;
                return response;
            }

            var seriesTask = response.SonarrConfigured ? LibraryList("sonarr", sUrl, sKey) : Task.FromResult<List<ArrLibraryEntry>?>(null);
            var moviesTask = response.RadarrConfigured ? LibraryList("radarr", rUrl, rKey) : Task.FromResult<List<ArrLibraryEntry>?>(null);
            var series = await seriesTask.ConfigureAwait(false);
            var movies = await moviesTask.ConfigureAwait(false);

            var seriesByImdb = new HashSet<string>((series ?? new List<ArrLibraryEntry>()).Where(s => !string.IsNullOrEmpty(s.imdbId)).Select(s => s.imdbId!), StringComparer.Ordinal);
            var seriesTvdb = new HashSet<int>((series ?? new List<ArrLibraryEntry>()).Select(s => s.tvdbId));
            var moviesByImdb = new HashSet<string>((movies ?? new List<ArrLibraryEntry>()).Where(m => !string.IsNullOrEmpty(m.imdbId)).Select(m => m.imdbId!), StringComparer.Ordinal);
            var moviesTmdb = new HashSet<int>((movies ?? new List<ArrLibraryEntry>()).Select(m => m.tmdbId));

            foreach (var it in items)
            {
                it.Imdb = (it.Imdb ?? "").Trim();
                it.Tmdb = (it.Tmdb ?? "").Trim();
                it.MediaType = (it.MediaType ?? "").Trim().ToLowerInvariant();
                // A list that doesn't say movie or show: Sonarr / Radarr already holding it settles it.
                if (it.MediaType.Length == 0 && it.Imdb.Length > 0)
                {
                    if (moviesByImdb.Contains(it.Imdb)) it.MediaType = "movie";
                    else if (seriesByImdb.Contains(it.Imdb)) it.MediaType = "tv";
                }
            }
            await ResolveUnknownTypes(items).ConfigureAwait(false);

            // Sonarr: by IMDb id on its library, else via a lookup to the TVDB id (3 at a time).
            var tvToLookUp = new List<ArrStatusItem>();
            foreach (var it in items)
            {
                if (it.MediaType == "movie" && movies != null)
                {
                    it.InRadarr = it.Imdb.StartsWith("tt", StringComparison.Ordinal)
                        ? moviesByImdb.Contains(it.Imdb)
                        : int.TryParse(it.Tmdb, out var tmdb) && moviesTmdb.Contains(tmdb);
                }
                else if (it.MediaType == "tv" && series != null && it.Imdb.Length > 0)
                {
                    if (seriesByImdb.Contains(it.Imdb)) it.InSonarr = true;
                    else if (it.Imdb.StartsWith("tt", StringComparison.Ordinal)) tvToLookUp.Add(it);
                }
            }
            if (tvToLookUp.Count > 0)
            {
                var gate = new SemaphoreSlim(MaxConcurrentSonarrLookups);
                await Task.WhenAll(tvToLookUp.Select(async it =>
                {
                    await gate.WaitAsync().ConfigureAwait(false);
                    try
                    {
                        var tvdb = await SonarrTvdbId(it.Imdb, sUrl, sKey).ConfigureAwait(false);
                        it.InSonarr = tvdb.HasValue && seriesTvdb.Contains(tvdb.Value);
                    }
                    finally { gate.Release(); }
                })).ConfigureAwait(false);
            }

            response.Items = items;
            return response;
        }

        // Titles whose list didn't say movie or show: TMDb's find by IMDb id,
        // when a TMDb key is set. Otherwise they stay unknown.
        private async Task ResolveUnknownTypes(List<ArrStatusItem> items)
        {
            var apiKey = (Config.TmdbApiKey ?? "").Trim();
            var unknown = items.Where(i => i.MediaType.Length == 0 && i.Imdb.StartsWith("tt", StringComparison.Ordinal)).ToList();
            if (apiKey.Length == 0 || unknown.Count == 0) return;
            bool isToken = apiKey.Length > 100 || apiKey.Contains(".");
            var gate = new SemaphoreSlim(4);
            await Task.WhenAll(unknown.Select(async it =>
            {
                await gate.WaitAsync().ConfigureAwait(false);
                try
                {
                    var url = $"https://api.themoviedb.org/3/find/{Uri.EscapeDataString(it.Imdb)}?external_source=imdb_id" + (isToken ? "" : "&api_key=" + Uri.EscapeDataString(apiKey));
                    var req = new HttpRequestMessage(HttpMethod.Get, url);
                    if (isToken) req.Headers.Add("Authorization", "Bearer " + apiKey);
                    using (var res = await Http.SendAsync(req).ConfigureAwait(false))
                    {
                        if (!res.IsSuccessStatusCode) return;
                        var found = _json.DeserializeFromString<TmdbFindResult>(await res.Content.ReadAsStringAsync().ConfigureAwait(false));
                        if (found?.movie_results?.Count > 0) { it.MediaType = "movie"; if (it.Tmdb.Length == 0) it.Tmdb = found.movie_results[0].id.ToString(); }
                        else if (found?.tv_results?.Count > 0) it.MediaType = "tv";
                    }
                }
                catch (Exception ex) { _logger.Debug($"Arr: TMDb find failed for {it.Imdb}: {ex.Message}"); }
                finally { gate.Release(); }
            })).ConfigureAwait(false);
        }

        // ---- Add dialog: root folders and quality profiles ----

        public async Task<object> Get(ArrOptionsRequest request)
        {
            var kind = (request.Kind ?? "").ToLowerInvariant() == "sonarr" ? "sonarr" : "radarr";
            var (url, key) = Settings(kind);
            if (url.Length == 0 || key.Length == 0)
                return new ArrOptionsResponse { Success = false, Message = $"{Name(kind)} URL or API Key not set in settings." };
            try
            {
                var foldersTask = GetJson<List<ArrRootFolder>>(url, key, "/api/v3/rootfolder");
                var profilesTask = GetJson<List<ArrQualityProfile>>(url, key, "/api/v3/qualityprofile");
                var folders = await foldersTask.ConfigureAwait(false) ?? new List<ArrRootFolder>();
                var profiles = await profilesTask.ConfigureAwait(false) ?? new List<ArrQualityProfile>();
                return new ArrOptionsResponse
                {
                    Success = true,
                    RootFolders = folders.Select(f => f.path ?? "").Where(p => p.Length > 0).ToList(),
                    QualityProfiles = profiles.Select(p => new ArrProfile { Id = p.id, Name = p.name ?? "" }).ToList(),
                    LastRootFolderPath = (kind == "sonarr" ? Config.SonarrLastRootFolderPath : Config.RadarrLastRootFolderPath) ?? ""
                };
            }
            catch (Exception ex)
            {
                return new ArrOptionsResponse { Success = false, Message = $"Failed to load {Name(kind)} options: {ex.Message}" };
            }
        }

        // ---- Add ----

        public async Task<object> Post(ArrAddRequest request)
        {
            var kind = (request.Kind ?? "").ToLowerInvariant() == "sonarr" ? "sonarr" : "radarr";
            var (url, key) = Settings(kind);
            if (url.Length == 0 || key.Length == 0)
                return new ArrAddResponse { Success = false, Message = $"{Name(kind)} URL or API Key not set in settings." };
            var sourceId = (request.SourceId ?? "").Trim();
            try
            {
                var result = kind == "sonarr"
                    ? await AddToSonarr(url, key, sourceId, request).ConfigureAwait(false)
                    : await AddToRadarr(url, key, sourceId, request).ConfigureAwait(false);
                if (result.Success)
                {
                    ForgetLibrary(kind);
                    var root = (request.RootFolderPath ?? "").Trim();
                    if (root.Length > 0)
                    {
                        if (kind == "sonarr") Config.SonarrLastRootFolderPath = root;
                        else Config.RadarrLastRootFolderPath = root;
                        Plugin.Instance!.SaveConfiguration();
                    }
                }
                return result;
            }
            catch (Exception ex)
            {
                return new ArrAddResponse { Success = false, Message = (kind == "sonarr" ? "Failed to add show: " : "Failed to add movie: ") + ex.Message };
            }
        }

        private async Task<ArrAddResponse> AddToSonarr(string url, string key, string imdbId, ArrAddRequest request)
        {
            const string already = "This show is already in Sonarr!";
            // Step 1: already in Sonarr?
            List<ArrLibraryEntry>? existing = null;
            try { existing = await GetJson<List<ArrLibraryEntry>>(url, key, "/api/v3/series").ConfigureAwait(false); } catch { }
            if (existing != null && existing.Any(s => s.imdbId == imdbId))
                return new ArrAddResponse { AlreadyIn = true, Message = already };

            // Step 2: IMDb id -> TVDB id
            var lookup = await GetJson<List<ArrLibraryEntry>>(url, key, "/api/v3/series/lookup?term=imdb:" + Uri.EscapeDataString(imdbId)).ConfigureAwait(false);
            var series = lookup?.FirstOrDefault();
            if (series == null)
                return new ArrAddResponse { Message = $"Failed to add show: Show not found on Sonarr with IMDb ID: {imdbId}" };
            if (existing != null && existing.Any(s => s.tvdbId == series.tvdbId))
                return new ArrAddResponse { AlreadyIn = true, Message = already };

            // Step 3: add
            var body = "{"
                + "\"tvdbId\":" + series.tvdbId + ","
                + "\"title\":" + ListFetcher.EscapeJsonString(series.title ?? "") + ","
                + "\"qualityProfileId\":" + request.QualityProfileId + ","
                + "\"rootFolderPath\":" + ListFetcher.EscapeJsonString(request.RootFolderPath ?? "") + ","
                + "\"monitored\":true,"
                + "\"seasonFolder\":true,"
                + "\"addOptions\":{\"searchForMissingEpisodes\":true}"
                + "}";
            var (status, resBody) = await Send(HttpMethod.Post, url, key, "/api/v3/series", body).ConfigureAwait(false);
            if (status >= 200 && status <= 299)
                return new ArrAddResponse { Success = true, Message = "Show added to Sonarr successfully!" };
            return ArrError(resBody, status, "SeriesExistsValidator", already, "Failed to add show: ");
        }

        private async Task<ArrAddResponse> AddToRadarr(string url, string key, string sourceId, ArrAddRequest request)
        {
            const string already = "This movie is already in Radarr!";
            // Step 1: already in Radarr?
            List<ArrLibraryEntry>? existing = null;
            try { existing = await GetJson<List<ArrLibraryEntry>>(url, key, "/api/v3/movie").ConfigureAwait(false); } catch { }
            bool isImdb = sourceId.StartsWith("tt", StringComparison.Ordinal);
            int.TryParse(sourceId, out var sourceTmdb);
            if (existing != null && existing.Any(m => m.imdbId == sourceId || (!isImdb && m.tmdbId == sourceTmdb)))
                return new ArrAddResponse { AlreadyIn = true, Message = already };

            // Step 2: TMDb id
            int tmdbId;
            if (isImdb)
            {
                var movies = await GetJson<List<ArrLibraryEntry>>(url, key, "/api/v3/movie/lookup?term=imdb:" + Uri.EscapeDataString(sourceId)).ConfigureAwait(false);
                if (movies == null || movies.Count == 0)
                    return new ArrAddResponse { Message = $"Failed to add movie: Movie not found on Radarr with IMDb ID: {sourceId}" };
                tmdbId = movies[0].tmdbId;
                if (tmdbId <= 0)
                    return new ArrAddResponse { Message = $"Failed to add movie: Lookup for {sourceId} did not return a TMDB ID." };
                if (existing != null && existing.Any(m => m.tmdbId == tmdbId))
                    return new ArrAddResponse { AlreadyIn = true, Message = already };
            }
            else if (sourceTmdb > 0 && sourceId.All(char.IsDigit))
            {
                tmdbId = sourceTmdb;
            }
            else
            {
                return new ArrAddResponse { Message = $"Failed to add movie: Invalid Source ID format: {sourceId}" };
            }

            // Step 3: add
            var body = "{"
                + "\"title\":" + ListFetcher.EscapeJsonString(request.Title ?? "") + ","
                + "\"qualityProfileId\":" + request.QualityProfileId + ","
                + "\"rootFolderPath\":" + ListFetcher.EscapeJsonString(request.RootFolderPath ?? "") + ","
                + "\"monitored\":" + (request.Monitored ? "true" : "false") + ","
                + "\"minimumAvailability\":" + ListFetcher.EscapeJsonString(request.MinimumAvailability ?? "announced") + ","
                + "\"addOptions\":{\"searchForMovie\":true},"
                + "\"tmdbId\":" + tmdbId
                + "}";
            var (status, resBody) = await Send(HttpMethod.Post, url, key, "/api/v3/movie", body).ConfigureAwait(false);
            if (status >= 200 && status <= 299)
                return new ArrAddResponse { Success = true, Message = "Movie added to Radarr successfully!" };
            return ArrError(resBody, status, "MovieExistsValidator", already, "Failed to add movie: ");
        }

        // Sonarr / Radarr answer a failed add with a list of validation errors.
        private ArrAddResponse ArrError(string body, int status, string existsCode, string already, string failPrefix)
        {
            try
            {
                var errors = _json.DeserializeFromString<List<ArrValidationError>>(body);
                var first = errors?.FirstOrDefault();
                if (first != null)
                {
                    if (first.errorCode == existsCode) return new ArrAddResponse { AlreadyIn = true, Message = already };
                    if (!string.IsNullOrEmpty(first.errorMessage)) return new ArrAddResponse { Message = "Error: " + first.errorMessage };
                    if (!string.IsNullOrEmpty(first.propertyName)) return new ArrAddResponse { Message = $"Error: {first.propertyName} - {first.attemptedValue ?? "Invalid value"}" };
                }
            }
            catch { }
            return new ArrAddResponse { Message = $"{failPrefix}HTTP {status}: {body}" };
        }

        // ---- JSON shapes ----

        public class ArrSystemStatus { public string? version { get; set; } }
        public class ArrLibraryEntry
        {
            public int tvdbId { get; set; }
            public int tmdbId { get; set; }
            public string? imdbId { get; set; }
            public string? title { get; set; }
        }
        public class ArrRootFolder { public string? path { get; set; } }
        public class ArrQualityProfile { public int id { get; set; } public string? name { get; set; } }
        public class ArrValidationError
        {
            public string? propertyName { get; set; }
            public string? errorMessage { get; set; }
            public string? errorCode { get; set; }
            public string? attemptedValue { get; set; }
        }
        public class TmdbFindResult
        {
            public List<TmdbFindEntry>? movie_results { get; set; }
            public List<TmdbFindEntry>? tv_results { get; set; }
        }
        public class TmdbFindEntry { public int id { get; set; } }
    }
}
