using MediaBrowser.Common.Net;
using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Library;
using MediaBrowser.Controller.Net;
using MediaBrowser.Model.Entities;
using MediaBrowser.Model.Logging;
using MediaBrowser.Model.Serialization;
using MediaBrowser.Model.Services;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace HomeScreenCompanion
{
    // The Missing tab: titles the External and AI sources want that the library does not have.
    // Only when Sonarr or Radarr is set up (URL + API key): otherwise the sync records nothing and
    // the tab is hidden. The sync records each source's missing titles (MissingTitles.Record) in
    // missing_titles.json in the plugin data folder; Refresh reads the External lists again.

    public class MissingTitle
    {
        public string Imdb { get; set; } = "";
        public string Tmdb { get; set; } = "";
        public string Title { get; set; } = "";
        public int? Year { get; set; }
        public string MediaType { get; set; } = "";   // "movie" | "tv" | ""
        public int Rank { get; set; }                 // position on the source's list (1 = first)
    }

    public class MissingSource
    {
        public string Key { get; set; } = "";
        public string Group { get; set; } = "";
        public string Kind { get; set; } = "";        // "External" | "AI"
        public string Url { get; set; } = "";
        public DateTime CheckedUtc { get; set; }
        public int ListCount { get; set; }
        public List<MissingTitle> Items { get; set; } = new List<MissingTitle>();
    }

    // What TMDb says about an IMDb id (title, year, type, poster), looked up once.
    public class MissingMeta
    {
        public string Title { get; set; } = "";
        public int? Year { get; set; }
        public string MediaType { get; set; } = "";
        public string Tmdb { get; set; } = "";
        public string Poster { get; set; } = "";
        public DateTime CheckedUtc { get; set; }
    }

    public class MissingFile
    {
        public List<MissingSource> Sources { get; set; } = new List<MissingSource>();
        public Dictionary<string, MissingMeta> Meta { get; set; } = new Dictionary<string, MissingMeta>();
    }

    internal static class MissingTitles
    {
        private static readonly object Gate = new object();
        private static readonly HttpClient Http = new HttpClient { Timeout = TimeSpan.FromSeconds(30) };
        private static MissingFile? _data;
        private static int _enriching;

        private static string FilePath => Path.Combine(Plugin.Instance!.DataFolderPath, "missing_titles.json");

        /// <summary>Sonarr or Radarr has a URL and an API key.</summary>
        internal static bool Enabled
        {
            get
            {
                var c = Plugin.Instance?.Configuration;
                if (c == null) return false;
                bool sonarr = !string.IsNullOrWhiteSpace(c.SonarrUrl) && !string.IsNullOrWhiteSpace(c.SonarrApiKey);
                bool radarr = !string.IsNullOrWhiteSpace(c.RadarrUrl) && !string.IsNullOrWhiteSpace(c.RadarrApiKey);
                return sonarr || radarr;
            }
        }

        internal static string KeyFor(TagConfig t)
        {
            var kind = t.SourceType == "AI" ? "AI" : "External";
            return kind + "\x1E" + GroupIssues.Key(t.Name, t.Tag) + "\x1E" + (kind == "AI" ? "" : (t.Url ?? "").Trim());
        }

        internal static string MediaTypeOf(string? type)
        {
            var t = (type ?? "").Trim().ToLowerInvariant();
            if (t.Length == 0) return "";
            if (t.Contains("movie") || t.Contains("film")) return "movie";
            if (t == "tv" || t.Contains("show") || t.Contains("series")) return "tv";
            return "";
        }

        internal static MissingTitle FromAi(AiListItem item, int rank) => new MissingTitle
        {
            Imdb = (item.imdb_id ?? "").Trim(),
            Title = item.title ?? "",
            Year = item.year,
            MediaType = MediaTypeOf(item.type),
            Rank = rank
        };

        private static MissingFile Load(IJsonSerializer json)
        {
            if (_data != null) return _data;
            try
            {
                if (File.Exists(FilePath)) _data = json.DeserializeFromFile<MissingFile>(FilePath);
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            _data ??= new MissingFile();
            _data.Sources ??= new List<MissingSource>();
            _data.Meta ??= new Dictionary<string, MissingMeta>(StringComparer.OrdinalIgnoreCase);
            if (!(_data.Meta.Comparer is StringComparer))
                _data.Meta = new Dictionary<string, MissingMeta>(_data.Meta, StringComparer.OrdinalIgnoreCase);
            return _data;
        }

        private static void Save(IJsonSerializer json)
        {
            if (_data == null) return;
            try
            {
                Directory.CreateDirectory(Path.GetDirectoryName(FilePath)!);
                var tmp = FilePath + ".tmp";
                json.SerializeToFile(_data, tmp);
                if (File.Exists(FilePath)) File.Delete(FilePath);
                File.Move(tmp, FilePath);
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
        }

        /// <summary>
        /// A source's missing titles from a read that returned a list. Nothing is written when
        /// Sonarr / Radarr are not set up.
        /// </summary>
        internal static void Record(TagConfig source, List<MissingTitle> missing, int listCount, IJsonSerializer json)
        {
            if (!Enabled || source == null || listCount <= 0) return;
            try
            {
                lock (Gate)
                {
                    var data = Load(json);
                    var key = KeyFor(source);
                    data.Sources.RemoveAll(s => s.Key == key);
                    data.Sources.Add(new MissingSource
                    {
                        Key = key,
                        Group = !string.IsNullOrWhiteSpace(source.Name) ? source.Name.Trim() : (source.Tag ?? "").Trim(),
                        Kind = source.SourceType == "AI" ? "AI" : "External",
                        Url = source.SourceType == "AI" ? "" : (source.Url ?? "").Trim(),
                        CheckedUtc = DateTime.UtcNow,
                        ListCount = listCount,
                        Items = missing ?? new List<MissingTitle>()
                    });
                    Save(json);
                }
                EnrichInBackground(json);
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
        }

        internal static MissingFile Snapshot(IJsonSerializer json)
        {
            lock (Gate)
            {
                var data = Load(json);
                return new MissingFile
                {
                    Sources = data.Sources.ToList(),
                    Meta = new Dictionary<string, MissingMeta>(data.Meta, StringComparer.OrdinalIgnoreCase)
                };
            }
        }

        // Title, year, type and poster from TMDb's find by IMDb id, for ids not looked up yet (needs
        // a TMDb key). One run at a time; each id is looked up once.
        internal static void EnrichInBackground(IJsonSerializer json)
        {
            var apiKey = (Plugin.Instance?.Configuration?.TmdbApiKey ?? "").Trim();
            if (apiKey.Length == 0) return;
            if (Interlocked.CompareExchange(ref _enriching, 1, 0) != 0) return;
            Task.Run(async () =>
            {
                try { await Enrich(apiKey, json).ConfigureAwait(false); }
                catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
                finally { Interlocked.Exchange(ref _enriching, 0); }
            });
        }

        internal static async Task Enrich(string apiKey, IJsonSerializer json)
        {
            List<string> todo;
            lock (Gate)
            {
                var data = Load(json);
                todo = data.Sources.SelectMany(s => s.Items)
                    .Select(i => i.Imdb)
                    .Where(id => !string.IsNullOrEmpty(id) && id.StartsWith("tt", StringComparison.Ordinal) && !data.Meta.ContainsKey(id))
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .ToList();
            }
            if (todo.Count == 0) return;
            bool isToken = apiKey.Length > 100 || apiKey.Contains(".");
            var gate = new SemaphoreSlim(4);
            var found = new System.Collections.Concurrent.ConcurrentDictionary<string, MissingMeta>(StringComparer.OrdinalIgnoreCase);
            await Task.WhenAll(todo.Select(async imdb =>
            {
                await gate.WaitAsync().ConfigureAwait(false);
                try
                {
                    var url = $"https://api.themoviedb.org/3/find/{Uri.EscapeDataString(imdb)}?external_source=imdb_id" + (isToken ? "" : "&api_key=" + Uri.EscapeDataString(apiKey));
                    var req = new HttpRequestMessage(HttpMethod.Get, url);
                    if (isToken) req.Headers.Add("Authorization", "Bearer " + apiKey);
                    using (var res = await Http.SendAsync(req).ConfigureAwait(false))
                    {
                        if (!res.IsSuccessStatusCode) return;   // tried again next time
                        var body = await res.Content.ReadAsStringAsync().ConfigureAwait(false);
                        var r = json.DeserializeFromString<TmdbFind>(body);
                        var meta = new MissingMeta { CheckedUtc = DateTime.UtcNow };
                        var m = r?.movie_results?.FirstOrDefault();
                        var t = r?.tv_results?.FirstOrDefault();
                        if (m != null)
                        {
                            meta.MediaType = "movie"; meta.Title = m.title ?? ""; meta.Tmdb = m.id.ToString();
                            meta.Year = YearOf(m.release_date); meta.Poster = m.poster_path ?? "";
                        }
                        else if (t != null)
                        {
                            meta.MediaType = "tv"; meta.Title = t.name ?? ""; meta.Tmdb = t.id.ToString();
                            meta.Year = YearOf(t.first_air_date); meta.Poster = t.poster_path ?? "";
                        }
                        found[imdb] = meta;
                    }
                }
                catch (Exception ex) { PluginLog.Swallowed(ex); }
                finally { gate.Release(); }
            })).ConfigureAwait(false);
            if (found.Count == 0) return;
            lock (Gate)
            {
                var data = Load(json);
                foreach (var kv in found) data.Meta[kv.Key] = kv.Value;
                Save(json);
            }
        }

        private static int? YearOf(string? date) =>
            !string.IsNullOrEmpty(date) && date!.Length >= 4 && int.TryParse(date.Substring(0, 4), out var y) ? y : (int?)null;

        public class TmdbFind
        {
            public List<TmdbFindHit>? movie_results { get; set; }
            public List<TmdbFindHit>? tv_results { get; set; }
        }
        public class TmdbFindHit
        {
            public int id { get; set; }
            public string? title { get; set; }
            public string? name { get; set; }
            public string? release_date { get; set; }
            public string? first_air_date { get; set; }
            public string? poster_path { get; set; }
        }
    }

    // ---- API ----

    [Route("/HomeScreenCompanion/Missing", "GET")]
    [Authenticated(Roles = "Admin")]
    public class MissingListRequest : IReturn<MissingListResponse> { }

    [Route("/HomeScreenCompanion/Missing/Refresh", "POST")]
    [Authenticated(Roles = "Admin")]
    public class MissingRefreshRequest : IReturn<MissingListResponse> { }

    public class MissingListResponse
    {
        public bool Enabled { get; set; }
        public string Message { get; set; } = "";
        public DateTime? CheckedUtc { get; set; }
        public List<MissingSourceInfo> Sources { get; set; } = new List<MissingSourceInfo>();
        public List<MissingRow> Items { get; set; } = new List<MissingRow>();
    }

    public class MissingSourceInfo
    {
        public string Group { get; set; } = "";
        public string Kind { get; set; } = "";
        public int Urls { get; set; }
        public int ListCount { get; set; }
        public int MissingCount { get; set; }
        public DateTime CheckedUtc { get; set; }
    }

    public class MissingRow
    {
        public string Imdb { get; set; } = "";
        public string Tmdb { get; set; } = "";
        public string Title { get; set; } = "";
        public int? Year { get; set; }
        public string MediaType { get; set; } = "";
        public string Poster { get; set; } = "";
        public int Rank { get; set; }   // best rank over its sources
        public List<MissingRowSource> Lists { get; set; } = new List<MissingRowSource>();
    }

    public class MissingRowSource
    {
        public string Group { get; set; } = "";
        public string Kind { get; set; } = "";
        public int Rank { get; set; }
    }

    public class MissingService : IService
    {
        private readonly IJsonSerializer _json;
        private readonly IHttpClient _httpClient;
        private readonly ILibraryManager _libraryManager;
        private readonly ILogger _logger;
        private static readonly SemaphoreSlim RefreshGate = new SemaphoreSlim(1, 1);

        public MissingService(IJsonSerializer jsonSerializer, IHttpClient httpClient, ILibraryManager libraryManager, ILogManager logManager)
        {
            _json = jsonSerializer;
            _httpClient = httpClient;
            _libraryManager = libraryManager;
            _logger = logManager.GetLogger("HomeScreenCompanion");
        }

        // Movies and shows in the library by IMDb id; top-list and ranked copies don't count (as in the sync).
        private HashSet<string> LibraryImdbIds()
        {
            var topListsFolder = Path.Combine(Plugin.Instance!.DataFolderPath, "toplists") + Path.DirectorySeparatorChar;
            var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var item in _libraryManager.GetItemList(new InternalItemsQuery
            {
                IncludeItemTypes = new[] { "Movie", "Series" },
                Recursive = true,
                IsVirtualItem = false
            }))
            {
                if (item.LocationType != LocationType.FileSystem) continue;
                if (!string.IsNullOrEmpty(item.Path) &&
                    (item.Path.StartsWith(topListsFolder, StringComparison.OrdinalIgnoreCase) || RankedCollections.IsRankedPath(item.Path)))
                    continue;
                var imdb = item.GetProviderId("Imdb");
                if (!string.IsNullOrEmpty(imdb)) ids.Add(imdb);
            }
            return ids;
        }

        private static List<TagConfig> ListSources() =>
            (Plugin.Instance?.Configuration?.Tags ?? new List<TagConfig>())
                .Where(t => t.Active && !string.IsNullOrWhiteSpace(t.Tag)
                    && (t.SourceType == "AI" || string.IsNullOrEmpty(t.SourceType) || t.SourceType == "External"))
                .ToList();

        public object Get(MissingListRequest request) => Build();

        private MissingListResponse Build()
        {
            if (!MissingTitles.Enabled) return new MissingListResponse { Enabled = false };
            var data = MissingTitles.Snapshot(_json);
            // Sources that are gone, switched off or changed since are left out.
            var live = new HashSet<string>(ListSources().Select(MissingTitles.KeyFor));
            var sources = data.Sources.Where(s => live.Contains(s.Key)).ToList();
            var inLibrary = LibraryImdbIds();

            var rows = new Dictionary<string, MissingRow>(StringComparer.OrdinalIgnoreCase);
            var order = new List<MissingRow>();
            foreach (var src in sources)
            {
                foreach (var it in src.Items)
                {
                    if (!string.IsNullOrEmpty(it.Imdb) && inLibrary.Contains(it.Imdb!)) continue;
                    string id = !string.IsNullOrEmpty(it.Imdb) ? it.Imdb!
                        : !string.IsNullOrEmpty(it.Tmdb) ? "tmdb:" + (it.MediaType ?? "") + ":" + it.Tmdb
                        : "title:" + (it.Title ?? "").Trim().ToLowerInvariant() + "|" + it.Year;
                    if (!rows.TryGetValue(id, out var row))
                    {
                        row = new MissingRow { Imdb = it.Imdb ?? "", Tmdb = it.Tmdb ?? "", Title = it.Title ?? "", Year = it.Year, MediaType = it.MediaType ?? "", Rank = it.Rank };
                        if (!string.IsNullOrEmpty(it.Imdb) && data.Meta.TryGetValue(it.Imdb!, out var meta))
                        {
                            if (meta.Title.Length > 0 && row.Title.Length == 0) row.Title = meta.Title;
                            row.Year ??= meta.Year;
                            if (row.MediaType.Length == 0) row.MediaType = meta.MediaType;
                            if (row.Tmdb.Length == 0) row.Tmdb = meta.Tmdb;
                            if (meta.Poster.Length > 0) row.Poster = "https://image.tmdb.org/t/p/w154" + meta.Poster;
                        }
                        rows[id] = row;
                        order.Add(row);
                    }
                    else
                    {
                        if (row.Year == null) row.Year = it.Year;
                        if (row.MediaType.Length == 0) row.MediaType = it.MediaType ?? "";
                        if (row.Tmdb.Length == 0) row.Tmdb = it.Tmdb ?? "";
                        if (it.Rank > 0 && (row.Rank <= 0 || it.Rank < row.Rank)) row.Rank = it.Rank;
                    }
                    // One chip per source (a source with several URLs: its best rank).
                    var chip = row.Lists.FirstOrDefault(l => l.Group == src.Group && l.Kind == src.Kind);
                    if (chip == null) row.Lists.Add(new MissingRowSource { Group = src.Group, Kind = src.Kind, Rank = it.Rank });
                    else if (it.Rank > 0 && it.Rank < chip.Rank) chip.Rank = it.Rank;
                }
            }

            var infos = sources.GroupBy(s => s.Group + "\x1F" + s.Kind).Select(g => new MissingSourceInfo
            {
                Group = g.First().Group,
                Kind = g.First().Kind,
                Urls = g.Count(),
                ListCount = g.Sum(s => s.ListCount),
                MissingCount = order.Count(r => r.Lists.Any(l => l.Group == g.First().Group && l.Kind == g.First().Kind)),
                CheckedUtc = g.Max(s => s.CheckedUtc)
            }).OrderBy(i => i.Group, StringComparer.OrdinalIgnoreCase).ToList();

            if (order.Any(r => !string.IsNullOrEmpty(r.Imdb) && r.Poster.Length == 0 && !data.Meta.ContainsKey(r.Imdb)))
                MissingTitles.EnrichInBackground(_json);

            return new MissingListResponse
            {
                Enabled = true,
                CheckedUtc = sources.Count > 0 ? sources.Max(s => s.CheckedUtc) : (DateTime?)null,
                Sources = infos,
                Items = order
            };
        }

        // Reads every active External list again, as the source Test does, and records what is
        // missing. AI sources keep what the last sync recorded (asking again costs and changes the answer).
        public async Task<object> Post(MissingRefreshRequest request)
        {
            if (!MissingTitles.Enabled) return new MissingListResponse { Enabled = false };
            var config = Plugin.Instance!.Configuration;
            await RefreshGate.WaitAsync().ConfigureAwait(false);
            try
            {
                var inLibrary = LibraryImdbIds();
                var external = ListSources().Where(t => t.SourceType != "AI" && !string.IsNullOrWhiteSpace(t.Url)).ToList();
                var gate = new SemaphoreSlim(3);
                await Task.WhenAll(external.Select(async src =>
                {
                    await gate.WaitAsync().ConfigureAwait(false);
                    try
                    {
                        var blacklist = new HashSet<string>(src.Blacklist ?? new List<string>(), StringComparer.OrdinalIgnoreCase);
                        var fetcher = new ListFetcher(_httpClient, _json);
                        int limit = src.Limit <= 0 ? 10000 : src.Limit;
                        var items = await fetcher.FetchItems(src.Url, limit, config.TraktClientId, config.MdblistApiKey, config.TmdbApiKey, CancellationToken.None,
                            e => !string.IsNullOrEmpty(e.Imdb) && !blacklist.Contains(e.Imdb) && inLibrary.Contains(e.Imdb)).ConfigureAwait(false);
                        if (items == null || items.Count == 0) return;   // unreadable: keep the last result
                        MissingTitles.Record(src, Missing(items, inLibrary, blacklist), items.Count, _json);
                    }
                    catch (Exception ex) { _logger.Debug($"Missing: refresh of {src.Url} failed: {ex.Message}"); }
                    finally { gate.Release(); }
                })).ConfigureAwait(false);
            }
            finally { RefreshGate.Release(); }
            return Build();
        }

        /// <summary>The titles of an External list that are not in the library (same rules as the sync).</summary>
        internal static List<MissingTitle> Missing(List<ExternalItemDto> items, ICollection<string> inLibrary, HashSet<string> blacklist)
        {
            var list = new List<MissingTitle>();
            for (int i = 0; i < items.Count; i++)
            {
                var e = items[i];
                if (string.IsNullOrEmpty(e.Imdb) || blacklist.Contains(e.Imdb) || inLibrary.Contains(e.Imdb)) continue;
                list.Add(new MissingTitle { Imdb = e.Imdb, Tmdb = e.Tmdb ?? "", Title = e.Name ?? "", MediaType = MissingTitles.MediaTypeOf(e.MediaType), Rank = i + 1 });
            }
            return list;
        }
    }
}
