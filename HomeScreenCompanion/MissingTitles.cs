using MediaBrowser.Common.Net;
using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Entities.Movies;
using MediaBrowser.Controller.Entities.TV;
using MediaBrowser.Controller.Library;
using MediaBrowser.Model.Entities;
using MediaBrowser.Model.Serialization;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using HttpRequestOptions = MediaBrowser.Common.Net.HttpRequestOptions;

namespace HomeScreenCompanion
{
    /// <summary>
    /// "Show missing titles" (Settings): the titles on the External and AI lists of Tags &amp;
    /// Collections that are not in the library, and the aired episodes the library lacks of the
    /// shows on those lists.
    ///
    /// A sync saves what each source's list held (plugin data folder, missing_titles/sources.json);
    /// a source that gave nothing keeps its last entries. The episodes come from TMDb (needs the
    /// TMDb key) and are cached in missing_titles/tmdb_series.json. The tab compares the saved
    /// lists with the library as it is when it is opened, so a title added since is already gone.
    /// </summary>
    internal static class MissingTitles
    {
        // ---- Stored files ----

        public sealed class SourcesFile
        {
            public DateTime UpdatedUtc { get; set; }
            public List<StoredSource> Sources { get; set; } = new List<StoredSource>();
        }

        public sealed class StoredSource
        {
            public string Key { get; set; } = "";
            public string Name { get; set; } = "";
            public DateTime ReadUtc { get; set; }
            public List<ExternalItemDto> Items { get; set; } = new List<ExternalItemDto>();
        }

        public sealed class SeriesCacheFile
        {
            public List<CachedSeries> Series { get; set; } = new List<CachedSeries>();
            public List<IdLookup> Lookups { get; set; } = new List<IdLookup>();
        }

        public sealed class CachedSeries
        {
            public string TmdbId { get; set; } = "";
            public string Status { get; set; } = "";
            public string? TvdbId { get; set; }
            public bool IdsRead { get; set; }       // TvdbId was asked for (caches from before it was are read again)
            public DateTime FetchedUtc { get; set; }
            public List<CachedSeason> Seasons { get; set; } = new List<CachedSeason>();
        }

        public sealed class CachedSeason
        {
            public int Number { get; set; }
            public int EpisodeCount { get; set; }
            public List<CachedEpisode> Episodes { get; set; } = new List<CachedEpisode>();
        }

        public sealed class CachedEpisode
        {
            public int Number { get; set; }
            public string? Name { get; set; }
            public string? AirDate { get; set; }   // yyyy-MM-dd, null = not announced
        }

        // An IMDb id whose TMDb id or type the list did not give, as TMDb's /find answered it.
        public sealed class IdLookup
        {
            public string Imdb { get; set; } = "";
            public string? Tmdb { get; set; }
            public string? Type { get; set; }       // "movie" / "show", null = TMDb does not know it
            public DateTime CheckedUtc { get; set; }
        }

        private static readonly object Gate = new object();
        private static string Folder => Path.Combine(Plugin.Instance!.DataFolderPath, "missing_titles");
        private static string SourcesPath => Path.Combine(Folder, "sources.json");
        private static string SeriesPath => Path.Combine(Folder, "tmdb_series.json");

        private static T Load<T>(string path, IJsonSerializer json) where T : class, new()
        {
            try { if (File.Exists(path)) return json.DeserializeFromFile<T>(path) ?? new T(); }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            return new T();
        }

        // ---- Sources ----

        /// <summary>True for a source with a list of its own: an External URL or an AI prompt.</summary>
        internal static bool IsListSource(TagConfig t) =>
            t.SourceType == "AI" || ((string.IsNullOrEmpty(t.SourceType) || t.SourceType == "External") && !string.IsNullOrWhiteSpace(t.Url));

        internal static string SourceKey(TagConfig t) => t.SourceType == "AI"
            ? "ai|" + (t.Tag ?? "").Trim() + "|" + (t.AiPrompt ?? "").Trim()
            : "url|" + (t.Url ?? "").Trim();

        private static string SourceName(TagConfig t) => !string.IsNullOrWhiteSpace(t.Name) ? t.Name.Trim() : (t.Tag ?? "").Trim();

        internal static ExternalItemDto FromAi(AiListItem a)
        {
            var type = (a.type ?? "").Trim().ToLowerInvariant();
            return new ExternalItemDto
            {
                Name = a.title.Trim(),
                Imdb = (a.imdb_id ?? "").Trim(),
                Type = type == "movie" || type == "film" ? "movie" : type == "show" || type == "series" || type == "tv" ? "show" : null,
                Year = a.year > 0 ? a.year : null
            };
        }

        /// <summary>What the sources of one run read. Saved with <see cref="SaveAsync"/>.</summary>
        internal sealed class Run
        {
            internal readonly Dictionary<string, StoredSource> Read = new Dictionary<string, StoredSource>(StringComparer.Ordinal);

            /// <summary>The entries a source's list gave (blacklisted ones left out). A source
            /// that gave nothing is not recorded and keeps the entries it had.</summary>
            internal void Record(TagConfig source, IEnumerable<ExternalItemDto> items)
            {
                var list = items.Where(i => i != null && !string.IsNullOrWhiteSpace(i.Name)).ToList();
                if (list.Count == 0) return;
                var key = SourceKey(source);
                if (Read.TryGetValue(key, out var had)) { had.Items.AddRange(list); return; }
                Read[key] = new StoredSource { Key = key, Name = SourceName(source), ReadUtc = DateTime.UtcNow, Items = list };
            }
        }

        /// <summary>
        /// Saves the run's lists over the stored ones (sources no longer in the settings, or turned
        /// off, are dropped) and brings the TMDb episodes of their shows up to date.
        /// </summary>
        internal static async Task SaveAsync(Run run, PluginConfiguration config, IHttpClient http, IJsonSerializer json,
            RunLog log, CancellationToken cancellationToken)
        {
            var file = Load<SourcesFile>(SourcesPath, json);
            var current = (config.Tags ?? new List<TagConfig>())
                .Where(t => t.Active && !string.IsNullOrWhiteSpace(t.Tag) && IsListSource(t))
                .ToList();
            var keys = new HashSet<string>(current.Select(SourceKey), StringComparer.Ordinal);
            var names = current.GroupBy(SourceKey).ToDictionary(g => g.Key, g => SourceName(g.First()), StringComparer.Ordinal);

            var byKey = file.Sources.Where(s => keys.Contains(s.Key)).ToDictionary(s => s.Key, StringComparer.Ordinal);
            foreach (var r in run.Read.Values) byKey[r.Key] = r;
            foreach (var s in byKey.Values) if (names.TryGetValue(s.Key, out var n)) s.Name = n;   // renamed since it was read
            file.Sources = byKey.Values.ToList();
            file.UpdatedUtc = DateTime.UtcNow;

            lock (Gate)
            {
                Directory.CreateDirectory(Folder);
                json.SerializeToFile(file, SourcesPath);
            }

            int entries = file.Sources.Sum(s => s.Items.Count);
            log.Debug($"  Missing titles: {run.Read.Count} list(s) read this run, {file.Sources.Count} saved, {entries} entries");

            if (string.IsNullOrWhiteSpace(config.TmdbApiKey))
            {
                log.Skip("Missing episodes: no TMDb key in Settings, so only the titles are listed");
                return;
            }
            try { await RefreshSeriesAsync(file, config.TmdbApiKey.Trim(), http, json, log, cancellationToken).ConfigureAwait(false); }
            catch (OperationCanceledException) { throw; }
            catch (Exception ex) { log.Warn($"Missing episodes could not be read from TMDb: {ex.Message}"); }
        }

        // ---- TMDb ----

        public sealed class TmdbFindResponse
        {
            public List<TmdbListItem> movie_results { get; set; } = new List<TmdbListItem>();
            public List<TmdbListItem> tv_results { get; set; } = new List<TmdbListItem>();
        }

        public sealed class TmdbTvDetails
        {
            public string? status { get; set; }
            public TmdbTvExternalIds? external_ids { get; set; }
            public List<TmdbTvSeason> seasons { get; set; } = new List<TmdbTvSeason>();
        }

        public sealed class TmdbTvExternalIds
        {
            public int? tvdb_id { get; set; }
        }

        public sealed class TmdbTvSeason
        {
            public int season_number { get; set; }
            public int episode_count { get; set; }
            public List<TmdbTvEpisode> episodes { get; set; } = new List<TmdbTvEpisode>();
        }

        public sealed class TmdbTvEpisode
        {
            public int episode_number { get; set; }
            public string? name { get; set; }
            public string? air_date { get; set; }
        }

        private static async Task<T?> TmdbGet<T>(string path, string key, IHttpClient http, IJsonSerializer json, CancellationToken cancellationToken) where T : class
        {
            // Same key handling as ListFetcher: a v4 read access token is sent as a bearer token.
            bool isToken = key.Length > 100 || key.Contains(".");
            var url = "https://api.themoviedb.org/3/" + path;
            if (!isToken) url += (url.Contains("?") ? "&" : "?") + "api_key=" + Uri.EscapeDataString(key);
            var options = new HttpRequestOptions { Url = url, CancellationToken = cancellationToken };
            if (isToken) options.RequestHeaders.Add("Authorization", "Bearer " + key);
            using (var stream = await http.Get(options).ConfigureAwait(false))
                return json.DeserializeFromStream<T>(stream);
        }

        // A show that has ended changes seldom; a running one gets new episodes every week.
        private static bool IsStale(CachedSeries s) => !s.IdsRead ||
            DateTime.UtcNow - s.FetchedUtc > (s.Status == "Ended" || s.Status == "Canceled" ? TimeSpan.FromDays(30) : TimeSpan.FromHours(20));

        private static bool Aired(string? airDate, DateTime today) =>
            DateTime.TryParseExact(airDate, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var d) && d <= today;

        private static async Task RefreshSeriesAsync(SourcesFile file, string key, IHttpClient http, IJsonSerializer json, RunLog log, CancellationToken cancellationToken)
        {
            var cache = Load<SeriesCacheFile>(SeriesPath, json);
            var lookups = cache.Lookups.Where(l => !string.IsNullOrEmpty(l.Imdb))
                .GroupBy(l => l.Imdb, StringComparer.OrdinalIgnoreCase).ToDictionary(g => g.Key, g => g.First(), StringComparer.OrdinalIgnoreCase);
            var entries = file.Sources.SelectMany(s => s.Items).ToList();
            var throttle = new SemaphoreSlim(4);
            int failed = 0;

            // 1. The TMDb id (and type) of titles the list gave by IMDb id only (MDBList): the shows need it for
            // their episodes, and the export carries it for movies too. A found id is kept for good.
            var toFind = entries.Where(i => string.IsNullOrEmpty(i.Tmdb) && !string.IsNullOrEmpty(i.Imdb))
                .Select(i => i.Imdb.Trim()).Distinct(StringComparer.OrdinalIgnoreCase)
                .Where(imdb => !lookups.TryGetValue(imdb, out var l) || (l.Tmdb == null && DateTime.UtcNow - l.CheckedUtc > TimeSpan.FromDays(30)))
                .ToList();
            var found = await Task.WhenAll(toFind.Select(async imdb =>
            {
                await throttle.WaitAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    var r = await TmdbGet<TmdbFindResponse>($"find/{Uri.EscapeDataString(imdb)}?external_source=imdb_id", key, http, json, cancellationToken).ConfigureAwait(false);
                    var tv = r?.tv_results?.FirstOrDefault();
                    var movie = r?.movie_results?.FirstOrDefault();
                    return new IdLookup
                    {
                        Imdb = imdb, CheckedUtc = DateTime.UtcNow,
                        Tmdb = tv != null ? tv.id.ToString() : movie?.id.ToString(),
                        Type = tv != null ? "show" : movie != null ? "movie" : null
                    };
                }
                catch (OperationCanceledException) { throw; }
                catch { Interlocked.Increment(ref failed); return null; }
                finally { throttle.Release(); }
            })).ConfigureAwait(false);
            foreach (var l in found) if (l != null) lookups[l.Imdb] = l;

            // 2. The shows, by TMDb id.
            var showIds = new HashSet<string>(StringComparer.Ordinal);
            foreach (var i in entries)
            {
                var type = i.Type;
                var tmdb = i.Tmdb;
                if (!string.IsNullOrEmpty(i.Imdb) && lookups.TryGetValue(i.Imdb.Trim(), out var l))
                {
                    type ??= l.Type;
                    tmdb ??= l.Tmdb;
                }
                if (type == "show" && !string.IsNullOrEmpty(tmdb)) showIds.Add(tmdb!);
            }

            var cached = cache.Series.Where(s => !string.IsNullOrEmpty(s.TmdbId))
                .GroupBy(s => s.TmdbId).ToDictionary(g => g.Key, g => g.First(), StringComparer.Ordinal);
            var stale = showIds.Where(id => !cached.TryGetValue(id, out var s) || IsStale(s)).ToList();
            int done = 0;
            var today = DateTime.UtcNow.Date;
            var fresh = await Task.WhenAll(stale.Select(async id =>
            {
                await throttle.WaitAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    log.Count("Missing episodes", Interlocked.Increment(ref done), stale.Count, "shows");
                    cached.TryGetValue(id, out var old);
                    var details = await TmdbGet<TmdbTvDetails>($"tv/{id}?append_to_response=external_ids", key, http, json, cancellationToken).ConfigureAwait(false);
                    if (details == null) return null;
                    var series = new CachedSeries
                    {
                        TmdbId = id, Status = details.status ?? "", FetchedUtc = DateTime.UtcNow, IdsRead = true,
                        TvdbId = details.external_ids?.tvdb_id > 0 ? details.external_ids.tvdb_id.ToString() : null
                    };
                    foreach (var season in (details.seasons ?? new List<TmdbTvSeason>()).Where(s => s.season_number > 0).OrderBy(s => s.season_number))
                    {
                        // A season read before, complete and fully aired, has nothing new.
                        var had = old?.Seasons.FirstOrDefault(s => s.Number == season.season_number);
                        if (had != null && had.EpisodeCount == season.episode_count && had.Episodes.Count == season.episode_count
                            && had.Episodes.All(e => Aired(e.AirDate, today)))
                        {
                            series.Seasons.Add(had);
                            continue;
                        }
                        var full = await TmdbGet<TmdbTvSeason>($"tv/{id}/season/{season.season_number}", key, http, json, cancellationToken).ConfigureAwait(false);
                        series.Seasons.Add(new CachedSeason
                        {
                            Number = season.season_number,
                            EpisodeCount = season.episode_count,
                            Episodes = (full?.episodes ?? new List<TmdbTvEpisode>())
                                .Select(e => new CachedEpisode { Number = e.episode_number, Name = e.name, AirDate = string.IsNullOrEmpty(e.air_date) ? null : e.air_date })
                                .ToList()
                        });
                    }
                    return series;
                }
                catch (OperationCanceledException) { throw; }
                catch { Interlocked.Increment(ref failed); return null; }
                finally { throttle.Release(); }
            })).ConfigureAwait(false);
            foreach (var s in fresh) if (s != null) cached[s.TmdbId] = s;

            // Only what the saved lists still hold is kept.
            var imdbs = new HashSet<string>(file.Sources.SelectMany(s => s.Items).Select(i => (i.Imdb ?? "").Trim()), StringComparer.OrdinalIgnoreCase);
            cache.Series = cached.Values.Where(s => showIds.Contains(s.TmdbId)).ToList();
            cache.Lookups = lookups.Values.Where(l => imdbs.Contains(l.Imdb)).ToList();
            lock (Gate)
            {
                Directory.CreateDirectory(Folder);
                json.SerializeToFile(cache, SeriesPath);
            }

            int refreshed = fresh.Count(s => s != null);
            log.Debug($"  Missing episodes: {showIds.Count} shows on the lists, {refreshed} read from TMDb, {showIds.Count - stale.Count} from the cache");
            // A few ids TMDb no longer knows fail on every run: only a real outage is worth a warning.
            int asked = toFind.Count + stale.Count;
            if (failed > 0) log.Debug($"  Missing episodes: {failed} of {asked} TMDb requests failed, retried on the next sync");
            if (failed > 5 && failed * 10 > showIds.Count + toFind.Count)
                log.Warn($"Missing episodes: {RunLog.Plural(failed, "TMDb request")} failed — those shows are retried on the next sync");
        }

        // ---- The tab ----

        public sealed class MissingTitlesResult
        {
            public bool HasData { get; set; }
            public string? UpdatedUtc { get; set; }
            public bool HasTmdbKey { get; set; }
            public int ListCount { get; set; }
            // "Keep the list in a file" (MissingTitlesExport): the folder used when none is set,
            // when the file was last written, and why the last write failed.
            public string ExportDefaultFolder { get; set; } = "";
            public string? ExportWrittenUtc { get; set; }
            public string? ExportError { get; set; }
            public List<MissingMovie> Movies { get; set; } = new List<MissingMovie>();
            public List<MissingShow> Shows { get; set; } = new List<MissingShow>();
        }

        public sealed class MissingMovie
        {
            public string Name { get; set; } = "";
            public int? Year { get; set; }
            public string Imdb { get; set; } = "";
            public string Tmdb { get; set; } = "";
            public List<string> Lists { get; set; } = new List<string>();
        }

        public sealed class MissingShow
        {
            public string Name { get; set; } = "";
            public int? Year { get; set; }
            public string Imdb { get; set; } = "";
            public string Tmdb { get; set; } = "";
            public string Tvdb { get; set; } = "";
            public List<string> Lists { get; set; } = new List<string>();
            public string LibraryId { get; set; } = "";   // the show in the library; "" = not in it
            public bool EpisodesKnown { get; set; }        // TMDb's episodes are cached for it
            public int AiredEpisodes { get; set; }
            public int MissingEpisodes { get; set; }
            public List<MissingSeason> Seasons { get; set; } = new List<MissingSeason>();
        }

        public sealed class MissingSeason
        {
            public int Number { get; set; }
            public int AiredEpisodes { get; set; }
            public List<MissingEpisode> Episodes { get; set; } = new List<MissingEpisode>();
        }

        public sealed class MissingEpisode
        {
            public int Number { get; set; }
            public string Name { get; set; } = "";
            public string AirDate { get; set; } = "";
        }

        // One title, merged over every list that has it.
        private sealed class Title
        {
            public string Name = "";
            public int? Year;
            public string Imdb = "";
            public string Tmdb = "";
            public string? Type;
            public readonly List<string> Lists = new List<string>();
        }

        private static string NormTitle(string s) =>
            new string((s ?? "").ToLowerInvariant().Where(char.IsLetterOrDigit).ToArray());

        private static List<MissingEpisode> ToEpisodes(IEnumerable<CachedEpisode> episodes) =>
            episodes.Select(e => new MissingEpisode { Number = e.Number, Name = e.Name ?? "", AirDate = e.AirDate ?? "" }).ToList();

        /// <summary>Every aired episode of a show, by season (the Missing tab, for a show that is not in the library).</summary>
        internal static List<MissingSeason> Episodes(string tmdbId, IJsonSerializer json)
        {
            var today = DateTime.UtcNow.Date;
            var series = Load<SeriesCacheFile>(SeriesPath, json).Series.FirstOrDefault(s => s.TmdbId == tmdbId);
            if (series == null) return new List<MissingSeason>();
            return series.Seasons.OrderBy(s => s.Number)
                .Select(s => new { s.Number, Aired = s.Episodes.Where(e => Aired(e.AirDate, today)).OrderBy(e => e.Number).ToList() })
                .Where(s => s.Aired.Count > 0)
                .Select(s => new MissingSeason { Number = s.Number, AiredEpisodes = s.Aired.Count, Episodes = ToEpisodes(s.Aired) })
                .ToList();
        }

        internal static MissingTitlesResult Build(ILibraryManager libraryManager, PluginConfiguration config, IJsonSerializer json)
        {
            var result = new MissingTitlesResult { HasTmdbKey = !string.IsNullOrWhiteSpace(config.TmdbApiKey) };
            if (!File.Exists(SourcesPath)) return result;
            var file = Load<SourcesFile>(SourcesPath, json);
            var cache = Load<SeriesCacheFile>(SeriesPath, json);
            result.HasData = true;
            result.UpdatedUtc = file.UpdatedUtc.ToString("o");

            // Only the sources that are still in the settings and on.
            var keys = new HashSet<string>((config.Tags ?? new List<TagConfig>())
                .Where(t => t.Active && !string.IsNullOrWhiteSpace(t.Tag) && IsListSource(t)).Select(SourceKey), StringComparer.Ordinal);
            var sources = file.Sources.Where(s => keys.Contains(s.Key)).ToList();
            result.ListCount = sources.Count;

            var lookups = cache.Lookups.GroupBy(l => l.Imdb, StringComparer.OrdinalIgnoreCase).ToDictionary(g => g.Key, g => g.First(), StringComparer.OrdinalIgnoreCase);
            var seriesById = cache.Series.GroupBy(s => s.TmdbId).ToDictionary(g => g.Key, g => g.First(), StringComparer.Ordinal);

            // Merge the lists: one title per IMDb id (else TMDb id, else name + year).
            var titles = new Dictionary<string, Title>(StringComparer.OrdinalIgnoreCase);
            foreach (var src in sources)
                foreach (var i in src.Items)
                {
                    var imdb = (i.Imdb ?? "").Trim();
                    var k = imdb.Length > 0 ? "imdb:" + imdb : !string.IsNullOrEmpty(i.Tmdb) ? "tmdb:" + (i.Type ?? "") + i.Tmdb : "name:" + NormTitle(i.Name) + "|" + i.Year;
                    if (!titles.TryGetValue(k, out var t))
                        titles[k] = t = new Title { Name = i.Name.Trim(), Year = i.Year, Imdb = imdb };
                    t.Year ??= i.Year;
                    if (string.IsNullOrEmpty(t.Tmdb) && !string.IsNullOrEmpty(i.Tmdb)) t.Tmdb = i.Tmdb!;
                    t.Type ??= i.Type;
                    if (!t.Lists.Contains(src.Name)) t.Lists.Add(src.Name);
                }
            foreach (var t in titles.Values)
                if (t.Imdb.Length > 0 && lookups.TryGetValue(t.Imdb, out var l))
                {
                    t.Type ??= l.Type;
                    if (t.Tmdb.Length == 0 && l.Tmdb != null && (l.Type == t.Type || t.Type == null)) t.Tmdb = l.Tmdb;
                }

            // The library's movies and shows (not the plugin's own top-list / ranked copies).
            var topListsFolder = Path.Combine(Plugin.Instance!.DataFolderPath, "toplists") + Path.DirectorySeparatorChar;
            var library = libraryManager.GetItemList(new InternalItemsQuery
            {
                IncludeItemTypes = new[] { "Movie", "Series" },
                Recursive = true,
                IsVirtualItem = false
            }).Where(i => string.IsNullOrEmpty(i.Path)
                || !(i.Path.StartsWith(topListsFolder, StringComparison.OrdinalIgnoreCase) || RankedCollections.IsRankedPath(i.Path))).ToList();
            var byImdb = new Dictionary<string, List<BaseItem>>(StringComparer.OrdinalIgnoreCase);
            var byTmdb = new Dictionary<string, List<BaseItem>>(StringComparer.OrdinalIgnoreCase);
            var byName = new Dictionary<string, List<BaseItem>>(StringComparer.Ordinal);
            void Add(Dictionary<string, List<BaseItem>> d, string? k, BaseItem item)
            {
                if (string.IsNullOrEmpty(k)) return;
                if (!d.TryGetValue(k!, out var l)) d[k!] = l = new List<BaseItem>();
                l.Add(item);
            }
            foreach (var item in library)
            {
                Add(byImdb, item.GetProviderId("Imdb"), item);
                Add(byTmdb, (item is Series ? "show:" : "movie:") + item.GetProviderId("Tmdb"), item);
                Add(byName, NormTitle(item.Name), item);
            }

            List<BaseItem> InLibrary(Title t)
            {
                if (t.Imdb.Length > 0 && byImdb.TryGetValue(t.Imdb, out var a)) return a;
                if (t.Tmdb.Length > 0 && t.Type != null && byTmdb.TryGetValue((t.Type == "show" ? "show:" : "movie:") + t.Tmdb, out var b)) return b;
                // Not under its ids (an AI's wrong IMDb id, an item without ids): by name and year, as
                // the sync matches AI titles. A title with ids needs both years to agree.
                if (!byName.TryGetValue(NormTitle(t.Name), out var c)) return new List<BaseItem>();
                bool noIds = t.Imdb.Length == 0 && t.Tmdb.Length == 0;
                return c.Where(x => (t.Type == null || (t.Type == "show") == (x is Series))
                    && (t.Year == null || x.ProductionYear == null ? noIds : Math.Abs(x.ProductionYear.Value - t.Year.Value) <= 1)).ToList();
            }

            var shows = new List<(Title Title, List<Series> Local)>();
            foreach (var t in titles.Values)
            {
                var local = InLibrary(t);
                if (local.Count == 0)
                {
                    if (t.Type == "show") shows.Add((t, new List<Series>()));
                    else result.Movies.Add(new MissingMovie { Name = t.Name, Year = t.Year, Imdb = t.Imdb, Tmdb = t.Tmdb, Lists = t.Lists });
                    continue;
                }
                var localShows = local.OfType<Series>().ToList();
                if (localShows.Count == 0) continue;   // the film is there
                if (t.Tmdb.Length == 0) t.Tmdb = localShows.Select(s => s.GetProviderId("Tmdb")).FirstOrDefault(id => !string.IsNullOrEmpty(id)) ?? "";
                shows.Add((t, localShows));
            }

            // The episodes the library has, per show (a show in two libraries counts both).
            var have = new Dictionary<long, HashSet<long>>();
            if (shows.Any(s => s.Local.Count > 0))
            {
                var showIds = new HashSet<long>(shows.SelectMany(s => s.Local).Select(s => s.InternalId));
                foreach (var ep in libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "Episode" }, Recursive = true, IsVirtualItem = false }).OfType<Episode>())
                {
                    if (!showIds.Contains(ep.SeriesId) || ep.ParentIndexNumber == null || ep.IndexNumber == null) continue;
                    if (!have.TryGetValue(ep.SeriesId, out var set)) have[ep.SeriesId] = set = new HashSet<long>();
                    int last = ep.IndexNumberEnd != null && ep.IndexNumberEnd > ep.IndexNumber ? ep.IndexNumberEnd.Value : ep.IndexNumber.Value;
                    for (int n = ep.IndexNumber.Value; n <= last; n++) set.Add(((long)ep.ParentIndexNumber.Value << 20) | (uint)n);
                }
            }

            var today = DateTime.UtcNow.Date;
            foreach (var (t, local) in shows)
            {
                var show = new MissingShow
                {
                    Name = t.Name, Year = t.Year, Imdb = t.Imdb, Tmdb = t.Tmdb, Lists = t.Lists,
                    LibraryId = local.Count > 0 ? local[0].Id.ToString("N") : ""
                };
                if (local.Count > 0 && t.Year == null) show.Year = local[0].ProductionYear;
                show.Tvdb = local.Select(s => s.GetProviderId("Tvdb")).FirstOrDefault(id => !string.IsNullOrEmpty(id)) ?? "";
                var owned = new HashSet<long>(local.SelectMany(s => have.TryGetValue(s.InternalId, out var set) ? set : Enumerable.Empty<long>()));
                if (t.Tmdb.Length > 0 && seriesById.TryGetValue(t.Tmdb, out var cached))
                {
                    show.EpisodesKnown = true;
                    if (show.Tvdb.Length == 0) show.Tvdb = cached.TvdbId ?? "";
                    foreach (var season in cached.Seasons.OrderBy(s => s.Number))
                    {
                        var aired = season.Episodes.Where(e => Aired(e.AirDate, today)).OrderBy(e => e.Number).ToList();
                        var gone = aired.Where(e => !owned.Contains(((long)season.Number << 20) | (uint)e.Number)).ToList();
                        show.AiredEpisodes += aired.Count;
                        if (gone.Count == 0) continue;
                        show.MissingEpisodes += gone.Count;
                        show.Seasons.Add(new MissingSeason
                        {
                            Number = season.Number,
                            AiredEpisodes = aired.Count,
                            // A show that is not in the library lacks every episode: they are read when
                            // its season is opened (Episodes), which keeps the tab small.
                            Episodes = local.Count == 0 ? new List<MissingEpisode>() : ToEpisodes(gone)
                        });
                    }
                }
                // A show in the library is listed only for the episodes it lacks.
                if (local.Count > 0 && show.MissingEpisodes == 0) continue;
                result.Shows.Add(show);
            }

            result.Movies = result.Movies.OrderBy(m => m.Name, StringComparer.CurrentCultureIgnoreCase).ThenBy(m => m.Year).ToList();
            result.Shows = result.Shows.OrderBy(s => s.Name, StringComparer.CurrentCultureIgnoreCase).ThenBy(s => s.Year).ToList();
            return result;
        }
    }
}
