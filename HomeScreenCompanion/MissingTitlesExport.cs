using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Entities.Movies;
using MediaBrowser.Controller.Entities.TV;
using MediaBrowser.Controller.Library;
using MediaBrowser.Model.Serialization;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;

namespace HomeScreenCompanion
{
    /// <summary>
    /// The Missing tab's list as files: JSON (every id, for other programs) and CSV (one file for
    /// movies, one for shows). Downloaded from the tab, and with "Keep the list in a file" written
    /// to a folder after every sync and a few minutes after the library changes.
    /// </summary>
    internal static class MissingTitlesExport
    {
        internal const string JsonName = "missing_titles.json";
        internal const string MoviesCsvName = "missing_movies.csv";
        internal const string ShowsCsvName = "missing_shows.csv";

        // ---- The JSON file ----

        public sealed class ExportFile
        {
            public string generated { get; set; } = "";
            public List<ExportMovie> movies { get; set; } = new List<ExportMovie>();
            public List<ExportShow> shows { get; set; } = new List<ExportShow>();
        }

        public sealed class ExportMovie
        {
            public string title { get; set; } = "";
            public int? year { get; set; }
            public string? imdb_id { get; set; }
            public int? tmdb_id { get; set; }
            public List<string> lists { get; set; } = new List<string>();
        }

        public sealed class ExportShow
        {
            public string title { get; set; } = "";
            public int? year { get; set; }
            public string? imdb_id { get; set; }
            public int? tmdb_id { get; set; }
            public int? tvdb_id { get; set; }
            public bool in_library { get; set; }
            public int? aired_episodes { get; set; }
            public int? missing_episodes { get; set; }
            public List<string> lists { get; set; } = new List<string>();
            public List<ExportSeason>? seasons { get; set; }   // a show in the library: the episodes it lacks
        }

        public sealed class ExportSeason
        {
            public int season { get; set; }
            public List<ExportEpisode> episodes { get; set; } = new List<ExportEpisode>();
        }

        public sealed class ExportEpisode
        {
            public int episode { get; set; }
            public string title { get; set; } = "";
            public string air_date { get; set; } = "";
        }

        private static int? Id(string? s) => int.TryParse(s, NumberStyles.None, CultureInfo.InvariantCulture, out var n) && n > 0 ? n : (int?)null;
        private static string? Text(string? s) => string.IsNullOrEmpty(s) ? null : s;

        // The shows that go out: those not in the library, and with includeGaps the ones that are.
        private static IEnumerable<MissingTitles.MissingShow> Shows(MissingTitles.MissingTitlesResult r, bool includeGaps) =>
            r.Shows.Where(s => includeGaps || s.LibraryId.Length == 0);

        internal static string Json(MissingTitles.MissingTitlesResult r, bool includeGaps, IJsonSerializer json)
        {
            var file = new ExportFile
            {
                generated = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture),
                movies = r.Movies.Select(m => new ExportMovie { title = m.Name, year = m.Year, imdb_id = Text(m.Imdb), tmdb_id = Id(m.Tmdb), lists = m.Lists }).ToList(),
                shows = Shows(r, includeGaps).Select(s => new ExportShow
                {
                    title = s.Name, year = s.Year, imdb_id = Text(s.Imdb), tmdb_id = Id(s.Tmdb), tvdb_id = Id(s.Tvdb),
                    in_library = s.LibraryId.Length > 0,
                    aired_episodes = s.EpisodesKnown ? s.AiredEpisodes : (int?)null,
                    missing_episodes = s.EpisodesKnown ? s.MissingEpisodes : (int?)null,
                    lists = s.Lists,
                    seasons = s.LibraryId.Length == 0 ? null : s.Seasons.Select(se => new ExportSeason
                    {
                        season = se.Number,
                        episodes = se.Episodes.Select(e => new ExportEpisode { episode = e.Number, title = e.Name, air_date = e.AirDate }).ToList()
                    }).ToList()
                }).ToList()
            };
            return json.SerializeToString(file);
        }

        // ---- CSV ----

        private static string Csv(object? value)
        {
            var s = Convert.ToString(value, CultureInfo.InvariantCulture) ?? "";
            return s.IndexOfAny(new[] { ',', '"', '\n', '\r' }) >= 0 ? "\"" + s.Replace("\"", "\"\"") + "\"" : s;
        }

        private static string Line(params object?[] values) => string.Join(",", values.Select(Csv)) + "\r\n";

        internal static string MoviesCsv(MissingTitles.MissingTitlesResult r)
        {
            var sb = new StringBuilder(Line("title", "year", "imdb_id", "tmdb_id", "lists"));
            foreach (var m in r.Movies) sb.Append(Line(m.Name, m.Year, m.Imdb, m.Tmdb, string.Join("; ", m.Lists)));
            return sb.ToString();
        }

        internal static string ShowsCsv(MissingTitles.MissingTitlesResult r, bool includeGaps)
        {
            var sb = new StringBuilder(Line("title", "year", "imdb_id", "tmdb_id", "tvdb_id", "in_library", "aired_episodes", "missing_episodes", "missing", "lists"));
            foreach (var s in Shows(r, includeGaps))
            {
                // A show in the library lists the episodes it lacks (S01E03 S01E04 …); one that is not lacks them all.
                var missing = s.LibraryId.Length == 0 ? "" : string.Join(" ", s.Seasons.SelectMany(se => se.Episodes.Select(e => $"S{se.Number:00}E{e.Number:00}")));
                sb.Append(Line(s.Name, s.Year, s.Imdb, s.Tmdb, s.Tvdb, s.LibraryId.Length > 0 ? "yes" : "no",
                    s.EpisodesKnown ? s.AiredEpisodes : (int?)null, s.EpisodesKnown ? s.MissingEpisodes : (int?)null, missing, string.Join("; ", s.Lists)));
            }
            return sb.ToString();
        }

        // ---- The kept file ----

        private static readonly object Gate = new object();
        private static ILibraryManager? _libraryManager;
        private static IJsonSerializer? _json;
        private static Timer? _timer;

        /// <summary>Why the last write failed; null when it worked (or none was made since startup).</summary>
        internal static string? LastError { get; private set; }

        internal static string DefaultFolder => Path.Combine(Plugin.Instance!.DataFolderPath, "missing_titles", "export");

        internal static string Folder(PluginConfiguration config) =>
            string.IsNullOrWhiteSpace(config.MissingExportFolder) ? DefaultFolder : config.MissingExportFolder.Trim();

        private static bool IsOn(PluginConfiguration? config) =>
            config != null && config.ShowMissingTitles && config.MissingExportToFile && (config.MissingExportJson || config.MissingExportCsv);

        /// <summary>When the kept file was last written (the newest of its files), or null.</summary>
        internal static DateTime? WrittenUtc(PluginConfiguration config)
        {
            try
            {
                var folder = Folder(config);
                var times = new[] { JsonName, MoviesCsvName, ShowsCsvName }.Select(n => Path.Combine(folder, n))
                    .Where(File.Exists).Select(File.GetLastWriteTimeUtc).ToList();
                return times.Count > 0 ? times.Max() : (DateTime?)null;
            }
            catch { return null; }
        }

        internal static void Initialize(ILibraryManager libraryManager, IJsonSerializer json)
        {
            _libraryManager = libraryManager;
            _json = json;
            libraryManager.ItemAdded += OnLibraryChanged;
            libraryManager.ItemRemoved += OnLibraryChanged;
        }

        internal static void Dispose()
        {
            if (_libraryManager != null)
            {
                _libraryManager.ItemAdded -= OnLibraryChanged;
                _libraryManager.ItemRemoved -= OnLibraryChanged;
            }
            lock (Gate) { _timer?.Dispose(); _timer = null; }
        }

        private static void OnLibraryChanged(object? sender, ItemChangeEventArgs e)
        {
            if (!(e.Item is Movie || e.Item is Series || e.Item is Episode) || e.Item.IsVirtualItem) return;
            if (IsOn(Plugin.Instance?.Configuration)) Queue(TimeSpan.FromMinutes(3));
        }

        /// <summary>Writes the file after a pause; another call within it moves the write on
        /// (a library scan adds many titles, the file is written once after it).</summary>
        internal static void Queue(TimeSpan delay)
        {
            lock (Gate)
            {
                if (_timer == null) _timer = new Timer(_ => Write(), null, delay, Timeout.InfiniteTimeSpan);
                else _timer.Change(delay, Timeout.InfiniteTimeSpan);
            }
        }

        /// <summary>Writes the kept file now (when it is on). Returns the folder, or null.</summary>
        internal static string? Write()
        {
            var config = Plugin.Instance?.Configuration;
            if (!IsOn(config) || _libraryManager == null || _json == null) return null;
            var folder = Folder(config!);
            try
            {
                var result = MissingTitles.Build(_libraryManager, config!, _json);
                if (!result.HasData) return null;
                lock (Gate)
                {
                    Directory.CreateDirectory(folder);
                    if (config!.MissingExportJson) WriteFile(folder, JsonName, Json(result, config.MissingIncludeGaps, _json));
                    if (config.MissingExportCsv)
                    {
                        WriteFile(folder, MoviesCsvName, MoviesCsv(result));
                        WriteFile(folder, ShowsCsvName, ShowsCsv(result, config.MissingIncludeGaps));
                    }
                }
                if (LastError != null) PluginLog.Write("INF", "Missing", $"The missing titles are written to {folder} again");
                LastError = null;
                PluginLog.Write("DBG", "Missing", $"Missing titles written to {folder}: {result.Movies.Count} movies, {result.Shows.Count} shows");
                return folder;
            }
            catch (Exception ex)
            {
                LastError = ex.Message;
                PluginLog.Write("WRN", "Missing", $"The missing titles could not be written to {folder}: {ex.Message}");
                return null;
            }
        }

        // Written beside the file and then moved over it, so a program reading it never gets half a file.
        private static void WriteFile(string folder, string name, string content)
        {
            var path = Path.Combine(folder, name);
            var temp = path + ".tmp";
            File.WriteAllText(temp, content, new UTF8Encoding(name.EndsWith(".csv", StringComparison.Ordinal)));   // a BOM lets Excel read CSV as UTF-8
            if (File.Exists(path)) File.Delete(path);
            File.Move(temp, path);
        }
    }
}
