using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Library;
using MediaBrowser.Controller.Providers;
using MediaBrowser.Model.Drawing;
using MediaBrowser.Model.Entities;
using MediaBrowser.Model.Logging;
using MediaBrowser.Model.Serialization;
using SkiaSharp;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace HomeScreenCompanion
{
    /// <summary>
    /// "Badge the real posters too" (per top list, HomeSectionSettings.RealPosterBadge = "label|size"
    /// or "label|size|{"pos":"tl"}", "" = off; pos tl | tr | bl | bc | br, none = top right; values
    /// saved by 4.2.0.35/.36 with a whole Customise look
    /// there still load: only their "pos" counts): which real library items are in a top list with the option on, and at what rank.
    /// Supports() of the image enhancer runs for every image Emby lists, so it only looks in this
    /// in-memory map; the map is rebuilt (debounced, on a background thread) after a config save,
    /// a sync / Run Group and at server start.
    /// </summary>
    internal static class RealPosterBadges
    {
        internal sealed class Entry
        {
            public int Rank;
            public string Label = "top10";   // "top10" | "top10rank"
            public string Size = "m";        // "s" | "m" | "l"
            public string Corner = "tr";     // "tl" | "tr" | "bl" | "bc" | "br"
            public string CacheKey => "hsc-rpb2|" + Rank + "|" + Label + "|" + Size + "|" + Corner;
        }

        private static volatile Dictionary<long, Entry> _map = new Dictionary<long, Entry>();
        private static ILibraryManager? _libraryManager;
        private static IJsonSerializer? _json;
        private static PluginLogger? _logger;
        private static Timer? _timer;
        private static readonly object Gate = new object();

        internal static void Initialize(ILibraryManager libraryManager, IJsonSerializer json, PluginLogger logger)
        {
            _libraryManager = libraryManager;
            _json = json;
            _logger = logger;
            QueueRebuild(15000);   // server start: give the library a moment
        }

        internal static bool TryGet(BaseItem item, out Entry entry)
        {
            entry = null!;
            return item != null && _map.TryGetValue(item.InternalId, out entry!);
        }

        /// <summary>Rebuilds the map shortly (several calls in a row cause one rebuild).</summary>
        internal static void QueueRebuild(int delayMs = 1500)
        {
            if (_libraryManager == null) return;
            lock (Gate)
            {
                if (_timer == null) _timer = new Timer(_ => { try { Rebuild(); } catch (Exception ex) { _logger?.ErrorException("Real poster badges: rebuild failed", ex); } }, null, delayMs, Timeout.Infinite);
                else _timer.Change(delayMs, Timeout.Infinite);
            }
        }

        private static int _pendingAfterRun;

        /// <summary>A config save: rebuilds shortly, or once the running sync has finished (a sync
        /// saves the config many times, and each rebuild loads every movie).</summary>
        internal static void ConfigSaved()
        {
            Interlocked.Exchange(ref _pendingAfterRun, 1);
            if (!RunGate.Busy) RunFinished();
        }

        /// <summary>Called when RunGate is released: the rebuild config saves during the run asked for.</summary>
        internal static void RunFinished()
        {
            if (Interlocked.Exchange(ref _pendingAfterRun, 0) == 1) QueueRebuild();
        }

        /// <summary>"label|size[|{"pos":..}]" from a list's settings, or null when the option is off.</summary>
        internal static (string Label, string Size, string Corner)? OptionOf(string? value)
        {
            if (string.IsNullOrWhiteSpace(value)) return null;
            var parts = value!.Split(new[] { '|' }, 3);
            var label = parts[0] == "top10rank" ? "top10rank" : "top10";
            var size = parts.Length > 1 && (parts[1] == "s" || parts[1] == "l") ? parts[1] : "m";
            var options = parts.Length > 2 ? parts[2] : "";
            // No position = top right (the square's place since the start).
            var m = Regex.Match(options, "\"pos\"\\s*:\\s*\"(tl|tr|bl|br|bc)\"", RegexOptions.IgnoreCase);
            var corner = m.Success ? m.Groups[1].Value.ToLowerInvariant() : "tr";
            return (label, size, corner);
        }

        private static void Rebuild()
        {
            var lm = _libraryManager; var json = _json; var config = Plugin.Instance?.Configuration;
            if (lm == null || json == null || config == null) return;

            var wanted = new List<(TopListHomeSection Tl, string Label, string Size, string Corner)>();
            foreach (var tl in config.TopLists ?? new List<TopListHomeSection>())
            {
                Dictionary<string, string>? settings = null;
                try { settings = json.DeserializeFromString<Dictionary<string, string>>(string.IsNullOrEmpty(tl.HomeSectionSettings) ? "{}" : tl.HomeSectionSettings); } catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
                if (settings == null || !settings.TryGetValue("RealPosterBadge", out var v)) continue;
                var opt = OptionOf(v);
                if (opt != null) wanted.Add((tl, opt.Value.Label, opt.Value.Size, opt.Value.Corner));
            }

            var map = new Dictionary<long, Entry>();
            if (wanted.Count > 0)
            {
                var dataPath = Plugin.Instance!.DataFolderPath;
                var topListsFolder = Path.Combine(dataPath, "toplists") + Path.DirectorySeparatorChar;

                // One pass over the real movies: by path (the .strm targets) and by provider id (all versions).
                List<BaseItem>? movies = null;
                Dictionary<string, BaseItem>? byPath = null;
                Dictionary<string, List<BaseItem>>? byKey = null;

                void Add(BaseItem item, int rank, string label, string size, string corner)
                {
                    if (map.TryGetValue(item.InternalId, out var e) && e.Rank <= rank) return;
                    map[item.InternalId] = new Entry { Rank = rank, Label = label, Size = size, Corner = corner };
                }

                foreach (var (tl, label, size, corner) in wanted)
                {
                    var folder = Path.Combine(dataPath, "toplists", SanitizeFolderName(tl.TagName));
                    // A show list's folder lists its ranked series (the real ones) in its manifest.
                    if (ShowTopList.IsShows(tl))
                    {
                        foreach (var e in ShowTopList.ReadManifest(folder, json))
                        {
                            if (!Guid.TryParse(e.SeriesId, out var g)) continue;
                            var series = lm.GetItemById(g);
                            if (series != null) Add(series, e.Rank, label, size, corner);
                        }
                        continue;
                    }

                    if (!Directory.Exists(folder)) continue;
                    if (movies == null)
                    {
                        movies = lm.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "Movie" }, Recursive = true, IsVirtualItem = false })
                            .Where(i => !string.IsNullOrEmpty(i.Path) && !i.Path.StartsWith(topListsFolder, StringComparison.OrdinalIgnoreCase) && !RankedCollections.IsRankedPath(i.Path)).ToList();
                        byPath = new Dictionary<string, BaseItem>(StringComparer.OrdinalIgnoreCase);
                        byKey = new Dictionary<string, List<BaseItem>>(StringComparer.OrdinalIgnoreCase);
                        foreach (var m in movies)
                        {
                            if (!byPath.ContainsKey(m.Path)) byPath[m.Path] = m;
                            var key = KeyOf(m);
                            if (key == null) continue;
                            if (!byKey.TryGetValue(key, out var l)) byKey[key] = l = new List<BaseItem>();
                            l.Add(m);
                        }
                    }

                    foreach (var strm in Directory.GetFiles(folder, "*.strm"))
                    {
                        int rank = int.MaxValue;
                        try
                        {
                            var nfo = Path.Combine(folder, Path.GetFileNameWithoutExtension(strm) + ".nfo");
                            if (File.Exists(nfo))
                            {
                                var match = Regex.Match(File.ReadAllText(nfo), @"<sorttitle>(\d+)</sorttitle>");
                                if (match.Success) rank = int.Parse(match.Groups[1].Value);
                            }
                            if (rank == int.MaxValue) continue;
                            if (!byPath!.TryGetValue(File.ReadAllText(strm).Trim(), out var movie)) continue;
                            var key = KeyOf(movie);
                            if (key != null && byKey!.TryGetValue(key, out var versions))
                                foreach (var v in versions) Add(v, rank, label, size, corner);
                            else Add(movie, rank, label, size, corner);
                        }
                        catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
                    }
                }
            }

            _map = map;
            _logger?.Info($"Real poster badges: {map.Count} item(s) badged");
        }

        private static string? KeyOf(BaseItem m)
        {
            var imdb = m.GetProviderId("Imdb");
            if (!string.IsNullOrWhiteSpace(imdb)) return "imdb:" + imdb;
            var tmdb = m.GetProviderId("Tmdb");
            return string.IsNullOrWhiteSpace(tmdb) ? null : "tmdb:" + tmdb;
        }

        // Same as HomeScreenCompanionService.SanitizeFolderName (the top-list folder name).
        private static string SanitizeFolderName(string name)
        {
            var invalid = Path.GetInvalidFileNameChars();
            var safe = new string((name ?? "unknown").Select(c => Array.IndexOf(invalid, c) >= 0 ? '_' : c).ToArray()).Trim('.');
            return string.IsNullOrWhiteSpace(safe) ? "unknown" : safe;
        }
    }

    /// <summary>
    /// Draws the TOP 10 badge on the real poster as Emby serves it. The image file is never
    /// touched: Emby writes the result to its own enhanced-image cache, keyed by the image tag,
    /// which includes GetConfigurationCacheKey (rank, label, size, corner) while Supports() is true.
    /// </summary>
    public class RealPosterBadgeEnhancer : IImageEnhancer
    {
        public RealPosterBadgeEnhancer(ILibraryManager libraryManager, IJsonSerializer jsonSerializer, ILogManager logManager)
        {
            RealPosterBadges.Initialize(libraryManager, jsonSerializer, PluginLog.Wrap(logManager.GetLogger("HomeScreenCompanion"), "Real posters"));
        }

        public MetadataProviderPriority Priority => MetadataProviderPriority.Last;

        public bool Supports(BaseItem item, ImageType imageType)
            => imageType == ImageType.Primary && RealPosterBadges.TryGet(item, out _);

        public string GetConfigurationCacheKey(BaseItem item, ImageType imageType)
            => RealPosterBadges.TryGet(item, out var e) ? e.CacheKey : "";

        public ImageSize GetEnhancedImageSize(BaseItem item, ImageType imageType, int imageIndex, ImageSize originalImageSize)
            => originalImageSize;

        public EnhancedImageInfo GetEnhancedImageInfo(BaseItem item, string inputFile, ImageType imageType, int imageIndex)
            => new EnhancedImageInfo { RequiresTransparency = false };

        public Task EnhanceImageAsync(BaseItem item, string inputFile, string outputFile, ImageType imageType, int imageIndex)
        {
            if (!RealPosterBadges.TryGet(item, out var e))
            {
                File.Copy(inputFile, outputFile, true);
                return Task.CompletedTask;
            }
            return Task.Run(() =>
            {
                BadgeRenderer.RealPosterBadge(inputFile, outputFile, e.Rank, e.Label == "top10rank", e.Size, e.Corner);
            });
        }
    }
}
