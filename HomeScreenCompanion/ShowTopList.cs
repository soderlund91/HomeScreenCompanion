using MediaBrowser.Common.Net;
using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Entities.TV;
using MediaBrowser.Controller.Library;
using MediaBrowser.Controller.Persistence;
using MediaBrowser.Controller.Providers;
using MediaBrowser.Model.Entities;
using MediaBrowser.Model.IO;
using MediaBrowser.Model.Serialization;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security;
using System.Text;
using System.Threading;

namespace HomeScreenCompanion
{
    // TV-show top-lists. A series cannot simply be added to a second library — Emby keys items by
    // path, so the same folder in two libraries leaves the second one empty. Instead every show
    // list gets its own tvshows library holding a copy of each ranked series:
    //
    //   <list>/<Series (Year)>/tvshow.nfo            original's provider ids, rank as sorttitle
    //   <list>/<Series (Year)>/poster.jpg            ranked poster (landscape.jpg = ranked thumb)
    //   <list>/<Series (Year)>/Season 01/<..> S01E01.strm + .nfo   → the real episode file
    //
    // Emby keys episode user data by the series' provider ids + season/episode number, so played
    // state and resume points are shared with the originals in both directions. The remaining
    // images (backdrops, logos, season and episode images) are linked straight to the originals'
    // image paths, so the library needs no metadata providers and nothing is downloaded.
    //
    // Files are written differentially: unchanged files are left alone, so re-running a sync does
    // not make Emby re-create items or lose anything.
    internal static class ShowTopList
    {
        internal const string ContentTypeShows = "Shows";
        internal const string ManifestFileName = "hsc_shows.json";
        private const string PosterFileName = "poster.jpg";
        private const string ThumbFileName = "landscape.jpg";

        // Image types taken over from the original series/season/episode when the copy lacks them.
        // Primary and Thumb of a series copy are the ranked images and never come from the original.
        private static readonly ImageType[] SeriesLinkedImageTypes =
            { ImageType.Backdrop, ImageType.Logo, ImageType.Banner, ImageType.Art, ImageType.Disc };
        private static readonly ImageType[] ChildLinkedImageTypes =
            { ImageType.Primary, ImageType.Thumb, ImageType.Backdrop, ImageType.Banner };

        public class ManifestEntry
        {
            public int Rank { get; set; }
            public string Folder { get; set; } = "";
            public string SeriesId { get; set; } = "";
            public string ImdbId { get; set; } = "";
            public string Name { get; set; } = "";
            public int? Year { get; set; }
        }

        internal static bool IsShows(TopListHomeSection? tl) => tl != null && IsShows(tl.ContentType);
        internal static bool IsShows(string? contentType) =>
            string.Equals(contentType, ContentTypeShows, StringComparison.OrdinalIgnoreCase);

        internal static string? TopListsFolder
        {
            get
            {
                var dataPath = Plugin.Instance?.DataFolderPath;
                return string.IsNullOrEmpty(dataPath) ? null : Path.Combine(dataPath, "toplists") + Path.DirectorySeparatorChar;
            }
        }

        internal static bool IsCopy(BaseItem? item)
        {
            var folder = TopListsFolder;
            return folder != null && !string.IsNullOrEmpty(item?.Path)
                && item!.Path.StartsWith(folder, StringComparison.OrdinalIgnoreCase);
        }

        // ── Which series ─────────────────────────────────────────────────────────────────────

        internal static List<BaseItem> AllSeries(ILibraryManager libraryManager) =>
            libraryManager.GetItemList(new InternalItemsQuery
            {
                IncludeItemTypes = new[] { "Series" },
                Recursive = true,
                IsVirtualItem = false
            }).Where(s => !IsCopy(s)).ToList();

        // The series of a source's top-list, in list order (TopListMembers). Before the source's
        // first run since 4.5: the series behind its tag, in the tag's source order (tag_ranks/<tag>.json
        // holds the IMDb ids the tag sync matched, in list order), then by name. A season or episode
        // counts for its series.
        internal static List<BaseItem> SeriesForTag(ILibraryManager libraryManager, IJsonSerializer jsonSerializer, string tag)
        {
            var series = new List<BaseItem>();
            var seen = new HashSet<long>();
            var members = TopListMembers.Load(tag, libraryManager, jsonSerializer);
            foreach (var item in members?.Where(i => !i.IsVirtualItem && (i is Series || i is Season || i is Episode))
                ?? libraryManager.GetItemList(new InternalItemsQuery
                {
                    Tags = new[] { tag },
                    IncludeItemTypes = new[] { "Series", "Season", "Episode" },
                    Recursive = true,
                    IsVirtualItem = false
                }))
            {
                if (IsCopy(item)) continue;
                var s = item as Series ?? (item as Season)?.Series ?? (item as Episode)?.Series;
                if (s == null || IsCopy(s) || !seen.Add(s.InternalId)) continue;
                series.Add(s);
            }
            if (members != null) return series;

            var rank = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            try
            {
                var rankFile = Path.Combine(Plugin.Instance!.DataFolderPath, "tag_ranks", SanitizeName(tag) + ".json");
                if (File.Exists(rankFile))
                {
                    var ids = jsonSerializer.DeserializeFromFile<List<string>>(rankFile) ?? new List<string>();
                    for (int i = 0; i < ids.Count; i++)
                        if (!string.IsNullOrEmpty(ids[i]) && !rank.ContainsKey(ids[i])) rank[ids[i]] = i;
                }
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }

            return series
                .OrderBy(s => rank.TryGetValue(s.GetProviderId("Imdb") ?? "", out var r) ? r : int.MaxValue)
                .ThenBy(s => s.SortName ?? s.Name, StringComparer.OrdinalIgnoreCase)
                .ToList();
        }

        // Resolves manual list entries to library series: by item id, then (backups restored on
        // another server) by IMDb id.
        internal static List<BaseItem> ResolveSeries(ILibraryManager libraryManager, IEnumerable<(string ItemId, string ImdbId)> entries)
        {
            var result = new List<BaseItem>();
            var seen = new HashSet<long>();
            Dictionary<string, BaseItem>? imdbLookup = null;
            foreach (var (itemId, imdbId) in entries)
            {
                BaseItem? s = null;
                if (Guid.TryParse(itemId, out var g)) s = libraryManager.GetItemById(g);
                if ((!(s is Series) || IsCopy(s)) && !string.IsNullOrWhiteSpace(imdbId))
                {
                    imdbLookup ??= AllSeries(libraryManager)
                        .Where(x => !string.IsNullOrEmpty(x.GetProviderId("Imdb")))
                        .GroupBy(x => x.GetProviderId("Imdb"), StringComparer.OrdinalIgnoreCase)
                        .ToDictionary(x => x.Key, x => x.First(), StringComparer.OrdinalIgnoreCase);
                    imdbLookup.TryGetValue(imdbId.Trim(), out s);
                }
                if (s is Series && !IsCopy(s) && seen.Add(s.InternalId)) result.Add(s);
            }
            return result;
        }

        // ── Writing the list folder ─────────────────────────────────────────────────────────

        // Makes the list folder match rankedSeries (in rank order). Returns the number of series
        // written; series without any episode file are skipped. badgeStyle is the list's look
        // (BadgeLook); rankExtras, when given, works out the badge extras for the final ranking.
        internal static int WriteFolder(
            string folderPath, IList<BaseItem> rankedSeries, string badgeStyle,
            ILibraryManager libraryManager, IJsonSerializer jsonSerializer, IHttpClient httpClient,
            IProviderManager providerManager, IFileSystem fileSystem, Action<string>? log = null,
            Func<IList<BaseItem>, Dictionary<Guid, RankExtras>>? rankExtras = null)
        {
            Directory.CreateDirectory(folderPath);

            // A list that was a movie list before: its top-level files do not belong here.
            foreach (var pattern in new[] { "*.strm", "*.nfo", "*.jpg" })
                foreach (var f in Directory.GetFiles(folderPath, pattern))
                    TryDelete(f);

            var withEpisodes = rankedSeries
                .Select(s => (Series: s, Episodes: EpisodesOf(libraryManager, s)))
                .Where(x => x.Episodes.Count > 0)
                .ToList();
            foreach (var skipped in rankedSeries.Where(s => withEpisodes.All(x => x.Series.InternalId != s.InternalId)))
                log?.Invoke($"Top-list: '{skipped.Name}' has no episode files — left out");

            int digits = Math.Max(2, withEpisodes.Count.ToString().Length);
            var extras = rankExtras?.Invoke(withEpisodes.Select(x => x.Series).ToList()) ?? new Dictionary<Guid, RankExtras>();
            var manifest = new List<ManifestEntry>();
            var usedFolders = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var tempDir = Path.Combine(Path.GetTempPath(), "hsc_showlist_" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(tempDir);
            try
            {
                int rank = 0;
                foreach (var (series, episodes) in withEpisodes)
                {
                    rank++;
                    var folderName = SeriesFolderName(series, usedFolders);
                    var seriesDir = Path.Combine(folderPath, folderName);
                    Directory.CreateDirectory(seriesDir);

                    var sortTitle = rank.ToString().PadLeft(digits, '0');
                    WriteIfChanged(Path.Combine(seriesDir, "tvshow.nfo"), BuildSeriesNfo(series, sortTitle));

                    var renderBase = Path.Combine(tempDir, "rank" + rank);
                    HomeScreenCompanionService.WriteRankedImages(series, rank, renderBase, badgeStyle, tempDir,
                        httpClient, providerManager, libraryManager, fileSystem, log,
                        extras.TryGetValue(series.Id, out var rx) ? rx : null);
                    CopyIfChanged(renderBase + ".jpg", Path.Combine(seriesDir, PosterFileName));
                    CopyIfChanged(renderBase + "-thumb.jpg", Path.Combine(seriesDir, ThumbFileName));

                    WriteEpisodes(seriesDir, series, episodes);

                    manifest.Add(new ManifestEntry
                    {
                        Rank = rank,
                        Folder = folderName,
                        SeriesId = series.Id.ToString("N"),
                        ImdbId = series.GetProviderId("Imdb") ?? "",
                        Name = series.Name ?? "",
                        Year = series.ProductionYear
                    });
                }
            }
            finally { try { Directory.Delete(tempDir, true); } catch { } }

            // Series that left the list.
            foreach (var dir in Directory.GetDirectories(folderPath))
                if (!usedFolders.Contains(Path.GetFileName(dir)))
                    try { Directory.Delete(dir, true); } catch (Exception ex) { log?.Invoke($"Top-list: could not remove '{dir}' — {ex.Message}"); }

            jsonSerializer.SerializeToFile(manifest, Path.Combine(folderPath, ManifestFileName));
            return manifest.Count;
        }

        private static List<BaseItem> EpisodesOf(ILibraryManager libraryManager, BaseItem series) =>
            libraryManager.GetItemList(new InternalItemsQuery
            {
                AncestorIds = new[] { series.InternalId },
                IncludeItemTypes = new[] { "Episode" },
                Recursive = true,
                IsVirtualItem = false
            }).Where(e => !string.IsNullOrEmpty(e.Path) && e.IndexNumber.HasValue && !IsCopy(e))
              .OrderBy(e => e.ParentIndexNumber ?? 1).ThenBy(e => e.IndexNumber)
              .ToList();

        private static string SeriesFolderName(BaseItem series, HashSet<string> used)
        {
            var name = SanitizeName(series.Name).Trim();
            if (series.ProductionYear.HasValue && series.ProductionYear > 0) name += $" ({series.ProductionYear})";
            var candidate = name;
            for (int i = 2; !used.Add(candidate); i++) candidate = $"{name} [{i}]";
            return candidate;
        }

        private static void WriteEpisodes(string seriesDir, BaseItem series, List<BaseItem> episodes)
        {
            var wanted = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var seriesName = SanitizeName(series.Name);

            foreach (var ep in episodes)
            {
                int season = ep.ParentIndexNumber ?? 1;
                int number = ep.IndexNumber!.Value;
                var end = (ep as Episode)?.IndexNumberEnd;
                var seasonDir = Path.Combine(seriesDir, season == 0 ? "Specials" : $"Season {season:00}");
                var baseName = $"{seriesName} S{season:00}E{number:00}" + (end.HasValue && end > number ? $"-E{end:00}" : "");
                var strmPath = Path.Combine(seasonDir, baseName + ".strm");
                if (!wanted.Add(strmPath)) continue; // another version of the same episode

                Directory.CreateDirectory(seasonDir);
                var seasonNfo = Path.Combine(seasonDir, "season.nfo");
                if (wanted.Add(seasonNfo)) WriteIfChanged(seasonNfo, BuildSeasonNfo(season));

                var nfoPath = Path.Combine(seasonDir, baseName + ".nfo");
                wanted.Add(nfoPath);
                WriteIfChanged(nfoPath, BuildEpisodeNfo(ep, season, number, end));
                // .strm last: the folder is a watched library and Emby creates the episode the
                // moment it sees the .strm — its .nfo must already be there.
                WriteIfChanged(strmPath, ep.Path);
            }

            // Episodes and seasons that are gone from the original.
            foreach (var seasonDir in Directory.GetDirectories(seriesDir))
            {
                foreach (var f in Directory.GetFiles(seasonDir))
                    if (!wanted.Contains(f)) TryDelete(f);
                if (Directory.GetFileSystemEntries(seasonDir).Length == 0)
                    try { Directory.Delete(seasonDir); } catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            }
        }

        // ── NFO files ───────────────────────────────────────────────────────────────────────

        private static string BuildSeriesNfo(BaseItem series, string sortTitle)
        {
            var sb = new StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<tvshow>\n");
            Tag(sb, "title", series.Name);
            Tag(sb, "originaltitle", series.OriginalTitle);
            Tag(sb, "sorttitle", sortTitle);
            if (series.ProductionYear > 0) Tag(sb, "year", series.ProductionYear.ToString());
            if (series.PremiereDate.HasValue) Tag(sb, "premiered", NfoDate(series.PremiereDate.Value));
            Tag(sb, "plot", series.Overview);
            Tag(sb, "tagline", series.Tagline);
            Tag(sb, "mpaa", series.OfficialRating);
            if (series.CommunityRating.HasValue)
                Tag(sb, "rating", series.CommunityRating.Value.ToString(System.Globalization.CultureInfo.InvariantCulture));
            foreach (var g in series.Genres ?? Array.Empty<string>()) Tag(sb, "genre", g);
            foreach (var s in series.Studios ?? Array.Empty<string>()) Tag(sb, "studio", s);
            Tag(sb, "imdbid", series.GetProviderId("Imdb"));
            Tag(sb, "tvdbid", series.GetProviderId("Tvdb"));
            Tag(sb, "tmdbid", series.GetProviderId("Tmdb"));
            sb.Append("  <lockedfields>SortName|Images</lockedfields>\n</tvshow>");
            return sb.ToString();
        }

        private static string BuildSeasonNfo(int season) =>
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<season>\n" +
            $"  <seasonnumber>{season}</seasonnumber>\n  <lockedfields>Images</lockedfields>\n</season>";

        private static string BuildEpisodeNfo(BaseItem ep, int season, int number, int? end)
        {
            var sb = new StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<episodedetails>\n");
            Tag(sb, "title", ep.Name);
            Tag(sb, "season", season.ToString());
            Tag(sb, "episode", number.ToString());
            if (end.HasValue && end > number) Tag(sb, "episodenumberend", end.Value.ToString());
            Tag(sb, "plot", ep.Overview);
            if (ep.PremiereDate.HasValue) Tag(sb, "aired", NfoDate(ep.PremiereDate.Value));
            sb.Append("  <lockedfields>Images</lockedfields>\n</episodedetails>");
            return sb.ToString();
        }

        // Dates from online providers are stored as UTC midnight; dates Emby read from an nfo are
        // local midnight converted to UTC. Emby reads the date we write as local midnight again,
        // so give back the calendar date it was meant to be.
        private static string NfoDate(DateTimeOffset date)
        {
            var utc = date.UtcDateTime;
            return (utc.TimeOfDay == TimeSpan.Zero ? utc : date.ToLocalTime().DateTime).ToString("yyyy-MM-dd");
        }

        private static void Tag(StringBuilder sb, string name, string? value)
        {
            if (string.IsNullOrWhiteSpace(value)) return;
            sb.Append("  <").Append(name).Append('>').Append(SecurityElement.Escape(value)).Append("</").Append(name).Append(">\n");
        }

        // ── Reading a list folder ───────────────────────────────────────────────────────────

        internal static bool IsShowFolder(string folderPath) => File.Exists(Path.Combine(folderPath, ManifestFileName));

        // A list folder that is about to hold movies: drop what a show list left in it.
        internal static void ClearShowFolder(string folderPath)
        {
            if (!IsShowFolder(folderPath)) return;
            foreach (var dir in Directory.GetDirectories(folderPath))
                try { Directory.Delete(dir, true); } catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            TryDelete(Path.Combine(folderPath, ManifestFileName));
        }

        internal static List<ManifestEntry> ReadManifest(string folderPath, IJsonSerializer jsonSerializer)
        {
            try
            {
                var file = Path.Combine(folderPath, ManifestFileName);
                if (File.Exists(file))
                    return (jsonSerializer.DeserializeFromFile<List<ManifestEntry>>(file) ?? new List<ManifestEntry>())
                        .OrderBy(e => e.Rank).ToList();
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            return new List<ManifestEntry>();
        }

        // ── Indexed copies: rank, ranked images, images linked from the originals ───────────

        // Pushes rank (SortName) and images onto the copies Emby has already indexed, so a new
        // order or badge shows without waiting for a library scan. Safe to run repeatedly.
        internal static void ApplyIndexed(string folderPath, ILibraryManager libraryManager, IJsonSerializer jsonSerializer)
        {
            var manifest = ReadManifest(folderPath, jsonSerializer);
            if (manifest.Count == 0) return;

            // The originals' episodes by path, one query per series instead of one per episode.
            var originalsByPath = new Dictionary<string, BaseItem>(StringComparer.OrdinalIgnoreCase);
            foreach (var entry in manifest)
            {
                var series = Guid.TryParse(entry.SeriesId, out var g) ? libraryManager.GetItemById(g) : null;
                if (series == null) continue;
                foreach (var ep in EpisodesOf(libraryManager, series))
                    originalsByPath[ep.Path] = ep;
            }

            var prefix = folderPath.TrimEnd(Path.DirectorySeparatorChar) + Path.DirectorySeparatorChar;
            var copies = libraryManager.GetItemList(new InternalItemsQuery
            {
                IncludeItemTypes = new[] { "Series", "Season", "Episode" },
                Recursive = true,
                IsVirtualItem = false,
                PathStartsWith = prefix
            }).Where(i => !string.IsNullOrEmpty(i.Path) && i.Path.StartsWith(prefix, StringComparison.OrdinalIgnoreCase));

            foreach (var copy in copies)
                try { Refresh(copy, libraryManager, jsonSerializer, p => originalsByPath.TryGetValue(p, out var o) ? o : null); } catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
        }

        // Brings one copy (series, season or episode) up to date with its original: rank, images
        // and the metadata shown on screen. Emby only fills empty fields when it re-reads an nfo,
        // so later changes to the original are copied here. Returns true when the copy was saved.
        internal static bool Refresh(BaseItem copy, ILibraryManager libraryManager, IJsonSerializer jsonSerializer,
            Func<string, BaseItem?>? findOriginalEpisode = null)
        {
            bool changed = false;
            if (copy is Series)
            {
                var seriesDir = copy.Path.TrimEnd(Path.DirectorySeparatorChar);
                var manifest = ReadManifest(Path.GetDirectoryName(seriesDir) ?? "", jsonSerializer);
                var entry = manifest.FirstOrDefault(e => string.Equals(e.Folder, Path.GetFileName(seriesDir), StringComparison.OrdinalIgnoreCase));
                if (entry == null) return false;

                var sortName = entry.Rank.ToString().PadLeft(Math.Max(2, manifest.Count.ToString().Length), '0');
                if (!string.Equals(copy.SortName, sortName, StringComparison.Ordinal)) { copy.SortName = sortName; changed = true; }

                changed |= SetLocalImage(copy, ImageType.Primary, Path.Combine(seriesDir, PosterFileName));
                changed |= SetLocalImage(copy, ImageType.Thumb, Path.Combine(seriesDir, ThumbFileName));

                var original = Guid.TryParse(entry.SeriesId, out var g) ? libraryManager.GetItemById(g) : null;
                if (original != null)
                {
                    changed |= SyncMetadata(copy, original, isSeries: true);
                    changed |= LinkImages(copy, original, SeriesLinkedImageTypes);
                }
            }
            else if (copy is Season season)
            {
                var original = OriginalSeason(season, libraryManager, jsonSerializer);
                if (original != null)
                {
                    changed |= SyncMetadata(copy, original, isSeries: false);
                    changed |= LinkImages(copy, original, ChildLinkedImageTypes);
                }
            }
            else if (copy is Episode)
            {
                string? target = null;
                try { target = File.ReadAllText(copy.Path).Trim(); } catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
                if (string.IsNullOrEmpty(target)) return false;
                var original = findOriginalEpisode != null ? findOriginalEpisode(target!) : libraryManager.FindByPath(target, false);
                if (original == null || IsCopy(original)) return false;

                changed |= SyncMetadata(copy, original, isSeries: false);
                changed |= SyncVideoInfo(copy, original);
                changed |= LinkImages(copy, original, ChildLinkedImageTypes);
                CopyMediaStreams(copy, original);
            }

            if (changed)
                libraryManager.UpdateItem(copy, copy.GetParent(), ItemUpdateType.MetadataEdit, null);
            return changed;
        }

        // ── Media info ───────────────────────────────────────────────────────────────────────
        // Emby never probes a .strm during a scan, so a copy episode would have no streams
        // (resolution, audio and subtitle tracks on its page) until it is played. Movie copies
        // get them from an ffprobe per item; episodes take them from the original instead,
        // which gives the same result without probing every episode of every list.

        private static IItemRepository? _itemRepository;
        private static IMediaSourceManager? _mediaSourceManager;

        internal static void Initialize(IItemRepository itemRepository, IMediaSourceManager mediaSourceManager)
        {
            _itemRepository = itemRepository;
            _mediaSourceManager = mediaSourceManager;
        }

        private static bool SyncVideoInfo(BaseItem copy, BaseItem original)
        {
            bool changed = false;
            if (copy.Container != original.Container) { copy.Container = original.Container; changed = true; }
            if (copy.Width != original.Width) { copy.Width = original.Width; changed = true; }
            if (copy.Height != original.Height) { copy.Height = original.Height; changed = true; }
            if (copy.TotalBitrate != original.TotalBitrate) { copy.TotalBitrate = original.TotalBitrate; changed = true; }
            if (copy.Size != original.Size) { copy.Size = original.Size; changed = true; }
            return changed;
        }

        private static void CopyMediaStreams(BaseItem copy, BaseItem original)
        {
            if (_itemRepository == null || _mediaSourceManager == null) return;
            var theirs = _mediaSourceManager.GetMediaStreams(original) ?? new List<MediaStream>();
            if (theirs.Count == 0) return;
            var mine = _mediaSourceManager.GetMediaStreams(copy) ?? new List<MediaStream>();
            if (StreamSignature(mine) == StreamSignature(theirs)) return;
            _itemRepository.SaveMediaStreams(copy.InternalId, theirs, CancellationToken.None);
        }

        private static string StreamSignature(List<MediaStream> streams) =>
            string.Join("|", streams.OrderBy(s => s.Index)
                .Select(s => $"{s.Index}:{s.Type}:{s.Codec}:{s.Language}:{s.Width}x{s.Height}:{s.Channels}:{s.IsExternal}:{s.Path}"));

        // The fields a card or details page shows. A copy's SortName is its rank and stays.
        private static bool SyncMetadata(BaseItem copy, BaseItem original, bool isSeries)
        {
            bool changed = false;
            if (!string.IsNullOrEmpty(original.Name) && copy.Name != original.Name) { copy.Name = original.Name; changed = true; }
            if (copy.Overview != original.Overview) { copy.Overview = original.Overview; changed = true; }
            if (copy.PremiereDate != original.PremiereDate) { copy.PremiereDate = original.PremiereDate; changed = true; }
            if (copy.ProductionYear != original.ProductionYear) { copy.ProductionYear = original.ProductionYear; changed = true; }
            if (copy.CommunityRating != original.CommunityRating) { copy.CommunityRating = original.CommunityRating; changed = true; }
            if (copy.OfficialRating != original.OfficialRating) { copy.OfficialRating = original.OfficialRating; changed = true; }
            // Emby does not probe a .strm during a scan, and without a runtime a stopped episode
            // is marked played instead of getting a resume point.
            if ((original.RunTimeTicks ?? 0) > 0 && copy.RunTimeTicks != original.RunTimeTicks) { copy.RunTimeTicks = original.RunTimeTicks; changed = true; }
            if (isSeries)
            {
                if (copy.Tagline != original.Tagline) { copy.Tagline = original.Tagline; changed = true; }
                if (!SameSet(copy.Genres, original.Genres)) { copy.Genres = (original.Genres ?? Array.Empty<string>()).ToArray(); changed = true; }
                if (!SameSet(copy.Studios, original.Studios)) { copy.Studios = (original.Studios ?? Array.Empty<string>()).ToArray(); changed = true; }
            }
            return changed;
        }

        private static bool SameSet(string[]? a, string[]? b) =>
            (a ?? Array.Empty<string>()).OrderBy(x => x).SequenceEqual((b ?? Array.Empty<string>()).OrderBy(x => x));

        private static BaseItem? OriginalSeason(Season season, ILibraryManager libraryManager, IJsonSerializer jsonSerializer)
        {
            var seriesCopy = season.Series;
            if (seriesCopy == null || string.IsNullOrEmpty(seriesCopy.Path) || !season.IndexNumber.HasValue) return null;
            var seriesDir = seriesCopy.Path.TrimEnd(Path.DirectorySeparatorChar);
            var entry = ReadManifest(Path.GetDirectoryName(seriesDir) ?? "", jsonSerializer)
                .FirstOrDefault(e => string.Equals(e.Folder, Path.GetFileName(seriesDir), StringComparison.OrdinalIgnoreCase));
            var original = entry != null && Guid.TryParse(entry.SeriesId, out var g) ? libraryManager.GetItemById(g) : null;
            if (original == null) return null;
            return libraryManager.GetItemList(new InternalItemsQuery
            {
                AncestorIds = new[] { original.InternalId },
                IncludeItemTypes = new[] { "Season" },
                IndexNumber = season.IndexNumber,
                Recursive = true
            }).FirstOrDefault(s => !IsCopy(s));
        }

        // Adds the original's images of the given types where the copy has none.
        private static bool LinkImages(BaseItem copy, BaseItem original, ImageType[] types)
        {
            var images = (copy.ImageInfos ?? Array.Empty<ItemImageInfo>()).ToList();
            var source = original.ImageInfos ?? Array.Empty<ItemImageInfo>();
            bool changed = false;
            foreach (var type in types)
            {
                if (images.Any(i => i.Type == type)) continue;
                foreach (var img in source.Where(i => i.Type == type && !string.IsNullOrEmpty(i.Path)))
                {
                    images.Add(new ItemImageInfo
                    {
                        Path = img.Path,
                        Type = img.Type,
                        DateModified = img.DateModified,
                        Width = img.Width,
                        Height = img.Height
                    });
                    changed = true;
                }
            }
            if (changed) copy.ImageInfos = images.ToArray();
            return changed;
        }

        // Points the copy's image of this type at a ranked file in its folder. The files are
        // replaced in place when the rank or badge changes, so DateModified is what tells Emby
        // to stop serving the cached picture.
        private static bool SetLocalImage(BaseItem copy, ImageType type, string path)
        {
            if (!File.Exists(path)) return false;
            var modified = File.GetLastWriteTimeUtc(path);
            var images = (copy.ImageInfos ?? Array.Empty<ItemImageInfo>()).ToList();
            var current = images.Where(i => i.Type == type).ToList();
            if (current.Count == 1 && string.Equals(current[0].Path, path, StringComparison.OrdinalIgnoreCase)
                && current[0].DateModified.UtcDateTime == modified)
                return false;
            images.RemoveAll(i => i.Type == type);
            images.Add(new ItemImageInfo { Path = path, Type = type, DateModified = modified });
            copy.ImageInfos = images.ToArray();
            return true;
        }

        // ── Helpers ────────────────────────────────────────────────────────────────────────

        // Same rule as the list folders and tag_ranks files.
        internal static string SanitizeName(string? name)
        {
            var invalid = Path.GetInvalidFileNameChars();
            var safe = new string((name ?? "unknown").Select(c => Array.IndexOf(invalid, c) >= 0 ? '_' : c).ToArray()).Trim('.');
            return string.IsNullOrWhiteSpace(safe) ? "unknown" : safe;
        }

        // Both return true when they wrote the file (it was missing or different).
        internal static bool WriteIfChanged(string path, string content)
        {
            try
            {
                if (File.Exists(path) && File.ReadAllText(path) == content) return false;
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            File.WriteAllText(path, content);
            return true;
        }

        internal static bool CopyIfChanged(string source, string target)
        {
            if (!File.Exists(source)) return false;
            try
            {
                if (File.Exists(target) && new FileInfo(target).Length == new FileInfo(source).Length
                    && File.ReadAllBytes(target).SequenceEqual(File.ReadAllBytes(source)))
                    return false;
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            File.Copy(source, target, overwrite: true);
            return true;
        }

        private static void TryDelete(string path)
        {
            try { File.Delete(path); } catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
        }
    }
}
