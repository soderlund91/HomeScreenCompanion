using MediaBrowser.Common.Net;
using MediaBrowser.Controller.Collections;
using MediaBrowser.Controller.Playlists;
using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Library;
using MediaBrowser.Controller.Providers;
using MediaBrowser.Model.Entities;
using MediaBrowser.Model.IO;
using MediaBrowser.Model.Logging;
using MediaBrowser.Model.Querying;
using MediaBrowser.Model.Serialization;
using MediaBrowser.Model.Tasks;
using MediaBrowser.Model.Users;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace HomeScreenCompanion
{
    public class HomeScreenCompanionTask : IScheduledTask
    {
        private readonly ILibraryManager _libraryManager;
        private readonly ICollectionManager _collectionManager;
        private readonly IUserManager _userManager;
        private readonly IUserViewManager _userViewManager;
        private readonly IUserDataManager _userDataManager;
        private readonly IHttpClient _httpClient;
        private readonly IJsonSerializer _jsonSerializer;
        private readonly ILogger _logger;
        private readonly ILibraryMonitor _libraryMonitor;
        private readonly IPlaylistManager _playlistManager;
        private readonly IProviderManager _providerManager;
        private readonly IFileSystem _fileSystem;
        private RunLog _log;

        public static HomeScreenCompanionTask? Instance { get; private set; }
        public static string LastRunStatus { get; private set; } = "Never";
        public static List<string> ExecutionLog { get; } = new List<string>();
        public static bool IsRunning { get; private set; } = false;
        public static DateTime? LastStartedUtc { get; private set; }

        private static void PersistLog() => LogStore.Save(LogStore.Sync, ExecutionLog, LastRunStatus, LastStartedUtc);

        // Brings back the last run's log and status after a server restart.
        internal static void RestoreLog()
        {
            var saved = LogStore.Load(LogStore.Sync);
            if (saved == null || IsRunning) return;
            lock (ExecutionLog)
            {
                if (ExecutionLog.Count > 0) return;
                ExecutionLog.AddRange(saved.Lines ?? new List<string>());
            }
            LastRunStatus = LogStore.RestoredStatus(saved.Status);
            LastStartedUtc = saved.StartedUtc;
        }

        private struct CachedMediaInfo
        {
            public bool Is4k, Is8k, Is1080, Is720, IsSd;
            public bool IsHevc, IsAv1, IsH264;
            public bool IsHdr, IsHdr10, IsDv;
            public bool IsAtmos, IsTrueHd, IsDtsHdMa, IsDts, IsAc3, IsAac;
            public bool Is71, Is51, IsStereo, IsMono;
            public HashSet<string> AudioLanguages;
            public double? DateModifiedDays;
            public double? FileSizeMb;
            // Music-specific fields
            public int? BitRate;       // kbps
            public int? SampleRate;    // Hz
            public int? BitsPerSample; // bit depth
            public int? TrackNumber;   // IndexNumber on the item
            public int? DiscNumber;    // ParentIndexNumber on the item
        }

        // Utökar ItemsQuery med IsUnplayed så att Embys JSON-serialisering inkluderar fältet.
        // IsPlayed finns nativt i ItemsQuery (Emby 4.10.0.10+) och sätts via basklassen.
        private class ExtendedItemsQuery : ItemsQuery
        {
            public bool? IsUnplayed { get; set; }
        }

        private class GroupRunStats
        {
            public string? DisplayName;
            public string? SourceType;
            public bool Skipped;
            public string? SkipReason;
            public string? ErrorMessage;
            public int ListCount;
            public int MatchCount;
            public bool EnableTag;
            public int TagsAdded;
            public int TagsRemoved;
            public bool EnableCollection;
            public bool CollectionCreated;
            public int CollectionItemsAdded;
            public int CollectionItemsRemoved;
            public bool EnableHomeSection;
            public bool HomeSectionSynced;
            public int HomeSectionUserCount;
            public bool HomeSectionRemoved;
            public bool BoxSetHse;   // tag applied to BoxSet only, not to items
            public bool BoxSetFound; // the target BoxSet was found in the library
            public int BoxSetTaggedCount; // how many BoxSets were successfully tagged (for merged log display)
            public string? TagName;
            public string? CollectionName;
            public int GroupIndex;
            public int GroupTotal;
            // Display-only fields used by the execution log
            public string? SourceLabel;                 // "Trakt", "AI · OpenAI", "Smart playlist", ...
            public bool EnablePlaylist;
            public string? PlaylistName;
            public int PlaylistUsersTotal;
            public int PlaylistUsersCreated;
            public int PlaylistUsersUpdated;
            public int PlaylistUsersFailed;
            public List<string> Warnings = new List<string>();
            public List<string> MissingItems = new List<string>(); // list titles not found in the library
            public long ElapsedMs;
        }

        public HomeScreenCompanionTask(ILibraryManager libraryManager, ICollectionManager collectionManager, IPlaylistManager playlistManager, IUserManager userManager, IUserViewManager userViewManager, IUserDataManager userDataManager, IHttpClient httpClient, IJsonSerializer jsonSerializer, ILogManager logManager, ILibraryMonitor libraryMonitor, IProviderManager providerManager, IFileSystem fileSystem)
        {
            _libraryManager = libraryManager;
            _collectionManager = collectionManager;
            _userManager = userManager;
            _userViewManager = userViewManager;
            _userDataManager = userDataManager;
            _httpClient = httpClient;
            _jsonSerializer = jsonSerializer;
            _logger = logManager.GetLogger("HomeScreenCompanion");
            _libraryMonitor = libraryMonitor;
            _playlistManager = playlistManager;
            _providerManager = providerManager;
            _fileSystem = fileSystem;
            _log = new RunLog(ExecutionLog, _logger, "", false);
            Instance = this;
        }

        public string Key => "HomeScreenCompanionSyncTask";
        public string Name => "Tag & Collection Sync";
        public string Description => "Syncs tags and collections from MDBList, Trakt, Playlists and Local Media.";
        public string Category => "Home Screen Companion";

        public IEnumerable<TaskTriggerInfo> GetDefaultTriggers()
        {
            return new[] { new TaskTriggerInfo { Type = TaskTriggerInfo.TriggerDaily, TimeOfDayTicks = TimeSpan.FromHours(4).Ticks } };
        }

        public async Task Execute(CancellationToken cancellationToken, IProgress<double> progress)
        {
            IsRunning = true;
            try
            {
                lock (ExecutionLog) ExecutionLog.Clear();
                LastStartedUtc = DateTime.UtcNow;
                LastRunStatus = "Running...";

                var config = Plugin.Instance?.Configuration;
                if (config == null) return;
                _writtenTags = BuildWrittenTags(config);

                bool debug = config.ExtendedConsoleOutput;
                bool dryRun = config.DryRunMode;
                bool logMissing = config.LogMissingItems;
                _log = new RunLog(ExecutionLog, _logger, "", debug);

                var startTime = DateTime.Now;
                var runTimer = System.Diagnostics.Stopwatch.StartNew();
                _log.Rule();
                _log.Info($"Home Screen Companion v{Plugin.Instance?.Version}  ·  {startTime:yyyy-MM-dd HH:mm}  ·  Full sync");
                if (dryRun) _log.Warn("DRY RUN — nothing will be changed, the log shows what would happen");

                var allItems = _libraryManager.GetItemList(new InternalItemsQuery
                {
                    IncludeItemTypes = BuildItemTypes(config),
                    Recursive = true,
                    IsVirtualItem = false
                }).ToList();

                // Items that live under the top-list folder are .strm virtual copies — never tag them
                // so they don't bleed into tag-based home screen sections.
                var topListsFolder = Path.Combine(Plugin.Instance.DataFolderPath, "toplists") + Path.DirectorySeparatorChar;

                var imdbLookup = new Dictionary<string, List<BaseItem>>(StringComparer.OrdinalIgnoreCase);
                foreach (var item in allItems)
                {
                    if (item.LocationType != LocationType.FileSystem) continue;
                    if (!string.IsNullOrEmpty(item.Path) &&
                        item.Path.StartsWith(topListsFolder, StringComparison.OrdinalIgnoreCase))
                        continue;
                    var imdb = item.GetProviderId("Imdb");
                    if (!string.IsNullOrEmpty(imdb))
                    {
                        if (!imdbLookup.ContainsKey(imdb)) imdbLookup[imdb] = new List<BaseItem>();
                        imdbLookup[imdb].Add(item);
                    }
                }

                // Candidates for local matching (MediaInfo filters, AI title fallback) — top-list
                // copies share genre/year/media info with their original and must never match.
                // allItems itself still includes them so stale managed tags get cleaned off.
                var matchableItems = allItems.Where(i => string.IsNullOrEmpty(i.Path)
                    || !i.Path.StartsWith(topListsFolder, StringComparison.OrdinalIgnoreCase)).ToList();

                int movieCount = allItems.Count(i => i.GetType().Name.Contains("Movie"));
                int seriesCount = allItems.Count(i => i.GetType().Name.Contains("Series"));
                int activeGroupTotal = config.Tags.Count(t => t.Active && !string.IsNullOrWhiteSpace(t.Tag));
                _log.Info($"  Library: {movieCount:N0} movies, {seriesCount:N0} series");
                _log.Info($"  Groups: {activeGroupTotal} active");
                _log.Rule();
                _log.Debug($"Library scan: {allItems.Count:N0} items, {imdbLookup.Count:N0} with IMDb id  ·  {RunLog.Elapsed(runTimer.Elapsed)}");

                var fetcher = new ListFetcher(_httpClient, _jsonSerializer);
                var desiredTagsMap = new Dictionary<Guid, HashSet<string>>();
                var allScannedEpisodeItems = new Dictionary<Guid, BaseItem>();
                var allScannedSeasonItems = new Dictionary<Guid, BaseItem>();
                var desiredCollectionsMap = new Dictionary<string, HashSet<long>>(StringComparer.OrdinalIgnoreCase);
                var collectionDescriptions = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                var collectionPosters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                var collectionThumbs = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                var collectionBackdrops = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                var managedTags = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                var activeCollections = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                var failedFetches = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                // A group with several sources (URLs / local sources) is stored as one flat TagConfig per
                // source. Playlists and rank files must be built from the union of all sources in the group,
                // so they are accumulated here and written once after the loop.
                var groupPlaylistItems = new Dictionary<string, (TagConfig Owner, List<BaseItem> Items, HashSet<Guid> Seen)>(StringComparer.OrdinalIgnoreCase);
                var rankIdsByTag = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
                // Groups where a source failed / returned nothing — their playlists are left untouched
                var playlistGroupsToSkip = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                var previouslyManagedTags = LoadFileHistory("homescreencompanion_history.txt");
                foreach (var t in previouslyManagedTags) managedTags.Add(t);

                var previouslyManagedCollections = LoadFileHistory("homescreencompanion_collections.txt");
                // Also track collection names from inactive groups so they get cleaned up
                // even if the group was only ever run via single-entry sync (which doesn't update history)
                foreach (var tc in config.Tags)
                {
                    if (tc.EnableCollection && !string.IsNullOrWhiteSpace(tc.Tag))
                    {
                        string cn = string.IsNullOrWhiteSpace(tc.CollectionName) ? tc.Tag.Trim() : tc.CollectionName.Trim();
                        if (!previouslyManagedCollections.Contains(cn))
                            previouslyManagedCollections.Add(cn);
                    }
                }

                TagCacheManager.Instance.Initialize(Plugin.Instance.DataFolderPath, _jsonSerializer);
                TagCacheManager.Instance.ClearCache();

                double step = 30.0 / (config.Tags.Count > 0 ? config.Tags.Count : 1);
                double currentProgress = 0;

                var seriesEpisodeCache = new Dictionary<long, BaseItem>();

                var personCache = new Dictionary<string, HashSet<long>>(StringComparer.OrdinalIgnoreCase);
                {
                    bool anyEpisodePersonCriteria = config.Tags.Any(t => t.Active
                        && TagConfigTargetsEpisodes(t)
                        && GetAllCriteria(t).Any(c => { var s = c.TrimStart('!'); return s.StartsWith("Actor:") || s.StartsWith("Director:") || s.StartsWith("Writer:"); }));
                    var allPersonCriteria = config.Tags
                        .Where(t => t.Active && (t.MediaInfoFilters?.Count > 0 || t.MediaInfoConditions?.Count > 0))
                        .SelectMany(t => (t.MediaInfoFilters ?? new List<MediaInfoFilter>())
                            .SelectMany(f => f.Criteria ?? new List<string>())
                            .Concat(t.MediaInfoConditions ?? new List<string>()))
                        .Select(c => c.Length > 0 && c[0] == '!' ? c.Substring(1) : c)
                        .Distinct(StringComparer.OrdinalIgnoreCase);
                    BaseItem[]? allPersonsGlobal = null;
                    foreach (var c in allPersonCriteria)
                    {
                        var p = c.Split(':');
                        if ((p.Length == 2 || (p.Length == 3 && (p[1] == "exact" || p[1] == "contains")))
                            && (p[0] == "Actor" || p[0] == "Director" || p[0] == "Writer")
                            && Enum.TryParse<MediaBrowser.Model.Entities.PersonType>(p[0], out var personTypeEnum))
                        {
                            string matchOp = p.Length == 3 ? p[1] : "exact";
                            string personNameRaw = p.Length == 3 ? p[2].Trim() : p[1].Trim();
                            var personTypes = anyEpisodePersonCriteria && p[0] == "Actor"
                                ? new[] { personTypeEnum, MediaBrowser.Model.Entities.PersonType.GuestStar }
                                : new[] { personTypeEnum };
                            foreach (var singleName in personNameRaw.Split(new[] { ',', '\n', '\r' }, StringSplitOptions.RemoveEmptyEntries).Select(n => n.Trim()).Where(n => n.Length > 0))
                            {
                                if (matchOp == "contains")
                                {
                                    string containsKey = $"{p[0]}:contains:{singleName}";
                                    if (personCache.ContainsKey(containsKey)) continue;
                                    allPersonsGlobal ??= _libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "Person" } }).ToArray();
                                    var combinedIds = new HashSet<long>();
                                    foreach (var matchingPerson in allPersonsGlobal.Where(person => person.Name?.IndexOf(singleName, StringComparison.OrdinalIgnoreCase) >= 0))
                                    {
                                        foreach (var mi in _libraryManager.GetItemList(new InternalItemsQuery
                                        {
                                            PersonIds = new[] { matchingPerson.InternalId },
                                            PersonTypes = personTypes,
                                            IncludeItemTypes = anyEpisodePersonCriteria ? new[] { "Movie", "Series", "Episode" } : new[] { "Movie", "Series" },
                                            Recursive = true,
                                            IsVirtualItem = false
                                        })) combinedIds.Add(mi.InternalId);
                                    }
                                    personCache[containsKey] = combinedIds;
                                }
                                else
                                {
                                    string indivKey = p.Length == 3 ? $"{p[0]}:{p[1]}:{singleName}" : $"{p[0]}:{singleName}";
                                    if (personCache.ContainsKey(indivKey)) continue;
                                    var personItem = _libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "Person" }, Name = singleName }).FirstOrDefault();
                                    personCache[indivKey] = personItem == null ? new HashSet<long>() :
                                        _libraryManager.GetItemList(new InternalItemsQuery
                                        {
                                            PersonIds = new[] { personItem.InternalId },
                                            PersonTypes = personTypes,
                                            IncludeItemTypes = anyEpisodePersonCriteria ? new[] { "Movie", "Series", "Episode" } : new[] { "Movie", "Series" },
                                            Recursive = true,
                                            IsVirtualItem = false
                                        }).Select(x => x.InternalId).ToHashSet();
                                }
                            }
                        }
                    }
                }

                var collectionMembershipCache = new Dictionary<string, HashSet<long>>(StringComparer.OrdinalIgnoreCase);
                {
                    var allCollPlCriteria = config.Tags
                        .Where(t => t.Active && (t.MediaInfoFilters?.Count > 0 || t.MediaInfoConditions?.Count > 0))
                        .SelectMany(t => GetAllCriteria(t))
                        .Select(c => c.Length > 0 && c[0] == '!' ? c.Substring(1) : c)
                        .Where(c => c.StartsWith("Collection:", StringComparison.OrdinalIgnoreCase) || c.StartsWith("Playlist:", StringComparison.OrdinalIgnoreCase))
                        .Distinct(StringComparer.OrdinalIgnoreCase);
                    foreach (var crit in allCollPlCriteria)
                    {
                        var colonIdx = crit.IndexOf(':');
                        if (colonIdx < 1) continue;
                        var sourceKind = crit.Substring(0, colonIdx);
                        var sourceNamesRaw = crit.Substring(colonIdx + 1).Trim();
                        string[] folderTypes = sourceKind.Equals("Playlist", StringComparison.OrdinalIgnoreCase)
                            ? new[] { "Playlist" } : new[] { "BoxSet" };
                        foreach (var singleName in SplitCommaValues(sourceNamesRaw))
                        {
                            var indivKey = sourceKind + ":" + singleName;
                            if (collectionMembershipCache.ContainsKey(indivKey)) continue;
                            var folder = _libraryManager.GetItemList(new InternalItemsQuery
                            {
                                IncludeItemTypes = folderTypes,
                                Recursive = true
                            }).FirstOrDefault(i => string.Equals(i.Name, singleName, StringComparison.OrdinalIgnoreCase));
                            if (folder == null) { collectionMembershipCache[indivKey] = new HashSet<long>(); continue; }
                            var members = sourceKind.Equals("Playlist", StringComparison.OrdinalIgnoreCase)
                                ? _libraryManager.GetItemList(new InternalItemsQuery { ListIds = new[] { folder.InternalId } })
                                : _libraryManager.GetItemList(new InternalItemsQuery { CollectionIds = new[] { folder.InternalId }, IsVirtualItem = false });
                            var ids = new HashSet<long>();
                            foreach (var m in members)
                            {
                                ids.Add(m.InternalId);
                                if (m.GetType().Name.Contains("Series"))
                                {
                                    foreach (var ep in _libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "Episode" }, Parent = m, Recursive = true, IsVirtualItem = false }))
                                        ids.Add(ep.InternalId);
                                }
                            }
                            collectionMembershipCache[indivKey] = ids;
                        }
                    }
                }

                var mediaInfoCache = new Dictionary<long, CachedMediaInfo>();
                if (config.Tags.Any(t => t.Active && (t.MediaInfoFilters?.Count > 0 || t.MediaInfoConditions?.Count > 0)))
                {
                    foreach (var item in allItems)
                    {
                        if (item.LocationType != LocationType.FileSystem) continue;
                        var resolved = ResolveItemForMediaInfo(item, seriesEpisodeCache);
                        mediaInfoCache[item.InternalId] = ExtractMediaInfo(resolved);
                    }
                }

                var userDataCache = new Dictionary<(Guid, long), (bool Played, DateTimeOffset? LastPlayedDate, int PlayCount)>();
                var seriesLastPlayedCache = new Dictionary<(Guid, long), DateTimeOffset?>();
                var preloadedUsers = _userManager.GetUserList(new UserQuery { IsDisabled = false });

                var activeTagOverrides = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                var activeCollectionOverrides = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (var tc in config.Tags)
                {
                    if (!tc.Active || !tc.OverrideWhenActive || string.IsNullOrWhiteSpace(tc.Tag)) continue;
                    if (!IsScheduleActive(tc.ActiveIntervals)) continue;
                    activeTagOverrides.Add(tc.Tag.Trim());
                    if (tc.EnableCollection)
                    {
                        var overrideCName = string.IsNullOrWhiteSpace(tc.CollectionName) ? tc.Tag.Trim() : tc.CollectionName.Trim();
                        activeCollectionOverrides.Add(overrideCName);
                    }
                }

                // Determine which pre-loads are needed based on active group criteria
                bool _needsSeriesLastPlayed = config.Tags.Any(t => t.Active && GetAllCriteria(t).Any(c =>
                    c.TrimStart('!').Split(':') is var _p && _p.Length == 4 && _p[0] == "LastPlayed"));
                bool _needsItemUserData = _needsSeriesLastPlayed || config.Tags.Any(t => t.Active && GetAllCriteria(t).Any(c =>
                {
                    var _p2 = c.TrimStart('!').Split(':');
                    return _p2[0] == "IsPlayed" || _p2[0] == "PlayCount" || _p2[0] == "WatchedByCount";
                }));

                if (_needsItemUserData && preloadedUsers?.Length > 0)
                {
                    // Pre-populate userDataCache for all top-level items (movies + series).
                    // Covers lazy GetUserData calls for IsPlayed / PlayCount / WatchedByCount / LastPlayed on movies.
                    foreach (var _user in preloadedUsers)
                    {
                        foreach (var _topItem in allItems)
                        {
                            var _k = (_user.Id, _topItem.InternalId);
                            if (userDataCache.ContainsKey(_k)) continue;
                            var _ud0 = _userDataManager?.GetUserData(_user, _topItem);
                            userDataCache[_k] = _ud0 == null ? (false, (DateTimeOffset?)null, 0) : (_ud0.Played, _ud0.LastPlayedDate, _ud0.PlayCount);
                        }
                    }
                }

                if (_needsSeriesLastPlayed && preloadedUsers?.Length > 0)
                {
                    // Pre-populate userDataCache for all episodes + build seriesLastPlayedCache.
                    // Without this, Execute() falls back to O(series × users) lazy GetItemList calls during the scan.
                    var _allEps = _libraryManager.GetItemList(new InternalItemsQuery
                    {
                        IncludeItemTypes = new[] { "Episode" },
                        Recursive = true,
                        IsVirtualItem = false
                    });
                    foreach (var _user in preloadedUsers)
                    {
                        foreach (var _ep in _allEps)
                        {
                            var _epKey = (_user.Id, _ep.InternalId);
                            if (userDataCache.ContainsKey(_epKey)) continue;
                            var _ud = _userDataManager?.GetUserData(_user, _ep);
                            userDataCache[_epKey] = _ud == null ? (false, (DateTimeOffset?)null, 0) : (_ud.Played, _ud.LastPlayedDate, _ud.PlayCount);
                        }
                        var _epsBySeries = new Dictionary<long, List<BaseItem>>();
                        foreach (var _ep in _allEps)
                        {
                            BaseItem? _ser = null;
                            var _par = _ep.Parent;
                            if (_par != null)
                            {
                                if (_par.GetType().Name.Contains("Series")) _ser = _par;
                                else if (_par.GetType().Name.Contains("Season") && _par.Parent?.GetType().Name.Contains("Series") == true) _ser = _par.Parent;
                            }
                            if (_ser == null) continue;
                            if (!_epsBySeries.ContainsKey(_ser.InternalId)) _epsBySeries[_ser.InternalId] = new List<BaseItem>();
                            _epsBySeries[_ser.InternalId].Add(_ep);
                        }
                        foreach (var _kvp in _epsBySeries)
                        {
                            var _sKey = (_user.Id, _kvp.Key);
                            if (seriesLastPlayedCache.ContainsKey(_sKey)) continue;
                            DateTimeOffset? _max = null;
                            foreach (var _ep in _kvp.Value)
                            {
                                if (userDataCache.TryGetValue((_user.Id, _ep.InternalId), out var _cd) && _cd.LastPlayedDate.HasValue)
                                    if (_max == null || _cd.LastPlayedDate > _max) _max = _cd.LastPlayedDate;
                            }
                            seriesLastPlayedCache[_sKey] = _max;
                        }
                    }
                }

                _log.Debug($"Caches ready after {RunLog.Elapsed(runTimer.Elapsed)}  ·  media-info {mediaInfoCache.Count:N0} items  ·  user-data {userDataCache.Count:N0} entries for {preloadedUsers?.Length ?? 0} users  ·  person lookups {personCache.Count}  ·  collection/playlist lookups {collectionMembershipCache.Count}");
                _log.Blank();
                _log.Info("» Fetching sources");
                var phaseTimer = System.Diagnostics.Stopwatch.StartNew();

                var statsList = new List<GroupRunStats>();
                var statsByGroupKey = new Dictionary<string, GroupRunStats>(StringComparer.OrdinalIgnoreCase);
                int activeGroupIdx = 0;
                bool aiConfigChanged = false;
                var tagAddedByTag = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                var tagRemovedByTag = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                var collCreatedSet = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                var collItemsAdded = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                var collItemsRemoved = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

                // Process OverrideWhenActive entries first so they can remove themselves from
                // activeTagOverrides before non-override entries for the same tag are evaluated.
                var orderedTags = config.Tags
                    .Where(t => t.OverrideWhenActive)
                    .Concat(config.Tags.Where(t => !t.OverrideWhenActive))
                    .ToList();

                foreach (var tagConfig in orderedTags)
                {
                    if (string.IsNullOrWhiteSpace(tagConfig.Tag)) continue;
                    string tagName = tagConfig.Tag.Trim();
                    managedTags.Add(tagName); // track all groups (active or inactive) so cleanup always runs

                    if (!tagConfig.Active) continue;

                    string displayName = !string.IsNullOrWhiteSpace(tagConfig.Name) ? $"{tagConfig.Name} [{tagName}]" : tagName;
                    string srcLabel = string.IsNullOrEmpty(tagConfig.SourceType) ? "External" : tagConfig.SourceType;
                    var ruleFeatures = new List<string>();
                    if (tagConfig.EnableTag && !tagConfig.OnlyCollection) ruleFeatures.Add("Tag");
                    if (tagConfig.EnableCollection) ruleFeatures.Add("Collection");
                    if (tagConfig.EnableHomeSection) ruleFeatures.Add("HS");
                    string featureStr = ruleFeatures.Count > 0 ? $"  ({string.Join(", ", ruleFeatures)})" : "";

                    activeGroupIdx++;
                    var gs = new GroupRunStats
                    {
                        DisplayName = displayName,
                        SourceType = srcLabel,
                        EnableTag = tagConfig.EnableTag && !tagConfig.OnlyCollection,
                        EnableCollection = tagConfig.EnableCollection,
                        EnableHomeSection = tagConfig.EnableHomeSection,
                        BoxSetHse = IsBoxSetHomeSectionEntry(tagConfig),
                        TagName = tagName,
                        GroupIndex = activeGroupIdx,
                        GroupTotal = activeGroupTotal,
                        SourceLabel = DescribeSource(tagConfig),
                        EnablePlaylist = tagConfig.EnablePlaylist,
                        PlaylistName = string.IsNullOrWhiteSpace(tagConfig.PlaylistName) ? tagConfig.Name : tagConfig.PlaylistName,
                        PlaylistUsersTotal = tagConfig.PlaylistUserIds?.Count ?? 0
                    };
                    // A multi-source group is stored as several flat entries; the playlist is synced once
                    // per group, so only the first entry's stats carry (and display) the playlist result.
                    if (!statsByGroupKey.ContainsKey(GroupKey(tagConfig))) statsByGroupKey[GroupKey(tagConfig)] = gs;
                    else gs.EnablePlaylist = false;

                    if (!IsScheduleActive(tagConfig.ActiveIntervals))
                    {
                        gs.Skipped = true;
                        gs.SkipReason = "not in schedule";
                        statsList.Add(gs);
                        WriteFetchLine(gs);
                        continue;
                    }

                    if (tagConfig.SourceType == "AI" && tagConfig.AiRefreshIntervalDays > 0 &&
                        tagConfig.AiLastRunDate > DateTime.MinValue &&
                        (DateTime.UtcNow - tagConfig.AiLastRunDate).TotalDays < tagConfig.AiRefreshIntervalDays)
                    {
                        var _nextAiRun = tagConfig.AiLastRunDate.AddDays(tagConfig.AiRefreshIntervalDays);
                        gs.Skipped = true;
                        gs.SkipReason = $"AI refresh not due until {_nextAiRun:yyyy-MM-dd}";
                        statsList.Add(gs);
                        WriteFetchLine(gs);
                        continue;
                    }

                    string cName = string.IsNullOrWhiteSpace(tagConfig.CollectionName) ? tagName : tagConfig.CollectionName.Trim();
                    gs.CollectionName = cName;

                    if (!tagConfig.OverrideWhenActive &&
                        (activeTagOverrides.Contains(tagName) ||
                         (tagConfig.EnableCollection && activeCollectionOverrides.Contains(cName))))
                    {
                        gs.Skipped = true;
                        gs.SkipReason = "overridden by a priority group with the same tag";
                        statsList.Add(gs);
                        WriteFetchLine(gs);
                        continue;
                    }
                    if (tagConfig.EnableCollection)
                    {
                        activeCollections.Add(cName);
                        if (!string.IsNullOrWhiteSpace(tagConfig.CollectionDescription))
                            collectionDescriptions[cName] = tagConfig.CollectionDescription;
                        if (!string.IsNullOrWhiteSpace(tagConfig.CollectionPosterPath) && File.Exists(tagConfig.CollectionPosterPath))
                            collectionPosters[cName] = tagConfig.CollectionPosterPath;
                        if (!string.IsNullOrWhiteSpace(tagConfig.CollectionThumbPath) && File.Exists(tagConfig.CollectionThumbPath))
                            collectionThumbs[cName] = tagConfig.CollectionThumbPath;
                        if (!string.IsNullOrWhiteSpace(tagConfig.CollectionBackdropPath) && File.Exists(tagConfig.CollectionBackdropPath))
                            collectionBackdrops[cName] = tagConfig.CollectionBackdropPath;
                    }

                    var groupTimer = System.Diagnostics.Stopwatch.StartNew();
                    try
                    {
                        int effectiveLimit = tagConfig.Limit <= 0 ? 10000 : tagConfig.Limit;
                        _log.Section($"[{gs.GroupIndex}/{gs.GroupTotal}] {displayName}");
                        _log.Debug("  " + DescribeSourceDetail(tagConfig, effectiveLimit));
                        var blacklist = new HashSet<string>(tagConfig.Blacklist ?? new List<string>(), StringComparer.OrdinalIgnoreCase);
                        var matchedLocalItems = new List<BaseItem>();
                        List<BaseItem> tagOutputItems = matchedLocalItems;
                        List<BaseItem> collectionOutputItems = matchedLocalItems;
                        int matchCount = 0;
                        Dictionary<long, List<string>>? seriesEpisodeNamesCache = null;
                        if (GetAllCriteria(tagConfig).Any(c => c.TrimStart('!').StartsWith("EpisodeTitle:", StringComparison.OrdinalIgnoreCase)))
                        {
                            seriesEpisodeNamesCache = new Dictionary<long, List<string>>();
                            var allEpsForTitle = _libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "Episode" }, Recursive = true, IsVirtualItem = false });
                            foreach (var ep in allEpsForTitle)
                            {
                                if (string.IsNullOrEmpty(ep.Name)) continue;
                                BaseItem? ser = null;
                                var par = ep.Parent;
                                if (par != null)
                                {
                                    if (par.GetType().Name.Contains("Series")) ser = par;
                                    else if (par.GetType().Name.Contains("Season") && par.Parent?.GetType().Name.Contains("Series") == true) ser = par.Parent;
                                }
                                if (ser == null) continue;
                                if (!seriesEpisodeNamesCache.TryGetValue(ser.InternalId, out var nl)) { nl = new List<string>(); seriesEpisodeNamesCache[ser.InternalId] = nl; }
                                nl.Add(ep.Name);
                            }
                        }

                        if (string.IsNullOrEmpty(tagConfig.SourceType) || tagConfig.SourceType == "External")
                        {
                            var fetchTimer = System.Diagnostics.Stopwatch.StartNew();
                            var items = await fetcher.FetchItems(tagConfig.Url, effectiveLimit, config.TraktClientId, config.MdblistApiKey, config.TmdbApiKey, cancellationToken);
                            fetchTimer.Stop();
                            gs.ListCount = items.Count;
                            int _extBlacklisted = 0, _extNoImdb = 0;

                            if (items.Count > 0)
                            {
                                if (items.Count > effectiveLimit) items = items.Take(effectiveLimit).ToList();

                                foreach (var extItem in items)
                                {
                                    if (string.IsNullOrEmpty(extItem.Imdb)) { _extNoImdb++; continue; }

                                    if (blacklist.Contains(extItem.Imdb))
                                    {
                                        _extBlacklisted++;
                                        _log.Debug($"    Blacklisted: {extItem.Name} ({extItem.Imdb})");
                                        continue;
                                    }

                                    if (tagConfig.EnableTag && !tagConfig.OnlyCollection)
                                        TagCacheManager.Instance.AddToCache($"imdb_{extItem.Imdb}", tagName);

                                    if (imdbLookup.TryGetValue(extItem.Imdb, out var localItems))
                                    {
                                        foreach (var localItem in localItems)
                                        {
                                            if (!matchedLocalItems.Contains(localItem)) matchedLocalItems.Add(localItem);
                                        }
                                    }
                                    else
                                    {
                                        gs.MissingItems.Add($"{extItem.Name}  {extItem.Imdb}");
                                    }
                                }
                            }
                            _log.Debug($"  Fetched {gs.ListCount} items in {fetchTimer.ElapsedMilliseconds} ms  ·  {matchedLocalItems.Count} matched by IMDb id  ·  {gs.MissingItems.Count} not in library  ·  {_extBlacklisted} blacklisted  ·  {_extNoImdb} without IMDb id");
                        }
                        else if (tagConfig.SourceType == "LocalCollection" || tagConfig.SourceType == "LocalPlaylist")
                        {
                            if (!string.IsNullOrEmpty(tagConfig.LocalSourceId))
                            {
                                string[] folderTypes = tagConfig.SourceType == "LocalPlaylist"
                                    ? new[] { "Playlist" }
                                    : new[] { "BoxSet" };
                                var allFolders = _libraryManager.GetItemList(new InternalItemsQuery
                                {
                                    IncludeItemTypes = folderTypes,
                                    Recursive = true
                                });
                                var localSourceFolder = allFolders.FirstOrDefault(i =>
                                    string.Equals(i.Name, tagConfig.LocalSourceId, StringComparison.OrdinalIgnoreCase)
                                );

                                if (localSourceFolder != null)
                                {
                                    var children = new List<BaseItem>();
                                    _log.Debug($"  Found source '{localSourceFolder.Name}'  ({localSourceFolder.GetType().Name})");

                                    if (tagConfig.SourceType == "LocalCollection")
                                    {
                                        children = _libraryManager.GetItemList(new InternalItemsQuery
                                        {
                                            CollectionIds = new[] { localSourceFolder.InternalId },
                                            IsVirtualItem = false
                                        }).ToList();
                                    }
                                    else
                                    {
                                        children = _libraryManager.GetItemList(new InternalItemsQuery
                                        {
                                            ListIds = new[] { localSourceFolder.InternalId }
                                        }).ToList();
                                    }

                                    gs.ListCount = children.Count;
                                    if (children.Count == 0)
                                        gs.Warnings.Add($"'{tagConfig.LocalSourceId}' is empty (or only contains virtual items)");
                                    else
                                        _log.Debug($"  Items in source: {children.Count}");

                                    foreach (var child in children)
                                    {
                                        if (child == null) continue;

                                        BaseItem itemToTag = child;

                                        if (child.GetType().Name.Contains("PlaylistItem"))
                                        {
                                            try { 
                                                var inner = ((dynamic)child).Item; 
                                                if (inner != null) itemToTag = inner;
                                            } catch { }
                                        }

                                        if (itemToTag.GetType().Name.Contains("Episode"))
                                        {
                                            try {
                                                var series = ((dynamic)itemToTag).Series;
                                                if (series != null) itemToTag = series;
                                            } catch { }
                                        }

                                        if (!IsTaggableTopLevelItem(itemToTag))
                                            continue;

                                        var imdb = itemToTag.GetProviderId("Imdb");
                                        if (!string.IsNullOrEmpty(imdb) && blacklist.Contains(imdb))
                                        {
                                            _log.Debug($"    Blacklisted: {itemToTag.Name} ({imdb})");
                                            continue;
                                        }

                                        if (!matchedLocalItems.Contains(itemToTag))
                                        {
                                            matchedLocalItems.Add(itemToTag);
                                        }
                                    }
                                    _log.Debug($"  {matchedLocalItems.Count} usable movies/series in source");
                                }
                                else
                                {
                                    gs.Warnings.Add($"{DescribeSource(tagConfig)} '{tagConfig.LocalSourceId}' was not found in the library");
                                }

                                if (effectiveLimit < 10000 && matchedLocalItems.Count > effectiveLimit)
                                    matchedLocalItems = matchedLocalItems.Take(effectiveLimit).ToList();
                            }
                        }
                        // Apply MediaInfo post-filter for non-MediaInfo source types
                        if (tagConfig.SourceType != "MediaInfo" && matchedLocalItems.Count > 0
                            && (tagConfig.MediaInfoFilters?.Count > 0 || tagConfig.MediaInfoConditions?.Count > 0))
                        {
                            var beforeCount = matchedLocalItems.Count;
                            matchedLocalItems = matchedLocalItems.Where(item =>
                            {
                                CachedMediaInfo? ci = mediaInfoCache.TryGetValue(item.InternalId, out var ciVal) ? ciVal : (CachedMediaInfo?)null;
                                return ItemMatchesMediaInfo(item, tagConfig, debug, seriesEpisodeCache, personCache, userDataCache, ci, preloadedUsers, seriesLastPlayedCache, collectionMembershipCache, seriesEpisodeNamesCache);
                            }).ToList();
                            _log.Debug($"  Filter conditions: {beforeCount} → {matchedLocalItems.Count} items");
                        }

                        if (tagConfig.SourceType == "MediaInfo")
                        {
                            IList<BaseItem> itemsToScan;
                            if (TagConfigTargetsEpisodes(tagConfig))
                            {
                                var episodeQuery = new InternalItemsQuery
                                {
                                    IncludeItemTypes = new[] { "Episode" },
                                    Recursive = true,
                                    IsVirtualItem = false
                                };
                                var titleContains = ExtractTitleContains(tagConfig);
                                if (!string.IsNullOrEmpty(titleContains))
                                    episodeQuery.NameContains = titleContains;
                                itemsToScan = _libraryManager.GetItemList(episodeQuery).ToList();
                            }
                            else
                            {
                                itemsToScan = matchableItems;
                            }

                            foreach (var item in itemsToScan)
                            {
                                if (item.LocationType != LocationType.FileSystem) continue;

                                var imdb = item.GetProviderId("Imdb");
                                if (!string.IsNullOrEmpty(imdb) && blacklist.Contains(imdb)) continue;

                                CachedMediaInfo? ci = mediaInfoCache.TryGetValue(item.InternalId, out var ciVal) ? ciVal : (CachedMediaInfo?)null;
                                if (ItemMatchesMediaInfo(item, tagConfig, debug, seriesEpisodeCache, personCache, userDataCache, ci, preloadedUsers, seriesLastPlayedCache, collectionMembershipCache, seriesEpisodeNamesCache))
                                {
                                    matchedLocalItems.Add(item);
                                    if (effectiveLimit < 10000 && matchedLocalItems.Count >= effectiveLimit) break;
                                }
                            }
                            gs.ListCount = itemsToScan.Count;
                            if (TagConfigTargetsEpisodes(tagConfig))
                            {
                                foreach (var ep in itemsToScan)
                                    allScannedEpisodeItems.TryAdd(ep.Id, ep);
                            }
                            // Redirect matched items to the selected output level (tag and collection independently)
                            tagOutputItems = matchedLocalItems;
                            collectionOutputItems = matchedLocalItems;
                            {
                                bool scannedEpisodes = TagConfigTargetsEpisodes(tagConfig);
                                var (tEp, tSea, tSer) = EffectiveTagTargets(tagConfig);
                                var (cEp, cSea, cSer) = EffectiveCollectionTargets(tagConfig);

                                List<BaseItem> BuildOutputList(bool ep, bool sea, bool ser, bool anyNew)
                                {
                                    if (!anyNew) return scannedEpisodes ? ResolveParentSeries(matchedLocalItems) : matchedLocalItems.ToList();
                                    var list = new List<BaseItem>();
                                    var seriesOnly = matchedLocalItems.Where(i => i.GetType().Name.Contains("Series")).ToList();
                                    if (scannedEpisodes)
                                    {
                                        // Collapse up: episodes → season/series
                                        if (ep) list.AddRange(matchedLocalItems);
                                        if (sea) { var s = ResolveParentSeasons(matchedLocalItems); _log.Debug($"  Output level: {matchedLocalItems.Count} episodes → {s.Count} seasons"); list.AddRange(s); foreach (var x in s) allScannedSeasonItems.TryAdd(x.Id, x); }
                                        if (ser) { var s = ResolveParentSeries(matchedLocalItems); _log.Debug($"  Output level: {matchedLocalItems.Count} episodes → {s.Count} series"); list.AddRange(s); }
                                    }
                                    else
                                    {
                                        // Expand down: series → seasons/episodes; movies stay as-is for any target
                                        var movies = matchedLocalItems.Where(i => !i.GetType().Name.Contains("Series")).ToList();
                                        if (ser) list.AddRange(matchedLocalItems);
                                        if (sea) { var s = ResolveChildSeasons(seriesOnly); _log.Debug($"  Output level: {seriesOnly.Count} series → {s.Count} seasons"); list.AddRange(s); foreach (var x in s) allScannedSeasonItems.TryAdd(x.Id, x); list.AddRange(movies); }
                                        if (ep)  { var e = ResolveChildEpisodes(seriesOnly); _log.Debug($"  Output level: {seriesOnly.Count} series → {e.Count} episodes"); list.AddRange(e); foreach (var x in e) allScannedEpisodeItems.TryAdd(x.Id, x); list.AddRange(movies); }
                                    }
                                    return list;
                                }

                                tagOutputItems        = BuildOutputList(tEp, tSea, tSer, tEp || tSea || tSer);
                                collectionOutputItems = BuildOutputList(cEp, cSea, cSer, cEp || cSea || cSer);
                            }
                            if (debug)
                            {
                                _log.Debug($"  Scanned {itemsToScan.Count:N0} items in {groupTimer.ElapsedMilliseconds} ms  ·  {matchedLocalItems.Count} matched  (tag output {tagOutputItems.Count}, collection output {collectionOutputItems.Count})");
                                WriteMatchedItemsDebug(matchedLocalItems);
                            }
                        }
                        else if (tagConfig.SourceType == "AI")
                        {
                            var recentlyWatchedContext = BuildRecentlyWatchedContext(tagConfig);
                            var fetchTimer = System.Diagnostics.Stopwatch.StartNew();
                            var aiItems = await fetcher.FetchAiList(
                                tagConfig.AiProvider,
                                tagConfig.AiPrompt,
                                config.OpenAiApiKey,
                                config.OpenAiModel,
                                config.GeminiApiKey,
                                config.GeminiModel,
                                config.ClaudeApiKey,
                                config.ClaudeModel,
                                config.OllamaBaseUrl,
                                config.OllamaModel,
                                config.AiSystemPrompt,
                                recentlyWatchedContext,
                                effectiveLimit,
                                cancellationToken);
                            fetchTimer.Stop();

                            gs.ListCount = aiItems.Count;
                            int _aiTitleMatched = 0, _aiBlacklisted = 0;

                            foreach (var aiItem in aiItems)
                            {
                                if (string.IsNullOrWhiteSpace(aiItem.title)) continue;
                                string _aiLabel = aiItem.year.HasValue ? $"{aiItem.title} ({aiItem.year})" : aiItem.title;

                                if (!string.IsNullOrEmpty(aiItem.imdb_id))
                                {
                                    var imdbId = aiItem.imdb_id.Trim();
                                    if (blacklist.Contains(imdbId))
                                    {
                                        _aiBlacklisted++;
                                        _log.Debug($"    Blacklisted: {_aiLabel} ({imdbId})");
                                        continue;
                                    }

                                    if (tagConfig.EnableTag && !tagConfig.OnlyCollection)
                                        TagCacheManager.Instance.AddToCache($"imdb_{imdbId}", tagName);

                                    if (imdbLookup.TryGetValue(imdbId, out var localItems))
                                    {
                                        foreach (var localItem in localItems)
                                        {
                                            if (!matchedLocalItems.Contains(localItem))
                                                matchedLocalItems.Add(localItem);
                                        }
                                    }
                                    else
                                    {
                                        // IMDB ID not found in library — fall back to title+year match
                                        var titleMatches = FindByTitleAndYear(matchableItems, aiItem.title, aiItem.year);
                                        if (titleMatches.Count > 0) { _aiTitleMatched++; _log.Debug($"    {imdbId} not in library — matched '{_aiLabel}' by title"); }
                                        else gs.MissingItems.Add($"{_aiLabel}  {imdbId}");
                                        foreach (var localItem in titleMatches)
                                        {
                                            var imdb = localItem.GetProviderId("Imdb");
                                            if (!string.IsNullOrEmpty(imdb) && blacklist.Contains(imdb)) continue;
                                            if (!matchedLocalItems.Contains(localItem))
                                                matchedLocalItems.Add(localItem);
                                        }
                                    }
                                }
                                else
                                {
                                    // Fallback: title+year match when AI didn't return an IMDB ID
                                    var titleMatches = FindByTitleAndYear(matchableItems, aiItem.title, aiItem.year);
                                    if (titleMatches.Count > 0) _aiTitleMatched++;
                                    else gs.MissingItems.Add($"{_aiLabel}  (no IMDb id from AI)");
                                    foreach (var localItem in titleMatches)
                                    {
                                        var imdb = localItem.GetProviderId("Imdb");
                                        if (!string.IsNullOrEmpty(imdb) && blacklist.Contains(imdb))
                                        {
                                            _aiBlacklisted++;
                                            _log.Debug($"    Blacklisted: {localItem.Name} ({imdb})");
                                            continue;
                                        }
                                        if (!matchedLocalItems.Contains(localItem))
                                            matchedLocalItems.Add(localItem);
                                    }
                                }
                            }

                            _log.Debug($"  AI ({tagConfig.AiProvider}) returned {gs.ListCount} items in {fetchTimer.ElapsedMilliseconds} ms  ·  {matchedLocalItems.Count} matched ({_aiTitleMatched} by title only)  ·  {gs.MissingItems.Count} not in library  ·  {_aiBlacklisted} blacklisted");
                            if (tagConfig.AiRefreshIntervalDays > 0)
                            {
                                tagConfig.AiLastRunDate = DateTime.UtcNow;
                                aiConfigChanged = true;
                            }
                        }
                        // For non-MediaInfo sources, apply output level selection (expand down from Series/Movie)
                        if (tagConfig.SourceType != "MediaInfo")
                        {
                            var (tEp, tSea, tSer) = EffectiveTagTargets(tagConfig);
                            var (cEp, cSea, cSer) = EffectiveCollectionTargets(tagConfig);

                            List<BaseItem> BuildNonMiOutputList(bool ep, bool sea, bool ser, bool any)
                            {
                                if (!any) return matchedLocalItems.ToList();
                                var list = new List<BaseItem>();
                                var seriesOnly = matchedLocalItems.Where(i => i.GetType().Name.Contains("Series")).ToList();
                                var movies = matchedLocalItems.Where(i => !i.GetType().Name.Contains("Series")).ToList();
                                if (ser) list.AddRange(matchedLocalItems);
                                if (sea) { var s = ResolveChildSeasons(seriesOnly); list.AddRange(s); foreach (var x in s) allScannedSeasonItems.TryAdd(x.Id, x); list.AddRange(movies); }
                                if (ep)  { var e = ResolveChildEpisodes(seriesOnly); list.AddRange(e); foreach (var x in e) allScannedEpisodeItems.TryAdd(x.Id, x); list.AddRange(movies); }
                                return list;
                            }

                            tagOutputItems        = BuildNonMiOutputList(tEp, tSea, tSer, tEp || tSea || tSer);
                            collectionOutputItems = BuildNonMiOutputList(cEp, cSea, cSer, cEp || cSea || cSer);
                        }

                        var allOutputIds = new HashSet<Guid>(tagOutputItems.Select(i => i.Id));
                        foreach (var id in collectionOutputItems.Select(i => i.Id)) allOutputIds.Add(id);
                        gs.MatchCount = allOutputIds.Count;
                        matchCount += allOutputIds.Count;

                        // Collect rank order so top-list .strm files can be numbered in list order.
                        // Accumulated across all flat entries of the tag; written once after the loop.
                        if (!rankIdsByTag.TryGetValue(tagName, out var rankIds))
                        {
                            rankIds = new List<string>();
                            rankIdsByTag[tagName] = rankIds;
                        }
                        var rankSeen = new HashSet<string>(rankIds, StringComparer.OrdinalIgnoreCase);
                        foreach (var rankId in matchedLocalItems.Select(i => i.GetProviderId("Imdb") ?? "").Where(id => !string.IsNullOrEmpty(id)))
                            if (rankSeen.Add(rankId)) rankIds.Add(rankId);

                        // If this is a priority-override entry but produced zero results,
                        // remove it from the override sets so other entries for the same tag are not suppressed.
                        if (tagConfig.OverrideWhenActive && allOutputIds.Count == 0)
                        {
                            activeTagOverrides.Remove(tagName);
                            if (tagConfig.EnableCollection) activeCollectionOverrides.Remove(cName);
                        }

                        // For External and AI sources: if the remote returned zero items and the user has
                        // opted to preserve tags on empty results, treat it as a failed fetch.
                        bool isRemoteSource = string.IsNullOrEmpty(tagConfig.SourceType) || tagConfig.SourceType == "External" || tagConfig.SourceType == "AI";
                        if (isRemoteSource && gs.ListCount == 0 && config.PreserveTagsOnEmptyResult)
                        {
                            gs.Warnings.Add(tagConfig.SourceType == "AI"
                                ? "The AI returned 0 items — existing tags, collection and playlist were kept. Check the prompt and the API key in Settings."
                                : "The list returned 0 items — existing tags, collection and playlist were kept. Check the list URL and the API key in Settings.");
                            failedFetches.Add(tagName);
                            if (tagConfig.EnableCollection) failedFetches.Add(cName);
                            playlistGroupsToSkip.Add(GroupKey(tagConfig));
                            gs.ElapsedMs = groupTimer.ElapsedMilliseconds;
                            statsList.Add(gs);
                            WriteFetchLine(gs);
                            currentProgress += step;
                            progress.Report(currentProgress);
                            continue;
                        }
                        if (isRemoteSource && gs.ListCount == 0)
                            gs.Warnings.Add(tagConfig.SourceType == "AI"
                                ? "The AI returned 0 items — its tags, collection and playlist are being cleared (\"Preserve tags and collections on empty result\" is off in Settings)"
                                : "The list returned 0 items — its tags, collection and playlist are being cleared (\"Preserve tags and collections on empty result\" is off in Settings). Check the list URL and the API key.");

                        if (tagConfig.EnableTag && !tagConfig.OnlyCollection && !IsBoxSetHomeSectionEntry(tagConfig))
                        {
                            foreach (var localItem in tagOutputItems)
                            {
                                if (!desiredTagsMap.ContainsKey(localItem.Id))
                                    desiredTagsMap[localItem.Id] = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                                desiredTagsMap[localItem.Id].Add(tagName);

                                // The real-time tag cache is keyed by IMDb id, so only list sources belong in
                                // it (External is cached from the list itself above). MediaInfo and local
                                // collection/playlist results are per item: caching them would re-tag every
                                // other item with the same IMDb id (other versions, top-list copies) on its
                                // next update, straight after this sync removed the tag.
                                var imdb = localItem.GetProviderId("Imdb");
                                bool listSource = string.IsNullOrEmpty(tagConfig.SourceType) || tagConfig.SourceType == "AI";
                                if (!string.IsNullOrEmpty(imdb) && listSource)
                                    TagCacheManager.Instance.AddToCache($"imdb_{imdb}", tagName);
                            }
                        }

                        if (tagConfig.EnableCollection)
                        {
                            if (!desiredCollectionsMap.ContainsKey(cName))
                                desiredCollectionsMap[cName] = new HashSet<long>();
                            foreach (var localItem in collectionOutputItems)
                                desiredCollectionsMap[cName].Add(localItem.InternalId);
                        }

                        // Collect this entry's items for the group's playlist sync (done once per group after
                        // the loop so all sources of a multi-source group end up in the same playlist).
                        // Placed here so it inherits the loop's skip/preserve/override guards above.
                        if (tagConfig.EnablePlaylist)
                        {
                            var groupKey = GroupKey(tagConfig);
                            if (!groupPlaylistItems.TryGetValue(groupKey, out var plGroup))
                            {
                                plGroup = (tagConfig, new List<BaseItem>(), new HashSet<Guid>());
                                groupPlaylistItems[groupKey] = plGroup;
                            }
                            foreach (var localItem in collectionOutputItems)
                                if (plGroup.Seen.Add(localItem.Id)) plGroup.Items.Add(localItem);
                        }

                        gs.BoxSetFound = ApplyTagToSourceBoxSet(tagConfig, tagName, dryRun, cancellationToken);
                        gs.BoxSetTaggedCount = gs.BoxSetFound ? 1 : 0;
                        if (gs.BoxSetHse && !gs.BoxSetFound)
                            gs.Warnings.Add($"Collection '{tagConfig.LocalSourceId}' was not found in the library");
                        if (gs.MissingItems.Count > 0 && debug)
                        {
                            _log.Debug($"  Not in library ({gs.MissingItems.Count}):");
                            foreach (var _missing in gs.MissingItems) _log.Debug("    " + _missing);
                        }
                        _log.Debug($"  Group done in {groupTimer.ElapsedMilliseconds} ms");
                    }
                    catch (Exception ex)
                    {
                        gs.ErrorMessage = ex.Message;
                        WriteExceptionDebug(ex);
                        failedFetches.Add(tagName);
                        if (tagConfig.EnableCollection) failedFetches.Add(cName);
                        playlistGroupsToSkip.Add(GroupKey(tagConfig));
                    }

                    gs.ElapsedMs = groupTimer.ElapsedMilliseconds;
                    statsList.Add(gs);
                    WriteFetchLine(gs);
                    currentProgress += step;
                    progress.Report(currentProgress);
                }
                _log.Debug($"Fetch phase done in {RunLog.Elapsed(phaseTimer.Elapsed)}");

                // Playlist sync — once per group, with the union of all its sources.
                // Skipped for groups where any source failed, so a bad fetch never empties the playlist.
                if (groupPlaylistItems.Count > 0)
                {
                    _log.Blank();
                    _log.Info("» Playlists");
                    phaseTimer.Restart();
                    if (dryRun) _log.Skip("Dry run — playlists are not changed");
                }
                foreach (var kvp in groupPlaylistItems)
                {
                    statsByGroupKey.TryGetValue(kvp.Key, out var plStats);
                    if (playlistGroupsToSkip.Contains(kvp.Key))
                    {
                        plStats?.Warnings.Add("Playlist left unchanged because the source failed or returned nothing");
                        _log.Skip($"Playlist for '{plStats?.DisplayName ?? kvp.Value.Owner.Name}' left unchanged — source failed or returned nothing");
                        continue;
                    }
                    await SyncPlaylistsForEntryAsync(kvp.Value.Owner, kvp.Value.Items, dryRun, plStats);
                }
                if (groupPlaylistItems.Count > 0 && !dryRun)
                {
                    int _plCreated = statsList.Sum(g => g.PlaylistUsersCreated), _plUpdated = statsList.Sum(g => g.PlaylistUsersUpdated), _plFailed = statsList.Sum(g => g.PlaylistUsersFailed);
                    _log.Info($"    {_plCreated} created, {_plUpdated} updated{(_plFailed > 0 ? $", {_plFailed} failed" : "")}  ·  {RunLog.Elapsed(phaseTimer.Elapsed)}");
                }

                if (!dryRun)
                {
                    foreach (var kvp in rankIdsByTag)
                        WriteRankFile(kvp.Key, kvp.Value);
                    TagCacheManager.Instance.Save();
                    SaveFileHistory("homescreencompanion_history.txt", managedTags.ToList());
                }

                // Collect episodes that currently carry managed tags so they can be cleaned up
                // even when the corresponding group is inactive or removed
                foreach (var managedTag in managedTags)
                {
                    var taggedEpisodes = _libraryManager.GetItemList(new InternalItemsQuery
                    {
                        IncludeItemTypes = new[] { "Episode" },
                        Tags = new[] { managedTag },
                        Recursive = true,
                        IsVirtualItem = false
                    });
                    foreach (var ep in taggedEpisodes)
                        allScannedEpisodeItems.TryAdd(ep.Id, ep);

                    // Collect seasons that currently carry managed tags for cleanup
                    var taggedSeasons = _libraryManager.GetItemList(new InternalItemsQuery
                    {
                        IncludeItemTypes = new[] { "Season" },
                        Tags = new[] { managedTag },
                        Recursive = true,
                        IsVirtualItem = false
                    });
                    foreach (var s in taggedSeasons)
                        allScannedSeasonItems.TryAdd(s.Id, s);
                }

                _log.Blank();
                _log.Info("» Applying tags");
                phaseTimer.Restart();
                int tagsAdded = 0, tagsRemoved = 0, itemsChanged = 0, updateCount = 0;
                var _dbgTagAdded = debug ? new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase) : null;
                var _dbgTagRemoved = debug ? new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase) : null;
                foreach (var item in allItems)
                {
                    var existingTags = new HashSet<string>(item.Tags, StringComparer.OrdinalIgnoreCase);
                    var targetTags = desiredTagsMap.ContainsKey(item.Id) ? desiredTagsMap[item.Id] : new HashSet<string>();

                    var toRemove = existingTags.Where(t => managedTags.Contains(t) && !targetTags.Contains(t) && !failedFetches.Contains(t)).ToList();
                    var toAdd = targetTags.Where(t => !existingTags.Contains(t)).ToList();

                    if (toRemove.Count == 0 && toAdd.Count == 0) continue;

                    itemsChanged++;
                    if (debug)
                    {
                        var _tagYr = item.ProductionYear.HasValue ? $" ({item.ProductionYear})" : "";
                        var _tagTp = item.GetType().Name.Contains("Series") ? "Series" : "Movie";
                        string _itemLabel = $"{item.Name}{_tagYr}  [{_tagTp}]";
                        foreach (var t in toAdd) { if (!_dbgTagAdded!.ContainsKey(t)) _dbgTagAdded[t] = new List<string>(); _dbgTagAdded[t].Add(_itemLabel); }
                        foreach (var t in toRemove) { if (!_dbgTagRemoved!.ContainsKey(t)) _dbgTagRemoved[t] = new List<string>(); _dbgTagRemoved[t].Add(_itemLabel); }
                    }
                    if (!dryRun)
                    {
                        foreach (var t in toRemove) { item.RemoveTag(t); tagsRemoved++; tagRemovedByTag[t] = tagRemovedByTag.GetValueOrDefault(t) + 1; }
                        foreach (var t in toAdd) { item.AddTag(t); tagsAdded++; tagAddedByTag[t] = tagAddedByTag.GetValueOrDefault(t) + 1; }
                        try { _libraryManager.UpdateItem(item, item.Parent, ItemUpdateType.MetadataEdit, null); }
                        catch (Exception ex) { _log.Warn($"Could not save tags for '{item.Name}': {ex.Message}"); }
                        if (++updateCount % 25 == 0)
                            await Task.Yield();
                    }
                    else
                    {
                        tagsAdded += toAdd.Count; tagsRemoved += toRemove.Count;
                        foreach (var t in toAdd) tagAddedByTag[t] = tagAddedByTag.GetValueOrDefault(t) + 1;
                        foreach (var t in toRemove) tagRemovedByTag[t] = tagRemovedByTag.GetValueOrDefault(t) + 1;
                    }
                }
                foreach (var item in allScannedEpisodeItems.Values)
                {
                    var existingTags = new HashSet<string>(item.Tags, StringComparer.OrdinalIgnoreCase);
                    var targetTags = desiredTagsMap.ContainsKey(item.Id) ? desiredTagsMap[item.Id] : new HashSet<string>();

                    var toRemove = existingTags.Where(t => managedTags.Contains(t) && !targetTags.Contains(t) && !failedFetches.Contains(t)).ToList();
                    var toAdd = targetTags.Where(t => !existingTags.Contains(t)).ToList();

                    if (toRemove.Count == 0 && toAdd.Count == 0) continue;

                    itemsChanged++;
                    if (debug)
                    {
                        var _tagYr = item.ProductionYear.HasValue ? $" ({item.ProductionYear})" : "";
                        string _itemLabel = $"{item.Name}{_tagYr}  [Episode]";
                        foreach (var t in toAdd) { if (!_dbgTagAdded!.ContainsKey(t)) _dbgTagAdded[t] = new List<string>(); _dbgTagAdded[t].Add(_itemLabel); }
                        foreach (var t in toRemove) { if (!_dbgTagRemoved!.ContainsKey(t)) _dbgTagRemoved[t] = new List<string>(); _dbgTagRemoved[t].Add(_itemLabel); }
                    }
                    if (!dryRun)
                    {
                        foreach (var t in toRemove) { item.RemoveTag(t); tagsRemoved++; tagRemovedByTag[t] = tagRemovedByTag.GetValueOrDefault(t) + 1; }
                        foreach (var t in toAdd) { item.AddTag(t); tagsAdded++; tagAddedByTag[t] = tagAddedByTag.GetValueOrDefault(t) + 1; }
                        try { _libraryManager.UpdateItem(item, item.Parent, ItemUpdateType.MetadataEdit, null); }
                        catch (Exception ex) { _log.Warn($"Could not save tags for '{item.Name}': {ex.Message}"); }
                        if (++updateCount % 25 == 0)
                            await Task.Yield();
                    }
                    else
                    {
                        tagsAdded += toAdd.Count; tagsRemoved += toRemove.Count;
                        foreach (var t in toAdd) tagAddedByTag[t] = tagAddedByTag.GetValueOrDefault(t) + 1;
                        foreach (var t in toRemove) tagRemovedByTag[t] = tagRemovedByTag.GetValueOrDefault(t) + 1;
                    }
                }
                foreach (var item in allScannedSeasonItems.Values)
                {
                    var existingTags = new HashSet<string>(item.Tags, StringComparer.OrdinalIgnoreCase);
                    var targetTags = desiredTagsMap.ContainsKey(item.Id) ? desiredTagsMap[item.Id] : new HashSet<string>();

                    var toRemove = existingTags.Where(t => managedTags.Contains(t) && !targetTags.Contains(t) && !failedFetches.Contains(t)).ToList();
                    var toAdd = targetTags.Where(t => !existingTags.Contains(t)).ToList();

                    if (toRemove.Count == 0 && toAdd.Count == 0) continue;

                    itemsChanged++;
                    if (debug)
                    {
                        var _tagYr = item.ProductionYear.HasValue ? $" ({item.ProductionYear})" : "";
                        string _itemLabel = $"{item.Name}{_tagYr}  [Season]";
                        foreach (var t in toAdd) { if (!_dbgTagAdded!.ContainsKey(t)) _dbgTagAdded[t] = new List<string>(); _dbgTagAdded[t].Add(_itemLabel); }
                        foreach (var t in toRemove) { if (!_dbgTagRemoved!.ContainsKey(t)) _dbgTagRemoved[t] = new List<string>(); _dbgTagRemoved[t].Add(_itemLabel); }
                    }
                    if (!dryRun)
                    {
                        foreach (var t in toRemove) { item.RemoveTag(t); tagsRemoved++; tagRemovedByTag[t] = tagRemovedByTag.GetValueOrDefault(t) + 1; }
                        foreach (var t in toAdd) { item.AddTag(t); tagsAdded++; tagAddedByTag[t] = tagAddedByTag.GetValueOrDefault(t) + 1; }
                        try { _libraryManager.UpdateItem(item, item.Parent, ItemUpdateType.MetadataEdit, null); }
                        catch (Exception ex) { _log.Warn($"Could not save tags for season '{item.Name}': {ex.Message}"); }
                        if (++updateCount % 25 == 0)
                            await Task.Yield();
                    }
                    else
                    {
                        tagsAdded += toAdd.Count; tagsRemoved += toRemove.Count;
                        foreach (var t in toAdd) tagAddedByTag[t] = tagAddedByTag.GetValueOrDefault(t) + 1;
                        foreach (var t in toRemove) tagRemovedByTag[t] = tagRemovedByTag.GetValueOrDefault(t) + 1;
                    }
                }
                foreach (var gs in statsList)
                {
                    if (gs.TagName != null)
                    {
                        gs.TagsAdded = tagAddedByTag.GetValueOrDefault(gs.TagName);
                        gs.TagsRemoved = tagRemovedByTag.GetValueOrDefault(gs.TagName);
                    }
                }

                WriteTagDiffDebug(_dbgTagAdded, _dbgTagRemoved);
                _log.Info(tagsAdded == 0 && tagsRemoved == 0
                    ? $"    No tag changes needed  ·  {RunLog.Elapsed(phaseTimer.Elapsed)}"
                    : dryRun
                        ? $"    Would add {tagsAdded} and remove {tagsRemoved} tags on {RunLog.Plural(itemsChanged, "item")}"
                        : $"    +{tagsAdded} added, -{tagsRemoved} removed on {RunLog.Plural(itemsChanged, "item")}  ·  {RunLog.Elapsed(phaseTimer.Elapsed)}");

                _log.Blank();
                _log.Info("» Collections");
                phaseTimer.Restart();
                int collCreated = 0, collUpdated = 0, collWouldCreate = 0, collWouldUpdate = 0;
                _log.Section("Collections");
                foreach (var kvp in desiredCollectionsMap)
                {
                    string cName = kvp.Key;
                    var desiredIds = kvp.Value;
                    if (desiredIds.Count == 0) continue;

                    try
                    {
                        var existingColl = _libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "BoxSet" }, Name = cName, Recursive = true }).FirstOrDefault();

                        if (existingColl == null)
                        {
                            if (dryRun) { collWouldCreate++; _log.Debug($"  {cName}  →  would be created ({desiredIds.Count} items)"); continue; }
                            var createdRef = await _collectionManager.CreateCollection(new CollectionCreationOptions { Name = cName, IsLocked = false, ItemIdList = desiredIds.ToArray() });
                            if (createdRef != null)
                            {
                                collCreated++;
                                collCreatedSet.Add(cName);
                                collItemsAdded[cName] = desiredIds.Count;
                                _log.Debug($"  {cName}  →  created ({desiredIds.Count} items)");
                                if (collectionDescriptions.ContainsKey(cName) || collectionPosters.ContainsKey(cName)
                                    || collectionThumbs.ContainsKey(cName) || collectionBackdrops.ContainsKey(cName))
                                    ApplyCollectionMeta(createdRef, cName, collectionDescriptions, collectionPosters, collectionThumbs, collectionBackdrops, debug);
                            }
                        }
                        else
                        {
                            // Top-list copies are managed by TopListCollectionMirror — never remove them here.
                            var currentMembers = _libraryManager.GetItemList(new InternalItemsQuery { CollectionIds = new[] { existingColl.InternalId }, Recursive = true, IsVirtualItem = false }).Where(i => !TopListCollectionMirror.IsTopListItem(i)).Select(i => i.InternalId).ToHashSet();
                            var toAdd = desiredIds.Where(id => !currentMembers.Contains(id)).ToList();
                            var toRemove = currentMembers.Where(id => !desiredIds.Contains(id)).ToList();
                            if (toAdd.Count > 0 && !dryRun)
                                await _collectionManager.AddToCollection(existingColl.InternalId, toAdd.ToArray());
                            if (toRemove.Count > 0 && !dryRun && existingColl is BoxSet boxSet)
                                _collectionManager.RemoveFromCollection(boxSet, toRemove.ToArray());
                            if ((toAdd.Count > 0 || toRemove.Count > 0) && !dryRun)
                            {
                                collUpdated++;
                                collItemsAdded[cName] = toAdd.Count;
                                collItemsRemoved[cName] = toRemove.Count;
                                if (debug)
                                {
                                    _log.Debug($"  {cName}  →  updated (+{toAdd.Count}, -{toRemove.Count})");
                                    var _collMap = allItems.ToDictionary(i => i.InternalId, i => i.Name + (i.ProductionYear.HasValue ? $" ({i.ProductionYear})" : ""));
                                    string CollLabel(long id) => _collMap.TryGetValue(id, out var _cn) ? _cn : id.ToString();
                                    foreach (var id in toAdd) _log.Debug($"    + {CollLabel(id)}");
                                    foreach (var id in toRemove) _log.Debug($"    - {CollLabel(id)}");
                                }
                            }
                            else if (toAdd.Count > 0 || toRemove.Count > 0)
                            {
                                collWouldUpdate++;
                                _log.Debug($"  {cName}  →  would be updated (+{toAdd.Count}, -{toRemove.Count})");
                            }
                            else
                            {
                                _log.Debug($"  {cName}  →  up to date ({currentMembers.Count} items)");
                            }
                            if (!dryRun && (collectionDescriptions.ContainsKey(cName) || collectionPosters.ContainsKey(cName)
                                || collectionThumbs.ContainsKey(cName) || collectionBackdrops.ContainsKey(cName)))
                                ApplyCollectionMeta(existingColl, cName, collectionDescriptions, collectionPosters, collectionThumbs, collectionBackdrops, debug);
                        }
                    }
                    catch (Exception ex)
                    {
                        _log.Error($"Collection \"{cName}\" could not be updated: {ex.Message}");
                        WriteExceptionDebug(ex);
                        foreach (var _gsC in statsList.Where(g => string.Equals(g.CollectionName, cName, StringComparison.OrdinalIgnoreCase)))
                            _gsC.Warnings.Add($"Collection could not be updated: {ex.Message}");
                    }
                }
                foreach (var gs in statsList)
                {
                    if (gs.CollectionName != null)
                    {
                        gs.CollectionCreated = collCreatedSet.Contains(gs.CollectionName);
                        gs.CollectionItemsAdded = collItemsAdded.GetValueOrDefault(gs.CollectionName);
                        gs.CollectionItemsRemoved = collItemsRemoved.GetValueOrDefault(gs.CollectionName);
                    }
                }

                int collDeleted = 0;
                var toDelete = previouslyManagedCollections.Where(h => !activeCollections.Contains(h)).ToList();
                foreach (var oldName in toDelete)
                {
                    if (failedFetches.Contains(oldName))
                    {
                        _log.Warn($"Collection \"{oldName}\" was kept because its source failed to load (safety check)");
                        activeCollections.Add(oldName);
                        continue;
                    }

                    try
                    {
                        var coll = _libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "BoxSet" }, Name = oldName, Recursive = true }).FirstOrDefault();
                        if (coll != null && !dryRun)
                        {
                            _libraryManager.DeleteItem(coll, new DeleteOptions { DeleteFileLocation = false });
                            collDeleted++;
                            _log.Skip($"Collection \"{oldName}\" removed (its group is deleted, disabled or not in schedule)");
                        }
                        else if (coll != null)
                        {
                            _log.Skip($"Collection \"{oldName}\" would be removed (its group is deleted, disabled or not in schedule)");
                        }
                    }
                    catch (Exception ex)
                    {
                        _log.Warn($"Collection \"{oldName}\" could not be removed: {ex.Message}");
                    }
                }
                _log.Info(dryRun
                    ? $"    Would create {collWouldCreate} and update {collWouldUpdate} collections"
                    : $"    {collCreated} created, {collUpdated} updated, {collDeleted} removed  ·  {RunLog.Elapsed(phaseTimer.Elapsed)}");
                if (!dryRun) SaveFileHistory("homescreencompanion_collections.txt", activeCollections.ToList());
                if (!dryRun) Plugin.Instance.SaveConfiguration();

                tagsRemoved += CleanupBoxSetTags(config, dryRun, cancellationToken);

                _log.Blank();
                _log.Info("» Home sections");
                phaseTimer.Restart();
                if (!dryRun) ManageHomeSections(config, cancellationToken, debug, statsList);
                else _log.Skip("Dry run — home sections are not changed");
                if (!dryRun)
                    _log.Info($"    {statsList.Count(g => g.HomeSectionSynced)} synced, {statsList.Count(g => g.HomeSectionRemoved)} removed  ·  {RunLog.Elapsed(phaseTimer.Elapsed)}");

                CleanupDisabledPlaylists(config, dryRun);
                bool _hasTopLists = (config.TopLists ?? new List<TopListHomeSection>()).Any(t => !string.IsNullOrWhiteSpace(t.TagName));
                if (_hasTopLists) { _log.Blank(); _log.Info("» Top-lists"); }
                SyncTopListFolders(config, dryRun);
                if (!dryRun) TopListSyncTask.SyncAll(_libraryManager, _userViewManager, _userManager, _jsonSerializer, _logger, cancellationToken, _log);

                progress.Report(100);
                string elapsedStr = RunLog.Elapsed(DateTime.Now - startTime);

                // Merge BoxSet HSE entries with the same DisplayName + TagName into one display block
                var displayStatsList = new List<GroupRunStats>();
                var boxSetMergeMap = new Dictionary<string, GroupRunStats>(StringComparer.OrdinalIgnoreCase);
                foreach (var gs in statsList)
                {
                    if (gs.BoxSetHse)
                    {
                        var key = $"{gs.DisplayName}\x00{gs.TagName}";
                        if (boxSetMergeMap.TryGetValue(key, out var existing))
                        {
                            existing.BoxSetTaggedCount += gs.BoxSetTaggedCount;
                            existing.Warnings.AddRange(gs.Warnings);
                            existing.ElapsedMs += gs.ElapsedMs;
                            if (gs.HomeSectionSynced)
                            {
                                existing.HomeSectionSynced = true;
                                existing.HomeSectionUserCount = Math.Max(existing.HomeSectionUserCount, gs.HomeSectionUserCount);
                            }
                            if (gs.HomeSectionRemoved) existing.HomeSectionRemoved = true;
                        }
                        else
                        {
                            var merged = new GroupRunStats
                            {
                                DisplayName      = gs.DisplayName,
                                SourceType       = gs.SourceType,
                                Skipped          = gs.Skipped,
                                SkipReason       = gs.SkipReason,
                                ErrorMessage     = gs.ErrorMessage,
                                EnableTag        = gs.EnableTag,
                                EnableCollection = gs.EnableCollection,
                                EnableHomeSection = gs.EnableHomeSection,
                                HomeSectionSynced = gs.HomeSectionSynced,
                                HomeSectionUserCount = gs.HomeSectionUserCount,
                                HomeSectionRemoved = gs.HomeSectionRemoved,
                                BoxSetHse        = true,
                                BoxSetFound      = gs.BoxSetFound,
                                BoxSetTaggedCount = gs.BoxSetTaggedCount,
                                TagName          = gs.TagName,
                                CollectionName   = gs.CollectionName,
                                SourceLabel      = gs.SourceLabel,
                                EnablePlaylist   = gs.EnablePlaylist,
                                PlaylistName     = gs.PlaylistName,
                                PlaylistUsersTotal = gs.PlaylistUsersTotal,
                                PlaylistUsersCreated = gs.PlaylistUsersCreated,
                                PlaylistUsersUpdated = gs.PlaylistUsersUpdated,
                                PlaylistUsersFailed = gs.PlaylistUsersFailed,
                                Warnings         = new List<string>(gs.Warnings),
                                MissingItems     = new List<string>(gs.MissingItems),
                                ElapsedMs        = gs.ElapsedMs,
                            };
                            boxSetMergeMap[key] = merged;
                            displayStatsList.Add(merged);
                        }
                    }
                    else
                    {
                        displayStatsList.Add(gs);
                    }
                }
                int displayTotal = displayStatsList.Count;
                for (int i = 0; i < displayStatsList.Count; i++)
                {
                    displayStatsList[i].GroupIndex = i + 1;
                    displayStatsList[i].GroupTotal = displayTotal;
                }

                // Emit per-group blocks
                _log.Blank();
                _log.Info("Results");
                foreach (var gs in displayStatsList)
                    WriteGroupBlock(gs, dryRun, logMissing);

                // Log cleanup of tags from deleted/disabled groups (tags that had removals but
                // no matching entry in displayStatsList to attribute them to).
                var displayedTagNames = new HashSet<string>(
                    displayStatsList.Where(g => g.TagName != null).Select(g => g.TagName!),
                    StringComparer.OrdinalIgnoreCase);
                foreach (var kvp in tagRemovedByTag.Where(kvp => kvp.Value > 0 && !displayedTagNames.Contains(kvp.Key)))
                {
                    _log.Info("[Cleanup]");
                    _log.Skip($"Tag \"{kvp.Key}\" {(dryRun ? "would be removed" : "removed")} from {RunLog.Plural(kvp.Value, "item")} (its group is deleted or disabled)");
                    _log.Blank();
                }

                // Final summary
                int totalCollCreated = statsList.Count(g => g.CollectionCreated);
                int totalCollUpdated = statsList.Count(g => !g.CollectionCreated && !g.Skipped && g.EnableCollection && (g.CollectionItemsAdded > 0 || g.CollectionItemsRemoved > 0));
                int totalHsSynced = statsList.Count(g => g.HomeSectionSynced);
                int totalHsRemoved = statsList.Count(g => g.HomeSectionRemoved);

                int totalBoxSetsTagged = displayStatsList
                    .Where(g => g.BoxSetHse && !g.Skipped && g.ErrorMessage == null)
                    .Sum(g => g.BoxSetTaggedCount);
                int summaryTagsAdded = tagsAdded + totalBoxSetsTagged;

                if (aiConfigChanged)
                    Plugin.Instance.SaveConfiguration();

                int groupsFailed  = displayStatsList.Count(g => g.ErrorMessage != null);
                int groupsSkipped = displayStatsList.Count(g => g.Skipped);
                int groupsWarned  = displayStatsList.Count(g => !g.Skipped && g.ErrorMessage == null && g.Warnings.Count > 0);
                int groupsOk      = displayStatsList.Count - groupsFailed - groupsSkipped - groupsWarned;
                string finalStatus = BuildFinalStatus(dryRun, groupsFailed, groupsWarned);
                LastRunStatus = $"{finalStatus} ({DateTime.Now:HH:mm})";

                _log.Rule();
                _log.Info("Summary");
                var _groupParts = new List<string> { $"{groupsOk} OK" };
                if (groupsWarned > 0) _groupParts.Add($"{groupsWarned} with warnings");
                if (groupsSkipped > 0) _groupParts.Add($"{groupsSkipped} skipped");
                if (groupsFailed > 0) _groupParts.Add($"{groupsFailed} failed");
                _log.Info($"  Groups:        {string.Join(", ", _groupParts)}");
                _log.Info($"  Tags:          +{summaryTagsAdded} added, -{tagsRemoved} removed");
                _log.Info(dryRun
                    ? $"  Collections:   {collWouldCreate} would be created, {collWouldUpdate} updated"
                    : $"  Collections:   {totalCollCreated} created, {totalCollUpdated} updated, {collDeleted} removed");
                if (statsList.Any(g => g.EnablePlaylist))
                    _log.Info($"  Playlists:     {statsList.Sum(g => g.PlaylistUsersCreated)} created, {statsList.Sum(g => g.PlaylistUsersUpdated)} updated{(statsList.Sum(g => g.PlaylistUsersFailed) > 0 ? $", {statsList.Sum(g => g.PlaylistUsersFailed)} failed" : "")}");
                _log.Info($"  Home sections: {totalHsSynced} synced, {totalHsRemoved} removed");
                _log.Info($"  Done in {elapsedStr}  ·  {StatusSymbol(groupsFailed, groupsWarned)} {finalStatus}");
                _log.Rule();
            }
            catch (Exception ex)
            {
                LastRunStatus = $"Failed: {ex.Message}";
                _log.Error($"Sync aborted: {ex.Message}");
                WriteExceptionDebug(ex);
            }
            finally { IsRunning = false; PersistLog(); }
        }

        public async Task<(bool Success, string Message)> RunSingleEntryAsync(string entryName, CancellationToken cancellationToken)
        {
            IsRunning = true;
            lock (ExecutionLog) ExecutionLog.Clear();
            LastStartedUtc = DateTime.UtcNow;
            LastRunStatus = "Running...";
            try
            {
            return await RunSingleEntryInternalAsync(entryName, cancellationToken);
            }
            finally
            {
                IsRunning = false;
                PersistLog();
            }
        }

        private async Task<(bool Success, string Message)> RunSingleEntryInternalAsync(string entryName, CancellationToken cancellationToken)
        {
            var config = Plugin.Instance?.Configuration;
            if (config == null) return (false, "Config not found");
            _writtenTags = BuildWrittenTags(config);

            var tagConfig = config.Tags.FirstOrDefault(t =>
                string.Equals(t.Name, entryName, StringComparison.OrdinalIgnoreCase) ||
                (!string.IsNullOrWhiteSpace(t.Name) == false && string.Equals(t.Tag, entryName, StringComparison.OrdinalIgnoreCase)));
            if (tagConfig == null) { LastRunStatus = $"Failed: entry not found"; _log.Error($"Group '{entryName}' was not found in the saved settings — save your settings and try again"); return (false, $"Entry '{entryName}' not found in saved config"); }
            if (string.IsNullOrWhiteSpace(tagConfig.Tag)) { LastRunStatus = "Failed: no tag name"; _log.Error($"Group '{entryName}' has no tag name"); return (false, "Entry has no tag name"); }

            // A group with several URLs / local sources is stored as one flat TagConfig per source
            // (same Name + Tag). tagConfig owns the shared settings; groupEntries supplies the sources.
            var groupEntryKey = GroupKey(tagConfig);
            var groupEntries = config.Tags
                .Where(t => string.Equals(GroupKey(t), groupEntryKey, StringComparison.OrdinalIgnoreCase))
                .ToList();

            string _displayName = !string.IsNullOrWhiteSpace(tagConfig.Name) ? $"{tagConfig.Name} [{tagConfig.Tag.Trim()}]" : tagConfig.Tag.Trim();
            string _srcLabel = string.IsNullOrEmpty(tagConfig.SourceType) ? "External" : tagConfig.SourceType;

            bool debug = config.ExtendedConsoleOutput;
            bool dryRun = config.DryRunMode;
            bool logMissing = config.LogMissingItems;
            var startTime = DateTime.Now;
            _log = new RunLog(ExecutionLog, _logger, "", debug);

            if (tagConfig.SourceType == "AI" && tagConfig.AiRefreshIntervalDays > 0 &&
                tagConfig.AiLastRunDate > DateTime.MinValue &&
                (DateTime.UtcNow - tagConfig.AiLastRunDate).TotalDays < tagConfig.AiRefreshIntervalDays)
            {
                var _nextAiRun = tagConfig.AiLastRunDate.AddDays(tagConfig.AiRefreshIntervalDays);
                _log.Rule();
                _log.Info($"Home Screen Companion v{Plugin.Instance?.Version}  ·  {startTime:yyyy-MM-dd HH:mm}  ·  Single group: {_displayName}");
                _log.Skip($"Skipped: AI refresh not due until {_nextAiRun:yyyy-MM-dd}");
                _log.Rule();
                LastRunStatus = $"Skipped ({DateTime.Now:HH:mm})";
                return (true, $"Skipped — AI refresh not due until {_nextAiRun:yyyy-MM-dd}");
            }

            var allItems = _libraryManager.GetItemList(new InternalItemsQuery
            {
                IncludeItemTypes = BuildItemTypes(config),
                Recursive = true,
                IsVirtualItem = false
            }).ToList();

            int _movieCount = allItems.Count(i => i.GetType().Name.Contains("Movie"));
            int _seriesCount = allItems.Count(i => i.GetType().Name.Contains("Series"));
            _log.Rule();
            _log.Info($"Home Screen Companion v{Plugin.Instance?.Version}  ·  {startTime:yyyy-MM-dd HH:mm}  ·  Single group: {_displayName}");
            if (dryRun) _log.Warn("DRY RUN — nothing will be changed, the log shows what would happen");
            _log.Info($"  Library: {_movieCount:N0} movies, {_seriesCount:N0} series");
            _log.Rule();

            var gs = new GroupRunStats
            {
                DisplayName = _displayName,
                SourceType = _srcLabel,
                SourceLabel = DescribeSource(tagConfig),
                EnableTag = tagConfig.EnableTag && !tagConfig.OnlyCollection,
                EnableCollection = tagConfig.EnableCollection,
                EnableHomeSection = tagConfig.EnableHomeSection,
                EnablePlaylist = tagConfig.EnablePlaylist,
                PlaylistName = string.IsNullOrWhiteSpace(tagConfig.PlaylistName) ? tagConfig.Name : tagConfig.PlaylistName,
                PlaylistUsersTotal = tagConfig.PlaylistUserIds?.Count ?? 0,
                BoxSetHse = IsBoxSetHomeSectionEntry(tagConfig),
                TagName = tagConfig.Tag.Trim(),
                CollectionName = string.IsNullOrWhiteSpace(tagConfig.CollectionName) ? tagConfig.Tag.Trim() : tagConfig.CollectionName.Trim(),
                GroupIndex = 1,
                GroupTotal = 1
            };
            var groupTimer = System.Diagnostics.Stopwatch.StartNew();

            var _topListsFolder = Path.Combine(Plugin.Instance.DataFolderPath, "toplists") + Path.DirectorySeparatorChar;
            var imdbLookup = new Dictionary<string, List<BaseItem>>(StringComparer.OrdinalIgnoreCase);
            foreach (var item in allItems)
            {
                if (item.LocationType != LocationType.FileSystem) continue;
                if (!string.IsNullOrEmpty(item.Path) &&
                    item.Path.StartsWith(_topListsFolder, StringComparison.OrdinalIgnoreCase))
                    continue;
                var imdb = item.GetProviderId("Imdb");
                if (!string.IsNullOrEmpty(imdb))
                {
                    if (!imdbLookup.ContainsKey(imdb)) imdbLookup[imdb] = new List<BaseItem>();
                    imdbLookup[imdb].Add(item);
                }
            }

            // See Execute: top-list copies never take part in local matching.
            var matchableItems = allItems.Where(i => string.IsNullOrEmpty(i.Path)
                || !i.Path.StartsWith(_topListsFolder, StringComparison.OrdinalIgnoreCase)).ToList();

            var seriesEpisodeCache = new Dictionary<long, BaseItem>();
            var personCache = new Dictionary<string, HashSet<long>>(StringComparer.OrdinalIgnoreCase);
            var collectionMembershipCache = new Dictionary<string, HashSet<long>>(StringComparer.OrdinalIgnoreCase);
            var mediaInfoCache = new Dictionary<long, CachedMediaInfo>();
            var userDataCache = new Dictionary<(Guid, long), (bool Played, DateTimeOffset? LastPlayedDate, int PlayCount)>();
            var seriesLastPlayedCache = new Dictionary<(Guid, long), DateTimeOffset?>();
            var preloadedUsers = _userManager.GetUserList(new UserQuery { IsDisabled = false });
            Dictionary<long, List<string>>? seriesEpisodeNamesCache = null;

            var needsMediaInfoEval = tagConfig.SourceType == "MediaInfo"
                || (tagConfig.MediaInfoFilters?.Count > 0 || tagConfig.MediaInfoConditions?.Count > 0);

            if (needsMediaInfoEval)
            {
                bool singleTagAnyEpisodePersonCriteria = TagConfigTargetsEpisodes(tagConfig)
                    && GetAllCriteria(tagConfig).Any(c => { var s = c.TrimStart('!'); return s.StartsWith("Actor:") || s.StartsWith("Director:") || s.StartsWith("Writer:"); });
                var allCriteria = (tagConfig.MediaInfoFilters ?? new List<MediaInfoFilter>())
                    .SelectMany(f => f.Criteria ?? new List<string>())
                    .Concat(tagConfig.MediaInfoConditions ?? new List<string>())
                    .Select(c => c.Length > 0 && c[0] == '!' ? c.Substring(1) : c)
                    .Distinct(StringComparer.OrdinalIgnoreCase);
                BaseItem[]? allPersonsTag = null;
                foreach (var c in allCriteria)
                {
                    var p = c.Split(':');
                    if ((p.Length == 2 || (p.Length == 3 && (p[1] == "exact" || p[1] == "contains")))
                        && (p[0] == "Actor" || p[0] == "Director" || p[0] == "Writer")
                        && Enum.TryParse<MediaBrowser.Model.Entities.PersonType>(p[0], out var personTypeEnum))
                    {
                        string matchOp = p.Length == 3 ? p[1] : "exact";
                        string personNameRaw = p.Length == 3 ? p[2].Trim() : p[1].Trim();
                        var personTypes = singleTagAnyEpisodePersonCriteria && p[0] == "Actor"
                            ? new[] { personTypeEnum, MediaBrowser.Model.Entities.PersonType.GuestStar }
                            : new[] { personTypeEnum };
                        foreach (var singleName in personNameRaw.Split(new[] { ',', '\n', '\r' }, StringSplitOptions.RemoveEmptyEntries).Select(n => n.Trim()).Where(n => n.Length > 0))
                        {
                            if (matchOp == "contains")
                            {
                                string containsKey = $"{p[0]}:contains:{singleName}";
                                if (personCache.ContainsKey(containsKey)) continue;
                                allPersonsTag ??= _libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "Person" } }).ToArray();
                                var combinedIds = new HashSet<long>();
                                foreach (var matchingPerson in allPersonsTag.Where(person => person.Name?.IndexOf(singleName, StringComparison.OrdinalIgnoreCase) >= 0))
                                {
                                    foreach (var mi in _libraryManager.GetItemList(new InternalItemsQuery
                                    {
                                        PersonIds = new[] { matchingPerson.InternalId },
                                        PersonTypes = personTypes,
                                        IncludeItemTypes = singleTagAnyEpisodePersonCriteria ? new[] { "Movie", "Series", "Episode" } : new[] { "Movie", "Series" },
                                        Recursive = true,
                                        IsVirtualItem = false
                                    })) combinedIds.Add(mi.InternalId);
                                }
                                personCache[containsKey] = combinedIds;
                            }
                            else
                            {
                                string indivKey = p.Length == 3 ? $"{p[0]}:{p[1]}:{singleName}" : $"{p[0]}:{singleName}";
                                if (personCache.ContainsKey(indivKey)) continue;
                                var personItem = _libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "Person" }, Name = singleName }).FirstOrDefault();
                                personCache[indivKey] = personItem == null ? new HashSet<long>() :
                                    _libraryManager.GetItemList(new InternalItemsQuery
                                    {
                                        PersonIds = new[] { personItem.InternalId },
                                        PersonTypes = personTypes,
                                        IncludeItemTypes = singleTagAnyEpisodePersonCriteria ? new[] { "Movie", "Series", "Episode" } : new[] { "Movie", "Series" },
                                        Recursive = true,
                                        IsVirtualItem = false
                                    }).Select(x => x.InternalId).ToHashSet();
                            }
                        }
                    }
                }
                foreach (var item in allItems)
                {
                    if (item.LocationType != LocationType.FileSystem) continue;
                    var resolved = ResolveItemForMediaInfo(item, seriesEpisodeCache);
                    mediaInfoCache[item.InternalId] = ExtractMediaInfo(resolved);
                }
                var singleTagCollPlCriteria = GetAllCriteria(tagConfig)
                    .Select(c => c.Length > 0 && c[0] == '!' ? c.Substring(1) : c)
                    .Where(c => c.StartsWith("Collection:", StringComparison.OrdinalIgnoreCase) || c.StartsWith("Playlist:", StringComparison.OrdinalIgnoreCase))
                    .Distinct(StringComparer.OrdinalIgnoreCase);
                foreach (var crit in singleTagCollPlCriteria)
                {
                    var colonIdx = crit.IndexOf(':');
                    if (colonIdx < 1) continue;
                    var sourceKind = crit.Substring(0, colonIdx);
                    var sourceNamesRaw = crit.Substring(colonIdx + 1).Trim();
                    string[] folderTypes = sourceKind.Equals("Playlist", StringComparison.OrdinalIgnoreCase)
                        ? new[] { "Playlist" } : new[] { "BoxSet" };
                    foreach (var singleName in SplitCommaValues(sourceNamesRaw))
                    {
                        var indivKey = sourceKind + ":" + singleName;
                        if (collectionMembershipCache.ContainsKey(indivKey)) continue;
                        var folder = _libraryManager.GetItemList(new InternalItemsQuery
                        {
                            IncludeItemTypes = folderTypes,
                            Recursive = true
                        }).FirstOrDefault(i => string.Equals(i.Name, singleName, StringComparison.OrdinalIgnoreCase));
                        if (folder == null) { collectionMembershipCache[indivKey] = new HashSet<long>(); continue; }
                        var members = sourceKind.Equals("Playlist", StringComparison.OrdinalIgnoreCase)
                            ? _libraryManager.GetItemList(new InternalItemsQuery { ListIds = new[] { folder.InternalId } })
                            : _libraryManager.GetItemList(new InternalItemsQuery { CollectionIds = new[] { folder.InternalId }, IsVirtualItem = false });
                        var ids = new HashSet<long>();
                        foreach (var m in members)
                        {
                            ids.Add(m.InternalId);
                            if (m.GetType().Name.Contains("Series"))
                            {
                                foreach (var ep in _libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "Episode" }, Parent = m, Recursive = true, IsVirtualItem = false }))
                                    ids.Add(ep.InternalId);
                            }
                        }
                        collectionMembershipCache[indivKey] = ids;
                    }
                }

                // Pre-populate userDataCache for all top-level items when IsPlayed/PlayCount/WatchedByCount/LastPlayed criteria exist
                bool needsItemUserData = GetAllCriteria(tagConfig).Any(c =>
                {
                    var _cp = c.TrimStart('!').Split(':');
                    return (_cp.Length == 4 && _cp[0] == "LastPlayed") || _cp[0] == "IsPlayed" || _cp[0] == "PlayCount" || _cp[0] == "WatchedByCount";
                });
                if (needsItemUserData && preloadedUsers?.Length > 0)
                {
                    foreach (var _user in preloadedUsers)
                    {
                        foreach (var _topItem in allItems)
                        {
                            var _k = (_user.Id, _topItem.InternalId);
                            if (userDataCache.ContainsKey(_k)) continue;
                            var _ud0 = _userDataManager?.GetUserData(_user, _topItem);
                            userDataCache[_k] = _ud0 == null ? (false, (DateTimeOffset?)null, 0) : (_ud0.Played, _ud0.LastPlayedDate, _ud0.PlayCount);
                        }
                    }
                }

                // Pre-fetch all episodes once if needed for LastPlayed or EpisodeTitle caches
                bool needsSeriesLastPlayed = GetAllCriteria(tagConfig).Any(c =>
                    c.TrimStart('!').Split(':') is var p && p.Length == 4 && p[0] == "LastPlayed");
                bool needsEpisodeTitleCache = GetAllCriteria(tagConfig).Any(c =>
                    c.TrimStart('!').StartsWith("EpisodeTitle:", StringComparison.OrdinalIgnoreCase));
                List<BaseItem>? allEpisodes = null;
                if (needsSeriesLastPlayed || needsEpisodeTitleCache)
                {
                    allEpisodes = _libraryManager.GetItemList(new InternalItemsQuery
                    {
                        IncludeItemTypes = new[] { "Episode" },
                        Recursive = true,
                        IsVirtualItem = false
                    }).ToList();
                }

                if (needsSeriesLastPlayed && preloadedUsers?.Length > 0 && allEpisodes != null)
                {
                    foreach (var user in preloadedUsers)
                    {
                        // Pre-populate userDataCache for all episodes so GetSeriesLastPlayed hits cache during scan
                        foreach (var ep in allEpisodes)
                        {
                            var epCacheKey = (user.Id, ep.InternalId);
                            if (userDataCache.ContainsKey(epCacheKey)) continue;
                            var ud = _userDataManager?.GetUserData(user, ep);
                            userDataCache[epCacheKey] = ud == null ? (false, (DateTimeOffset?)null, 0) : (ud.Played, ud.LastPlayedDate, ud.PlayCount);
                        }

                        // Pre-compute seriesLastPlayedCache for this user
                        var episodesBySeriesInternalId = new Dictionary<long, List<BaseItem>>();
                        foreach (var ep in allEpisodes)
                        {
                            BaseItem? seriesItem = null;
                            var parent = ep.Parent;
                            if (parent != null)
                            {
                                if (parent.GetType().Name.Contains("Series")) seriesItem = parent;
                                else if (parent.GetType().Name.Contains("Season") && parent.Parent?.GetType().Name.Contains("Series") == true) seriesItem = parent.Parent;
                            }
                            if (seriesItem == null) continue;
                            if (!episodesBySeriesInternalId.ContainsKey(seriesItem.InternalId))
                                episodesBySeriesInternalId[seriesItem.InternalId] = new List<BaseItem>();
                            episodesBySeriesInternalId[seriesItem.InternalId].Add(ep);
                        }
                        foreach (var kvp in episodesBySeriesInternalId)
                        {
                            var seriesCacheKey = (user.Id, kvp.Key);
                            if (seriesLastPlayedCache.ContainsKey(seriesCacheKey)) continue;
                            DateTimeOffset? maxDate = null;
                            foreach (var ep in kvp.Value)
                            {
                                if (userDataCache.TryGetValue((user.Id, ep.InternalId), out var cd) && cd.LastPlayedDate.HasValue)
                                    if (maxDate == null || cd.LastPlayedDate > maxDate) maxDate = cd.LastPlayedDate;
                            }
                            seriesLastPlayedCache[seriesCacheKey] = maxDate;
                        }
                    }
                }

                if (needsEpisodeTitleCache && allEpisodes != null)
                {
                    seriesEpisodeNamesCache = new Dictionary<long, List<string>>();
                    foreach (var ep in allEpisodes)
                    {
                        if (string.IsNullOrEmpty(ep.Name)) continue;
                        BaseItem? seriesItem = null;
                        var parent = ep.Parent;
                        if (parent != null)
                        {
                            if (parent.GetType().Name.Contains("Series")) seriesItem = parent;
                            else if (parent.GetType().Name.Contains("Season") && parent.Parent?.GetType().Name.Contains("Series") == true)
                                seriesItem = parent.Parent;
                        }
                        if (seriesItem == null) continue;
                        if (!seriesEpisodeNamesCache.TryGetValue(seriesItem.InternalId, out var nameList))
                        {
                            nameList = new List<string>();
                            seriesEpisodeNamesCache[seriesItem.InternalId] = nameList;
                        }
                        nameList.Add(ep.Name);
                    }
                }
            }

            string tagName = tagConfig.Tag.Trim();
            string cName = string.IsNullOrWhiteSpace(tagConfig.CollectionName) ? tagName : tagConfig.CollectionName.Trim();
            int effectiveLimit = tagConfig.Limit <= 0 ? 10000 : tagConfig.Limit;
            var blacklist = new HashSet<string>(tagConfig.Blacklist ?? new List<string>(), StringComparer.OrdinalIgnoreCase);
            // Remove this tag from the cache before repopulating so blacklisted items don't get
            // immediately re-tagged by the real-time event handler when UpdateItem is called.
            // (Full run clears the entire cache first; single run must do a targeted removal.)
            TagCacheManager.Instance.RemoveTagFromAllEntries(tagName);
            var matchedLocalItems = new List<BaseItem>();
            List<BaseItem> tagOutputItems = matchedLocalItems;
            List<BaseItem> collectionOutputItems = matchedLocalItems;
            int _listCount = 0;

            _log.Blank();
            _log.Info("» Fetching sources");
            _log.Section($"[1/1] {_displayName}");
            _log.Debug("  " + DescribeSourceDetail(tagConfig, effectiveLimit) + (groupEntries.Count > 1 ? $"  ·  {groupEntries.Count} sources in group" : ""));

            try
            {
                var fetcher = new ListFetcher(_httpClient, _jsonSerializer);

                if (string.IsNullOrEmpty(tagConfig.SourceType) || tagConfig.SourceType == "External")
                {
                    // One fetch per URL in the group; limit applies per URL (same as the full run)
                    foreach (var src in groupEntries)
                    {
                        if (string.IsNullOrWhiteSpace(src.Url)) continue;
                        int srcLimit = src.Limit <= 0 ? 10000 : src.Limit;
                        var fetchTimer = System.Diagnostics.Stopwatch.StartNew();
                        var items = await fetcher.FetchItems(src.Url, srcLimit, config.TraktClientId, config.MdblistApiKey, config.TmdbApiKey, cancellationToken);
                        fetchTimer.Stop();
                        _listCount += items.Count;
                        if (items.Count > srcLimit) items = items.Take(srcLimit).ToList();
                        int _srcMatched = 0, _srcBlacklisted = 0, _srcMissingBefore = gs.MissingItems.Count;
                        foreach (var extItem in items)
                        {
                            if (string.IsNullOrEmpty(extItem.Imdb)) continue;
                            if (blacklist.Contains(extItem.Imdb)) { _srcBlacklisted++; _log.Debug($"    Blacklisted: {extItem.Name} ({extItem.Imdb})"); continue; }
                            if (tagConfig.EnableTag && !tagConfig.OnlyCollection)
                                TagCacheManager.Instance.AddToCache($"imdb_{extItem.Imdb}", tagName);
                            if (imdbLookup.TryGetValue(extItem.Imdb, out var localItems))
                            {
                                _srcMatched++;
                                foreach (var localItem in localItems)
                                    if (!matchedLocalItems.Contains(localItem)) matchedLocalItems.Add(localItem);
                            }
                            else
                            {
                                gs.MissingItems.Add($"{extItem.Name}  {extItem.Imdb}");
                            }
                        }
                        _log.Debug($"  {src.Url}  →  {items.Count} items in {fetchTimer.ElapsedMilliseconds} ms  ·  {_srcMatched} matched by IMDb id  ·  {gs.MissingItems.Count - _srcMissingBefore} not in library  ·  {_srcBlacklisted} blacklisted");
                    }
                }
                else if (tagConfig.SourceType == "LocalCollection" || tagConfig.SourceType == "LocalPlaylist")
                {
                    string[] folderTypes = tagConfig.SourceType == "LocalPlaylist" ? new[] { "Playlist" } : new[] { "BoxSet" };
                    var allFolders = _libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = folderTypes, Recursive = true });
                    var missingSources = new List<string>();

                    // One pass per local source in the group; limit applies per source (same as the full run)
                    foreach (var src in groupEntries)
                    {
                        if (string.IsNullOrEmpty(src.LocalSourceId)) continue;
                        int srcLimit = src.Limit <= 0 ? 10000 : src.Limit;
                        var localSourceFolder = allFolders.FirstOrDefault(i => string.Equals(i.Name, src.LocalSourceId, StringComparison.OrdinalIgnoreCase));
                        if (localSourceFolder == null)
                        {
                            missingSources.Add(src.LocalSourceId);
                            gs.Warnings.Add($"{DescribeSource(tagConfig)} '{src.LocalSourceId}' was not found in the library");
                            continue;
                        }
                        _log.Debug($"  Found source '{localSourceFolder.Name}'  ({localSourceFolder.GetType().Name})");

                        var children = tagConfig.SourceType == "LocalCollection"
                            ? _libraryManager.GetItemList(new InternalItemsQuery { CollectionIds = new[] { localSourceFolder.InternalId }, IsVirtualItem = false }).ToList()
                            : _libraryManager.GetItemList(new InternalItemsQuery { ListIds = new[] { localSourceFolder.InternalId } }).ToList();
                        _listCount += children.Count;
                        var srcMatched = new List<BaseItem>();
                        foreach (var child in children)
                        {
                            if (child == null) continue;
                            BaseItem itemToTag = child;
                            if (child.GetType().Name.Contains("PlaylistItem")) { try { var inner = ((dynamic)child).Item; if (inner != null) itemToTag = inner; } catch { } }
                            if (itemToTag.GetType().Name.Contains("Episode")) { try { var series = ((dynamic)itemToTag).Series; if (series != null) itemToTag = series; } catch { } }
                            if (!IsTaggableTopLevelItem(itemToTag)) continue;
                            var imdb = itemToTag.GetProviderId("Imdb");
                            if (!string.IsNullOrEmpty(imdb) && blacklist.Contains(imdb)) continue;
                            if (!srcMatched.Contains(itemToTag)) srcMatched.Add(itemToTag);
                        }
                        if (srcLimit < 10000 && srcMatched.Count > srcLimit)
                            srcMatched = srcMatched.Take(srcLimit).ToList();
                        foreach (var m in srcMatched)
                            if (!matchedLocalItems.Contains(m)) matchedLocalItems.Add(m);
                        _log.Debug($"  '{src.LocalSourceId}'  →  {children.Count} items, {srcMatched.Count} usable movies/series");
                    }

                    // Only fail outright if no source in the group could be resolved
                    if (missingSources.Count > 0 && matchedLocalItems.Count == 0 && _listCount == 0)
                    {
                        gs.ErrorMessage = $"Source '{string.Join("', '", missingSources)}' not found";
                        gs.Warnings.Clear();
                        gs.ElapsedMs = groupTimer.ElapsedMilliseconds;
                        WriteFetchLine(gs);
                        WriteSingleRunFooter(gs, startTime, dryRun, logMissing);
                        return (false, $"Source '{string.Join("', '", missingSources)}' not found");
                    }
                }
                else if (tagConfig.SourceType == "MediaInfo")
                {
                    IList<BaseItem> _itemsToScan;
                    if (TagConfigTargetsEpisodes(tagConfig))
                    {
                        var _epQuery = new InternalItemsQuery { IncludeItemTypes = new[] { "Episode" }, Recursive = true, IsVirtualItem = false };
                        var _tc = ExtractTitleContains(tagConfig);
                        if (!string.IsNullOrEmpty(_tc)) _epQuery.NameContains = _tc;
                        _itemsToScan = _libraryManager.GetItemList(_epQuery).ToList();
                    }
                    else
                    {
                        _itemsToScan = matchableItems;
                    }
                    _listCount = _itemsToScan.Count;
                    foreach (var item in _itemsToScan)
                    {
                        if (item.LocationType != LocationType.FileSystem) continue;
                        var imdb = item.GetProviderId("Imdb");
                        if (!string.IsNullOrEmpty(imdb) && blacklist.Contains(imdb)) continue;
                        CachedMediaInfo? ci = mediaInfoCache.TryGetValue(item.InternalId, out var ciVal) ? ciVal : (CachedMediaInfo?)null;
                        if (ItemMatchesMediaInfo(item, tagConfig, debug, seriesEpisodeCache, personCache, userDataCache, ci, preloadedUsers, seriesLastPlayedCache, collectionMembershipCache, seriesEpisodeNamesCache))
                        {
                            matchedLocalItems.Add(item);
                            if (effectiveLimit < 10000 && matchedLocalItems.Count >= effectiveLimit) break;
                        }
                    }
                    if (debug)
                    {
                        _log.Debug($"  Scanned {_itemsToScan.Count:N0} items in {groupTimer.ElapsedMilliseconds} ms  ·  {matchedLocalItems.Count} matched");
                        WriteMatchedItemsDebug(matchedLocalItems);
                    }
                    // Redirect matched items to the selected output level (tag and collection independently)
                    tagOutputItems = matchedLocalItems;
                    collectionOutputItems = matchedLocalItems;
                    {
                        bool scannedEpisodes = TagConfigTargetsEpisodes(tagConfig);
                        var (tEp, tSea, tSer) = EffectiveTagTargets(tagConfig);
                        var (cEp, cSea, cSer) = EffectiveCollectionTargets(tagConfig);

                        List<BaseItem> BuildOutputList(bool ep, bool sea, bool ser, bool anyNew)
                        {
                            if (!anyNew) return scannedEpisodes ? ResolveParentSeries(matchedLocalItems) : matchedLocalItems.ToList();
                            var list = new List<BaseItem>();
                            var seriesOnly = matchedLocalItems.Where(i => i.GetType().Name.Contains("Series")).ToList();
                            if (scannedEpisodes)
                            {
                                if (ep) list.AddRange(matchedLocalItems);
                                if (sea) list.AddRange(ResolveParentSeasons(matchedLocalItems));
                                if (ser) list.AddRange(ResolveParentSeries(matchedLocalItems));
                            }
                            else
                            {
                                var movies = matchedLocalItems.Where(i => !i.GetType().Name.Contains("Series")).ToList();
                                if (ser) list.AddRange(matchedLocalItems);
                                if (sea) { list.AddRange(ResolveChildSeasons(seriesOnly)); list.AddRange(movies); }
                                if (ep)  { list.AddRange(ResolveChildEpisodes(seriesOnly)); list.AddRange(movies); }
                            }
                            return list;
                        }

                        tagOutputItems        = BuildOutputList(tEp, tSea, tSer, tEp || tSea || tSer);
                        collectionOutputItems = BuildOutputList(cEp, cSea, cSer, cEp || cSea || cSer);
                    }
                }
                else if (tagConfig.SourceType == "AI")
                {
                    var recentlyWatchedContext = BuildRecentlyWatchedContext(tagConfig);
                    var fetchTimer = System.Diagnostics.Stopwatch.StartNew();
                    var aiItems = await fetcher.FetchAiList(
                        tagConfig.AiProvider,
                        tagConfig.AiPrompt,
                        config.OpenAiApiKey,
                        config.OpenAiModel,
                        config.GeminiApiKey,
                        config.GeminiModel,
                        config.ClaudeApiKey,
                        config.ClaudeModel,
                        config.OllamaBaseUrl,
                        config.OllamaModel,
                        config.AiSystemPrompt,
                        recentlyWatchedContext,
                        effectiveLimit,
                        cancellationToken);
                    fetchTimer.Stop();

                    _listCount = aiItems.Count;
                    int _aiTitleMatched = 0, _aiBlacklisted = 0;

                    foreach (var aiItem in aiItems)
                    {
                        if (string.IsNullOrWhiteSpace(aiItem.title)) continue;
                        string _aiLabel = aiItem.year.HasValue ? $"{aiItem.title} ({aiItem.year})" : aiItem.title;

                        if (!string.IsNullOrEmpty(aiItem.imdb_id))
                        {
                            var imdbId = aiItem.imdb_id.Trim();
                            if (blacklist.Contains(imdbId)) { _aiBlacklisted++; _log.Debug($"    Blacklisted: {_aiLabel} ({imdbId})"); continue; }
                            if (tagConfig.EnableTag && !tagConfig.OnlyCollection)
                                TagCacheManager.Instance.AddToCache($"imdb_{imdbId}", tagName);
                            if (imdbLookup.TryGetValue(imdbId, out var localItems))
                            {
                                foreach (var localItem in localItems)
                                    if (!matchedLocalItems.Contains(localItem)) matchedLocalItems.Add(localItem);
                            }
                            else
                            {
                                // IMDB ID not found in library — fall back to title+year match
                                var titleMatches = FindByTitleAndYear(matchableItems, aiItem.title, aiItem.year);
                                if (titleMatches.Count > 0) { _aiTitleMatched++; _log.Debug($"    {imdbId} not in library — matched '{_aiLabel}' by title"); }
                                else gs.MissingItems.Add($"{_aiLabel}  {imdbId}");
                                foreach (var localItem in titleMatches)
                                {
                                    var imdb = localItem.GetProviderId("Imdb");
                                    if (!string.IsNullOrEmpty(imdb) && blacklist.Contains(imdb)) continue;
                                    if (!matchedLocalItems.Contains(localItem)) matchedLocalItems.Add(localItem);
                                }
                            }
                        }
                        else
                        {
                            var titleMatches = FindByTitleAndYear(matchableItems, aiItem.title, aiItem.year);
                            if (titleMatches.Count > 0) _aiTitleMatched++;
                            else gs.MissingItems.Add($"{_aiLabel}  (no IMDb id from AI)");
                            foreach (var localItem in titleMatches)
                            {
                                var imdb = localItem.GetProviderId("Imdb");
                                if (!string.IsNullOrEmpty(imdb) && blacklist.Contains(imdb)) { _aiBlacklisted++; continue; }
                                if (!matchedLocalItems.Contains(localItem)) matchedLocalItems.Add(localItem);
                            }
                        }
                    }
                    _log.Debug($"  AI ({tagConfig.AiProvider}) returned {_listCount} items in {fetchTimer.ElapsedMilliseconds} ms  ·  {matchedLocalItems.Count} matched ({_aiTitleMatched} by title only)  ·  {gs.MissingItems.Count} not in library  ·  {_aiBlacklisted} blacklisted");

                    if (tagConfig.AiRefreshIntervalDays > 0)
                    {
                        tagConfig.AiLastRunDate = DateTime.UtcNow;
                        Plugin.Instance.SaveConfiguration();
                    }
                }
            }
            catch (Exception ex)
            {
                gs.ErrorMessage = ex.Message;
                gs.ElapsedMs = groupTimer.ElapsedMilliseconds;
                WriteExceptionDebug(ex);
                WriteFetchLine(gs);
                WriteSingleRunFooter(gs, startTime, dryRun, logMissing);
                LastRunStatus = $"Failed: {ex.Message}";
                return (false, $"Error: {ex.Message}");
            }

            // Apply MediaInfo post-filter for non-MediaInfo source types
            if (tagConfig.SourceType != "MediaInfo" && matchedLocalItems.Count > 0
                && (tagConfig.MediaInfoFilters?.Count > 0 || tagConfig.MediaInfoConditions?.Count > 0))
            {
                var beforeCount = matchedLocalItems.Count;
                matchedLocalItems = matchedLocalItems.Where(item =>
                {
                    CachedMediaInfo? ci = mediaInfoCache.TryGetValue(item.InternalId, out var ciVal) ? ciVal : (CachedMediaInfo?)null;
                    return ItemMatchesMediaInfo(item, tagConfig, debug, seriesEpisodeCache, personCache, userDataCache, ci, preloadedUsers, seriesLastPlayedCache, collectionMembershipCache, seriesEpisodeNamesCache);
                }).ToList();
                _log.Debug($"  Filter conditions: {beforeCount} → {matchedLocalItems.Count} items");
            }

            // For non-MediaInfo sources, apply output level selection (expand down from Series/Movie)
            if (tagConfig.SourceType != "MediaInfo")
            {
                var (tEp, tSea, tSer) = EffectiveTagTargets(tagConfig);
                var (cEp, cSea, cSer) = EffectiveCollectionTargets(tagConfig);

                List<BaseItem> BuildNonMiOutput(bool ep, bool sea, bool ser, bool any)
                {
                    if (!any) return matchedLocalItems.ToList();
                    var list = new List<BaseItem>();
                    var seriesOnly = matchedLocalItems.Where(i => i.GetType().Name.Contains("Series")).ToList();
                    var movies = matchedLocalItems.Where(i => !i.GetType().Name.Contains("Series")).ToList();
                    if (ser) list.AddRange(matchedLocalItems);
                    if (sea) { var s = ResolveChildSeasons(seriesOnly); list.AddRange(s); list.AddRange(movies); }
                    if (ep)  { var e = ResolveChildEpisodes(seriesOnly); list.AddRange(e); list.AddRange(movies); }
                    return list;
                }

                tagOutputItems        = BuildNonMiOutput(tEp, tSea, tSer, tEp || tSea || tSer);
                collectionOutputItems = BuildNonMiOutput(cEp, cSea, cSer, cEp || cSea || cSer);
            }

            gs.ListCount = _listCount;
            if ((string.IsNullOrEmpty(tagConfig.SourceType) || tagConfig.SourceType == "External" || tagConfig.SourceType == "AI") && _listCount == 0)
                gs.Warnings.Add(tagConfig.SourceType == "AI"
                    ? "The AI returned 0 items — the group's tags, collection and playlist are being cleared. Check the prompt and the API key in Settings."
                    : "The list returned 0 items — the group's tags, collection and playlist are being cleared. Check the list URL and the API key in Settings.");
            {
                var _outIds = new HashSet<Guid>(tagOutputItems.Select(i => i.Id));
                foreach (var _id in collectionOutputItems.Select(i => i.Id)) _outIds.Add(_id);
                gs.MatchCount = _outIds.Count;
            }
            if (gs.MissingItems.Count > 0 && debug)
            {
                _log.Debug($"  Not in library ({gs.MissingItems.Count}):");
                foreach (var _missing in gs.MissingItems) _log.Debug("    " + _missing);
            }
            gs.ElapsedMs = groupTimer.ElapsedMilliseconds;
            _log.Debug($"  Group done in {gs.ElapsedMs} ms");
            WriteFetchLine(gs);

            // Save rank file so top-list .strm files can be numbered in list order
            if (!dryRun)
            {
                WriteRankFile(tagName, matchedLocalItems
                    .Select(i => i.GetProviderId("Imdb") ?? "")
                    .Where(id => !string.IsNullOrEmpty(id))
                    .ToList());
            }

            // Apply tags (scoped to this entry's tag only)
            _log.Blank();
            _log.Info("» Applying tags");
            var phaseTimer = System.Diagnostics.Stopwatch.StartNew();
            int tagsAdded = 0, tagsRemoved = 0;
            var _dbgTagAdded = debug ? new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase) : null;
            var _dbgTagRemoved = debug ? new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase) : null;
            var matchedIds = new HashSet<Guid>(tagOutputItems.Select(i => i.Id));
            var _isBoxSetHse = IsBoxSetHomeSectionEntry(tagConfig); // computed once, not per-item
            int updateCount = 0;
            foreach (var item in allItems)
            {
                var existingTags = new HashSet<string>(item.Tags, StringComparer.OrdinalIgnoreCase);
                bool shouldHave = tagConfig.EnableTag && !tagConfig.OnlyCollection && !_isBoxSetHse && matchedIds.Contains(item.Id);
                bool hasTag = existingTags.Contains(tagName);
                if (shouldHave == hasTag) continue;
                if (debug)
                {
                    var _tagYr = item.ProductionYear.HasValue ? $" ({item.ProductionYear})" : "";
                    var _tagTp = item.GetType().Name.Contains("Series") ? "Series" : "Movie";
                    string _itemLabel = $"{item.Name}{_tagYr}  [{_tagTp}]";
                    if (shouldHave) { if (!_dbgTagAdded!.ContainsKey(tagName)) _dbgTagAdded[tagName] = new List<string>(); _dbgTagAdded[tagName].Add(_itemLabel); }
                    else { if (!_dbgTagRemoved!.ContainsKey(tagName)) _dbgTagRemoved[tagName] = new List<string>(); _dbgTagRemoved[tagName].Add(_itemLabel); }
                }
                if (!dryRun)
                {
                    if (shouldHave) item.AddTag(tagName); else item.RemoveTag(tagName);
                    try { _libraryManager.UpdateItem(item, item.Parent, ItemUpdateType.MetadataEdit, null); }
                    catch (Exception ex) { _log.Warn($"Could not save tags for '{item.Name}': {ex.Message}"); }
                    if (++updateCount % 25 == 0) await Task.Yield();
                }
                if (shouldHave) tagsAdded++; else tagsRemoved++;
            }

            // Episode cleanup: remove stale tags from episodes; also tag matching episodes when episode-level targeting is active
            {
                var (tEpClean, _, _) = EffectiveTagTargets(tagConfig);
                bool targetsEpisodes = tagConfig.EnableTag && !tagConfig.OnlyCollection && !_isBoxSetHse && tEpClean &&
                    (tagConfig.SourceType != "MediaInfo" || TagConfigTargetsEpisodes(tagConfig));
                var matchedEpisodeIds = new HashSet<Guid>();
                var allEpisodeItemsMap = new Dictionary<Guid, BaseItem>();
                if (targetsEpisodes)
                {
                    if (tagConfig.SourceType == "MediaInfo" && TagConfigTargetsEpisodes(tagConfig))
                    {
                        foreach (var ep in _libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "Episode" }, Recursive = true, IsVirtualItem = false }))
                        {
                            if (ep.LocationType != LocationType.FileSystem) continue;
                            if (ItemMatchesMediaInfo(ep, tagConfig, debug, seriesEpisodeCache, personCache, userDataCache, null, preloadedUsers, seriesLastPlayedCache, collectionMembershipCache, seriesEpisodeNamesCache))
                            {
                                matchedEpisodeIds.Add(ep.Id);
                                allEpisodeItemsMap.TryAdd(ep.Id, ep);
                            }
                        }
                    }
                    else
                    {
                        // Non-MediaInfo: episodes were already resolved into tagOutputItems
                        foreach (var ep in tagOutputItems.Where(i => i.GetType().Name.Contains("Episode")))
                        {
                            matchedEpisodeIds.Add(ep.Id);
                            allEpisodeItemsMap.TryAdd(ep.Id, ep);
                        }
                    }
                }
                foreach (var ep in _libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "Episode" }, Tags = new[] { tagName }, Recursive = true, IsVirtualItem = false }))
                    allEpisodeItemsMap.TryAdd(ep.Id, ep);
                foreach (var ep in allEpisodeItemsMap.Values)
                {
                    bool shouldHaveEp = tagConfig.EnableTag && !tagConfig.OnlyCollection && matchedEpisodeIds.Contains(ep.Id);
                    bool hasTagEp = new HashSet<string>(ep.Tags, StringComparer.OrdinalIgnoreCase).Contains(tagName);
                    if (shouldHaveEp == hasTagEp) continue;
                    if (debug)
                    {
                        var _epYr = ep.ProductionYear.HasValue ? $" ({ep.ProductionYear})" : "";
                        string _epLabel = $"{ep.Name}{_epYr}  [Episode]";
                        if (shouldHaveEp) { if (!_dbgTagAdded!.ContainsKey(tagName)) _dbgTagAdded[tagName] = new List<string>(); _dbgTagAdded[tagName].Add(_epLabel); }
                        else { if (!_dbgTagRemoved!.ContainsKey(tagName)) _dbgTagRemoved[tagName] = new List<string>(); _dbgTagRemoved[tagName].Add(_epLabel); }
                    }
                    if (!dryRun)
                    {
                        if (shouldHaveEp) ep.AddTag(tagName); else ep.RemoveTag(tagName);
                        try { _libraryManager.UpdateItem(ep, ep.Parent, ItemUpdateType.MetadataEdit, null); }
                        catch (Exception ex) { _log.Warn($"Could not save tags for episode '{ep.Name}': {ex.Message}"); }
                        if (++updateCount % 25 == 0) await Task.Yield();
                    }
                    if (shouldHaveEp) tagsAdded++; else tagsRemoved++;
                }
            }

            // Season cleanup: remove stale tags from seasons; also tag matching seasons when season-level targeting is active
            {
                var (_, tSeaClean, _) = EffectiveTagTargets(tagConfig);
                bool targetsSeason = tagConfig.EnableTag && !tagConfig.OnlyCollection && !_isBoxSetHse &&
                    ((tagConfig.SourceType == "MediaInfo" && TagConfigTargetsSeason(tagConfig)) ||
                     (tagConfig.SourceType != "MediaInfo" && tSeaClean));
                var matchedSeasonIds = new HashSet<Guid>();
                var allSeasonItemsMap = new Dictionary<Guid, BaseItem>();
                if (targetsSeason)
                {
                    if (tagConfig.SourceType == "MediaInfo")
                    {
                        var matchingEpisodes = _libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "Episode" }, Recursive = true, IsVirtualItem = false })
                            .Where(ep => ep.LocationType == LocationType.FileSystem
                                      && ItemMatchesMediaInfo(ep, tagConfig, debug, seriesEpisodeCache, personCache, userDataCache, null, preloadedUsers, seriesLastPlayedCache, collectionMembershipCache, seriesEpisodeNamesCache))
                            .ToList();
                        foreach (var season in ResolveParentSeasons(matchingEpisodes))
                        {
                            matchedSeasonIds.Add(season.Id);
                            allSeasonItemsMap.TryAdd(season.Id, season);
                        }
                    }
                    else
                    {
                        // Non-MediaInfo: seasons were already resolved into tagOutputItems
                        foreach (var season in tagOutputItems.Where(i => i.GetType().Name.Contains("Season")))
                        {
                            matchedSeasonIds.Add(season.Id);
                            allSeasonItemsMap.TryAdd(season.Id, season);
                        }
                    }
                }
                foreach (var s in _libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "Season" }, Tags = new[] { tagName }, Recursive = true, IsVirtualItem = false }))
                    allSeasonItemsMap.TryAdd(s.Id, s);
                foreach (var season in allSeasonItemsMap.Values)
                {
                    bool shouldHaveSeason = tagConfig.EnableTag && !tagConfig.OnlyCollection && matchedSeasonIds.Contains(season.Id);
                    bool hasTagSeason = new HashSet<string>(season.Tags, StringComparer.OrdinalIgnoreCase).Contains(tagName);
                    if (shouldHaveSeason == hasTagSeason) continue;
                    if (debug)
                    {
                        var _sYr = season.ProductionYear.HasValue ? $" ({season.ProductionYear})" : "";
                        string _sLabel = $"{season.Name}{_sYr}  [Season]";
                        if (shouldHaveSeason) { if (!_dbgTagAdded!.ContainsKey(tagName)) _dbgTagAdded[tagName] = new List<string>(); _dbgTagAdded[tagName].Add(_sLabel); }
                        else { if (!_dbgTagRemoved!.ContainsKey(tagName)) _dbgTagRemoved[tagName] = new List<string>(); _dbgTagRemoved[tagName].Add(_sLabel); }
                    }
                    if (!dryRun)
                    {
                        if (shouldHaveSeason) season.AddTag(tagName); else season.RemoveTag(tagName);
                        try { _libraryManager.UpdateItem(season, season.Parent, ItemUpdateType.MetadataEdit, null); }
                        catch (Exception ex) { _log.Warn($"Could not save tags for season '{season.Name}': {ex.Message}"); }
                        if (++updateCount % 25 == 0) await Task.Yield();
                    }
                    if (shouldHaveSeason) tagsAdded++; else tagsRemoved++;
                }
            }

            WriteTagDiffDebug(_dbgTagAdded, _dbgTagRemoved);
            if (gs.EnableTag && !gs.BoxSetHse)
                _log.Info(tagsAdded == 0 && tagsRemoved == 0
                    ? $"    No tag changes needed  ·  {RunLog.Elapsed(phaseTimer.Elapsed)}"
                    : dryRun
                        ? $"    Would add {tagsAdded} and remove {tagsRemoved} tags"
                        : $"    +{tagsAdded} added, -{tagsRemoved} removed  ·  {RunLog.Elapsed(phaseTimer.Elapsed)}");
            else if (gs.BoxSetHse)
                _log.Skip("The tag is applied to the collection itself, not to its items (see results)");
            else
                _log.Skip("Tagging is not enabled for this group");

            // Apply collection (scoped to this entry's collection only)
            int collResult = 0;
            bool _collCreated = false;
            int _collItemsAdded = 0, _collItemsRemoved = 0;
            if (tagConfig.EnableCollection)
            {
                _log.Blank();
                _log.Info("» Collections");
                phaseTimer.Restart();
                if (dryRun) _log.Skip("Dry run — collections are not changed");
                else if (collectionOutputItems.Count == 0) _log.Skip($"Collection \"{cName}\" left unchanged — no items matched");
            }
            if (tagConfig.EnableCollection && collectionOutputItems.Count > 0 && !dryRun)
            {
                try
                {
                    var desiredIds = collectionOutputItems.Select(i => i.InternalId).ToHashSet();
                    var existingColl = _libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "BoxSet" }, Name = cName, Recursive = true }).FirstOrDefault();
                    if (existingColl == null)
                    {
                        await _collectionManager.CreateCollection(new CollectionCreationOptions { Name = cName, IsLocked = false, ItemIdList = desiredIds.ToArray() });
                        collResult = 1;
                        _collCreated = true;
                        _collItemsAdded = desiredIds.Count;
                        _log.Debug($"  {cName}  →  created ({desiredIds.Count} items)");
                    }
                    else
                    {
                        // Top-list copies are managed by TopListCollectionMirror — never remove them here.
                        var currentMembers = _libraryManager.GetItemList(new InternalItemsQuery { CollectionIds = new[] { existingColl.InternalId }, Recursive = true, IsVirtualItem = false }).Where(i => !TopListCollectionMirror.IsTopListItem(i)).Select(i => i.InternalId).ToHashSet();
                        var toAdd = desiredIds.Where(id => !currentMembers.Contains(id)).ToList();
                        var toRemove = currentMembers.Where(id => !desiredIds.Contains(id)).ToList();
                        if (toAdd.Count > 0) await _collectionManager.AddToCollection(existingColl.InternalId, toAdd.ToArray());
                        if (toRemove.Count > 0 && existingColl is BoxSet boxSet) _collectionManager.RemoveFromCollection(boxSet, toRemove.ToArray());
                        collResult = toAdd.Count + toRemove.Count;
                        _collItemsAdded = toAdd.Count;
                        _collItemsRemoved = toRemove.Count;
                        if (debug)
                        {
                            _log.Debug(collResult == 0
                                ? $"  {cName}  →  up to date ({currentMembers.Count} items)"
                                : $"  {cName}  →  updated (+{toAdd.Count}, -{toRemove.Count})");
                            var _collMap = allItems.ToDictionary(i => i.InternalId, i => i.Name + (i.ProductionYear.HasValue ? $" ({i.ProductionYear})" : ""));
                            string CollLabel(long id) => _collMap.TryGetValue(id, out var _cn) ? _cn : id.ToString();
                            foreach (var id in toAdd) _log.Debug($"    + {CollLabel(id)}");
                            foreach (var id in toRemove) _log.Debug($"    - {CollLabel(id)}");
                        }
                    }
                    gs.CollectionCreated = _collCreated;
                    gs.CollectionItemsAdded = _collItemsAdded;
                    gs.CollectionItemsRemoved = _collItemsRemoved;
                    _log.Info(_collCreated
                        ? $"    Collection \"{cName}\" created with {RunLog.Plural(_collItemsAdded, "item")}  ·  {RunLog.Elapsed(phaseTimer.Elapsed)}"
                        : collResult == 0
                            ? $"    Collection \"{cName}\" is up to date  ·  {RunLog.Elapsed(phaseTimer.Elapsed)}"
                            : $"    Collection \"{cName}\" updated (+{_collItemsAdded}, -{_collItemsRemoved})  ·  {RunLog.Elapsed(phaseTimer.Elapsed)}");
                }
                catch (Exception ex)
                {
                    _log.Error($"Collection \"{cName}\" could not be updated: {ex.Message}");
                    WriteExceptionDebug(ex);
                    gs.Warnings.Add($"Collection could not be updated: {ex.Message}");
                    gs.TagsAdded = tagsAdded; gs.TagsRemoved = tagsRemoved;
                    WriteSingleRunFooter(gs, startTime, dryRun, logMissing);
                    return (true, $"{matchedLocalItems.Count} matched, {tagsAdded}↑ {tagsRemoved}↓ tags — collection error: {ex.Message}");
                }
            }

            if (tagConfig.EnablePlaylist)
            {
                _log.Blank();
                _log.Info("» Playlists");
                if (dryRun) _log.Skip("Dry run — playlists are not changed");
            }
            await SyncPlaylistsForEntryAsync(tagConfig, collectionOutputItems, dryRun, gs);

            if (!dryRun)
            {
                TagCacheManager.Instance.Save();
                // Add this tag to the history file so a future full run can clean it up
                // if the group is later deleted or disabled.
                if (!string.IsNullOrEmpty(tagName))
                {
                    var _hist = LoadFileHistory("homescreencompanion_history.txt");
                    if (!_hist.Any(t => string.Equals(t, tagName, StringComparison.OrdinalIgnoreCase)))
                    {
                        _hist.Add(tagName);
                        SaveFileHistory("homescreencompanion_history.txt", _hist);
                    }
                }

                Plugin.Instance.SaveConfiguration();
            }

            var _boxSetFound = ApplyTagToSourceBoxSet(tagConfig, tagName, dryRun, cancellationToken);

            // For BoxSet HSE groups, also tag the group's other flat entries (one per local source)
            // so the count matches the full-run behaviour.
            int _boxSetTaggedCount = _boxSetFound ? 1 : 0;
            if (_isBoxSetHse)
            {
                foreach (var sib in groupEntries.Where(t => t != tagConfig && IsBoxSetHomeSectionEntry(t)))
                    if (ApplyTagToSourceBoxSet(sib, tagName, dryRun, cancellationToken))
                        _boxSetTaggedCount++;
            }

            tagsRemoved += CleanupBoxSetTags(config, dryRun, cancellationToken);
            gs.BoxSetFound = _boxSetFound;
            gs.BoxSetTaggedCount = _boxSetTaggedCount;
            gs.TagsAdded = tagsAdded;
            gs.TagsRemoved = tagsRemoved;
            if (_isBoxSetHse && !_boxSetFound)
                gs.Warnings.Add($"Collection '{tagConfig.LocalSourceId}' was not found in the library");

            // Manage home sections for this entry
            if (tagConfig.EnableHomeSection)
            {
                _log.Blank();
                _log.Info("» Home sections");
                if (dryRun) _log.Skip("Dry run — home sections are not changed");
            }
            if (!dryRun && tagConfig.EnableHomeSection)
                ManageHomeSections(config, cancellationToken, debug, new List<GroupRunStats> { gs }, tagName);
            CleanupDisabledPlaylists(config, dryRun);
            SyncTopListFolders(config, dryRun);

            WriteSingleRunFooter(gs, startTime, dryRun, logMissing);

            List<string> parts;
            if (_isBoxSetHse)
            {
                parts = new List<string> { $"{_boxSetTaggedCount} collection{(_boxSetTaggedCount == 1 ? "" : "s")} tagged" };
            }
            else
            {
                parts = new List<string> { $"{matchedLocalItems.Count} matched" };
                if (tagsAdded > 0 || tagsRemoved > 0) parts.Add($"{tagsAdded}↑ {tagsRemoved}↓ tags");
            }
            if (collResult > 0) parts.Add("collection updated");
            if (dryRun) parts.Add("(dry run)");
            var summary = string.Join(", ", parts);
            return (true, summary);
        }

        // Identifies the UI group a flat TagConfig belongs to. The config page stores one flat entry
        // per URL / local source with the same Name + Tag, so several entries can share one key.
        private static string GroupKey(TagConfig t) =>
            (t.Name ?? "").Trim() + "\x1F" + (t.Tag ?? "").Trim();

        // Writes tag_ranks/<tag>.json — the IMDb ids of a tag's matched items in source order,
        // used by SyncTopListFolders to number .strm files.
        private void WriteRankFile(string tagName, List<string> imdbIds)
        {
            try
            {
                var rankDir = Path.Combine(Plugin.Instance.DataFolderPath, "tag_ranks");
                Directory.CreateDirectory(rankDir);
                var invalidChars = Path.GetInvalidFileNameChars();
                var rankSafe = new string((tagName ?? "unknown").Select(c => Array.IndexOf(invalidChars, c) >= 0 ? '_' : c).ToArray()).Trim('.');
                if (string.IsNullOrWhiteSpace(rankSafe)) rankSafe = "unknown";
                var rankFile = Path.Combine(rankDir, rankSafe + ".json");
                _jsonSerializer.SerializeToFile(imdbIds, rankFile);
            }
            catch { }
        }

        // Playlist sync for a single group — one individual playlist per user in PlaylistUserIds.
        // Shared by both the full sync (Execute) and the single-group run (RunSingleEntryInternalAsync)
        // so both paths create/update playlists identically. collectionOutputItems must be the union
        // of all sources in the group, since the sync removes anything not in the list.
        private async Task SyncPlaylistsForEntryAsync(TagConfig tagConfig, List<BaseItem> collectionOutputItems, bool dryRun, GroupRunStats? gs = null)
        {
            // Playlist sync — one individual playlist per user in PlaylistUserIds
            if (tagConfig.EnablePlaylist && !dryRun)
            {
                string _plLogName = string.IsNullOrWhiteSpace(tagConfig.PlaylistName) ? tagConfig.Name : tagConfig.PlaylistName;
                _log.Section($"Playlist \"{_plLogName}\"");
                try
                {
                    // Deduplicate — pick one physical version per logical movie (IMDb > TMDb > InternalId).
                    // Keep an ordered list mirroring the source order plus a set for fast membership checks.
                    var seenPlKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    var desiredPlIdList = new List<long>();   // ordered, mirrors source
                    var desiredPlIdSet = new HashSet<long>();  // fast Contains
                    foreach (var item in collectionOutputItems)
                    {
                        var key = item.GetProviderId("Imdb")
                               ?? item.GetProviderId("Tmdb")
                               ?? item.InternalId.ToString();
                        if (seenPlKeys.Add(key))
                        {
                            desiredPlIdList.Add(item.InternalId);
                            desiredPlIdSet.Add(item.InternalId);
                        }
                    }
                    _log.Debug($"  {desiredPlIdList.Count} unique items for {tagConfig.PlaylistUserIds.Count} users");
                    bool plMappingChanged = false;
                    foreach (var userId in tagConfig.PlaylistUserIds)
                    {
                        if (!Guid.TryParse(userId, out var userGuid)) continue;
                        var plUser = _userManager.GetUserById(userGuid);
                        if (plUser == null) { _log.Debug($"  User {userId} no longer exists — skipped"); continue; }

                        var plName = string.IsNullOrWhiteSpace(tagConfig.PlaylistName) ? tagConfig.Name : tagConfig.PlaylistName;

                        // 1. Try to find playlist by stored ID (most reliable)
                        var mapping = tagConfig.PlaylistMappings?.FirstOrDefault(m =>
                            string.Equals(m.UserId, userId, StringComparison.OrdinalIgnoreCase));

                        BaseItem? existingPlaylist = null;
                        if (mapping != null && Guid.TryParse(mapping.PlaylistId, out var storedGuid))
                        {
                            var candidate = _libraryManager.GetItemById(storedGuid);
                            if (candidate != null && candidate.GetType().Name.Contains("Playlist"))
                                existingPlaylist = candidate;
                        }

                        // 2. Name-based recovery: only when a mapping existed but the stored playlist is gone.
                        // Skipped when mapping == null (user has never had a playlist) to prevent
                        // accidentally claiming another user's same-named playlist.
                        if (existingPlaylist == null && mapping != null)
                        {
                            var claimedByOthers = (tagConfig.PlaylistMappings ?? new List<PlaylistMapping>())
                                .Where(m => !string.Equals(m.UserId, userId, StringComparison.OrdinalIgnoreCase)
                                         && Guid.TryParse(m.PlaylistId, out _))
                                .Select(m => Guid.Parse(m.PlaylistId))
                                .ToHashSet();

                            var plQuery = _libraryManager.QueryItems(new InternalItemsQuery
                            {
                                IncludeItemTypes = new[] { "Playlist" },
                                SearchTerm = plName,
                                Limit = 10
                            });
                            existingPlaylist = plQuery.Items.FirstOrDefault(p =>
                                p.Name.Equals(plName, StringComparison.OrdinalIgnoreCase)
                                && !claimedByOthers.Contains(p.Id));

                            if (existingPlaylist != null)
                            {
                                mapping.PlaylistId = existingPlaylist.Id.ToString();
                                plMappingChanged = true;
                                _log.Debug($"  {plUser.Name}: stored playlist id was stale — re-linked by name to {existingPlaylist.Id}");
                            }
                        }

                        if (existingPlaylist == null)
                        {
                            // 3. Create new playlist
                            await _playlistManager.CreatePlaylist(new PlaylistCreationRequest
                            {
                                Name = plName,
                                ItemIdList = desiredPlIdList.ToArray(),
                                User = plUser
                            });

                            if (mapping == null)
                            {
                                mapping = new PlaylistMapping { UserId = userId };
                                tagConfig.PlaylistMappings ??= new List<PlaylistMapping>();
                                tagConfig.PlaylistMappings.Add(mapping);
                            }

                            // CreatePlaylist may return Id as InternalId (long) rather than a Guid depending
                            // on Emby version — confirm the real Guid by querying back by name immediately.
                            // Exclude playlists already claimed by other users in this run.
                            var claimedAtCreate = (tagConfig.PlaylistMappings ?? new List<PlaylistMapping>())
                                .Where(m => !string.Equals(m.UserId, userId, StringComparison.OrdinalIgnoreCase)
                                         && Guid.TryParse(m.PlaylistId, out _))
                                .Select(m => Guid.Parse(m.PlaylistId))
                                .ToHashSet();

                            var confirmQuery = _libraryManager.QueryItems(new InternalItemsQuery
                            {
                                IncludeItemTypes = new[] { "Playlist" },
                                SearchTerm = plName,
                                Limit = 20
                            });
                            var newPl = confirmQuery.Items
                                .Where(p => p.Name.Equals(plName, StringComparison.OrdinalIgnoreCase)
                                         && !claimedAtCreate.Contains(p.Id))
                                .OrderByDescending(p => p.DateCreated)
                                .FirstOrDefault();

                            if (newPl != null)
                            {
                                mapping.PlaylistId = newPl.Id.ToString();
                                plMappingChanged = true;

                                // Transfer ownership so the playlist belongs to the target user, not admin.
                                try
                                {
                                    dynamic dynPl = newPl;
                                    bool ownerSet = false;
                                    try { dynPl.OwnerUserId = plUser.Id; ownerSet = true; } catch { }
                                    if (!ownerSet) try { dynPl.UserId = plUser.Id; ownerSet = true; } catch { }
                                    if (ownerSet)
                                        _libraryManager.UpdateItem(newPl, newPl.Parent, ItemUpdateType.MetadataEdit, null);
                                    if (gs != null) gs.PlaylistUsersCreated++;
                                    _log.Ok($"Playlist \"{plName}\": created for {plUser.Name} ({RunLog.Plural(desiredPlIdList.Count, "item")})");
                                    _log.Debug($"  {plUser.Name}: playlist id {newPl.Id}, owner {(ownerSet ? "set" : "not set")}");
                                }
                                catch (Exception ex)
                                {
                                    if (gs != null) gs.PlaylistUsersCreated++;
                                    _log.Warn($"Playlist \"{plName}\": created for {plUser.Name} but the owner could not be set: {ex.Message}");
                                }
                            }
                            else
                            {
                                if (gs != null) gs.PlaylistUsersFailed++;
                                _log.Warn($"Playlist \"{plName}\": created for {plUser.Name} but its id could not be confirmed — will retry on next sync");
                            }
                        }
                        else
                        {
                            // Full sync: always read actual playlist contents and diff against desired.
                            // This ensures additions and deletions from the source are always reflected,
                            // regardless of prior sync state or manual playlist edits.
                            var playlistItems = _libraryManager.GetItemList(new InternalItemsQuery { ListIds = new[] { existingPlaylist.InternalId } });

                            // Build map: inner media item InternalId -> playlist entry ID
                            var currentEntryMap = new Dictionary<long, long>();
                            foreach (var pItem in playlistItems)
                            {
                                long entryId = 0;
                                try { entryId = pItem.ListItemEntryId; } catch { }
                                if (entryId == 0) entryId = pItem.InternalId;

                                long innerItemId = pItem.InternalId;
                                if (pItem.GetType().Name.Contains("PlaylistItem"))
                                {
                                    try { var temp = ((dynamic)pItem).Item; if (temp != null) innerItemId = ((BaseItem)temp).InternalId; } catch { }
                                }

                                currentEntryMap.TryAdd(innerItemId, entryId);
                            }

                            var currentItemIds = currentEntryMap.Keys.ToHashSet();

                            var entryIdsToRemove = currentItemIds
                                .Where(id => !desiredPlIdSet.Contains(id))
                                .Select(id => currentEntryMap[id])
                                .ToList();

                            var toAdd = desiredPlIdList.Where(id => !currentItemIds.Contains(id)).ToArray();

                            if (entryIdsToRemove.Count > 0)
                            {
                                try {
                                    await _playlistManager.RemoveFromPlaylist(existingPlaylist.InternalId, entryIdsToRemove.ToArray());
                                } catch (Exception ex) {
                                    _log.Warn($"Playlist \"{plName}\": could not remove {entryIdsToRemove.Count} items for {plUser.Name}: {ex.Message}");
                                }
                            }

                            if (toAdd.Length > 0)
                            {
                                _playlistManager.AddToPlaylist(existingPlaylist.InternalId, toAdd, plUser);
                            }

                            // Reorder the playlist so it mirrors the source order. AddToPlaylist appends new
                            // items at the end, so re-read the actual contents (to pick up entry IDs of the
                            // items just added) and move each item into its source position. Only issue a
                            // MoveItem when an item is actually out of place — no needless writes when the
                            // order already matches.
                            var afterItems = _libraryManager.GetItemList(
                                new InternalItemsQuery { ListIds = new[] { existingPlaylist.InternalId } });

                            var entryByInner = new Dictionary<long, long>();   // inner media item id -> playlist entry id
                            var currentOrder = new List<long>();               // inner media item id in current order
                            foreach (var pItem in afterItems)
                            {
                                long entryId = 0;
                                try { entryId = pItem.ListItemEntryId; } catch { }
                                if (entryId == 0) entryId = pItem.InternalId;

                                long innerItemId = pItem.InternalId;
                                if (pItem.GetType().Name.Contains("PlaylistItem"))
                                {
                                    try { var temp = ((dynamic)pItem).Item; if (temp != null) innerItemId = ((BaseItem)temp).InternalId; } catch { }
                                }
                                if (entryByInner.TryAdd(innerItemId, entryId))
                                    currentOrder.Add(innerItemId);
                            }

                            // Target order = source order, restricted to items actually present in the playlist.
                            var desiredOrder = desiredPlIdList.Where(entryByInner.ContainsKey).ToList();

                            bool reordered = false;
                            for (int targetIndex = 0; targetIndex < desiredOrder.Count; targetIndex++)
                            {
                                long wanted = desiredOrder[targetIndex];
                                if (currentOrder[targetIndex] == wanted) continue;   // already in place

                                try
                                {
                                    await _playlistManager.MoveItem(
                                        existingPlaylist.InternalId, entryByInner[wanted], targetIndex);

                                    // Mirror the same move locally so our model matches the server.
                                    currentOrder.Remove(wanted);
                                    currentOrder.Insert(targetIndex, wanted);
                                    reordered = true;
                                }
                                catch (Exception ex)
                                {
                                    _log.Warn($"Playlist \"{plName}\": could not reorder for {plUser.Name}: {ex.Message}");
                                    break;
                                }
                            }

                            if (entryIdsToRemove.Count > 0 || toAdd.Length > 0 || reordered)
                            {
                                plMappingChanged = true;
                                if (gs != null) gs.PlaylistUsersUpdated++;
                                var _plParts = new List<string>();
                                if (toAdd.Length > 0) _plParts.Add($"+{toAdd.Length}");
                                if (entryIdsToRemove.Count > 0) _plParts.Add($"-{entryIdsToRemove.Count}");
                                if (reordered) _plParts.Add("reordered");
                                _log.Ok($"Playlist \"{plName}\": updated for {plUser.Name} ({string.Join(", ", _plParts)})");
                            }
                            else
                            {
                                _log.Debug($"  {plUser.Name}: up to date ({currentOrder.Count} items)");
                            }
                        }
                    }
                    if (plMappingChanged)
                        Plugin.Instance?.SaveConfiguration();
                }
                catch (Exception ex)
                {
                    if (gs != null) { gs.PlaylistUsersFailed++; gs.Warnings.Add($"Playlist sync failed: {ex.Message}"); }
                    _log.Error($"Playlist \"{_plLogName}\" could not be synced: {ex.Message}");
                    WriteExceptionDebug(ex);
                }
            }
        }

        private void ManageHomeSections(PluginConfiguration config, CancellationToken cancellationToken, bool debug = false, List<GroupRunStats>? statsList = null, string? filterTagName = null)
        {
            bool configChanged = false;
            var processedHsKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var tc in config.Tags)
            {
                // When running single-entry, only process the specific tag
                if (filterTagName != null && !string.Equals(tc.Tag?.Trim(), filterTagName, StringComparison.OrdinalIgnoreCase))
                    continue;

                bool isActive = tc.Active && IsScheduleActive(tc.ActiveIntervals);

                if (tc.HomeSectionTracked == null)
                    tc.HomeSectionTracked = new List<HomeSectionTracking>();

                var safeTag = string.Concat((tc.Tag ?? tc.Name ?? "").Take(40)
                    .Select(c => char.IsLetterOrDigit(c) || c == '-' || c == '_' ? c : '_'));
                var sectionMarker = "hsc__" + safeTag;

                string _hsTagName = (tc.Tag ?? "").Trim();
                string _hsDisplayName = !string.IsNullOrWhiteSpace(tc.Name) ? $"{tc.Name} [{_hsTagName}]" : _hsTagName;

                // Deduplicate flat entries that share the same group (Name + Tag).
                // Must happen before the inactive check so that duplicate flat entries
                // for an inactive group don't each log a separate "home section removed".
                var hsKey = GroupKey(tc);
                bool isFirstForGroup = processedHsKeys.Add(hsKey);

                if (!tc.EnableHomeSection || !isActive)
                {
                    if (tc.HomeSectionTracked.Count > 0)
                    {
                        if (isFirstForGroup)
                        {
                            // First flat entry for this group — do the actual removal and log it.
                            int _removedHs = 0;
                            foreach (var tracking in tc.HomeSectionTracked)
                            {
                                try
                                {
                                    var uid = _userManager.GetInternalId(tracking.UserId);
                                    DeleteSectionForUser(uid, tracking.SectionId, sectionMarker, tc.HomeSectionSettings, cancellationToken);
                                    _removedHs++;
                                }
                                catch (Exception ex)
                                {
                                    _log.Warn($"{_hsDisplayName}: home section could not be removed: {ex.Message}");
                                }
                            }
                            if (_removedHs > 0)
                            {
                                var _gsR = statsList?.FirstOrDefault(s => s.TagName != null && string.Equals(s.TagName, _hsTagName, StringComparison.OrdinalIgnoreCase));
                                if (_gsR != null) _gsR.HomeSectionRemoved = true;
                                _log.Skip($"{_hsDisplayName}: home section removed for {RunLog.Plural(_removedHs, "user")} (group is {(tc.EnableHomeSection ? "inactive or not in schedule" : "no longer set to show a home section")})");
                            }
                        }
                        // Always clear tracking (including duplicate flat entries) so this
                        // doesn't repeat on subsequent runs.
                        tc.HomeSectionTracked.Clear();
                        configChanged = true;
                    }
                    continue;
                }

                if (!isFirstForGroup)
                {
                    if (tc.HomeSectionTracked.Count > 0) { tc.HomeSectionTracked.Clear(); configChanged = true; }
                    continue;
                }

                Dictionary<string, string> settingsDict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                try
                {
                    if (!string.IsNullOrEmpty(tc.HomeSectionSettings) && tc.HomeSectionSettings != "{}")
                        settingsDict = _jsonSerializer.DeserializeFromString<Dictionary<string, string>>(tc.HomeSectionSettings) ?? settingsDict;
                }
                catch { /* ignore malformed settings */ }

                if (!settingsDict.ContainsKey("SectionType"))
                    settingsDict["SectionType"] = (tc.EnableCollection && !string.IsNullOrEmpty(tc.CollectionName)) ? "boxset" : "items";

                // Back-fill CustomName from group name/tag when not explicitly configured
                if (!settingsDict.TryGetValue("CustomName", out var _existingCn) || string.IsNullOrWhiteSpace(_existingCn))
                {
                    var _defaultCn = !string.IsNullOrWhiteSpace(tc.Name) ? tc.Name : tc.Tag;
                    if (!string.IsNullOrWhiteSpace(_defaultCn))
                        settingsDict["CustomName"] = _defaultCn;
                }

                settingsDict.TryGetValue("SectionType", out var sectionType);

                // For items-type sections, dynamically ensure all top-list libraries are excluded
                if (sectionType == "items")
                {
                    var topListLibIds = (config?.TopLists ?? new System.Collections.Generic.List<TopListHomeSection>())
                        .Where(t => !string.IsNullOrEmpty(t.HomeSectionLibraryId) && t.HomeSectionLibraryId != "auto")
                        .Select(t => t.HomeSectionLibraryId)
                        .ToList();
                    if (topListLibIds.Count > 0)
                    {
                        var current = (settingsDict.TryGetValue("_queryExcludeViewIds", out var ev) ? ev : "")
                            .Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries)
                            .Select(s => s.Trim())
                            .ToList();
                        bool excChanged = false;
                        foreach (var id in topListLibIds)
                            if (!current.Contains(id, StringComparer.OrdinalIgnoreCase))
                            { current.Add(id); excChanged = true; }
                        if (excChanged)
                        {
                            var excStr = string.Join(",", current);
                            settingsDict["_queryExcludeViewIds"] = excStr;
                            settingsDict["ExcludedFolders"] = excStr;
                        }
                    }
                }

                string resolvedLibraryId = null;
                if (sectionType == "boxset")
                {
                    if (tc.HomeSectionLibraryId == "auto")
                    {
                        if (tc.EnableCollection && !string.IsNullOrEmpty(tc.CollectionName))
                        {
                            var coll = _libraryManager.GetItemList(new InternalItemsQuery
                            {
                                IncludeItemTypes = new[] { "BoxSet" },
                                Name = tc.CollectionName,
                                Recursive = true
                            }).FirstOrDefault();
                            if (coll != null)
                                resolvedLibraryId = coll.InternalId.ToString();
                            else
                                HsWarn(statsList, _hsTagName, _hsDisplayName, $"collection '{tc.CollectionName}' was not found, so the home section could not be created");
                        }
                    }
                    else if (!string.IsNullOrEmpty(tc.HomeSectionLibraryId))
                    {
                        resolvedLibraryId = tc.HomeSectionLibraryId;
                    }

                    if (string.IsNullOrEmpty(resolvedLibraryId))
                    {
                        HsWarn(statsList, _hsTagName, _hsDisplayName, "home section skipped — no collection to show");
                        continue;
                    }
                }

                if (sectionType == "items" && !string.IsNullOrEmpty(tc.Tag))
                {
                    var tagItem = _libraryManager.GetItemList(new InternalItemsQuery
                    {
                        IncludeItemTypes = new[] { "Tag" },
                        Name = tc.Tag,
                        Recursive = true
                    }).FirstOrDefault();
                    if (tagItem != null)
                        settingsDict["_queryTagId"] = tagItem.InternalId.ToString();
                    else
                        HsWarn(statsList, _hsTagName, _hsDisplayName, $"tag '{tc.Tag}' does not exist in the library yet, so the home section may be empty");
                }

                var removedUsers = tc.HomeSectionTracked.Where(t => !tc.HomeSectionUserIds.Contains(t.UserId)).ToList();
                foreach (var t in removedUsers)
                {
                    try
                    {
                        var uid = _userManager.GetInternalId(t.UserId);
                        DeleteSectionForUser(uid, t.SectionId, sectionMarker, tc.HomeSectionSettings, cancellationToken);
                    }
                    catch { }
                    tc.HomeSectionTracked.Remove(t);
                    configChanged = true;
                }

                int _hsSynced = 0;
                if (tc.HomeSectionUserIds.Count > 0)
                    _log.Section($"Home section: {_hsDisplayName}");
                foreach (var userId in tc.HomeSectionUserIds)
                {
                    string _hsAction = "created";
                    try
                    {
                        var userInternalId = _userManager.GetInternalId(userId);

                        var tracked = tc.HomeSectionTracked.FirstOrDefault(t => t.UserId == userId);

                        string trackId = sectionMarker;

                        // Hämta alla sektioner en gång — återanvänds för både ID-sökning och markör-fallback
                        var currentSections = _userManager.GetHomeSections(userInternalId, cancellationToken);
                        var allCurrentSections = currentSections?.Sections ?? Array.Empty<ContentSection>();

                        // Hitta vår sektion: 1) via spårat ID, 2) via CustomName-fallback (skyddar mot att Emby tilldelar nytt ID vid omordning)
                        ContentSection? ownedSection = null;
                        if (tracked != null && !string.IsNullOrEmpty(tracked.SectionId) && !tracked.SectionId.StartsWith("hsc__"))
                            ownedSection = allCurrentSections.FirstOrDefault(s => s.Id == tracked.SectionId);
                        if (ownedSection == null && settingsDict.TryGetValue("CustomName", out var _hsFallbackName) && !string.IsNullOrEmpty(_hsFallbackName))
                            ownedSection = allCurrentSections.FirstOrDefault(s => string.Equals(s.CustomName, _hsFallbackName, StringComparison.OrdinalIgnoreCase));

                        if (ownedSection != null)
                        {
                            try
                            {
                                // Hämta befintlig sektion som bas — plugin-inställningar appliceras ovanpå utan att nollställa Emby-egna värden
                                var updateSection = BuildContentSection(_jsonSerializer, settingsDict, resolvedLibraryId, ownedSection);
                                typeof(ContentSection).GetProperty("Id")?.SetValue(updateSection, ownedSection.Id);
                                _userManager.UpdateHomeSection(userInternalId, updateSection, cancellationToken);
                                trackId = ownedSection.Id ?? sectionMarker;
                                _hsAction = "updated";
                                goto _hsSectionDone;
                            }
                            catch
                            {
                                // Uppdatering misslyckades — fortsätt till skapande
                            }
                        }

                        // Sektion finns inte — skapa ny
                        {
                            var beforeIds = new HashSet<string>(
                                allCurrentSections.Where(s => !string.IsNullOrEmpty(s.Id)).Select(s => s.Id));
                            _userManager.AddHomeSection(userInternalId, BuildContentSection(_jsonSerializer, settingsDict, resolvedLibraryId), cancellationToken);
                            var afterSections = _userManager.GetHomeSections(userInternalId, cancellationToken);
                            var newId = (afterSections?.Sections ?? Array.Empty<ContentSection>())
                                .Where(s => !string.IsNullOrEmpty(s.Id) && !beforeIds.Contains(s.Id))
                                .Select(s => s.Id).FirstOrDefault() ?? "";
                            trackId = !string.IsNullOrEmpty(newId) ? newId : sectionMarker;
                        }

                        _hsSectionDone:

                        if (tracked != null)
                            tracked.SectionId = trackId;
                        else
                            tc.HomeSectionTracked.Add(new HomeSectionTracking { UserId = userId, SectionId = trackId });

                        configChanged = true;
                        _hsSynced++;
                        if (debug)
                        {
                            string _hsUserName = Guid.TryParse(userId, out var _hsGuid)
                                ? (_userManager.GetUserById(_hsGuid)?.Name ?? userId)
                                : userId;
                            _log.Debug($"  {_hsUserName}: {_hsAction} (section id {trackId})");
                        }
                    }
                    catch (Exception ex)
                    {
                        string _hsUserName2 = Guid.TryParse(userId, out var _hsGuid2)
                            ? (_userManager.GetUserById(_hsGuid2)?.Name ?? userId)
                            : userId;
                        HsWarn(statsList, _hsTagName, _hsDisplayName, $"home section failed for {_hsUserName2}: {ex.Message}");
                    }
                }
                if (_hsSynced > 0)
                {
                    var _gsS = statsList?.FirstOrDefault(s => s.TagName != null && string.Equals(s.TagName, _hsTagName, StringComparison.OrdinalIgnoreCase));
                    if (_gsS != null) { _gsS.HomeSectionSynced = true; _gsS.HomeSectionUserCount = _hsSynced; }
                    _log.Ok($"{_hsDisplayName}: home section synced for {RunLog.Plural(_hsSynced, "user")}");
                }
                else if (tc.HomeSectionUserIds.Count == 0)
                {
                    HsWarn(statsList, _hsTagName, _hsDisplayName, "home section enabled but no users are selected");
                }
            }

            if (configChanged)
                Plugin.Instance.SaveConfiguration();
        }

        private void DeleteSectionForUser(long userInternalId, string sectionId, string sectionMarker, string settingsJson, CancellationToken cancellationToken)
        {
            if (!string.IsNullOrEmpty(sectionId) && !sectionId.StartsWith("hsc__"))
            {
                _userManager.DeleteHomeSections(userInternalId, new[] { sectionId }, cancellationToken);
                return;
            }

            ContentSection[] allSections;
            try { allSections = _userManager.GetHomeSections(userInternalId, cancellationToken)?.Sections ?? Array.Empty<ContentSection>(); }
            catch { return; }

            var marker = (!string.IsNullOrEmpty(sectionId) && sectionId.StartsWith("hsc__")) ? sectionId : sectionMarker;
            if (!string.IsNullOrEmpty(marker))
            {
                var markerIds = allSections
                    .Where(s => s.Subtitle == marker && !string.IsNullOrEmpty(s.Id))
                    .Select(s => s.Id).ToArray();
                if (markerIds.Length > 0)
                {
                    _userManager.DeleteHomeSections(userInternalId, markerIds, cancellationToken);
                    return;
                }
            }

            try
            {
                var hint = _jsonSerializer.DeserializeFromString<Dictionary<string, string>>(settingsJson ?? "{}");
                if (hint != null && hint.TryGetValue("CustomName", out var cn) && !string.IsNullOrEmpty(cn))
                {
                    var fallbackIds = allSections
                        .Where(s => s.CustomName == cn && !string.IsNullOrEmpty(s.Id))
                        .Select(s => s.Id).ToArray();
                    if (fallbackIds.Length > 0)
                        _userManager.DeleteHomeSections(userInternalId, fallbackIds, cancellationToken);
                }
            }
            catch { }
        }

        // Returns true when this entry is a LocalCollection home section with media type = Collections.
        // Used to decide that only the BoxSet itself (not its items) should receive the tag.
        // Tags this plugin writes onto items (some entry of the group has tagging on). A group's
        // Tag: criteria must not see its own output tag, or an item that got it once keeps
        // matching its own rule forever (e.g. tag "zzrow-musicals" + Tag:contains:Musicals).
        private HashSet<string> _writtenTags = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        private HashSet<string> BuildWrittenTags(PluginConfiguration config) => new HashSet<string>(
            (config.Tags ?? new List<TagConfig>())
                .Where(t => !string.IsNullOrWhiteSpace(t.Tag) && t.EnableTag && !t.OnlyCollection && !IsBoxSetHomeSectionEntry(t))
                .Select(t => t.Tag.Trim()),
            StringComparer.OrdinalIgnoreCase);

        private bool IsBoxSetHomeSectionEntry(TagConfig tc)
        {
            if (!tc.EnableHomeSection || tc.SourceType != "LocalCollection" || string.IsNullOrEmpty(tc.LocalSourceId)) return false;
            var sd = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            try { sd = _jsonSerializer.DeserializeFromString<Dictionary<string, string>>(tc.HomeSectionSettings ?? "{}") ?? sd; } catch { return false; }
            if (!sd.TryGetValue("ItemTypes", out var itJson) || string.IsNullOrEmpty(itJson)) return false;
            string[] it;
            try { it = _jsonSerializer.DeserializeFromString<string[]>(itJson) ?? Array.Empty<string>(); }
            catch { it = itJson.Split(',').Select(s => s.Trim()).Where(s => s.Length > 0).ToArray(); }
            return it.Any(t => string.Equals(t, "BoxSet", StringComparison.OrdinalIgnoreCase));
        }

        // Returns true if the target BoxSet was found (and tagged if needed), false if not found.
        private bool ApplyTagToSourceBoxSet(TagConfig tc, string tagName, bool dryRun, CancellationToken cancellationToken)
        {
            // Only ADDS the tag to the target BoxSet. Removal of stale tags is handled by CleanupBoxSetTags
            // after all entries have run, so multiple entries sharing the same tag don't undo each other.
            if (!IsBoxSetHomeSectionEntry(tc) || string.IsNullOrEmpty(tagName)) return false;

            var allBoxSets = _libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "BoxSet" }, Recursive = true });
            var target = allBoxSets.FirstOrDefault(b => string.Equals(b.Name, tc.LocalSourceId, StringComparison.OrdinalIgnoreCase));
            if (target == null) { _log.Debug($"  Collection '{tc.LocalSourceId}' not found in library — cannot tag it"); return false; }

            var hasTag = (target.Tags ?? Array.Empty<string>()).Any(t => string.Equals(t, tagName, StringComparison.OrdinalIgnoreCase));
            if (!hasTag)
            {
                target.AddTag(tagName);
                if (!dryRun)
                {
                    try { _libraryManager.UpdateItem(target, target.Parent, ItemUpdateType.MetadataEdit, null); }
                    catch (Exception ex) { _log.Warn($"Could not save tag on collection '{target.Name}': {ex.Message}"); }
                }
                _log.Debug($"  Collection '{target.Name}' {(dryRun ? "would be tagged" : "tagged")} with '{tagName}'");
            }
            else
            {
                _log.Debug($"  Collection '{target.Name}' already has tag '{tagName}'");
            }
            return true;
        }

        private void CleanupDisabledPlaylists(PluginConfiguration config, bool dryRun)
        {
            bool configChanged = false;
            foreach (var tc in config.Tags ?? new List<TagConfig>())
            {
                if (tc.PlaylistMappings == null || tc.PlaylistMappings.Count == 0) continue;

                bool isGroupActive = tc.Active && IsScheduleActive(tc.ActiveIntervals);

                if (!tc.EnablePlaylist || !isGroupActive)
                {
                    // Delete all playlists for this group (disabled or inactive)
                    foreach (var mapping in tc.PlaylistMappings.ToList())
                    {
                        if (!string.IsNullOrEmpty(mapping.PlaylistId) && Guid.TryParse(mapping.PlaylistId, out var guid))
                        {
                            var pl = _libraryManager.GetItemById(guid);
                            if (pl != null && !dryRun)
                            {
                                try { _libraryManager.DeleteItem(pl, new DeleteOptions { DeleteFileLocation = false }); }
                                catch (Exception ex) { _log.Warn($"Playlist '{pl.Name}' could not be removed: {ex.Message}"); }
                            }
                        }
                    }
                    if (!dryRun) tc.PlaylistMappings.Clear();
                    configChanged = true;
                    _log.Skip($"Playlists for '{tc.Name ?? tc.Tag}' {(dryRun ? "would be removed" : "removed")} (group is {(tc.EnablePlaylist ? "inactive or not in schedule" : "no longer set to create playlists")})");
                }
                else
                {
                    // Delete playlists for users that have been unchecked from PlaylistUserIds
                    var activeUserIds = new HashSet<string>(tc.PlaylistUserIds ?? new List<string>(), StringComparer.OrdinalIgnoreCase);
                    var orphans = tc.PlaylistMappings.Where(m => !activeUserIds.Contains(m.UserId)).ToList();
                    foreach (var orphan in orphans)
                    {
                        if (!string.IsNullOrEmpty(orphan.PlaylistId) && Guid.TryParse(orphan.PlaylistId, out var guid))
                        {
                            var pl = _libraryManager.GetItemById(guid);
                            if (pl != null && !dryRun)
                            {
                                try { _libraryManager.DeleteItem(pl, new DeleteOptions { DeleteFileLocation = false }); }
                                catch (Exception ex) { _log.Warn($"Playlist '{pl.Name}' could not be removed: {ex.Message}"); }
                            }
                        }
                        if (!dryRun) tc.PlaylistMappings.Remove(orphan);
                        configChanged = true;
                        string _orphanUser = Guid.TryParse(orphan.UserId, out var _orphanGuid) ? (_userManager.GetUserById(_orphanGuid)?.Name ?? orphan.UserId) : orphan.UserId;
                        _log.Skip($"Playlist for '{tc.Name ?? tc.Tag}' {(dryRun ? "would be removed" : "removed")} for {_orphanUser} (user no longer selected)");
                    }
                }
            }
            if (configChanged && !dryRun)
                Plugin.Instance?.SaveConfiguration();
        }

        // Runs once per full/single run — removes the BoxSet tag from any BoxSet that is no longer a target,
        // and adds it to any that should have it. Handles multiple entries sharing the same tag correctly.
        private int CleanupBoxSetTags(PluginConfiguration config, bool dryRun, CancellationToken cancellationToken)
        {
            // activeTags: for each tag, which BoxSet names should currently have it
            var activeTags = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
            // allHseTags: every tag managed by any BoxSet-HSE entry (to know what to clean up)
            var allHseTags = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var tc in config.Tags)
            {
                if (!IsBoxSetHomeSectionEntry(tc) || string.IsNullOrEmpty(tc.Tag)) continue;
                allHseTags.Add(tc.Tag);
                if (tc.Active && !string.IsNullOrEmpty(tc.LocalSourceId))
                {
                    if (!activeTags.ContainsKey(tc.Tag))
                        activeTags[tc.Tag] = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    activeTags[tc.Tag].Add(tc.LocalSourceId);
                }
            }

            if (allHseTags.Count == 0) return 0;

            int removed = 0;
            var allBoxSets = _libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "BoxSet" }, Recursive = true });
            foreach (var boxSet in allBoxSets)
            {
                bool updated = false;
                foreach (var tagName in allHseTags)
                {
                    var hasTag    = (boxSet.Tags ?? Array.Empty<string>()).Any(t => string.Equals(t, tagName, StringComparison.OrdinalIgnoreCase));
                    activeTags.TryGetValue(tagName, out var targets);
                    var shouldHave = targets != null && targets.Any(n => string.Equals(n, boxSet.Name, StringComparison.OrdinalIgnoreCase));
                    if (shouldHave == hasTag) continue;
                    if (shouldHave) boxSet.AddTag(tagName); else { boxSet.RemoveTag(tagName); removed++; }
                    updated = true;
                }
                if (updated && !dryRun)
                {
                    try { _libraryManager.UpdateItem(boxSet, boxSet.Parent, ItemUpdateType.MetadataEdit, null); }
                    catch (Exception ex) { _log.Warn($"Could not update tags on collection '{boxSet.Name}': {ex.Message}"); }
                }
            }
            return removed;
        }

        internal static int UpdateUntrackedSections(
            IJsonSerializer jsonSerializer,
            IUserManager userManager,
            PluginConfiguration config,
            IEnumerable<string> libraryIdsToExclude,
            CancellationToken cancellationToken)
        {
            var allTrackedIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var tag in config.Tags ?? new List<TagConfig>())
                foreach (var tr in tag.HomeSectionTracked ?? new List<HomeSectionTracking>())
                    if (!string.IsNullOrEmpty(tr.SectionId)) allTrackedIds.Add(tr.SectionId);
            foreach (var topList in config.TopLists ?? new List<TopListHomeSection>())
                foreach (var tr in topList.HomeSectionTracked ?? new List<HomeSectionTracking>())
                    if (!string.IsNullOrEmpty(tr.SectionId)) allTrackedIds.Add(tr.SectionId);

            var managedUserIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var tag in config.Tags ?? new List<TagConfig>())
                foreach (var uid in tag.HomeSectionUserIds ?? new List<string>())
                    managedUserIds.Add(uid);
            foreach (var topList in config.TopLists ?? new List<TopListHomeSection>())
                foreach (var uid in topList.HomeSectionUserIds ?? new List<string>())
                    managedUserIds.Add(uid);

            var libIds = libraryIdsToExclude
                .Select(s => s.Trim().ToLowerInvariant()).Where(s => s.Length > 0)
                .Distinct().ToList();
            if (libIds.Count == 0) return 0;

            var parentIdProp  = typeof(ContentSection).GetProperty("ParentId");
            var exFoldersProp = typeof(ContentSection).GetProperty("ExcludedFolders");
            var queryPropInfo = typeof(ContentSection).GetProperty("Query");
            int updated = 0;

            foreach (var userId in managedUserIds)
            {
                try
                {
                    var uid = userManager.GetInternalId(userId);
                    var allSecs = userManager.GetHomeSections(uid, cancellationToken)?.Sections
                        ?? Array.Empty<ContentSection>();

                    foreach (var sec in allSecs)
                    {
                        if (string.IsNullOrEmpty(sec.Id)) continue;
                        if (allTrackedIds.Contains(sec.Id)) continue;

                        // Skip library-scoped sections — they already filter to one library
                        var parentId = parentIdProp?.GetValue(sec) as string;
                        if (!string.IsNullOrEmpty(parentId)) continue;

                        // Collect current exclusions from ExcludedFolders and Query.ExcludeUserViewIds
                        var existingExcluded = ((exFoldersProp?.GetValue(sec) as string[]) ?? Array.Empty<string>())
                            .Select(s => s.Trim().ToLowerInvariant()).Where(s => s.Length > 0).ToList();
                        try
                        {
                            var query = queryPropInfo?.GetValue(sec);
                            if (query != null)
                            {
                                var excProp = query.GetType().GetProperty("ExcludeUserViewIds");
                                var viewIds = excProp?.GetValue(query) as string[];
                                if (viewIds != null)
                                    existingExcluded.AddRange(
                                        viewIds.Select(s => s.Trim().ToLowerInvariant()).Where(s => s.Length > 0));
                            }
                        }
                        catch { }

                        existingExcluded = existingExcluded.Distinct().ToList();
                        var missing = libIds
                            .Where(id => !existingExcluded.Contains(id, StringComparer.OrdinalIgnoreCase))
                            .ToList();
                        if (missing.Count == 0) continue;

                        existingExcluded.AddRange(missing);
                        var newExcluded = existingExcluded.ToArray();

                        exFoldersProp?.SetValue(sec, newExcluded);
                        try
                        {
                            var query = queryPropInfo?.GetValue(sec);
                            if (query != null)
                            {
                                var excViewProp = query.GetType().GetProperty("ExcludeUserViewIds");
                                if (excViewProp?.CanWrite == true)
                                {
                                    if (excViewProp.PropertyType == typeof(string[]))
                                        excViewProp.SetValue(query, newExcluded);
                                    else if (excViewProp.PropertyType == typeof(Guid[]))
                                        excViewProp.SetValue(query, newExcluded
                                            .Select(id => Guid.TryParse(id, out var g) ? g : Guid.Empty).ToArray());
                                }
                            }
                        }
                        catch { }
                        userManager.UpdateHomeSection(uid, sec, cancellationToken);
                        updated++;
                    }
                }
                catch { }
            }
            return updated;
        }

        internal static ContentSection BuildContentSection(IJsonSerializer jsonSerializer, Dictionary<string, string> settings, string libraryId, ContentSection existing = null)
        {
            var section = existing ?? new ContentSection();
            var props = typeof(ContentSection).GetProperties(BindingFlags.Public | BindingFlags.Instance);

            foreach (var prop in props)
            {
                if (!prop.CanWrite || prop.Name == "Id" || prop.Name == "ParentId") continue;
                if (!settings.TryGetValue(prop.Name, out var strVal)) continue;
                // Tomt värde → rensa egenskapen (nullable → null, string → null)
                if (string.IsNullOrEmpty(strVal))
                {
                    if (Nullable.GetUnderlyingType(prop.PropertyType) != null || prop.PropertyType == typeof(string))
                        prop.SetValue(section, null);
                    continue;
                }
                try
                {
                    var t = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
                    object converted = null;
                    if (t == typeof(string)) converted = strVal;
                    else if (t == typeof(bool)) converted = bool.Parse(strVal);
                    else if (t == typeof(int)) converted = int.Parse(strVal);
                    else if (t == typeof(long)) converted = long.Parse(strVal);
                    else if (t == typeof(DateTime)) converted = DateTime.Parse(strVal);
                    else if (t.IsEnum) { try { converted = Enum.Parse(t, strVal, true); } catch { } }
                    if (converted != null)
                        prop.SetValue(section, converted);
                }
                catch { /* skip malformed value */ }
            }

            foreach (var prop in props)
            {
                if (!prop.CanWrite || prop.Name == "Id") continue;
                if (prop.PropertyType != typeof(string[])) continue;
                if (!settings.TryGetValue(prop.Name, out var arrVal) || string.IsNullOrEmpty(arrVal)) continue;
                try
                {
                    var values = arrVal.TrimStart().StartsWith("[")
                        ? jsonSerializer.DeserializeFromString<string[]>(arrVal)
                        : arrVal.Split(',').Select(s => s.Trim()).Where(s => s.Length > 0).ToArray();
                    prop.SetValue(section, values);
                }
                catch { }
            }

            var queryProp = props.FirstOrDefault(p => p.Name == "Query");
            if (queryProp != null)
            {
                try
                {
                    // Använd ExtendedItemsQuery för att exponera IsPlayed till Embys JSON-serialisering
                    var extQuery = new ExtendedItemsQuery();
                    var queryProps = typeof(ItemsQuery).GetProperties(BindingFlags.Public | BindingFlags.Instance);

                    // Specialfall: _queryTagId → TagIds[]
                    if (settings.TryGetValue("_queryTagId", out var qTagId) && !string.IsNullOrEmpty(qTagId))
                    {
                        var tagIdsProp = queryProps.FirstOrDefault(p => p.Name == "TagIds");
                        if (tagIdsProp != null && tagIdsProp.CanWrite && tagIdsProp.PropertyType == typeof(string[]))
                            tagIdsProp.SetValue(extQuery, new[] { qTagId });
                    }

                    // Specialfall: _queryExcludeViewIds → ExcludeUserViewIds[]
                    if (settings.TryGetValue("_queryExcludeViewIds", out var qExcludeViewIds) && !string.IsNullOrWhiteSpace(qExcludeViewIds))
                    {
                        var excludeProp = queryProps.FirstOrDefault(p => p.Name == "ExcludeUserViewIds");
                        if (excludeProp != null && excludeProp.CanWrite)
                        {
                            var ids = qExcludeViewIds.Split(',')
                                .Select(s => s.Trim()).Where(s => s.Length > 0).ToArray();
                            if (ids.Length > 0)
                            {
                                try
                                {
                                    if (excludeProp.PropertyType == typeof(string[]))
                                        excludeProp.SetValue(extQuery, ids);
                                    else if (excludeProp.PropertyType == typeof(Guid[]))
                                        excludeProp.SetValue(extQuery, ids.Select(id => Guid.TryParse(id, out var g) ? g : Guid.Empty).ToArray());
                                }
                                catch { }
                            }
                        }
                    }

                    // Bakåtkompatibilitet: sätt ContentSection.ExcludedFolders från _queryExcludeViewIds
                    // om ExcludedFolders inte sparats explicit (gamla plugin-versioner).
                    if (!settings.ContainsKey("ExcludedFolders") &&
                        settings.TryGetValue("_queryExcludeViewIds", out var qExcludeFolders) &&
                        !string.IsNullOrWhiteSpace(qExcludeFolders))
                    {
                        var folderIds = qExcludeFolders.Split(',')
                            .Select(s => s.Trim()).Where(s => s.Length > 0).ToArray();
                        var exFoldersProp = props.FirstOrDefault(p => p.Name == "ExcludedFolders" && p.CanWrite
                                                                  && p.PropertyType == typeof(string[]));
                        if (exFoldersProp != null && folderIds.Length > 0)
                            exFoldersProp.SetValue(section, folderIds);
                    }

                    // Specialfall: _queryIsPlayed → IsPlayed; tomt = Any = null
                    // Emby 4.10.0.10+: IsPlayed finns nativt i ItemsQuery.
                    //   true  → Played   (IsPlayed = true)
                    //   false → Unplayed (IsPlayed = false)
                    //   annat → ingen filtrering
                    if (settings.TryGetValue("_queryIsPlayed", out var qIsPlayed))
                    {
                        if (qIsPlayed == "true")
                        {
                            extQuery.IsPlayed = true;
                            extQuery.IsUnplayed = null;
                        }
                        else if (qIsPlayed == "false")
                        {
                            extQuery.IsPlayed = false;
                            extQuery.IsUnplayed = null;
                        }
                        else
                        {
                            extQuery.IsPlayed = null;
                            extQuery.IsUnplayed = null;
                        }
                    }
                    else if (existing?.Query != null)
                    {
                        // _queryIsPlayed saknas i inställningar — bevara befintligt värde istället för att tyst nollställa
                        extQuery.IsPlayed = existing.Query.IsPlayed;
                    }

                    // Generisk _query* → övriga ItemsQuery-properties
                    foreach (var key in settings.Keys.Where(k =>
                        k.StartsWith("_query", StringComparison.OrdinalIgnoreCase) &&
                        !string.Equals(k, "_queryTagId", StringComparison.OrdinalIgnoreCase) &&
                        !string.Equals(k, "_queryIsPlayed", StringComparison.OrdinalIgnoreCase) &&
                        !string.Equals(k, "_queryExcludeViewIds", StringComparison.OrdinalIgnoreCase)))
                    {
                        var val = settings[key];
                        if (string.IsNullOrEmpty(val)) continue;
                        var propName = key.Substring(6);
                        if (propName.Length == 0) continue;
                        var qProp = queryProps.FirstOrDefault(p => string.Equals(p.Name, propName, StringComparison.OrdinalIgnoreCase));
                        if (qProp == null || !qProp.CanWrite) continue;
                        try
                        {
                            var t = Nullable.GetUnderlyingType(qProp.PropertyType) ?? qProp.PropertyType;
                            if (t == typeof(bool)) qProp.SetValue(extQuery, bool.Parse(val));
                            else if (t == typeof(string)) qProp.SetValue(extQuery, val);
                        }
                        catch { }
                    }

                    if (queryProp.CanWrite)
                        queryProp.SetValue(section, extQuery);
                }
                catch { }
            }

            // Migration: gamla inställningar sparade ScrollDirection i DisplayMode — rensa bort det
            {
                var displayModeProp = props.FirstOrDefault(p => p.Name == "DisplayMode" && p.CanRead);
                if (displayModeProp != null)
                {
                    var dm = displayModeProp.GetValue(section) as string;
                    if (dm == "Horizontal" || dm == "Vertical")
                        displayModeProp.SetValue(section, null);
                }
            }

            if (!string.IsNullOrEmpty(libraryId))
            {
                var parentProp = props.FirstOrDefault(p => p.Name == "ParentId" && p.CanWrite && p.PropertyType == typeof(string));
                if (parentProp != null) parentProp.SetValue(section, libraryId);
            }

            return section;
        }

        private bool IsScheduleActive(List<DateInterval> intervals) => IsScheduleActive(intervals, DateTime.Now);

        internal static bool IsScheduleActive(List<DateInterval> intervals, DateTime now)
        {
            if (intervals == null || intervals.Count == 0) return true;
            foreach (var interval in intervals)
            {
                bool match = false;
                if (interval.Type == "Weekly") { if (!string.IsNullOrEmpty(interval.DayOfWeek) && interval.DayOfWeek.IndexOf(now.DayOfWeek.ToString(), StringComparison.OrdinalIgnoreCase) >= 0) match = true; }
                else if (interval.Type == "EveryYear")
                {
                    if (interval.Start.HasValue && interval.End.HasValue)
                    {
                        var sDay = Math.Min(interval.Start.Value.Day, DateTime.DaysInMonth(now.Year, interval.Start.Value.Month));
                        var eDay = Math.Min(interval.End.Value.Day, DateTime.DaysInMonth(now.Year, interval.End.Value.Month));
                        var s = new DateTime(now.Year, interval.Start.Value.Month, sDay);
                        var e = new DateTime(now.Year, interval.End.Value.Month, eDay);
                        var today = now.Date;
                        // A window that wraps the new year (e.g. Dec 27 → Jan 3) is active from its start
                        // through Dec 31 and again from Jan 1 through its end.
                        match = e >= s ? today >= s && today <= e : today >= s || today <= e;
                    }
                }
                else { if ((!interval.Start.HasValue || now.Date >= interval.Start.Value.Date) && (!interval.End.HasValue || now.Date <= interval.End.Value.Date)) match = true; }
                if (match) return true;
            }
            return false;
        }

        private BaseItem ResolveItemForMediaInfo(BaseItem item, Dictionary<long, BaseItem> seriesEpisodeCache)
        {
            if (!item.GetType().Name.Contains("Series")) return item;
            if (!seriesEpisodeCache.TryGetValue(item.InternalId, out var cached))
            {
                cached = _libraryManager.GetItemList(new InternalItemsQuery
                {
                    IncludeItemTypes = new[] { "Episode" },
                    Parent = item,
                    Recursive = true,
                    Limit = 1
                }).FirstOrDefault() ?? item;
                seriesEpisodeCache[item.InternalId] = cached;
            }
            return cached;
        }

        private DateTimeOffset? GetSeriesLastPlayed(User user, BaseItem seriesItem,
            Dictionary<(Guid, long), DateTimeOffset?> cache,
            Dictionary<(Guid, long), (bool Played, DateTimeOffset? LastPlayedDate, int PlayCount)>? userDataCache = null)
        {
            var key = (user.Id, seriesItem.InternalId);
            if (cache.TryGetValue(key, out var cached)) return cached;

            var episodes = _libraryManager.GetItemList(new InternalItemsQuery
            {
                IncludeItemTypes = new[] { "Episode" },
                Parent = seriesItem,
                Recursive = true
            });

            DateTimeOffset? maxDate = null;
            foreach (var ep in episodes)
            {
                var epKey = (user.Id, ep.InternalId);
                DateTimeOffset? lpDate;
                if (userDataCache != null && userDataCache.TryGetValue(epKey, out var cd))
                {
                    lpDate = cd.LastPlayedDate;
                }
                else
                {
                    var ud = _userDataManager?.GetUserData(user, ep);
                    lpDate = ud?.LastPlayedDate;
                    if (userDataCache != null)
                        userDataCache[epKey] = ud == null ? (false, (DateTimeOffset?)null, 0) : (ud.Played, ud.LastPlayedDate, ud.PlayCount);
                }
                if (lpDate.HasValue && (maxDate == null || lpDate > maxDate))
                    maxDate = lpDate;
            }

            cache[key] = maxDate;
            return maxDate;
        }

        private static CachedMediaInfo ExtractMediaInfo(BaseItem itemToCheck)
        {
            var info = new CachedMediaInfo { AudioLanguages = new HashSet<string>(StringComparer.OrdinalIgnoreCase) };
            try
            {
                dynamic dynItem = itemToCheck;
                try {
                    int defaultWidth = (int)dynItem.Width;
                    if (defaultWidth >= 7680) info.Is8k = true;
                    else if (defaultWidth >= 3800) info.Is4k = true;
                    else if (defaultWidth >= 1900 && !info.Is4k && !info.Is8k) info.Is1080 = true;
                    else if (defaultWidth >= 1200 && !info.Is1080 && !info.Is4k && !info.Is8k) info.Is720 = true;
                    else if (defaultWidth > 0 && !info.Is720 && !info.Is1080 && !info.Is4k && !info.Is8k) info.IsSd = true;
                } catch { }

                System.Collections.IEnumerable streams = null;
                try { streams = dynItem.GetMediaStreams(); } catch { }
                if (streams == null) {
                    try {
                        var sources = dynItem.GetMediaSources(false);
                        if (sources != null) { foreach (var src in sources) { if (src.MediaStreams != null) { streams = src.MediaStreams; break; } } }
                    } catch { }
                }
                if (streams == null) { try { streams = dynItem.MediaStreams; } catch { } }

                if (streams != null)
                {
                    foreach (dynamic stream in streams)
                    {
                        try
                        {
                            string type = stream.Type?.ToString() ?? "";
                            string codec = stream.Codec?.ToString() ?? "";
                            string profile = stream.Profile?.ToString() ?? "";
                            string videoRange = "";
                            try { videoRange = stream.VideoRange?.ToString() ?? ""; } catch { }

                            if (type.Equals("Video", StringComparison.OrdinalIgnoreCase))
                            {
                                try { int w = (int)stream.Width; if (w >= 7680) info.Is8k = true; else if (w >= 3800) info.Is4k = true; else if (w >= 1900 && !info.Is4k && !info.Is8k) info.Is1080 = true; else if (w >= 1200 && !info.Is1080 && !info.Is4k && !info.Is8k) info.Is720 = true; else if (w > 0 && !info.Is720 && !info.Is1080 && !info.Is4k && !info.Is8k) info.IsSd = true; } catch { }
                                if (codec.IndexOf("hevc", StringComparison.OrdinalIgnoreCase) >= 0 || codec.IndexOf("h265", StringComparison.OrdinalIgnoreCase) >= 0) info.IsHevc = true;
                                if (codec.IndexOf("av1", StringComparison.OrdinalIgnoreCase) >= 0) info.IsAv1 = true;
                                if (codec.IndexOf("h264", StringComparison.OrdinalIgnoreCase) >= 0 || codec.IndexOf("avc", StringComparison.OrdinalIgnoreCase) >= 0) info.IsH264 = true;
                                if (profile.IndexOf("dv", StringComparison.OrdinalIgnoreCase) >= 0 || profile.IndexOf("dolby vision", StringComparison.OrdinalIgnoreCase) >= 0) info.IsDv = true;
                                if (profile.IndexOf("hdr10", StringComparison.OrdinalIgnoreCase) >= 0 || videoRange.IndexOf("hdr10", StringComparison.OrdinalIgnoreCase) >= 0) info.IsHdr10 = true;
                                if (videoRange.IndexOf("hdr", StringComparison.OrdinalIgnoreCase) >= 0 || profile.IndexOf("hdr", StringComparison.OrdinalIgnoreCase) >= 0) info.IsHdr = true;
                            }
                            else if (type.Equals("Audio", StringComparison.OrdinalIgnoreCase))
                            {
                                if (profile.IndexOf("atmos", StringComparison.OrdinalIgnoreCase) >= 0) info.IsAtmos = true;
                                if (codec.IndexOf("truehd", StringComparison.OrdinalIgnoreCase) >= 0) info.IsTrueHd = true;
                                if (codec.IndexOf("dts", StringComparison.OrdinalIgnoreCase) >= 0) { info.IsDts = true; if (profile.IndexOf("ma", StringComparison.OrdinalIgnoreCase) >= 0) info.IsDtsHdMa = true; }
                                if (codec.IndexOf("ac3", StringComparison.OrdinalIgnoreCase) >= 0 || codec.IndexOf("eac3", StringComparison.OrdinalIgnoreCase) >= 0) info.IsAc3 = true;
                                if (codec.IndexOf("aac", StringComparison.OrdinalIgnoreCase) >= 0) info.IsAac = true;
                                try { int ch = (int)stream.Channels; if (ch == 1) info.IsMono = true; else if (ch == 2) info.IsStereo = true; else if (ch == 6) info.Is51 = true; else if (ch >= 8) info.Is71 = true; } catch { }
                                try { var lang = stream.Language?.ToString(); if (!string.IsNullOrWhiteSpace(lang)) info.AudioLanguages.Add(lang); } catch { }
                                // Music-specific audio stream properties (only populated for Audio/MusicVideo items)
                                try { int br = (int)stream.BitRate; if (br > 0) info.BitRate = br / 1000; } catch { }
                                try { int sr = (int)stream.SampleRate; if (sr > 0) info.SampleRate = sr; } catch { }
                                try { int bps = (int)stream.BitDepth; if (bps > 0) info.BitsPerSample = bps; } catch { }
                            }
                        }
                        catch { }
                    }
                }

                info.DateModifiedDays = TryGetDateModified(itemToCheck);
                info.FileSizeMb = TryGetFileSize(itemToCheck);
                // Music item-level properties (IndexNumber = track, ParentIndexNumber = disc)
                try { int tn = (int)dynItem.IndexNumber; if (tn > 0) info.TrackNumber = tn; } catch { }
                try { int dn = (int)dynItem.ParentIndexNumber; if (dn > 0) info.DiscNumber = dn; } catch { }
            }
            catch { }
            return info;
        }

        private bool ItemMatchesMediaInfo(BaseItem item, TagConfig tagConfig, bool debug,
            Dictionary<long, BaseItem>? seriesEpisodeCache = null,
            Dictionary<string, HashSet<long>>? personCache = null,
            Dictionary<(Guid, long), (bool Played, DateTimeOffset? LastPlayedDate, int PlayCount)>? userDataCache = null,
            CachedMediaInfo? cachedInfo = null,
            User[]? preloadedUsers = null,
            Dictionary<(Guid, long), DateTimeOffset?>? seriesLastPlayedCache = null,
            Dictionary<string, HashSet<long>>? collectionMembershipCache = null,
            Dictionary<long, List<string>>? seriesEpisodeNamesCache = null)
        {
            var filters = tagConfig.MediaInfoFilters;
            var legacy = tagConfig.MediaInfoConditions;
            bool hasFilters = filters != null && filters.Count > 0;
            bool hasLegacy = legacy != null && legacy.Count > 0;
            if (!hasFilters && !hasLegacy) return true;

            BaseItem itemToCheck;
            bool is4k, is1080, is720, is8k, isSd, isHevc, isAv1, isH264;
            bool isHdr, isHdr10, isDv, isAtmos, isTrueHd, isDtsHdMa, isDts, isAc3, isAac;
            bool is51, is71, isStereo, isMono;
            HashSet<string> audioLanguages;
            double? cachedDateModifiedDays, cachedFileSizeMb;
            double? cachedBitRate, cachedSampleRate, cachedBitsPerSample, cachedTrackNumber, cachedDiscNumber;

            if (cachedInfo.HasValue)
            {
                itemToCheck = item; // metadata (Studios, Genres etc.) from original item
                var ci = cachedInfo.Value;
                is4k = ci.Is4k; is8k = ci.Is8k; is1080 = ci.Is1080; is720 = ci.Is720; isSd = ci.IsSd;
                isHevc = ci.IsHevc; isAv1 = ci.IsAv1; isH264 = ci.IsH264;
                isHdr = ci.IsHdr; isHdr10 = ci.IsHdr10; isDv = ci.IsDv;
                isAtmos = ci.IsAtmos; isTrueHd = ci.IsTrueHd; isDtsHdMa = ci.IsDtsHdMa;
                isDts = ci.IsDts; isAc3 = ci.IsAc3; isAac = ci.IsAac;
                is51 = ci.Is51; is71 = ci.Is71; isStereo = ci.IsStereo; isMono = ci.IsMono;
                audioLanguages = ci.AudioLanguages ?? new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                cachedDateModifiedDays = ci.DateModifiedDays;
                cachedFileSizeMb = ci.FileSizeMb;
                cachedBitRate = ci.BitRate;
                cachedSampleRate = ci.SampleRate;
                cachedBitsPerSample = ci.BitsPerSample;
                cachedTrackNumber = ci.TrackNumber;
                cachedDiscNumber = ci.DiscNumber;
            }
            else
            {
                itemToCheck = item;
                if (item.GetType().Name.Contains("Series"))
                    itemToCheck = seriesEpisodeCache != null
                        ? ResolveItemForMediaInfo(item, seriesEpisodeCache)
                        : (_libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "Episode" }, Parent = item, Recursive = true, Limit = 1 }).FirstOrDefault() ?? item);

                var extracted = ExtractMediaInfo(itemToCheck);
                is4k = extracted.Is4k; is8k = extracted.Is8k; is1080 = extracted.Is1080; is720 = extracted.Is720; isSd = extracted.IsSd;
                isHevc = extracted.IsHevc; isAv1 = extracted.IsAv1; isH264 = extracted.IsH264;
                isHdr = extracted.IsHdr; isHdr10 = extracted.IsHdr10; isDv = extracted.IsDv;
                isAtmos = extracted.IsAtmos; isTrueHd = extracted.IsTrueHd; isDtsHdMa = extracted.IsDtsHdMa;
                isDts = extracted.IsDts; isAc3 = extracted.IsAc3; isAac = extracted.IsAac;
                is51 = extracted.Is51; is71 = extracted.Is71; isStereo = extracted.IsStereo; isMono = extracted.IsMono;
                audioLanguages = extracted.AudioLanguages;
                cachedDateModifiedDays = extracted.DateModifiedDays;
                cachedFileSizeMb = extracted.FileSizeMb;
                cachedBitRate = extracted.BitRate;
                cachedSampleRate = extracted.SampleRate;
                cachedBitsPerSample = extracted.BitsPerSample;
                cachedTrackNumber = extracted.TrackNumber;
                cachedDiscNumber = extracted.DiscNumber;
            }

            string mediaType = item.GetType().Name;
            string[] itemTags = item.Tags ?? Array.Empty<string>();

            // When EpisodeIncludeSeries: inherit parent series' tags so Tag criteria can match series-level tags
            if (item.GetType().Name.Contains("Episode") && TagConfigIncludesParentSeries(tagConfig))
            {
                try
                {
                    var parentSeries = ((dynamic)item).Series as BaseItem;
                    if (parentSeries?.Tags != null && parentSeries.Tags.Length > 0)
                        itemTags = itemTags.Concat(parentSeries.Tags)
                                           .Distinct(StringComparer.OrdinalIgnoreCase)
                                           .ToArray();
                }
                catch { }
            }

            // Never let the group's own output tag satisfy its Tag: criteria (see _writtenTags).
            var ownTag = tagConfig.Tag?.Trim();
            if (!string.IsNullOrEmpty(ownTag) && _writtenTags.Contains(ownTag))
                itemTags = itemTags.Where(t => !string.Equals(t, ownTag, StringComparison.OrdinalIgnoreCase)).ToArray();

            if (hasFilters)
            {
                bool EvalCrit(string c) => EvaluateCriterion(c, itemToCheck, is4k, is1080, is720, is8k, isSd,
                    isHevc, isAv1, isH264, isHdr, isHdr10, isDv, isAtmos, isTrueHd, isDtsHdMa, isDts,
                    isAc3, isAac, is51, is71, isStereo, isMono, personCache, audioLanguages, mediaType, itemTags,
                    userDataCache, cachedDateModifiedDays, cachedFileSizeMb, preloadedUsers, seriesLastPlayedCache,
                    cachedBitRate, cachedSampleRate, cachedBitsPerSample, cachedTrackNumber, cachedDiscNumber,
                    collectionMembershipCache, seriesEpisodeNamesCache);
                bool EvalGroup(MediaInfoFilter f)
                {
                    if (f.Criteria == null || f.Criteria.Count == 0) return true;
                    bool isOr = string.Equals(f.Operator, "OR", StringComparison.OrdinalIgnoreCase);
                    return isOr ? f.Criteria.Any(EvalCrit) : f.Criteria.All(EvalCrit);
                }
                bool result = EvalGroup(filters![0]);
                for (int gi = 1; gi < filters.Count; gi++)
                {
                    bool groupResult = EvalGroup(filters[gi]);
                    bool useOr = string.Equals(filters[gi].GroupOperator, "OR", StringComparison.OrdinalIgnoreCase);
                    result = useOr ? result || groupResult : result && groupResult;
                }
                return result;
            }

            foreach (var cond in legacy!)
            {
                if (!EvaluateCriterion(cond, itemToCheck, is4k, is1080, is720, is8k, isSd, isHevc, isAv1, isH264,
                    isHdr, isHdr10, isDv, isAtmos, isTrueHd, isDtsHdMa, isDts, isAc3, isAac, is51, is71, isStereo, isMono,
                    personCache, audioLanguages, mediaType, itemTags, userDataCache, cachedDateModifiedDays, cachedFileSizeMb,
                    preloadedUsers, seriesLastPlayedCache, cachedBitRate, cachedSampleRate, cachedBitsPerSample, cachedTrackNumber,
                    cachedDiscNumber, collectionMembershipCache, seriesEpisodeNamesCache))
                    return false;
            }
            return true;
        }

        private bool EvaluateCriterion(string cond, BaseItem item, bool is4k, bool is1080, bool is720,
            bool is8k, bool isSd, bool isHevc, bool isAv1, bool isH264,
            bool isHdr, bool isHdr10, bool isDv, bool isAtmos, bool isTrueHd,
            bool isDtsHdMa, bool isDts, bool isAc3, bool isAac,
            bool is51, bool is71, bool isStereo, bool isMono,
            Dictionary<string, HashSet<long>>? personCache = null,
            HashSet<string>? audioLanguages = null,
            string? mediaType = null,
            string[]? itemTags = null,
            Dictionary<(Guid, long), (bool Played, DateTimeOffset? LastPlayedDate, int PlayCount)>? userDataCache = null,
            double? cachedDateModifiedDays = null,
            double? cachedFileSizeMb = null,
            User[]? preloadedUsers = null,
            Dictionary<(Guid, long), DateTimeOffset?>? seriesLastPlayedCache = null,
            double? cachedBitRate = null,
            double? cachedSampleRate = null,
            double? cachedBitsPerSample = null,
            double? cachedTrackNumber = null,
            double? cachedDiscNumber = null,
            Dictionary<string, HashSet<long>>? collectionMembershipCache = null,
            Dictionary<long, List<string>>? seriesEpisodeNamesCache = null)
        {
            bool negate = cond.Length > 0 && cond[0] == '!';
            if (negate) cond = cond.Substring(1);
            bool evalResult = EvaluateCriterionCore(cond);
            return negate ? !evalResult : evalResult;

            bool EvaluateCriterionCore(string c)
            {
            // Handle Collection/Playlist before Split(':') — names may contain colons
            if (c.StartsWith("Collection:", StringComparison.OrdinalIgnoreCase) ||
                c.StartsWith("Playlist:", StringComparison.OrdinalIgnoreCase))
            {
                var ci = c.IndexOf(':');
                var cpProp = c.Substring(0, ci);
                var cpVal  = c.Substring(ci + 1).Trim();
                return cpProp.Equals("Collection", StringComparison.OrdinalIgnoreCase)
                    ? collectionMembershipCache != null && SplitCommaValues(cpVal).Any(n => collectionMembershipCache.TryGetValue("Collection:" + n, out var cIds) && cIds.Contains(item.InternalId))
                    : collectionMembershipCache != null && SplitCommaValues(cpVal).Any(n => collectionMembershipCache.TryGetValue("Playlist:" + n, out var pIds)  && pIds.Contains(item.InternalId));
            }
            var parts = c.Split(':');
            if (parts.Length == 2)
            {
                var prop = parts[0]; var val = parts[1].Trim();
                return prop switch
                {
                    "Studio"        => SplitCommaValues(val).Any(v => MatchesAny(item.Studios, v)),
                    "Genre"         => SplitCommaValues(val).Any(v => MatchesAny(item.Genres, v)),
                    "Actor"         => personCache != null && SplitCommaValues(val).Any(n => personCache.TryGetValue("Actor:" + n, out var aIds) && aIds.Contains(item.InternalId)),
                    "Director"      => personCache != null && SplitCommaValues(val).Any(n => personCache.TryGetValue("Director:" + n, out var dIds) && dIds.Contains(item.InternalId)),
                    "Writer"        => personCache != null && SplitCommaValues(val).Any(n => personCache.TryGetValue("Writer:" + n, out var wIds) && wIds.Contains(item.InternalId)),
                    "Title"         => SplitCommaValues(val).Any(v => GetTitleName(item)?.IndexOf(v, StringComparison.OrdinalIgnoreCase) >= 0),
                    "EpisodeTitle"  => SplitCommaValues(val).Any(v => MatchesEpisodeTitle(item, v, false, seriesEpisodeNamesCache)),
                    "Overview"      => SplitCommaValues(val).Any(v => item.Overview?.IndexOf(v, StringComparison.OrdinalIgnoreCase) >= 0),
                    "ContentRating" => SplitCommaValues(val).Any(v => string.Equals(item.OfficialRating, v, StringComparison.OrdinalIgnoreCase)),
                    "AudioLanguage" => audioLanguages != null && SplitCommaValues(val).Any(v => audioLanguages.Contains(v)),
                    "MediaType"     => val.Equals("EpisodeIncludeSeries", StringComparison.OrdinalIgnoreCase)
                                        ? item.GetType().Name.Contains("Episode")
                                        : string.Equals(mediaType, val, StringComparison.OrdinalIgnoreCase),
                    "Tag"           => itemTags != null && SplitCommaValues(val).Any(v => MatchesAny(itemTags, v)),
                    "ImdbId"        => MatchesImdbId(item.GetProviderId("Imdb"), val),
                    "TvdbId"        => MatchesImdbId(item.GetProviderId("Tvdb"), val),
                    "FolderPath"    => SplitCommaValues(val).Any(v => !string.IsNullOrEmpty(item.Path) && item.Path.IndexOf(v, StringComparison.OrdinalIgnoreCase) >= 0),
                    "Country"       => item.ProductionLocations != null && SplitCommaValues(val).Any(v => MatchesAny(item.ProductionLocations, v)),
                    "Artist"        => SplitCommaValues(val).Any(v => MatchesArtistOrAlbumArtist(item, v, false)),
                    "Album"         => SplitCommaValues(val).Any(v => MatchesAlbumTitle(item, v, false)),
                    _ => false
                };
            }
            if (parts.Length == 4)
            {
                var prop4 = parts[0]; var userId4 = parts[1]; var op4 = parts[2]; var valStr4 = parts[3];
                if (userId4 == "__any__" || userId4 == "__all__")
                {
                    bool matchAll = userId4 == "__all__";
                    var allUsers = preloadedUsers ?? _userManager.GetUserList(new UserQuery { IsDisabled = false });
                    if (allUsers == null || allUsers.Length == 0) return false;
                    if (prop4 == "IsPlayed")
                    {
                        bool wantWatched = string.Equals(valStr4, "Watched", StringComparison.OrdinalIgnoreCase);
                        Func<User, bool> checkPlayed = u => {
                            var k = (u.Id, item.InternalId);
                            if (userDataCache != null && userDataCache.TryGetValue(k, out var cd)) return cd.Played == wantWatched;
                            var ud2 = _userDataManager?.GetUserData(u, item);
                            if (userDataCache != null) userDataCache[k] = ud2 == null ? (false, null, 0) : (ud2.Played, ud2.LastPlayedDate, ud2.PlayCount);
                            return ud2 != null && ud2.Played == wantWatched;
                        };
                        return matchAll ? allUsers.All(checkPlayed) : allUsers.Any(checkPlayed);
                    }
                    if (prop4 == "LastPlayed" &&
                        double.TryParse(valStr4, System.Globalization.NumberStyles.Any,
                                        System.Globalization.CultureInfo.InvariantCulture, out var daysU))
                    {
                        bool isSeries = item.GetType().Name.Contains("Series");
                        Func<User, bool> checkLp = u => {
                            DateTimeOffset? lpDate;
                            if (isSeries)
                                lpDate = seriesLastPlayedCache != null ? GetSeriesLastPlayed(u, item, seriesLastPlayedCache, userDataCache) : null;
                            else
                            {
                                var k = (u.Id, item.InternalId);
                                if (userDataCache != null && userDataCache.TryGetValue(k, out var cd)) { lpDate = cd.LastPlayedDate; }
                                else { var ud2 = _userDataManager?.GetUserData(u, item); lpDate = ud2?.LastPlayedDate;
                                       if (userDataCache != null) userDataCache[k] = ud2 == null ? (false, (DateTimeOffset?)null, 0) : (ud2.Played, ud2.LastPlayedDate, ud2.PlayCount); }
                            }
                            if (lpDate == null) return false;
                            return ApplyNumericOp((DateTimeOffset.UtcNow - lpDate.Value).TotalDays, op4, daysU);
                        };
                        return matchAll ? allUsers.All(checkLp) : allUsers.Any(checkLp);
                    }
                    if (prop4 == "PlayCount" &&
                        double.TryParse(valStr4, System.Globalization.NumberStyles.Any,
                                        System.Globalization.CultureInfo.InvariantCulture, out var countU))
                    {
                        Func<User, bool> checkPc = u => {
                            var k = (u.Id, item.InternalId);
                            int playCount;
                            if (userDataCache != null && userDataCache.TryGetValue(k, out var cd)) { playCount = cd.PlayCount; }
                            else { var ud2 = _userDataManager?.GetUserData(u, item); playCount = ud2?.PlayCount ?? 0;
                                   if (userDataCache != null) userDataCache[k] = ud2 == null ? (false, (DateTimeOffset?)null, 0) : (ud2.Played, ud2.LastPlayedDate, ud2.PlayCount); }
                            return ApplyNumericOp(playCount, op4, countU);
                        };
                        return matchAll ? allUsers.All(checkPc) : allUsers.Any(checkPc);
                    }
                    return false;
                }
                if (!Guid.TryParse(userId4, out var guid4)) return false;
                var udKey = (guid4, item.InternalId);
                (bool Played, DateTimeOffset? LastPlayedDate, int PlayCount) udResult;
                if (userDataCache != null && userDataCache.TryGetValue(udKey, out udResult))
                {
                }
                else
                {
                    var user4 = _userManager.GetUserById(guid4);
                    if (user4 == null) return false;
                    var ud = _userDataManager?.GetUserData(user4, item);
                    if (ud == null) return false;
                    udResult = (ud.Played, ud.LastPlayedDate, ud.PlayCount);
                    if (userDataCache != null) userDataCache[udKey] = udResult;
                }
                if (prop4 == "IsPlayed")
                {
                    bool wantWatched = string.Equals(valStr4, "Watched", StringComparison.OrdinalIgnoreCase);
                    return udResult.Played == wantWatched;
                }
                if (prop4 == "LastPlayed" &&
                    double.TryParse(valStr4, System.Globalization.NumberStyles.Any,
                                    System.Globalization.CultureInfo.InvariantCulture, out var days4))
                {
                    DateTimeOffset? lpDate4 = item.GetType().Name.Contains("Series") && seriesLastPlayedCache != null
                        ? GetSeriesLastPlayed(_userManager.GetUserById(guid4)!, item, seriesLastPlayedCache, userDataCache)
                        : udResult.LastPlayedDate;
                    if (!lpDate4.HasValue) return false;
                    return ApplyNumericOp((DateTime.UtcNow - lpDate4.Value).TotalDays, op4, days4);
                }
                if (prop4 == "PlayCount" &&
                    double.TryParse(valStr4, System.Globalization.NumberStyles.Any,
                                    System.Globalization.CultureInfo.InvariantCulture, out var count4))
                {
                    return ApplyNumericOp(udResult.PlayCount, op4, count4);
                }
                return false;
            }
            if (parts.Length == 3 && (parts[1] == "contains" || parts[1] == "exact"))
            {
                var tProp = parts[0]; var tOp = parts[1]; var tVal = parts[2].Trim();
                bool exact = tOp == "exact";
                return tProp switch
                {
                    "Title"         => exact ? SplitCommaValues(tVal).Any(v => string.Equals(GetTitleName(item), v, StringComparison.OrdinalIgnoreCase))
                                             : SplitCommaValues(tVal).Any(v => GetTitleName(item)?.IndexOf(v, StringComparison.OrdinalIgnoreCase) >= 0),
                    "EpisodeTitle"  => SplitCommaValues(tVal).Any(v => MatchesEpisodeTitle(item, v, exact, seriesEpisodeNamesCache)),
                    "Overview"      => exact ? SplitCommaValues(tVal).Any(v => string.Equals(item.Overview, v, StringComparison.OrdinalIgnoreCase))
                                             : SplitCommaValues(tVal).Any(v => item.Overview?.IndexOf(v, StringComparison.OrdinalIgnoreCase) >= 0),
                    "Studio"        => exact ? item.Studios != null && SplitCommaValues(tVal).Any(v => item.Studios.Any(s => string.Equals(s, v, StringComparison.OrdinalIgnoreCase)))
                                             : SplitCommaValues(tVal).Any(v => MatchesAny(item.Studios, v)),
                    "Genre"         => exact ? item.Genres != null && SplitCommaValues(tVal).Any(v => item.Genres.Any(g => string.Equals(g, v, StringComparison.OrdinalIgnoreCase)))
                                             : SplitCommaValues(tVal).Any(v => MatchesAny(item.Genres, v)),
                    "Tag"           => exact ? itemTags != null && SplitCommaValues(tVal).Any(v => itemTags.Any(t => string.Equals(t, v, StringComparison.OrdinalIgnoreCase)))
                                             : itemTags != null && SplitCommaValues(tVal).Any(v => MatchesAny(itemTags, v)),
                    "ContentRating" => exact ? SplitCommaValues(tVal).Any(v => string.Equals(item.OfficialRating, v, StringComparison.OrdinalIgnoreCase))
                                             : SplitCommaValues(tVal).Any(v => item.OfficialRating?.IndexOf(v, StringComparison.OrdinalIgnoreCase) >= 0),
                    "AudioLanguage" => exact ? audioLanguages != null && SplitCommaValues(tVal).Any(v => audioLanguages.Contains(v))
                                             : audioLanguages != null && SplitCommaValues(tVal).Any(v => audioLanguages.Any(l => l.IndexOf(v, StringComparison.OrdinalIgnoreCase) >= 0)),
                    "Actor"         => personCache != null && (exact
                                        ? SplitCommaValues(tVal).Any(n => personCache.TryGetValue("Actor:exact:" + n, out var aIds3) && aIds3.Contains(item.InternalId))
                                        : SplitCommaValues(tVal).Any(n => personCache.TryGetValue("Actor:contains:" + n, out var aIdsC) && aIdsC.Contains(item.InternalId))),
                    "Director"      => personCache != null && (exact
                                        ? SplitCommaValues(tVal).Any(n => personCache.TryGetValue("Director:exact:" + n, out var dIds3) && dIds3.Contains(item.InternalId))
                                        : SplitCommaValues(tVal).Any(n => personCache.TryGetValue("Director:contains:" + n, out var dIdsC) && dIdsC.Contains(item.InternalId))),
                    "Writer"        => personCache != null && (exact
                                        ? SplitCommaValues(tVal).Any(n => personCache.TryGetValue("Writer:exact:" + n, out var wIds3) && wIds3.Contains(item.InternalId))
                                        : SplitCommaValues(tVal).Any(n => personCache.TryGetValue("Writer:contains:" + n, out var wIdsC) && wIdsC.Contains(item.InternalId))),
                    "Artist"        => SplitCommaValues(tVal).Any(v => MatchesArtistOrAlbumArtist(item, v, exact)),
                    "Album"         => SplitCommaValues(tVal).Any(v => MatchesAlbumTitle(item, v, exact)),
                    "FolderPath"    => exact
                                        ? SplitCommaValues(tVal).Any(v => string.Equals(item.Path, v, StringComparison.OrdinalIgnoreCase))
                                        : SplitCommaValues(tVal).Any(v => !string.IsNullOrEmpty(item.Path) && item.Path.IndexOf(v, StringComparison.OrdinalIgnoreCase) >= 0),
                    "Country"       => exact
                                        ? item.ProductionLocations != null && SplitCommaValues(tVal).Any(v => item.ProductionLocations.Any(c => string.Equals(c, v, StringComparison.OrdinalIgnoreCase)))
                                        : item.ProductionLocations != null && SplitCommaValues(tVal).Any(v => MatchesAny(item.ProductionLocations, v)),
                    _ => false
                };
            }
            if (parts.Length == 3 && double.TryParse(parts[2],
                System.Globalization.NumberStyles.Any,
                System.Globalization.CultureInfo.InvariantCulture, out var num))
            {
                double? v = parts[0] switch
                {
                    // float → double without rounding turns 7.2 into 7.1999998…, so ">= 7.2" would skip
                    // exact 7.2s (and 6.9 into 6.9000001…). Ratings have at most 3 decimals.
                    "CommunityRating" => item.CommunityRating.HasValue ? Math.Round((double)item.CommunityRating.Value, 4) : (double?)null,
                    "Year"            => (double?)item.ProductionYear,
                    "Runtime"         => item.RunTimeTicks.HasValue
                                        ? (double?)(item.RunTimeTicks.Value / TimeSpan.TicksPerMinute) : null,
                    "DateAdded"       => (double?)(DateTime.UtcNow - item.DateCreated).TotalDays,
                    "DateModified"    => cachedDateModifiedDays ?? TryGetDateModified(item),
                    "FileSize"        => cachedFileSizeMb ?? TryGetFileSize(item),
                    "BitRate"          => cachedBitRate,
                    "SampleRate"       => cachedSampleRate,
                    "BitsPerSample"    => cachedBitsPerSample,
                    "TrackNumber"      => cachedTrackNumber,
                    "DiscNumber"       => cachedDiscNumber,
                    "WatchedByCount"   => (double?)CountWatchedByUsers(item, preloadedUsers, userDataCache),
                    _ => null
                };
                if (!v.HasValue) return false;
                return ApplyNumericOp(v.Value, parts[1], num);
            }
            return c switch
            {
                "4K" => is4k, "8K" => is8k, "1080p" => is1080, "720p" => is720, "SD" => isSd,
                "HEVC" => isHevc, "AV1" => isAv1, "H264" => isH264,
                "HDR" => isHdr || isDv, "HDR10" => isHdr10, "DolbyVision" => isDv,
                "Atmos" => isAtmos, "TrueHD" => isTrueHd, "DtsHdMa" => isDtsHdMa,
                "DTS" => isDts, "AC3" => isAc3, "AAC" => isAac,
                "7.1" => is71, "5.1" => is51, "Stereo" => isStereo, "Mono" => isMono,
                _ => false
            };
            } // EvaluateCriterionCore
        }

        private static bool MatchesAny(string[] values, string search) =>
            values != null && values.Any(v =>
                v.IndexOf(search, StringComparison.OrdinalIgnoreCase) >= 0);

        private static string[] SplitCommaValues(string val) =>
            val.Split(new[] { '\n', '\r' }, StringSplitOptions.RemoveEmptyEntries)
               .Select(v => v.Trim()).Where(v => v.Length > 0).ToArray();

        private static bool MatchesImdbId(string? itemImdb, string val) =>
            !string.IsNullOrEmpty(itemImdb) &&
            val.Split(new[] { '\n', '\r' }, StringSplitOptions.RemoveEmptyEntries)
               .Any(id => string.Equals(itemImdb, id.Trim(), StringComparison.OrdinalIgnoreCase));

        private static bool MatchesPerson(BaseItem item, string name, string type)
        {
            try
            {
                dynamic dynItem = item;
                var people = dynItem.People;
                if (people == null) return false;
                foreach (dynamic p in people)
                {
                    string pType = p.Type?.ToString() ?? "";
                    string pName = p.Name ?? "";
                    if (string.Equals(pType, type, StringComparison.OrdinalIgnoreCase) &&
                        pName.IndexOf(name, StringComparison.OrdinalIgnoreCase) >= 0)
                        return true;
                }
            }
            catch { }
            return false;
        }

        private static string? GetTitleName(BaseItem item)
        {
            if (item.GetType().Name.Contains("Episode"))
            {
                try { return ((dynamic)item).Series?.Name as string; } catch { }
                return null;
            }
            return item.Name;
        }

        private static IEnumerable<string> GetAllCriteria(TagConfig tagConfig)
        {
            var fromFilters = tagConfig.MediaInfoFilters?.SelectMany(f => f.Criteria ?? Enumerable.Empty<string>())
                ?? Enumerable.Empty<string>();
            var fromConditions = tagConfig.MediaInfoConditions?.AsEnumerable()
                ?? Enumerable.Empty<string>();
            return fromFilters.Concat(fromConditions);
        }

        // Returns the legacy single target type for backwards compat (MediaInfoTargetType or MediaInfoSeasonMode)
        private static string EffectiveLegacyTargetType(TagConfig tagConfig)
        {
            if (!string.IsNullOrEmpty(tagConfig.MediaInfoTargetType))
                return tagConfig.MediaInfoTargetType;
            if (tagConfig.MediaInfoSeasonMode && tagConfig.SourceType == "MediaInfo")
                return "Season";
            return "";
        }

        // Effective tag output targets (new fields → old MediaInfoTarget* → legacy string)
        private static (bool ep, bool sea, bool ser) EffectiveTagTargets(TagConfig tc)
        {
            if (tc.TagTargetEpisode || tc.TagTargetSeason || tc.TagTargetSeries)
                return (tc.TagTargetEpisode, tc.TagTargetSeason, tc.TagTargetSeries);
            if (tc.MediaInfoTargetEpisode || tc.MediaInfoTargetSeason || tc.MediaInfoTargetSeries)
                return (tc.MediaInfoTargetEpisode, tc.MediaInfoTargetSeason, tc.MediaInfoTargetSeries);
            var leg = EffectiveLegacyTargetType(tc);
            return (leg == "Episode", leg == "Season", leg == "Series");
        }

        // Effective collection output targets (same fallback chain)
        private static (bool ep, bool sea, bool ser) EffectiveCollectionTargets(TagConfig tc)
        {
            if (tc.CollectionTargetEpisode || tc.CollectionTargetSeason || tc.CollectionTargetSeries)
                return (tc.CollectionTargetEpisode, tc.CollectionTargetSeason, tc.CollectionTargetSeries);
            if (tc.MediaInfoTargetEpisode || tc.MediaInfoTargetSeason || tc.MediaInfoTargetSeries)
                return (tc.MediaInfoTargetEpisode, tc.MediaInfoTargetSeason, tc.MediaInfoTargetSeries);
            var leg = EffectiveLegacyTargetType(tc);
            return (leg == "Episode", leg == "Season", leg == "Series");
        }

        // Returns true if episodes should be scanned — only when MediaType:Episode is explicitly set
        private static bool TagConfigTargetsEpisodes(TagConfig tagConfig) =>
            GetAllCriteria(tagConfig).Any(c =>
                c.TrimStart('!').StartsWith("MediaType:Episode", StringComparison.OrdinalIgnoreCase));

        // Returns true if any active MediaInfo group references music-specific criteria
        private static bool ConfigNeedsMusicItems(PluginConfiguration config) =>
            config.Tags.Any(t => t.Active && t.SourceType == "MediaInfo"
                && GetAllCriteria(t).Any(c => {
                    var s = c.TrimStart('!');
                    return s.StartsWith("MediaType:Audio", StringComparison.OrdinalIgnoreCase)
                        || s.StartsWith("MediaType:MusicVideo", StringComparison.OrdinalIgnoreCase)
                        || s.StartsWith("MediaType:MusicAlbum", StringComparison.OrdinalIgnoreCase)
                        || s.StartsWith("MediaType:MusicArtist", StringComparison.OrdinalIgnoreCase)
                        || s.StartsWith("Artist:", StringComparison.OrdinalIgnoreCase)
                        || s.StartsWith("Album:", StringComparison.OrdinalIgnoreCase)
                        || s.StartsWith("BitRate:", StringComparison.OrdinalIgnoreCase)
                        || s.StartsWith("SampleRate:", StringComparison.OrdinalIgnoreCase)
                        || s.StartsWith("BitsPerSample:", StringComparison.OrdinalIgnoreCase)
                        || s.StartsWith("TrackNumber:", StringComparison.OrdinalIgnoreCase)
                        || s.StartsWith("DiscNumber:", StringComparison.OrdinalIgnoreCase);
                }));

        // Builds the IncludeItemTypes array, adding music types only when the config needs them
        private static string[] BuildItemTypes(PluginConfiguration config)
        {
            var types = new List<string> { "Movie", "Series" };
            if (ConfigNeedsMusicItems(config))
                types.AddRange(new[] { "Audio", "MusicVideo", "MusicAlbum", "MusicArtist" });
            return types.ToArray();
        }

        // Returns true if the item type is a taggable top-level item (Movie, Series, or music types)
        private static bool IsTaggableTopLevelItem(BaseItem item)
        {
            var name = item.GetType().Name;
            return name.Contains("Movie") || name.Contains("Series")
                || name.Contains("MusicAlbum") || name.Contains("MusicArtist")
                || name.Contains("MusicVideo") || name.Contains("Audio");
        }

        // Matches an item's artist or album artist against a search name
        private static bool MatchesArtistOrAlbumArtist(BaseItem item, string name, bool exact)
        {
            try
            {
                dynamic d = item;
                try
                {
                    string albumArtist = d.AlbumArtist ?? "";
                    if (exact ? string.Equals(albumArtist, name, StringComparison.OrdinalIgnoreCase)
                              : albumArtist.IndexOf(name, StringComparison.OrdinalIgnoreCase) >= 0)
                        return true;
                }
                catch { }
                try
                {
                    System.Collections.IEnumerable artists = d.Artists;
                    if (artists != null)
                        foreach (string a in artists)
                            if (a != null && (exact ? string.Equals(a, name, StringComparison.OrdinalIgnoreCase)
                                                    : a.IndexOf(name, StringComparison.OrdinalIgnoreCase) >= 0))
                                return true;
                }
                catch { }
            }
            catch { }
            return false;
        }

        // Matches an item's Album property against a search name
        private static bool MatchesAlbumTitle(BaseItem item, string name, bool exact)
        {
            try
            {
                dynamic d = item;
                string album = d.Album ?? "";
                return exact ? string.Equals(album, name, StringComparison.OrdinalIgnoreCase)
                             : album.IndexOf(name, StringComparison.OrdinalIgnoreCase) >= 0;
            }
            catch { return false; }
        }

        // Returns the number of non-disabled users who have marked the item as played
        private int CountWatchedByUsers(BaseItem item,
            User[]? users,
            Dictionary<(Guid, long), (bool Played, DateTimeOffset? LastPlayedDate, int PlayCount)>? userDataCache)
        {
            if (users == null || users.Length == 0) return 0;
            int count = 0;
            foreach (var u in users)
            {
                var k = (u.Id, item.InternalId);
                bool played;
                if (userDataCache != null && userDataCache.TryGetValue(k, out var cd))
                    played = cd.Played;
                else
                {
                    var ud = _userDataManager?.GetUserData(u, item);
                    played = ud?.Played ?? false;
                    if (userDataCache != null)
                        userDataCache[k] = ud == null
                            ? (false, (DateTimeOffset?)null, 0)
                            : (ud.Played, ud.LastPlayedDate, ud.PlayCount);
                }
                if (played) count++;
            }
            return count;
        }

        private static bool TagConfigIncludesParentSeries(TagConfig tagConfig) =>
            GetAllCriteria(tagConfig).Any(c =>
                c.TrimStart('!').Equals("MediaType:EpisodeIncludeSeries", StringComparison.OrdinalIgnoreCase));

        private static bool TagConfigTargetsSeason(TagConfig tagConfig)
        {
            var (_, tSea, _) = EffectiveTagTargets(tagConfig);
            var (_, cSea, _) = EffectiveCollectionTargets(tagConfig);
            return tSea || cSea;
        }

        private List<BaseItem> ResolveParentSeasons(IEnumerable<BaseItem> matchedEpisodes)
        {
            var seasonIds = new HashSet<long>();
            var seasons = new List<BaseItem>();
            foreach (var ep in matchedEpisodes)
            {
                var parent = ep.Parent;
                if (parent != null && parent.GetType().Name.Contains("Season"))
                {
                    if (seasonIds.Add(parent.InternalId))
                        seasons.Add(parent);
                }
            }
            return seasons;
        }

        private List<BaseItem> ResolveParentSeries(IEnumerable<BaseItem> matchedEpisodes)
        {
            var seriesIds = new HashSet<long>();
            var seriesList = new List<BaseItem>();
            foreach (var ep in matchedEpisodes)
            {
                BaseItem? seriesItem = null;
                var parent = ep.Parent;
                if (parent != null)
                {
                    if (parent.GetType().Name.Contains("Series"))
                        seriesItem = parent;
                    else if (parent.GetType().Name.Contains("Season") && parent.Parent != null && parent.Parent.GetType().Name.Contains("Series"))
                        seriesItem = parent.Parent;
                }
                if (seriesItem != null && seriesIds.Add(seriesItem.InternalId))
                    seriesList.Add(seriesItem);
            }
            return seriesList;
        }

        // Expand down: series → all child seasons (Season.Parent = Series)
        private List<BaseItem> ResolveChildSeasons(IEnumerable<BaseItem> matchedSeries)
        {
            var seriesIds = new HashSet<long>(matchedSeries
                .Where(i => i.GetType().Name.Contains("Series"))
                .Select(i => i.InternalId));
            if (seriesIds.Count == 0) return new List<BaseItem>();

            var seasonIds = new HashSet<long>();
            var seasons = new List<BaseItem>();
            var allSeasons = _libraryManager.GetItemList(new InternalItemsQuery
            {
                IncludeItemTypes = new[] { "Season" },
                Recursive = true,
                IsVirtualItem = false
            });
            foreach (var s in allSeasons)
            {
                var parentId = s.Parent?.InternalId ?? 0;
                if (parentId != 0 && seriesIds.Contains(parentId) && seasonIds.Add(s.InternalId))
                    seasons.Add(s);
            }
            return seasons;
        }

        // Expand down: series → all child episodes (Episode.Parent = Season, Season.Parent = Series)
        private List<BaseItem> ResolveChildEpisodes(IEnumerable<BaseItem> matchedSeries)
        {
            var seriesIds = new HashSet<long>(matchedSeries
                .Where(i => i.GetType().Name.Contains("Series"))
                .Select(i => i.InternalId));
            if (seriesIds.Count == 0) return new List<BaseItem>();

            var episodeIds = new HashSet<long>();
            var episodes = new List<BaseItem>();
            var allEpisodes = _libraryManager.GetItemList(new InternalItemsQuery
            {
                IncludeItemTypes = new[] { "Episode" },
                Recursive = true,
                IsVirtualItem = false
            });
            foreach (var ep in allEpisodes)
            {
                var seriesId = ep.Parent?.Parent?.InternalId ?? 0;
                if (seriesId != 0 && seriesIds.Contains(seriesId) && episodeIds.Add(ep.InternalId))
                    episodes.Add(ep);
            }
            return episodes;
        }

        private static string? ExtractTitleContains(TagConfig tagConfig)
        {
            foreach (var c in GetAllCriteria(tagConfig))
            {
                var s = c.TrimStart('!');
                if (s.StartsWith("Title:", StringComparison.OrdinalIgnoreCase))
                {
                    var val = s.Substring("Title:".Length).Trim();
                    if (!string.IsNullOrEmpty(val)) return val;
                }
            }
            return null;
        }

        private bool MatchesEpisodeTitle(BaseItem item, string val, bool exact,
            Dictionary<long, List<string>>? seriesEpisodeNamesCache = null)
        {
            Func<string?, bool> matches = exact
                ? (n => string.Equals(n, val, StringComparison.OrdinalIgnoreCase))
                : (n => n?.IndexOf(val, StringComparison.OrdinalIgnoreCase) >= 0);

            var typeName = item.GetType().Name;

            if (typeName.Contains("Movie"))   return false;
            if (typeName.Contains("Episode")) return matches(item.Name);
            if (typeName.Contains("Series"))
            {
                if (seriesEpisodeNamesCache != null && seriesEpisodeNamesCache.TryGetValue(item.InternalId, out var names))
                    return names.Any(n => matches(n));
                var episodes = _libraryManager.GetItemList(new InternalItemsQuery
                {
                    IncludeItemTypes = new[] { "Episode" },
                    Parent = item,
                    Recursive = true
                });
                return episodes.Any(ep => matches(ep.Name));
            }
            return false;
        }

        private static bool ApplyNumericOp(double v, string op, double num) => op switch
        {
            ">"  => v > num,
            ">=" => v >= num,
            "<"  => v < num,
            "<=" => v <= num,
            "="  => Math.Abs(v - num) < 0.01,
            _ => false
        };

        private static double? TryGetDateModified(BaseItem item)
        {
            try { dynamic d = item; DateTime dt = d.DateModified; return (DateTime.UtcNow - dt).TotalDays; }
            catch { return null; }
        }

        private static double? TryGetFileSize(BaseItem item)
        {
            try { dynamic d = item; long? sz = d.Size; return sz.HasValue ? (double?)(sz.Value / 1048576.0) : null; }
            catch { return null; }
        }

        // ───────────────────────── Execution log helpers ─────────────────────────
        // Everything below only formats log output; none of it affects what the run does.

        /// <summary>Short, user-facing label for a group's source: "Trakt", "AI · OpenAI", "Smart playlist"...</summary>
        private static string DescribeSource(TagConfig tc)
        {
            switch (tc.SourceType)
            {
                case "MediaInfo":       return "Smart playlist";
                case "LocalCollection": return "Local collection";
                case "LocalPlaylist":   return "Local playlist";
                case "AI":              return "AI · " + (string.IsNullOrWhiteSpace(tc.AiProvider) ? "unknown provider" : tc.AiProvider);
                default:
                    var url = tc.Url ?? "";
                    if (url.IndexOf("mdblist.com", StringComparison.OrdinalIgnoreCase) >= 0) return "MDBList";
                    if (url.IndexOf("themoviedb.org", StringComparison.OrdinalIgnoreCase) >= 0) return "TMDb";
                    if (url.IndexOf("trakt", StringComparison.OrdinalIgnoreCase) >= 0) return "Trakt";
                    return "External list";
            }
        }

        /// <summary>One debug line describing where a group's items come from and which features it drives.</summary>
        private string DescribeSourceDetail(TagConfig tc, int effectiveLimit)
        {
            var parts = new List<string> { "Source: " + DescribeSource(tc) };
            switch (tc.SourceType)
            {
                case "MediaInfo":
                    parts.Add($"{tc.MediaInfoFilters?.Count ?? 0} filter groups, {GetAllCriteria(tc).Count()} conditions");
                    break;
                case "LocalCollection":
                case "LocalPlaylist":
                    parts.Add($"'{tc.LocalSourceId}'");
                    break;
                case "AI":
                    parts.Add($"prompt {tc.AiPrompt?.Length ?? 0} chars" + (tc.AiIncludeRecentlyWatched ? ", includes watch history" : ""));
                    break;
                default:
                    parts.Add(tc.Url ?? "");
                    break;
            }
            if (tc.SourceType != "MediaInfo" && (tc.MediaInfoFilters?.Count > 0 || tc.MediaInfoConditions?.Count > 0))
                parts.Add($"{GetAllCriteria(tc).Count()} extra filter conditions");
            parts.Add(effectiveLimit >= 10000 ? "no limit" : $"limit {effectiveLimit}");
            if ((tc.Blacklist?.Count ?? 0) > 0) parts.Add($"{tc.Blacklist!.Count} blacklisted");
            var features = new List<string>();
            if (tc.EnableTag && !tc.OnlyCollection) features.Add("tag");
            if (tc.EnableCollection) features.Add("collection");
            if (tc.EnablePlaylist) features.Add("playlist");
            if (tc.EnableHomeSection) features.Add("home section");
            if (features.Count > 0) parts.Add("creates: " + string.Join(", ", features));
            return string.Join("  ·  ", parts);
        }

        /// <summary>
        /// The one-line live progress entry written under "» Fetching sources" as soon as a group
        /// has been fetched (or skipped / failed).
        /// </summary>
        private void WriteFetchLine(GroupRunStats gs)
        {
            string head = $"[{gs.GroupIndex}/{gs.GroupTotal}] {gs.DisplayName}  ({gs.SourceLabel})";
            if (gs.Skipped)
            {
                _log.Skip($"{head}  ·  skipped: {gs.SkipReason}");
                return;
            }
            if (gs.ErrorMessage != null)
            {
                _log.Error($"{head}  ·  {gs.ErrorMessage}");
                return;
            }
            if (gs.Warnings.Count > 0)
            {
                _log.Warn($"{head}  ·  {gs.Warnings[0]}{(gs.Warnings.Count > 1 ? $"  (+{gs.Warnings.Count - 1} more, see results)" : "")}");
                return;
            }
            _log.Info("    " + head + "  ·  " + DescribeSourceCounts(gs));
        }

        private static string DescribeSourceCounts(GroupRunStats gs)
        {
            if (gs.BoxSetHse) return gs.BoxSetTaggedCount > 0 ? $"{RunLog.Plural(gs.BoxSetTaggedCount, "collection")} tagged" : "collection not found";
            if (gs.SourceType == "MediaInfo") return $"scanned {gs.ListCount:N0} items, {gs.MatchCount} matched";
            if (gs.SourceType == "LocalCollection" || gs.SourceType == "LocalPlaylist") return $"{gs.ListCount} in source, {gs.MatchCount} matched";
            return $"{gs.ListCount} in list, {gs.MatchCount} in your library";
        }

        /// <summary>The per-group block under "Results" — identical for full and single runs.</summary>
        private void WriteGroupBlock(GroupRunStats gs, bool dryRun, bool logMissing)
        {
            _log.Info($"[{gs.GroupIndex}/{gs.GroupTotal}] {gs.DisplayName}  ({gs.SourceLabel})");
            if (gs.Skipped)
            {
                _log.Skip($"Skipped: {gs.SkipReason}");
                _log.Blank();
                return;
            }
            if (gs.ErrorMessage != null)
            {
                _log.Error($"Failed: {gs.ErrorMessage}");
                _log.Detail("Tags, collection and playlist were left unchanged (safety)");
                _log.Blank();
                return;
            }

            bool isRemote = gs.SourceType == "External" || gs.SourceType == "AI" || string.IsNullOrEmpty(gs.SourceType);
            if (gs.BoxSetHse)
            {
                if (gs.BoxSetTaggedCount > 0)
                    _log.Ok($"{RunLog.Plural(gs.BoxSetTaggedCount, "collection")} tagged with \"{gs.TagName}\"");
            }
            else if (gs.SourceType == "MediaInfo")
                _log.Ok($"Scanned {gs.ListCount:N0} items  ·  {gs.MatchCount} matched your conditions");
            else if (gs.SourceType == "LocalCollection" || gs.SourceType == "LocalPlaylist")
            {
                if (gs.ListCount > 0) _log.Ok($"Source: {gs.ListCount} items  ·  {gs.MatchCount} matched");
            }
            else if (gs.ListCount > 0)
                _log.Ok($"List: {gs.ListCount} items  ·  {gs.MatchCount} in your library");

            if (gs.EnableTag && !gs.BoxSetHse)
            {
                if (dryRun) _log.Skip($"Tag \"{gs.TagName}\": would add {gs.TagsAdded}, remove {gs.TagsRemoved} (dry run)");
                else if (gs.TagsAdded == 0 && gs.TagsRemoved == 0) _log.Ok($"Tag \"{gs.TagName}\": up to date");
                else _log.Ok($"Tag \"{gs.TagName}\": +{gs.TagsAdded} added, -{gs.TagsRemoved} removed");
            }

            if (gs.EnableCollection)
            {
                if (dryRun) _log.Skip($"Collection \"{gs.CollectionName}\": not changed (dry run)");
                else if (gs.CollectionCreated) _log.Ok($"Collection \"{gs.CollectionName}\": created with {RunLog.Plural(gs.CollectionItemsAdded, "item")}");
                else if (gs.CollectionItemsAdded > 0 || gs.CollectionItemsRemoved > 0) _log.Ok($"Collection \"{gs.CollectionName}\": updated (+{gs.CollectionItemsAdded}, -{gs.CollectionItemsRemoved})");
                else if (gs.MatchCount == 0) _log.Skip($"Collection \"{gs.CollectionName}\": left unchanged — no items matched");
                else _log.Ok($"Collection \"{gs.CollectionName}\": up to date");
            }

            if (gs.EnablePlaylist)
            {
                if (dryRun) _log.Skip($"Playlist \"{gs.PlaylistName}\": not changed (dry run)");
                else if (gs.PlaylistUsersFailed > 0) _log.Warn($"Playlist \"{gs.PlaylistName}\": failed for {RunLog.Plural(gs.PlaylistUsersFailed, "user")}");
                else if (gs.PlaylistUsersCreated > 0 || gs.PlaylistUsersUpdated > 0)
                {
                    var p = new List<string>();
                    if (gs.PlaylistUsersCreated > 0) p.Add($"created for {RunLog.Plural(gs.PlaylistUsersCreated, "user")}");
                    if (gs.PlaylistUsersUpdated > 0) p.Add($"updated for {RunLog.Plural(gs.PlaylistUsersUpdated, "user")}");
                    _log.Ok($"Playlist \"{gs.PlaylistName}\": {string.Join(", ", p)}");
                }
                else if (gs.PlaylistUsersTotal == 0) _log.Skip($"Playlist \"{gs.PlaylistName}\": no users selected");
                else if (!gs.Warnings.Any(w => w.StartsWith("Playlist", StringComparison.OrdinalIgnoreCase)))
                    _log.Ok($"Playlist \"{gs.PlaylistName}\": up to date for {RunLog.Plural(gs.PlaylistUsersTotal, "user")}");
            }

            if (gs.EnableHomeSection)
            {
                if (dryRun) _log.Skip("Home section: not changed (dry run)");
                else if (gs.HomeSectionSynced) _log.Ok($"Home section: synced for {RunLog.Plural(gs.HomeSectionUserCount, "user")}");
                else if (gs.HomeSectionRemoved) _log.Skip("Home section: removed");
                else _log.Skip("Home section: not synced");
            }

            foreach (var w in gs.Warnings) _log.Warn(w);

            if (isRemote && gs.MissingItems.Count > 0)
            {
                if (logMissing)
                {
                    _log.Skip($"Missing from your library ({gs.MissingItems.Count}):");
                    foreach (var m in gs.MissingItems) _log.Detail(m);
                }
                else
                {
                    _log.Skip($"{RunLog.Plural(gs.MissingItems.Count, "title is", "titles are")} not in your library (enable \"Log missing movies and shows\" in Settings to list them)");
                }
            }
            _log.Blank();
        }

        /// <summary>Results block + Summary for a single-group run.</summary>
        private void WriteSingleRunFooter(GroupRunStats gs, DateTime startTime, bool dryRun, bool logMissing)
        {
            _log.Blank();
            _log.Info("Results");
            WriteGroupBlock(gs, dryRun, logMissing);

            int failed = gs.ErrorMessage != null ? 1 : 0;
            int warned = failed == 0 && gs.Warnings.Count > 0 ? 1 : 0;
            string finalStatus = BuildFinalStatus(dryRun, failed, warned);
            LastRunStatus = $"{finalStatus} ({DateTime.Now:HH:mm})";

            _log.Rule();
            _log.Info("Summary");
            if (gs.EnableTag && !gs.BoxSetHse)
                _log.Info($"  Tags:          +{gs.TagsAdded} added, -{gs.TagsRemoved} removed");
            if (gs.BoxSetHse)
                _log.Info($"  Collections tagged: {gs.BoxSetTaggedCount}");
            if (gs.EnableCollection)
                _log.Info($"  Collections:   {(gs.CollectionCreated ? 1 : 0)} created, {(!gs.CollectionCreated && (gs.CollectionItemsAdded > 0 || gs.CollectionItemsRemoved > 0) ? 1 : 0)} updated");
            if (gs.EnablePlaylist)
                _log.Info($"  Playlists:     {gs.PlaylistUsersCreated} created, {gs.PlaylistUsersUpdated} updated{(gs.PlaylistUsersFailed > 0 ? $", {gs.PlaylistUsersFailed} failed" : "")}");
            if (gs.EnableHomeSection)
                _log.Info($"  Home sections: {(gs.HomeSectionSynced ? 1 : 0)} synced");
            _log.Info($"  Done in {RunLog.Elapsed(DateTime.Now - startTime)}  ·  {StatusSymbol(failed, warned)} {finalStatus}");
            _log.Rule();
        }

        private static string BuildFinalStatus(bool dryRun, int failed, int warned)
        {
            string s = failed > 0 ? $"Completed with {RunLog.Plural(failed, "error")}"
                     : warned > 0 ? $"Completed with {RunLog.Plural(warned, "warning")}"
                     : "Completed";
            return dryRun ? "Dry run — " + s.Replace("Completed", "completed") : s;
        }

        private static string StatusSymbol(int failed, int warned) => failed > 0 ? "✖" : warned > 0 ? "⚠" : "✔";

        /// <summary>Records a home-section warning both in the live log and on the group's stats.</summary>
        private void HsWarn(List<GroupRunStats>? statsList, string tagName, string displayName, string message)
        {
            _log.Warn($"{displayName}: {message}");
            var gs = statsList?.FirstOrDefault(s => s.TagName != null && string.Equals(s.TagName, tagName, StringComparison.OrdinalIgnoreCase));
            gs?.Warnings.Add("Home section: " + message);
        }

        private void WriteMatchedItemsDebug(List<BaseItem> matchedLocalItems)
        {
            if (!_log.Extended || matchedLocalItems.Count == 0) return;
            _log.Debug($"  Matched items ({matchedLocalItems.Count}):");
            int shown = 0;
            foreach (var mi in matchedLocalItems)
            {
                if (shown >= 50) { _log.Debug($"    … and {matchedLocalItems.Count - shown} more"); break; }
                var yr = mi.ProductionYear.HasValue ? $" ({mi.ProductionYear})" : "";
                var tp = mi.GetType().Name.Contains("Series") ? "Series"
                       : mi.GetType().Name.Contains("Episode") ? "Episode"
                       : mi.GetType().Name.Contains("Season") ? "Season"
                       : "Movie";
                _log.Debug($"    {mi.Name}{yr}  [{tp}]");
                shown++;
            }
        }

        private void WriteTagDiffDebug(Dictionary<string, List<string>>? added, Dictionary<string, List<string>>? removed)
        {
            if (!_log.Extended || added == null || removed == null) return;
            if (added.Count == 0 && removed.Count == 0) { _log.Debug("  No tag changes"); return; }
            _log.Section("Tags");
            foreach (var tName in added.Keys.Concat(removed.Keys).Distinct(StringComparer.OrdinalIgnoreCase))
            {
                var a = added.GetValueOrDefault(tName) ?? new List<string>();
                var r = removed.GetValueOrDefault(tName) ?? new List<string>();
                _log.Debug($"  {tName}  (+{a.Count} / -{r.Count})");
                int shown = 0;
                foreach (var lbl in a)
                {
                    if (shown >= 30) { _log.Debug($"    … and {a.Count - shown} more added"); break; }
                    _log.Debug($"    + {lbl}"); shown++;
                }
                shown = 0;
                foreach (var lbl in r)
                {
                    if (shown >= 30) { _log.Debug($"    … and {r.Count - shown} more removed"); break; }
                    _log.Debug($"    - {lbl}"); shown++;
                }
            }
        }

        private void WriteExceptionDebug(Exception ex)
        {
            if (!_log.Extended) return;
            var lines = ex.ToString().Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
            int shown = 0;
            foreach (var line in lines)
            {
                if (shown >= 8) { _log.Debug($"    … {lines.Length - shown} more stack lines"); break; }
                _log.Debug("    " + line.Trim());
                shown++;
            }
        }

        private string BuildRecentlyWatchedContext(TagConfig tagConfig)
        {
            if (!tagConfig.AiIncludeRecentlyWatched || string.IsNullOrWhiteSpace(tagConfig.AiRecentlyWatchedUserId))
                return string.Empty;

            try
            {
                if (!Guid.TryParse(tagConfig.AiRecentlyWatchedUserId, out var userGuid)) return string.Empty;
                var user = _userManager.GetUserById(userGuid);
                if (user == null) return string.Empty;

                int maxCount = tagConfig.AiRecentlyWatchedCount > 0 ? tagConfig.AiRecentlyWatchedCount : 20;

                // Query only played items for this user directly, avoiding per-item GetUserData calls
                var playedLibraryItems = _libraryManager.GetItemList(new InternalItemsQuery
                {
                    IncludeItemTypes = new[] { "Movie", "Series" },
                    User = user,
                    IsPlayed = true,
                    Recursive = true,
                    IsVirtualItem = false
                });

                var playedItems = playedLibraryItems
                    .Select(item => new { item, ud = _userDataManager?.GetUserData(user, item) })
                    .OrderByDescending(x => x.ud?.LastPlayedDate ?? DateTimeOffset.MinValue)
                    .Take(maxCount)
                    .Select(x => x.item)
                    .ToList();

                if (playedItems.Count == 0) return string.Empty;

                var sb = new System.Text.StringBuilder("The user has recently watched these movies and TV shows (most recent first):\n");
                foreach (var item in playedItems)
                {
                    var yearStr = item.ProductionYear.HasValue ? $" ({item.ProductionYear})" : "";
                    var typeStr = item.GetType().Name.Contains("Series") ? "show" : "movie";
                    sb.AppendLine($"- {item.Name}{yearStr} [{typeStr}]");
                }
                sb.AppendLine("Use this to personalize your recommendations.");
                return sb.ToString();
            }
            catch
            {
                return string.Empty;
            }
        }

        private static List<BaseItem> FindByTitleAndYear(List<BaseItem> allItems, string title, int? year)
        {
            if (string.IsNullOrWhiteSpace(title)) return new List<BaseItem>();
            return allItems
                .Where(i =>
                    string.Equals(i.Name, title, StringComparison.OrdinalIgnoreCase)
                    && (year == null || !i.ProductionYear.HasValue || i.ProductionYear == year))
                .ToList();
        }

        private List<string> LoadFileHistory(string filename)
        {
            try { var path = Path.Combine(Plugin.Instance.DataFolderPath, filename); if (File.Exists(path)) return File.ReadAllLines(path).Select(l => l.Trim()).Where(l => !string.IsNullOrEmpty(l)).ToList(); } catch { }
            return new List<string>();
        }

        private void SaveFileHistory(string filename, List<string> data)
        {
            try { var path = Path.Combine(Plugin.Instance.DataFolderPath, filename); Directory.CreateDirectory(Path.GetDirectoryName(path)); File.WriteAllLines(path, data); } catch { }
        }

        private void ApplyCollectionMeta(BaseItem item, string cName,
            Dictionary<string, string> descriptions, Dictionary<string, string> posters,
            Dictionary<string, string> thumbs, Dictionary<string, string> backdrops, bool debug)
        {
            bool metaChanged = false;

            if (descriptions.TryGetValue(cName, out var desc) && !string.IsNullOrWhiteSpace(desc))
            {
                item.Overview = desc;
                metaChanged = true;
            }

            var imageUpdates = new List<(string Path, ImageType Type, string Label)>();
            if (posters.TryGetValue(cName, out var posterPath) && File.Exists(posterPath))
                imageUpdates.Add((posterPath, ImageType.Primary, "poster"));
            if (thumbs.TryGetValue(cName, out var thumbPath) && File.Exists(thumbPath))
                imageUpdates.Add((thumbPath, ImageType.Thumb, "thumb"));
            if (backdrops.TryGetValue(cName, out var backdropPath) && File.Exists(backdropPath))
                imageUpdates.Add((backdropPath, ImageType.Backdrop, "backdrop"));

            if (imageUpdates.Count > 0)
            {
                var updatedTypes = new HashSet<ImageType>(imageUpdates.Select(u => u.Type));
                var otherImages = (item.ImageInfos ?? Array.Empty<ItemImageInfo>())
                    .Where(i => !updatedTypes.Contains(i.Type)).ToList();
                foreach (var (path, type, _) in imageUpdates)
                {
                    otherImages.Add(new ItemImageInfo
                    {
                        Path = path,
                        Type = type,
                        DateModified = File.GetLastWriteTimeUtc(path)
                    });
                }
                item.ImageInfos = otherImages.ToArray();
                _libraryManager.UpdateItem(item, item.Parent, ItemUpdateType.ImageUpdate, null);
                _log.Debug($"  {cName}  →  {string.Join(", ", imageUpdates.Select(u => u.Label))} applied");
            }

            if (metaChanged)
                _libraryManager.UpdateItem(item, item.Parent, ItemUpdateType.MetadataEdit, null);
        }

        private void SyncTopListFolders(PluginConfiguration config, bool dryRun)
        {
            var dataPath = Plugin.Instance.DataFolderPath;
            var topListsPath = Path.Combine(dataPath, "toplists");

            var configuredNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var tl in config.TopLists ?? new List<TopListHomeSection>())
                if (!string.IsNullOrWhiteSpace(tl.TagName))
                    configuredNames.Add(SanitizeTopListFolderName(tl.TagName));

            // Remove folders for tags that no longer exist in config
            if (Directory.Exists(topListsPath))
            {
                foreach (var dir in Directory.GetDirectories(topListsPath))
                {
                    var folderName = Path.GetFileName(dir);
                    if (!configuredNames.Contains(folderName))
                    {
                        if (!dryRun)
                        {
                            try { Directory.Delete(dir, true); }
                            catch (Exception ex) { _log.Warn($"Top-list '{folderName}': folder could not be removed: {ex.Message}"); }
                        }
                        _log.Skip($"Top-list '{folderName}': folder {(dryRun ? "would be removed" : "removed")} (no longer configured)");
                    }
                }
            }

            if (dryRun) return;

            // Build lookup of original (non-strm) movies keyed by IMDb ID so we can link
            // each top-list STRM item to its source as an Emby alternate version.
            var topListsFolder = Path.Combine(dataPath, "toplists") + Path.DirectorySeparatorChar;
            var origLookup = new Dictionary<string, BaseItem>(StringComparer.OrdinalIgnoreCase);
            try
            {
                foreach (var m in _libraryManager.GetItemList(new InternalItemsQuery
                {
                    IncludeItemTypes = new[] { "Movie" },
                    Recursive = true,
                    IsVirtualItem = false
                }))
                {
                    if (string.IsNullOrEmpty(m.Path)) continue;
                    if (m.Path.StartsWith(topListsFolder, StringComparison.OrdinalIgnoreCase)) continue;
                    var imdb = m.GetProviderId("Imdb");
                    if (!string.IsNullOrEmpty(imdb) && !origLookup.ContainsKey(imdb))
                        origLookup[imdb] = m;
                }
            }
            catch { }

            // Build set of tag names that are managed by the plugin's source groups.
            // Manual top-lists have names that don't match any source tag, so they are
            // excluded here — their .strm files are maintained by the UI (PrepareManualFolder)
            // and must not be wiped by the automatic sync.
            var managedTagNames = new HashSet<string>(
                (config.Tags ?? new List<TagConfig>())
                    .Where(t => !string.IsNullOrWhiteSpace(t.Tag))
                    .Select(t => t.Tag.Trim()),
                StringComparer.OrdinalIgnoreCase
            );

            // Recreate .strm/.nfo files for each tag-based top-list
            foreach (var tl in config.TopLists ?? new List<TopListHomeSection>())
            {
                if (string.IsNullOrWhiteSpace(tl.TagName)) continue;
                if (!managedTagNames.Contains(tl.TagName)) continue;

                var sanitized = SanitizeTopListFolderName(tl.TagName);
                var folderPath = Path.Combine(topListsPath, sanitized);
                Directory.CreateDirectory(folderPath);

                foreach (var f in Directory.GetFiles(folderPath, "*.strm")) File.Delete(f);
                foreach (var f in Directory.GetFiles(folderPath, "*.nfo")) File.Delete(f);
                foreach (var f in Directory.GetFiles(folderPath, "*.jpg")) File.Delete(f);

                var badgeStyle = "neutral";
                int effectiveMaxItems = tl.MaxItems;
                if (!string.IsNullOrEmpty(tl.HomeSectionSettings) && tl.HomeSectionSettings != "{}")
                {
                    try
                    {
                        var tlSettings = _jsonSerializer.DeserializeFromString<Dictionary<string, string>>(tl.HomeSectionSettings);
                        if (tlSettings != null && tlSettings.TryGetValue("BadgeStyle", out var bs) && !string.IsNullOrEmpty(bs))
                            badgeStyle = bs;
                        if (effectiveMaxItems <= 0 && tlSettings != null
                            && tlSettings.TryGetValue("MaxItems", out var miStr)
                            && int.TryParse(miStr, out var parsedMax) && parsedMax > 0)
                            effectiveMaxItems = parsedMax;
                    }
                    catch { }
                }

                // Top-list copies are skipped — see PrepareTopListFolderRequest handler.
                var items = _libraryManager.GetItemList(new InternalItemsQuery
                {
                    Tags = new[] { tl.TagName },
                    IncludeItemTypes = new[] { "Movie" },
                    Recursive = true,
                    IsVirtualItem = false
                }).Where(i => string.IsNullOrEmpty(i.Path)
                           || !i.Path.StartsWith(topListsFolder, StringComparison.OrdinalIgnoreCase))
                  .ToList();

                var rankFile = Path.Combine(dataPath, "tag_ranks", sanitized + ".json");
                if (File.Exists(rankFile))
                {
                    try
                    {
                        var rankIds = _jsonSerializer.DeserializeFromFile<List<string>>(rankFile);
                        if (rankIds?.Count > 0)
                        {
                            var rankMap = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                            for (int i = 0; i < rankIds.Count; i++)
                                if (!string.IsNullOrEmpty(rankIds[i])) rankMap[rankIds[i]] = i;
                            items = items.OrderBy(item =>
                            {
                                var imdb = item.GetProviderId("Imdb");
                                return (!string.IsNullOrEmpty(imdb) && rankMap.TryGetValue(imdb, out var rank)) ? rank : int.MaxValue;
                            }).ToList();
                        }
                    }
                    catch { }
                }

                var seenKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                var selected = new List<(string BaseName, string FilePath, BaseItem Item)>();
                foreach (var item in items)
                {
                    if (string.IsNullOrEmpty(item.Path)) continue;
                    var baseName = SanitizeTopListFolderName(item.Name);
                    if (item.ProductionYear.HasValue && item.ProductionYear > 0)
                        baseName += $" ({item.ProductionYear})";
                    if (!seenKeys.Add(baseName)) continue;
                    selected.Add((baseName, item.Path, item));
                }

                if (effectiveMaxItems > 0 && selected.Count > effectiveMaxItems)
                    selected = selected.Take(effectiveMaxItems).ToList();

                int digits = Math.Max(2, selected.Count.ToString().Length);
                int count = 0;
                var tempDir = Path.Combine(Path.GetTempPath(), "hsc_" + Guid.NewGuid().ToString("N"));
                Directory.CreateDirectory(tempDir);
                try
                {
                    foreach (var entry in selected)
                    {
                        count++;
                        var sortPrefix = count.ToString().PadLeft(digits, '0');
                        File.WriteAllText(Path.Combine(folderPath, entry.BaseName + ".nfo"),
                            HomeScreenCompanionService.BuildTopListNfo(entry.Item, sortPrefix));
                        HomeScreenCompanionService.WriteRankedImages(
                            entry.Item, count, Path.Combine(folderPath, entry.BaseName), badgeStyle, tempDir,
                            _httpClient, _providerManager, _libraryManager, _fileSystem, m => _log.Warn(m));
                        // .strm last: the folder is a watched library, and Emby creates the item the
                        // moment it sees the .strm — the nfo and badged images must already be there.
                        File.WriteAllText(Path.Combine(folderPath, entry.BaseName + ".strm"), entry.FilePath);
                    }
                }
                finally
                {
                    try { Directory.Delete(tempDir, true); } catch { }
                }

                // Notify the file system monitor so Emby is aware of the changes
                try
                {
                    _libraryMonitor?.ReportFileSystemChanged(folderPath);
                }
                catch { }

                // For items already indexed in the library: update SortName and poster directly
                // so changes are visible immediately without waiting for a full library scan.
                try
                {
                    var sortPaths = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                    for (int si = 0; si < selected.Count; si++)
                        sortPaths[Path.Combine(folderPath, selected[si].BaseName + ".strm")]
                            = (si + 1).ToString().PadLeft(digits, '0');

                    var libItems = _libraryManager.GetItemList(new InternalItemsQuery
                    {
                        Recursive = true,
                        IncludeItemTypes = new[] { "Movie" }
                    }).Where(i => !string.IsNullOrEmpty(i.Path)
                               && i.Path.StartsWith(folderPath + Path.DirectorySeparatorChar,
                                                    StringComparison.OrdinalIgnoreCase))
                      .ToList();

                    foreach (var li in libItems)
                    {
                        var strmPath = li.Path;
                        if (string.IsNullOrEmpty(strmPath)) continue;

                        // Update SortName
                        if (sortPaths.TryGetValue(strmPath, out var newSort))
                        {
                            var prop = li.GetType().GetProperty("SortName");
                            if (prop?.CanWrite == true) prop.SetValue(li, newSort);
                        }

                        // Point poster and thumb at our local ranked images
                        HomeScreenCompanionService.ApplyRankedImages(li, folderPath);

                        // Merge STRM as an alternate version of the original library movie.
                        // MergeItems replicates what Emby does automatically when two video
                        // files for the same film share a folder, but across different virtual
                        // libraries.
                        try
                        {
                            var strmImdb = li.GetProviderId("Imdb");
                            if (!string.IsNullOrEmpty(strmImdb)
                                && origLookup.TryGetValue(strmImdb, out var primary)
                                && li.Id != primary.Id)
                            {
                                _libraryManager.MergeItems(new[] { primary, li });
                                MergeTopListVersionsTask.QueueStrmProbe(_providerManager, _fileSystem, li);
                            }
                        }
                        catch { }

                        try { _libraryManager.UpdateItem(li, li.Parent, ItemUpdateType.MetadataEdit, null); }
                        catch { }
                    }
                }
                catch { }

                _log.Ok($"Top-list '{tl.TagName}': {RunLog.Plural(count, "movie")} synced to its library folder");
            }
        }

        private static string SanitizeTopListFolderName(string name)
        {
            var invalid = Path.GetInvalidFileNameChars();
            var safe = new string((name ?? "unknown").Select(c => Array.IndexOf(invalid, c) >= 0 ? '_' : c).ToArray()).Trim('.');
            return string.IsNullOrWhiteSpace(safe) ? "unknown" : safe;
        }
    }
}