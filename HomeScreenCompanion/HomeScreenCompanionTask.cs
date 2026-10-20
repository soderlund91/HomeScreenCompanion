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
        private readonly PluginLogger _logger;
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

        private static void PersistLog() { Progress.Clear(); LogStore.Save(LogStore.Sync, ExecutionLog, LastRunStatus, LastStartedUtc); }
        /// <summary>The step a running sync / Run Group is on, shown next to "Running" on the page.</summary>
        internal static RunProgress Progress { get; } = new RunProgress();

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
            public string? GroupName;   // the group's Name (with TagName: its key in GroupIssues)
            public string? SourceType;
            public bool Skipped;
            public string? SkipReason;
            public string? ErrorMessage;
            public int ListCount;
            public int MatchCount;
            public string? LimitShort;   // "9 of 10": the run put fewer titles in than Limit
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
            // A source could not be read (HTTP error, no API key …): reported as an error, but the
            // group is handled exactly as for the matching "returned 0 items" warning.
            public List<string> Errors = new List<string>();
            public List<string> MissingItems = new List<string>(); // list titles not found in the library
            public long ElapsedMs;
        }

        private readonly MediaBrowser.Model.Activity.IActivityManager _activityManager;

        public HomeScreenCompanionTask(ILibraryManager libraryManager, ICollectionManager collectionManager, IPlaylistManager playlistManager, IUserManager userManager, IUserViewManager userViewManager, IUserDataManager userDataManager, IHttpClient httpClient, IJsonSerializer jsonSerializer, ILogManager logManager, ILibraryMonitor libraryMonitor, IProviderManager providerManager, IFileSystem fileSystem, MediaBrowser.Model.Activity.IActivityManager activityManager)
        {
            _activityManager = activityManager;
            _libraryManager = libraryManager;
            _collectionManager = collectionManager;
            _userManager = userManager;
            _userViewManager = userViewManager;
            _userDataManager = userDataManager;
            _httpClient = httpClient;
            _jsonSerializer = jsonSerializer;
            _logger = PluginLog.Wrap(logManager.GetLogger("HomeScreenCompanion"), "Sync");
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
            // One run at a time (see RunGate): a running Run Group or other sync finishes first.
            using (await RunGate.EnterAsync(Name, _logger, cancellationToken).ConfigureAwait(false))
                await ExecuteCore(cancellationToken, progress).ConfigureAwait(false);
        }

        private async Task ExecuteCore(CancellationToken cancellationToken, IProgress<double> progress)
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
                _deletedGroupRowsRemoved = 0;
                _userStatus = UserStatus.Read(_userManager);

                // The detail lines are always made: the plugin's log file gets them all, the page
                // shows them only with Extended log on (RunLog).
                bool debug = true;
                bool dryRun = config.DryRunMode;
                bool logMissing = config.LogMissingItems;
                _log = new RunLog(ExecutionLog, _logger, "", config.ExtendedConsoleOutput, Progress);

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
                var topListsFolder = Path.Combine(Plugin.Instance!.DataFolderPath, "toplists") + Path.DirectorySeparatorChar;

                var imdbLookup = new Dictionary<string, List<BaseItem>>(StringComparer.OrdinalIgnoreCase);
                foreach (var item in allItems)
                {
                    if (item.LocationType != LocationType.FileSystem) continue;
                    if (!string.IsNullOrEmpty(item.Path) &&
                        (item.Path.StartsWith(topListsFolder, StringComparison.OrdinalIgnoreCase) || RankedCollections.IsRankedPath(item.Path)))
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
                    || !(i.Path.StartsWith(topListsFolder, StringComparison.OrdinalIgnoreCase) || RankedCollections.IsRankedPath(i.Path))).ToList();

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
                // Ranked collections (experimental): each collection's items in list order.
                var collectionOrder = new Dictionary<string, List<BaseItem>>(StringComparer.OrdinalIgnoreCase);
                var collectionDescriptions = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                var collectionSortNames = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                var collectionPosters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                var collectionSources = new Dictionary<string, TagConfig>(StringComparer.OrdinalIgnoreCase);
                // Tag art (ApplyTagArt): the source that owns each tag and its titles in list order,
                // across all flat entries of the tag.
                var tagArtSources = new Dictionary<string, TagConfig>(StringComparer.OrdinalIgnoreCase);
                var tagArtTitles = new Dictionary<string, List<BaseItem>>(StringComparer.OrdinalIgnoreCase);
                var managedTags = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                var activeCollections = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                var failedFetches = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                // A group with several sources (URLs / local sources) is stored as one flat TagConfig per
                // source. Playlists and rank files must be built from the union of all sources in the group,
                // so they are accumulated here and written once after the loop.
                var groupPlaylistItems = new Dictionary<string, (TagConfig Owner, List<BaseItem> Items, HashSet<Guid> Seen)>(StringComparer.OrdinalIgnoreCase);
                // "Your Next Watch": one list per user, written to that user's own playlist.
                var nextWatchByGroup = new Dictionary<string, (TagConfig Owner, Dictionary<string, List<BaseItem>> PerUser)>(StringComparer.OrdinalIgnoreCase);
                var rankIdsByTag = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
                // Each group's matched titles in list order, per tag, for the source's top-list (TopListMembers),
                // and the groups that failed with their titles kept (their part is kept too).
                var memberItemsByGroup = new Dictionary<string, Dictionary<string, List<BaseItem>>>(StringComparer.OrdinalIgnoreCase);
                var failedGroups = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                // Groups where a source failed / returned nothing — their playlists are left untouched
                var playlistGroupsToSkip = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                // managedTags = tags of every configured group + tags from the history file. The
                // history holds tags of groups that have since been deleted, so this run can take
                // them off the items once; afterwards they are dropped from the history (see
                // below) and the tag is free to be used by hand again.
                var previouslyManagedTags = LoadFileHistory("homescreencompanion_history.txt");
                foreach (var t in previouslyManagedTags) managedTags.Add(t);
                var configuredTags = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                // Tags of deleted groups that could not be taken off every item — kept in the
                // history so the next run tries again.
                var orphanCleanupFailed = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                var previouslyManagedCollections = LoadFileHistory("homescreencompanion_collections.txt");
                // Imported collections (TagConfig.CollectionImported): remembered for good, so they
                // are kept even after their source is gone from the config.
                var importedCollections = RecordImportedCollections(config);
                // Also track collection names from inactive groups so they get cleaned up
                // even if their collection was never recorded in the history file
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
                        var p = SplitTextCriterion(c);
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
                    Dictionary<string, List<long>>? versionIdsByKey = null;
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
                            collectionMembershipCache[indivKey] = MembershipIds(members, allItems, ref versionIdsByKey);
                        }
                    }
                }

                var mediaInfoCache = new Dictionary<long, CachedMediaInfo>();
                if (config.Tags.Any(t => t.Active && NeedsMediaDetails(t)))
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
                StartPopularity(allItems);
                StartPlaybackHistory(config, config.Tags.Where(t => t.Active));
                var preloadedUsers = _userManager.GetUserList(new UserQuery { IsDisabled = false });

                var activeTagOverrides = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                var activeCollectionOverrides = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                // When several overrides for one tag (or collection) are in schedule at once, the first
                // one in the list that finds items owns it; the others are skipped.
                var overrideOwnerByTag = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                var overrideOwnerByCollection = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
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
                            userDataCache[_k] = UserDataOf(_user, _topItem);
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
                                if (userDataCache.TryGetValue((_user.Id, _ep.InternalId), out var _cd) && Played(_user, _ep, _cd.LastPlayedDate) is DateTimeOffset _epLp)
                                    if (_max == null || _epLp > _max) _max = _epLp;
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
                var collCreatedArt = new List<(string Name, TagConfig Source, IList<BaseItem> Items)>();
                var collItemsAdded = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                var collItemsRemoved = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

                // Process OverrideWhenActive entries first so they can remove themselves from
                // activeTagOverrides before non-override entries for the same tag are evaluated.
                var orderedTags = config.Tags
                    .Where(t => t.OverrideWhenActive)
                    .Concat(config.Tags.Where(t => !t.OverrideWhenActive))
                    .ToList();

                // A group with several URLs is stored as one flat entry per URL. Its fetch lines wait
                // until its last entry is done: only then is it known whether every URL came back
                // empty, and the "other lists were used" messages become "being cleared".
                var fetchEntriesLeft = orderedTags.Where(t => t.Active && !string.IsNullOrWhiteSpace(t.Tag))
                    .GroupBy(GroupKey, StringComparer.OrdinalIgnoreCase)
                    .ToDictionary(g => g.Key, g => g.Count(), StringComparer.OrdinalIgnoreCase);
                var heldFetchLines = new Dictionary<string, List<GroupRunStats>>(StringComparer.OrdinalIgnoreCase);
                var groupsWithItems = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                var clearedIfAllEmpty = new Dictionary<string, List<(List<string> Messages, int Index, string Text)>>(StringComparer.OrdinalIgnoreCase);
                void FlushFetchLines(string key)
                {
                    if (!groupsWithItems.Contains(key) && clearedIfAllEmpty.TryGetValue(key, out var notes))
                        foreach (var (messages, index, text) in notes) messages[index] = text;
                    clearedIfAllEmpty.Remove(key);
                    if (heldFetchLines.TryGetValue(key, out var held))
                        foreach (var g in held) WriteFetchLine(g);
                    heldFetchLines.Remove(key);
                }
                void FetchLineDone(GroupRunStats g, TagConfig source)
                {
                    var key = GroupKey(source);
                    if (!g.Skipped && g.ListCount > 0) groupsWithItems.Add(key);
                    if (!heldFetchLines.TryGetValue(key, out var held)) heldFetchLines[key] = held = new List<GroupRunStats>();
                    held.Add(g);
                    int left = fetchEntriesLeft.TryGetValue(key, out var n) ? n - 1 : 0;
                    fetchEntriesLeft[key] = left;
                    if (left <= 0) FlushFetchLines(key);
                }

                foreach (var tagConfig in orderedTags)
                {
                    if (string.IsNullOrWhiteSpace(tagConfig.Tag)) continue;
                    string tagName = tagConfig.Tag.Trim();
                    managedTags.Add(tagName); // track all groups (active or inactive) so cleanup always runs
                    configuredTags.Add(tagName);
                    NormalizeNextWatch(tagConfig);

                    if (!tagConfig.Active) continue;

                    string displayName = !string.IsNullOrWhiteSpace(tagConfig.Name) ? $"{tagConfig.Name} [{tagName}]" : tagName;
                    string srcLabel = string.IsNullOrEmpty(tagConfig.SourceType) ? "External" : tagConfig.SourceType;
                    var ruleFeatures = new List<string>();
                    if (tagConfig.EnableTag && !tagConfig.OnlyCollection) ruleFeatures.Add("Tag");
                    if (tagConfig.EnableCollection) ruleFeatures.Add("Collection");
                    if (SourceTopLists.PlainRow(tagConfig)) ruleFeatures.Add("HS");
                    if (SourceTopLists.IsOn(tagConfig)) ruleFeatures.Add("Top-list");
                    string featureStr = ruleFeatures.Count > 0 ? $"  ({string.Join(", ", ruleFeatures)})" : "";

                    activeGroupIdx++;
                    var gs = new GroupRunStats
                    {
                        DisplayName = displayName,
                        GroupName = tagConfig.Name,
                        SourceType = srcLabel,
                        EnableTag = tagConfig.EnableTag && !tagConfig.OnlyCollection,
                        EnableCollection = tagConfig.EnableCollection,
                        EnableHomeSection = SourceTopLists.PlainRow(tagConfig),
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
                        FetchLineDone(gs, tagConfig);
                        continue;
                    }

                    if (tagConfig.SourceType == "AI" && tagConfig.AiRefreshIntervalDays > 0 &&
                        tagConfig.AiLastRunDate > DateTime.MinValue &&
                        (DateTime.UtcNow - tagConfig.AiLastRunDate).TotalDays < tagConfig.AiRefreshIntervalDays)
                    {
                        var _nextAiRun = tagConfig.AiLastRunDate.AddDays(tagConfig.AiRefreshIntervalDays);
                        gs.Skipped = true;
                        gs.SkipReason = $"AI refresh not due until {_nextAiRun:yyyy-MM-dd}";
                        // Not asked this time: the last answer stays — its tags, collection and playlist are kept.
                        failedFetches.Add(tagName);
                        failedGroups.Add(GroupKey(tagConfig));
                        if (tagConfig.EnableCollection)
                        {
                            string _aiCName = string.IsNullOrWhiteSpace(tagConfig.CollectionName) ? tagName : tagConfig.CollectionName.Trim();
                            failedFetches.Add(_aiCName);
                            activeCollections.Add(_aiCName);
                        }
                        playlistGroupsToSkip.Add(GroupKey(tagConfig));
                        statsList.Add(gs);
                        FetchLineDone(gs, tagConfig);
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
                        FetchLineDone(gs, tagConfig);
                        continue;
                    }
                    if (tagConfig.OverrideWhenActive &&
                        ((overrideOwnerByTag.TryGetValue(tagName, out var _tagOwner) && _tagOwner != GroupKey(tagConfig)) ||
                         (tagConfig.EnableCollection && overrideOwnerByCollection.TryGetValue(cName, out var _collOwner) && _collOwner != GroupKey(tagConfig))))
                    {
                        gs.Skipped = true;
                        gs.SkipReason = "another priority group higher in the list is active for the same tag";
                        statsList.Add(gs);
                        FetchLineDone(gs, tagConfig);
                        continue;
                    }
                    if (tagConfig.EnableCollection)
                    {
                        activeCollections.Add(cName);
                        collectionSortNames[cName] = (tagConfig.CollectionSortName ?? "").Trim();
                        CollectCollectionMeta(tagConfig, cName, collectionDescriptions, collectionPosters);
                        collectionSources[cName] = tagConfig;
                    }

                    // Saved without a URL / local source yet: skipped, and its tags, collection, playlist
                    // and home row stay exactly as they are (never treated as an empty list).
                    if (HasNoSourceYet(tagConfig))
                    {
                        failedFetches.Add(tagName);
                        failedGroups.Add(GroupKey(tagConfig));
                        if (tagConfig.EnableCollection) failedFetches.Add(cName);
                        playlistGroupsToSkip.Add(GroupKey(tagConfig));
                        gs.Skipped = true;
                        gs.SkipReason = NoSourceYetReason(tagConfig);
                        statsList.Add(gs);
                        FetchLineDone(gs, tagConfig);
                        currentProgress += step;
                        progress.Report(currentProgress);
                        continue;
                    }

                    _log.Step($"Fetching sources — [{gs.GroupIndex}/{gs.GroupTotal}] {displayName}");
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
                        string? fetchError = null;   // why a list / the AI came back empty (ListFetcher.LastError)
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
                        var passesFilters = FilterConditionsCheck(tagConfig, seriesEpisodeCache, personCache, userDataCache, mediaInfoCache,
                            preloadedUsers, seriesLastPlayedCache, collectionMembershipCache, seriesEpisodeNamesCache);

                        if (string.IsNullOrEmpty(tagConfig.SourceType) || tagConfig.SourceType == "External")
                        {
                            var fetchTimer = System.Diagnostics.Stopwatch.StartNew();
                            var items = await fetcher.FetchItems(tagConfig.Url!, effectiveLimit, config.TraktClientId, config.MdblistApiKey, config.TmdbApiKey, cancellationToken,
                                CountsTowardLimit(imdbLookup, blacklist, passesFilters));
                            fetchTimer.Stop();
                            fetchError = fetcher.LastError;
                            // Empty with "Preserve…" on: the URL's last good entries stand in for it (see LastGoodLists).
                            if (items.Count > 0) { if (!dryRun) LastGoodLists.Save(tagConfig.Url!, items, _jsonSerializer); }
                            else if (config.PreserveTagsOnEmptyResult && LastGoodLists.Load(tagConfig.Url!, _jsonSerializer) is var _lastGood && _lastGood != null)
                            {
                                items = _lastGood.Value.Items;
                                (ListFetcher.IsFailure(fetchError) ? gs.Errors : gs.Warnings).Add(StandInMessage(tagConfig.Url!, fetchError, _lastGood.Value.SavedUtc));
                            }
                            gs.ListCount = items.Count;
                            int _extBlacklisted = 0, _extNoImdb = 0;

                            if (items.Count > 0)
                            {
                                foreach (var extItem in items)
                                {
                                    if (string.IsNullOrEmpty(extItem.Imdb)) { _extNoImdb++; continue; }

                                    if (blacklist.Contains(extItem.Imdb))
                                    {
                                        _extBlacklisted++;
                                        _log.Debug($"    Blacklisted: {extItem.Name} ({extItem.Imdb})");
                                        continue;
                                    }

                                    if (tagConfig.EnableTag && !tagConfig.OnlyCollection && !HasFilterConditions(tagConfig))
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

                                        if (passesFilters != null && !passesFilters(itemToTag)) continue;

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
                        else if (tagConfig.SourceType == "Manual")
                        {
                            matchedLocalItems = ManualItems(tagConfig, blacklist, effectiveLimit, gs, passesFilters);
                            gs.ListCount = tagConfig.ManualItemIds?.Count ?? 0;
                            _log.Debug($"  {matchedLocalItems.Count} of {gs.ListCount} hand-picked items found");
                        }
                        else if (IsNextWatch(tagConfig))
                        {
                            Func<BaseItem, bool>? _nwAllow = null;
                            if (tagConfig.MediaInfoFilters?.Count > 0 || tagConfig.MediaInfoConditions?.Count > 0)
                                _nwAllow = item => ItemMatchesMediaInfo(item, tagConfig, debug, seriesEpisodeCache, personCache, userDataCache,
                                    mediaInfoCache.TryGetValue(item.InternalId, out var _nwCi) ? _nwCi : (CachedMediaInfo?)null,
                                    preloadedUsers, seriesLastPlayedCache, collectionMembershipCache, seriesEpisodeNamesCache);
                            var _nwPerUser = ComputeNextWatch(tagConfig, matchableItems, config, blacklist, _nwAllow, gs, NextWatchUserIds(tagConfig));
                            if (_nwPerUser.Count > 0 && !dryRun)
                                nextWatchByGroup[GroupKey(tagConfig)] = (tagConfig, _nwPerUser);
                            // The source's shared items (a tag or collection, when enabled): the titles picked
                            // for the most users — the same list the playlist art is drawn from.
                            matchedLocalItems = PlaylistArtItems(_nwPerUser.Values);
                        }
                        // MediaInfo post-filter for list sources (External, AI). Local collections /
                        // playlists and Manual lists were already filtered while they were read.
                        bool _filteredWhileRead = passesFilters != null
                            && (tagConfig.SourceType == "LocalCollection" || tagConfig.SourceType == "LocalPlaylist" || tagConfig.SourceType == "Manual");
                        if (tagConfig.SourceType != "MediaInfo" && !_filteredWhileRead && matchedLocalItems.Count > 0
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
                                    if (PopularRuleDays(tagConfig) == null && effectiveLimit < 10000 && matchedLocalItems.Count >= effectiveLimit) break;
                                }
                            }
                            OrderByPopularity(tagConfig, matchedLocalItems, effectiveLimit);
                            if (!dryRun)
                                RankWeekAgo(tagName, tagConfig, itemsToScan.Where(i => i.LocationType == LocationType.FileSystem && !blacklist.Contains(i.GetProviderId("Imdb") ?? "")),
                                    i => ItemMatchesMediaInfo(i, tagConfig, debug, seriesEpisodeCache, personCache, userDataCache, mediaInfoCache.TryGetValue(i.InternalId, out var wci) ? wci : (CachedMediaInfo?)null,
                                        preloadedUsers, seriesLastPlayedCache, collectionMembershipCache, seriesEpisodeNamesCache), effectiveLimit);
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
                                AiAskCount(effectiveLimit),
                                cancellationToken);
                            fetchTimer.Stop();

                            gs.ListCount = aiItems.Count;
                            fetchError = fetcher.LastError;
                            int _aiTitleMatched = 0, _aiBlacklisted = 0;

                            foreach (var aiItem in aiItems)
                            {
                                // Asked for more than the limit (see AiAskCount): the first hits fill it.
                                if (effectiveLimit < 10000 && TitleCount(matchedLocalItems) >= effectiveLimit) break;
                                if (string.IsNullOrWhiteSpace(aiItem.title)) continue;
                                string _aiLabel = aiItem.year.HasValue ? $"{aiItem.title} ({aiItem.year})" : aiItem.title;

                                if (!string.IsNullOrEmpty(aiItem.imdb_id))
                                {
                                    var imdbId = aiItem.imdb_id!.Trim();
                                    if (blacklist.Contains(imdbId))
                                    {
                                        _aiBlacklisted++;
                                        _log.Debug($"    Blacklisted: {_aiLabel} ({imdbId})");
                                        continue;
                                    }

                                    if (tagConfig.EnableTag && !tagConfig.OnlyCollection && !HasFilterConditions(tagConfig))
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
                            if (tagConfig.AiRefreshIntervalDays > 0 && aiItems.Count > 0)
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
                        // List sources count titles ("54 in list, 50 in your library"), not Emby items: a film
                        // with two versions, or a show tagged with its seasons, is one title.
                        if (!IsNextWatch(tagConfig)) gs.MatchCount = tagConfig.SourceType == "MediaInfo" ? allOutputIds.Count : TitleCount(matchedLocalItems);
                        matchCount += allOutputIds.Count;
                        if (!dryRun && !gs.BoxSetHse) RecordLastRun(new[] { (tagConfig, TitleCount(matchedLocalItems)) }, gs);

                        // Collect rank order so top-list .strm files can be numbered in list order.
                        // Accumulated across all flat entries of the tag; written once after the loop.
                        if (!rankIdsByTag.TryGetValue(tagName, out var rankIds))
                        {
                            rankIds = new List<string>();
                            rankIdsByTag[tagName] = rankIds;
                        }
                        var rankSeen = new HashSet<string>(rankIds, StringComparer.OrdinalIgnoreCase);
                        foreach (var rankId in matchedLocalItems.Select(RankKey).Where(id => !string.IsNullOrEmpty(id)))
                            if (rankSeen.Add(rankId)) rankIds.Add(rankId);
                        if (!memberItemsByGroup.TryGetValue(tagName, out var memberGroups))
                            memberItemsByGroup[tagName] = memberGroups = new Dictionary<string, List<BaseItem>>(StringComparer.OrdinalIgnoreCase);
                        if (!memberGroups.TryGetValue(GroupKey(tagConfig), out var memberItems)) memberGroups[GroupKey(tagConfig)] = memberItems = new List<BaseItem>();
                        memberItems.AddRange(matchedLocalItems);

                        // If this is a priority-override entry but produced zero results,
                        // remove it from the override sets so other entries for the same tag are not suppressed.
                        if (tagConfig.OverrideWhenActive && allOutputIds.Count == 0)
                        {
                            activeTagOverrides.Remove(tagName);
                            if (tagConfig.EnableCollection) activeCollectionOverrides.Remove(cName);
                        }
                        else if (tagConfig.OverrideWhenActive)
                        {
                            if (!overrideOwnerByTag.ContainsKey(tagName)) overrideOwnerByTag[tagName] = GroupKey(tagConfig);
                            if (tagConfig.EnableCollection && !overrideOwnerByCollection.ContainsKey(cName)) overrideOwnerByCollection[cName] = GroupKey(tagConfig);
                        }

                        // For External and AI sources: if the remote returned zero items and the user has
                        // opted to preserve tags on empty results, treat it as a failed fetch.
                        bool isRemoteSource = string.IsNullOrEmpty(tagConfig.SourceType) || tagConfig.SourceType == "External" || tagConfig.SourceType == "AI";
                        // A group with several URLs gets one message per URL: say which one.
                        string emptySrc = isRemoteSource && gs.ListCount == 0 && tagConfig.SourceType != "AI"
                            && config.Tags.Count(t => string.Equals(GroupKey(t), GroupKey(tagConfig), StringComparison.OrdinalIgnoreCase)) > 1
                            ? (tagConfig.Url ?? "").Trim() + ": " : "";
                        var emptyList = ListFetcher.IsFailure(fetchError) ? gs.Errors : gs.Warnings;
                        // One URL of several that does not exist (404) is left out: keeping the whole group
                        // as it was would stop it from ever losing a title (see LastGoodLists for URLs that
                        // were read before).
                        bool goneInGroup = emptySrc.Length > 0 && ListFetcher.IsGone(fetchError);
                        if (isRemoteSource && gs.ListCount == 0 && goneInGroup)
                        {
                            emptyList.Add($"{emptySrc}the list does not exist{Why(fetchError)} — it was left out and the group's other lists were used. Remove the URL from the source or fix it.");
                        }
                        else if (isRemoteSource && gs.ListCount == 0 && config.PreserveTagsOnEmptyResult)
                        {
                            emptyList.Add(emptySrc + (tagConfig.SourceType == "AI"
                                ? $"The AI returned 0 items{Why(fetchError)} — existing tags, collection, playlist and top-list were kept. Check the prompt and the API key in Settings."
                                : $"The list returned 0 items{Why(fetchError)} — existing tags, collection, playlist and top-list were kept. Check the list URL and the API key in Settings."));
                            failedFetches.Add(tagName);
                            failedGroups.Add(GroupKey(tagConfig));
                            if (tagConfig.EnableCollection) failedFetches.Add(cName);
                            playlistGroupsToSkip.Add(GroupKey(tagConfig));
                            gs.ElapsedMs = groupTimer.ElapsedMilliseconds;
                            statsList.Add(gs);
                            FetchLineDone(gs, tagConfig);
                            currentProgress += step;
                            progress.Report(currentProgress);
                            continue;
                        }
                        else if (isRemoteSource && gs.ListCount == 0 && emptySrc.Length > 0)
                        {
                            // Same wording as Run Group: the group's other URLs still supply its items,
                            // unless they all come back empty (FlushFetchLines then swaps in the second text).
                            emptyList.Add($"{emptySrc}the list returned 0 items{Why(fetchError)} — the group's other lists were used");
                            var groupKey = GroupKey(tagConfig);
                            if (!clearedIfAllEmpty.TryGetValue(groupKey, out var notes)) clearedIfAllEmpty[groupKey] = notes = new List<(List<string>, int, string)>();
                            notes.Add((emptyList, emptyList.Count - 1,
                                $"{emptySrc}the list returned 0 items{Why(fetchError)} — the group's tags, collection and playlist are being cleared (\"Preserve tags and collections on empty result\" is off in Settings). Check the list URLs and the API key."));
                        }
                        else if (isRemoteSource && gs.ListCount == 0)
                            emptyList.Add(tagConfig.SourceType == "AI"
                                ? $"The AI returned 0 items{Why(fetchError)} — its tags, collection and playlist are being cleared (\"Preserve tags and collections on empty result\" is off in Settings)"
                                : $"The list returned 0 items{Why(fetchError)} — its tags, collection and playlist are being cleared (\"Preserve tags and collections on empty result\" is off in Settings). Check the list URL and the API key.");

                        if (tagConfig.EnableTag && !tagConfig.OnlyCollection && !IsBoxSetHomeSectionEntry(tagConfig))
                        {
                            if (!tagArtSources.ContainsKey(tagName)) { tagArtSources[tagName] = tagConfig; tagArtTitles[tagName] = new List<BaseItem>(); }
                            tagArtTitles[tagName].AddRange(matchedLocalItems);
                            foreach (var localItem in tagOutputItems)
                            {
                                if (!desiredTagsMap.ContainsKey(localItem.Id))
                                    desiredTagsMap[localItem.Id] = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                                desiredTagsMap[localItem.Id].Add(tagName);

                                // The real-time tag cache is keyed by IMDb id, so only list sources belong in
                                // it (cached from the list itself above, unless the source has filter
                                // conditions: then only the titles that passed them, i.e. these). MediaInfo
                                // and local collection/playlist results are per item: caching them would
                                // re-tag every other item with the same IMDb id (other versions, top-list
                                // copies) on its next update, straight after this sync removed the tag.
                                var imdb = localItem.GetProviderId("Imdb");
                                bool listSource = IsListSource(tagConfig);
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
                            if (!collectionOrder.TryGetValue(cName, out var _order)) collectionOrder[cName] = _order = new List<BaseItem>();
                            _order.AddRange(collectionOutputItems);
                        }

                        // Collect this entry's items for the group's playlist sync (done once per group after
                        // the loop so all sources of a multi-source group end up in the same playlist).
                        // Placed here so it inherits the loop's skip/preserve/override guards above.
                        if (tagConfig.EnablePlaylist && !IsNextWatch(tagConfig))
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
                        failedGroups.Add(GroupKey(tagConfig));
                        if (tagConfig.EnableCollection) failedFetches.Add(cName);
                        playlistGroupsToSkip.Add(GroupKey(tagConfig));
                    }

                    gs.ElapsedMs = groupTimer.ElapsedMilliseconds;
                    statsList.Add(gs);
                    FetchLineDone(gs, tagConfig);
                    currentProgress += step;
                    progress.Report(currentProgress);
                }
                foreach (var key in heldFetchLines.Keys.ToList()) FlushFetchLines(key);
                _log.Debug($"Fetch phase done in {RunLog.Elapsed(phaseTimer.Elapsed)}");

                // Playlist sync — once per group, with the union of all its sources.
                // Skipped for groups where any source failed, so a bad fetch never empties the playlist.
                if (groupPlaylistItems.Count > 0 || nextWatchByGroup.Count > 0)
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
                foreach (var kvp in nextWatchByGroup)
                {
                    statsByGroupKey.TryGetValue(kvp.Key, out var nwStats);
                    await SyncPlaylistsForEntryAsync(kvp.Value.Owner, new List<BaseItem>(), dryRun, nwStats, kvp.Value.PerUser);
                }
                if ((groupPlaylistItems.Count > 0 || nextWatchByGroup.Count > 0) && !dryRun)
                {
                    int _plCreated = statsList.Sum(g => g.PlaylistUsersCreated), _plUpdated = statsList.Sum(g => g.PlaylistUsersUpdated), _plFailed = statsList.Sum(g => g.PlaylistUsersFailed);
                    _log.Info($"    {_plCreated} created, {_plUpdated} updated{(_plFailed > 0 ? $", {_plFailed} failed" : "")}  ·  {RunLog.Elapsed(phaseTimer.Elapsed)}");
                }

                if (!dryRun)
                {
                    foreach (var kvp in rankIdsByTag)
                        WriteRankFile(kvp.Key, kvp.Value);
                    SaveTopListMembers(config, memberItemsByGroup, failedGroups);
                    TagCacheManager.Instance.Save();
                    LastGoodLists.Prune(config.Tags.Where(t => string.IsNullOrEmpty(t.SourceType) || t.SourceType == "External").Select(t => t.Url ?? ""));
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
                int _tagScanDone = 0;
                foreach (var item in allItems)
                {
                    _log.Count("Applying tags:", ++_tagScanDone, allItems.Count, "items");
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
                        catch (Exception ex)
                        {
                            _log.Warn($"Could not save tags for '{item.Name}': {ex.Message}");
                            foreach (var t in toRemove) if (!configuredTags.Contains(t)) orphanCleanupFailed.Add(t);
                        }
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
                        catch (Exception ex)
                        {
                            _log.Warn($"Could not save tags for '{item.Name}': {ex.Message}");
                            foreach (var t in toRemove) if (!configuredTags.Contains(t)) orphanCleanupFailed.Add(t);
                        }
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
                        catch (Exception ex)
                        {
                            _log.Warn($"Could not save tags for season '{item.Name}': {ex.Message}");
                            foreach (var t in toRemove) if (!configuredTags.Contains(t)) orphanCleanupFailed.Add(t);
                        }
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
                // Saved after the clean-up above, not before: a deleted group's tag stays in the
                // history only until it has been taken off its items. Keeping every tag ever
                // managed made the plugin strip a name forever, even when it was later added by hand.
                if (!dryRun)
                    SaveFileHistory("homescreencompanion_history.txt", configuredTags.Concat(orphanCleanupFailed).ToList());

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
                if (!dryRun)
                    foreach (var kvp in tagArtSources)
                        ApplyTagArt(kvp.Key, kvp.Value, tagArtTitles[kvp.Key].GroupBy(i => i.Id).Select(g => g.First()).ToList());

                _log.Blank();
                _log.Info("» Collections");
                phaseTimer.Restart();
                int collCreated = 0, collUpdated = 0, collWouldCreate = 0, collWouldUpdate = 0;
                _log.Section("Collections");
                // Ranked collections (experimental): sources that keep it (also when their list failed
                // to load this time); every other ranked folder, its copies and library go first, so
                // this run puts the real films back.
                var rankedSources = collectionSources
                    .Where(c => RankedCollections.Wants(config, c.Value))
                    .ToDictionary(c => c.Key, c => c.Value, StringComparer.OrdinalIgnoreCase);
                if (!dryRun)
                    RankedCollections.Cleanup(new HashSet<string>(rankedSources.Values.Select(RankedCollections.FolderName), StringComparer.OrdinalIgnoreCase),
                        null, _libraryManager, _collectionManager, _userManager, _log);
                int _collIdx = 0;
                var allItemsById = ItemsById(allItems);
                foreach (var kvp in desiredCollectionsMap)
                {
                    string cName = kvp.Key;
                    _log.Step($"Collections — [{++_collIdx}/{desiredCollectionsMap.Count}] {cName}");
                    var _extraVersions = ExtraVersionIds(OrderedItems(allItemsById, kvp.Value));
                    var desiredIds = kvp.Value.Where(id => !_extraVersions.Contains(id)).ToHashSet();
                    if (desiredIds.Count == 0) continue;
                    bool _ranked = false;
                    List<BaseItem>? _rankProbe = null;
                    if (!dryRun && rankedSources.TryGetValue(cName, out var _rankSrc) && collectionOrder.TryGetValue(cName, out var _rankOrder))
                    {
                        try
                        {
                            _log.Step($"Ranked collections — {cName}");
                            var _rankedIds = await RankedCollections.SyncAsync(_rankSrc, cName, _rankOrder, _libraryManager, _providerManager,
                                _fileSystem, _userManager, _collectionManager, config, _log, cancellationToken);
                            if (_rankedIds != null)
                            {
                                desiredIds = _rankedIds.Value.Members.ToHashSet(); _rankProbe = _rankedIds.Value.ToProbe; _ranked = true;
                                // Before the members change: that queues a refresh of the collection, which
                                // would save the order it loaded over a later change.
                                RankedCollections.SetDisplayOrder(_libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "BoxSet" }, Name = cName, Recursive = true }).FirstOrDefault(), _libraryManager);
                            }
                        }
                        catch (OperationCanceledException) { throw; }
                        catch (Exception ex) { _log.Warn($"Ranked collection '{cName}' failed, the real films are used: {ex.Message}"); }
                    }

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
                                if (collectionDescriptions.ContainsKey(cName) || collectionPosters.ContainsKey(cName))
                                    ApplyCollectionMeta(createdRef, cName, collectionDescriptions, collectionPosters, debug);
                                ApplyCollectionSortName(createdRef, cName, collectionSortNames.GetValueOrDefault(cName));
                                if (collectionSources.TryGetValue(cName, out var _artSrc))
                                {
                                    var _artItems = _ranked ? RankedArtItems(collectionOrder[cName]) : OrderedItems(allItemsById, desiredIds);
                                    ApplyCollectionArt(createdRef, cName, _artSrc, _artItems);
                                    collCreatedArt.Add((cName, _artSrc, _artItems));
                                }
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
                                    string CollLabel(long id) => allItemsById.TryGetValue(id, out var _ci) ? _ci.Name + (_ci.ProductionYear.HasValue ? $" ({_ci.ProductionYear})" : "") : id.ToString();
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
                            if (!dryRun && (collectionDescriptions.ContainsKey(cName) || collectionPosters.ContainsKey(cName)))
                                ApplyCollectionMeta(existingColl, cName, collectionDescriptions, collectionPosters, debug);
                            if (!dryRun)
                                ApplyCollectionSortName(existingColl, cName, collectionSortNames.GetValueOrDefault(cName));
                            if (!dryRun && collectionSources.TryGetValue(cName, out var _artSrc2))
                                ApplyCollectionArt(existingColl, cName, _artSrc2, _ranked ? RankedArtItems(collectionOrder[cName]) : OrderedItems(allItemsById, desiredIds));
                        }
                        if (_ranked)
                            RankedCollections.SetDisplayOrder(_libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "BoxSet" }, Name = cName, Recursive = true }).FirstOrDefault(), _libraryManager);
                        RankedCollections.Probe(_rankProbe, _providerManager, _fileSystem);
                    }
                    catch (Exception ex)
                    {
                        _log.Error($"Collection \"{cName}\" could not be updated: {ex.Message}");
                        WriteExceptionDebug(ex);
                        foreach (var _gsC in statsList.Where(g => string.Equals(g.CollectionName, cName, StringComparison.OrdinalIgnoreCase)))
                            _gsC.Warnings.Add($"Collection could not be updated: {ex.Message}");
                    }
                }
                if (!dryRun) RankedCollections.RestoreDisplayOrders(rankedSources.Keys, _libraryManager);
                await ReapplyNewCollectionArtAsync(collCreatedArt, cancellationToken);
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
                    if (importedCollections.Contains(oldName))
                    {
                        // Taken over with Import collection: it existed before HSC, so it stays in Emby.
                        _log.Skip($"Collection \"{oldName}\" kept (imported collection; its group is deleted, disabled or not in schedule, so HSC no longer changes it)");
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
                // Top-lists switched off in their source go first: the source's plain row comes back
                // in their place (and must not take over their rows).
                if (!dryRun) SourceTopLists.RemoveQueued(config, _libraryManager, _userManager, _log, cancellationToken);
                if (!dryRun) ManageHomeSections(config, cancellationToken, debug, statsList);
                else _log.Skip("Dry run — home sections are not changed");
                if (!dryRun)
                    _log.Info($"    {statsList.Count(g => g.HomeSectionSynced)} synced, {statsList.Count(g => g.HomeSectionRemoved) + _deletedGroupRowsRemoved} removed  ·  {RunLog.Elapsed(phaseTimer.Elapsed)}");

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
                            existing.Errors.AddRange(gs.Errors);
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
                                Errors           = new List<string>(gs.Errors),
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
                _resultsShown = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (var gs in displayStatsList)
                    WriteGroupBlock(gs, dryRun, logMissing);
                _resultsShown = null;
                // Dry runs skip writes, so they would clear real failures; inactive groups count as gone.
                if (!dryRun) RecordGroupIssues(statsList, config.Tags.Where(t => t.Active).Select(GroupKey));

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

                int groupsFailed  = displayStatsList.Count(g => g.ErrorMessage != null || g.Errors.Count > 0);
                int groupsSkipped = displayStatsList.Count(g => g.Skipped);
                int groupsWarned  = displayStatsList.Count(g => !g.Skipped && g.ErrorMessage == null && g.Errors.Count == 0 && g.Warnings.Count > 0);
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
                _log.Info($"  Home sections: {totalHsSynced} synced, {totalHsRemoved + _deletedGroupRowsRemoved} removed");
                if (_userStatus?.SummaryLine() is string _disabledLine) _log.Info("  " + _disabledLine);
                _log.Info($"  Done in {elapsedStr}  ·  {StatusSymbol(groupsFailed, groupsWarned)} {finalStatus}");
                _log.Rule();
            }
            catch (Exception ex)
            {
                LastRunStatus = $"Failed: {ex.Message}";
                _log.Error($"Sync aborted: {ex.Message}");
                WriteExceptionDebug(ex);
            }
            finally { IsRunning = false; PersistLog(); RealPosterBadges.QueueRebuild(); }
        }

        public async Task<(bool Success, string Message)> RunSingleEntryAsync(string entryName, CancellationToken cancellationToken)
        {
            // One run at a time (see RunGate): waits for a running sync / Run Group to finish.
            using (await RunGate.EnterAsync($"Run Group '{entryName}'", _logger, cancellationToken).ConfigureAwait(false))
                return await RunSingleEntryGatedAsync(entryName, cancellationToken).ConfigureAwait(false);
        }

        private async Task<(bool Success, string Message)> RunSingleEntryGatedAsync(string entryName, CancellationToken cancellationToken)
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

        /// <summary>
        /// Run Group on a top-list card: rebuilds just that list — its folder, tiles and badges
        /// (the same code the full sync uses), its home sections and its collection mirror. No
        /// source is fetched and no other list is touched. Manual movie lists are rebuilt from
        /// their current entries by <paramref name="rebuildManual"/> (the PrepareManualFolder code).
        /// </summary>
        public async Task<(bool Success, string Message)> RunSingleTopListAsync(
            string tagName, Func<TopListHomeSection, (bool Ok, string Message)> rebuildManual, CancellationToken cancellationToken)
        {
            // One run at a time (see RunGate): waits for a running sync / Run Group to finish.
            using (await RunGate.EnterAsync($"Run Group '{tagName}'", _logger, cancellationToken).ConfigureAwait(false))
                return await RunSingleTopListGatedAsync(tagName, rebuildManual, cancellationToken).ConfigureAwait(false);
        }

        private async Task<(bool Success, string Message)> RunSingleTopListGatedAsync(
            string tagName, Func<TopListHomeSection, (bool Ok, string Message)> rebuildManual, CancellationToken cancellationToken)
        {
            IsRunning = true;
            lock (ExecutionLog) ExecutionLog.Clear();
            LastStartedUtc = DateTime.UtcNow;
            LastRunStatus = "Running...";
            try
            {
                return await Task.Run(() => RunSingleTopListCore(tagName, rebuildManual, cancellationToken), cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                LastRunStatus = $"Failed: {ex.Message}";
                _log.Error($"Top-list '{tagName}' failed — {ex.Message}");
                return (false, ex.Message);
            }
            finally
            {
                IsRunning = false;
                PersistLog();
                RealPosterBadges.QueueRebuild();
            }
        }

        private (bool Success, string Message) RunSingleTopListCore(
            string tagName, Func<TopListHomeSection, (bool Ok, string Message)> rebuildManual, CancellationToken cancellationToken)
        {
            var config = Plugin.Instance?.Configuration;
            var startTime = DateTime.Now;
            _log = new RunLog(ExecutionLog, _logger, "", config?.ExtendedConsoleOutput ?? false, Progress);
            _log.Rule();
            _log.Info($"Home Screen Companion v{Plugin.Instance?.Version}  ·  {startTime:yyyy-MM-dd HH:mm}  ·  Single top-list: {tagName}");
            _log.Rule();
            if (config == null) { LastRunStatus = "Failed: no config"; _log.Error("Plugin configuration could not be loaded"); return (false, "Config not found"); }

            var tl = (config.TopLists ?? new List<TopListHomeSection>())
                .FirstOrDefault(t => string.Equals(t.TagName, tagName, StringComparison.OrdinalIgnoreCase));
            if (tl == null)
            {
                LastRunStatus = "Failed: top-list not found";
                _log.Error($"Top-list '{tagName}' was not found in the saved settings — save your settings and try again");
                return (false, $"Top-list '{tagName}' not found in saved config");
            }

            bool isShow = ShowTopList.IsShows(tl);
            bool byTag = (config.Tags ?? new List<TagConfig>()).Any(t => string.Equals((t.Tag ?? "").Trim(), tl.TagName, StringComparison.OrdinalIgnoreCase));
            int warnings = 0, errors = 0;
            string listSummary;

            _log.Blank();
            _log.Info("» Top-list");
            if (byTag || isShow)
            {
                // Same rebuild as the full sync, limited to this list. A show list is rebuilt from
                // its tag or, when picked by hand, from the shows saved in its folder.
                SyncTopListFolders(config, false, tl.TagName);
                var tlFolder = Path.Combine(Plugin.Instance!.DataFolderPath, "toplists", SanitizeTopListFolderName(tl.TagName));
                listSummary = isShow ? $"{RunLog.Plural(ShowTopList.ReadManifest(tlFolder, _jsonSerializer).Count, "show")} synced to its library folder"
                    : $"{RunLog.Plural(Directory.Exists(tlFolder) ? Directory.GetFiles(tlFolder, "*.strm").Length : 0, "movie")} synced to its library folder";
            }
            else
            {
                var (ok, msg) = rebuildManual(tl);
                if (ok) _log.Ok($"Top-list '{tl.TagName}': {msg}");
                else { errors++; _log.Error($"Top-list '{tl.TagName}' failed — {msg}"); }
                try { _libraryMonitor?.ReportFileSystemChanged(Path.Combine(Plugin.Instance!.DataFolderPath, "toplists", SanitizeTopListFolderName(tl.TagName))); }
                catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
                listSummary = ok ? msg : "rebuild failed";
            }

            if (!isShow)
            {
                _log.Blank();
                _log.Info("» Home sections");
                TopListSyncTask.SyncAll(_libraryManager, _userViewManager, _userManager, _jsonSerializer, _logger, cancellationToken, _log, tl.TagName);

                _log.Blank();
                _log.Info("» Collection mirror");
                var folder = Path.Combine(Plugin.Instance!.DataFolderPath, "toplists", SanitizeTopListFolderName(tl.TagName));
                var changes = TopListCollectionMirror.SyncFolder(folder, cancellationToken).GetAwaiter().GetResult();
                if (TopListCollectionMirror.Enabled || changes > 0)
                    _log.Ok($"Top-list '{tl.TagName}': {RunLog.Plural(changes, "collection membership")} changed");
                else
                    _log.Skip($"Top-list '{tl.TagName}': collection mirror is off — nothing to do");
            }

            string finalStatus = BuildFinalStatus(false, errors, errors == 0 && warnings > 0 ? 1 : 0);
            LastRunStatus = $"{finalStatus} ({DateTime.Now:HH:mm})";
            _log.Rule();
            _log.Info($"Top-list '{tl.TagName}': {listSummary}  ·  {RunLog.Elapsed(DateTime.Now - startTime)}");
            _log.Rule();
            return (errors == 0, $"Top-list '{tl.TagName}': {listSummary}");
        }

        // Set while PreviewEntryAsync runs: the single-entry run uses this unsaved source, changes
        // nothing and stops once the source's matches are known.
        private SourcePreview? _preview;

        /// <summary>
        /// What a source would tag right now, from its unsaved settings (any source type; group =
        /// every URL / collection / playlist of the card). Runs the real single-entry fetch and
        /// matching code with nothing written (no tags, no tag cache, no log, no AiLastRunDate).
        /// An AI source asks the provider once, ignoring the refresh interval.
        /// </summary>
        internal async Task<SourcePreview> PreviewEntryAsync(TagConfig source, CancellationToken cancellationToken, List<TagConfig>? group = null, string previewUserId = "")
        {
            var preview = new SourcePreview(source, group) { UserId = previewUserId ?? "" };
            // Previews share the run state below, so they take turns (a second preview waits for
            // the first instead of failing); a real sync still turns them away. The preview holds
            // RunGate while it runs, so a sync or Run Group started meanwhile waits for it.
            await _previewGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
            using var runGate = RunGate.TryEnter("Preview");
            if (runGate == null || IsRunning) { preview.Message = "A sync is running — try again when it has finished."; return preview; }
            IsRunning = true;
            var savedLog = _log;
            var savedWrittenTags = _writtenTags;
            var savedStatus = LastRunStatus;
            _preview = preview;
            try
            {
                var (success, message) = await RunSingleEntryInternalAsync(source.Name, cancellationToken);
                if (!preview.Done) preview.Message = success ? "This source has no preview." : message;
            }
            catch (Exception ex) { preview.Message = "Preview failed: " + ex.Message; }
            finally
            {
                _preview = null;
                _log = savedLog;
                _writtenTags = savedWrittenTags;
                LastRunStatus = savedStatus;
                IsRunning = false;
            }
            return preview;
            }
            finally { _previewGate.Release(); }
        }

        private readonly SemaphoreSlim _previewGate = new SemaphoreSlim(1, 1);

        private async Task<(bool Success, string Message)> RunSingleEntryInternalAsync(string entryName, CancellationToken cancellationToken)
        {
            var config = Plugin.Instance?.Configuration;
            if (config == null) return (false, "Config not found");
            _writtenTags = BuildWrittenTags(config);
            _userStatus = UserStatus.Read(_userManager);

            var tagConfig = _preview?.Source ?? config.Tags.FirstOrDefault(t =>
                string.Equals(t.Name, entryName, StringComparison.OrdinalIgnoreCase) ||
                (!string.IsNullOrWhiteSpace(t.Name) == false && string.Equals(t.Tag, entryName, StringComparison.OrdinalIgnoreCase)));
            if (tagConfig == null) { LastRunStatus = $"Failed: entry not found"; _log.Error($"Group '{entryName}' was not found in the saved settings — save your settings and try again"); return (false, $"Entry '{entryName}' not found in saved config"); }
            if (string.IsNullOrWhiteSpace(tagConfig.Tag)) { LastRunStatus = "Failed: no tag name"; _log.Error($"Group '{entryName}' has no tag name"); return (false, "Entry has no tag name"); }
            NormalizeNextWatch(tagConfig);

            // A group with several URLs / local sources is stored as one flat TagConfig per source
            // (same Name + Tag). tagConfig owns the shared settings; groupEntries supplies the sources.
            var groupEntryKey = GroupKey(tagConfig);
            var groupEntries = _preview != null ? _preview.Group : config.Tags
                .Where(t => string.Equals(GroupKey(t), groupEntryKey, StringComparison.OrdinalIgnoreCase))
                .ToList();

            string _displayName = !string.IsNullOrWhiteSpace(tagConfig.Name) ? $"{tagConfig.Name} [{tagConfig.Tag.Trim()}]" : tagConfig.Tag.Trim();
            string _srcLabel = string.IsNullOrEmpty(tagConfig.SourceType) ? "External" : tagConfig.SourceType;

            // Detail lines are always made: the plugin's log file gets them all, the page shows them
            // only with Extended log on (RunLog). A preview has no log to write them to.
            bool debug = _preview == null;
            bool dryRun = config.DryRunMode || _preview != null;
            bool logMissing = config.LogMissingItems;
            var startTime = DateTime.Now;
            _log = _preview != null ? new RunLog(new List<string>(), null, "", false) : new RunLog(ExecutionLog, _logger, "", config.ExtendedConsoleOutput, Progress);
            if (_preview == null && tagConfig.SourceType == "AI" && tagConfig.AiRefreshIntervalDays > 0 &&
                tagConfig.Active && IsScheduleActive(tagConfig.ActiveIntervals) &&
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
            StartPopularity(allItems);
            StartPlaybackHistory(config, new[] { tagConfig });

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
                GroupName = tagConfig.Name,
                SourceType = _srcLabel,
                SourceLabel = DescribeSource(tagConfig),
                EnableTag = tagConfig.EnableTag && !tagConfig.OnlyCollection,
                EnableCollection = tagConfig.EnableCollection,
                EnableHomeSection = SourceTopLists.PlainRow(tagConfig),
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

            if (_preview == null && (!tagConfig.Active || !IsScheduleActive(tagConfig.ActiveIntervals)))
                return OffScheduleSingleRun(config, tagConfig, gs, startTime, dryRun, logMissing, debug, cancellationToken);

            var _topListsFolder = Path.Combine(Plugin.Instance!.DataFolderPath, "toplists") + Path.DirectorySeparatorChar;
            var imdbLookup = new Dictionary<string, List<BaseItem>>(StringComparer.OrdinalIgnoreCase);
            foreach (var item in allItems)
            {
                if (item.LocationType != LocationType.FileSystem) continue;
                if (!string.IsNullOrEmpty(item.Path) &&
                    (item.Path.StartsWith(_topListsFolder, StringComparison.OrdinalIgnoreCase) || RankedCollections.IsRankedPath(item.Path)))
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
                || !(i.Path.StartsWith(_topListsFolder, StringComparison.OrdinalIgnoreCase) || RankedCollections.IsRankedPath(i.Path))).ToList();

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
                    var p = SplitTextCriterion(c);
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
                if (NeedsMediaDetails(tagConfig))
                {
                    foreach (var item in allItems)
                    {
                        if (item.LocationType != LocationType.FileSystem) continue;
                        var resolved = ResolveItemForMediaInfo(item, seriesEpisodeCache);
                        mediaInfoCache[item.InternalId] = ExtractMediaInfo(resolved);
                    }
                }
                var singleTagCollPlCriteria = GetAllCriteria(tagConfig)
                    .Select(c => c.Length > 0 && c[0] == '!' ? c.Substring(1) : c)
                    .Where(c => c.StartsWith("Collection:", StringComparison.OrdinalIgnoreCase) || c.StartsWith("Playlist:", StringComparison.OrdinalIgnoreCase))
                    .Distinct(StringComparer.OrdinalIgnoreCase);
                Dictionary<string, List<long>>? versionIdsByKey = null;
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
                        collectionMembershipCache[indivKey] = MembershipIds(members, allItems, ref versionIdsByKey);
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
                            userDataCache[_k] = UserDataOf(_user, _topItem);
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
                                if (userDataCache.TryGetValue((user.Id, ep.InternalId), out var cd) && Played(user, ep, cd.LastPlayedDate) is DateTimeOffset epLp)
                                    if (maxDate == null || epLp > maxDate) maxDate = epLp;
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

            RecordImportedCollections(Plugin.Instance?.Configuration);
            string tagName = tagConfig.Tag.Trim();
            string cName = string.IsNullOrWhiteSpace(tagConfig.CollectionName) ? tagName : tagConfig.CollectionName.Trim();
            int effectiveLimit = tagConfig.Limit <= 0 ? 10000 : tagConfig.Limit;
            var blacklist = new HashSet<string>(tagConfig.Blacklist ?? new List<string>(), StringComparer.OrdinalIgnoreCase);
            var passesFilters = FilterConditionsCheck(tagConfig, seriesEpisodeCache, personCache, userDataCache, mediaInfoCache,
                preloadedUsers, seriesLastPlayedCache, collectionMembershipCache, seriesEpisodeNamesCache);
            // Remove this tag from the cache before repopulating so blacklisted items don't get
            // immediately re-tagged by the real-time event handler when UpdateItem is called.
            // (Full run clears the entire cache first; single run must do a targeted removal.)
            // Another group in schedule with the same tag: its titles keep the tag. This run only adds
            // the tag; taking it off titles this source no longer has is left to the full sync, which
            // knows every group's titles.
            bool _tagShared = _preview == null && TagUsedElsewhereInSchedule(config, tagConfig);
            if (_preview == null && !_tagShared) TagCacheManager.Instance.RemoveTagFromAllEntries(tagName);
            var matchedLocalItems = new List<BaseItem>();
            var _srcTitles = new Dictionary<TagConfig, int>();   // per URL / local source of the group
            List<BaseItem> tagOutputItems = matchedLocalItems;
            List<BaseItem> collectionOutputItems = matchedLocalItems;
            int _listCount = 0;
            // List sources (URLs / the AI) that returned nothing: with "Preserve tags and collections
            // on empty result" the run leaves the group as it is, like the full sync.
            var _emptySources = new List<string>();
            var _failedSources = new List<string>();   // the empty sources that could not be read (see ListFetcher.IsFailure)

            _log.Blank();
            _log.Info("» Fetching sources");
            _log.Section($"[1/1] {_displayName}");
            _log.Step($"Fetching sources — {_displayName}");
            _log.Debug("  " + DescribeSourceDetail(tagConfig, effectiveLimit) + (groupEntries.Count > 1 ? $"  ·  {groupEntries.Count} sources in group" : ""));

            // Saved without a URL / local source yet: nothing is changed (see Execute).
            if (groupEntries.All(HasNoSourceYet))
            {
                if (_preview != null) return (false, "Nothing to preview yet: " + NoSourceYetReason(tagConfig) + ".");
                gs.Skipped = true;
                gs.SkipReason = NoSourceYetReason(tagConfig);
                gs.ElapsedMs = groupTimer.ElapsedMilliseconds;
                WriteFetchLine(gs);
                WriteSingleRunFooter(gs, startTime, dryRun, logMissing);
                return (true, gs.SkipReason + " — skipped");
            }

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
                        var items = await fetcher.FetchItems(src.Url, srcLimit, config.TraktClientId, config.MdblistApiKey, config.TmdbApiKey, cancellationToken,
                            CountsTowardLimit(imdbLookup, blacklist, passesFilters));
                        fetchTimer.Stop();
                        // Empty with "Preserve…" on: the URL's last good entries stand in for it (see LastGoodLists).
                        if (items.Count > 0) { if (_preview == null && !dryRun) LastGoodLists.Save(src.Url, items, _jsonSerializer); }
                        else if (_preview == null && config.PreserveTagsOnEmptyResult && LastGoodLists.Load(src.Url, _jsonSerializer) is var _lastGood && _lastGood != null)
                        {
                            items = _lastGood.Value.Items;
                            (ListFetcher.IsFailure(fetcher.LastError) ? gs.Errors : gs.Warnings).Add(StandInMessage(src.Url, fetcher.LastError, _lastGood.Value.SavedUtc));
                        }
                        _listCount += items.Count;
                        // One URL of several that does not exist (404): left out, as in the full sync.
                        if (items.Count == 0 && groupEntries.Count > 1 && ListFetcher.IsGone(fetcher.LastError))
                        {
                            if (_preview == null)
                                gs.Errors.Add($"{src.Url.Trim()}: the list does not exist{Why(fetcher.LastError)} — it was left out and the group's other lists were used. Remove the URL from the source or fix it.");
                            continue;
                        }
                        if (items.Count == 0) _emptySources.Add(src.Url.Trim() + (fetcher.LastError != null ? $" ({fetcher.LastError})" : ""));
                        if (items.Count == 0 && ListFetcher.IsFailure(fetcher.LastError)) _failedSources.Add(_emptySources[_emptySources.Count - 1]);
                        int _srcMatched = 0, _srcBlacklisted = 0, _srcMissingBefore = gs.MissingItems.Count;
                        foreach (var extItem in items)
                        {
                            if (string.IsNullOrEmpty(extItem.Imdb)) continue;
                            if (blacklist.Contains(extItem.Imdb)) { _srcBlacklisted++; _log.Debug($"    Blacklisted: {extItem.Name} ({extItem.Imdb})"); continue; }
                            if (_preview == null && tagConfig.EnableTag && !tagConfig.OnlyCollection && !HasFilterConditions(tagConfig))
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
                                _preview?.Missing.Add(new PreviewMissingTitle(extItem.Name, null, extItem.Imdb));
                            }
                        }
                        _srcTitles[src] = _srcMatched;
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
                            if (passesFilters != null && !passesFilters(itemToTag)) continue;
                            if (!srcMatched.Contains(itemToTag)) srcMatched.Add(itemToTag);
                        }
                        if (srcLimit < 10000 && srcMatched.Count > srcLimit)
                            srcMatched = srcMatched.Take(srcLimit).ToList();
                        foreach (var m in srcMatched)
                            if (!matchedLocalItems.Contains(m)) matchedLocalItems.Add(m);
                        _srcTitles[src] = TitleCount(srcMatched);
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
                else if (tagConfig.SourceType == "Manual")
                {
                    matchedLocalItems = ManualItems(tagConfig, blacklist, tagConfig.Limit <= 0 ? 10000 : tagConfig.Limit, gs, passesFilters);
                    _listCount = tagConfig.ManualItemIds?.Count ?? 0;
                    _log.Debug($"  {matchedLocalItems.Count} of {_listCount} hand-picked items found");
                }
                else if (IsNextWatch(tagConfig))
                {
                    Func<BaseItem, bool>? _nwAllow = null;
                    if (tagConfig.MediaInfoFilters?.Count > 0 || tagConfig.MediaInfoConditions?.Count > 0)
                        _nwAllow = item => ItemMatchesMediaInfo(item, tagConfig, debug, seriesEpisodeCache, personCache, userDataCache,
                            mediaInfoCache.TryGetValue(item.InternalId, out var _nwCi) ? _nwCi : (CachedMediaInfo?)null,
                            preloadedUsers, seriesLastPlayedCache, collectionMembershipCache, seriesEpisodeNamesCache);
                    if (_preview != null)
                    {
                        // Preview: one user (picked in the dialog; else the first selected user).
                        var _pvUser = !string.IsNullOrEmpty(_preview.UserId) ? _preview.UserId : NextWatchUserIds(tagConfig).FirstOrDefault() ?? "";
                        if (string.IsNullOrEmpty(_pvUser)) return (false, "Pick a user to preview.");
                        var _pv = ComputeNextWatch(tagConfig, matchableItems, config, blacklist, _nwAllow, gs, new[] { _pvUser });
                        _pv.TryGetValue(_pvUser, out var _pvItems);
                        _preview.Complete(_pvItems ?? new List<BaseItem>(), gs.ListCount, null, gs.Warnings);
                        return (true, "");
                    }
                    _nextWatchPerUser = ComputeNextWatch(tagConfig, matchableItems, config, blacklist, _nwAllow, gs, NextWatchUserIds(tagConfig));
                    // The source's shared items (a tag or collection, when enabled): the titles picked
                    // for the most users — the same list the playlist art is drawn from.
                    matchedLocalItems = PlaylistArtItems(_nextWatchPerUser.Values);
                }
                else if (tagConfig.SourceType == "MediaInfo")
                {
                    IList<BaseItem> _itemsToScan;
                    if (TagConfigTargetsEpisodes(tagConfig))
                    {
                        var _epQuery = new InternalItemsQuery { IncludeItemTypes = new[] { "Episode" }, Recursive = true, IsVirtualItem = false };
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
                            if (PopularRuleDays(tagConfig) == null && effectiveLimit < 10000 && matchedLocalItems.Count >= effectiveLimit) break;
                        }
                    }
                    OrderByPopularity(tagConfig, matchedLocalItems, effectiveLimit);
                    if (_preview != null)
                    {
                        var _popDays = PopularRuleDays(tagConfig);
                        _preview.Complete(matchedLocalItems, _itemsToScan.Count,
                            _popDays.HasValue ? matchedLocalItems.ToDictionary(i => i.Id, i => ViewersOf(i, _popDays.Value)) : null);
                        return (true, "");
                    }
                    if (!dryRun)
                        RankWeekAgo(tagName, tagConfig, _itemsToScan.Where(i => i.LocationType == LocationType.FileSystem && !blacklist.Contains(i.GetProviderId("Imdb") ?? "")),
                            i => ItemMatchesMediaInfo(i, tagConfig, debug, seriesEpisodeCache, personCache, userDataCache, mediaInfoCache.TryGetValue(i.InternalId, out var wci) ? wci : (CachedMediaInfo?)null,
                                preloadedUsers, seriesLastPlayedCache, collectionMembershipCache, seriesEpisodeNamesCache), effectiveLimit);
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
                        AiAskCount(effectiveLimit),
                        cancellationToken);
                    fetchTimer.Stop();

                    _listCount = aiItems.Count;
                    if (aiItems.Count == 0) _emptySources.Add((string.IsNullOrWhiteSpace(tagConfig.AiProvider) ? "the AI" : tagConfig.AiProvider) + (fetcher.LastError != null ? $" ({fetcher.LastError})" : ""));
                    if (aiItems.Count == 0 && ListFetcher.IsFailure(fetcher.LastError)) _failedSources.Add(_emptySources[_emptySources.Count - 1]);
                    int _aiTitleMatched = 0, _aiBlacklisted = 0;

                    foreach (var aiItem in aiItems)
                    {
                        // Asked for more than the limit (see AiAskCount): the first hits fill it.
                        if (effectiveLimit < 10000 && TitleCount(matchedLocalItems) >= effectiveLimit) break;
                        if (string.IsNullOrWhiteSpace(aiItem.title)) continue;
                        string _aiLabel = aiItem.year.HasValue ? $"{aiItem.title} ({aiItem.year})" : aiItem.title;

                        if (!string.IsNullOrEmpty(aiItem.imdb_id))
                        {
                            var imdbId = aiItem.imdb_id!.Trim();
                            if (blacklist.Contains(imdbId)) { _aiBlacklisted++; _log.Debug($"    Blacklisted: {_aiLabel} ({imdbId})"); continue; }
                            if (_preview == null && tagConfig.EnableTag && !tagConfig.OnlyCollection && !HasFilterConditions(tagConfig))
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
                                else { gs.MissingItems.Add($"{_aiLabel}  {imdbId}"); _preview?.Missing.Add(new PreviewMissingTitle(aiItem.title, aiItem.year, imdbId)); }
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
                            else { gs.MissingItems.Add($"{_aiLabel}  (no IMDb id from AI)"); _preview?.Missing.Add(new PreviewMissingTitle(aiItem.title, aiItem.year, "")); }
                            foreach (var localItem in titleMatches)
                            {
                                var imdb = localItem.GetProviderId("Imdb");
                                if (!string.IsNullOrEmpty(imdb) && blacklist.Contains(imdb)) { _aiBlacklisted++; continue; }
                                if (!matchedLocalItems.Contains(localItem)) matchedLocalItems.Add(localItem);
                            }
                        }
                    }
                    _log.Debug($"  AI ({tagConfig.AiProvider}) returned {_listCount} items in {fetchTimer.ElapsedMilliseconds} ms  ·  {matchedLocalItems.Count} matched ({_aiTitleMatched} by title only)  ·  {gs.MissingItems.Count} not in library  ·  {_aiBlacklisted} blacklisted");

                    if (_preview != null)
                        _preview.Note = $"Asked {(string.IsNullOrWhiteSpace(tagConfig.AiProvider) ? "the AI" : tagConfig.AiProvider)} once; the result may differ next time.";
                    else if (tagConfig.AiRefreshIntervalDays > 0 && aiItems.Count > 0)
                    {
                        tagConfig.AiLastRunDate = DateTime.UtcNow;
                        Plugin.Instance!.SaveConfiguration();
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
                if (_preview == null) LastRunStatus = $"Failed: {ex.Message}";
                return (false, $"Error: {ex.Message}");
            }

            // Some of the group's URLs came back empty (and had no last good list to stand in), the
            // others gave titles: like the full sync, those are used and nothing the group has now is
            // removed (the empty URL's titles are not known).
            bool _keepExisting = _preview == null && _emptySources.Count > 0 && config.PreserveTagsOnEmptyResult && _listCount > 0;
            if (_preview == null && _emptySources.Count > 0 && config.PreserveTagsOnEmptyResult && !_keepExisting)
            {
                gs.ListCount = _listCount;
                (_failedSources.Count > 0 ? gs.Errors : gs.Warnings).Add((tagConfig.SourceType == "AI" ? "The AI returned 0 items" : $"{(_emptySources.Count == 1 ? "A list" : $"{_emptySources.Count} lists")} returned 0 items")
                    + $" ({string.Join(", ", _emptySources)}) — existing tags, collection, playlist and top-list were kept (\"Preserve tags and collections on empty result\" is on in Settings).");
                gs.ElapsedMs = groupTimer.ElapsedMilliseconds;
                WriteFetchLine(gs);
                WriteSingleRunFooter(gs, startTime, dryRun, logMissing);
                return (true, "The source returned 0 items — nothing was changed");
            }

            // MediaInfo post-filter for list sources (External, AI). Local collections / playlists and
            // Manual lists were already filtered while they were read (passesFilters).
            bool _filteredWhileRead = passesFilters != null
                && (tagConfig.SourceType == "LocalCollection" || tagConfig.SourceType == "LocalPlaylist" || tagConfig.SourceType == "Manual");
            if (tagConfig.SourceType != "MediaInfo" && !_filteredWhileRead && matchedLocalItems.Count > 0
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

            // Preview of a list source: the matches in list order plus what is not in the library.
            if (_preview != null)
            {
                // Say why a list or the AI came back empty (a wrong key, an unreachable server …).
                foreach (var _empty in _emptySources)
                    gs.Warnings.Add(tagConfig.SourceType == "AI" ? $"The AI returned 0 items: {_empty}" : $"The list returned 0 items: {_empty}");
                _preview.Complete(matchedLocalItems, _listCount, null, gs.Warnings);
                return (true, "");
            }

            // Keep what the group has now (see _keepExisting): its tagged titles and collection members
            // join the list, after it (top-list and ranked copies are not the group's own titles).
            if (_keepExisting)
            {
                var _keptTlFolder = Path.Combine(Plugin.Instance!.DataFolderPath, "toplists") + Path.DirectorySeparatorChar;
                bool Own(BaseItem i) => IsTaggableTopLevelItem(i) && !string.IsNullOrEmpty(i.Path)
                    && !i.Path.StartsWith(_keptTlFolder, StringComparison.OrdinalIgnoreCase) && !RankedCollections.IsRankedPath(i.Path);
                var _kept = new List<BaseItem>();
                if (tagConfig.EnableTag && !tagConfig.OnlyCollection)
                    _kept.AddRange(_libraryManager.GetItemList(new InternalItemsQuery { Tags = new[] { tagName }, IncludeItemTypes = new[] { "Movie", "Series" }, Recursive = true, IsVirtualItem = false }));
                else if (SourceTopLists.IsOn(tagConfig))
                    _kept.AddRange(TopListMembers.Load(tagName, _libraryManager, _jsonSerializer) ?? new List<BaseItem>());
                if (tagConfig.EnableCollection
                    && _libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "BoxSet" }, Name = cName, Recursive = true }).FirstOrDefault() is BaseItem _keptColl)
                    _kept.AddRange(_libraryManager.GetItemList(new InternalItemsQuery { CollectionIds = new[] { _keptColl.InternalId }, Recursive = true, IsVirtualItem = false }));
                foreach (var i in _kept.Where(Own))
                    if (!matchedLocalItems.Contains(i)) matchedLocalItems.Add(i);
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

            if (!IsNextWatch(tagConfig)) gs.ListCount = _listCount;
            if ((string.IsNullOrEmpty(tagConfig.SourceType) || tagConfig.SourceType == "External" || tagConfig.SourceType == "AI") && _listCount == 0)
                (_failedSources.Count > 0 ? gs.Errors : gs.Warnings).Add(tagConfig.SourceType == "AI"
                    ? $"The AI returned 0 items{Why(_emptySources)} — the group's tags, collection and playlist are being cleared. Check the prompt and the API key in Settings."
                    : $"The list returned 0 items{Why(_emptySources)} — the group's tags, collection and playlist are being cleared. Check the list URL and the API key in Settings.");
            else
                // Some of the group's URLs came back empty, the others were used (same as the full sync).
                foreach (var _empty in _emptySources)
                    (_failedSources.Contains(_empty) ? gs.Errors : gs.Warnings).Add(_keepExisting
                        ? $"{_empty}: the list returned 0 items — the group's other lists were used and its current titles were kept (\"Preserve tags and collections on empty result\" is on)"
                        : $"{_empty}: the list returned 0 items — the group's other lists were used");
            {
                var _outIds = new HashSet<Guid>(tagOutputItems.Select(i => i.Id));
                foreach (var _id in collectionOutputItems.Select(i => i.Id)) _outIds.Add(_id);
                if (!IsNextWatch(tagConfig)) gs.MatchCount = tagConfig.SourceType == "MediaInfo" ? _outIds.Count : TitleCount(matchedLocalItems);
            }
            // Several URLs / local sources: each has its own Limit and count.
            if (_preview == null && !dryRun && !gs.BoxSetHse)
                RecordLastRun(groupEntries.Count > 1
                    ? groupEntries.Select(t => (t, _srcTitles.TryGetValue(t, out var n) ? n : -1))
                    : new[] { (tagConfig, TitleCount(matchedLocalItems)) }, gs);
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
                    .Select(RankKey)
                    .Where(id => !string.IsNullOrEmpty(id))
                    .ToList());
                SaveTopListPart(config, tagConfig, tagName, matchedLocalItems);
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
            // A list source with filter conditions caches only the titles that passed them (see
            // HasFilterConditions) — before the tags are written, as the full run does.
            if (!dryRun && tagConfig.EnableTag && !tagConfig.OnlyCollection && !_isBoxSetHse && IsListSource(tagConfig) && HasFilterConditions(tagConfig))
                foreach (var _imdb in tagOutputItems.Select(i => i.GetProviderId("Imdb")).Where(id => !string.IsNullOrEmpty(id)))
                    TagCacheManager.Instance.AddToCache($"imdb_{_imdb}", tagName);
            int updateCount = 0;
            foreach (var item in allItems)
            {
                var existingTags = new HashSet<string>(item.Tags, StringComparer.OrdinalIgnoreCase);
                bool shouldHave = tagConfig.EnableTag && !tagConfig.OnlyCollection && !_isBoxSetHse && matchedIds.Contains(item.Id);
                bool hasTag = existingTags.Contains(tagName);
                if (shouldHave == hasTag) continue;
                if (!shouldHave && _tagShared) continue;
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
                    if (!shouldHaveEp && _tagShared) continue;
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
                    if (!shouldHaveSeason && _tagShared) continue;
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

            // Tags this plugin wrote earlier (history file) that no group uses any more — the old name
            // of a renamed tag, or a deleted group's tag. The full run removes these through the
            // history too; doing it here means Run Group cleans up a rename straight away.
            // Tags of other groups (active or not) are left to the full run.
            if (_preview == null)
            {
                var orphanTags = LoadFileHistory("homescreencompanion_history.txt")
                    .Where(t => !string.Equals(t, tagName, StringComparison.OrdinalIgnoreCase)
                        && !config.Tags.Any(c => !string.IsNullOrWhiteSpace(c.Tag) && string.Equals(c.Tag.Trim(), t, StringComparison.OrdinalIgnoreCase)))
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .ToList();
                foreach (var orphan in orphanTags)
                {
                    // Drop it from the real-time cache first, or UpdateItem would re-add it.
                    if (!dryRun) TagCacheManager.Instance.RemoveTagFromAllEntries(orphan);
                    // BoxSets asked for separately: an old tag HSC put on a collection itself
                    // (ApplyTagToSourceBoxSet) must go too.
                    var orphanItems = _libraryManager.GetItemList(new InternalItemsQuery
                    {
                        Recursive = true,
                        IsVirtualItem = false,
                        Tags = new[] { orphan }
                    }).Concat(_libraryManager.GetItemList(new InternalItemsQuery
                    {
                        IncludeItemTypes = new[] { "BoxSet" },
                        Recursive = true,
                        Tags = new[] { orphan }
                    })).GroupBy(i => i.Id).Select(g => g.First()).ToList();
                    int orphanRemoved = 0;
                    foreach (var item in orphanItems)
                    {
                        if (debug)
                        {
                            var _tagYr = item.ProductionYear.HasValue ? $" ({item.ProductionYear})" : "";
                            string _itemLabel = $"{item.Name}{_tagYr}  [{item.GetType().Name}]";
                            if (!_dbgTagRemoved!.ContainsKey(orphan)) _dbgTagRemoved[orphan] = new List<string>();
                            _dbgTagRemoved[orphan].Add(_itemLabel);
                        }
                        if (!dryRun)
                        {
                            item.RemoveTag(orphan);
                            try { _libraryManager.UpdateItem(item, item.Parent, ItemUpdateType.MetadataEdit, null); }
                            catch (Exception ex) { _log.Warn($"Could not save tags for '{item.Name}': {ex.Message}"); }
                            if (++updateCount % 25 == 0) await Task.Yield();
                        }
                        orphanRemoved++;
                    }
                    if (orphanRemoved > 0)
                    {
                        tagsRemoved += orphanRemoved;
                        _log.Skip(dryRun
                            ? $"Old tag \"{orphan}\" would be removed from {orphanRemoved} item(s) (no group uses it any more)"
                            : $"Old tag \"{orphan}\" removed from {orphanRemoved} item(s) (no group uses it any more)");
                    }
                }
                if (orphanTags.Count > 0 && !dryRun)
                {
                    TagCacheManager.Instance.Save();
                    // Cleaned up once: drop them from the history so HSC leaves these tags alone from now on.
                    SaveFileHistory("homescreencompanion_history.txt", LoadFileHistory("homescreencompanion_history.txt")
                        .Where(t => !orphanTags.Contains(t, StringComparer.OrdinalIgnoreCase)).ToList());
                }
            }

            WriteTagDiffDebug(_dbgTagAdded, _dbgTagRemoved);
            if (_tagShared && gs.EnableTag && !gs.BoxSetHse)
                _log.Skip($"Tag \"{tagName}\" is also used by another group in schedule — titles that left this source keep it until the next full sync");
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
            if (!dryRun && tagConfig.EnableTag && !tagConfig.OnlyCollection && !_isBoxSetHse)
                ApplyTagArt(tagName, tagConfig, matchedLocalItems);

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
            // Ranked collection (experimental): no longer wanted → its copies, folder and library go,
            // so the collection gets the real films back below.
            if (!dryRun && !RankedCollections.Wants(config, tagConfig))
                RankedCollections.Cleanup(new HashSet<string>(StringComparer.OrdinalIgnoreCase), RankedCollections.FolderName(tagConfig),
                    _libraryManager, _collectionManager, _userManager, _log);
            if (tagConfig.EnableCollection && collectionOutputItems.Count > 0 && !dryRun)
            {
                try
                {
                    var _extraVersions = ExtraVersionIds(collectionOutputItems);
                    var desiredIds = collectionOutputItems.Select(i => i.InternalId).Where(id => !_extraVersions.Contains(id)).ToHashSet();
                    bool _ranked = false;
                    List<BaseItem>? _rankProbe = null;
                    if (RankedCollections.Wants(config, tagConfig))
                    {
                        try
                        {
                            _log.Step($"Ranked collections — {cName}");
                            var _rankedIds = await RankedCollections.SyncAsync(tagConfig, cName, collectionOutputItems, _libraryManager, _providerManager,
                                _fileSystem, _userManager, _collectionManager, config, _log, cancellationToken);
                            if (_rankedIds != null)
                            {
                                desiredIds = _rankedIds.Value.Members.ToHashSet(); _rankProbe = _rankedIds.Value.ToProbe; _ranked = true;
                                // Before the members change: that queues a refresh of the collection, which
                                // would save the order it loaded over a later change.
                                RankedCollections.SetDisplayOrder(_libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "BoxSet" }, Name = cName, Recursive = true }).FirstOrDefault(), _libraryManager);
                            }
                        }
                        catch (OperationCanceledException) { throw; }
                        catch (Exception ex) { _log.Warn($"Ranked collection '{cName}' failed, the real films are used: {ex.Message}"); }
                    }
                    var existingColl = _libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "BoxSet" }, Name = cName, Recursive = true }).FirstOrDefault();
                    bool _newCollArt = false;
                    if (existingColl == null)
                    {
                        var createdColl = await _collectionManager.CreateCollection(new CollectionCreationOptions { Name = cName, IsLocked = false, ItemIdList = desiredIds.ToArray() });
                        collResult = 1;
                        _collCreated = true;
                        _collItemsAdded = desiredIds.Count;
                        _log.Debug($"  {cName}  →  created ({desiredIds.Count} items)");
                        if (createdColl != null) ApplyCollectionSortName(createdColl, cName, tagConfig.CollectionSortName);
                        _newCollArt = true;
                    }
                    else
                    {
                        // Top-list copies are managed by TopListCollectionMirror — never remove them here.
                        var currentMembers = _libraryManager.GetItemList(new InternalItemsQuery { CollectionIds = new[] { existingColl.InternalId }, Recursive = true, IsVirtualItem = false }).Where(i => !TopListCollectionMirror.IsTopListItem(i)).Select(i => i.InternalId).ToHashSet();
                        var toAdd = desiredIds.Where(id => !currentMembers.Contains(id)).ToList();
                        var toRemove = currentMembers.Where(id => !desiredIds.Contains(id)).ToList();
                        if (toAdd.Count > 0) await _collectionManager.AddToCollection(existingColl.InternalId, toAdd.ToArray());
                        if (toRemove.Count > 0 && existingColl is BoxSet boxSet) _collectionManager.RemoveFromCollection(boxSet, toRemove.ToArray());
                        ApplyCollectionSortName(existingColl, cName, tagConfig.CollectionSortName);
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
                    var _artColl = _libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "BoxSet" }, Name = cName, Recursive = true }).FirstOrDefault();
                    if (_artColl != null)
                    {
                        // Same description / uploaded poster handling as the full sync.
                        var _collDescs = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                        var _collPosters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                        CollectCollectionMeta(tagConfig, cName, _collDescs, _collPosters);
                        if (_collDescs.Count > 0 || _collPosters.Count > 0)
                            ApplyCollectionMeta(_artColl, cName, _collDescs, _collPosters, debug);
                        var _artItems = _ranked ? RankedArtItems(collectionOutputItems) : collectionOutputItems;
                        ApplyCollectionArt(_artColl, cName, tagConfig, _artItems);
                        if (_newCollArt)
                            await ReapplyNewCollectionArtAsync(new List<(string, TagConfig, IList<BaseItem>)> { (cName, tagConfig, _artItems) }, cancellationToken);
                        if (_ranked) RankedCollections.SetDisplayOrder(_artColl, _libraryManager);
                    }
                    RankedCollections.Probe(_rankProbe, _providerManager, _fileSystem);
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

            if (!dryRun && !RankedCollections.Wants(config, tagConfig))
                RankedCollections.RestoreDisplayOrders(config.Tags.Where(t => RankedCollections.Wants(config, t))
                    .Select(t => string.IsNullOrWhiteSpace(t.CollectionName) ? t.Tag.Trim() : t.CollectionName.Trim()), _libraryManager);

            if (tagConfig.EnablePlaylist)
            {
                _log.Blank();
                _log.Info("» Playlists");
                if (dryRun) _log.Skip("Dry run — playlists are not changed");
            }
            if (IsNextWatch(tagConfig))
            {
                // Each user's own picks; nothing is written for a user whose list could not be made.
                if (_nextWatchPerUser != null && _nextWatchPerUser.Count > 0)
                    await SyncPlaylistsForEntryAsync(tagConfig, new List<BaseItem>(), dryRun, gs, _nextWatchPerUser);
                _nextWatchPerUser = null;
            }
            else
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
                // Same for the collection, so the full run removes it once the group is renamed or deleted.
                if (tagConfig.EnableCollection && !string.IsNullOrEmpty(cName))
                {
                    var _collHist = LoadFileHistory("homescreencompanion_collections.txt");
                    if (!_collHist.Any(c => string.Equals(c, cName, StringComparison.OrdinalIgnoreCase)))
                    {
                        _collHist.Add(cName);
                        SaveFileHistory("homescreencompanion_collections.txt", _collHist);
                    }
                }

                Plugin.Instance!.SaveConfiguration();
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

            // This group's top-list switched off: removed before its plain row comes back.
            if (!dryRun) SourceTopLists.RemoveQueued(config, _libraryManager, _userManager, _log, cancellationToken, tagName, tagConfig.Name);
            // Manage home sections for this entry
            // A source whose row is its top-list: ManageHomeSections removes a plain row it still
            // has (the top-list's own row is made below).
            bool _hsRows = tagConfig.EnableHomeSection;
            if (SourceTopLists.PlainRow(tagConfig))
            {
                _log.Blank();
                _log.Info("» Home sections");
                if (dryRun) _log.Skip("Dry run — home sections are not changed");
            }
            if (!dryRun && _hsRows)
                ManageHomeSections(config, cancellationToken, debug, new List<GroupRunStats> { gs }, tagName);
            else if (!dryRun)
                RemoveDeletedGroupLeftovers(cancellationToken);
            CleanupDisabledPlaylists(config, dryRun);
            // Only this group's own top-list (other lists are not fed by this tag).
            SyncTopListFolders(config, dryRun, tagName);
            // Made with the group's Create as top-list: its rows too (the group has no other row).
            if (!dryRun && SourceTopLists.IsOn(tagConfig) && (config.TopLists ?? new List<TopListHomeSection>()).Any(t => t.FromSource && string.Equals(t.TagName, tagName, StringComparison.OrdinalIgnoreCase)))
            {
                _log.Blank();
                _log.Info("» Top-list");
                TopListSyncTask.SyncAll(_libraryManager, _userViewManager, _userManager, _jsonSerializer, _logger, cancellationToken, _log, tagName);
            }

            WriteSingleRunFooter(gs, startTime, dryRun, logMissing);

            List<string> parts;
            if (_isBoxSetHse)
            {
                parts = new List<string> { $"{_boxSetTaggedCount} collection{(_boxSetTaggedCount == 1 ? "" : "s")} tagged" };
            }
            else
            {
                parts = new List<string> { IsNextWatch(tagConfig) ? $"{gs.MatchCount} picks for {RunLog.Plural(gs.ListCount, "user")}" : $"{matchedLocalItems.Count} matched" };
                if (tagsAdded > 0 || tagsRemoved > 0) parts.Add($"{tagsAdded}↑ {tagsRemoved}↓ tags");
            }
            if (collResult > 0) parts.Add("collection updated");
            if (dryRun) parts.Add("(dry run)");
            var summary = string.Join(", ", parts);
            return (true, summary);
        }

        // A single-group run outside the group's schedule: nothing is fetched, and the group ends up
        // as a full run leaves it — its tag off the titles, its collection removed (unless imported),
        // its playlists removed and its home row marked inactive. A tag or collection that another
        // group in schedule also uses is left alone.
        private (bool Success, string Message) OffScheduleSingleRun(PluginConfiguration config, TagConfig tagConfig, GroupRunStats gs,
            DateTime startTime, bool dryRun, bool logMissing, bool debug, CancellationToken cancellationToken)
        {
            // Switched off (Active unticked) ends up the same as out of schedule, as in a full run.
            string why = tagConfig.Active ? "not in schedule" : "switched off";
            string tagName = tagConfig.Tag.Trim();
            string cName = string.IsNullOrWhiteSpace(tagConfig.CollectionName) ? tagName : tagConfig.CollectionName.Trim();
            var others = config.Tags.Where(t => t.Active && IsScheduleActive(t.ActiveIntervals)
                && !string.Equals(GroupKey(t), GroupKey(tagConfig), StringComparison.OrdinalIgnoreCase)).ToList();

            gs.Skipped = true;
            gs.SkipReason = why;
            _log.Blank();
            _log.Info("» Fetching sources");
            WriteFetchLine(gs);

            _log.Blank();
            _log.Info("» Applying tags");
            int tagsRemoved = 0;
            if (others.Any(t => string.Equals(t.Tag?.Trim(), tagName, StringComparison.OrdinalIgnoreCase)))
                _log.Skip($"Tag \"{tagName}\" kept — another group in schedule uses it");
            else
            {
                if (!dryRun) { TagCacheManager.Instance.RemoveTagFromAllEntries(tagName); TagCacheManager.Instance.Save(); }
                foreach (var item in _libraryManager.GetItemList(new InternalItemsQuery
                {
                    IncludeItemTypes = new[] { "Movie", "Series", "Season", "Episode" },
                    Tags = new[] { tagName },
                    Recursive = true
                }))
                {
                    if (debug) _log.Debug($"    - {item.Name}{(item.ProductionYear.HasValue ? $" ({item.ProductionYear})" : "")}  [{item.GetType().Name}]");
                    if (!dryRun)
                    {
                        item.RemoveTag(tagName);
                        try { _libraryManager.UpdateItem(item, item.Parent, ItemUpdateType.MetadataEdit, null); }
                        catch (Exception ex) { _log.Warn($"Could not save tags for '{item.Name}': {ex.Message}"); }
                    }
                    tagsRemoved++;
                }
                _log.Info(tagsRemoved == 0 ? "    No tag changes needed"
                    : dryRun ? $"    Would remove {tagsRemoved} tags" : $"    -{tagsRemoved} removed ({why})");
            }
            // A collection that carried the tag itself (home row of collections) loses it too.
            tagsRemoved += CleanupBoxSetTags(config, dryRun, cancellationToken);
            gs.TagsRemoved = tagsRemoved;

            if (tagConfig.EnableCollection)
            {
                _log.Blank();
                _log.Info("» Collections");
                var coll = _libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "BoxSet" }, Name = cName, Recursive = true }).FirstOrDefault();
                if (coll == null || TagAnchor.IsAnchor(coll, _jsonSerializer)) _log.Skip($"Collection \"{cName}\" does not exist");
                else if (RecordImportedCollections(config).Contains(cName))
                    _log.Skip($"Collection \"{cName}\" kept (imported collection; HSC no longer changes it while the group is {why})");
                else if (others.Any(t => t.EnableCollection && string.Equals(
                    string.IsNullOrWhiteSpace(t.CollectionName) ? t.Tag?.Trim() : t.CollectionName.Trim(), cName, StringComparison.OrdinalIgnoreCase)))
                    _log.Skip($"Collection \"{cName}\" kept — another group in schedule uses it");
                else if (dryRun) _log.Skip($"Collection \"{cName}\" would be removed ({why})");
                else
                {
                    try
                    {
                        _libraryManager.DeleteItem(coll, new DeleteOptions { DeleteFileLocation = false });
                        _log.Skip($"Collection \"{cName}\" removed ({why})");
                    }
                    catch (Exception ex) { _log.Warn($"Collection \"{cName}\" could not be removed: {ex.Message}"); }
                }
            }

            if (tagConfig.EnablePlaylist || (tagConfig.PlaylistMappings?.Count ?? 0) > 0)
            {
                _log.Blank();
                _log.Info("» Playlists");
            }
            CleanupDisabledPlaylists(config, dryRun);

            // A top-list built from the tag loses this group's titles, as the tag does.
            if (!dryRun && TopListMembers.Wanted(config, tagName))
            {
                _log.Blank();
                _log.Info("» Top-list");
                SaveTopListPart(config, tagConfig, tagName, new List<BaseItem>());
                SyncTopListFolders(config, false, tagName);
            }

            if (tagConfig.EnableHomeSection)
            {
                _log.Blank();
                _log.Info("» Home sections");
                if (dryRun) _log.Skip("Dry run — home sections are not changed");
                else ManageHomeSections(config, cancellationToken, debug, new List<GroupRunStats> { gs }, tagName);
            }
            else if (!dryRun)
                RemoveDeletedGroupLeftovers(cancellationToken);

            gs.ElapsedMs = (long)(DateTime.Now - startTime).TotalMilliseconds;
            WriteSingleRunFooter(gs, startTime, dryRun, logMissing);
            if (_preview == null) LastRunStatus = $"Skipped ({DateTime.Now:HH:mm})";
            var whyCap = char.ToUpper(why[0]) + why.Substring(1);
            return (true, tagsRemoved > 0 ? $"{whyCap} — {tagsRemoved}↓ tags" : $"{whyCap} — skipped");
        }

        // Identifies the UI group a flat TagConfig belongs to. The config page stores one flat entry
        // per URL / local source with the same Name + Tag, so several entries can share one key.
        // An External source without a URL, or a local collection / playlist source without one picked.
        // A MediaInfo source without a single rule would match (and tag) the whole library: it is
        // skipped like a source without a URL until it has rules.
        internal static bool HasNoSourceYet(TagConfig t) =>
            (string.IsNullOrEmpty(t.SourceType) || t.SourceType == "External") ? string.IsNullOrWhiteSpace(t.Url)
            : t.SourceType == "MediaInfo" ? !HasFilterConditions(t)
            : (t.SourceType == "LocalCollection" || t.SourceType == "LocalPlaylist") && string.IsNullOrWhiteSpace(t.LocalSourceId);

        private static string NoSourceYetReason(TagConfig t) =>
            (string.IsNullOrEmpty(t.SourceType) || t.SourceType == "External") ? "no URL yet"
            : t.SourceType == "MediaInfo" ? "no filter rules yet"
            : t.SourceType == "LocalPlaylist" ? "no playlist picked yet" : "no collection picked yet";

        private static string GroupKey(TagConfig t) => GroupIssues.Key(t.Name, t.Tag);

        // Fallback id a home row is found by when its real section id is not known.
        private static string SectionMarker(TagConfig tc) =>
            "hsc__" + string.Concat((tc.Tag ?? tc.Name ?? "").Take(40)
                .Select(c => char.IsLetterOrDigit(c) || c == '-' || c == '_' ? c : '_'));

        private static readonly object _deletedGroupsLock = new object();
        private static string DeletedGroupsFile => Path.Combine(Plugin.Instance!.DataFolderPath, "deleted_groups.xml");

        private static DeletedGroupCleanup LoadDeletedGroups()
        {
            try
            {
                if (File.Exists(DeletedGroupsFile))
                    return Plugin.XmlSerializer.DeserializeFromFile(typeof(DeletedGroupCleanup), DeletedGroupsFile) as DeletedGroupCleanup ?? new DeletedGroupCleanup();
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            return new DeletedGroupCleanup();
        }

        private static void SaveDeletedGroups(DeletedGroupCleanup c)
        {
            try
            {
                if (c.HomeRows.Count == 0 && c.Playlists.Count == 0) { if (File.Exists(DeletedGroupsFile)) File.Delete(DeletedGroupsFile); return; }
                Directory.CreateDirectory(Path.GetDirectoryName(DeletedGroupsFile)!);
                Plugin.XmlSerializer.SerializeToFile(c, DeletedGroupsFile);
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
        }

        // tag_ranks/<tag>.json (WriteRankFile); SyncTopListFolders reads it by the top-list's folder name, the same name.
        private static string RankFilePath(string tagName)
            => Path.Combine(Plugin.Instance!.DataFolderPath, "tag_ranks", SanitizeTopListFolderName(tagName) + ".json");

        // Another group in schedule (not this one) writes the same tag.
        private static bool TagUsedElsewhereInSchedule(PluginConfiguration config, TagConfig tc)
        {
            var tag = (tc.Tag ?? "").Trim();
            return tag.Length > 0 && (config.Tags ?? new List<TagConfig>()).Any(t => t.Active && t.EnableTag && !t.OnlyCollection
                && IsScheduleActive(t.ActiveIntervals, DateTime.Now)
                && !string.Equals(GroupKey(t), GroupKey(tc), StringComparison.OrdinalIgnoreCase)
                && string.Equals((t.Tag ?? "").Trim(), tag, StringComparison.OrdinalIgnoreCase));
        }

        // Called when the settings page saves. A deleted group leaves nothing in the config, so
        // what it left behind is written down here for the next full run: its tag and collection
        // go into their history files (the run then takes the tag off its titles and removes the
        // collection, also when no run had recorded them), and its home rows and playlists are
        // queued for removal. Its rank file goes now. A renamed group keeps its tracked rows and
        // playlists, so only what no remaining group tracks is queued; a tag or collection name
        // another group still uses is left alone.
        internal static void RecordDeletedGroups(PluginConfiguration? oldConfig, PluginConfiguration? newConfig)
        {
            if (oldConfig?.Tags == null || newConfig?.Tags == null || Plugin.Instance == null) return;
            try
            {
                var newKeys = new HashSet<string>(newConfig.Tags.Select(GroupKey), StringComparer.OrdinalIgnoreCase);
                var deleted = oldConfig.Tags.Where(t => !newKeys.Contains(GroupKey(t))).ToList();
                if (deleted.Count == 0) return;

                var newTags = new HashSet<string>(newConfig.Tags.Where(t => !string.IsNullOrWhiteSpace(t.Tag)).Select(t => t.Tag.Trim()), StringComparer.OrdinalIgnoreCase);
                var newCollections = new HashSet<string>(newConfig.Tags.Where(t => t.EnableCollection).Select(CollectionNameOf), StringComparer.OrdinalIgnoreCase);
                var stillTracked = new HashSet<string>(newConfig.Tags
                    .SelectMany(t => t.HomeSectionTracked ?? new List<HomeSectionTracking>())
                    .Select(r => r.UserId + "\x1F" + r.SectionId), StringComparer.OrdinalIgnoreCase);
                var stillMapped = new HashSet<string>(newConfig.Tags
                    .SelectMany(t => t.PlaylistMappings ?? new List<PlaylistMapping>())
                    .Select(m => m.PlaylistId), StringComparer.OrdinalIgnoreCase);

                lock (_deletedGroupsLock)
                {
                    var tagHist = LoadFileHistory("homescreencompanion_history.txt");
                    var collHist = LoadFileHistory("homescreencompanion_collections.txt");
                    var queue = LoadDeletedGroups();
                    bool tagHistChanged = false, collHistChanged = false, queueChanged = false;
                    foreach (var t in deleted)
                    {
                        var tag = (t.Tag ?? "").Trim();
                        var groupName = !string.IsNullOrWhiteSpace(t.Name) ? $"{t.Name} [{tag}]" : tag;

                        if (t.EnableTag && tag.Length > 0 && !newTags.Contains(tag)
                            && !tagHist.Any(h => string.Equals(h, tag, StringComparison.OrdinalIgnoreCase)))
                        {
                            tagHist.Add(tag);
                            tagHistChanged = true;
                        }
                        if (tag.Length > 0 && !newTags.Contains(tag))
                            try { var rf = RankFilePath(tag); if (File.Exists(rf)) File.Delete(rf); } catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }

                        var coll = CollectionNameOf(t);
                        if (t.EnableCollection && coll.Length > 0 && !newCollections.Contains(coll)
                            && !collHist.Any(h => string.Equals(h, coll, StringComparison.OrdinalIgnoreCase)))
                        {
                            collHist.Add(coll);
                            collHistChanged = true;
                        }

                        foreach (var r in t.HomeSectionTracked ?? new List<HomeSectionTracking>())
                        {
                            if (string.IsNullOrEmpty(r.UserId) || string.IsNullOrEmpty(r.SectionId)) continue;
                            if (stillTracked.Contains(r.UserId + "\x1F" + r.SectionId)) continue;
                            if (queue.HomeRows.Any(x => string.Equals(x.UserId, r.UserId, StringComparison.OrdinalIgnoreCase) && x.SectionId == r.SectionId)) continue;
                            queue.HomeRows.Add(new RemovedHomeRow
                            {
                                GroupName = groupName,
                                UserId = r.UserId,
                                SectionId = r.SectionId,
                                SectionMarker = SectionMarker(t),
                                HomeSectionSettings = t.HomeSectionSettings ?? "{}",
                                DefaultTitle = DefaultRowTitle(t)
                            });
                            queueChanged = true;
                        }

                        foreach (var m in t.PlaylistMappings ?? new List<PlaylistMapping>())
                        {
                            if (string.IsNullOrEmpty(m.PlaylistId) || stillMapped.Contains(m.PlaylistId)) continue;
                            if (queue.Playlists.Any(x => string.Equals(x.PlaylistId, m.PlaylistId, StringComparison.OrdinalIgnoreCase))) continue;
                            queue.Playlists.Add(new RemovedPlaylist { GroupName = groupName, UserId = m.UserId, PlaylistId = m.PlaylistId });
                            queueChanged = true;
                        }
                    }
                    if (tagHistChanged) SaveFileHistory("homescreencompanion_history.txt", tagHist);
                    if (collHistChanged) SaveFileHistory("homescreencompanion_collections.txt", collHist);
                    if (queueChanged) SaveDeletedGroups(queue);
                }
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
        }

        private static string CollectionNameOf(TagConfig t) =>
            (string.IsNullOrWhiteSpace(t.CollectionName) ? (t.Tag ?? "") : t.CollectionName).Trim();

        // Full run: removes the home rows and playlists of deleted groups (see RecordDeletedGroups).
        // Returns the number of groups whose home rows were removed.
        private int _deletedGroupRowsRemoved;

        private int RemoveDeletedGroupLeftovers(CancellationToken cancellationToken)
        {
            DeletedGroupCleanup queue;
            lock (_deletedGroupsLock) queue = LoadDeletedGroups();
            if (queue.HomeRows.Count == 0 && queue.Playlists.Count == 0) return 0;

            var failedRows = new List<RemovedHomeRow>();
            var failedPlaylists = new List<RemovedPlaylist>();
            int groupsWithRows = 0;
            foreach (var byGroup in queue.HomeRows.GroupBy(r => r.GroupName))
            {
                int n = 0;
                foreach (var r in byGroup)
                {
                    long uid;
                    // A user deleted since: nothing left to remove.
                    try { uid = _userManager.GetInternalId(r.UserId); }
                    catch { continue; }
                    try
                    {
                        if (DeleteSectionForUser(uid, r.UserId, r.SectionId, r.SectionMarker, r.HomeSectionSettings, cancellationToken, r.DefaultTitle)) n++;
                    }
                    catch (Exception ex)
                    {
                        _log.Warn($"{byGroup.Key}: home section could not be removed: {ex.Message}");
                        failedRows.Add(r);
                    }
                }
                if (n > 0)
                {
                    groupsWithRows++;
                    _log.Skip($"{byGroup.Key}: home section removed for {RunLog.Plural(n, "user")} (group was deleted)");
                }
            }

            foreach (var byGroup in queue.Playlists.GroupBy(p => p.GroupName))
            {
                int n = 0;
                foreach (var p in byGroup)
                {
                    if (!Guid.TryParse(p.PlaylistId, out var guid)) continue;
                    var pl = _libraryManager.GetItemById(guid);
                    if (pl == null) continue; // already gone
                    try { DeletePlaylist(pl); n++; }
                    catch (Exception ex)
                    {
                        _log.Warn($"Playlist '{pl.Name}' could not be removed: {ex.Message}");
                        failedPlaylists.Add(p);
                    }
                }
                if (n > 0)
                    _log.Skip($"{byGroup.Key}: playlist removed for {RunLog.Plural(n, "user")} (group was deleted)");
            }

            lock (_deletedGroupsLock)
            {
                // Anything queued while this ran stays for the next run.
                var now = LoadDeletedGroups();
                now.HomeRows.RemoveAll(x => queue.HomeRows.Any(r => r.UserId == x.UserId && r.SectionId == x.SectionId)
                    && !failedRows.Any(f => f.UserId == x.UserId && f.SectionId == x.SectionId));
                now.Playlists.RemoveAll(x => queue.Playlists.Any(p => p.PlaylistId == x.PlaylistId)
                    && !failedPlaylists.Any(f => f.PlaylistId == x.PlaylistId));
                SaveDeletedGroups(now);
            }
            return groupsWithRows;
        }

        // Rank file entry: the IMDb id, or the item's own id for items without one (shows from
        // local sources often have none). Readers that only know IMDb ids skip the others.
        internal static string RankKey(BaseItem item)
        {
            var imdb = item.GetProviderId("Imdb");
            return !string.IsNullOrEmpty(imdb) ? imdb : item.Id.ToString("N");
        }

        // ── "Popular on this server" rule (criterion Popular:<days>:<op>:<viewers>) ───────────
        // Viewers = different users who watched a title in the last <days> days (0 = all time);
        // anyone who watched an episode counts for the show. Counted once per run per window
        // (PopularityCounter reads each user's watch data once).
        private PopularityCounter? _popularity;
        private List<BaseItem> _popularityScope = new List<BaseItem>();
        private readonly Dictionary<(int Days, int Ago), Dictionary<Guid, int>> _viewersByDays = new Dictionary<(int Days, int Ago), Dictionary<Guid, int>>();
        // Rank movement: while > 0, the Popular rule is worked out as if today were that many days
        // earlier. The week-ago ranking of each Popular source, by tag, is saved with the rank file.
        private int _popularityAgo;
        private readonly Dictionary<string, List<string>> _weekAgoByTag = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);

        // "Ignore titles only marked as watched" (Settings): the real plays in Emby's activity log,
        // read once per run when a source has a Last Played or Popular rule or asks the AI with the
        // user's recently watched titles. Null when off or not needed.
        private PlaybackHistory? _playHistory;

        private void StartPlaybackHistory(PluginConfiguration config, IEnumerable<TagConfig> sources)
        {
            _playHistory = null;
            if (!config.IgnoreMarkedAsWatched) return;
            int days = 0;
            foreach (var t in sources)
            {
                if (t.SourceType == "AI" && t.AiIncludeRecentlyWatched) days = Math.Max(days, 365);
                foreach (var c in GetAllCriteria(t))
                {
                    var p = c.TrimStart('!').Split(':');
                    if (p.Length == 4 && p[0] == "LastPlayed" && double.TryParse(p[3], System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out var n))
                        days = Math.Max(days, (int)Math.Ceiling(n) + 1);
                    // A Popular window, plus the week back that rank movement looks at.
                    if (p.Length == 4 && p[0] == "Popular" && int.TryParse(p[1], out var d) && d > 0)
                        days = Math.Max(days, d + 8);
                }
            }
            if (days == 0) return;
            try
            {
                _playHistory = PlaybackHistory.Load(_activityManager, days);
                _log.Debug($"Real plays (activity log, last {days} days): {_playHistory.Plays} user/title pairs — titles only marked as watched are not counted as recently played");
            }
            catch (Exception ex)
            {
                _log.Warn($"The activity log could not be read, so titles marked as watched count as played: {ex.Message}");
            }
        }

        // The last-played date a rule sees: with "Ignore titles only marked as watched" only a real play counts.
        private DateTimeOffset? Played(User user, BaseItem item, DateTimeOffset? lastPlayed)
            => _playHistory == null ? lastPlayed : _playHistory.Effective(user.InternalId, item.InternalId, lastPlayed);

        // The members of a collection or playlist for the Collection: / Playlist: rules: each title
        // with its other versions, a show with all its episodes (one query for every show — one per
        // show made a big collection take minutes), and an episode in a playlist with its show (as
        // for a Local playlist source).
        private HashSet<long> MembershipIds(IEnumerable<BaseItem> members, List<BaseItem> allItems, ref Dictionary<string, List<long>>? versionIdsByKey)
        {
            var ids = new HashSet<long>();
            var seriesIds = new List<long>();
            foreach (var m in members)
            {
                ids.Add(m.InternalId);
                AddOtherVersions(ids, m, allItems, ref versionIdsByKey);
                if (m is MediaBrowser.Controller.Entities.TV.Series) seriesIds.Add(m.InternalId);
                else if (m is MediaBrowser.Controller.Entities.TV.Episode ep && ep.Series is BaseItem show) ids.Add(show.InternalId);
            }
            if (seriesIds.Count > 0)
                foreach (var ep in _libraryManager.GetItemList(new InternalItemsQuery
                {
                    IncludeItemTypes = new[] { "Episode" },
                    AncestorIds = seriesIds.ToArray(),
                    Recursive = true,
                    IsVirtualItem = false
                }))
                    ids.Add(ep.InternalId);
            return ids;
        }

        // A user's played state of one title for the filter rules (IsPlayed, PlayCount, LastPlayed,
        // WatchedByCount). Emby keeps no played flag on a Series — it works it out from the episodes
        // (every episode watched = played), as its own apps show it — so a show asks Emby for that.
        private (bool Played, DateTimeOffset? LastPlayedDate, int PlayCount) UserDataOf(User user, BaseItem item)
        {
            var ud = _userDataManager?.GetUserData(user, item);
            bool played = item is MediaBrowser.Controller.Entities.TV.Series ? SeriesPlayed(user, item) : ud?.Played ?? false;
            return (played, ud?.LastPlayedDate, ud?.PlayCount ?? 0);
        }

        // The shows a user has watched to the end, one query per user and run (see UserDataOf).
        private readonly Dictionary<Guid, HashSet<long>> _playedSeries = new Dictionary<Guid, HashSet<long>>();
        private bool SeriesPlayed(User user, BaseItem series)
        {
            HashSet<long> set;
            lock (_playedSeries)
            {
                if (!_playedSeries.TryGetValue(user.Id, out set!))
                {
                    set = new HashSet<long>(_libraryManager.GetItemList(new InternalItemsQuery
                    {
                        User = user,
                        IncludeItemTypes = new[] { "Series" },
                        Recursive = true,
                        IsPlayed = true
                    }).Select(i => i.InternalId));
                    _playedSeries[user.Id] = set;
                }
            }
            return set.Contains(series.InternalId);
        }

        private void StartPopularity(List<BaseItem> scope)
        {
            lock (_playedSeries) _playedSeries.Clear();
            _popularity = new PopularityCounter(_libraryManager, _userManager, _userDataManager);
            _popularityScope = scope;
            _viewersByDays.Clear();
            _weekAgoByTag.Clear();
            _nextWatch = null;
        }

        // ── "Your Next Watch" (source type NextWatch) ─────────────────────────────────────────
        // Per-user picks from NextWatchRecommender, written to each Playlist-tab user's own playlist
        // (the standard per-user playlist of every source, but with each user's own items). A tag or
        // collection, when enabled, gets the titles picked for the most users (shared by everyone).
        private NextWatchRecommender? _nextWatch;
        private Dictionary<string, List<BaseItem>>? _nextWatchPerUser;

        internal static bool IsNextWatch(TagConfig t) => string.Equals(t?.SourceType, "NextWatch", StringComparison.OrdinalIgnoreCase);

        // One-time migration: older NextWatch configs kept their users on the Home Screen tab
        // (HomeSectionUserIds) and left PlaylistUserIds empty. The Playlist tab's users now decide
        // who gets a list, so an empty PlaylistUserIds (with Create Playlist on) takes the Home
        // Screen tab's users — once: NextWatchUsersMigrated is then set, so a Playlist user list
        // the user empties later stays empty. Returns true when the config was changed (the caller saves it).
        internal static bool NormalizeNextWatch(TagConfig t)
        {
            if (!IsNextWatch(t) || t.NextWatchUsersMigrated) return false;
            if (t.EnablePlaylist && (t.PlaylistUserIds?.Count ?? 0) == 0 && (t.HomeSectionUserIds?.Count ?? 0) > 0)
                t.PlaylistUserIds = t.HomeSectionUserIds.Distinct(StringComparer.OrdinalIgnoreCase).ToList();
            t.NextWatchUsersMigrated = true;
            return true;
        }

        // The users a NextWatch source makes lists (playlists) for: the Playlist tab's users.
        private static List<string> NextWatchUserIds(TagConfig t) =>
            (t.PlaylistUserIds ?? new List<string>()).Distinct(StringComparer.OrdinalIgnoreCase).ToList();

        // The InternalId (as a section's ParentId wants it) of a user's playlist from this source.
        // A user's own playlists with this exact name. Scoped to the user, so another user's
        // private playlist is never returned (playlists HSC makes are private to their user).
        private List<BaseItem> OwnPlaylistsNamed(User user, string name)
        {
            try
            {
                return _libraryManager.GetItemList(new InternalItemsQuery
                {
                    User = user,
                    IncludeItemTypes = new[] { "Playlist" },
                    Name = name,
                    Recursive = true
                }).Where(p => p.GetType().Name.Contains("Playlist")
                           && string.Equals(p.Name, name, StringComparison.OrdinalIgnoreCase)).ToList();
            }
            catch { return new List<BaseItem>(); }
        }

        // A playlist is removed with its folder (userplaylists\<name> [playlist]\<name>.m3u): left
        // behind, Emby indexes the .m3u again as a new playlist that no user owns.
        private void DeletePlaylist(BaseItem playlist)
            => _libraryManager.DeleteItem(playlist, new DeleteOptions { DeleteFileLocation = true });

        // A playlist's entries in playlist order: (entry id, the item it stands for). An episode stands
        // for its season or show when the source lists that season / show instead (Emby expands both).
        private static List<(long Entry, long Id)> PlaylistEntries(IEnumerable<BaseItem> playlistItems, HashSet<long> desired)
        {
            var list = new List<(long Entry, long Id)>();
            foreach (var pItem in playlistItems)
            {
                long entryId = 0;
                try { entryId = pItem.ListItemEntryId; } catch { }
                if (entryId == 0) entryId = pItem.InternalId;

                BaseItem inner = pItem;
                if (pItem.GetType().Name.Contains("PlaylistItem"))
                {
                    try { var temp = ((dynamic)pItem).Item; if (temp != null) inner = (BaseItem)temp; } catch { }
                }
                long id = inner.InternalId;
                if (!desired.Contains(id) && inner is MediaBrowser.Controller.Entities.TV.Episode ep)
                {
                    var season = ep.Season;
                    var series = ep.Series;
                    if (season != null && desired.Contains(season.InternalId)) id = season.InternalId;
                    else if (series != null && desired.Contains(series.InternalId)) id = series.InternalId;
                }
                list.Add((entryId, id));
            }
            return list;
        }

        private int PlaylistEntryCount(BaseItem playlist)
        {
            try { return _libraryManager.GetItemList(new InternalItemsQuery { ListIds = new[] { playlist.InternalId } }).Length; }
            catch { return 0; }
        }

        private List<long> PlaylistEntryIds(BaseItem playlist)
        {
            try { return _libraryManager.GetItemList(new InternalItemsQuery { ListIds = new[] { playlist.InternalId } }).Select(i => i.InternalId).ToList(); }
            catch { return new List<long> { -1 }; }   // unreadable: never treated as ours
        }

        private string? PlaylistInternalIdFor(TagConfig t, string userId)
        {
            var mapping = t.PlaylistMappings?.FirstOrDefault(m => string.Equals(m.UserId, userId, StringComparison.OrdinalIgnoreCase));
            if (mapping == null || !Guid.TryParse(mapping.PlaylistId, out var g)) return null;
            var pl = _libraryManager.GetItemById(g);
            return pl != null && pl.GetType().Name.Contains("Playlist") ? pl.InternalId.ToString() : null;
        }

        // The look of every playlist a source makes (any type; Your Next Watch too), the same for
        // all its users: TagConfig.Playlist* — a generated or uploaded poster (Primary) and
        // background (Backdrop), and PlaylistDescription as the Overview (Your Next Watch: empty =
        // DefaultNextWatchDescription). Only what differs is written, so a sync over many
        // playlists rewrites nothing when the look is unchanged; generated art is drawn once per
        // source and run (RenderCollectionArt caches it by titles + style). A saved Primary that
        // is a file outside the playlist's own metadata folder is never replaced by Emby's collage
        // (BaseCollageImageProvider.HasChanged), so nothing needs locking.
        internal const string DefaultNextWatchDescription = "Picked just for you from what you've been watching — fresh recommendations every day.";

        private static bool HasPlaylistLook(TagConfig tc) =>
            CollectionArtRenderer.IsStyle(tc.PlaylistPosterStyle) || CollectionArtRenderer.IsStyle(tc.PlaylistBackgroundStyle)
            || !string.IsNullOrWhiteSpace(tc.PlaylistPosterPath) || !string.IsNullOrWhiteSpace(tc.PlaylistBackgroundPath)
            || !string.IsNullOrWhiteSpace(PlaylistDescriptionOf(tc));

        private static string PlaylistDescriptionOf(TagConfig tc) =>
            !string.IsNullOrWhiteSpace(tc.PlaylistDescription) ? tc.PlaylistDescription.Trim()
            : IsNextWatch(tc) ? DefaultNextWatchDescription : "";

        // Titles for generated playlist art (shows instead of their episodes or seasons). One list:
        // in list order. Several (Your Next Watch, one per user): the titles most users got first,
        // so the art is the same for everyone.
        private static List<BaseItem> PlaylistArtItems(IEnumerable<List<BaseItem>> lists)
        {
            var stats = new Dictionary<Guid, (BaseItem Item, int Users, int BestRank)>();
            foreach (var list in lists)
            {
                int rank = 0;
                foreach (var raw in list)
                {
                    var item = raw is MediaBrowser.Controller.Entities.TV.Episode ep ? ep.Series
                             : raw is MediaBrowser.Controller.Entities.TV.Season se ? se.Series : raw;
                    rank++;
                    if (item == null) continue;
                    stats[item.Id] = stats.TryGetValue(item.Id, out var s)
                        ? (s.Item, s.Users + 1, Math.Min(s.BestRank, rank))
                        : (item, 1, rank);
                }
            }
            return stats.Values.OrderByDescending(s => s.Users).ThenBy(s => s.BestRank).ThenBy(s => s.Item.Id)
                .Select(s => s.Item).ToList();
        }

        private void ApplyPlaylistLook(TagConfig tc, string plName, string userName, PlaylistMapping mapping, List<BaseItem> artItems)
        {
            try
            {
                if (!Guid.TryParse(mapping.PlaylistId, out var g)) return;
                var pl = _libraryManager.GetItemById(g);
                if (pl == null || !pl.GetType().Name.Contains("Playlist")) return;
                // Art lives in playlist_art/<source>; the text on it is the playlist name.
                if (SetGeneratedArt(pl, plName, "playlist_art", tc.PlaylistPosterStyle, tc.PlaylistBackgroundStyle,
                        tc.PlaylistArtTitle, tc.PlaylistBackgroundPath, artItems, true, tc.PlaylistPosterPath, artKey: tc.Name + " - " + tc.Tag,
                        posterOptions: tc.PlaylistPosterOptions, backgroundOptions: tc.PlaylistBackgroundOptions))
                {
                    _libraryManager.UpdateItem(pl, pl.GetParent(), ItemUpdateType.ImageUpdate, null);
                    _log.Debug($"  {userName}: playlist poster / background updated");
                }
                var desc = PlaylistDescriptionOf(tc);
                // An empty description leaves the Overview alone.
                if (desc.Length > 0 && !string.Equals(pl.Overview ?? "", desc, StringComparison.Ordinal))
                {
                    pl.Overview = desc;
                    _libraryManager.UpdateItem(pl, pl.GetParent(), ItemUpdateType.MetadataEdit, null);
                    _log.Debug($"  {userName}: playlist description updated");
                }
            }
            catch (Exception ex)
            {
                _log.Warn($"Playlist \"{plName}\": look for {userName} could not be applied: {ex.Message}");
            }
        }

        // An empty Custom Title is the pre-filled one: the group's name, else its tag.
        internal static void FillDefaultCustomName(Dictionary<string, string> settings, TagConfig tc)
        {
            if (settings.TryGetValue("CustomName", out var cn) && !string.IsNullOrWhiteSpace(cn)) return;
            var name = DefaultRowTitle(tc);
            if (name.Length > 0) settings["CustomName"] = name;
        }

        internal static string DefaultRowTitle(TagConfig tc)
            => (!string.IsNullOrWhiteSpace(tc.Name) ? tc.Name : tc.Tag ?? "").Trim();

        // A "playlist" home row (SectionType "playlist", any source with Create Playlist) shows the
        // user's own playlist from the source: the playlist's items (no item-type or library filter;
        // ParentId is set per user). The row keeps the source's own Sort By / Sort Order like any other row:
        // empty (the default) = playlist order, best pick first; Random = Emby shuffles the row on
        // every home-screen load (the web client sends a new RandomSeed). Image type Primary (the default here;
        // only an explicit Thumb is kept) makes Emby's home screen draw a show's first episode with
        // the show's poster (hometab: ImageType "Primary" → preferSeriesImage; imagehelper then uses
        // SeriesPrimaryImageTag, 2:3), so shows look like the movies next to them. The episode's own
        // image is not touched.
        internal static void NormalizePlaylistSectionSettings(Dictionary<string, string> settings)
        {
            if (!settings.TryGetValue("SectionType", out var st) || st != "playlist") return;
            if (!settings.TryGetValue("ImageType", out var imageType) || !string.Equals(imageType, "Thumb", StringComparison.OrdinalIgnoreCase))
                settings["ImageType"] = "Primary";
            settings["ItemTypes"] = "[]";
            settings["ExcludedFolders"] = "[]";
            if (!settings.ContainsKey("SortBy")) settings["SortBy"] = "";
            if (!settings.ContainsKey("SortOrder")) settings["SortOrder"] = "";
            foreach (var k in settings.Keys.Where(k => k.StartsWith("_query", StringComparison.OrdinalIgnoreCase)).ToList())
                settings.Remove(k);
        }

        private Dictionary<string, List<BaseItem>> ComputeNextWatch(TagConfig tagConfig, List<BaseItem> matchableItems, PluginConfiguration config,
            HashSet<string> blacklist, Func<BaseItem, bool>? allow, GroupRunStats gs, IEnumerable<string> userIds)
        {
            var perUser = new Dictionary<string, List<BaseItem>>(StringComparer.OrdinalIgnoreCase);
            var ids = userIds.ToList();
            gs.ListCount = ids.Count;
            if (ids.Count == 0)
            {
                gs.Warnings.Add("No users selected — tick users on the Playlist tab (each gets their own playlist)");
                return perUser;
            }
            int limit = tagConfig.Limit > 0 ? tagConfig.Limit : NextWatchRecommender.DefaultLimit;
            var total = System.Diagnostics.Stopwatch.StartNew();
            if (_nextWatch == null)
            {
                var ignoredTags = (config.Tags ?? new List<TagConfig>()).Select(t => (t.Tag ?? "").Trim())
                    .Concat((config.TopLists ?? new List<TopListHomeSection>()).Select(t => (t.TagName ?? "").Trim()))
                    .Where(t => t.Length > 0);
                _popularity ??= new PopularityCounter(_libraryManager, _userManager, _userDataManager);
                _nextWatch = new NextWatchRecommender(_libraryManager, _userManager, _popularity,
                    matchableItems.Where(i => i.LocationType == LocationType.FileSystem), ignoredTags, m => _log.Debug("  " + m));
            }
            var prepTimer = System.Diagnostics.Stopwatch.StartNew();
            _nextWatch.EnsurePrepared();
            long prepMs = prepTimer.ElapsedMilliseconds;
            int picks = 0;
            foreach (var userId in ids)
            {
                if (!Guid.TryParse(userId, out var guid)) continue;
                var user = _userManager.GetUserById(guid);
                if (user == null) { _log.Debug($"  User {userId} no longer exists — skipped"); continue; }
                if (_preview == null && SkipDisabled(userId)) { _log.Debug($"    Next Watch for {user.Name}: skipped — disabled in Emby (their playlist is kept)"); continue; }
                try
                {
                    var r = _nextWatch.Recommend(user, limit, allow, blacklist, tagConfig.NextWatchSeedDays, tagConfig.NextWatchUseTaste);
                    // Preview lists the picks; a playlist gets a show's first episode (see FirstEpisode).
                    perUser[userId] = _preview != null ? r.Picks.Select(p => p.Item).ToList()
                        : r.Picks.Select(p => p.Item is MediaBrowser.Controller.Entities.TV.Series ? _nextWatch.FirstEpisode(p.Item) : p.Item)
                            .Where(i => i != null).Select(i => i!).ToList();
                    picks += r.Picks.Count;
                    _log.Info($"    Next Watch for {user.Name}: {r.Picks.Count} picks in {r.ElapsedMs} ms  ·  seed {r.SeedCount}{(r.ColdStart ? " (cold start: popular titles)" : "")}  ·  {r.Candidates:N0} candidates");
                    if (_preview != null)
                    {
                        _preview.Note = $"{user.Name}: {r.SeedSummary}";
                        foreach (var p in r.Picks) _preview.Reasons[p.Item.Id] = p.Reason;
                    }
                    else if (r.Picks.Count > 0)
                        _log.Debug("      " + string.Join(", ", r.Picks.Take(10).Select((p, n) => $"{n + 1}. {p.Item.Name}")));
                }
                catch (Exception ex)
                {
                    gs.Warnings.Add($"Next Watch for {user.Name} failed: {ex.Message}");
                    WriteExceptionDebug(ex);
                }
            }
            gs.MatchCount = picks;
            _log.Info($"    Next Watch: {perUser.Count} user(s), {picks} picks  ·  index {prepMs} ms, total {total.ElapsedMilliseconds} ms");
            return perUser;
        }

        // The settings page does not send LastRunCount / LastRunLimit (runs write them): a save
        // keeps each source's values (same group, URL and local source).
        internal static void KeepLastRunCounts(PluginConfiguration? old, PluginConfiguration? saved)
        {
            if (old?.Tags == null || saved?.Tags == null) return;
            string Key(TagConfig t) => GroupKey(t) + "\x1F" + (t.Url ?? "").Trim() + "\x1F" + (t.LocalSourceId ?? "").Trim();
            var last = new Dictionary<string, TagConfig>(StringComparer.OrdinalIgnoreCase);
            foreach (var t in old.Tags) if (t.LastRunLimit > 0) last[Key(t)] = t;
            foreach (var t in saved.Tags)
                if (t.LastRunLimit == 0 && last.TryGetValue(Key(t), out var o)) { t.LastRunCount = o.LastRunCount; t.LastRunLimit = o.LastRunLimit; }
        }

        // Titles in a source's result: a film's versions count once.
        private int TitleCount(List<BaseItem> items) =>
            items.Select(PopularityCounter.FilmKeys(items)).Distinct(StringComparer.OrdinalIgnoreCase).Count();

        // Stores each source's titles against its Limit (the "9 of 10" chip on the group's card;
        // saved with the run's config save). A source without a Limit, or not counted (-1), is
        // recorded as having none. Your Next Watch is skipped: its Limit is per user.
        private static void RecordLastRun(IEnumerable<(TagConfig Source, int Titles)> sources, GroupRunStats gs)
        {
            int found = 0, limit = 0;
            foreach (var (t, n) in sources)
            {
                bool counted = t.Limit > 0 && n >= 0 && !IsNextWatch(t);
                t.LastRunLimit = counted ? t.Limit : 0;
                t.LastRunCount = counted ? n : 0;
                if (counted) { found += n; limit += t.Limit; }
            }
            if (found < limit) gs.LimitShort = $"{found} of {limit}";
        }

        private int ViewersOf(BaseItem item, int days)
        {
            if (!_viewersByDays.TryGetValue((days, _popularityAgo), out var counts))
            {
                _popularity ??= new PopularityCounter(_libraryManager, _userManager, _userDataManager);
                _viewersByDays[(days, _popularityAgo)] = counts = _popularity.CountViewers(_popularityScope, days, m => _log.Debug("  " + m), _popularityAgo,
                    _playHistory == null ? null : new Func<long, long, DateTimeOffset?, DateTimeOffset?>(_playHistory.Effective));
            }
            return counts.TryGetValue(item.Id, out var n) ? n : 0;
        }

        // The time window of the source's first (not negated) Popular rule, or null without one.
        private static int? PopularRuleDays(TagConfig tagConfig)
        {
            foreach (var c in GetAllCriteria(tagConfig))
            {
                var p = c.Split(':');
                if (p.Length == 4 && p[0] == "Popular")
                    return int.TryParse(p[1], out var d) && d > 0 ? d : 0;
            }
            return null;
        }

        // A source with a Popular rule lists its matches most viewed first, so Max items keeps the
        // most popular and a top-list built from the tag follows that order. Sorting in place keeps
        // tagOutputItems/collectionOutputItems (same list) in step. Max items counts films: a film's
        // versions (separate items, same viewers) share one place and stay together.
        private void OrderByPopularity(TagConfig tagConfig, List<BaseItem> matched, int limit)
        {
            var days = PopularRuleDays(tagConfig);
            if (days == null) return;
            var films = matched
                .OrderByDescending(i => ViewersOf(i, days.Value))
                .ThenByDescending(i => i.CommunityRating ?? 0)
                .ThenBy(i => i.SortName, StringComparer.OrdinalIgnoreCase)
                .GroupBy(PopularityCounter.FilmKeys(_popularityScope))
                .ToList();
            if (limit < 10000 && films.Count > limit) films = films.Take(limit).ToList();
            matched.Clear();
            matched.AddRange(films.SelectMany(f => f));
            _log.Info($"  Most viewed first ({(days.Value > 0 ? $"last {days.Value} days" : "all time")}): {films.Count} kept"
                + (films.Count > 0 ? " — " + string.Join(", ", films.Take(10).Select((f, n) => $"{n + 1}. {f.First().Name} ({ViewersOf(f.First(), days.Value)})")) : ""));
        }

        // Rank movement for a source with a Popular rule: the same rule worked out as if today were
        // 7 days earlier — window moved back a week, most viewed first, cut to Max items, the other
        // rules as they are now. Kept by tag and saved with the rank file for TopListHistory.
        private void RankWeekAgo(string tagName, TagConfig tagConfig, IEnumerable<BaseItem> candidates, Func<BaseItem, bool> matches, int limit)
        {
            var days = PopularRuleDays(tagConfig);
            if (days == null) return;
            var sw = System.Diagnostics.Stopwatch.StartNew();
            _popularityAgo = 7;
            try
            {
                // Same as OrderByPopularity: Max items counts films (a film's versions share one place).
                var films = candidates.Where(matches)
                    .OrderByDescending(i => ViewersOf(i, days.Value))
                    .ThenByDescending(i => i.CommunityRating ?? 0)
                    .ThenBy(i => i.SortName, StringComparer.OrdinalIgnoreCase)
                    .GroupBy(PopularityCounter.FilmKeys(_popularityScope))
                    .ToList();
                if (limit < 10000 && films.Count > limit) films = films.Take(limit).ToList();
                var keys = films.SelectMany(f => f).Select(RankKey).Where(k => !string.IsNullOrEmpty(k)).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
                if (!_weekAgoByTag.TryGetValue(tagName, out var list)) _weekAgoByTag[tagName] = keys;
                else list.AddRange(keys.Where(k => !list.Contains(k, StringComparer.OrdinalIgnoreCase)));
                _log.Info($"  Rank movement vs 7 days ago: {films.Count} ranked then"
                    + (films.Count > 0 ? " — " + string.Join(", ", films.Take(10).Select((f, n) => $"{n + 1}. {f.First().Name} ({ViewersOf(f.First(), days.Value)})")) : "")
                    + $"  ·  {sw.ElapsedMilliseconds} ms");
            }
            finally { _popularityAgo = 0; }
        }

        // Writes tag_ranks/<tag>.json — the IMDb ids of a tag's matched items in source order,
        // used by SyncTopListFolders to number .strm files.
        private void WriteRankFile(string tagName, List<string> imdbIds)
        {
            try
            {
                var rankFile = RankFilePath(tagName);
                Directory.CreateDirectory(Path.GetDirectoryName(rankFile)!);
                _jsonSerializer.SerializeToFile(imdbIds, rankFile);
                TopListHistory.SaveWeekAgo(tagName ?? "", _weekAgoByTag.TryGetValue(tagName ?? "", out var weekAgo) ? weekAgo : null, _jsonSerializer);
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
        }

        // Full sync: the titles of each source top-list, one part per group that writes its tag
        // (TopListMembers). A group that ran writes its titles (with the old ones kept when one of its
        // sources failed with "Preserve…" on); a group that failed altogether keeps its part; a group
        // switched off or out of its schedule gets an empty one, as its tag is taken off the titles.
        private void SaveTopListMembers(PluginConfiguration config, Dictionary<string, Dictionary<string, List<BaseItem>>> memberItemsByGroup, HashSet<string> failedGroups)
        {
            foreach (var tag in (config.TopLists ?? new List<TopListHomeSection>())
                .Where(t => t.FromSource && !string.IsNullOrWhiteSpace(t.TagName)).Select(t => t.TagName.Trim())
                .Distinct(StringComparer.OrdinalIgnoreCase))
            {
                var groups = TopListMembers.GroupsOf(config, tag);
                memberItemsByGroup.TryGetValue(tag, out var ran);
                bool saved = true;
                foreach (var g in groups)
                {
                    if (ran != null && ran.TryGetValue(g, out var items))
                        saved &= TopListMembers.SavePart(tag, g, items, _jsonSerializer, keepOld: failedGroups.Contains(g));
                    else if (!failedGroups.Contains(g) && (config.Tags ?? new List<TagConfig>())
                        .Where(t => string.Equals(GroupKey(t), g, StringComparison.OrdinalIgnoreCase))
                        .All(t => !t.Active || !IsScheduleActive(t.ActiveIntervals)))
                        saved &= TopListMembers.SavePart(tag, g, new List<BaseItem>(), _jsonSerializer);
                }
                // A part that could not be written: the list keeps its last titles.
                if (!saved || !TopListMembers.Combine(tag, groups, _jsonSerializer))
                    _log.Warn($"Top-list '{tag}': its titles could not be saved — it keeps its last titles (see the plugin log)");
            }
        }

        // Run Group: this group's part of each source top-list built from its tag (the other groups'
        // parts stay as the last run left them). empty: the group is switched off or out of schedule.
        private void SaveTopListPart(PluginConfiguration config, TagConfig tagConfig, string tagName, IEnumerable<BaseItem> items)
        {
            if (!TopListMembers.Wanted(config, tagName)) return;
            if (!TopListMembers.SavePart(tagName, GroupKey(tagConfig), items, _jsonSerializer)
                || !TopListMembers.Combine(tagName, TopListMembers.GroupsOf(config, tagName), _jsonSerializer))
                _log.Warn($"Top-list '{tagName}': its titles could not be saved — it keeps its last titles (see the plugin log)");
        }

        // Playlist sync for a single group — one individual playlist per user in PlaylistUserIds.
        // Shared by both the full sync (Execute) and the single-group run (RunSingleEntryInternalAsync)
        // so both paths create/update playlists identically. collectionOutputItems must be the union
        // of all sources in the group, since the sync removes anything not in the list.
        // perUser (NextWatch): each user's own list instead of collectionOutputItems; only the users
        // in it are synced.
        private async Task SyncPlaylistsForEntryAsync(TagConfig tagConfig, List<BaseItem> collectionOutputItems, bool dryRun, GroupRunStats? gs = null,
            Dictionary<string, List<BaseItem>>? perUser = null)
        {
            // Playlist sync — one individual playlist per user in PlaylistUserIds
            if (tagConfig.EnablePlaylist && !dryRun && !StillWanted(tagConfig))
                _log.Skip($"Playlist \"{(string.IsNullOrWhiteSpace(tagConfig.PlaylistName) ? tagConfig.Name : tagConfig.PlaylistName)}\": not synced — the group was deleted or switched off while this run was busy");
            else if (tagConfig.EnablePlaylist && !dryRun)
            {
                string _plLogName = string.IsNullOrWhiteSpace(tagConfig.PlaylistName) ? tagConfig.Name : tagConfig.PlaylistName;
                _log.Section($"Playlist \"{_plLogName}\"");
                try
                {
                    // Deduplicate — pick one physical version per logical movie (IMDb > TMDb > InternalId).
                    // Keep an ordered list mirroring the source order plus a set for fast membership checks.
                    (List<long> List, HashSet<long> Set) Desired(List<BaseItem> items)
                    {
                        var seenPlKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                        var list = new List<long>();   // ordered, mirrors source
                        var set = new HashSet<long>();  // fast Contains
                        foreach (var item in items)
                        {
                            var key = item.GetProviderId("Imdb")
                                   ?? item.GetProviderId("Tmdb")
                                   ?? item.InternalId.ToString();
                            if (seenPlKeys.Add(key)) { list.Add(item.InternalId); set.Add(item.InternalId); }
                        }
                        return (list, set);
                    }
                    var shared = Desired(collectionOutputItems);
                    var plName0 = string.IsNullOrWhiteSpace(tagConfig.PlaylistName) ? tagConfig.Name : tagConfig.PlaylistName;
                    var plUserIds = perUser == null ? tagConfig.PlaylistUserIds : tagConfig.PlaylistUserIds.Where(perUser.ContainsKey).ToList();
                    if (perUser == null) _log.Debug($"  {shared.List.Count} unique items for {tagConfig.PlaylistUserIds.Count} users");
                    var lookItems = perUser == null ? PlaylistArtItems(new[] { collectionOutputItems }) : PlaylistArtItems(perUser.Values);
                    var createdPlaylists = new List<(string UserName, PlaylistMapping Mapping)>();
                    bool plMappingChanged = false;
                    int _plUserIdx = 0;
                    foreach (var userId in plUserIds)
                    {
                        _log.Count($"Playlists — {plName0}:", ++_plUserIdx, plUserIds.Count, "users");
                        var (desiredPlIdList, desiredPlIdSet) = perUser == null ? shared : Desired(perUser[userId]);
                        if (!Guid.TryParse(userId, out var userGuid)) continue;
                        var plUser = _userManager.GetUserById(userGuid);
                        if (plUser == null) { _log.Debug($"  User {userId} no longer exists — skipped"); continue; }
                        if (SkipDisabled(userId)) { _log.Debug($"  {plUser.Name}: skipped — disabled in Emby (their playlist is kept)"); continue; }

                        var plName =string.IsNullOrWhiteSpace(tagConfig.PlaylistName) ? tagConfig.Name : tagConfig.PlaylistName;

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

                        // Playlists already mapped to other users of this source, or to any user of
                        // another source (which may use the same playlist name) — never claimed here.
                        var claimedByOthers = (tagConfig.PlaylistMappings ?? new List<PlaylistMapping>())
                            .Where(m => !string.Equals(m.UserId, userId, StringComparison.OrdinalIgnoreCase))
                            .Concat((Plugin.Instance?.Configuration?.Tags ?? new List<TagConfig>())
                                .Where(t => !ReferenceEquals(t, tagConfig)
                                         && !string.Equals(GroupKey(t), GroupKey(tagConfig), StringComparison.OrdinalIgnoreCase))
                                .SelectMany(t => t.PlaylistMappings ?? new List<PlaylistMapping>()))
                            .Select(m => Guid.TryParse(m.PlaylistId, out var g) ? g : Guid.Empty)
                            .Where(g => g != Guid.Empty)
                            .ToHashSet();

                        // 2. This user's own playlists with this name (a user-scoped query only sees
                        // the user's private playlists, never another user's). Adopts one made by an
                        // earlier run whose id was not confirmed, and removes duplicates of it.
                        var ownSameName = OwnPlaylistsNamed(plUser, plName)
                            .Where(p => !claimedByOthers.Contains(p.Id))
                            .ToList();
                        if (existingPlaylist == null && ownSameName.Count > 0)
                        {
                            existingPlaylist = ownSameName
                                .OrderByDescending(p => PlaylistEntryCount(p))
                                .ThenByDescending(p => p.DateCreated)
                                .First();
                            if (mapping == null)
                            {
                                mapping = new PlaylistMapping { UserId = userId };
                                tagConfig.PlaylistMappings ??= new List<PlaylistMapping>();
                                tagConfig.PlaylistMappings.Add(mapping);
                            }
                            mapping.PlaylistId = existingPlaylist.Id.ToString();
                            plMappingChanged = true;
                            _log.Info($"  Playlist \"{plName}\": {plUser.Name}'s existing playlist linked (id {existingPlaylist.Id})");
                        }
                        if (existingPlaylist != null)
                        {
                            // Only a duplicate holding nothing but titles this source puts there is
                            // one of ours; a playlist the user made by hand with the same name stays.
                            var ourIds = new HashSet<long>(desiredPlIdSet);
                            if (mapping?.LastSyncedItemIds != null) ourIds.UnionWith(mapping.LastSyncedItemIds);
                            foreach (var dup in ownSameName.Where(p => p.Id != existingPlaylist.Id))
                            {
                                if (!PlaylistEntryIds(dup).All(ourIds.Contains))
                                {
                                    _log.Debug($"  Playlist \"{plName}\": {plUser.Name} has another playlist with this name (id {dup.Id}) with other titles — left alone");
                                    continue;
                                }
                                try
                                {
                                    DeletePlaylist(dup);
                                    _log.Info($"  Playlist \"{plName}\": removed duplicate playlist of {plUser.Name} (id {dup.Id})");
                                }
                                catch (Exception ex) { _log.Warn($"Playlist \"{plName}\": duplicate of {plUser.Name} could not be removed: {ex.Message}"); }
                            }
                        }

                        if (existingPlaylist == null)
                        {
                            // 3. Create new playlist
                            var created = await _playlistManager.CreatePlaylist(new PlaylistCreationRequest
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

                            // The id Emby returns (a Guid or an InternalId depending on the Emby
                            // version); else this user's own newest playlist with the name, retried
                            // briefly (a search right after creating can miss it under load).
                            BaseItem? newPl = null;
                            var createdId = created?.Id;
                            if (!string.IsNullOrEmpty(createdId))
                            {
                                if (Guid.TryParse(createdId, out var cg)) newPl = _libraryManager.GetItemById(cg);
                                else if (long.TryParse(createdId, out var cl)) newPl = _libraryManager.GetItemById(cl);
                                if (newPl != null && !newPl.GetType().Name.Contains("Playlist")) newPl = null;
                            }
                            for (int attempt = 0; newPl == null && attempt < 5; attempt++)
                            {
                                if (attempt > 0) await Task.Delay(1000);
                                newPl = OwnPlaylistsNamed(plUser, plName)
                                    .Where(p => !claimedByOthers.Contains(p.Id))
                                    .OrderByDescending(p => p.DateCreated)
                                    .FirstOrDefault();
                            }

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
                            // Emby puts a show (or season) into a playlist as all its episodes: an episode
                            // counts as the show / season this source lists, so the episodes are not taken
                            // out and the show added again on every sync.
                            var playlistItems = _libraryManager.GetItemList(
                                new InternalItemsQuery { ListIds = new[] { existingPlaylist.InternalId }, OrderBy = new[] { ("ListItemOrder", SortOrder.Ascending) } });
                            var currentEntries = PlaylistEntries(playlistItems, desiredPlIdSet);
                            var presentIds = new HashSet<long>(currentEntries.Select(e => e.Id));

                            var entryIdsToRemove = currentEntries
                                .Where(e => !desiredPlIdSet.Contains(e.Id))
                                .Select(e => e.Entry)
                                .ToList();

                            var toAdd = desiredPlIdList.Where(id => !presentIds.Contains(id)).ToArray();

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
                            // items just added) and move each entry into its place; a show's episodes move
                            // together, in their own order. Only an entry that is out of place is moved.
                            var afterEntries = PlaylistEntries(_libraryManager.GetItemList(
                                new InternalItemsQuery { ListIds = new[] { existingPlaylist.InternalId }, OrderBy = new[] { ("ListItemOrder", SortOrder.Ascending) } }),
                                desiredPlIdSet);
                            var targetOrder = desiredPlIdList
                                .SelectMany(id => afterEntries.Where(e => e.Id == id).Select(e => e.Entry))
                                .Concat(afterEntries.Where(e => !desiredPlIdSet.Contains(e.Id)).Select(e => e.Entry))
                                .ToList();
                            var currentOrder = afterEntries.Select(e => e.Entry).ToList();

                            bool reordered = false;
                            for (int targetIndex = 0; targetIndex < targetOrder.Count && targetIndex < currentOrder.Count; targetIndex++)
                            {
                                long wanted = targetOrder[targetIndex];
                                if (currentOrder[targetIndex] == wanted) continue;   // already in place

                                try
                                {
                                    await _playlistManager.MoveItem(existingPlaylist.InternalId, wanted, targetIndex);

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
                        if (mapping != null)
                        {
                            ApplyPlaylistLook(tagConfig, plName, plUser.Name, mapping, lookItems);
                            if (existingPlaylist == null) createdPlaylists.Add((plUser.Name, mapping));
                        }
                    }
                    // A new playlist gets a metadata refresh from Emby right after it is made, which
                    // saves its own copy of the item and so drops the look set above. Put it back
                    // once that refresh is done.
                    if (createdPlaylists.Count > 0 && HasPlaylistLook(tagConfig))
                    {
                        await Task.Delay(3000);
                        foreach (var (userName, m) in createdPlaylists)
                            ApplyPlaylistLook(tagConfig, plName0, userName, m, lookItems);
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
            // Also on a single-group run: a deleted group's row whose tag is gone would show every title.
            _deletedGroupRowsRemoved = RemoveDeletedGroupLeftovers(cancellationToken);
            // The tags of the tag rows (see TagAnchor): kept on the anchor so they always exist.
            var anchoredTags = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            // A source's row is never looked up among the top-lists' rows (see the title fallback below).
            var topListLibraries = TopListSyncTask.TopListLibraryIds(config, _libraryManager);

            foreach (var tc in config.Tags)
            {
                // When running single-entry, only process the specific tag
                if (filterTagName != null && !string.Equals(tc.Tag?.Trim(), filterTagName, StringComparison.OrdinalIgnoreCase))
                    continue;

                bool isActive = tc.Active && IsScheduleActive(tc.ActiveIntervals);
                if (NormalizeNextWatch(tc)) configChanged = true;
                _log.Step($"Home sections — {(string.IsNullOrWhiteSpace(tc.Name) ? tc.Tag : tc.Name)}");

                if (tc.HomeSectionTracked == null)
                    tc.HomeSectionTracked = new List<HomeSectionTracking>();

                var sectionMarker = SectionMarker(tc);

                string _hsTagName = (tc.Tag ?? "").Trim();
                string _hsDisplayName = !string.IsNullOrWhiteSpace(tc.Name) ? $"{tc.Name} [{_hsTagName}]" : _hsTagName;

                // Deduplicate flat entries that share the same group (Name + Tag).
                // Must happen before the inactive check so that duplicate flat entries
                // for an inactive group don't each log a separate "home section removed".
                var hsKey = GroupKey(tc);
                bool isFirstForGroup = processedHsKeys.Add(hsKey);

                // A tag row exists also outside its schedule (and before the schedule has ever
                // started): the tag is off its titles, so the row is empty and Emby hides it, but it
                // can be moved in the home screen settings. Its title gets InactivePrefix, and every
                // sync puts its settings back (e.g. the tag Emby's own editor may have dropped).
                // Not when another group in schedule writes the same tag: the row would show that
                // group's titles, so it is removed like a collection row (and comes back in place).
                bool plainRow = SourceTopLists.PlainRow(tc);
                bool scheduledOffTagRow = plainRow && tc.Active && !isActive && IsTagRow(tc, _jsonSerializer)
                    && !TagUsedElsewhereInSchedule(config, tc);
                if (scheduledOffTagRow && isFirstForGroup) anchoredTags.Add(_hsTagName);
                else if (plainRow && isActive && isFirstForGroup && IsTagRow(tc, _jsonSerializer)) anchoredTags.Add(_hsTagName);

                if (!plainRow || (!isActive && !scheduledOffTagRow))
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
                                    // Switched off by its schedule (or made inactive): remember where the row
                                    // was, so it comes back at the same place when it switches on again.
                                    if (plainRow) RememberRowPosition(hsKey, tracking.UserId, uid, tracking.SectionId, cancellationToken);
                                    if (DeleteSectionForUser(uid, tracking.UserId, tracking.SectionId, sectionMarker, tc.HomeSectionSettings, cancellationToken, DefaultRowTitle(tc))) _removedHs++;
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
                                _log.Skip($"{_hsDisplayName}: home section removed for {RunLog.Plural(_removedHs, "user")} (group is {(plainRow ? "inactive or not in schedule" : SourceTopLists.IsOn(tc) ? "now shown as its top-list" : "no longer set to show a home section")})");
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

                // Deleted or switched off on the settings page while this run was busy (it works on
                // the settings it started with): no row is made or updated for it. The next run
                // removes what is left, from the saved settings.
                if (!StillWanted(tc))
                {
                    _log.Skip($"{_hsDisplayName}: home section not synced — the group was deleted or switched off while this run was busy");
                    continue;
                }

                Dictionary<string, string> settingsDict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                try
                {
                    if (!string.IsNullOrEmpty(tc.HomeSectionSettings) && tc.HomeSectionSettings != "{}")
                        settingsDict = _jsonSerializer.DeserializeFromString<Dictionary<string, string>>(tc.HomeSectionSettings) ?? settingsDict;
                }
                catch (Exception swallowed) { /* ignore malformed settings */ PluginLog.Swallowed(swallowed); }

                if (!settingsDict.ContainsKey("SectionType"))
                    settingsDict["SectionType"] = DefaultSectionType(tc);

                FillDefaultCustomName(settingsDict, tc);
                if (scheduledOffTagRow) MarkInactive(settingsDict);
                NormalizePlaylistSectionSettings(settingsDict);

                settingsDict.TryGetValue("SectionType", out var sectionType);

                // For items-type sections, dynamically ensure all top-list libraries are excluded
                if (sectionType == "items")
                {
                    var topListLibIds = (config.TopLists ?? new System.Collections.Generic.List<TopListHomeSection>())
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

                string? resolvedLibraryId = null;
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
                    // The anchor keeps the tag (and its id) also when no title has it.
                    if (tc.EnableTag) TagAnchor.Ensure(_libraryManager, _collectionManager, _jsonSerializer, _hsTagName, m => _log.Warn(m));
                    var tagItem = _libraryManager.GetItemList(new InternalItemsQuery
                    {
                        IncludeItemTypes = new[] { "Tag" },
                        Name = _hsTagName,
                        Recursive = true
                    }).FirstOrDefault();
                    // A tag row without its tag would show every title: no row until the tag exists.
                    if (tagItem == null && tc.EnableTag)
                    {
                        HsWarn(statsList, _hsTagName, _hsDisplayName, $"tag '{_hsTagName}' does not exist in the library yet, so the home section was not created or updated");
                        continue;
                    }
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
                        DeleteSectionForUser(uid, t.UserId, t.SectionId, sectionMarker, tc.HomeSectionSettings, cancellationToken, DefaultRowTitle(tc));
                    }
                    catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
                    tc.HomeSectionTracked.Remove(t);
                    configChanged = true;
                }

                int _hsSynced = 0;
                // Position changed since it was last put into effect: the rows already there move too.
                var _posNow = settingsDict.TryGetValue("_position", out var _posSaved) ? _posSaved ?? "" : "";
                var _posKey = RowPlacement.SourceKey(tc);
                bool _posMove = RowPlacement.ShouldMove(_posKey, _posNow, false);
                int _posMoved = 0;
                if (tc.HomeSectionUserIds.Count > 0)
                    _log.Section($"Home section: {_hsDisplayName}");
                foreach (var userId in tc.HomeSectionUserIds)
                {
                    // Disabled in Emby: their row (and its tracking) stays as it is.
                    if (SkipDisabled(userId)) { _log.Debug($"  {UserName(userId)}: skipped — disabled in Emby (the row is kept)"); continue; }
                    string _hsAction = "created";
                    // A playlist row shows this user's own playlist from the source.
                    string? userLibraryId = resolvedLibraryId;
                    if (sectionType == "playlist")
                    {
                        userLibraryId = PlaylistInternalIdFor(tc, userId);
                        if (userLibraryId == null)
                        {
                            string _nwName = Guid.TryParse(userId, out var _nwG) ? (_userManager.GetUserById(_nwG)?.Name ?? userId) : userId;
                            HsWarn(statsList, _hsTagName, _hsDisplayName, $"no home row for {_nwName}: they have no playlist from this source (tick them on the Playlist tab; or it was not made yet — see the playlist lines above)");
                            continue;
                        }
                    }
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
                        // The title fallback never takes a row another group or top-list owns: two sources
                        // with the same Custom Title for one user would otherwise share (and keep
                        // overwriting) one row. Nor any row on a top-list's library, tracked or not: it
                        // would turn the top-list's row into this source's row.
                        if (ownedSection == null && settingsDict.TryGetValue("CustomName", out var _hsFallbackName) && !string.IsNullOrEmpty(_hsFallbackName))
                            ownedSection = allCurrentSections.FirstOrDefault(s => SameTitle(s.CustomName, _hsFallbackName)
                                && !SectionOwnedElsewhere(config, hsKey, userId, s.Id)
                                && !TopListSyncTask.IsTopListRow(s, topListLibraries));

                        if (ownedSection != null)
                        {
                            try
                            {
                                // Hämta befintlig sektion som bas — plugin-inställningar appliceras ovanpå utan att nollställa Emby-egna värden
                                var updateSection = BuildContentSection(_jsonSerializer, settingsDict, userLibraryId, ownedSection);
                                typeof(ContentSection).GetProperty("Id")?.SetValue(updateSection, ownedSection.Id);
                                _userManager.UpdateHomeSection(userInternalId, updateSection, cancellationToken);
                                trackId = ownedSection.Id ?? sectionMarker;
                                _hsAction = "updated";
                                if (_posMove && MoveToPosition(_userManager, userInternalId, ownedSection.Id ?? "", _posNow, cancellationToken,
                                        tc.HomeSectionUserIds.Where(u => u != userId), settingsDict.TryGetValue("CustomName", out var _movedName) ? _movedName : null))
                                {
                                    _posMoved++;
                                    _hsAction = "updated, moved to its new position";
                                }
                                goto _hsSectionDone;
                            }
                            catch (Exception ex)
                            {
                                // The row is there: never add a second one. It is tried again on the next sync.
                                string _hsUserName0 = Guid.TryParse(userId, out var _hsGuid0) ? (_userManager.GetUserById(_hsGuid0)?.Name ?? userId) : userId;
                                HsWarn(statsList, _hsTagName, _hsDisplayName, $"home section could not be updated for {_hsUserName0}: {ex.Message}");
                                if (tracked != null) tracked.SectionId = ownedSection.Id ?? sectionMarker;
                                else tc.HomeSectionTracked.Add(new HomeSectionTracking { UserId = userId, SectionId = ownedSection.Id ?? sectionMarker });
                                configChanged = true;
                                continue;
                            }
                        }

                        // Sektion finns inte — skapa ny
                        {
                            var beforeIds = new HashSet<string>(
                                allCurrentSections.Where(s => !string.IsNullOrEmpty(s.Id)).Select(s => s.Id));
                            _userManager.AddHomeSection(userInternalId, BuildContentSection(_jsonSerializer, settingsDict, userLibraryId), cancellationToken);
                            var afterSections = _userManager.GetHomeSections(userInternalId, cancellationToken);
                            var newId = (afterSections?.Sections ?? Array.Empty<ContentSection>())
                                .Where(s => !string.IsNullOrEmpty(s.Id) && !beforeIds.Contains(s.Id))
                                .Select(s => s.Id).FirstOrDefault() ?? "";
                            trackId = !string.IsNullOrEmpty(newId) ? newId : sectionMarker;
                            // Back where it was when it was removed, else at the row's chosen position.
                            if (!string.IsNullOrEmpty(newId) && !RestoreRowPosition(hsKey, userId, userInternalId, newId, cancellationToken))
                                PlaceNewRow(_userManager, userInternalId, newId, settingsDict.TryGetValue("_position", out var _pos) ? _pos : "", cancellationToken,
                                    tc.HomeSectionUserIds.Where(u => u != userId), settingsDict.TryGetValue("CustomName", out var _ownName) ? _ownName : null);
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
                if (_posMove)
                {
                    RowPlacement.MarkApplied(_posKey, _posNow);
                    if (_posMoved > 0) _log.Ok($"{_hsDisplayName}: row moved to its new position ({PositionLabel(_posNow)}) for {RunLog.Plural(_posMoved, "user")}");
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

            // A full run knows every tag row: tags the anchor holds for rows that are gone come off it.
            if (filterTagName == null)
                TagAnchor.Prune(_libraryManager, _jsonSerializer, anchoredTags, m => _log.Warn(m));

            if (configChanged)
                Plugin.Instance.SaveConfiguration();
        }

        // Is the group still in the saved settings, switched on? A run works on the settings object it
        // started with; a Save on the settings page meanwhile replaces Plugin.Instance.Configuration.
        private static bool StillWanted(TagConfig tc)
        {
            var live = Plugin.Instance?.Configuration;
            if (live?.Tags == null || live.Tags.Contains(tc)) return true;
            var key = GroupKey(tc);
            return live.Tags.Any(t => t.Active && string.Equals(GroupKey(t), key, StringComparison.OrdinalIgnoreCase));
        }

        internal static bool SectionOwnedElsewhere(PluginConfiguration config, TagConfig tc, string userId, string? sectionId)
            => SectionOwnedElsewhere(config, GroupKey(tc), userId, sectionId);

        // A user's home row that another group (not groupKey) or a top-list tracks as its own.
        private static bool SectionOwnedElsewhere(PluginConfiguration config, string groupKey, string userId, string? sectionId)
        {
            if (string.IsNullOrEmpty(sectionId)) return false;
            bool Tracks(List<HomeSectionTracking>? tracked) =>
                tracked?.Any(t => t.SectionId == sectionId && string.Equals(t.UserId, userId, StringComparison.OrdinalIgnoreCase)) == true;
            return (config.Tags ?? new List<TagConfig>()).Any(o => !string.Equals(GroupKey(o), groupKey, StringComparison.OrdinalIgnoreCase) && Tracks(o.HomeSectionTracked))
                || (config.TopLists ?? new List<TopListHomeSection>()).Any(tl => Tracks(tl.HomeSectionTracked));
        }

        // Where a home row was when it was removed (its group made inactive, or a collection /
        // playlist row outside its schedule), per group and user, so it comes back at that place
        // instead of at the bottom: right below the row that was above it (its section id), else
        // at the same index. Plugin data files; the index file predates the anchor file.
        private static readonly object _rowPositionsLock = new object();
        private static Dictionary<string, int>? _rowPositions;
        private static Dictionary<string, string>? _rowAnchors;
        private static string RowPositionsFile => Path.Combine(Plugin.Instance!.DataFolderPath, "home_row_positions.json");
        private static string RowAnchorsFile => Path.Combine(Plugin.Instance!.DataFolderPath, "home_row_anchors.json");
        private const string TopAnchor = "^";

        private static Dictionary<string, int> RowPositions(IJsonSerializer json)
        {
            if (_rowPositions != null) return _rowPositions;
            try
            {
                if (File.Exists(RowPositionsFile))
                    _rowPositions = json.DeserializeFromFile<Dictionary<string, int>>(RowPositionsFile);
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            return _rowPositions ??= new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        }

        private static Dictionary<string, string> RowAnchors(IJsonSerializer json)
        {
            if (_rowAnchors != null) return _rowAnchors;
            try
            {
                if (File.Exists(RowAnchorsFile))
                    _rowAnchors = json.DeserializeFromFile<Dictionary<string, string>>(RowAnchorsFile);
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            return _rowAnchors ??= new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        }

        private static void SaveRowPositions(IJsonSerializer json)
        {
            try { json.SerializeToFile(RowPositions(json), RowPositionsFile); } catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            try { json.SerializeToFile(RowAnchors(json), RowAnchorsFile); } catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
        }

        private void RememberRowPosition(string groupKey, string userId, long userInternalId, string sectionId, CancellationToken cancellationToken)
            => RememberRowPosition(_userManager, _jsonSerializer, groupKey, userId, userInternalId, sectionId, cancellationToken);

        private bool RestoreRowPosition(string groupKey, string userId, long userInternalId, string sectionId, CancellationToken cancellationToken)
            => RestoreRowPosition(_userManager, _jsonSerializer, groupKey, userId, userInternalId, sectionId, cancellationToken);

        internal static void RememberRowPosition(IUserManager userManager, IJsonSerializer json, string groupKey, string userId, long userInternalId, string sectionId, CancellationToken cancellationToken)
        {
            try
            {
                if (string.IsNullOrEmpty(sectionId) || sectionId.StartsWith("hsc__")) return;
                var sections = userManager.GetHomeSections(userInternalId, cancellationToken)?.Sections ?? Array.Empty<ContentSection>();
                int index = Array.FindIndex(sections, s => s.Id == sectionId);
                if (index < 0) return;
                var key = groupKey + "\u001F" + userId;
                lock (_rowPositionsLock)
                {
                    RowPositions(json)[key] = index;
                    RowAnchors(json)[key] = index == 0 ? TopAnchor : sections[index - 1].Id ?? "";
                    SaveRowPositions(json);
                }
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
        }

        /// <summary>Puts a re-created row back where it was; false when no place was remembered.</summary>
        internal static bool RestoreRowPosition(IUserManager userManager, IJsonSerializer json, string groupKey, string userId, long userInternalId, string sectionId, CancellationToken cancellationToken)
        {
            var key = groupKey + "\u001F" + userId;
            int index;
            string? anchor;
            lock (_rowPositionsLock)
            {
                if (!RowPositions(json).TryGetValue(key, out index)) return false;
                RowAnchors(json).TryGetValue(key, out anchor);
            }
            try
            {
                var sections = userManager.GetHomeSections(userInternalId, cancellationToken)?.Sections ?? Array.Empty<ContentSection>();
                var others = sections.Where(s => s.Id != sectionId).ToList();
                // Below the row that was above it, when that row is still there; else the old index.
                int target = anchor == TopAnchor ? 0 : index;
                if (!string.IsNullOrEmpty(anchor) && anchor != TopAnchor)
                {
                    int above = others.FindIndex(s => s.Id == anchor);
                    if (above >= 0) target = above + 1;
                }
                MoveRow(userManager, userInternalId, sectionId, target, sections.Length, cancellationToken);
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            lock (_rowPositionsLock)
            {
                RowPositions(json).Remove(key);
                RowAnchors(json).Remove(key);
                SaveRowPositions(json);
            }
            return true;
        }

        // A home row's identity across users (section ids differ per user): its custom title, else
        // its type and library. The settings page offers "After <row>" with the same key.
        // An inactive row keeps its key: the title counts without InactivePrefix.
        internal static string SectionKey(ContentSection s)
            => !string.IsNullOrWhiteSpace(s.CustomName) ? "name:" + StripInactive(s.CustomName).Trim().ToLowerInvariant()
                : "type:" + (s.SectionType ?? "").ToLowerInvariant() + "|" + (s.ParentId ?? "");

        // Where a new row goes (HomeSectionSettings "_position"): "" or "bottom" = where Emby puts it
        // (the bottom), "top", "before:<SectionKey>" = right above that row, or "after:<SectionKey>" =
        // right below it. When this user's home screen does not have that row: next to the nearest
        // row around it (in the order of another of the users who has it) that this user does have —
        // below the nearest one above it for "after" (else at the top), above the nearest one below it
        // for "before" (else at the bottom); at the bottom when none of them has the row.
        internal static void PlaceNewRow(IUserManager userManager, long userInternalId, string sectionId, string position, CancellationToken cancellationToken,
            IEnumerable<string>? referenceUserIds = null, string? ownTitle = null)
        {
            if (string.IsNullOrEmpty(position) || position == "bottom") return;
            try
            {
                var sections = userManager.GetHomeSections(userInternalId, cancellationToken)?.Sections ?? Array.Empty<ContentSection>();
                var target = TargetIndex(userManager, sections, sectionId, position, referenceUserIds, ownTitle, cancellationToken);
                if (target.HasValue) MoveRow(userManager, userInternalId, sectionId, target.Value, sections.Length, cancellationToken);
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
        }

        /// <summary>Moves a row that is already there to the position (the setting was changed): also
        /// to the bottom for "". A row whose anchor nobody has stays where it is. True when it moved.</summary>
        internal static bool MoveToPosition(IUserManager userManager, long userInternalId, string sectionId, string position, CancellationToken cancellationToken,
            IEnumerable<string>? referenceUserIds = null, string? ownTitle = null)
        {
            try
            {
                var sections = userManager.GetHomeSections(userInternalId, cancellationToken)?.Sections ?? Array.Empty<ContentSection>();
                int current = Array.FindIndex(sections, s => s.Id == sectionId);
                if (current < 0) return false;
                int? target = string.IsNullOrEmpty(position) || position == "bottom" ? sections.Length - 1
                    : TargetIndex(userManager, sections, sectionId, position, referenceUserIds, ownTitle, cancellationToken);
                if (!target.HasValue || target.Value == current) return false;
                userManager.MoveHomeSections(userInternalId, new[] { sectionId }, target.Value, cancellationToken);
                return true;
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); return false; }
        }

        // A Position for the log: "bottom", "top", "after 'Continue Watching'".
        internal static string PositionLabel(string? position)
        {
            if (string.IsNullOrEmpty(position) || position == "bottom") return "bottom";
            if (position == "top") return "top";
            bool before = position!.StartsWith("before:", StringComparison.Ordinal);
            if (!before && !position.StartsWith("after:", StringComparison.Ordinal)) return position;
            var key = position.Substring(before ? "before:".Length : "after:".Length);
            var row = key.StartsWith("name:", StringComparison.Ordinal) ? "'" + key.Substring(5) + "'"
                : key.StartsWith("type:", StringComparison.Ordinal) ? key.Substring(5).Split('|')[0] + " row" : key;
            return (before ? "before " : "after ") + row;
        }

        // The index the row gets among the other rows (its final index); null: leave it where it is.
        private static int? TargetIndex(IUserManager userManager, ContentSection[] sections, string sectionId, string position,
            IEnumerable<string>? referenceUserIds, string? ownTitle, CancellationToken cancellationToken)
        {
            if (position == "top") return 0;
            bool before = position.StartsWith("before:", StringComparison.Ordinal);
            if (!before && !position.StartsWith("after:", StringComparison.Ordinal)) return null;
            var anchor = position.Substring(before ? "before:".Length : "after:".Length);
            var others = sections.Where(s => s.Id != sectionId).ToList();
            int at = others.FindIndex(s => SectionKey(s) == anchor);
            if (at >= 0) return before ? at : at + 1;
            var near = RowKeysNear(userManager, anchor, before, referenceUserIds, ownTitle, cancellationToken);
            if (near == null) return null;
            foreach (var key in near)
            {
                int found = others.FindIndex(s => SectionKey(s) == key);
                if (found >= 0) return before ? found : found + 1;
            }
            // None of the rows around it: "after" goes to the top, "before" to the bottom.
            return before ? (int?)null : 0;
        }

        // The keys of the rows next to the anchor row, nearest first — those above it for "after",
        // those below it for "before" — on the home screen of the first reference user who has that
        // row (the row being placed left out); null when none has it.
        private static List<string>? RowKeysNear(IUserManager userManager, string anchor, bool below, IEnumerable<string>? referenceUserIds, string? ownTitle, CancellationToken cancellationToken)
        {
            if (referenceUserIds == null) return null;
            string? ownKey = string.IsNullOrWhiteSpace(ownTitle) ? null : "name:" + StripInactive(ownTitle).Trim().ToLowerInvariant();
            foreach (var userId in referenceUserIds)
            {
                try
                {
                    var keys = (userManager.GetHomeSections(userManager.GetInternalId(userId), cancellationToken)?.Sections ?? Array.Empty<ContentSection>())
                        .Select(SectionKey).Where(k => k != ownKey).ToList();
                    int at = keys.IndexOf(anchor);
                    if (at < 0) continue;
                    if (below) return keys.Skip(at + 1).ToList();
                    var above = keys.Take(at).ToList();
                    above.Reverse();
                    return above;
                }
                catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            }
            return null;
        }

        private static void MoveRow(IUserManager userManager, long userInternalId, string sectionId, int target, int count, CancellationToken cancellationToken)
        {
            // A new row is already last: nothing to move when the target is the end.
            if (count > 0 && target < count - 1)
                userManager.MoveHomeSections(userInternalId, new[] { sectionId }, target, cancellationToken);
        }

        // The row type a source's home section uses when none is saved.
        internal static string DefaultSectionType(TagConfig tc)
            => (tc.EnableCollection && !string.IsNullOrEmpty(tc.CollectionName)) ? "boxset"
                : !tc.EnableTag && tc.EnablePlaylist ? "playlist" : "items";

        // A home row that shows the source's tag (Dynamic Media), not its collection or playlist.
        internal static bool IsTagRow(TagConfig tc, IJsonSerializer json)
        {
            var st = "";
            try
            {
                var s = json.DeserializeFromString<Dictionary<string, string>>(string.IsNullOrEmpty(tc.HomeSectionSettings) ? "{}" : tc.HomeSectionSettings);
                if (s != null) s.TryGetValue("SectionType", out st);
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            if (string.IsNullOrEmpty(st)) st = DefaultSectionType(tc);
            return st == "items" && tc.EnableTag && !string.IsNullOrWhiteSpace(tc.Tag);
        }

        // The title of a tag row outside its schedule starts with this, so it stands out in the home
        // screen settings (the row itself is empty and hidden).
        internal const string InactivePrefix = "[INACTIVE] ";

        internal static string StripInactive(string? title)
            => title != null && title.StartsWith(InactivePrefix, StringComparison.OrdinalIgnoreCase) ? title.Substring(InactivePrefix.Length) : title ?? "";

        // The title a group's row is written with: the pre-filled one when empty, with InactivePrefix
        // for a tag row outside its schedule (as the sync sets it).
        internal static void ApplyRowTitle(Dictionary<string, string> settings, TagConfig tc, IJsonSerializer json)
        {
            FillDefaultCustomName(settings, tc);
            if (tc.Active && !IsScheduleActive(tc.ActiveIntervals, DateTime.Now) && IsTagRow(tc, json))
                MarkInactive(settings);
        }

        internal static void MarkInactive(Dictionary<string, string> settings)
        {
            if (settings.TryGetValue("CustomName", out var cn) && !string.IsNullOrWhiteSpace(cn))
                settings["CustomName"] = InactivePrefix + StripInactive(cn);
        }

        // Same row title, with or without InactivePrefix.
        internal static bool SameTitle(string? a, string? b)
            => !string.IsNullOrEmpty(a) && !string.IsNullOrEmpty(b)
               && string.Equals(StripInactive(a).Trim(), StripInactive(b).Trim(), StringComparison.OrdinalIgnoreCase);

        // Removes a group's row for one user: by its tracked id, else (the id is gone, e.g. Copy & Sync
        // re-created the user's rows with new ids) by its marker, else by its title. True when a row
        // was removed. Never a top-list's row: not one a top-list tracks, and by marker or title never
        // a row on a top-list's library either — a source and a top-list can have the same title.
        private bool DeleteSectionForUser(long userInternalId, string userId, string sectionId, string sectionMarker, string settingsJson, CancellationToken cancellationToken, string? defaultTitle = null)
        {
            ContentSection[]? allSections = null;
            try { allSections = _userManager.GetHomeSections(userInternalId, cancellationToken)?.Sections ?? Array.Empty<ContentSection>(); }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }

            var cfg = Plugin.Instance?.Configuration;
            var topListTracked = new HashSet<string>(
                (cfg?.TopLists ?? new List<TopListHomeSection>()).SelectMany(t => t.HomeSectionTracked ?? new List<HomeSectionTracking>())
                    .Select(t => t.SectionId).Where(id => !string.IsNullOrEmpty(id)));

            bool realId = !string.IsNullOrEmpty(sectionId) && !sectionId.StartsWith("hsc__");
            if (realId && (allSections == null || allSections.Any(s => s.Id == sectionId)))
            {
                if (topListTracked.Contains(sectionId))
                {
                    _log.Debug($"  {UserName(userId)}: row {sectionId} kept — it is a top-list's row");
                    return false;
                }
                _userManager.DeleteHomeSections(userInternalId, new[] { sectionId }, cancellationToken);
                return true;
            }
            if (allSections == null) return false;

            var topListLibs = TopListSyncTask.TopListLibraryIds(cfg, _libraryManager);
            bool IsTopListRow(ContentSection s) => topListTracked.Contains(s.Id ?? "") || TopListSyncTask.IsTopListRow(s, topListLibs);

            var marker = (!string.IsNullOrEmpty(sectionId) && sectionId.StartsWith("hsc__")) ? sectionId : sectionMarker;
            if (!string.IsNullOrEmpty(marker))
            {
                var markerIds = allSections
                    .Where(s => s.Subtitle == marker && !string.IsNullOrEmpty(s.Id) && !IsTopListRow(s))
                    .Select(s => s.Id).ToArray();
                if (markerIds.Length > 0)
                {
                    _userManager.DeleteHomeSections(userInternalId, markerIds, cancellationToken);
                    _log.Debug($"  {UserName(userId)}: {RunLog.Plural(markerIds.Length, "row")} removed by marker '{marker}' (the saved id {(realId ? sectionId : "-")} was gone)");
                    return true;
                }
            }

            try
            {
                var hint = _jsonSerializer.DeserializeFromString<Dictionary<string, string>>(settingsJson ?? "{}");
                var cn = hint != null && hint.TryGetValue("CustomName", out var savedName) ? savedName : "";
                // An empty saved title: the row was written with the default one (FillDefaultCustomName).
                if (string.IsNullOrWhiteSpace(cn)) cn = defaultTitle ?? "";
                if (!string.IsNullOrEmpty(cn))
                {
                    // Never a row another group or top-list tracks (same title, different source).
                    var trackedIds = new HashSet<string>(
                        (cfg?.Tags ?? new List<TagConfig>()).SelectMany(t => t.HomeSectionTracked ?? new List<HomeSectionTracking>())
                            .Select(t => t.SectionId).Where(id => !string.IsNullOrEmpty(id)));
                    var sameTitle = allSections.Where(s => SameTitle(s.CustomName, cn) && !string.IsNullOrEmpty(s.Id)).ToList();
                    foreach (var s in sameTitle.Where(IsTopListRow))
                        _log.Debug($"  {UserName(userId)}: '{s.CustomName}' ({s.Id}) kept — a top-list's row with the same title");
                    var fallbackIds = sameTitle
                        .Where(s => !trackedIds.Contains(s.Id) && !IsTopListRow(s))
                        .Select(s => s.Id).ToArray();
                    if (fallbackIds.Length > 0)
                    {
                        _userManager.DeleteHomeSections(userInternalId, fallbackIds, cancellationToken);
                        _log.Debug($"  {UserName(userId)}: {RunLog.Plural(fallbackIds.Length, "row")} '{cn}' removed by title (the saved id {(realId ? sectionId : "-")} was gone)");
                        return true;
                    }
                }
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            return false;
        }

        private string UserName(string userId)
            => Guid.TryParse(userId, out var g) ? (_userManager.GetUserById(g)?.Name ?? userId) : userId;

        // Returns true when this entry is a LocalCollection home section with media type = Collections.
        // Used to decide that only the BoxSet itself (not its items) should receive the tag.
        // Tags this plugin writes onto items (some entry of the group has tagging on). A group's
        // Tag: criteria must not see its own output tag, or an item that got it once keeps
        // matching its own rule forever (e.g. tag "zzrow-musicals" + Tag:contains:Musicals).
        private HashSet<string> _writtenTags = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        // Users disabled in Emby, read at the start of each run: left out of everything done per user
        // (see UserStatus). Null outside a run.
        private UserStatus? _userStatus;
        private bool SkipDisabled(string userId) => _userStatus?.Skip(userId) == true;

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
                if (NormalizeNextWatch(tc)) configChanged = true;
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
                                try { DeletePlaylist(pl); }
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
                                try { DeletePlaylist(pl); }
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
                // Outside its schedule the collection loses the tag, like titles do.
                if (tc.Active && IsScheduleActive(tc.ActiveIntervals) && !string.IsNullOrEmpty(tc.LocalSourceId))
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
                if (TagAnchor.IsAnchor(boxSet, _jsonSerializer)) continue;   // holds the tag rows' tags
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
                        catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }

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
                        catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
                        userManager.UpdateHomeSection(uid, sec, cancellationToken);
                        updated++;
                    }
                }
                catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            }
            return updated;
        }

        internal static ContentSection BuildContentSection(IJsonSerializer jsonSerializer, Dictionary<string, string> settings, string? libraryId, ContentSection? existing = null)
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
                    object? converted = null;
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
                catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
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
                                catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
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
                        catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
                    }

                    if (queryProp.CanWrite)
                        queryProp.SetValue(section, extQuery);
                }
                catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
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
                else if (interval.Type == "TimeOfDay") match = TimeOfDayMatches(interval, now);
                else if (interval.Type == "MovingDate" || interval.Type == "Easter") match = MovingDateMatches(interval, now);
                else if (interval.Type == "Advent") match = AdventMatches(interval, now);
                else if (interval.Type == "Friday13") match = Friday13Matches(interval, now);
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

        // A "Manual" source: the hand-picked movies and shows, in list order. Items that were
        // deleted from the library are skipped with a warning.
        private List<BaseItem> ManualItems(TagConfig tagConfig, HashSet<string> blacklist, int limit, GroupRunStats gs, Func<BaseItem, bool>? passesFilters = null)
        {
            var result = new List<BaseItem>();
            int gone = 0;
            foreach (var id in tagConfig.ManualItemIds ?? new List<string>())
            {
                BaseItem? item = null;
                if (long.TryParse(id, out var internalId)) item = _libraryManager.GetItemById(internalId);
                else if (Guid.TryParse(id, out var guid)) item = _libraryManager.GetItemById(guid);
                if (item == null || !IsTaggableTopLevelItem(item)) { gone++; continue; }
                var imdb = item.GetProviderId("Imdb");
                if (!string.IsNullOrEmpty(imdb) && blacklist.Contains(imdb)) continue;
                if (passesFilters != null && !passesFilters(item)) continue;
                if (!result.Contains(item)) result.Add(item);
                if (result.Count >= limit) break;
            }
            if (gone > 0) gs.Warnings.Add($"{gone} hand-picked item(s) are no longer in the library");
            return result;
        }

        // External and AI sources: their titles go into the real-time tag cache (keyed by IMDb id).
        internal static bool IsListSource(TagConfig t) =>
            string.IsNullOrEmpty(t.SourceType) || t.SourceType == "External" || t.SourceType == "AI";

        // A source with filter conditions must not cache every list entry: an entry that failed a
        // filter would get the tag back from the real-time tagger the moment the sync takes it off
        // (the sync's own UpdateItem raises ItemUpdated). Such a source caches only the titles that
        // passed its filters.
        internal static bool HasFilterConditions(TagConfig t) =>
            t.MediaInfoFilters?.Any(f => f?.Criteria?.Count > 0) == true || t.MediaInfoConditions?.Count > 0;

        // A URL that came back empty and was replaced by its last good entries (LastGoodLists).
        private static string StandInMessage(string url, string? error, DateTime savedUtc) =>
            $"{(url ?? "").Trim()}: the list returned 0 items{Why(error)} — its titles from the last good read ({savedUtc.ToLocalTime():yyyy-MM-dd}) were used instead (\"Preserve tags and collections on empty result\" is on). Check the list URL and the API key in Settings.";

        // " (HTTP 403 Forbidden)" — the reason a source came back empty, for its warning.
        private static string Why(string? error) => string.IsNullOrEmpty(error) ? "" : $" ({error})";
        private static string Why(List<string> sources) => sources.Count == 0 ? "" : $" ({string.Join(", ", sources)})";

        // A source's Limit is a number of hits, not of list entries: a title counts only when it is
        // usable — in the library, not blacklisted and passing the group's filter conditions — and
        // the source is read on until the Limit is filled. Null when the group has no filter conditions
        // (or is a MediaInfo source, whose conditions are the source itself).
        private Func<BaseItem, bool>? FilterConditionsCheck(TagConfig tagConfig,
            Dictionary<long, BaseItem> seriesEpisodeCache,
            Dictionary<string, HashSet<long>> personCache,
            Dictionary<(Guid, long), (bool Played, DateTimeOffset? LastPlayedDate, int PlayCount)> userDataCache,
            Dictionary<long, CachedMediaInfo> mediaInfoCache,
            User[]? preloadedUsers,
            Dictionary<(Guid, long), DateTimeOffset?> seriesLastPlayedCache,
            Dictionary<string, HashSet<long>> collectionMembershipCache,
            Dictionary<long, List<string>>? seriesEpisodeNamesCache)
        {
            if (tagConfig.SourceType == "MediaInfo" || IsNextWatch(tagConfig)) return null;
            if (!(tagConfig.MediaInfoFilters?.Count > 0 || tagConfig.MediaInfoConditions?.Count > 0)) return null;
            return item => ItemMatchesMediaInfo(item, tagConfig, false, seriesEpisodeCache, personCache, userDataCache,
                mediaInfoCache.TryGetValue(item.InternalId, out var ci) ? ci : (CachedMediaInfo?)null,
                preloadedUsers, seriesLastPlayedCache, collectionMembershipCache, seriesEpisodeNamesCache);
        }

        // An external list entry counts toward the Limit when a usable copy of it is in the library.
        private static Func<ExternalItemDto, bool> CountsTowardLimit(Dictionary<string, List<BaseItem>> imdbLookup,
            HashSet<string> blacklist, Func<BaseItem, bool>? passesFilters)
            => e => !string.IsNullOrEmpty(e.Imdb) && !blacklist.Contains(e.Imdb)
                    && imdbLookup.TryGetValue(e.Imdb, out var local) && (passesFilters == null || local.Any(passesFilters));

        // How many titles an AI source is asked for: more than its Limit, since some are not in the
        // library; the first hits fill the Limit.
        private static int AiAskCount(int limit) => limit >= 10000 ? limit : Math.Min(limit * 2, limit + 50);

        // A "MovingDate" window: the anchor date (e.g. last Monday of May, or Easter Sunday) of last
        // year, this year and next year, each widened by DaysBefore/DaysAfter (0–60), so a window
        // that crosses New Year works. Inclusive on both ends, whole days (server time).
        private static bool MovingDateMatches(DateInterval interval, DateTime now)
        {
            int before = Math.Max(0, Math.Min(60, interval.DaysBefore));
            int after = Math.Max(0, Math.Min(60, interval.DaysAfter));
            var today = now.Date;
            for (int y = now.Year - 1; y <= now.Year + 1; y++)
            {
                var anchor = MovingDateAnchor(interval, y);
                if (anchor == null) return false;
                if (today >= anchor.Value.AddDays(-before) && today <= anchor.Value.AddDays(after)) return true;
            }
            return false;
        }

        internal static DateTime? MovingDateAnchor(DateInterval interval, int year)
        {
            var occ = (interval.Occurrence ?? "1").Trim().ToLowerInvariant();
            if (interval.Type == "Easter" || occ == "easter") return WesternEaster(year);
            if (interval.Month < 1 || interval.Month > 12) return null;
            if (!Enum.TryParse<DayOfWeek>((interval.DayOfWeek ?? "").Split(',')[0].Trim(), true, out var wd)) return null;
            if (occ == "last")
            {
                var last = new DateTime(year, interval.Month, DateTime.DaysInMonth(year, interval.Month));
                return last.AddDays(-(((int)last.DayOfWeek - (int)wd + 7) % 7));
            }
            if (!int.TryParse(occ, out var n) || n < 1 || n > 4) return null;
            var first = new DateTime(year, interval.Month, 1);
            return first.AddDays(((int)wd - (int)first.DayOfWeek + 7) % 7 + 7 * (n - 1));
        }

        // Western (Gregorian) Easter Sunday: the anonymous Gregorian algorithm (Meeus/Jones/Butcher).
        internal static DateTime WesternEaster(int year)
        {
            int a = year % 19, b = year / 100, c = year % 100, d = b / 4, e = b % 4;
            int f = (b + 8) / 25, g = (b - f + 1) / 3, h = (19 * a + b - d - g + 15) % 30;
            int i = c / 4, k = c % 4, l = (32 + 2 * e + 2 * i - h - k) % 7, m = (a + 11 * h + 22 * l) / 451;
            int month = (h + l - 7 * m + 114) / 31, day = (h + l - 7 * m + 114) % 31 + 1;
            return new DateTime(year, month, day);
        }

        // First Sunday of Advent: the fourth Sunday before Christmas Day (Nov 27 – Dec 3).
        internal static DateTime FirstAdvent(int year)
        {
            var christmas = new DateTime(year, 12, 25);
            int back = (int)christmas.DayOfWeek == 0 ? 7 : (int)christmas.DayOfWeek;
            return christmas.AddDays(-back - 21);
        }

        // An "Advent" window: from the first Sunday of Advent through Christmas Day, widened by
        // DaysBefore/DaysAfter (0–60).
        private static bool AdventMatches(DateInterval interval, DateTime now)
        {
            int before = Math.Max(0, Math.Min(60, interval.DaysBefore));
            int after = Math.Max(0, Math.Min(60, interval.DaysAfter));
            var today = now.Date;
            for (int y = now.Year - 1; y <= now.Year; y++)
                if (today >= FirstAdvent(y).AddDays(-before) && today <= new DateTime(y, 12, 25).AddDays(after)) return true;
            return false;
        }

        // A "Friday13" window: every Friday the 13th, widened by DaysBefore/DaysAfter (0–60).
        private static bool Friday13Matches(DateInterval interval, DateTime now)
        {
            int before = Math.Max(0, Math.Min(60, interval.DaysBefore));
            int after = Math.Max(0, Math.Min(60, interval.DaysAfter));
            var today = now.Date;
            // Today is in the window when a Friday the 13th lies between today - after and today + before.
            for (var d = today.AddDays(-after); d <= today.AddDays(before); d = d.AddDays(1))
                if (d.Day == 13 && d.DayOfWeek == DayOfWeek.Friday) return true;
            return false;
        }

        // A "TimeOfDay" window. One that runs past midnight (22:00 → 02:00) belongs to the day it
        // starts on, so Friday 22:00 → 02:00 is still active at 01:00 on Saturday.
        private static bool TimeOfDayMatches(DateInterval interval, DateTime now)
        {
            if (!TimeSpan.TryParse(interval.FromTime ?? "", out var from) || !TimeSpan.TryParse(interval.ToTime ?? "", out var to)) return false;
            bool DayOk(DayOfWeek d) => string.IsNullOrWhiteSpace(interval.DayOfWeek)
                || interval.DayOfWeek.IndexOf(d.ToString(), StringComparison.OrdinalIgnoreCase) >= 0;
            var t = now.TimeOfDay;
            if (from < to) return DayOk(now.DayOfWeek) && t >= from && t < to;
            if (from == to) return DayOk(now.DayOfWeek);
            return (t >= from && DayOk(now.DayOfWeek)) || (t < to && DayOk(now.AddDays(-1).DayOfWeek));
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
                lpDate = Played(user, ep, lpDate);
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

                System.Collections.IEnumerable? streams = null;
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

            if (!cachedInfo.HasValue && !NeedsMediaDetails(tagConfig))
                cachedInfo = new CachedMediaInfo { AudioLanguages = new HashSet<string>(StringComparer.OrdinalIgnoreCase) };

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
            if (!string.IsNullOrEmpty(ownTag) && _writtenTags.Contains(ownTag!))
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
            // Values may contain ':' (titles such as "Mission: Impossible", Windows paths): unless the
            // rule is a user-data rule or a number comparison, everything after the property (and its
            // contains / exact) is the value.
            bool userDataRule = parts.Length == 4 && UserDataRuleProps.Contains(parts[0]);
            bool numericRule = parts.Length == 3 && double.TryParse(parts[2], System.Globalization.NumberStyles.Any,
                System.Globalization.CultureInfo.InvariantCulture, out _);
            if (parts.Length > 2 && !userDataRule && !numericRule) parts = SplitTextCriterion(c);
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
                if (prop4 == "Popular")
                {
                    int popDays = int.TryParse(userId4, out var pd) && pd > 0 ? pd : 0;
                    return double.TryParse(valStr4, System.Globalization.NumberStyles.Any,
                                           System.Globalization.CultureInfo.InvariantCulture, out var minViewers)
                        && ApplyNumericOp(ViewersOf(item, popDays), op4, minViewers);
                }
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
                            var ud2 = UserDataOf(u, item);
                            if (userDataCache != null) userDataCache[k] = ud2;
                            return ud2.Played == wantWatched;
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
                                else { var ud2 = UserDataOf(u, item); lpDate = ud2.LastPlayedDate;
                                       if (userDataCache != null) userDataCache[k] = ud2; }
                                lpDate = Played(u, item, lpDate);
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
                            else { var ud2 = UserDataOf(u, item); playCount = ud2.PlayCount;
                                   if (userDataCache != null) userDataCache[k] = ud2; }
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
                    udResult = UserDataOf(user4, item);
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
                        : _playHistory != null && _userManager.GetUserById(guid4) is User _u4 ? Played(_u4, item, udResult.LastPlayedDate)
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

        // Rules of the form <prop>:<user or days>:<op>:<value> — none of their fields is free text.
        private static readonly HashSet<string> UserDataRuleProps = new HashSet<string>(StringComparer.Ordinal)
            { "Popular", "IsPlayed", "LastPlayed", "PlayCount" };

        // A text rule as {prop, value} or {prop, contains|exact, value}; the value keeps any ':' in it.
        internal static string[] SplitTextCriterion(string c)
        {
            int i = c.IndexOf(':');
            if (i < 0) return new[] { c };
            var prop = c.Substring(0, i);
            var rest = c.Substring(i + 1);
            int j = rest.IndexOf(':');
            if (j > 0)
            {
                var op = rest.Substring(0, j);
                if (op == "contains" || op == "exact") return new[] { prop, op, rest.Substring(j + 1) };
            }
            return new[] { prop, rest };
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

        // Criteria that read a file's video/audio details (ExtractMediaInfo): the bare flags
        // ("4K", "HEVC", "Atmos", "5.1", …) and these properties. Reading the details costs a
        // lookup per title (and per show), so it is skipped when no criterion needs them.
        private static readonly HashSet<string> MediaDetailProperties = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "AudioLanguage", "DateModified", "FileSize", "BitRate", "SampleRate", "BitsPerSample", "TrackNumber", "DiscNumber"
        };

        private static bool NeedsMediaDetails(TagConfig tagConfig)
            => GetAllCriteria(tagConfig).Any(c =>
            {
                var crit = c.TrimStart('!');
                int colon = crit.IndexOf(':');
                return colon < 0 ? crit.Length > 0 : MediaDetailProperties.Contains(crit.Substring(0, colon));
            });

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
        internal static bool IsTaggableTopLevelItem(BaseItem item)
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
                    var ud = UserDataOf(u, item);
                    played = ud.Played;
                    if (userDataCache != null) userDataCache[k] = ud;
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
                case "Manual":          return "Manual list";
                case "NextWatch":       return "Your Next Watch";
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
                case "Manual":
                    parts.Add($"{tc.ManualItemIds?.Count ?? 0} hand-picked items");
                    break;
                case "NextWatch":
                    parts.Add($"{NextWatchUserIds(tc).Count} user(s), {(tc.Limit > 0 ? tc.Limit : NextWatchRecommender.DefaultLimit)} picks each");
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
            if (SourceTopLists.PlainRow(tc)) features.Add("home section");
            if (SourceTopLists.IsOn(tc)) features.Add("top-list");
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
            if (gs.Errors.Count > 0)
            {
                int more = gs.Errors.Count - 1 + gs.Warnings.Count;
                _log.Error($"{head}  ·  {gs.Errors[0]}{(more > 0 ? $"  (+{more} more, see results)" : "")}");
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
            if (gs.SourceType == "NextWatch") return $"{RunLog.Plural(gs.ListCount, "user")}, {gs.MatchCount} picks";
            if (gs.SourceType == "LocalCollection" || gs.SourceType == "LocalPlaylist" || gs.SourceType == "Manual") return $"{gs.ListCount} in source, {gs.MatchCount} matched";
            return $"{gs.ListCount} in list, {gs.MatchCount} in your library";
        }

        // A group with several URLs has one block per URL; its tag and collection lines are the
        // group's and are shown once (full run only; null = show them).
        private HashSet<string>? _resultsShown;
        private bool FirstShown(string key) => _resultsShown == null || _resultsShown.Add(key);

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
            else if (gs.SourceType == "NextWatch")
                _log.Ok($"Your Next Watch: {RunLog.Plural(gs.ListCount, "user")}  ·  {gs.MatchCount} picks in total");
            else if (gs.SourceType == "LocalCollection" || gs.SourceType == "LocalPlaylist")
            {
                if (gs.ListCount > 0) _log.Ok($"Source: {gs.ListCount} items  ·  {gs.MatchCount} matched");
            }
            else if (gs.ListCount > 0)
                _log.Ok($"List: {gs.ListCount} items  ·  {gs.MatchCount} in your library");
            // A list that came back empty has its own warning / error below.
            if (gs.LimitShort != null && !(isRemote && gs.ListCount == 0)) _log.Warn($"Found {gs.LimitShort} (Max items)");

            if (gs.EnableTag && !gs.BoxSetHse && FirstShown("tag:" + gs.TagName))
            {
                if (dryRun) _log.Skip($"Tag \"{gs.TagName}\": would add {gs.TagsAdded}, remove {gs.TagsRemoved} (dry run)");
                else if (gs.TagsAdded == 0 && gs.TagsRemoved == 0) _log.Ok($"Tag \"{gs.TagName}\": up to date");
                else _log.Ok($"Tag \"{gs.TagName}\": +{gs.TagsAdded} added, -{gs.TagsRemoved} removed");
            }

            if (gs.EnableCollection && FirstShown("coll:" + gs.CollectionName))
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

            foreach (var e in gs.Errors) _log.Error(e);
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
            if (_preview == null && !dryRun) RecordGroupIssues(new[] { gs });

            int failed = gs.ErrorMessage != null || gs.Errors.Count > 0 ? 1 : 0;
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
            if (_userStatus?.SummaryLine() is string disabledLine) _log.Info("  " + disabledLine);
            _log.Info($"  Done in {RunLog.Elapsed(DateTime.Now - startTime)}  ·  {StatusSymbol(failed, warned)} {finalStatus}");
            _log.Rule();
        }

        // Stores each group's warnings / error for the flag on its card (see GroupIssues). A group
        // stored as several flat sources has one stats entry per source; skipped ones are left out.
        private static void RecordGroupIssues(IEnumerable<GroupRunStats> stats, IEnumerable<string>? liveKeys = null)
        {
            var results = stats
                .Where(g => !g.Skipped && g.TagName != null)
                .GroupBy(g => GroupIssues.Key(g.GroupName, g.TagName), StringComparer.OrdinalIgnoreCase)
                .Select(grp =>
                {
                    var first = grp.First();
                    var errors = grp.Where(g => g.ErrorMessage != null).Select(g => g.ErrorMessage!)
                        .Concat(grp.SelectMany(g => g.Errors)).Distinct().ToList();
                    var messages = errors.Concat(grp.SelectMany(g => g.Warnings)).Distinct().ToList();
                    GroupIssue? issue = messages.Count == 0 ? null : new GroupIssue
                    {
                        Level = errors.Count > 0 ? "error" : "warning",
                        Messages = messages,
                        RunUtc = DateTime.UtcNow
                    };
                    return (first.GroupName ?? "", first.TagName!, issue);
                })
                .ToList();
            GroupIssues.Record(results, liveKeys);
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
            if (matchedLocalItems.Count == 0) return;
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
            if (added == null || removed == null) return;
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

                // With "Ignore titles only marked as watched", a title counts only with a real play.
                var playedItems = playedLibraryItems
                    .Select(item => new { item, lp = Played(user, item, _userDataManager?.GetUserData(user, item)?.LastPlayedDate) })
                    .Where(x => _playHistory == null || x.lp.HasValue)
                    .OrderByDescending(x => x.lp ?? DateTimeOffset.MinValue)
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

        // Imported collections (TagConfig.CollectionImported) are never deleted by HSC. Their names
        // are added to this file on every config save and at the start of every run (full sync and
        // Run Group), and never removed, so the protection stays after the source is changed,
        // switched off or deleted. Returns everything protected so far.
        private const string ImportedCollectionsFile = "homescreencompanion_collections_imported.txt";
        private static readonly object _importedFileLock = new object();
        internal static HashSet<string> RecordImportedCollections(PluginConfiguration? cfg)
        {
            var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var dir = Plugin.Instance?.DataFolderPath;
            if (string.IsNullOrEmpty(dir)) return set;
            var path = Path.Combine(dir, ImportedCollectionsFile);
            lock (_importedFileLock)
            {
                try { if (File.Exists(path)) foreach (var l in File.ReadAllLines(path)) if (l.Trim().Length > 0) set.Add(l.Trim()); } catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
                int before = set.Count;
                foreach (var tc in (cfg?.Tags ?? new List<TagConfig>()).Where(t => t.CollectionImported))
                {
                    string cn = string.IsNullOrWhiteSpace(tc.CollectionName) ? (tc.Tag ?? "").Trim() : tc.CollectionName.Trim();
                    if (cn.Length > 0) set.Add(cn);
                }
                if (set.Count != before)
                    try { Directory.CreateDirectory(dir); File.WriteAllLines(path, set.OrderBy(n => n, StringComparer.OrdinalIgnoreCase)); } catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            }
            return set;
        }

        private static List<string> LoadFileHistory(string filename)
        {
            try { var path = Path.Combine(Plugin.Instance.DataFolderPath, filename); if (File.Exists(path)) return File.ReadAllLines(path).Select(l => l.Trim()).Where(l => !string.IsNullOrEmpty(l)).ToList(); } catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            return new List<string>();
        }

        private static void SaveFileHistory(string filename, List<string> data)
        {
            try { var path = Path.Combine(Plugin.Instance.DataFolderPath, filename); Directory.CreateDirectory(Path.GetDirectoryName(path)); File.WriteAllLines(path, data); } catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
        }

        // A film's versions (e.g. 2160p and 1080p files) are separate items that Emby shows as one
        // movie. A collection gets one of them, the first (lowest id), like Emby's own grouping;
        // this returns the ids of the other versions. Anything that is not a movie is kept.
        internal static HashSet<long> ExtraVersionIds(IEnumerable<BaseItem> items)
        {
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var extra = new HashSet<long>();
            foreach (var item in items.Where(i => i is MediaBrowser.Controller.Entities.Movies.Movie).OrderBy(i => i.InternalId))
            {
                var key = VersionKey(item);
                if (key != null && !seen.Add(key)) extra.Add(item.InternalId);
            }
            return extra;
        }

        // What makes two movie items versions of one film: the same IMDb id, else the same PresentationUniqueKey.
        private static string? VersionKey(BaseItem item)
        {
            var imdb = item.GetProviderId("Imdb");
            return !string.IsNullOrEmpty(imdb) ? "imdb:" + imdb
                : !string.IsNullOrEmpty(item.PresentationUniqueKey) ? "puk:" + item.PresentationUniqueKey : null;
        }

        // A playlist or collection holds one version of a film; its other versions count as members too,
        // so a "Playlist:" / "Collection:" rule (or its "not in" form) treats the film as a whole.
        private static void AddOtherVersions(HashSet<long> ids, BaseItem member, List<BaseItem> allItems, ref Dictionary<string, List<long>>? versionIdsByKey)
        {
            if (!(member is MediaBrowser.Controller.Entities.Movies.Movie)) return;
            var key = VersionKey(member);
            if (key == null) return;
            if (versionIdsByKey == null)
            {
                versionIdsByKey = new Dictionary<string, List<long>>(StringComparer.OrdinalIgnoreCase);
                foreach (var item in allItems.Where(i => i is MediaBrowser.Controller.Entities.Movies.Movie))
                {
                    var k = VersionKey(item);
                    if (k == null) continue;
                    if (!versionIdsByKey.TryGetValue(k, out var list)) versionIdsByKey[k] = list = new List<long>();
                    list.Add(item.InternalId);
                }
            }
            if (versionIdsByKey.TryGetValue(key, out var versions)) ids.UnionWith(versions);
        }

        // Ranked collection: the art is drawn from the real films in list order (one version each).
        private static List<BaseItem> RankedArtItems(List<BaseItem> ordered)
        {
            var extra = ExtraVersionIds(ordered);
            return ordered.Where(i => !extra.Contains(i.InternalId)).GroupBy(i => i.InternalId).Select(g => g.First()).ToList();
        }

        private static Dictionary<long, BaseItem> ItemsById(List<BaseItem> allItems)
        {
            var byId = new Dictionary<long, BaseItem>();
            foreach (var i in allItems) byId[i.InternalId] = i;
            return byId;
        }

        private static List<BaseItem> OrderedItems(Dictionary<long, BaseItem> byId, IEnumerable<long> ids)
            => ids.Select(id => byId.TryGetValue(id, out var it) ? it : null).Where(i => i != null).Select(i => i!).ToList();

        // New collections with generated or uploaded art: Emby refreshes a collection right after
        // CreateCollection, and that refresh can save the item it loaded before the art was put on
        // it (the art then only shows after the next sync). Two short checks put the art back; when
        // it is still there nothing is written (SetGeneratedArt compares path and date).
        private async Task ReapplyNewCollectionArtAsync(List<(string Name, TagConfig Source, IList<BaseItem> Items)> created, CancellationToken cancellationToken)
        {
            var withArt = created.Where(c => CollectionArtRenderer.IsStyle(c.Source.CollectionPosterStyle)
                || CollectionArtRenderer.IsStyle(c.Source.CollectionBackgroundStyle)
                || !string.IsNullOrWhiteSpace(c.Source.CollectionBackgroundPath)
                || CollectionArtRenderer.IsStyle(c.Source.CollectionThumbStyle)
                || !string.IsNullOrWhiteSpace(c.Source.CollectionThumbPath)
                || !string.IsNullOrWhiteSpace(c.Source.CollectionBannerPath)).ToList();
            if (withArt.Count == 0) return;
            foreach (var delay in new[] { 3000, 5000 })
            {
                await Task.Delay(delay, cancellationToken);
                foreach (var (name, source, items) in withArt)
                {
                    var coll = _libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "BoxSet" }, Name = name, Recursive = true }).FirstOrDefault();
                    if (coll != null) ApplyCollectionArt(coll, name, source, items);
                }
            }
        }

        // Generated poster / background (CollectionArtRenderer) and "top of Collections" for a
        // source's collection. Art is drawn from the first titles in list order and only redrawn
        // when the titles, style or name change.
        private void ApplyCollectionArt(BaseItem coll, string cName, TagConfig tc, IList<BaseItem> items)
        {
            try
            {
                if (SetGeneratedArt(coll, cName, "collection_art", tc.CollectionPosterStyle, tc.CollectionBackgroundStyle,
                        tc.CollectionArtTitle, tc.CollectionBackgroundPath, items, false,
                        posterOptions: tc.CollectionPosterOptions, backgroundOptions: tc.CollectionBackgroundOptions,
                        thumb: (tc.CollectionThumbStyle ?? "", tc.CollectionThumbOptions ?? "", tc.CollectionThumbPath ?? ""),
                        banner: (tc.CollectionBannerStyle ?? "", tc.CollectionBannerPath ?? "")))
                {
                    _libraryManager.UpdateItem(coll, coll.Parent, ItemUpdateType.ImageUpdate, null);
                    _log.Debug($"  {cName}  →  collection art applied");
                }
                if (tc.CollectionImported) MarkReplacedArt(coll, cName);
            }
            catch (Exception ex)
            {
                _log.Warn($"Collection art for \"{cName}\" failed: {ex.Message}");
            }
        }

        // An imported collection whose poster / background is now HSC's own (generated or uploaded,
        // both kept in the plugin's data folder) no longer has its original image: remember that, so
        // the card stops offering "Keep current image" for it. Saved with the config after the run.
        private static void MarkReplacedArt(BaseItem coll, string cName)
        {
            var dataDir = Plugin.Instance?.DataFolderPath;
            if (string.IsNullOrEmpty(dataDir)) return;
            bool Ours(ImageType type)
            {
                var path = (coll.ImageInfos ?? Array.Empty<ItemImageInfo>()).FirstOrDefault(i => i.Type == type)?.Path;
                return !string.IsNullOrEmpty(path) && path!.StartsWith(dataDir!, StringComparison.OrdinalIgnoreCase);
            }
            foreach (var t in Plugin.Instance!.Configuration.Tags.Where(t => t.CollectionImported && t.EnableCollection))
            {
                var name = string.IsNullOrWhiteSpace(t.CollectionName) ? (t.Tag ?? "").Trim() : t.CollectionName.Trim();
                if (!string.Equals(name, cName, StringComparison.OrdinalIgnoreCase)) continue;
                if (!CollectionArtRenderer.IsKeep(t.CollectionPosterStyle) && Ours(ImageType.Primary)) t.CollectionPosterReplaced = true;
                if (!CollectionArtRenderer.IsKeep(t.CollectionBackgroundStyle) && Ours(ImageType.Backdrop)) t.CollectionBackgroundReplaced = true;
                if (!CollectionArtRenderer.IsKeep(t.CollectionThumbStyle) && Ours(ImageType.Thumb)) t.CollectionThumbReplaced = true;
                if (!CollectionArtRenderer.IsKeep(t.CollectionBannerStyle) && Ours(ImageType.Banner)) t.CollectionBannerReplaced = true;
            }
        }

        // Generated poster / background on the tag's own page (Emby opens a tag as an item; its
        // background is only ever the tag item's own Backdrop). Same styles and cache as a
        // collection, drawn into <plugin data>/tag_art/<tag>/. With no style set (the default)
        // nothing is touched, except that art this plugin put on the tag earlier is removed so
        // Emby's own collage poster returns.
        private void ApplyTagArt(string tagName, TagConfig tc, IList<BaseItem> titles)
        {
            try
            {
                bool wanted = CollectionArtRenderer.IsStyle(tc.TagPosterStyle) || CollectionArtRenderer.IsStyle(tc.TagBackgroundStyle)
                    || !string.IsNullOrWhiteSpace(tc.TagBackgroundPath);
                BaseItem? tagItem = null;
                // The tag item appears once the first title is saved with the tag; give it a moment.
                for (int attempt = 0; attempt < (wanted ? 5 : 1) && tagItem == null; attempt++)
                {
                    if (attempt > 0) Thread.Sleep(300);
                    tagItem = _libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "Tag" }, Name = tagName }).FirstOrDefault();
                }
                if (tagItem == null) return;
                if (!wanted && !(tagItem.ImageInfos ?? Array.Empty<ItemImageInfo>()).Any(i => IsPluginTagArt(i.Path))) return;

                // Art shows titles, never episodes or seasons: those count as their show.
                var items = titles.Select(i => i is MediaBrowser.Controller.Entities.TV.Episode ep ? ep.Series
                                             : i is MediaBrowser.Controller.Entities.TV.Season se ? se.Series : i)
                                  .Where(i => i != null).Select(i => i!).ToList();
                if (SetGeneratedArt(tagItem, tagName, "tag_art", tc.TagPosterStyle, tc.TagBackgroundStyle,
                        tc.TagArtTitle, tc.TagBackgroundPath, items, true,
                        posterOptions: tc.TagPosterOptions, backgroundOptions: tc.TagBackgroundOptions))
                {
                    _libraryManager.UpdateItem(tagItem, tagItem.GetParent(), ItemUpdateType.ImageUpdate, null);
                    _log.Debug(wanted ? $"  {tagName}  →  tag art applied" : $"  {tagName}  →  tag art removed");
                }
            }
            catch (Exception ex)
            {
                _log.Warn($"Tag art for \"{tagName}\" failed: {ex.Message}");
            }
        }

        private const string EmbyCollagePoster = "emby://playlistcollage";

        // Images this plugin put on a tag item or a Your Next Watch playlist: generated art or an upload.
        private static bool IsPluginTagArt(string? path)
        {
            if (string.IsNullOrEmpty(path) || Plugin.Instance == null) return false;
            var data = Plugin.Instance.DataFolderPath;
            return path!.StartsWith(Path.Combine(data, "tag_art"), StringComparison.OrdinalIgnoreCase)
                || path.StartsWith(Path.Combine(data, "playlist_art"), StringComparison.OrdinalIgnoreCase)
                || path.StartsWith(Path.Combine(data, "collection_images"), StringComparison.OrdinalIgnoreCase);
        }

        // Puts the generated (or uploaded) poster and background on the item's ImageInfos. With
        // removeWhenNone, a kind with no style drops the plugin's earlier image (the poster goes
        // back to Emby's own collage). posterPath: an uploaded poster used when no poster style is
        // set (playlists). artKey: the art's folder name when it differs from name. thumb / banner
        // (collections only): the collection's Thumb (drawn like the background) and Banner
        // (upload only); null = not handled. True when the item's images changed.
        private bool SetGeneratedArt(BaseItem target, string name, string artFolder, string posterStyle, string backgroundStyle,
            string artTitle, string backgroundPath, IList<BaseItem> items, bool removeWhenNone, string? posterPath = null, string? artKey = null,
            string? posterOptions = null, string? backgroundOptions = null,
            (string Style, string Options, string Path)? thumb = null, (string Style, string Path)? banner = null)
        {
            bool changed = false;
            var kinds = new List<(string Style, ImageType Type, bool Wide, string Options, string? Upload, string File)>
            {
                (posterStyle, ImageType.Primary, false, posterOptions ?? "", posterPath, "poster"),
                (backgroundStyle, ImageType.Backdrop, true, backgroundOptions ?? "", backgroundPath, "background")
            };
            if (thumb != null) kinds.Add((thumb.Value.Style, ImageType.Thumb, true, thumb.Value.Options, thumb.Value.Path, "thumb"));
            // No generated banner: any style but Keep is Custom (the upload).
            if (banner != null) kinds.Add((CollectionArtRenderer.IsKeep(banner.Value.Style) ? banner.Value.Style : "", ImageType.Banner, true, "", banner.Value.Path, "banner"));
            foreach (var (style, type, background, options, upload, file) in kinds)
            {
                // Keep current image: this kind is never touched (no render, upload or removal).
                if (CollectionArtRenderer.IsKeep(style)) continue;
                var opts = ArtOptions.Parse(options);
                var current = (target.ImageInfos ?? Array.Empty<ItemImageInfo>()).FirstOrDefault(i => i.Type == type);
                string? path;
                if (CollectionArtRenderer.IsStyle(style)) path = RenderCollectionArt(artKey ?? name, artFolder, CollectionArtRenderer.ArtTitle(artTitle, name), style, background, items, opts, file);
                else if (!string.IsNullOrWhiteSpace(upload) && File.Exists(upload))
                    path = upload;   // Custom: the uploaded image
                else
                {
                    if (removeWhenNone && current != null && IsPluginTagArt(current.Path))
                    {
                        var kept = (target.ImageInfos ?? Array.Empty<ItemImageInfo>()).Where(i => i.Type != type).ToList();
                        if (type == ImageType.Primary) kept.Add(new ItemImageInfo { Path = EmbyCollagePoster, Type = ImageType.Primary, DateModified = DateTime.UtcNow });
                        target.ImageInfos = kept.ToArray();
                        changed = true;
                    }
                    continue;
                }
                if (path == null) continue;
                if (current == null || !string.Equals(current.Path, path, StringComparison.OrdinalIgnoreCase)
                    || current.DateModified != File.GetLastWriteTimeUtc(path))
                {
                    var images = (target.ImageInfos ?? Array.Empty<ItemImageInfo>()).Where(i => i.Type != type).ToList();
                    images.Add(new ItemImageInfo { Path = path, Type = type, DateModified = File.GetLastWriteTimeUtc(path) });
                    target.ImageInfos = images.ToArray();
                    changed = true;
                }
            }
            return changed;
        }

        // Draws the art into <plugin data>/<artFolder>/<name>/ (collection_art or tag_art) and
        // returns its path, or the existing file when nothing changed. Null when no poster could be found.
        // kind: the file name ("thumb" is drawn as a background); default poster / background.
        private string? RenderCollectionArt(string cName, string artFolder, string title, string style, bool background, IList<BaseItem> items, ArtOptions? opts = null, string? kind = null)
        {
            opts ??= ArtOptions.None;
            var invalid = Path.GetInvalidFileNameChars();
            var safe = new string(cName.Select(ch => Array.IndexOf(invalid, ch) >= 0 ? '_' : ch).ToArray()).Trim().Trim('.');
            if (string.IsNullOrEmpty(safe)) safe = "collection";
            var dir = Path.Combine(Plugin.Instance!.DataFolderPath, artFolder, safe);
            Directory.CreateDirectory(dir);
            kind ??= background ? "background" : "poster";
            var output = Path.Combine(dir, kind + ".jpg");
            var keyFile = Path.Combine(dir, kind + ".key");

            var picks = CollectionArtRenderer.DistinctTitles(items).Take(CollectionArtRenderer.PostersFor(style, background, opts)).ToList();
            var key = style + "|" + title + "|" + string.Join(",", picks.Select(i => i.Id.ToString("N")));
            // Customise options join the key only when set, so art without them keeps its old key.
            var optKey = opts.Canonical();
            if (optKey.Length > 0) key += "|" + optKey;
            if (File.Exists(output) && File.Exists(keyFile) && File.ReadAllText(keyFile) == key) return output;

            var tempDir = Path.Combine(dir, "tmp");
            Directory.CreateDirectory(tempDir);
            try
            {
                // Each title's own poster file, read side by side (no thumbs, no image refresh).
                var posters = HomeScreenCompanionService.ArtPosterPaths(picks, _httpClient, tempDir).Where(p => p != null).Select(p => p!).ToList();
                if (posters.Count < picks.Count) _log.Debug($"  Art for \"{cName}\": {picks.Count - posters.Count} title(s) have no poster image");
                if (posters.Count == 0) { _log.Warn($"Art for \"{cName}\": no posters found"); return null; }
                CollectionArtRenderer.Render(style, posters, title, background, output, opts);
                File.WriteAllText(keyFile, key);
                return output;
            }
            finally
            {
                try { Directory.Delete(tempDir, true); } catch { }
            }
        }

        // Shared by the full sync and Run Group: records a source's collection description and
        // uploaded poster (when set) for ApplyCollectionMeta.
        private static void CollectCollectionMeta(TagConfig tc, string cName,
            Dictionary<string, string> descriptions, Dictionary<string, string> posters)
        {
            if (!string.IsNullOrWhiteSpace(tc.CollectionDescription))
                descriptions[cName] = tc.CollectionDescription;
            if (!CollectionArtRenderer.IsKeep(tc.CollectionPosterStyle)
                && !string.IsNullOrWhiteSpace(tc.CollectionPosterPath) && File.Exists(tc.CollectionPosterPath))
                posters[cName] = tc.CollectionPosterPath;
        }

        private void ApplyCollectionMeta(BaseItem item, string cName,
            Dictionary<string, string> descriptions, Dictionary<string, string> posters, bool debug)
        {
            bool metaChanged = false;

            if (descriptions.TryGetValue(cName, out var desc) && !string.IsNullOrWhiteSpace(desc))
            {
                item.Overview = desc;
                metaChanged = true;
            }

            if (posters.TryGetValue(cName, out var posterPath) && File.Exists(posterPath))
            {
                var imageInfo = new ItemImageInfo
                {
                    Path = posterPath,
                    Type = ImageType.Primary,
                    DateModified = File.GetLastWriteTimeUtc(posterPath)
                };
                var otherImages = (item.ImageInfos ?? Array.Empty<ItemImageInfo>())
                    .Where(i => i.Type != ImageType.Primary).ToList();
                otherImages.Add(imageInfo);
                item.ImageInfos = otherImages.ToArray();
                _libraryManager.UpdateItem(item, item.Parent, ItemUpdateType.ImageUpdate, null);
                _log.Debug($"  {cName}  →  poster applied");
            }

            if (metaChanged)
                _libraryManager.UpdateItem(item, item.Parent, ItemUpdateType.MetadataEdit, null);
        }

        // Sets a collection's sort title, locked so Emby's metadata refreshes keep it. When the
        // field is emptied, Emby's own sort order is restored — but only if the collection still
        // has the sort title the plugin gave it, so one set by hand in Emby is never touched.
        // homescreencompanion_sorttitles.json remembers which sort title the plugin set.
        private void ApplyCollectionSortName(BaseItem coll, string cName, string? sortTitle)
        {
            sortTitle = (sortTitle ?? "").Trim();
            var record = LoadSortTitleRecord();
            var locked = (coll.LockedFields ?? Array.Empty<MetadataFields>()).ToList();
            bool isLocked = locked.Contains(MetadataFields.SortName);

            if (sortTitle.Length > 0)
            {
                if (!string.Equals(coll.SortName, sortTitle, StringComparison.Ordinal) || !isLocked)
                {
                    coll.SortName = sortTitle;
                    if (!isLocked) { locked.Add(MetadataFields.SortName); coll.LockedFields = locked.ToArray(); }
                    _libraryManager.UpdateItem(coll, coll.Parent, ItemUpdateType.MetadataEdit, null);
                    _log.Debug($"  {cName}  →  sort title \"{sortTitle}\"");
                }
                if (!record.TryGetValue(cName, out var known) || known != sortTitle)
                {
                    record[cName] = sortTitle;
                    SaveSortTitleRecord(record);
                }
            }
            else if (record.TryGetValue(cName, out var applied))
            {
                if (string.Equals(coll.SortName, applied, StringComparison.Ordinal))
                {
                    locked.Remove(MetadataFields.SortName);
                    coll.LockedFields = locked.ToArray();
                    coll.SortName = null; // Emby derives it from the name again
                    _libraryManager.UpdateItem(coll, coll.Parent, ItemUpdateType.MetadataEdit, null);
                    _log.Debug($"  {cName}  →  sort title removed");
                }
                record.Remove(cName);
                SaveSortTitleRecord(record);
            }
        }

        private Dictionary<string, string> LoadSortTitleRecord()
        {
            try
            {
                var path = Path.Combine(Plugin.Instance.DataFolderPath, "homescreencompanion_sorttitles.json");
                if (File.Exists(path))
                {
                    var saved = _jsonSerializer.DeserializeFromFile<Dictionary<string, string>>(path);
                    if (saved != null) return new Dictionary<string, string>(saved, StringComparer.OrdinalIgnoreCase);
                }
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            return new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        }

        private void SaveSortTitleRecord(Dictionary<string, string> record)
        {
            try { _jsonSerializer.SerializeToFile(record, Path.Combine(Plugin.Instance.DataFolderPath, "homescreencompanion_sorttitles.json")); }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
        }

        // onlyTag: Run Group on one top-list — rebuild just that list and leave the other folders alone.
        private void SyncTopListFolders(PluginConfiguration config, bool dryRun, string? onlyTag = null)
        {
            var dataPath = Plugin.Instance.DataFolderPath;
            var topListsPath = Path.Combine(dataPath, "toplists");

            // Lists switched off in their source: rows, library and copies go first (a list made
            // again in the same folder is written below).
            if (!dryRun) SourceTopLists.RemoveQueued(config, _libraryManager, _userManager, _log, CancellationToken.None, onlyTag);

            var configuredNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var tl in config.TopLists ?? new List<TopListHomeSection>())
                if (!string.IsNullOrWhiteSpace(tl.TagName))
                    configuredNames.Add(SanitizeTopListFolderName(tl.TagName));

            // Remove folders for tags that no longer exist in config
            if (onlyTag == null && Directory.Exists(topListsPath))
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
            if (onlyTag != null && !(config.TopLists ?? new List<TopListHomeSection>()).Any(t => string.Equals(t.TagName, onlyTag, StringComparison.OrdinalIgnoreCase)))
                return;

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
                    if (RankedCollections.IsRankedPath(m.Path)) continue;
                    var imdb = m.GetProviderId("Imdb");
                    if (!string.IsNullOrEmpty(imdb) && !origLookup.ContainsKey(imdb))
                        origLookup[imdb] = m;
                }
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }

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

            SyncShowTopListFolders(config, topListsPath, managedTagNames, onlyTag);

            // Recreate .strm/.nfo files for each tag-based top-list
            foreach (var tl in config.TopLists ?? new List<TopListHomeSection>())
            {
                if (string.IsNullOrWhiteSpace(tl.TagName)) continue;
                if (onlyTag != null && !string.Equals(tl.TagName, onlyTag, StringComparison.OrdinalIgnoreCase)) continue;
                if (ShowTopList.IsShows(tl)) continue;
                if (!managedTagNames.Contains(tl.TagName)) continue;

                var sanitized = SanitizeTopListFolderName(tl.TagName);
                var folderPath = Path.Combine(topListsPath, sanitized);
                Directory.CreateDirectory(folderPath);

                var badgeStyle = "neutral";
                int effectiveMaxItems = tl.MaxItems;
                if (!string.IsNullOrEmpty(tl.HomeSectionSettings) && tl.HomeSectionSettings != "{}")
                {
                    try
                    {
                        var tlSettings = _jsonSerializer.DeserializeFromString<Dictionary<string, string>>(tl.HomeSectionSettings);
                        if (tlSettings != null)
                            badgeStyle = BadgeLook.FromSettings(tlSettings, "neutral");
                        if (effectiveMaxItems <= 0 && tlSettings != null
                            && tlSettings.TryGetValue("MaxItems", out var miStr)
                            && int.TryParse(miStr, out var parsedMax) && parsedMax > 0)
                            effectiveMaxItems = parsedMax;
                    }
                    catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
                }

                // The source's titles in list order (TopListMembers); before its first run since 4.5,
                // the titles with its tag sorted by the rank file. Top-list copies are skipped — see
                // PrepareTopListFolderRequest handler.
                var (items, fromList) = TopListMembers.MoviesFor(tl.TagName, _libraryManager, _jsonSerializer, topListsFolder);

                var rankFile = Path.Combine(dataPath, "tag_ranks", sanitized + ".json");
                if (!fromList && File.Exists(rankFile))
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
                    catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
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
                var rankExtras = TopListHistory.Update(tl.TagName, selected.Select(s => s.Item).ToList(), badgeStyle,
                    _jsonSerializer, _libraryManager, _userManager, _userDataManager, m => _log.Warn(m));
                // Only files that changed are written (and the ones no longer in the list removed): the
                // folder is a watched library, so rewriting every file made Emby refresh every title of
                // the list on each sync.
                var changedBases = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                var wantedFiles = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                var tempDir = Path.Combine(Path.GetTempPath(), "hsc_" + Guid.NewGuid().ToString("N"));
                var stageDir = Path.Combine(tempDir, "stage");
                Directory.CreateDirectory(stageDir);
                // What each title's badged images were drawn from (source images, rank, look, extras):
                // unchanged → not drawn again. Remote source images (TMDb) come back as different bytes
                // on every download, so comparing the drawn files alone still rewrote them each sync.
                var renderFile = Path.Combine(dataPath, "toplist_render", sanitized + ".json");
                Dictionary<string, string> renderSigs;
                try { renderSigs = File.Exists(renderFile) ? _jsonSerializer.DeserializeFromFile<Dictionary<string, string>>(renderFile) ?? new Dictionary<string, string>() : new Dictionary<string, string>(); }
                catch { renderSigs = new Dictionary<string, string>(); }
                var newSigs = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                try
                {
                    foreach (var entry in selected)
                    {
                        count++;
                        var sortPrefix = count.ToString().PadLeft(digits, '0');
                        bool changed = ShowTopList.WriteIfChanged(Path.Combine(folderPath, entry.BaseName + ".nfo"),
                            HomeScreenCompanionService.BuildTopListNfo(entry.Item, sortPrefix));
                        var rx = rankExtras.TryGetValue(entry.Item.Id, out var _rx) ? _rx : null;
                        var sig = RenderSignature(entry.Item, count, badgeStyle, rx);
                        newSigs[entry.BaseName] = sig;
                        bool drawn = renderSigs.TryGetValue(entry.BaseName, out var oldSig) && oldSig == sig
                            && File.Exists(Path.Combine(folderPath, entry.BaseName + ".jpg"));
                        if (!drawn)
                        {
                            // Drawn aside first, copied in only when different.
                            var stageBase = Path.Combine(stageDir, entry.BaseName);
                            HomeScreenCompanionService.WriteRankedImages(
                                entry.Item, count, stageBase, badgeStyle, tempDir,
                                _httpClient, _providerManager, _libraryManager, _fileSystem, m => _log.Warn(m), rx);
                            foreach (var suffix in new[] { ".jpg", "-thumb.jpg" })
                                if (ShowTopList.CopyIfChanged(stageBase + suffix, Path.Combine(folderPath, entry.BaseName + suffix))) changed = true;
                        }
                        foreach (var suffix in new[] { ".jpg", "-thumb.jpg" })
                            wantedFiles.Add(entry.BaseName + suffix);
                        // .strm last: the folder is a watched library, and Emby creates the item the
                        // moment it sees the .strm — the nfo and badged images must already be there.
                        if (ShowTopList.WriteIfChanged(Path.Combine(folderPath, entry.BaseName + ".strm"), entry.FilePath)) changed = true;
                        wantedFiles.Add(entry.BaseName + ".nfo");
                        wantedFiles.Add(entry.BaseName + ".strm");
                        if (changed) changedBases.Add(entry.BaseName);
                    }
                }
                finally
                {
                    try { Directory.Delete(tempDir, true); } catch { }
                }
                try
                {
                    Directory.CreateDirectory(Path.GetDirectoryName(renderFile)!);
                    _jsonSerializer.SerializeToFile(newSigs, renderFile);
                }
                catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
                int removedFiles = 0;
                foreach (var f in Directory.GetFiles(folderPath))
                {
                    var ext = Path.GetExtension(f);
                    if (!ext.Equals(".strm", StringComparison.OrdinalIgnoreCase) && !ext.Equals(".nfo", StringComparison.OrdinalIgnoreCase)
                        && !ext.Equals(".jpg", StringComparison.OrdinalIgnoreCase)) continue;
                    if (wantedFiles.Contains(Path.GetFileName(f))) continue;
                    try { File.Delete(f); removedFiles++; } catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
                }

                // Notify the file system monitor so Emby is aware of the changes
                if (changedBases.Count > 0 || removedFiles > 0)
                {
                    try { _libraryMonitor?.ReportFileSystemChanged(folderPath); }
                    catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
                }

                // For items already indexed in the library: update SortName and poster directly
                // so changes are visible immediately without waiting for a full library scan.
                try
                {
                    var sortPaths = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                    var originalNames = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                    for (int si = 0; si < selected.Count; si++)
                    {
                        var _strm = Path.Combine(folderPath, selected[si].BaseName + ".strm");
                        sortPaths[_strm] = (si + 1).ToString().PadLeft(digits, '0');
                        originalNames[_strm] = selected[si].Item.Name ?? "";
                    }

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
                        // Unchanged files and rank: the item is left alone (no update, no refresh).
                        bool rankChanged = sortPaths.TryGetValue(strmPath, out var newSort) && !string.Equals(li.SortName, newSort, StringComparison.Ordinal);
                        // The copy shows the original's title (see BuildTopListNfo), whatever language its library fetched.
                        bool nameChanged = originalNames.TryGetValue(strmPath, out var origName) && origName.Length > 0 && !string.Equals(li.Name, origName, StringComparison.Ordinal);
                        if (nameChanged) li.Name = origName;
                        if (!rankChanged && !nameChanged && !changedBases.Contains(Path.GetFileNameWithoutExtension(strmPath))
                            && HomeScreenCompanionService.RankedImagesApplied(li, folderPath)) continue;

                        // Update SortName
                        if (newSort != null)
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
                        catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }

                        try { _libraryManager.UpdateItem(li, li.Parent, ItemUpdateType.MetadataEdit, null); }
                        catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
                    }
                }
                catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }

                _log.Ok($"Top-list '{tl.TagName}': {RunLog.Plural(count, "movie")} synced to its library folder");
            }

            // A source's new top-list gets its library now that its files are there.
            if (SourceTopLists.EnsureLibraries(config, _libraryManager, _userManager, _providerManager, _fileSystem, _log, onlyTag))
                Plugin.Instance.SaveConfiguration();
        }

        // Show top-lists are rebuilt on every sync, manual ones too: their series get new episodes,
        // and each episode is a file in the list's folder. Tag-based lists also pick up the tag's
        // new members and order. Only changed files are touched (see ShowTopList.WriteFolder).
        private void SyncShowTopListFolders(PluginConfiguration config, string topListsPath, HashSet<string> managedTagNames, string? onlyTag = null)
        {
            foreach (var tl in (config.TopLists ?? new List<TopListHomeSection>()).Where(ShowTopList.IsShows))
            {
                if (string.IsNullOrWhiteSpace(tl.TagName)) continue;
                if (onlyTag != null && !string.Equals(tl.TagName, onlyTag, StringComparison.OrdinalIgnoreCase)) continue;
                try
                {
                    var folderPath = Path.Combine(topListsPath, SanitizeTopListFolderName(tl.TagName));
                    var settings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                    try
                    {
                        if (!string.IsNullOrEmpty(tl.HomeSectionSettings) && tl.HomeSectionSettings != "{}")
                            settings = _jsonSerializer.DeserializeFromString<Dictionary<string, string>>(tl.HomeSectionSettings) ?? settings;
                    }
                    catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
                    var badgeStyle = BadgeLook.FromSettings(settings, "neutral");
                    int maxItems = tl.MaxItems;
                    if (maxItems <= 0 && settings.TryGetValue("MaxItems", out var mi) && int.TryParse(mi, out var parsed)) maxItems = parsed;

                    List<BaseItem> series;
                    if (managedTagNames.Contains(tl.TagName))
                        series = ShowTopList.SeriesForTag(_libraryManager, _jsonSerializer, tl.TagName);
                    else
                    {
                        // Manual list: its series are the ones in the folder's manifest.
                        var manifest = ShowTopList.ReadManifest(folderPath, _jsonSerializer);
                        if (manifest.Count == 0) { _log.Skip($"Top-list '{tl.TagName}': no shows saved for this list"); continue; }
                        series = ShowTopList.ResolveSeries(_libraryManager, manifest.Select(e => (e.SeriesId, e.ImdbId)));
                    }
                    if (maxItems > 0) series = series.Take(maxItems).ToList();

                    var count = ShowTopList.WriteFolder(folderPath, series, badgeStyle,
                        _libraryManager, _jsonSerializer, _httpClient, _providerManager, _fileSystem, m => _log.Warn(m),
                        ranked => TopListHistory.Update(tl.TagName, ranked, badgeStyle, _jsonSerializer, _libraryManager, _userManager, _userDataManager, m => _log.Warn(m)));
                    try { _libraryMonitor?.ReportFileSystemChanged(folderPath); } catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
                    ShowTopList.ApplyIndexed(folderPath, _libraryManager, _jsonSerializer);
                    _log.Ok($"Top-list '{tl.TagName}': {RunLog.Plural(count, "show")} synced to its library folder");
                }
                catch (Exception ex)
                {
                    _log.Warn($"Top-list '{tl.TagName}': shows could not be synced — {ex.Message}");
                }
            }
        }

        // What a top-list title's badged images are drawn from: its source images (path and date),
        // its rank, the list's look and the extras (movement, weeks, plays).
        private static string RenderSignature(BaseItem item, int rank, string look, RankExtras? extras)
        {
            var images = item.ImageInfos ?? Array.Empty<ItemImageInfo>();
            string Src(ImageType t) => images.FirstOrDefault(i => i.Type == t) is ItemImageInfo ii ? ii.Path + "@" + ii.DateModified.Ticks : "";
            return string.Join("|", Src(ImageType.Primary), Src(ImageType.Thumb), Src(ImageType.Backdrop), rank, look,
                extras == null ? "" : $"{extras.Move}/{extras.Weeks}/{extras.Plays}/{extras.Show}");
        }

        private static string SanitizeTopListFolderName(string name)
        {
            var invalid = Path.GetInvalidFileNameChars();
            var safe = new string((name ?? "unknown").Select(c => Array.IndexOf(invalid, c) >= 0 ? '_' : c).ToArray()).Trim('.');
            return string.IsNullOrWhiteSpace(safe) ? "unknown" : safe;
        }
    }
}