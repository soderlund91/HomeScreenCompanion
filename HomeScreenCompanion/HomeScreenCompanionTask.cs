using MediaBrowser.Common.Net;
using MediaBrowser.Controller.Collections;
using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Library;
using MediaBrowser.Model.Entities;
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
        private readonly IUserDataManager _userDataManager;
        private readonly IHttpClient _httpClient;
        private readonly IJsonSerializer _jsonSerializer;
        private readonly ILogger _logger;

        public static HomeScreenCompanionTask? Instance { get; private set; }
        public static string LastRunStatus { get; private set; } = "Unknown (resets at server restart)";
        public static List<string> ExecutionLog { get; } = new List<string>();
        public static bool IsRunning { get; private set; } = false;

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

        // Utökar ItemsQuery med IsPlayed så att Embys JSON-serialisering inkluderar fältet
        private class ExtendedItemsQuery : ItemsQuery
        {
            public bool? IsPlayed { get; set; }
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
            public string? TagName;
            public string? CollectionName;
            public int GroupIndex;
            public int GroupTotal;
        }

        public HomeScreenCompanionTask(ILibraryManager libraryManager, ICollectionManager collectionManager, IUserManager userManager, IUserDataManager userDataManager, IHttpClient httpClient, IJsonSerializer jsonSerializer, ILogManager logManager)
        {
            _libraryManager = libraryManager;
            _collectionManager = collectionManager;
            _userManager = userManager;
            _userDataManager = userDataManager;
            _httpClient = httpClient;
            _jsonSerializer = jsonSerializer;
            _logger = logManager.GetLogger("HomeScreenCompanion");
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
                LastRunStatus = "Running...";

                var config = Plugin.Instance?.Configuration;
                if (config == null) return;

                bool debug = config.ExtendedConsoleOutput;
                bool dryRun = config.DryRunMode;

                var startTime = DateTime.Now;
                LogSummary("══════════════════════════════════════════════════");
                LogSummary($"Home Screen Companion v{Plugin.Instance?.Version}  ·  {startTime:yyyy-MM-dd HH:mm}");
                if (dryRun) LogSummary("  ! DRY RUN — no changes will be written");

                var allItems = _libraryManager.GetItemList(new InternalItemsQuery
                {
                    IncludeItemTypes = BuildItemTypes(config),
                    Recursive = true,
                    IsVirtualItem = false
                }).ToList();

                var imdbLookup = new Dictionary<string, List<BaseItem>>(StringComparer.OrdinalIgnoreCase);
                foreach (var item in allItems)
                {
                    if (item.LocationType != LocationType.FileSystem) continue;
                    var imdb = item.GetProviderId("Imdb");
                    if (!string.IsNullOrEmpty(imdb))
                    {
                        if (!imdbLookup.ContainsKey(imdb)) imdbLookup[imdb] = new List<BaseItem>();
                        imdbLookup[imdb].Add(item);
                    }
                }

                int movieCount = allItems.Count(i => i.GetType().Name.Contains("Movie"));
                int seriesCount = allItems.Count(i => i.GetType().Name.Contains("Series"));
                int activeGroupTotal = config.Tags.Count(t => t.Active && !string.IsNullOrWhiteSpace(t.Tag));
                LogSummary($"  Library: {movieCount} movies, {seriesCount} series");
                LogSummary($"  Active groups: {activeGroupTotal}");
                LogSummary("══════════════════════════════════════════════════");

                var fetcher = new ListFetcher(_httpClient, _jsonSerializer);
                var desiredTagsMap = new Dictionary<Guid, HashSet<string>>();
                var allScannedEpisodeItems = new Dictionary<Guid, BaseItem>();
                var allScannedSeasonItems = new Dictionary<Guid, BaseItem>();
                var desiredCollectionsMap = new Dictionary<string, HashSet<long>>(StringComparer.OrdinalIgnoreCase);
                var collectionDescriptions = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                var collectionPosters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                var managedTags = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                var activeCollections = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                var failedFetches = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                var previouslyManagedTags = LoadFileHistory("homescreencompanion_history.txt");
                foreach (var t in previouslyManagedTags) managedTags.Add(t);

                var previouslyManagedCollections = LoadFileHistory("homescreencompanion_collections.txt");

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
                    foreach (var c in allPersonCriteria)
                    {
                        var p = c.Split(':');
                        if ((p.Length == 2 || (p.Length == 3 && (p[1] == "exact" || p[1] == "contains")))
                            && (p[0] == "Actor" || p[0] == "Director" || p[0] == "Writer")
                            && Enum.TryParse<MediaBrowser.Model.Entities.PersonType>(p[0], out var personTypeEnum))
                        {
                            string matchOp = p.Length == 3 ? p[1] : "exact";
                            string personNameRaw = p.Length == 3 ? p[2].Trim() : p[1].Trim();
                            if (matchOp == "contains") continue;
                            foreach (var singleName in personNameRaw.Split(',').Select(n => n.Trim()).Where(n => n.Length > 0))
                            {
                                string indivKey = p.Length == 3 ? $"{p[0]}:{p[1]}:{singleName}" : $"{p[0]}:{singleName}";
                                if (personCache.ContainsKey(indivKey)) continue;
                                var personItem = _libraryManager.GetItemList(new InternalItemsQuery
                                {
                                    IncludeItemTypes = new[] { "Person" },
                                    Name = singleName
                                }).FirstOrDefault();
                                var personTypes = anyEpisodePersonCriteria && p[0] == "Actor"
                                    ? new[] { personTypeEnum, MediaBrowser.Model.Entities.PersonType.GuestStar }
                                    : new[] { personTypeEnum };
                                personCache[indivKey] = personItem == null ? new HashSet<long>() :
                                    _libraryManager.GetItemList(new InternalItemsQuery
                                    {
                                        PersonIds = new[] { personItem.InternalId },
                                        PersonTypes = personTypes,
                                        IncludeItemTypes = anyEpisodePersonCriteria
                                            ? new[] { "Movie", "Series", "Episode" }
                                            : new[] { "Movie", "Series" },
                                        Recursive = true,
                                        IsVirtualItem = false
                                    }).Select(x => x.InternalId).ToHashSet();
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
                        if (collectionMembershipCache.ContainsKey(crit)) continue;
                        var colonIdx = crit.IndexOf(':');
                        if (colonIdx < 1) continue;
                        var sourceKind = crit.Substring(0, colonIdx);
                        var sourceName = crit.Substring(colonIdx + 1).Trim();
                        string[] folderTypes = sourceKind.Equals("Playlist", StringComparison.OrdinalIgnoreCase)
                            ? new[] { "Playlist" } : new[] { "BoxSet" };
                        var folder = _libraryManager.GetItemList(new InternalItemsQuery
                        {
                            IncludeItemTypes = folderTypes,
                            Recursive = true
                        }).FirstOrDefault(i => string.Equals(i.Name, sourceName, StringComparison.OrdinalIgnoreCase));
                        if (folder == null) { collectionMembershipCache[crit] = new HashSet<long>(); continue; }
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
                        collectionMembershipCache[crit] = ids;
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

                var statsList = new List<GroupRunStats>();
                int activeGroupIdx = 0;
                var tagAddedByTag = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                var tagRemovedByTag = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                var collCreatedSet = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                var collItemsAdded = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                var collItemsRemoved = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

                foreach (var tagConfig in config.Tags)
                {
                    if (string.IsNullOrWhiteSpace(tagConfig.Tag) || !tagConfig.Active) continue;

                    string tagName = tagConfig.Tag.Trim();
                    string displayName = !string.IsNullOrWhiteSpace(tagConfig.Name) ? $"{tagConfig.Name} [{tagName}]" : tagName;
                    string srcLabel = string.IsNullOrEmpty(tagConfig.SourceType) ? "External" : tagConfig.SourceType;
                    var ruleFeatures = new List<string>();
                    if (tagConfig.EnableTag && !tagConfig.OnlyCollection) ruleFeatures.Add("Tag");
                    if (tagConfig.EnableCollection) ruleFeatures.Add("Collection");
                    if (tagConfig.EnableHomeSection) ruleFeatures.Add("HS");
                    string featureStr = ruleFeatures.Count > 0 ? $"  ({string.Join(", ", ruleFeatures)})" : "";
                    managedTags.Add(tagName);

                    activeGroupIdx++;
                    var gs = new GroupRunStats
                    {
                        DisplayName = displayName,
                        SourceType = srcLabel,
                        EnableTag = tagConfig.EnableTag && !tagConfig.OnlyCollection,
                        EnableCollection = tagConfig.EnableCollection,
                        EnableHomeSection = tagConfig.EnableHomeSection,
                        TagName = tagName,
                        GroupIndex = activeGroupIdx,
                        GroupTotal = activeGroupTotal
                    };

                    if (!IsScheduleActive(tagConfig.ActiveIntervals))
                    {
                        gs.Skipped = true;
                        gs.SkipReason = "out of schedule";
                        statsList.Add(gs);
                        continue;
                    }

                    string cName = string.IsNullOrWhiteSpace(tagConfig.CollectionName) ? tagName : tagConfig.CollectionName.Trim();
                    gs.CollectionName = cName;

                    if (!tagConfig.OverrideWhenActive &&
                        (activeTagOverrides.Contains(tagName) ||
                         (tagConfig.EnableCollection && activeCollectionOverrides.Contains(cName))))
                    {
                        gs.Skipped = true;
                        gs.SkipReason = "suppressed (overridden by priority entry)";
                        statsList.Add(gs);
                        continue;
                    }
                    if (tagConfig.EnableCollection)
                    {
                        activeCollections.Add(cName);
                        if (!string.IsNullOrWhiteSpace(tagConfig.CollectionDescription))
                            collectionDescriptions[cName] = tagConfig.CollectionDescription;
                        if (!string.IsNullOrWhiteSpace(tagConfig.CollectionPosterPath) && File.Exists(tagConfig.CollectionPosterPath))
                            collectionPosters[cName] = tagConfig.CollectionPosterPath;
                    }

                    try
                    {
                        if (debug) LogDebug($"── [{gs.GroupIndex}/{gs.GroupTotal}] {displayName}  ({srcLabel}) ──");
                        int effectiveLimit = tagConfig.Limit <= 0 ? 10000 : tagConfig.Limit;
                        var blacklist = new HashSet<string>(tagConfig.Blacklist ?? new List<string>(), StringComparer.OrdinalIgnoreCase);
                        var matchedLocalItems = new List<BaseItem>();
                        List<BaseItem> tagOutputItems = matchedLocalItems;
                        List<BaseItem> collectionOutputItems = matchedLocalItems;
                        int matchCount = 0;

                        if (string.IsNullOrEmpty(tagConfig.SourceType) || tagConfig.SourceType == "External")
                        {
                            var items = await fetcher.FetchItems(tagConfig.Url, effectiveLimit, config.TraktClientId, config.MdblistApiKey, cancellationToken);
                            gs.ListCount = items.Count;

                            if (items.Count > 0)
                            {
                                if (items.Count > effectiveLimit) items = items.Take(effectiveLimit).ToList();

                                foreach (var extItem in items)
                                {
                                    if (string.IsNullOrEmpty(extItem.Imdb)) continue;

                                    if (blacklist.Contains(extItem.Imdb))
                                    {
                                        if (debug) LogDebug($"  Blacklisted: {extItem.Name} ({extItem.Imdb})");
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
                                }
                            }
                            if (debug) LogDebug($"  Fetched {gs.ListCount} from list  ·  {matchedLocalItems.Count} matched in library");
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
                                    if (debug) LogDebug($"  Source: '{localSourceFolder.Name}'  ({localSourceFolder.GetType().Name})");

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
                                    {
                                        LogSummary($"  ! {displayName}  ·  '{tagConfig.LocalSourceId}' is empty or virtual", "Warn");
                                    }
                                    else if (debug)
                                    {
                                        LogDebug($"  Items in source: {children.Count}");
                                    }

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
                                            if (debug) LogDebug($"  Blacklisted: {itemToTag.Name} ({imdb})");
                                            continue;
                                        }

                                        if (!matchedLocalItems.Contains(itemToTag))
                                        {
                                            matchedLocalItems.Add(itemToTag);
                                        }
                                    }
                                }
                                else
                                {
                                    LogSummary($"  ! {displayName}  ·  {tagConfig.SourceType} '{tagConfig.LocalSourceId}' not found", "Warn");
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
                                return ItemMatchesMediaInfo(item, tagConfig, debug, seriesEpisodeCache, personCache, userDataCache, ci, preloadedUsers, seriesLastPlayedCache, collectionMembershipCache);
                            }).ToList();
                            if (debug) LogDebug($"  MediaInfo post-filter: {beforeCount} → {matchedLocalItems.Count} items");
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
                                itemsToScan = allItems;
                            }

                            foreach (var item in itemsToScan)
                            {
                                if (item.LocationType != LocationType.FileSystem) continue;

                                var imdb = item.GetProviderId("Imdb");
                                if (!string.IsNullOrEmpty(imdb) && blacklist.Contains(imdb)) continue;

                                CachedMediaInfo? ci = mediaInfoCache.TryGetValue(item.InternalId, out var ciVal) ? ciVal : (CachedMediaInfo?)null;
                                if (ItemMatchesMediaInfo(item, tagConfig, debug, seriesEpisodeCache, personCache, userDataCache, ci, preloadedUsers, seriesLastPlayedCache, collectionMembershipCache))
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
                                        if (sea) { var s = ResolveParentSeasons(matchedLocalItems); if (debug) LogDebug($"  Season ↑: {matchedLocalItems.Count} eps → {s.Count}"); list.AddRange(s); foreach (var x in s) allScannedSeasonItems.TryAdd(x.Id, x); }
                                        if (ser) { var s = ResolveParentSeries(matchedLocalItems); if (debug) LogDebug($"  Series ↑: {matchedLocalItems.Count} eps → {s.Count}"); list.AddRange(s); }
                                    }
                                    else
                                    {
                                        // Expand down: series → seasons/episodes; movies stay as-is for any target
                                        var movies = matchedLocalItems.Where(i => !i.GetType().Name.Contains("Series")).ToList();
                                        if (ser) list.AddRange(matchedLocalItems);
                                        if (sea) { var s = ResolveChildSeasons(seriesOnly); if (debug) LogDebug($"  Season ↓: {seriesOnly.Count} series → {s.Count}"); list.AddRange(s); foreach (var x in s) allScannedSeasonItems.TryAdd(x.Id, x); list.AddRange(movies); }
                                        if (ep)  { var e = ResolveChildEpisodes(seriesOnly); if (debug) LogDebug($"  Episode ↓: {seriesOnly.Count} series → {e.Count}"); list.AddRange(e); foreach (var x in e) allScannedEpisodeItems.TryAdd(x.Id, x); list.AddRange(movies); }
                                    }
                                    return list;
                                }

                                tagOutputItems        = BuildOutputList(tEp, tSea, tSer, tEp || tSea || tSer);
                                collectionOutputItems = BuildOutputList(cEp, cSea, cSer, cEp || cSea || cSer);
                            }
                            if (debug)
                            {
                                var _dbgItems = tagOutputItems.Count >= collectionOutputItems.Count ? tagOutputItems : collectionOutputItems;
                                LogDebug($"  Scanned {itemsToScan.Count} items  ·  {matchedLocalItems.Count} matched  (tag→{tagOutputItems.Count}, coll→{collectionOutputItems.Count})");
                                if (matchedLocalItems.Count > 0)
                                {
                                    LogDebug("  Matched items:");
                                    int _miShown = 0;
                                    foreach (var _mi in matchedLocalItems)
                                    {
                                        if (_miShown >= 50) { LogDebug($"    ... and {matchedLocalItems.Count - _miShown} more"); break; }
                                        var _miYr = _mi.ProductionYear.HasValue ? $" ({_mi.ProductionYear})" : "";
                                        var _miTp = _mi.GetType().Name.Contains("Series") ? "Series"
                                                  : _mi.GetType().Name.Contains("Episode") ? "Episode"
                                                  : _mi.GetType().Name.Contains("Season") ? "Season"
                                                  : "Movie";
                                        LogDebug($"    {_mi.Name}{_miYr}  [{_miTp}]");
                                        _miShown++;
                                    }
                                }
                            }
                        }
                        else if (tagConfig.SourceType == "AI")
                        {
                            var recentlyWatchedContext = BuildRecentlyWatchedContext(tagConfig);
                            var aiItems = await fetcher.FetchAiList(
                                tagConfig.AiProvider,
                                tagConfig.AiPrompt,
                                config.OpenAiApiKey,
                                config.GeminiApiKey,
                                recentlyWatchedContext,
                                effectiveLimit,
                                cancellationToken);

                            gs.ListCount = aiItems.Count;

                            foreach (var aiItem in aiItems)
                            {
                                if (string.IsNullOrWhiteSpace(aiItem.title)) continue;

                                if (!string.IsNullOrEmpty(aiItem.imdb_id))
                                {
                                    var imdbId = aiItem.imdb_id.Trim();
                                    if (blacklist.Contains(imdbId))
                                    {
                                        if (debug) LogDebug($"  Blacklisted: {aiItem.title} ({imdbId})");
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
                                }
                                else
                                {
                                    // Fallback: title+year match when AI didn't return an IMDB ID
                                    var titleMatches = FindByTitleAndYear(allItems, aiItem.title, aiItem.year);
                                    foreach (var localItem in titleMatches)
                                    {
                                        var imdb = localItem.GetProviderId("Imdb");
                                        if (!string.IsNullOrEmpty(imdb) && blacklist.Contains(imdb))
                                        {
                                            if (debug) LogDebug($"  Blacklisted: {localItem.Name} ({imdb})");
                                            continue;
                                        }
                                        if (!matchedLocalItems.Contains(localItem))
                                            matchedLocalItems.Add(localItem);
                                    }
                                }
                            }

                            if (debug) LogDebug($"  AI returned {gs.ListCount} items  ·  {matchedLocalItems.Count} matched in library");
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

                        if (tagConfig.EnableTag && !tagConfig.OnlyCollection)
                        {
                            foreach (var localItem in tagOutputItems)
                            {
                                if (!desiredTagsMap.ContainsKey(localItem.Id))
                                    desiredTagsMap[localItem.Id] = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                                desiredTagsMap[localItem.Id].Add(tagName);

                                var imdb = localItem.GetProviderId("Imdb");
                                if (!string.IsNullOrEmpty(imdb) && tagConfig.SourceType != "External")
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
                    }
                    catch (Exception ex)
                    {
                        gs.ErrorMessage = ex.Message;
                        LogSummary($"  ! {displayName}  ·  Error: {ex.Message}", "Error");
                        failedFetches.Add(tagName);
                        if (tagConfig.EnableCollection) failedFetches.Add(cName);
                    }

                    statsList.Add(gs);
                    currentProgress += step;
                    progress.Report(currentProgress);
                }

                if (!dryRun)
                {
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
                        catch (Exception ex) { LogSummary($"  ! Failed to save tags for '{item.Name}': {ex.Message}", "Warn"); }
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
                        catch (Exception ex) { LogSummary($"  ! Failed to save tags for '{item.Name}': {ex.Message}", "Warn"); }
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
                        catch (Exception ex) { LogSummary($"  ! Failed to save tags for season '{item.Name}': {ex.Message}", "Warn"); }
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

                if (debug && (_dbgTagAdded!.Count > 0 || _dbgTagRemoved!.Count > 0))
                {
                    LogDebug("── Tags ──────────────────────────────────────────");
                    var _allTagNames = _dbgTagAdded!.Keys.Concat(_dbgTagRemoved!.Keys)
                        .Distinct(StringComparer.OrdinalIgnoreCase);
                    foreach (var _tName in _allTagNames)
                    {
                        LogDebug($"  {_tName}");
                        var _added = _dbgTagAdded.GetValueOrDefault(_tName) ?? new List<string>();
                        var _removed = _dbgTagRemoved.GetValueOrDefault(_tName) ?? new List<string>();
                        int _shown = 0;
                        foreach (var _lbl in _added)
                        {
                            if (_shown >= 30) { LogDebug($"    ... and {_added.Count - _shown} more added"); break; }
                            LogDebug($"    + {_lbl}"); _shown++;
                        }
                        _shown = 0;
                        foreach (var _lbl in _removed)
                        {
                            if (_shown >= 30) { LogDebug($"    ... and {_removed.Count - _shown} more removed"); break; }
                            LogDebug($"    - {_lbl}"); _shown++;
                        }
                    }
                }

                int collCreated = 0, collUpdated = 0;
                if (debug && desiredCollectionsMap.Count > 0)
                    LogDebug("── Collections ───────────────────────────────────");
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
                            if (dryRun) continue;
                            var createdRef = await _collectionManager.CreateCollection(new CollectionCreationOptions { Name = cName, IsLocked = false, ItemIdList = desiredIds.ToArray() });
                            if (createdRef != null)
                            {
                                collCreated++;
                                collCreatedSet.Add(cName);
                                collItemsAdded[cName] = desiredIds.Count;
                                if (debug) LogDebug($"  {cName}  →  created ({desiredIds.Count} items)");
                                if (collectionDescriptions.ContainsKey(cName) || collectionPosters.ContainsKey(cName))
                                    ApplyCollectionMeta(createdRef, cName, collectionDescriptions, collectionPosters, debug);
                            }
                        }
                        else
                        {
                            var currentMembers = _libraryManager.GetItemList(new InternalItemsQuery { CollectionIds = new[] { existingColl.InternalId }, Recursive = true, IsVirtualItem = false }).Select(i => i.InternalId).ToHashSet();
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
                                    LogDebug($"  {cName}  →  updated (+{toAdd.Count}, -{toRemove.Count})");
                                    var _collMap = allItems.ToDictionary(i => i.InternalId, i => i.Name + (i.ProductionYear.HasValue ? $" ({i.ProductionYear})" : ""));
                                    string CollLabel(long id) => _collMap.TryGetValue(id, out var _cn) ? _cn : id.ToString();
                                    foreach (var id in toAdd) LogDebug($"    + {CollLabel(id)}");
                                    foreach (var id in toRemove) LogDebug($"    - {CollLabel(id)}");
                                }
                            }
                            if (!dryRun && (collectionDescriptions.ContainsKey(cName) || collectionPosters.ContainsKey(cName)))
                                ApplyCollectionMeta(existingColl, cName, collectionDescriptions, collectionPosters, debug);
                        }
                    }
                    catch (Exception ex)
                    {
                        LogSummary($"  ! Collection '{cName}'  ·  {ex.Message}", "Error");
                        if (debug) LogDebug($"  {cName}  →  ERROR: {ex.Message}");
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
                        LogSummary($"  ! Skipping cleanup of '{oldName}' — fetch failed (safety check)", "Warn");
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
                            if (debug) LogDebug($"  {oldName}  →  deleted (inactive)");
                        }
                    }
                    catch (Exception ex)
                    {
                        LogSummary($"  ! Cleanup of '{oldName}' failed  ·  {ex.Message}", "Warn");
                    }
                }
                if (!dryRun) SaveFileHistory("homescreencompanion_collections.txt", activeCollections.ToList());

                if (!dryRun) ManageHomeSections(config, cancellationToken, debug, statsList);

                progress.Report(100);
                var elapsed = DateTime.Now - startTime;
                string elapsedStr = elapsed.TotalMinutes >= 1
                    ? $"{(int)elapsed.TotalMinutes}m {elapsed.Seconds}s"
                    : $"{(int)elapsed.TotalSeconds}s";
                string finalStatus = dryRun ? "Dry Run" : "Success";
                LastRunStatus = $"{finalStatus} ({DateTime.Now:HH:mm})";

                // Emit per-group blocks
                LogSummary("");
                foreach (var gs in statsList)
                {
                    LogSummary($"[{gs.GroupIndex}/{gs.GroupTotal}] {gs.DisplayName}  ({gs.SourceType})");
                    if (gs.Skipped)
                    {
                        LogSummary($"  ~ {gs.SkipReason}");
                    }
                    else if (gs.ErrorMessage != null)
                    {
                        LogSummary($"  ! Error: {gs.ErrorMessage}");
                    }
                    else
                    {
                        if (gs.SourceType == "MediaInfo")
                            LogSummary($"  Scanned: {gs.ListCount} items · {gs.MatchCount} matched");
                        else if (gs.SourceType == "LocalCollection" || gs.SourceType == "LocalPlaylist")
                            LogSummary($"  Source: {gs.ListCount} items · {gs.MatchCount} matched");
                        else
                            LogSummary($"  List: {gs.ListCount} objects · {gs.MatchCount} matched in library");

                        if (gs.EnableTag)
                            LogSummary($"  Tag: +{gs.TagsAdded} added, -{gs.TagsRemoved} removed");
                        if (gs.EnableCollection)
                            LogSummary(gs.CollectionCreated
                                ? $"  Collection: created ({gs.CollectionItemsAdded} items)"
                                : $"  Collection: updated (+{gs.CollectionItemsAdded}, -{gs.CollectionItemsRemoved})");
                        if (gs.EnableHomeSection)
                            LogSummary(gs.HomeSectionSynced
                                ? $"  Home section: synced for {gs.HomeSectionUserCount} user(s)"
                                : "  Home section: not synced");
                    }
                    LogSummary("");
                }

                // Final summary
                int totalTagsAdded = statsList.Sum(g => g.TagsAdded);
                int totalTagsRemoved = statsList.Sum(g => g.TagsRemoved);
                int totalCollCreated = statsList.Count(g => g.CollectionCreated);
                int totalCollUpdated = statsList.Count(g => !g.CollectionCreated && !g.Skipped && g.EnableCollection && (g.CollectionItemsAdded > 0 || g.CollectionItemsRemoved > 0));
                int totalHsSynced = statsList.Count(g => g.HomeSectionSynced);
                int totalHsRemoved = statsList.Count(g => g.HomeSectionRemoved);

                LogSummary("══════════════════════════════════════════════════");
                LogSummary("Summary");
                if (statsList.Any(g => g.EnableTag))
                    LogSummary($"  Tags:          +{totalTagsAdded} added,  -{totalTagsRemoved} removed");
                LogSummary($"  Collections:   {totalCollCreated} created,   {totalCollUpdated} updated,   {collDeleted} removed");
                LogSummary($"  Home sections: {totalHsSynced} synced,    {totalHsRemoved} removed");
                LogSummary($"  Done in {elapsedStr}  ·  {finalStatus}");
                LogSummary("══════════════════════════════════════════════════");
            }
            catch (Exception ex)
            {
                LastRunStatus = $"Failed: {ex.Message}";
                LogSummary($"CRITICAL ERROR: {ex.Message}", "Error");
            }
            finally { IsRunning = false; }
        }

        public async Task<(bool Success, string Message)> RunSingleEntryAsync(string entryName, CancellationToken cancellationToken)
        {
            IsRunning = true;
            lock (ExecutionLog) ExecutionLog.Clear();
            LastRunStatus = "Running...";
            try
            {
            return await RunSingleEntryInternalAsync(entryName, cancellationToken);
            }
            finally
            {
                IsRunning = false;
            }
        }

        private async Task<(bool Success, string Message)> RunSingleEntryInternalAsync(string entryName, CancellationToken cancellationToken)
        {
            var config = Plugin.Instance?.Configuration;
            if (config == null) return (false, "Config not found");

            var tagConfig = config.Tags.FirstOrDefault(t =>
                string.Equals(t.Name, entryName, StringComparison.OrdinalIgnoreCase) ||
                (!string.IsNullOrWhiteSpace(t.Name) == false && string.Equals(t.Tag, entryName, StringComparison.OrdinalIgnoreCase)));
            if (tagConfig == null) { LastRunStatus = $"Failed: entry not found"; return (false, $"Entry '{entryName}' not found in saved config"); }
            if (string.IsNullOrWhiteSpace(tagConfig.Tag)) { LastRunStatus = "Failed: no tag name"; return (false, "Entry has no tag name"); }

            string _displayName = !string.IsNullOrWhiteSpace(tagConfig.Name) ? $"{tagConfig.Name} [{tagConfig.Tag.Trim()}]" : tagConfig.Tag.Trim();
            string _srcLabel = string.IsNullOrEmpty(tagConfig.SourceType) ? "External" : tagConfig.SourceType;

            bool debug = config.ExtendedConsoleOutput;
            bool dryRun = config.DryRunMode;
            var startTime = DateTime.Now;

            var allItems = _libraryManager.GetItemList(new InternalItemsQuery
            {
                IncludeItemTypes = BuildItemTypes(config),
                Recursive = true,
                IsVirtualItem = false
            }).ToList();

            int _movieCount = allItems.Count(i => i.GetType().Name.Contains("Movie"));
            int _seriesCount = allItems.Count(i => i.GetType().Name.Contains("Series"));
            LogSummary("══════════════════════════════════════════════════");
            LogSummary($"Home Screen Companion v{Plugin.Instance?.Version}  ·  {startTime:yyyy-MM-dd HH:mm}  (single-entry run)");
            if (dryRun) LogSummary("  ! DRY RUN — no changes will be written");
            LogSummary($"  Library: {_movieCount} movies, {_seriesCount} series");
            LogSummary("══════════════════════════════════════════════════");

            var imdbLookup = new Dictionary<string, List<BaseItem>>(StringComparer.OrdinalIgnoreCase);
            foreach (var item in allItems)
            {
                if (item.LocationType != LocationType.FileSystem) continue;
                var imdb = item.GetProviderId("Imdb");
                if (!string.IsNullOrEmpty(imdb))
                {
                    if (!imdbLookup.ContainsKey(imdb)) imdbLookup[imdb] = new List<BaseItem>();
                    imdbLookup[imdb].Add(item);
                }
            }

            var seriesEpisodeCache = new Dictionary<long, BaseItem>();
            var personCache = new Dictionary<string, HashSet<long>>(StringComparer.OrdinalIgnoreCase);
            var collectionMembershipCache = new Dictionary<string, HashSet<long>>(StringComparer.OrdinalIgnoreCase);
            var mediaInfoCache = new Dictionary<long, CachedMediaInfo>();
            var userDataCache = new Dictionary<(Guid, long), (bool Played, DateTimeOffset? LastPlayedDate, int PlayCount)>();
            var seriesLastPlayedCache = new Dictionary<(Guid, long), DateTimeOffset?>();
            var preloadedUsers = _userManager.GetUserList(new UserQuery { IsDisabled = false });

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
                foreach (var c in allCriteria)
                {
                    var p = c.Split(':');
                    if ((p.Length == 2 || (p.Length == 3 && (p[1] == "exact" || p[1] == "contains")))
                        && (p[0] == "Actor" || p[0] == "Director" || p[0] == "Writer")
                        && Enum.TryParse<MediaBrowser.Model.Entities.PersonType>(p[0], out var personTypeEnum))
                    {
                        string matchOp = p.Length == 3 ? p[1] : "exact";
                        string personNameRaw = p.Length == 3 ? p[2].Trim() : p[1].Trim();
                        if (matchOp == "contains") continue;
                        foreach (var singleName in personNameRaw.Split(',').Select(n => n.Trim()).Where(n => n.Length > 0))
                        {
                            string indivKey = p.Length == 3 ? $"{p[0]}:{p[1]}:{singleName}" : $"{p[0]}:{singleName}";
                            if (personCache.ContainsKey(indivKey)) continue;
                            var personItem = _libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "Person" }, Name = singleName }).FirstOrDefault();
                            var personTypes = singleTagAnyEpisodePersonCriteria && p[0] == "Actor"
                                ? new[] { personTypeEnum, MediaBrowser.Model.Entities.PersonType.GuestStar }
                                : new[] { personTypeEnum };
                            personCache[indivKey] = personItem == null ? new HashSet<long>() :
                                _libraryManager.GetItemList(new InternalItemsQuery
                                {
                                    PersonIds = new[] { personItem.InternalId },
                                    PersonTypes = personTypes,
                                    IncludeItemTypes = singleTagAnyEpisodePersonCriteria
                                        ? new[] { "Movie", "Series", "Episode" }
                                        : new[] { "Movie", "Series" },
                                    Recursive = true,
                                    IsVirtualItem = false
                                }).Select(x => x.InternalId).ToHashSet();
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
                    if (collectionMembershipCache.ContainsKey(crit)) continue;
                    var colonIdx = crit.IndexOf(':');
                    if (colonIdx < 1) continue;
                    var sourceKind = crit.Substring(0, colonIdx);
                    var sourceName = crit.Substring(colonIdx + 1).Trim();
                    string[] folderTypes = sourceKind.Equals("Playlist", StringComparison.OrdinalIgnoreCase)
                        ? new[] { "Playlist" } : new[] { "BoxSet" };
                    var folder = _libraryManager.GetItemList(new InternalItemsQuery
                    {
                        IncludeItemTypes = folderTypes,
                        Recursive = true
                    }).FirstOrDefault(i => string.Equals(i.Name, sourceName, StringComparison.OrdinalIgnoreCase));
                    if (folder == null) { collectionMembershipCache[crit] = new HashSet<long>(); continue; }
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
                    collectionMembershipCache[crit] = ids;
                }
            }

            string tagName = tagConfig.Tag.Trim();
            string cName = string.IsNullOrWhiteSpace(tagConfig.CollectionName) ? tagName : tagConfig.CollectionName.Trim();
            int effectiveLimit = tagConfig.Limit <= 0 ? 10000 : tagConfig.Limit;
            var blacklist = new HashSet<string>(tagConfig.Blacklist ?? new List<string>(), StringComparer.OrdinalIgnoreCase);
            var matchedLocalItems = new List<BaseItem>();
            List<BaseItem> tagOutputItems = matchedLocalItems;
            List<BaseItem> collectionOutputItems = matchedLocalItems;
            int _listCount = 0;

            try
            {
                var fetcher = new ListFetcher(_httpClient, _jsonSerializer);

                if (string.IsNullOrEmpty(tagConfig.SourceType) || tagConfig.SourceType == "External")
                {
                    var items = await fetcher.FetchItems(tagConfig.Url, effectiveLimit, config.TraktClientId, config.MdblistApiKey, cancellationToken);
                    _listCount = items.Count;
                    if (items.Count > effectiveLimit) items = items.Take(effectiveLimit).ToList();
                    foreach (var extItem in items)
                    {
                        if (string.IsNullOrEmpty(extItem.Imdb)) continue;
                        if (blacklist.Contains(extItem.Imdb)) continue;
                        if (tagConfig.EnableTag && !tagConfig.OnlyCollection)
                            TagCacheManager.Instance.AddToCache($"imdb_{extItem.Imdb}", tagName);
                        if (imdbLookup.TryGetValue(extItem.Imdb, out var localItems))
                            foreach (var localItem in localItems)
                                if (!matchedLocalItems.Contains(localItem)) matchedLocalItems.Add(localItem);
                    }
                }
                else if (tagConfig.SourceType == "LocalCollection" || tagConfig.SourceType == "LocalPlaylist")
                {
                    if (!string.IsNullOrEmpty(tagConfig.LocalSourceId))
                    {
                        string[] folderTypes = tagConfig.SourceType == "LocalPlaylist" ? new[] { "Playlist" } : new[] { "BoxSet" };
                        var allFolders = _libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = folderTypes, Recursive = true });
                        var localSourceFolder = allFolders.FirstOrDefault(i => string.Equals(i.Name, tagConfig.LocalSourceId, StringComparison.OrdinalIgnoreCase));
                        if (localSourceFolder != null)
                        {
                            var children = tagConfig.SourceType == "LocalCollection"
                                ? _libraryManager.GetItemList(new InternalItemsQuery { CollectionIds = new[] { localSourceFolder.InternalId }, IsVirtualItem = false }).ToList()
                                : _libraryManager.GetItemList(new InternalItemsQuery { ListIds = new[] { localSourceFolder.InternalId } }).ToList();
                            _listCount = children.Count;
                            foreach (var child in children)
                            {
                                if (child == null) continue;
                                BaseItem itemToTag = child;
                                if (child.GetType().Name.Contains("PlaylistItem")) { try { var inner = ((dynamic)child).Item; if (inner != null) itemToTag = inner; } catch { } }
                                if (itemToTag.GetType().Name.Contains("Episode")) { try { var series = ((dynamic)itemToTag).Series; if (series != null) itemToTag = series; } catch { } }
                                if (!IsTaggableTopLevelItem(itemToTag)) continue;
                                var imdb = itemToTag.GetProviderId("Imdb");
                                if (!string.IsNullOrEmpty(imdb) && blacklist.Contains(imdb)) continue;
                                if (!matchedLocalItems.Contains(itemToTag)) matchedLocalItems.Add(itemToTag);
                            }
                            if (effectiveLimit < 10000 && matchedLocalItems.Count > effectiveLimit)
                                matchedLocalItems = matchedLocalItems.Take(effectiveLimit).ToList();
                        }
                        else return (false, $"Source '{tagConfig.LocalSourceId}' not found");
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
                        _itemsToScan = allItems;
                    }
                    _listCount = _itemsToScan.Count;
                    foreach (var item in _itemsToScan)
                    {
                        if (item.LocationType != LocationType.FileSystem) continue;
                        var imdb = item.GetProviderId("Imdb");
                        if (!string.IsNullOrEmpty(imdb) && blacklist.Contains(imdb)) continue;
                        CachedMediaInfo? ci = mediaInfoCache.TryGetValue(item.InternalId, out var ciVal) ? ciVal : (CachedMediaInfo?)null;
                        if (ItemMatchesMediaInfo(item, tagConfig, debug, seriesEpisodeCache, personCache, userDataCache, ci, preloadedUsers, seriesLastPlayedCache, collectionMembershipCache))
                        {
                            matchedLocalItems.Add(item);
                            if (effectiveLimit < 10000 && matchedLocalItems.Count >= effectiveLimit) break;
                        }
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
                    var aiItems = await fetcher.FetchAiList(
                        tagConfig.AiProvider,
                        tagConfig.AiPrompt,
                        config.OpenAiApiKey,
                        config.GeminiApiKey,
                        recentlyWatchedContext,
                        effectiveLimit,
                        cancellationToken);

                    _listCount = aiItems.Count;

                    foreach (var aiItem in aiItems)
                    {
                        if (string.IsNullOrWhiteSpace(aiItem.title)) continue;

                        if (!string.IsNullOrEmpty(aiItem.imdb_id))
                        {
                            var imdbId = aiItem.imdb_id.Trim();
                            if (blacklist.Contains(imdbId)) continue;
                            if (tagConfig.EnableTag && !tagConfig.OnlyCollection)
                                TagCacheManager.Instance.AddToCache($"imdb_{imdbId}", tagName);
                            if (imdbLookup.TryGetValue(imdbId, out var localItems))
                                foreach (var localItem in localItems)
                                    if (!matchedLocalItems.Contains(localItem)) matchedLocalItems.Add(localItem);
                        }
                        else
                        {
                            var titleMatches = FindByTitleAndYear(allItems, aiItem.title, aiItem.year);
                            foreach (var localItem in titleMatches)
                            {
                                var imdb = localItem.GetProviderId("Imdb");
                                if (!string.IsNullOrEmpty(imdb) && blacklist.Contains(imdb)) continue;
                                if (!matchedLocalItems.Contains(localItem)) matchedLocalItems.Add(localItem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogSummary($"Error: {ex.Message}", "Error");
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
                    return ItemMatchesMediaInfo(item, tagConfig, debug, seriesEpisodeCache, personCache, userDataCache, ci, preloadedUsers, seriesLastPlayedCache, collectionMembershipCache);
                }).ToList();
                if (debug) LogDebug($"  MediaInfo post-filter: {beforeCount} → {matchedLocalItems.Count} items");
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

            // Apply tags (scoped to this entry's tag only)
            int tagsAdded = 0, tagsRemoved = 0;
            var _dbgTagAdded = debug ? new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase) : null;
            var _dbgTagRemoved = debug ? new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase) : null;
            var matchedIds = new HashSet<Guid>(tagOutputItems.Select(i => i.Id));
            int updateCount = 0;
            foreach (var item in allItems)
            {
                var existingTags = new HashSet<string>(item.Tags, StringComparer.OrdinalIgnoreCase);
                bool shouldHave = tagConfig.EnableTag && !tagConfig.OnlyCollection && matchedIds.Contains(item.Id);
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
                    catch { }
                    if (++updateCount % 25 == 0) await Task.Yield();
                }
                if (shouldHave) tagsAdded++; else tagsRemoved++;
            }

            // Episode cleanup: remove stale tags from episodes; also tag matching episodes when episode-level targeting is active
            {
                var (tEpClean, _, _) = EffectiveTagTargets(tagConfig);
                bool targetsEpisodes = tagConfig.EnableTag && !tagConfig.OnlyCollection && tEpClean &&
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
                            if (ItemMatchesMediaInfo(ep, tagConfig, debug, seriesEpisodeCache, personCache, userDataCache, null, preloadedUsers, seriesLastPlayedCache, collectionMembershipCache))
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
                        catch { }
                        if (++updateCount % 25 == 0) await Task.Yield();
                    }
                    if (shouldHaveEp) tagsAdded++; else tagsRemoved++;
                }
            }

            // Season cleanup: remove stale tags from seasons; also tag matching seasons when season-level targeting is active
            {
                var (_, tSeaClean, _) = EffectiveTagTargets(tagConfig);
                bool targetsSeason = tagConfig.EnableTag && !tagConfig.OnlyCollection &&
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
                                      && ItemMatchesMediaInfo(ep, tagConfig, debug, seriesEpisodeCache, personCache, userDataCache, null, preloadedUsers, seriesLastPlayedCache, collectionMembershipCache))
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
                        catch { }
                        if (++updateCount % 25 == 0) await Task.Yield();
                    }
                    if (shouldHaveSeason) tagsAdded++; else tagsRemoved++;
                }
            }

            if (debug && (_dbgTagAdded!.Count > 0 || _dbgTagRemoved!.Count > 0))
            {
                LogDebug("── Tags ──────────────────────────────────────────");
                var _allTagNames = _dbgTagAdded!.Keys.Concat(_dbgTagRemoved!.Keys).Distinct(StringComparer.OrdinalIgnoreCase);
                foreach (var _tName in _allTagNames)
                {
                    LogDebug($"  {_tName}");
                    var _added = _dbgTagAdded.GetValueOrDefault(_tName) ?? new List<string>();
                    var _removed = _dbgTagRemoved.GetValueOrDefault(_tName) ?? new List<string>();
                    int _shown = 0;
                    foreach (var _lbl in _added)
                    {
                        if (_shown >= 30) { LogDebug($"    ... and {_added.Count - _shown} more added"); break; }
                        LogDebug($"    + {_lbl}"); _shown++;
                    }
                    _shown = 0;
                    foreach (var _lbl in _removed)
                    {
                        if (_shown >= 30) { LogDebug($"    ... and {_removed.Count - _shown} more removed"); break; }
                        LogDebug($"    - {_lbl}"); _shown++;
                    }
                }
            }

            // Apply collection (scoped to this entry's collection only)
            int collResult = 0;
            bool _collCreated = false;
            int _collItemsAdded = 0, _collItemsRemoved = 0;
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
                    }
                    else
                    {
                        var currentMembers = _libraryManager.GetItemList(new InternalItemsQuery { CollectionIds = new[] { existingColl.InternalId }, Recursive = true, IsVirtualItem = false }).Select(i => i.InternalId).ToHashSet();
                        var toAdd = desiredIds.Where(id => !currentMembers.Contains(id)).ToList();
                        var toRemove = currentMembers.Where(id => !desiredIds.Contains(id)).ToList();
                        if (toAdd.Count > 0) await _collectionManager.AddToCollection(existingColl.InternalId, toAdd.ToArray());
                        if (toRemove.Count > 0 && existingColl is BoxSet boxSet) _collectionManager.RemoveFromCollection(boxSet, toRemove.ToArray());
                        collResult = toAdd.Count + toRemove.Count;
                        _collItemsAdded = toAdd.Count;
                        _collItemsRemoved = toRemove.Count;
                    }
                }
                catch (Exception ex)
                {
                    LogSummary($"  ! Collection error: {ex.Message}", "Warn");
                    LastRunStatus = $"Success ({DateTime.Now:HH:mm})";
                    return (true, $"{matchedLocalItems.Count} matched, {tagsAdded}↑ {tagsRemoved}↓ tags — collection error: {ex.Message}");
                }
            }

            // Manage home sections for this entry
            var _singleGs = new GroupRunStats
            {
                TagName = tagName,
                EnableHomeSection = tagConfig.EnableHomeSection
            };
            if (!dryRun && tagConfig.EnableHomeSection)
                ManageHomeSections(config, cancellationToken, debug, new List<GroupRunStats> { _singleGs }, tagName);

            var elapsed = DateTime.Now - startTime;
            string elapsedStr = elapsed.TotalMinutes >= 1
                ? $"{(int)elapsed.TotalMinutes}m {elapsed.Seconds}s"
                : $"{(int)elapsed.TotalSeconds}s";
            string finalStatus = dryRun ? "Dry Run" : "Success";

            LogSummary("");
            LogSummary($"[1/1] {_displayName}  ({_srcLabel})");
            if (_srcLabel == "MediaInfo")
                LogSummary($"  Scanned: {_listCount} items · {matchedLocalItems.Count} matched");
            else if (_srcLabel == "LocalCollection" || _srcLabel == "LocalPlaylist")
                LogSummary($"  Source: {_listCount} items · {matchedLocalItems.Count} matched");
            else
                LogSummary($"  List: {_listCount} objects · {matchedLocalItems.Count} matched in library");
            if (tagConfig.EnableTag && !tagConfig.OnlyCollection)
                LogSummary($"  Tag: +{tagsAdded} added, -{tagsRemoved} removed");
            if (tagConfig.EnableCollection)
                LogSummary(_collCreated
                    ? $"  Collection: created ({_collItemsAdded} items)"
                    : $"  Collection: updated (+{_collItemsAdded}, -{_collItemsRemoved})");
            if (tagConfig.EnableHomeSection)
                LogSummary(_singleGs.HomeSectionSynced
                    ? $"  Home section: synced for {_singleGs.HomeSectionUserCount} user(s)"
                    : "  Home section: not synced");
            LogSummary("");

            LogSummary("══════════════════════════════════════════════════");
            LogSummary("Summary");
            if (tagConfig.EnableTag && !tagConfig.OnlyCollection)
                LogSummary($"  Tags:          +{tagsAdded} added,  -{tagsRemoved} removed");
            if (tagConfig.EnableCollection)
                LogSummary($"  Collections:   {(_collCreated ? 1 : 0)} created,   {(!_collCreated && collResult > 0 ? 1 : 0)} updated");
            if (tagConfig.EnableHomeSection)
                LogSummary($"  Home sections: {(_singleGs.HomeSectionSynced ? 1 : 0)} synced");
            LogSummary($"  Done in {elapsedStr}  ·  {finalStatus}");
            LogSummary("══════════════════════════════════════════════════");

            var parts = new List<string> { $"{matchedLocalItems.Count} matched" };
            if (tagsAdded > 0 || tagsRemoved > 0) parts.Add($"{tagsAdded}↑ {tagsRemoved}↓ tags");
            if (collResult > 0) parts.Add("collection updated");
            if (dryRun) parts.Add("(dry run)");
            var summary = string.Join(", ", parts);
            LastRunStatus = $"Success ({DateTime.Now:HH:mm})";
            return (true, summary);
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

                if (!tc.EnableHomeSection || !isActive)
                {
                    if (tc.HomeSectionTracked.Count > 0)
                    {
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
                                LogSummary($"  ! {_hsDisplayName}  ·  failed to remove home section: {ex.Message}", "Warn");
                            }
                        }
                        if (_removedHs > 0)
                        {
                            var _gsR = statsList?.FirstOrDefault(s => s.TagName != null && string.Equals(s.TagName, _hsTagName, StringComparison.OrdinalIgnoreCase));
                            if (_gsR != null) _gsR.HomeSectionRemoved = true;
                            LogSummary($"  ~ {_hsDisplayName}  ·  home section removed  ({_removedHs} user{(_removedHs == 1 ? "" : "s")})");
                        }
                        tc.HomeSectionTracked.Clear();
                        configChanged = true;
                    }
                    continue;
                }

                var hsKey = (tc.Name ?? "") + "\x1F" + (tc.Tag ?? "");
                if (!processedHsKeys.Add(hsKey))
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

                settingsDict.TryGetValue("SectionType", out var sectionType);

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
                                LogSummary($"  ! {_hsDisplayName}  ·  collection '{tc.CollectionName}' not found", "Warn");
                        }
                    }
                    else if (!string.IsNullOrEmpty(tc.HomeSectionLibraryId))
                    {
                        resolvedLibraryId = tc.HomeSectionLibraryId;
                    }

                    if (string.IsNullOrEmpty(resolvedLibraryId))
                    {
                        LogSummary($"  ! {_hsDisplayName}  ·  no collection found — home section skipped", "Warn");
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
                        LogSummary($"  ! {_hsDisplayName}  ·  tag not found in library, section may be empty", "Warn");
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
                if (debug && tc.HomeSectionUserIds.Count > 0)
                    LogDebug($"── [{_hsDisplayName}]  Home sections ───────────────");
                foreach (var userId in tc.HomeSectionUserIds)
                {
                    try
                    {
                        var userInternalId = _userManager.GetInternalId(userId);

                        var tracked = tc.HomeSectionTracked.FirstOrDefault(t => t.UserId == userId);

                        string trackId = sectionMarker;

                        // Hämta alla sektioner en gång — återanvänds för både ID-sökning och markör-fallback
                        var currentSections = _userManager.GetHomeSections(userInternalId, cancellationToken);
                        var allCurrentSections = currentSections?.Sections ?? Array.Empty<ContentSection>();

                        // Hitta vår sektion: 1) via spårat ID, 2) via markör i Subtitle (skyddar mot att Emby tilldelar nytt ID vid omordning)
                        ContentSection? ownedSection = null;
                        if (tracked != null && !string.IsNullOrEmpty(tracked.SectionId) && !tracked.SectionId.StartsWith("hsc__"))
                            ownedSection = allCurrentSections.FirstOrDefault(s => s.Id == tracked.SectionId);
                        if (ownedSection == null && !string.IsNullOrEmpty(sectionMarker))
                            ownedSection = allCurrentSections.FirstOrDefault(s => s.Subtitle == sectionMarker);

                        if (ownedSection != null)
                        {
                            try
                            {
                                // Hämta befintlig sektion som bas — plugin-inställningar appliceras ovanpå utan att nollställa Emby-egna värden
                                var updateSection = BuildContentSection(settingsDict, resolvedLibraryId, ownedSection);
                                typeof(ContentSection).GetProperty("Id")?.SetValue(updateSection, ownedSection.Id);
                                _userManager.UpdateHomeSection(userInternalId, updateSection, cancellationToken);
                                trackId = ownedSection.Id ?? sectionMarker;
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
                            _userManager.AddHomeSection(userInternalId, BuildContentSection(settingsDict, resolvedLibraryId), cancellationToken);
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
                            LogDebug($"  → {_hsUserName}");
                        }
                    }
                    catch (Exception ex)
                    {
                        string _hsUserName2 = Guid.TryParse(userId, out var _hsGuid2)
                            ? (_userManager.GetUserById(_hsGuid2)?.Name ?? userId)
                            : userId;
                        LogSummary($"  ! {_hsDisplayName}  ·  home section failed for {_hsUserName2}: {ex.Message}", "Warn");
                    }
                }
                if (_hsSynced > 0)
                {
                    var _gsS = statsList?.FirstOrDefault(s => s.TagName != null && string.Equals(s.TagName, _hsTagName, StringComparison.OrdinalIgnoreCase));
                    if (_gsS != null) { _gsS.HomeSectionSynced = true; _gsS.HomeSectionUserCount = _hsSynced; }
                    LogSummary($"  ~ {_hsDisplayName}  ·  home section synced  ({_hsSynced} user{(_hsSynced == 1 ? "" : "s")})");
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

        private ContentSection BuildContentSection(Dictionary<string, string> settings, string libraryId, ContentSection existing = null)
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
                        ? _jsonSerializer.DeserializeFromString<string[]>(arrVal)
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

                    // Specialfall: _queryIsPlayed → IsPlayed (bool?); tomt = Any = null
                    if (settings.TryGetValue("_queryIsPlayed", out var qIsPlayed))
                    {
                        if (!string.IsNullOrEmpty(qIsPlayed) && bool.TryParse(qIsPlayed, out var isPlayedBool))
                            extQuery.IsPlayed = isPlayedBool;
                        else
                            extQuery.IsPlayed = null;
                    }

                    // Generisk _query* → övriga ItemsQuery-properties
                    foreach (var key in settings.Keys.Where(k =>
                        k.StartsWith("_query", StringComparison.OrdinalIgnoreCase) &&
                        !string.Equals(k, "_queryTagId", StringComparison.OrdinalIgnoreCase) &&
                        !string.Equals(k, "_queryIsPlayed", StringComparison.OrdinalIgnoreCase)))
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

        private bool IsScheduleActive(List<DateInterval> intervals)
        {
            if (intervals == null || intervals.Count == 0) return true;
            var now = DateTime.Now;
            foreach (var interval in intervals)
            {
                bool match = false;
                if (interval.Type == "Weekly") { if (!string.IsNullOrEmpty(interval.DayOfWeek) && interval.DayOfWeek.IndexOf(now.DayOfWeek.ToString(), StringComparison.OrdinalIgnoreCase) >= 0) match = true; }
                else if (interval.Type == "EveryYear") { if (interval.Start.HasValue && interval.End.HasValue) { var sDay = Math.Min(interval.Start.Value.Day, DateTime.DaysInMonth(now.Year, interval.Start.Value.Month)); var eDay = Math.Min(interval.End.Value.Day, DateTime.DaysInMonth(now.Year, interval.End.Value.Month)); var s = new DateTime(now.Year, interval.Start.Value.Month, sDay); var e = new DateTime(now.Year, interval.End.Value.Month, eDay); if (e < s) e = e.AddYears(1); if (now.Date >= s.Date && now.Date <= e.Date) match = true; } }
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
            Dictionary<(Guid, long), DateTimeOffset?> cache)
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
                var ud = _userDataManager?.GetUserData(user, ep);
                if (ud?.LastPlayedDate.HasValue == true && (maxDate == null || ud.LastPlayedDate > maxDate))
                    maxDate = ud.LastPlayedDate;
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
            Dictionary<string, HashSet<long>>? collectionMembershipCache = null)
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

            if (hasFilters)
            {
                bool EvalCrit(string c) => EvaluateCriterion(c, itemToCheck, is4k, is1080, is720, is8k, isSd,
                    isHevc, isAv1, isH264, isHdr, isHdr10, isDv, isAtmos, isTrueHd, isDtsHdMa, isDts,
                    isAc3, isAac, is51, is71, isStereo, isMono, personCache, audioLanguages, mediaType, itemTags,
                    userDataCache, cachedDateModifiedDays, cachedFileSizeMb, preloadedUsers, seriesLastPlayedCache,
                    cachedBitRate, cachedSampleRate, cachedBitsPerSample, cachedTrackNumber, cachedDiscNumber,
                    collectionMembershipCache);
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
                    cachedDiscNumber, collectionMembershipCache))
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
            Dictionary<string, HashSet<long>>? collectionMembershipCache = null)
        {
            bool negate = cond.Length > 0 && cond[0] == '!';
            if (negate) cond = cond.Substring(1);
            bool evalResult = EvaluateCriterionCore(cond);
            return negate ? !evalResult : evalResult;

            bool EvaluateCriterionCore(string c)
            {
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
                    "EpisodeTitle"  => SplitCommaValues(val).Any(v => MatchesEpisodeTitle(item, v, false)),
                    "Overview"      => SplitCommaValues(val).Any(v => item.Overview?.IndexOf(v, StringComparison.OrdinalIgnoreCase) >= 0),
                    "ContentRating" => SplitCommaValues(val).Any(v => string.Equals(item.OfficialRating, v, StringComparison.OrdinalIgnoreCase)),
                    "AudioLanguage" => audioLanguages != null && SplitCommaValues(val).Any(v => audioLanguages.Contains(v)),
                    "MediaType"     => val.Equals("EpisodeIncludeSeries", StringComparison.OrdinalIgnoreCase)
                                        ? item.GetType().Name.Contains("Episode")
                                        : string.Equals(mediaType, val, StringComparison.OrdinalIgnoreCase),
                    "Tag"           => itemTags != null && SplitCommaValues(val).Any(v => MatchesAny(itemTags, v)),
                    "ImdbId"        => MatchesImdbId(item.GetProviderId("Imdb"), val),
                    "Artist"        => SplitCommaValues(val).Any(v => MatchesArtistOrAlbumArtist(item, v, false)),
                    "Album"         => SplitCommaValues(val).Any(v => MatchesAlbumTitle(item, v, false)),
                    "Collection"    => collectionMembershipCache != null && SplitCommaValues(val).Any(n => collectionMembershipCache.TryGetValue("Collection:" + n, out var cIds) && cIds.Contains(item.InternalId)),
                    "Playlist"      => collectionMembershipCache != null && SplitCommaValues(val).Any(n => collectionMembershipCache.TryGetValue("Playlist:" + n, out var pIds) && pIds.Contains(item.InternalId)),
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
                                lpDate = seriesLastPlayedCache != null ? GetSeriesLastPlayed(u, item, seriesLastPlayedCache) : null;
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
                        ? GetSeriesLastPlayed(_userManager.GetUserById(guid4)!, item, seriesLastPlayedCache)
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
                    "EpisodeTitle"  => SplitCommaValues(tVal).Any(v => MatchesEpisodeTitle(item, v, exact)),
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
                    "Actor"         => exact ? personCache != null && SplitCommaValues(tVal).Any(n => personCache.TryGetValue("Actor:exact:" + n, out var aIds3) && aIds3.Contains(item.InternalId))
                                             : SplitCommaValues(tVal).Any(n => MatchesPerson(item, n, "Actor")),
                    "Director"      => exact ? personCache != null && SplitCommaValues(tVal).Any(n => personCache.TryGetValue("Director:exact:" + n, out var dIds3) && dIds3.Contains(item.InternalId))
                                             : SplitCommaValues(tVal).Any(n => MatchesPerson(item, n, "Director")),
                    "Writer"        => exact ? personCache != null && SplitCommaValues(tVal).Any(n => personCache.TryGetValue("Writer:exact:" + n, out var wIds3) && wIds3.Contains(item.InternalId))
                                             : SplitCommaValues(tVal).Any(n => MatchesPerson(item, n, "Writer")),
                    "Artist"        => SplitCommaValues(tVal).Any(v => MatchesArtistOrAlbumArtist(item, v, exact)),
                    "Album"         => SplitCommaValues(tVal).Any(v => MatchesAlbumTitle(item, v, exact)),
                    _ => false
                };
            }
            if (parts.Length == 3 && double.TryParse(parts[2],
                System.Globalization.NumberStyles.Any,
                System.Globalization.CultureInfo.InvariantCulture, out var num))
            {
                double? v = parts[0] switch
                {
                    "CommunityRating" => (double?)item.CommunityRating,
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
            val.Split(',').Select(v => v.Trim()).Where(v => v.Length > 0).ToArray();

        private static bool MatchesImdbId(string? itemImdb, string val) =>
            !string.IsNullOrEmpty(itemImdb) &&
            val.Split(',').Any(id => string.Equals(itemImdb, id.Trim(), StringComparison.OrdinalIgnoreCase));

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

        private bool MatchesEpisodeTitle(BaseItem item, string val, bool exact)
        {
            Func<string?, bool> matches = exact
                ? (n => string.Equals(n, val, StringComparison.OrdinalIgnoreCase))
                : (n => n?.IndexOf(val, StringComparison.OrdinalIgnoreCase) >= 0);

            var typeName = item.GetType().Name;

            if (typeName.Contains("Movie"))   return false;
            if (typeName.Contains("Episode")) return matches(item.Name);
            if (typeName.Contains("Series"))
            {
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

        private void LogSummary(string message, string level = "Info")
        {
            var msg = $"[{DateTime.Now:HH:mm:ss}] {message}";
            lock (ExecutionLog) { ExecutionLog.Add(msg); }
            if (level == "Error") _logger.Error(message); else if (level == "Warn") _logger.Warn(message); else _logger.Info(message);
        }

        private void LogDebug(string message)
        {
            var msg = $"[{DateTime.Now:HH:mm:ss}] [DEBUG] {message}";
            lock (ExecutionLog) { ExecutionLog.Add(msg); }
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

                var allLibraryItems = _libraryManager.GetItemList(new InternalItemsQuery
                {
                    IncludeItemTypes = new[] { "Movie", "Series" },
                    Recursive = true,
                    IsVirtualItem = false
                });

                var playedItems = allLibraryItems
                    .Select(item => new { item, ud = _userDataManager?.GetUserData(user, item) })
                    .Where(x => x.ud?.Played == true)
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
                if (debug) LogDebug($"Applied poster to '{cName}'");
            }

            if (metaChanged)
                _libraryManager.UpdateItem(item, item.Parent, ItemUpdateType.MetadataEdit, null);
        }
    }
}