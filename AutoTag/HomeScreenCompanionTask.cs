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
                LogSummary($"Home Screen Companion v{Plugin.Instance.Version}  ·  {startTime:yyyy-MM-dd HH:mm}");
                if (dryRun) LogSummary("! DRY RUN MODE — no changes will be saved");

                var allItems = _libraryManager.GetItemList(new InternalItemsQuery
                {
                    IncludeItemTypes = new[] { "Movie", "Series" },
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
                LogSummary($"Library: {movieCount} movies, {seriesCount} series");

                int activeRuleCount = config.Tags.Count(t => t.Active && !string.IsNullOrWhiteSpace(t.Tag));
                LogSummary($"Processing {activeRuleCount} active rule(s)...");

                var fetcher = new ListFetcher(_httpClient, _jsonSerializer);
                var desiredTagsMap = new Dictionary<Guid, HashSet<string>>();
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
                    var allPersonCriteria = config.Tags
                        .Where(t => t.Active && t.SourceType == "MediaInfo")
                        .SelectMany(t => (t.MediaInfoFilters ?? new List<MediaInfoFilter>())
                            .SelectMany(f => f.Criteria ?? new List<string>())
                            .Concat(t.MediaInfoConditions ?? new List<string>()))
                        .Select(c => c.Length > 0 && c[0] == '!' ? c.Substring(1) : c)
                        .Distinct(StringComparer.OrdinalIgnoreCase);
                    foreach (var c in allPersonCriteria)
                    {
                        if (personCache.ContainsKey(c)) continue;
                        var p = c.Split(':');
                        if ((p.Length == 2 || (p.Length == 3 && (p[1] == "exact" || p[1] == "contains")))
                            && (p[0] == "Actor" || p[0] == "Director" || p[0] == "Writer")
                            && Enum.TryParse<MediaBrowser.Model.Entities.PersonType>(p[0], out var personTypeEnum))
                        {
                            string matchOp = p.Length == 3 ? p[1] : "exact";
                            string personName = p.Length == 3 ? p[2].Trim() : p[1].Trim();
                            if (matchOp == "contains") continue;
                            var personItem = _libraryManager.GetItemList(new InternalItemsQuery
                            {
                                IncludeItemTypes = new[] { "Person" },
                                Name = personName
                            }).FirstOrDefault();
                            personCache[c] = personItem == null ? new HashSet<long>() :
                                _libraryManager.GetItemList(new InternalItemsQuery
                                {
                                    PersonIds = new[] { personItem.InternalId },
                                    PersonTypes = new[] { personTypeEnum },
                                    IncludeItemTypes = new[] { "Movie", "Series" },
                                    Recursive = true,
                                    IsVirtualItem = false
                                }).Select(x => x.InternalId).ToHashSet();
                        }
                    }
                }

                var mediaInfoCache = new Dictionary<long, CachedMediaInfo>();
                if (config.Tags.Any(t => t.Active && t.SourceType == "MediaInfo"))
                {
                    foreach (var item in allItems)
                    {
                        if (item.LocationType != LocationType.FileSystem) continue;
                        var resolved = ResolveItemForMediaInfo(item, seriesEpisodeCache);
                        mediaInfoCache[item.InternalId] = ExtractMediaInfo(resolved);
                    }
                }

                var userDataCache = new Dictionary<(Guid, long), (bool Played, DateTimeOffset? LastPlayedDate, int PlayCount)>();

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

                foreach (var tagConfig in config.Tags)
                {
                    if (string.IsNullOrWhiteSpace(tagConfig.Tag) || !tagConfig.Active) continue;

                    string tagName = tagConfig.Tag.Trim();
                    string displayName = !string.IsNullOrWhiteSpace(tagConfig.Name) ? $"{tagConfig.Name} [{tagName}]" : tagName;
                    string srcLabel = string.IsNullOrEmpty(tagConfig.SourceType) ? "External" : tagConfig.SourceType;
                    managedTags.Add(tagName);

                    if (!IsScheduleActive(tagConfig.ActiveIntervals))
                    {
                        LogSummary($"  ~ {displayName}  ·  skipped (out of schedule)");
                        continue;
                    }

                    string cName = string.IsNullOrWhiteSpace(tagConfig.CollectionName) ? tagName : tagConfig.CollectionName.Trim();

                    if (!tagConfig.OverrideWhenActive &&
                        (activeTagOverrides.Contains(tagName) ||
                         (tagConfig.EnableCollection && activeCollectionOverrides.Contains(cName))))
                    {
                        LogSummary($"  ~ {displayName}  ·  suppressed (overridden by priority entry)");
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
                        int effectiveLimit = tagConfig.Limit <= 0 ? 10000 : tagConfig.Limit;
                        var blacklist = new HashSet<string>(tagConfig.Blacklist ?? new List<string>(), StringComparer.OrdinalIgnoreCase);
                        var matchedLocalItems = new List<BaseItem>();
                        int matchCount = 0;

                        if (string.IsNullOrEmpty(tagConfig.SourceType) || tagConfig.SourceType == "External")
                        {
                            var items = await fetcher.FetchItems(tagConfig.Url, effectiveLimit, config.TraktClientId, config.MdblistApiKey, cancellationToken);

                            if (items.Count > 0)
                            {
                                if (items.Count > effectiveLimit) items = items.Take(effectiveLimit).ToList();

                                foreach (var extItem in items)
                                {
                                    if (string.IsNullOrEmpty(extItem.Imdb)) continue;

                                    if (blacklist.Contains(extItem.Imdb))
                                    {
                                        if (debug) LogDebug($"[BL] {extItem.Name} ({extItem.Imdb}) — blacklisted, skipping");
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
                            if (debug) LogDebug($"{displayName}: {items.Count} fetched from API, {matchedLocalItems.Count} in library");
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
                                    if (debug) LogDebug($"{displayName}: found '{localSourceFolder.Name}' ({localSourceFolder.GetType().Name})");

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

                                    if (children.Count == 0)
                                    {
                                        LogSummary($"  ! {displayName}  ·  '{tagConfig.LocalSourceId}' is empty or virtual", "Warn");
                                    }
                                    else if (debug)
                                    {
                                        LogDebug($"{displayName}: {children.Count} items in '{tagConfig.LocalSourceId}'");
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

                                        if (!itemToTag.GetType().Name.Contains("Movie") && !itemToTag.GetType().Name.Contains("Series"))
                                            continue;

                                        var imdb = itemToTag.GetProviderId("Imdb");
                                        if (!string.IsNullOrEmpty(imdb) && blacklist.Contains(imdb))
                                        {
                                            if (debug) LogDebug($"[BL] {itemToTag.Name} ({imdb}) — blacklisted, skipping");
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
                        else if (tagConfig.SourceType == "MediaInfo")
                        {
                            foreach (var item in allItems)
                            {
                                if (item.LocationType != LocationType.FileSystem) continue;

                                var imdb = item.GetProviderId("Imdb");
                                if (!string.IsNullOrEmpty(imdb) && blacklist.Contains(imdb)) continue;

                                CachedMediaInfo? ci = mediaInfoCache.TryGetValue(item.InternalId, out var ciVal) ? ciVal : (CachedMediaInfo?)null;
                                if (ItemMatchesMediaInfo(item, tagConfig, debug, seriesEpisodeCache, personCache, userDataCache, ci))
                                {
                                    matchedLocalItems.Add(item);
                                    if (effectiveLimit < 10000 && matchedLocalItems.Count >= effectiveLimit) break;
                                }
                            }
                            if (debug) LogDebug($"{displayName}: {allItems.Count} items scanned, {matchedLocalItems.Count} matched [MediaInfo]");
                        }

                        if (matchedLocalItems.Count > 0)
                        {
                            foreach (var localItem in matchedLocalItems)
                            {
                                matchCount++;
                                if (tagConfig.EnableTag && !tagConfig.OnlyCollection)
                                {
                                    if (!desiredTagsMap.ContainsKey(localItem.Id))
                                        desiredTagsMap[localItem.Id] = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                                    desiredTagsMap[localItem.Id].Add(tagName);

                                    var imdb = localItem.GetProviderId("Imdb");
                                    if (!string.IsNullOrEmpty(imdb) && tagConfig.SourceType != "External")
                                    {
                                        TagCacheManager.Instance.AddToCache($"imdb_{imdb}", tagName);
                                    }
                                }

                                if (tagConfig.EnableCollection)
                                {
                                    if (!desiredCollectionsMap.ContainsKey(cName))
                                        desiredCollectionsMap[cName] = new HashSet<long>();
                                    desiredCollectionsMap[cName].Add(localItem.InternalId);
                                }
                            }
                            LogSummary($"  + {displayName}  ·  {matchCount} matched  [{srcLabel}]");
                        }
                        else
                        {
                            LogSummary($"  - {displayName}  ·  0 matched  [{srcLabel}]");
                        }
                    }
                    catch (Exception ex)
                    {
                        LogSummary($"  ! {displayName}  ·  Error: {ex.Message}", "Error");
                        failedFetches.Add(tagName);
                        if (tagConfig.EnableCollection) failedFetches.Add(cName);
                    }

                    currentProgress += step;
                    progress.Report(currentProgress);
                }

                if (!dryRun)
                {
                    TagCacheManager.Instance.Save();
                    SaveFileHistory("homescreencompanion_history.txt", managedTags.ToList());
                }

                int tagsAdded = 0, tagsRemoved = 0, itemsChanged = 0, updateCount = 0;
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
                        foreach (var t in toAdd) LogDebug($"  +tag '{t}' → '{item.Name}'");
                        foreach (var t in toRemove) LogDebug($"  -tag '{t}' → '{item.Name}'");
                    }
                    if (!dryRun)
                    {
                        foreach (var t in toRemove) { item.RemoveTag(t); tagsRemoved++; }
                        foreach (var t in toAdd) { item.AddTag(t); tagsAdded++; }
                        try { _libraryManager.UpdateItem(item, item.Parent, ItemUpdateType.MetadataEdit, null); }
                        catch (Exception ex) { LogSummary($"  ! Failed to save tags for '{item.Name}': {ex.Message}", "Warn"); }
                        if (++updateCount % 25 == 0)
                            await Task.Yield();
                    }
                    else
                    {
                        tagsAdded += toAdd.Count; tagsRemoved += toRemove.Count;
                    }
                }
                if (tagsAdded > 0 || tagsRemoved > 0)
                    LogSummary($"Tags: +{tagsAdded} added, -{tagsRemoved} removed  ({itemsChanged} items)");
                else
                    LogSummary("Tags: no changes");

                int collCreated = 0, collUpdated = 0;
                foreach (var kvp in desiredCollectionsMap)
                {
                    string cName = kvp.Key;
                    var desiredIds = kvp.Value;
                    if (desiredIds.Count == 0) continue;

                    var existingColl = _libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "BoxSet" }, Name = cName, Recursive = true }).FirstOrDefault();

                    if (existingColl == null)
                    {
                        if (dryRun) continue;
                        var createdRef = await _collectionManager.CreateCollection(new CollectionCreationOptions { Name = cName, IsLocked = false, ItemIdList = desiredIds.ToArray() });
                        if (createdRef != null)
                        {
                            collCreated++;
                            if (debug) LogDebug($"Created collection '{cName}'  ({desiredIds.Count} items)");
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
                            if (debug) LogDebug($"Collection '{cName}': +{toAdd.Count} added, -{toRemove.Count} removed");
                        }
                        if (!dryRun && (collectionDescriptions.ContainsKey(cName) || collectionPosters.ContainsKey(cName)))
                            ApplyCollectionMeta(existingColl, cName, collectionDescriptions, collectionPosters, debug);
                    }
                }
                if (collCreated > 0 || collUpdated > 0)
                    LogSummary($"Collections: {collCreated} created, {collUpdated} updated");

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

                    var coll = _libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "BoxSet" }, Name = oldName, Recursive = true }).FirstOrDefault();
                    if (coll != null && !dryRun)
                    {
                        _libraryManager.DeleteItem(coll, new DeleteOptions { DeleteFileLocation = false });
                        collDeleted++;
                        if (debug) LogDebug($"Deleted collection '{oldName}'  (not active/scheduled)");
                    }
                }
                if (collDeleted > 0)
                    LogSummary($"Cleanup: {collDeleted} collection(s) removed");

                if (!dryRun) SaveFileHistory("homescreencompanion_collections.txt", activeCollections.ToList());

                if (!dryRun) ManageHomeSections(config, cancellationToken);

                progress.Report(100);
                var elapsed = DateTime.Now - startTime;
                string elapsedStr = elapsed.TotalMinutes >= 1
                    ? $"{(int)elapsed.TotalMinutes}m {elapsed.Seconds}s"
                    : $"{(int)elapsed.TotalSeconds}s";
                string finalStatus = dryRun ? "Dry Run" : "Success";
                LastRunStatus = $"{finalStatus} ({DateTime.Now:HH:mm})";
                LogSummary($"Done in {elapsedStr}  ·  {finalStatus}");
            }
            catch (Exception ex)
            {
                LastRunStatus = $"Failed: {ex.Message}";
                LogSummary($"CRITICAL ERROR: {ex.Message}", "Error");
            }
            finally { IsRunning = false; }
        }

        private void ManageHomeSections(PluginConfiguration config, CancellationToken cancellationToken)
        {
            bool configChanged = false;
            var processedHsKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var tc in config.Tags)
            {
                bool isActive = tc.Active && IsScheduleActive(tc.ActiveIntervals);

                if (tc.HomeSectionTracked == null)
                    tc.HomeSectionTracked = new List<HomeSectionTracking>();

                var safeTag = string.Concat((tc.Tag ?? tc.Name ?? "").Take(40)
                    .Select(c => char.IsLetterOrDigit(c) || c == '-' || c == '_' ? c : '_'));
                var sectionMarker = "hsc__" + safeTag;

                if (!tc.EnableHomeSection || !isActive)
                {
                    if (tc.HomeSectionTracked.Count > 0)
                    {
                        foreach (var tracking in tc.HomeSectionTracked)
                        {
                            try
                            {
                                var uid = _userManager.GetInternalId(tracking.UserId);
                                DeleteSectionForUser(uid, tracking.SectionId, sectionMarker, tc.HomeSectionSettings, cancellationToken);
                            }
                            catch (Exception ex)
                            {
                                LogSummary($"  [HomeSection] Failed to remove section for user {tracking.UserId}: {ex.Message}", "Warn");
                            }
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
                                LogSummary($"  [HomeSection] Collection '{tc.CollectionName}' not found for entry '{tc.Name}'.", "Warn");
                        }
                    }
                    else if (!string.IsNullOrEmpty(tc.HomeSectionLibraryId))
                    {
                        resolvedLibraryId = tc.HomeSectionLibraryId;
                    }

                    if (string.IsNullOrEmpty(resolvedLibraryId))
                    {
                        LogSummary($"  [HomeSection] Entry '{tc.Name}' is boxset type but no collection ParentId resolved. Skipping.", "Warn");
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
                        LogSummary($"  [HomeSection] Tag '{tc.Tag}' not found in Emby library — section will have no tag filter.", "Warn");
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

                foreach (var userId in tc.HomeSectionUserIds)
                {
                    try
                    {
                        var userInternalId = _userManager.GetInternalId(userId);

                        var tracked = tc.HomeSectionTracked.FirstOrDefault(t => t.UserId == userId);
                        if (tracked != null)
                            DeleteSectionForUser(userInternalId, tracked.SectionId, sectionMarker, tc.HomeSectionSettings, cancellationToken);

                        var beforeSections = _userManager.GetHomeSections(userInternalId, cancellationToken);
                        var beforeIds = new HashSet<string>(
                            (beforeSections?.Sections ?? Array.Empty<ContentSection>())
                                .Where(s => !string.IsNullOrEmpty(s.Id))
                                .Select(s => s.Id));

                        var newSection = BuildContentSection(settingsDict, resolvedLibraryId);
                        _userManager.AddHomeSection(userInternalId, newSection, cancellationToken);

                        var afterSections = _userManager.GetHomeSections(userInternalId, cancellationToken);
                        var newId = (afterSections?.Sections ?? Array.Empty<ContentSection>())
                            .Where(s => !string.IsNullOrEmpty(s.Id) && !beforeIds.Contains(s.Id))
                            .Select(s => s.Id)
                            .FirstOrDefault() ?? "";

                        var trackId = !string.IsNullOrEmpty(newId) ? newId : sectionMarker;
                        if (tracked != null)
                            tracked.SectionId = trackId;
                        else
                            tc.HomeSectionTracked.Add(new HomeSectionTracking { UserId = userId, SectionId = trackId });

                        configChanged = true;
                        LogSummary($"  [HomeSection] Added section for user {userId} (entry: '{tc.Name}', tracking: '{trackId}')");
                    }
                    catch (Exception ex)
                    {
                        LogSummary($"  [HomeSection] Failed for user {userId} (entry: '{tc.Name}'): {ex.Message}", "Warn");
                    }
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

        private ContentSection BuildContentSection(Dictionary<string, string> settings, string libraryId)
        {
            var section = new ContentSection();
            var props = typeof(ContentSection).GetProperties(BindingFlags.Public | BindingFlags.Instance);

            foreach (var prop in props)
            {
                if (!prop.CanWrite || prop.Name == "Id" || prop.Name == "ParentId") continue;
                if (!settings.TryGetValue(prop.Name, out var strVal) || string.IsNullOrEmpty(strVal)) continue;
                try
                {
                    var t = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
                    object converted = null;
                    if (t == typeof(string)) converted = strVal;
                    else if (t == typeof(bool)) converted = bool.Parse(strVal);
                    else if (t == typeof(int)) converted = int.Parse(strVal);
                    else if (t == typeof(long)) converted = long.Parse(strVal);
                    else if (t == typeof(DateTime)) converted = DateTime.Parse(strVal);
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

            if (settings.TryGetValue("_queryTagId", out var qTagId) && !string.IsNullOrEmpty(qTagId))
            {
                var queryProp = props.FirstOrDefault(p => p.Name == "Query");
                if (queryProp != null)
                {
                    try
                    {
                        var queryType = queryProp.PropertyType;
                        var queryObj = queryProp.GetValue(section) ?? Activator.CreateInstance(queryType);
                        var tagIdsProp = queryType.GetProperty("TagIds");
                        if (tagIdsProp != null && tagIdsProp.CanWrite && tagIdsProp.PropertyType == typeof(string[]))
                            tagIdsProp.SetValue(queryObj, new[] { qTagId });
                        if (queryProp.CanWrite)
                            queryProp.SetValue(section, queryObj);
                    }
                    catch { }
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
                            }
                        }
                        catch { }
                    }
                }

                info.DateModifiedDays = TryGetDateModified(itemToCheck);
                info.FileSizeMb = TryGetFileSize(itemToCheck);
            }
            catch { }
            return info;
        }

        private bool ItemMatchesMediaInfo(BaseItem item, TagConfig tagConfig, bool debug,
            Dictionary<long, BaseItem>? seriesEpisodeCache = null,
            Dictionary<string, HashSet<long>>? personCache = null,
            Dictionary<(Guid, long), (bool Played, DateTimeOffset? LastPlayedDate, int PlayCount)>? userDataCache = null,
            CachedMediaInfo? cachedInfo = null)
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
            }

            string mediaType = item.GetType().Name;
            string[] itemTags = item.Tags ?? Array.Empty<string>();

            if (hasFilters)
            {
                bool EvalCrit(string c) => EvaluateCriterion(c, itemToCheck, is4k, is1080, is720, is8k, isSd,
                    isHevc, isAv1, isH264, isHdr, isHdr10, isDv, isAtmos, isTrueHd, isDtsHdMa, isDts,
                    isAc3, isAac, is51, is71, isStereo, isMono, personCache, audioLanguages, mediaType, itemTags,
                    userDataCache, cachedDateModifiedDays, cachedFileSizeMb);
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
                    personCache, audioLanguages, mediaType, itemTags, userDataCache, cachedDateModifiedDays, cachedFileSizeMb))
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
            double? cachedFileSizeMb = null)
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
                    "Studio"        => MatchesAny(item.Studios, val),
                    "Genre"         => MatchesAny(item.Genres, val),
                    "Actor"         => personCache != null && personCache.TryGetValue(cond, out var aIds) && aIds.Contains(item.InternalId),
                    "Director"      => personCache != null && personCache.TryGetValue(cond, out var dIds) && dIds.Contains(item.InternalId),
                    "Writer"        => personCache != null && personCache.TryGetValue(cond, out var wIds) && wIds.Contains(item.InternalId),
                    "Title"         => item.Name?.IndexOf(val, StringComparison.OrdinalIgnoreCase) >= 0,
                    "ContentRating" => string.Equals(item.OfficialRating, val, StringComparison.OrdinalIgnoreCase),
                    "AudioLanguage" => audioLanguages != null && audioLanguages.Contains(val),
                    "MediaType"     => string.Equals(mediaType, val, StringComparison.OrdinalIgnoreCase),
                    "Tag"           => itemTags != null && MatchesAny(itemTags, val),
                    "ImdbId"        => MatchesImdbId(item.GetProviderId("Imdb"), val),
                    _ => false
                };
            }
            if (parts.Length == 4)
            {
                var prop4 = parts[0]; var userId4 = parts[1]; var op4 = parts[2]; var valStr4 = parts[3];
                if (userId4 == "__any__" || userId4 == "__all__")
                {
                    bool matchAll = userId4 == "__all__";
                    var allUsers = _userManager.GetUserList(new UserQuery { IsDisabled = false });
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
                        Func<User, bool> checkLp = u => {
                            var k = (u.Id, item.InternalId);
                            DateTimeOffset? lpDate;
                            if (userDataCache != null && userDataCache.TryGetValue(k, out var cd)) { lpDate = cd.LastPlayedDate; }
                            else {
                                var ud2 = _userDataManager?.GetUserData(u, item);
                                lpDate = ud2?.LastPlayedDate;
                                if (userDataCache != null) userDataCache[k] = ud2 == null ? (false, (DateTimeOffset?)null, 0) : (ud2.Played, ud2.LastPlayedDate, ud2.PlayCount);
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
                            else {
                                var ud2 = _userDataManager?.GetUserData(u, item);
                                playCount = ud2?.PlayCount ?? 0;
                                if (userDataCache != null) userDataCache[k] = ud2 == null ? (false, (DateTimeOffset?)null, 0) : (ud2.Played, ud2.LastPlayedDate, ud2.PlayCount);
                            }
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
                if (prop4 == "LastPlayed" && udResult.LastPlayedDate.HasValue &&
                    double.TryParse(valStr4, System.Globalization.NumberStyles.Any,
                                    System.Globalization.CultureInfo.InvariantCulture, out var days4))
                {
                    return ApplyNumericOp((DateTime.UtcNow - udResult.LastPlayedDate.Value).TotalDays, op4, days4);
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
                    "Title"         => exact ? string.Equals(item.Name, tVal, StringComparison.OrdinalIgnoreCase)
                                             : item.Name?.IndexOf(tVal, StringComparison.OrdinalIgnoreCase) >= 0,
                    "Studio"        => exact ? item.Studios != null && item.Studios.Any(s => string.Equals(s, tVal, StringComparison.OrdinalIgnoreCase))
                                             : MatchesAny(item.Studios, tVal),
                    "Genre"         => exact ? item.Genres != null && item.Genres.Any(g => string.Equals(g, tVal, StringComparison.OrdinalIgnoreCase))
                                             : MatchesAny(item.Genres, tVal),
                    "Tag"           => exact ? itemTags != null && itemTags.Any(t => string.Equals(t, tVal, StringComparison.OrdinalIgnoreCase))
                                             : itemTags != null && MatchesAny(itemTags, tVal),
                    "ContentRating" => exact ? string.Equals(item.OfficialRating, tVal, StringComparison.OrdinalIgnoreCase)
                                             : item.OfficialRating?.IndexOf(tVal, StringComparison.OrdinalIgnoreCase) >= 0,
                    "AudioLanguage" => exact ? audioLanguages != null && audioLanguages.Contains(tVal)
                                             : audioLanguages != null && audioLanguages.Any(l => l.IndexOf(tVal, StringComparison.OrdinalIgnoreCase) >= 0),
                    "Actor"         => exact ? personCache != null && personCache.TryGetValue(c, out var aIds3) && aIds3.Contains(item.InternalId)
                                             : MatchesPerson(item, tVal, "Actor"),
                    "Director"      => exact ? personCache != null && personCache.TryGetValue(c, out var dIds3) && dIds3.Contains(item.InternalId)
                                             : MatchesPerson(item, tVal, "Director"),
                    "Writer"        => exact ? personCache != null && personCache.TryGetValue(c, out var wIds3) && wIds3.Contains(item.InternalId)
                                             : MatchesPerson(item, tVal, "Writer"),
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
            lock (ExecutionLog) { ExecutionLog.Add(msg); if (ExecutionLog.Count > 200) ExecutionLog.RemoveAt(0); }
            if (level == "Error") _logger.Error(message); else if (level == "Warn") _logger.Warn(message); else _logger.Info(message);
        }

        private void LogDebug(string message)
        {
            var msg = $"[{DateTime.Now:HH:mm:ss}] [DEBUG] {message}";
            lock (ExecutionLog) { ExecutionLog.Add(msg); if (ExecutionLog.Count > 200) ExecutionLog.RemoveAt(0); }
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