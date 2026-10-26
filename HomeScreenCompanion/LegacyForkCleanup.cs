using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Library;
using MediaBrowser.Controller.Plugins;
using MediaBrowser.Model.Entities;
using MediaBrowser.Model.Logging;
using MediaBrowser.Model.Serialization;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace HomeScreenCompanion
{
    /// <summary>
    /// One-time clean-up for servers upgrading from the stoep-labs fork (4.2.0.x), whose show
    /// top-lists and "Show at the top of Collections" were replaced:
    ///
    ///   - Fork show lists gave every ranked series a tag of its own, marked that tag's item
    ///     favourite for each target user and showed a home row of favourite tags. The tags,
    ///     favourites, tag items, rows and the showtoplists/ folder are removed. The list itself
    ///     stays, as a show top-list of this version.
    ///   - "Show at the top" locked the collection's sort name to "!!! " + name (or a typed sort
    ///     title). That title moves into the source's Sort Title, where it can be changed or cleared.
    ///
    /// Only what the fork wrote is touched: the tags and rows recorded in its settings and tag items
    /// with the art it drew, and only when they still match exactly. At startup the fork's settings
    /// are read and cleared (so a later save cannot lose them); the library part waits in
    /// legacy_fork_cleanup.json until it has run.
    /// </summary>
    public class LegacyForkCleanup : IServerEntryPoint
    {
        private const string PendingFileName = "legacy_fork_cleanup.json";
        private const string SortToTopPrefix = "!!! ";
        private static readonly TimeSpan StartDelay = TimeSpan.FromMinutes(1);

        private readonly ILibraryManager _libraryManager;
        private readonly IUserManager _userManager;
        private readonly IUserDataManager _userDataManager;
        private readonly IJsonSerializer _jsonSerializer;
        private readonly PluginLogger _logger;

        public class PendingCleanup
        {
            public List<LegacyShowList> ShowLists { get; set; } = new List<LegacyShowList>();
            // Copy & Sync targets: they hold copies of the fork's rows that are not tracked.
            public List<string> CopySyncUserIds { get; set; } = new List<string>();
        }

        public class LegacyShowList
        {
            public string Name { get; set; } = "";
            public string RowName { get; set; } = "";
            public List<string> UserIds { get; set; } = new List<string>();
            public List<HomeSectionTracking> Rows { get; set; } = new List<HomeSectionTracking>();
            public List<ShowTopListEntry> Entries { get; set; } = new List<ShowTopListEntry>();
        }

        public LegacyForkCleanup(ILibraryManager libraryManager, IUserManager userManager, IUserDataManager userDataManager, IJsonSerializer jsonSerializer, ILogManager logManager)
        {
            _libraryManager = libraryManager;
            _userManager = userManager;
            _userDataManager = userDataManager;
            _jsonSerializer = jsonSerializer;
            _logger = PluginLog.Wrap(logManager.GetLogger("HomeScreenCompanion"), "Migration");
        }

        private static string PendingPath => Path.Combine(Plugin.Instance!.DataFolderPath, PendingFileName);
        private static string ShowTopListsFolder => Path.Combine(Plugin.Instance!.DataFolderPath, "showtoplists");

        public void Run()
        {
            if (Plugin.Instance == null) return;
            try
            {
                MigrateSettings();
            }
            catch (Exception ex)
            {
                _logger.Error("[Migration] Fork settings could not be migrated: " + ex.Message);
            }

            if (!File.Exists(PendingPath)) return;
            // Off the startup path, once the library is up.
            Task.Run(async () =>
            {
                await Task.Delay(StartDelay).ConfigureAwait(false);
                try { CleanLibrary(); }
                catch (Exception ex) { _logger.Error("[Migration] Fork clean-up failed: " + ex.Message); }
            });
        }

        public void Dispose() { }

        // A settings-page save posts a fresh config: keep the flag, so the clean-up never runs twice.
        internal static void KeepCleanupDone(PluginConfiguration? old, PluginConfiguration? saved)
        {
            if (old != null && saved != null && old.LegacyForkCleanupDone) saved.LegacyForkCleanupDone = true;
        }

        // Settings only (no library access): reads the fork's settings, moves the sort titles,
        // saves what the library clean-up needs and clears the fork's fields.
        private void MigrateSettings()
        {
            var config = Plugin.Instance!.Configuration;
            if (config == null || config.LegacyForkCleanupDone) return;

            var topLists = config.TopLists ?? new List<TopListHomeSection>();
            var tags = config.Tags ?? new List<TagConfig>();
            bool fromFork = config.ShowTopListsEnabled.HasValue || config.CollectionSortTitleEnabled
                || tags.Any(t => t.CollectionSortToTop || !string.IsNullOrEmpty(t.CollectionSortTitle))
                || topLists.Any(tl => (tl.ShowEntries?.Count ?? 0) > 0 || !string.IsNullOrEmpty(tl.ShowSourceTag) || (tl.ShowParkedSeriesIds?.Count ?? 0) > 0);
            if (!fromFork) return;

            var pending = new PendingCleanup();
            foreach (var tl in topLists.Where(ShowTopList.IsShows))
            {
                var settings = ReadSettings(tl.HomeSectionSettings);
                bool tagRow = settings.TryGetValue("ItemTypes", out var types) && types.Replace(" ", "") == "[\"Tag\"]";
                if ((tl.ShowEntries?.Count ?? 0) == 0 && !tagRow) continue;

                pending.ShowLists.Add(new LegacyShowList
                {
                    Name = tl.TagName ?? "",
                    RowName = settings.TryGetValue("CustomName", out var cn) && !string.IsNullOrWhiteSpace(cn) ? cn : tl.TagName ?? "",
                    UserIds = (tl.HomeSectionUserIds ?? new List<string>()).ToList(),
                    Rows = tagRow ? (tl.HomeSectionTracked ?? new List<HomeSectionTracking>()).ToList() : new List<HomeSectionTracking>(),
                    Entries = (tl.ShowEntries ?? new List<ShowTopListEntry>()).ToList()
                });

                // The fork's row was a row of favourite tags; this version's show row lists series.
                if (tagRow)
                {
                    settings["ItemTypes"] = "[\"Series\"]";
                    settings.Remove("_queryIsFavorite");
                    tl.HomeSectionSettings = _jsonSerializer.SerializeToString(settings);
                    tl.HomeSectionTracked = new List<HomeSectionTracking>();
                }
            }
            if (config.HomeSyncEnabled)
                pending.CopySyncUserIds = config.EffectiveHomeSyncProfiles().SelectMany(p => p.TargetUserIds ?? new List<string>()).Distinct().ToList();

            _logger.Info($"[Migration] Settings of the stoep-labs fork found: {pending.ShowLists.Count} show list(s) with "
                + $"{pending.ShowLists.Sum(l => l.Entries.Count)} ranked show(s), {tags.Count(t => t.CollectionSortToTop)} source(s) with \"Show at the top\"");

            foreach (var tc in tags.Where(t => t.CollectionSortToTop && t.EnableCollection))
            {
                var cName = string.IsNullOrWhiteSpace(tc.CollectionName) ? (tc.Tag ?? "").Trim() : tc.CollectionName.Trim();
                // What the fork wrote: the typed sort title when that option was on, else "!!! " + name.
                var custom = config.CollectionSortTitleEnabled ? (tc.CollectionSortTitle ?? "").Trim() : "";
                var title = custom.Length > 0 ? custom : SortToTopPrefix + cName;
                if (string.IsNullOrWhiteSpace(tc.CollectionSortName))
                {
                    tc.CollectionSortName = title;
                    _logger.Info($"[Migration] Collection '{cName}': \"Show at the top\" moved to its Sort Title \"{title}\"");
                }
                else
                    _logger.Info($"[Migration] Collection '{cName}': already has Sort Title \"{tc.CollectionSortName}\", \"Show at the top\" dropped");
            }

            if (pending.ShowLists.Count > 0 || Directory.Exists(ShowTopListsFolder))
                _jsonSerializer.SerializeToFile(pending, PendingPath);

            config.ShowTopListsEnabled = null;
            config.CollectionSortTitleEnabled = false;
            foreach (var tc in tags) { tc.CollectionSortToTop = false; tc.CollectionSortTitle = ""; }
            foreach (var tl in topLists)
            {
                tl.ShowEntries = new List<ShowTopListEntry>();
                tl.ShowSourceTag = "";
                tl.ShowParkedSeriesIds = new List<string>();
            }
            config.LegacyForkCleanupDone = true;
            Plugin.Instance.SaveConfiguration();
        }

        // Library part: favourites, tags and rows of the fork's show lists, then its tag items and folder.
        private void CleanLibrary()
        {
            var pending = _jsonSerializer.DeserializeFromFile<PendingCleanup>(PendingPath) ?? new PendingCleanup();

            foreach (var list in pending.ShowLists)
            {
                var userIds = list.UserIds.Concat(list.Rows.Select(r => r.UserId)).Where(id => !string.IsNullOrEmpty(id))
                    .Distinct(StringComparer.OrdinalIgnoreCase).ToList();
                var users = userIds.Select(id => Guid.TryParse(id, out var g) ? _userManager.GetUserById(g) : null)
                    .Where(u => u != null).Select(u => u!).ToList();

                foreach (var entry in list.Entries)
                {
                    try { CleanEntry(list.Name, entry, users); }
                    catch (Exception ex) { _logger.Warn($"[Migration] Show list '{list.Name}': tag '{entry.TagName}' could not be cleaned up — {ex.Message}"); }
                }

                foreach (var row in list.Rows)
                    DeleteRow(list, row.UserId, s => s.Id == row.SectionId);
                // Copies of the row: same name, a row of favourite tags.
                foreach (var userId in userIds.Concat(pending.CopySyncUserIds).Distinct(StringComparer.OrdinalIgnoreCase))
                    DeleteRow(list, userId, s => string.Equals(s.CustomName, list.RowName, StringComparison.Ordinal) && s.Query?.IsFavorite == true);
            }

            if (Directory.Exists(ShowTopListsFolder))
            {
                RetireTagItems();
                try
                {
                    Directory.Delete(ShowTopListsFolder, true);
                    _logger.Info("[Migration] Removed the fork's showtoplists folder");
                }
                catch (Exception ex) { _logger.Warn("[Migration] showtoplists folder could not be removed: " + ex.Message); }
            }

            try { File.Delete(PendingPath); } catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
        }

        private void CleanEntry(string listName, ShowTopListEntry entry, List<User> users)
        {
            if (string.IsNullOrEmpty(entry.TagName)) return;
            var done = new List<string>();

            var tagItem = _libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "Tag" }, Name = entry.TagName })
                .FirstOrDefault(t => string.Equals(t.Name, entry.TagName, StringComparison.Ordinal));
            if (tagItem != null)
            {
                int unmarked = 0;
                foreach (var user in users)
                {
                    var data = _userDataManager.GetUserData(user, tagItem);
                    if (data == null || !data.IsFavorite) continue;
                    data.IsFavorite = false;
                    _userDataManager.SaveUserData(user, tagItem, data, UserDataSaveReason.UpdateUserRating, CancellationToken.None);
                    unmarked++;
                }
                if (unmarked > 0) done.Add($"favourite removed for {RunLog.Plural(unmarked, "user")}");
            }

            if (Guid.TryParse(entry.SeriesId, out var seriesId) && _libraryManager.GetItemById(seriesId) is BaseItem series
                && RemoveTag(series, entry.TagName))
                done.Add($"tag removed from '{series.Name}'");

            if (done.Count > 0)
                _logger.Info($"[Migration] Show list '{listName}', tag '{entry.TagName}': {string.Join(", ", done)}");
        }

        // Every tag item the fork made, also of show lists deleted before the upgrade: it carries
        // the ranked art the fork drew into showtoplists/. Its tag comes off the series it was made
        // for (the fork named it "<series>" or "<series> · <list>"), then the item is deleted, or,
        // when other items still use the tag, keeps the tag without the fork's art and rank.
        private void RetireTagItems()
        {
            var tagItems = _libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "Tag" } }).Where(OwnsImages).ToList();
            foreach (var tagItem in tagItems)
            {
                var name = tagItem.Name ?? "";
                try
                {
                    var done = new List<string>();
                    foreach (var series in _libraryManager.GetItemList(new InternalItemsQuery { Tags = new[] { name }, IncludeItemTypes = new[] { "Series" }, Recursive = true }))
                    {
                        var seriesName = series.Name ?? "";
                        if (seriesName.Length == 0 || !(name == seriesName || name.StartsWith(seriesName + " · ", StringComparison.Ordinal))) continue;
                        if (RemoveTag(series, name)) done.Add($"tag removed from '{seriesName}'");
                    }

                    if (!_libraryManager.GetItemList(new InternalItemsQuery { Tags = new[] { name }, Recursive = true, Limit = 1 }).Any())
                    {
                        _libraryManager.DeleteItem(tagItem, new DeleteOptions { DeleteFileLocation = false });
                        done.Add("tag item deleted");
                    }
                    else
                    {
                        tagItem.ImageInfos = (tagItem.ImageInfos ?? Array.Empty<ItemImageInfo>()).Where(i => !IsForkImage(i)).ToArray();
                        tagItem.LockedFields = (tagItem.LockedFields ?? Array.Empty<MetadataFields>()).Where(f => f != MetadataFields.SortName).ToArray();
                        tagItem.SortName = null;
                        _libraryManager.UpdateItem(tagItem, tagItem.GetParent(), ItemUpdateType.ImageUpdate);
                        done.Add("tag item kept (still in use), ranked art removed");
                    }
                    _logger.Info($"[Migration] Fork tag '{name}': {string.Join(", ", done)}");
                }
                catch (Exception ex)
                {
                    _logger.Warn($"[Migration] Fork tag '{name}' could not be cleaned up — {ex.Message}");
                }
            }
        }

        // Only the exact tag the fork wrote.
        private bool RemoveTag(BaseItem item, string tag)
        {
            if (!(item.Tags ?? Array.Empty<string>()).Contains(tag, StringComparer.Ordinal)) return false;
            item.SetTags(item.Tags.Where(t => !string.Equals(t, tag, StringComparison.Ordinal)));
            _libraryManager.UpdateItem(item, item.GetParent(), ItemUpdateType.MetadataEdit);
            return true;
        }

        private static bool OwnsImages(BaseItem item) => (item.ImageInfos ?? Array.Empty<ItemImageInfo>()).Any(IsForkImage);

        private static bool IsForkImage(ItemImageInfo image)
            => !string.IsNullOrEmpty(image.Path)
               && image.Path.StartsWith(ShowTopListsFolder + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase);

        // Removes the user's rows that match and are rows of tags (the fork's row type).
        private void DeleteRow(LegacyShowList list, string userId, Func<ContentSection, bool> match)
        {
            if (string.IsNullOrEmpty(userId)) return;
            try
            {
                var internalId = _userManager.GetInternalId(userId);
                var rows = (_userManager.GetHomeSections(internalId, CancellationToken.None)?.Sections ?? Array.Empty<ContentSection>())
                    .Where(s => !string.IsNullOrEmpty(s.Id) && match(s)
                        && (s.ItemTypes ?? Array.Empty<string>()).SequenceEqual(new[] { "Tag" }, StringComparer.OrdinalIgnoreCase))
                    .ToList();
                if (rows.Count == 0) return;
                _userManager.DeleteHomeSections(internalId, rows.Select(s => s.Id).ToArray(), CancellationToken.None);
                var userName = Guid.TryParse(userId, out var g) ? _userManager.GetUserById(g)?.Name ?? userId : userId;
                _logger.Info($"[Migration] Show list '{list.Name}': removed the fork's row '{list.RowName}' for {userName}");
            }
            catch (Exception ex)
            {
                _logger.Warn($"[Migration] Show list '{list.Name}': row for user {userId} could not be removed — {ex.Message}");
            }
        }

        private Dictionary<string, string> ReadSettings(string? json)
        {
            try
            {
                if (!string.IsNullOrEmpty(json) && json != "{}")
                    return new Dictionary<string, string>(_jsonSerializer.DeserializeFromString<Dictionary<string, string>>(json!)
                        ?? new Dictionary<string, string>(), StringComparer.OrdinalIgnoreCase);
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            return new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        }
    }
}
