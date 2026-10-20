using MediaBrowser.Controller.Library;
using MediaBrowser.Controller.Providers;
using MediaBrowser.Model.Configuration;
using MediaBrowser.Model.IO;
using MediaBrowser.Model.Serialization;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

namespace HomeScreenCompanion
{
    /// <summary>
    /// A source's "Create as top-list" (TagConfig.EnableTopList, in its Home Screen tab): the source's home row is a ranked top-list
    /// instead of a plain row. The source is the only place it is edited; its TopLists entry (what
    /// the top-list engine works on: SyncTopListFolders, TopListSyncTask) is derived from it on every
    /// settings save (<see cref="Derive"/>), marked FromSource. Users, title, position, display mode,
    /// image type, card size and scroll direction come from the source's Home Screen tab; content
    /// type, max items and badges from the top-list fields above them.
    /// The list's titles come from the source's last run (TopListMembers), not from its tag, so
    /// Apply Tag can be off; the tag name is the list's key.
    /// A list switched off, renamed (its tag) or switched between movies and shows is queued
    /// (removed_toplists.xml) and its rows, library and folder are removed by the next run
    /// (<see cref="RemoveQueued"/>); a list made again before that run gets its library back.
    /// The library of a new list is made by the run (<see cref="EnsureLibraries"/>), so the source
    /// does not have to have run before its Top-List is switched on.
    /// </summary>
    internal static class SourceTopLists
    {
        internal const string Movies = "Movies";
        internal const string Shows = "Shows";

        // Set at startup (ServerEntryPoint): settings saves come in through Plugin.UpdateConfiguration.
        internal static IJsonSerializer? Json;

        private static readonly object _queueLock = new object();
        private static string QueueFile => Path.Combine(Plugin.Instance!.DataFolderPath, "removed_toplists.xml");

        /// <summary>Set when a run made a list's library (EnsureLibraries): a Run Group then hides it at once.</summary>
        internal static volatile bool LibraryAdded;

        /// <summary>The source has its own plain home row (not when its row is a top-list).</summary>
        internal static bool PlainRow(TagConfig tc) => tc.EnableHomeSection && !IsOn(tc);

        /// <summary>Create as top-list is on (never for Your Next Watch: its row is each user's own playlist).</summary>
        internal static bool IsOn(TagConfig tc) => tc.EnableTopList && !string.Equals(tc.SourceType, "NextWatch", StringComparison.OrdinalIgnoreCase);

        internal static bool IsShows(TagConfig tc) => string.Equals(tc.TopListContentType, Shows, StringComparison.OrdinalIgnoreCase);

        internal static string Sanitize(string? name)
        {
            var invalid = Path.GetInvalidFileNameChars();
            var safe = new string((name ?? "unknown").Select(c => Array.IndexOf(invalid, c) >= 0 ? '_' : c).ToArray()).Trim('.');
            return string.IsNullOrWhiteSpace(safe) ? "unknown" : safe;
        }

        private static string TagOf(TagConfig tc) => (tc.Tag ?? "").Trim();

        // The source that owns each tag's top-list: the first one (in list order) with Top-List on.
        // Apply Tag is not needed: the list is the source's matched titles (TopListMembers), and the
        // tag name (or Display Name when it is empty) is only the list's key.
        private static Dictionary<string, TagConfig> Owners(PluginConfiguration cfg)
        {
            var owners = new Dictionary<string, TagConfig>(StringComparer.OrdinalIgnoreCase);
            foreach (var tc in cfg.Tags ?? new List<TagConfig>())
            {
                var tag = TagOf(tc);
                if (!IsOn(tc) || tag.Length == 0 || owners.ContainsKey(tag)) continue;
                owners[tag] = tc;
            }
            return owners;
        }

        private static Dictionary<string, string> Parse(string? json)
        {
            var d = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            if (Json == null || string.IsNullOrWhiteSpace(json) || json == "{}") return d;
            try { return Json.DeserializeFromString<Dictionary<string, string>>(json!) is Dictionary<string, string> p ? new Dictionary<string, string>(p, StringComparer.OrdinalIgnoreCase) : d; }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); return d; }
        }

        private static string Get(Dictionary<string, string> d, string key) => d.TryGetValue(key, out var v) ? v ?? "" : "";

        /// <summary>
        /// Settings saved (Plugin.UpdateConfiguration): links old tag top-lists to their source, then
        /// makes cfg.TopLists match the sources with Top-List on. keepOrphans (a backup restore): a
        /// list whose source is not in the restored settings is kept as a Top-Lists tab list instead
        /// of being removed.
        /// </summary>
        internal static void Derive(PluginConfiguration? old, PluginConfiguration? cfg, bool keepOrphans = false)
        {
            if (cfg == null || Json == null || Plugin.Instance == null) return;
            try
            {
                // The page does not send it: done stays done.
                if (old != null && old.TopListsMovedToSources) cfg.TopListsMovedToSources = true;
                Migrate(cfg);
                cfg.TopLists ??= new List<TopListHomeSection>();
                var owners = Owners(cfg);
                // Source lists the saved settings left out altogether (written without them, e.g. over
                // the API; the page always sends the server's lists): queued like a list switched off —
                // unless their source still has Top-List on (same content type): made again below
                // with the old entry's library and rows.
                QueueVanished(old?.TopLists, cfg.TopLists, old?.Tags, "left out of the saved settings", onlyFromSource: true,
                    keep: t => owners.TryGetValue((t.TagName ?? "").Trim(), out var o) && string.Equals(ContentTypeOf(t), IsShows(o) ? Shows : Movies, StringComparison.OrdinalIgnoreCase));

                // Lists whose source no longer has one (Top-List off, source deleted, tag renamed) or
                // whose content type changed: queued for the next run, which removes what they made.
                var gone = new List<(TopListHomeSection List, string Reason, string Source)>();
                foreach (var tl in cfg.TopLists.Where(t => t.FromSource).ToList())
                {
                    bool hasOwner = owners.TryGetValue((tl.TagName ?? "").Trim(), out var owner);
                    if (hasOwner && string.Equals(ContentTypeOf(tl), IsShows(owner!) ? Shows : Movies, StringComparison.OrdinalIgnoreCase)) continue;
                    if (!hasOwner && keepOrphans)
                    {
                        // Its library, rows, folder and saved titles stay.
                        tl.FromSource = false;
                        PluginLog.Write("WRN", "Settings", $"Top-list '{tl.TagName}': no source with Create as top-list in the restored settings — kept as a Top-Lists tab list (restore its source or the Top-lists section to set it there again)");
                        continue;
                    }
                    cfg.TopLists.Remove(tl);
                    // The source it came from (also when its tag was renamed): its Run Group removes it.
                    var source = hasOwner ? owner!.Name : (old?.Tags ?? new List<TagConfig>())
                        .FirstOrDefault(t => IsOn(t) && string.Equals(TagOf(t), (tl.TagName ?? "").Trim(), StringComparison.OrdinalIgnoreCase))?.Name;
                    gone.Add((tl, hasOwner ? $"made again as {(IsShows(owner!) ? "TV shows" : "movies")} in its source"
                                           : "switched off, renamed or deleted in its source", source ?? ""));
                }
                if (gone.Count > 0) Queue(gone);

                foreach (var kv in owners)
                {
                    var tl = cfg.TopLists.FirstOrDefault(t => string.Equals((t.TagName ?? "").Trim(), kv.Key, StringComparison.OrdinalIgnoreCase));
                    if (tl == null)
                    {
                        tl = new TopListHomeSection { TagName = kv.Key, FromSource = true, HomeSectionLibraryId = "auto" };
                        var type = IsShows(kv.Value) ? Shows : Movies;
                        // Left out of the saved settings while its source still has it: the old entry's
                        // library and rows.
                        var prev = (old?.TopLists ?? new List<TopListHomeSection>()).FirstOrDefault(t => t.FromSource
                            && string.Equals((t.TagName ?? "").Trim(), kv.Key, StringComparison.OrdinalIgnoreCase)
                            && string.Equals(ContentTypeOf(t), type, StringComparison.OrdinalIgnoreCase));
                        // Made again before the run removed it: it keeps its library and rows.
                        var back = prev == null ? Unqueue(kv.Key, type) : null;
                        if (prev != null)
                        {
                            tl.HomeSectionLibraryId = string.IsNullOrEmpty(prev.HomeSectionLibraryId) ? "auto" : prev.HomeSectionLibraryId;
                            tl.HomeSectionTracked = (prev.HomeSectionTracked ?? new List<HomeSectionTracking>()).ToList();
                            tl.HomeSectionSettings = prev.HomeSectionSettings ?? "{}";
                        }
                        else if (back != null)
                        {
                            tl.HomeSectionLibraryId = string.IsNullOrEmpty(back.LibraryId) ? "auto" : back.LibraryId;
                            tl.HomeSectionTracked = back.Rows ?? new List<HomeSectionTracking>();
                            tl.HomeSectionSettings = back.HomeSectionSettings ?? "{}";
                        }
                        cfg.TopLists.Add(tl);
                    }
                    Fill(tl, kv.Value);
                }
            }
            catch (Exception ex) { PluginLog.Write("WRN", "Settings", "Top-lists of sources could not be updated — " + PluginLog.Describe(ex)); }
        }

        private static string ContentTypeOf(TopListHomeSection tl) => ShowTopList.IsShows(tl) ? Shows : Movies;

        // The source's settings into its list (the engine's fixed ones too). Kept from the list: its
        // library, tracked rows and the library exclusions the sync has collected.
        private static void Fill(TopListHomeSection tl, TagConfig src)
        {
            var s = Parse(src.HomeSectionSettings);
            var old = Parse(tl.HomeSectionSettings);
            bool shows = IsShows(src);
            var name = Get(s, "CustomName").Trim();
            if (name.Length == 0) name = HomeScreenCompanionTask.DefaultRowTitle(src);
            int max = Math.Max(0, src.TopListMaxItems);
            var cardSize = Get(s, "CardSizeOffset");

            var settings = new Dictionary<string, string>
            {
                ["SectionType"] = "items",
                ["DisplayMode"] = Get(s, "DisplayMode"),
                ["CustomName"] = name,
                ["MaxItems"] = max.ToString(),
                ["ViewType"] = "",
                ["ImageType"] = Get(s, "ImageType"),
                ["CardSizeOffset"] = cardSize.Length > 0 ? cardSize : "0",
                ["BadgeStyle"] = string.IsNullOrWhiteSpace(src.TopListBadgeStyle) ? "neutral" : src.TopListBadgeStyle,
                ["BadgeOptions"] = src.TopListBadgeOptions ?? "",
                ["RealPosterBadge"] = src.TopListRealPoster ?? "",
                ["SortBy"] = "SortName",
                ["SortOrder"] = "Ascending",
                ["ScrollDirection"] = Get(s, "ScrollDirection"),
                ["ItemTypes"] = shows ? "[\"Series\"]" : "[\"Movie\"]",
                ["_queryIsPlayed"] = "",
                ["_queryExcludeViewIds"] = Get(old, "_queryExcludeViewIds"),
                ["ExcludedFolders"] = Get(old, "ExcludedFolders"),
                ["_position"] = Get(s, "_position")
            };
            tl.HomeSectionSettings = Json!.SerializeToString(settings);
            tl.HomeSectionUserIds = src.EnableHomeSection
                ? (src.HomeSectionUserIds ?? new List<string>()).Where(u => !string.IsNullOrWhiteSpace(u)).ToList()
                : new List<string>();
            tl.MaxItems = max;
            tl.ContentType = shows ? Shows : Movies;
            tl.FromSource = true;
            tl.HomeSectionTracked ??= new List<HomeSectionTracking>();
            if (string.IsNullOrEmpty(tl.HomeSectionLibraryId)) tl.HomeSectionLibraryId = "auto";
        }

        /// <summary>
        /// Once (TopListsMovedToSources), per old tag top-list (made in the Top-Lists tab before 4.5):
        /// the first active source with its tag gets Top-List on, with the list's users, title,
        /// position and look. The list keeps its library and rows, and the source keeps Apply Tag
        /// (other sources, rules and rows may use the tag). A source that has its own home row
        /// keeps it: its list stays in the Top-Lists tab (a source has one row, plain or top-list).
        /// True when something changed.
        /// </summary>
        internal static bool Migrate(PluginConfiguration cfg)
        {
            if (Json == null || cfg.TopListsMovedToSources) return false;
            cfg.TopListsMovedToSources = true;
            foreach (var tl in (cfg.TopLists ?? new List<TopListHomeSection>()).Where(t => !t.FromSource && !string.IsNullOrWhiteSpace(t.TagName)))
            {
                var tag = tl.TagName.Trim();
                // Your Next Watch never has a top-list (IsOn): a list on its tag stays a tag list.
                var group = (cfg.Tags ?? new List<TagConfig>()).Where(t => t.Active && t.EnableTag && string.Equals(TagOf(t), tag, StringComparison.OrdinalIgnoreCase)
                    && !string.Equals(t.SourceType, "NextWatch", StringComparison.OrdinalIgnoreCase)).ToList();
                if (group.Count == 0) continue;   // a manual list, or a tag no active source writes
                // All flat entries of the first such source (one per URL / local source).
                var first = group[0];
                var key = GroupIssues.Key(first.Name, first.Tag);
                var entries = group.Where(t => GroupIssues.Key(t.Name, t.Tag) == key).ToList();
                // Another source with this tag already owns a list made with Create as top-list.
                if ((cfg.Tags ?? new List<TagConfig>()).Any(t => IsOn(t) && string.Equals(TagOf(t), tag, StringComparison.OrdinalIgnoreCase)
                    && GroupIssues.Key(t.Name, t.Tag) != key)) continue;
                // The source has its own home row: the list would replace it (and the next run would
                // remove it for its users), so the list stays as it is.
                if (entries.Any(PlainRow))
                {
                    PluginLog.Write("INF", "Settings", $"Top-list '{tag}' stays in the Top-Lists tab: its source '{(string.IsNullOrWhiteSpace(first.Name) ? tag : first.Name)}' has its own home row");
                    continue;
                }

                var ls = Parse(tl.HomeSectionSettings);
                int max = tl.MaxItems > 0 ? tl.MaxItems : (int.TryParse(Get(ls, "MaxItems"), out var m) ? Math.Max(0, m) : 0);
                foreach (var e in entries)
                {
                    var s = Parse(e.HomeSectionSettings);
                    foreach (var k in new[] { "CustomName", "DisplayMode", "ImageType", "CardSizeOffset", "ScrollDirection", "_position" })
                    {
                        var v = Get(ls, k);
                        if (v.Length > 0) s[k] = v; else s.Remove(k);
                    }
                    e.HomeSectionSettings = Json.SerializeToString(s);
                    e.EnableHomeSection = true;
                    e.HomeSectionUserIds = (tl.HomeSectionUserIds ?? new List<string>()).ToList();
                    e.EnableTopList = true;
                    e.TopListContentType = ContentTypeOf(tl);
                    e.TopListMaxItems = max;
                    var badge = Get(ls, "BadgeStyle");
                    e.TopListBadgeStyle = badge.Length > 0 ? badge : "neutral";
                    e.TopListBadgeOptions = Get(ls, "BadgeOptions");
                    e.TopListRealPoster = Get(ls, "RealPosterBadge");
                }
                tl.FromSource = true;
                PluginLog.Write("INF", "Settings", $"Top-list '{tag}' is now set in its source's Home Screen tab, Create as top-list (source '{(string.IsNullOrWhiteSpace(first.Name) ? tag : first.Name)}')");
            }
            return true;
        }

        // ── Removal queue ────────────────────────────────────────────────────────────────

        private static RemovedTopListQueue LoadQueue()
        {
            try
            {
                if (File.Exists(QueueFile))
                    return Plugin.XmlSerializer.DeserializeFromFile(typeof(RemovedTopListQueue), QueueFile) as RemovedTopListQueue ?? new RemovedTopListQueue();
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            return new RemovedTopListQueue();
        }

        private static void SaveQueue(RemovedTopListQueue q)
        {
            try
            {
                if (q.Items.Count == 0) { if (File.Exists(QueueFile)) File.Delete(QueueFile); return; }
                Directory.CreateDirectory(Path.GetDirectoryName(QueueFile)!);
                Plugin.XmlSerializer.SerializeToFile(q, QueueFile);
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
        }

        private static void Queue(List<(TopListHomeSection List, string Reason, string Source)> lists)
        {
            lock (_queueLock)
            {
                var q = LoadQueue();
                foreach (var (tl, reason, source) in lists)
                {
                    q.Items.Add(new RemovedTopList
                    {
                        TagName = (tl.TagName ?? "").Trim(),
                        ContentType = ContentTypeOf(tl),
                        LibraryId = tl.HomeSectionLibraryId == "auto" ? "" : (tl.HomeSectionLibraryId ?? ""),
                        HomeSectionSettings = tl.HomeSectionSettings ?? "{}",
                        Rows = (tl.HomeSectionTracked ?? new List<HomeSectionTracking>()).ToList(),
                        Reason = reason,
                        SourceName = source
                    });
                    PluginLog.Write("INF", "Settings", $"Top-list '{tl.TagName}' {reason}: its rows, library and copies are removed by the next run");
                }
                SaveQueue(q);
            }
        }

        /// <summary>
        /// Lists in <paramref name="before"/> that <paramref name="after"/> no longer has (by name and
        /// content type) are queued, so the next run removes their rows, library and folder — without
        /// it nothing would know they ever existed. Used by a settings save (source lists only) and by
        /// a backup restore, which replaces the lists (manual lists too). oldTags: the settings the
        /// lists came from, for the source name a Run Group removes them by.
        /// </summary>
        internal static void QueueVanished(IEnumerable<TopListHomeSection>? before, IEnumerable<TopListHomeSection>? after,
            IEnumerable<TagConfig>? oldTags, string reason, bool onlyFromSource, Func<TopListHomeSection, bool>? keep = null)
        {
            try
            {
                var kept = new HashSet<string>((after ?? Enumerable.Empty<TopListHomeSection>())
                    .Where(t => !string.IsNullOrWhiteSpace(t.TagName))
                    .Select(t => (t.TagName ?? "").Trim() + "\u001F" + ContentTypeOf(t)), StringComparer.OrdinalIgnoreCase);
                var gone = new List<(TopListHomeSection List, string Reason, string Source)>();
                foreach (var tl in before ?? Enumerable.Empty<TopListHomeSection>())
                {
                    if (string.IsNullOrWhiteSpace(tl.TagName) || (onlyFromSource && !tl.FromSource)) continue;
                    if (kept.Contains((tl.TagName ?? "").Trim() + "\u001F" + ContentTypeOf(tl)) || (keep != null && keep(tl))) continue;
                    // Nothing made yet (no library, no rows): nothing to remove.
                    bool hasLibrary = !string.IsNullOrEmpty(tl.HomeSectionLibraryId) && tl.HomeSectionLibraryId != "auto";
                    if (!hasLibrary && (tl.HomeSectionTracked ?? new List<HomeSectionTracking>()).Count == 0
                        && !Directory.Exists(Path.Combine(Plugin.Instance!.DataFolderPath, "toplists", Sanitize(tl.TagName)))) continue;
                    var source = tl.FromSource
                        ? (oldTags ?? Enumerable.Empty<TagConfig>()).FirstOrDefault(t => IsOn(t)
                            && string.Equals(TagOf(t), (tl.TagName ?? "").Trim(), StringComparison.OrdinalIgnoreCase))?.Name ?? ""
                        : "";
                    gone.Add((tl, reason, source));
                }
                if (gone.Count > 0) Queue(gone);
            }
            catch (Exception ex) { PluginLog.Write("WRN", "Settings", "Removed top-lists could not be queued for removal — " + PluginLog.Describe(ex)); }
        }

        private static RemovedTopList? Unqueue(string tag, string contentType)
        {
            lock (_queueLock)
            {
                var q = LoadQueue();
                var hit = q.Items.LastOrDefault(r => string.Equals(r.TagName, tag, StringComparison.OrdinalIgnoreCase)
                    && string.Equals(r.ContentType, contentType, StringComparison.OrdinalIgnoreCase));
                if (hit == null) return null;
                q.Items.RemoveAll(r => string.Equals(r.TagName, tag, StringComparison.OrdinalIgnoreCase)
                    && string.Equals(r.ContentType, contentType, StringComparison.OrdinalIgnoreCase));
                SaveQueue(q);
                return hit;
            }
        }

        /// <summary>The libraries of lists queued for removal (still top-list libraries until then).</summary>
        internal static List<string> QueuedLibraryIds()
        {
            if (Plugin.Instance == null) return new List<string>();
            lock (_queueLock) return LoadQueue().Items.Where(r => !string.IsNullOrEmpty(r.LibraryId)).Select(r => r.LibraryId).ToList();
        }

        /// <summary>
        /// Run: removes what the queued lists made — their home rows, library (and its My Media /
        /// Latest exclusions) and folder. onlyTag: Run Group of one source, only its own list.
        /// Runs before SyncTopListFolders, which may write a new list into the same folder.
        /// </summary>
        internal static void RemoveQueued(PluginConfiguration config, ILibraryManager libraryManager, IUserManager userManager, RunLog log, CancellationToken ct, string? onlyTag = null, string? onlySource = null)
        {
            RemovedTopListQueue q;
            lock (_queueLock) q = LoadQueue();
            var todo = q.Items.Where(r => onlyTag == null || string.Equals(r.TagName, onlyTag, StringComparison.OrdinalIgnoreCase)
                || (!string.IsNullOrWhiteSpace(onlySource) && string.Equals(r.SourceName, onlySource, StringComparison.OrdinalIgnoreCase))).ToList();
            if (todo.Count == 0) return;

            var root = Path.Combine(Plugin.Instance!.DataFolderPath, "toplists");
            var done = new List<RemovedTopList>();
            foreach (var r in todo)
            {
                bool failed = false;
                int rows = 0;
                foreach (var row in r.Rows ?? new List<HomeSectionTracking>())
                {
                    if (string.IsNullOrEmpty(row.UserId) || string.IsNullOrEmpty(row.SectionId)) continue;
                    long uid;
                    try { uid = userManager.GetInternalId(row.UserId); } catch { continue; }   // user deleted since
                    try { userManager.DeleteHomeSections(uid, new[] { row.SectionId }, ct); rows++; }
                    catch (Exception ex) { failed = true; log.Warn($"Top-list '{r.TagName}': a home row could not be removed: {ex.Message}"); }
                }

                var folder = Path.Combine(root, Sanitize(r.TagName));
                var lib = libraryManager.GetVirtualFolders().FirstOrDefault(f => !string.IsNullOrEmpty(r.LibraryId) && string.Equals(f.ItemId, r.LibraryId, StringComparison.OrdinalIgnoreCase))
                    ?? RankedCollections.LibraryFor(libraryManager, folder);
                // A list using the same folder now (made again as the other content type) keeps the
                // library only if it is its own.
                bool libInUse = lib != null && (config.TopLists ?? new List<TopListHomeSection>())
                    .Any(t => string.Equals(t.HomeSectionLibraryId, lib.ItemId, StringComparison.OrdinalIgnoreCase));
                bool libRemoved = false;
                if (lib != null && !libInUse && long.TryParse(lib.ItemId, out var libId))
                {
                    try
                    {
                        TopListLibraryVisibility.Forget(lib.ItemId, (lib.Guid ?? "").Replace("-", ""), userManager, m => log.Warn(m));
                        libraryManager.RemoveVirtualFolder(libId, false);
                        libRemoved = true;
                    }
                    catch (Exception ex) { failed = true; log.Warn($"Top-list '{r.TagName}': library '{lib.Name}' could not be removed: {ex.Message}"); }
                }

                // The folder stays only for a list that uses it with a library of its own; a list made
                // again as the other content type (no library yet) gets it written anew by this run.
                bool folderInUse = (config.TopLists ?? new List<TopListHomeSection>())
                    .Any(t => string.Equals(Sanitize(t.TagName), Sanitize(r.TagName), StringComparison.OrdinalIgnoreCase)
                        && (!t.FromSource || (!string.IsNullOrEmpty(t.HomeSectionLibraryId) && t.HomeSectionLibraryId != "auto"
                            && !string.Equals(t.HomeSectionLibraryId, lib?.ItemId, StringComparison.OrdinalIgnoreCase))));
                if (!folderInUse && Directory.Exists(folder))
                {
                    try { Directory.Delete(folder, true); }
                    catch (Exception ex) { failed = true; log.Warn($"Top-list '{r.TagName}': folder could not be removed: {ex.Message}"); }
                }
                // Its saved titles, unless a list with the same name is still there (made again as the
                // other content type: the run has just written its titles).
                if (!(config.TopLists ?? new List<TopListHomeSection>()).Any(t => string.Equals((t.TagName ?? "").Trim(), (r.TagName ?? "").Trim(), StringComparison.OrdinalIgnoreCase)))
                    TopListMembers.Delete(r.TagName);

                var what = new List<string>();
                if (rows > 0) what.Add($"home row for {RunLog.Plural(rows, "user")}");
                if (libRemoved) what.Add("library");
                if (!folderInUse) what.Add("copies");
                log.Skip($"Top-list '{r.TagName}': {(what.Count > 0 ? string.Join(", ", what) : "nothing left to")} removed ({(string.IsNullOrEmpty(r.Reason) ? "switched off in its source" : r.Reason)})");
                if (!failed) done.Add(r);
            }

            lock (_queueLock)
            {
                // Anything queued while this ran stays for the next run.
                var now = LoadQueue();
                now.Items.RemoveAll(x => done.Any(d => string.Equals(d.TagName, x.TagName, StringComparison.OrdinalIgnoreCase)
                    && string.Equals(d.ContentType, x.ContentType, StringComparison.OrdinalIgnoreCase)
                    && d.LibraryId == x.LibraryId));
                SaveQueue(now);
            }
        }

        // ── Library of a new list ────────────────────────────────────────────────────────

        /// <summary>
        /// Run, after SyncTopListFolders wrote the files: a source's list without a library gets
        /// one (made like the Top-Lists tab makes it: Emby may switch every user to "all libraries"
        /// when a library is added, so each user's access is put back) and the library is scanned.
        /// Rows and access then come from TopListSyncTask as for every list. True when a library id
        /// was set (the config needs saving).
        /// </summary>
        internal static bool EnsureLibraries(PluginConfiguration config, ILibraryManager libraryManager, IUserManager userManager,
            IProviderManager providerManager, IFileSystem fileSystem, RunLog log, string? onlyTag = null)
        {
            bool changed = false;
            var root = Path.Combine(Plugin.Instance!.DataFolderPath, "toplists");
            foreach (var tl in (config.TopLists ?? new List<TopListHomeSection>()).Where(t => t.FromSource))
            {
                if (onlyTag != null && !string.Equals(tl.TagName, onlyTag, StringComparison.OrdinalIgnoreCase)) continue;
                var folder = Path.Combine(root, Sanitize(tl.TagName));
                bool hasId = !string.IsNullOrEmpty(tl.HomeSectionLibraryId) && tl.HomeSectionLibraryId != "auto"
                    && libraryManager.GetVirtualFolders().Any(f => string.Equals(f.ItemId, tl.HomeSectionLibraryId, StringComparison.OrdinalIgnoreCase));
                if (hasId) continue;

                // A library on the folder already (made by an earlier run whose id was not saved).
                var lib = RankedCollections.LibraryFor(libraryManager, folder);
                bool created = false;
                if (lib == null)
                {
                    bool hasFiles = Directory.Exists(folder) && Directory.EnumerateFiles(folder, "*.strm", SearchOption.AllDirectories).Any();
                    if (!hasFiles) { log.Skip($"Top-list '{tl.TagName}': no library yet — the source has no titles in the library"); continue; }
                    bool shows = ShowTopList.IsShows(tl);
                    var name = TitleOf(tl);
                    lib = RankedCollections.CreateLibrary(name, folder, shows ? "tvshows" : "movies", LibraryOptionsFor(folder, shows),
                        "Top-list library", libraryManager, userManager, log);
                    if (lib == null) continue;
                    created = true;
                    LibraryAdded = true;
                }
                tl.HomeSectionLibraryId = lib.ItemId;
                changed = true;
                if (created && long.TryParse(lib.ItemId, out var libInternalId))
                {
                    // Only this library (never a full scan): the files were written before it existed.
                    try
                    {
                        providerManager.QueueRefresh(libInternalId, new MetadataRefreshOptions(fileSystem)
                        {
                            Recursive = true,
                            MetadataRefreshMode = MetadataRefreshMode.Default,
                            ImageRefreshMode = MetadataRefreshMode.Default
                        }, RefreshPriority.High);
                    }
                    catch (Exception ex) { log.Warn($"Top-list library '{lib.Name}' could not be scanned: {ex.Message}"); }
                }
            }
            return changed;
        }

        private static string TitleOf(TopListHomeSection tl)
        {
            var name = Get(Parse(tl.HomeSectionSettings), "CustomName").Trim();
            return name.Length > 0 ? name : (tl.TagName ?? "Top-list").Trim();
        }

        // Same options the Top-Lists tab uses (configPage.js executeTopListCreationSteps): a show
        // list holds copies the plugin writes complete (nfo + images), so it needs no online
        // metadata, and it stays out of search where every show would otherwise appear twice.
        private static LibraryOptions LibraryOptionsFor(string folder, bool shows)
        {
            var paths = new[] { new MediaPathInfo { Path = folder } };
            if (shows)
                return new LibraryOptions
                {
                    PathInfos = paths,
                    ExcludeFromSearch = true,
                    TypeOptions = new[] { "Series", "Season", "Episode" }.Select(t => new TypeOptions
                    {
                        Type = t,
                        MetadataFetchers = new[] { "Nfo" },
                        MetadataFetcherOrder = new[] { "Nfo" },
                        ImageFetchers = Array.Empty<string>(),
                        ImageFetcherOrder = Array.Empty<string>()
                    }).ToArray()
                };
            return new LibraryOptions
            {
                PathInfos = paths,
                TypeOptions = new[]
                {
                    new TypeOptions
                    {
                        Type = "Movie",
                        MetadataFetchers = new[] { "Nfo", "TheMovieDb", "TheTVDB" },
                        MetadataFetcherOrder = new[] { "Nfo", "TheMovieDb", "TheTVDB" },
                        ImageFetchers = new[] { "TheMovieDb", "TheTVDB" },
                        ImageFetcherOrder = new[] { "TheMovieDb", "TheTVDB" }
                    }
                }
            };
        }
    }

    // removed_toplists.xml in the plugin data folder (see SourceTopLists).
    public class RemovedTopListQueue
    {
        public List<RemovedTopList> Items { get; set; } = new List<RemovedTopList>();
    }

    public class RemovedTopList
    {
        public string TagName { get; set; } = "";
        public string ContentType { get; set; } = "Movies";
        public string LibraryId { get; set; } = "";
        public string HomeSectionSettings { get; set; } = "{}";
        public List<HomeSectionTracking> Rows { get; set; } = new List<HomeSectionTracking>();
        public string Reason { get; set; } = "";
        // Name of the source it came from ("" = unknown): that source's Run Group removes it too.
        public string SourceName { get; set; } = "";
    }
}
