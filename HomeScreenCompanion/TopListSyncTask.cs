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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace HomeScreenCompanion
{
    public class TopListSyncTask : IScheduledTask, IConfigurableScheduledTask
    {
        private readonly ILibraryManager _libraryManager;
        private readonly IUserViewManager _userViewManager;
        private readonly IUserManager _userManager;
        private readonly IJsonSerializer _jsonSerializer;
        private readonly PluginLogger _logger;

        public static List<string> ExecutionLog { get; } = new List<string>();
        public static bool IsRunning { get; private set; } = false;
        public static DateTime? LastStartedUtc { get; private set; }
        /// <summary>The step a standalone run is on, shown next to "Running" on the page.</summary>
        internal static RunProgress Progress { get; } = new RunProgress();
        private static RunLog _log = new RunLog(ExecutionLog, null, "", false);
        public static string LastRunStatus { get; private set; } = "Never";

        public TopListSyncTask(ILibraryManager libraryManager, IUserViewManager userViewManager, IUserManager userManager, IJsonSerializer jsonSerializer, ILogManager logManager)
        {
            _libraryManager = libraryManager;
            _userViewManager = userViewManager;
            _userManager = userManager;
            _jsonSerializer = jsonSerializer;
            _logger = PluginLog.Wrap(logManager.GetLogger("HomeScreenCompanion_Access"), "Top-lists");
        }

        public string Key => "TopListSyncTask";
        public string Name => "Top-list section sync";
        public string Description => "Runs by itself when a new library is created in Emby. Ensures each top-list home section only shows items from its own library, and that regular home sections never include top-list libraries.";
        public string Category => "Home Screen Companion";

        // Not listed in Emby's Scheduled Tasks (part of the Tag & Collection Sync; also queued when a top-list is saved or a new library appears (NewLibraryWatcher), and in the Run menu). It still runs on its triggers.
        public bool IsHidden => true;
        public bool IsEnabled => true;
        public bool IsLogged => true;

        public IEnumerable<TaskTriggerInfo> GetDefaultTriggers() => Array.Empty<TaskTriggerInfo>();

        public Task Execute(CancellationToken cancellationToken, IProgress<double> progress)
        {
            IsRunning = true;
            try
            {
                var (_, msg) = SyncAll(_libraryManager, _userViewManager, _userManager, _jsonSerializer, _logger, cancellationToken);
                LastRunStatus = msg;
            }
            catch (Exception ex)
            {
                LastRunStatus = $"Error: {ex.Message}";
                _log.Error($"Top-list sync aborted: {ex.Message}");
            }
            finally
            {
                IsRunning = false;
                PersistLog(); // again, so the abort line above is included
            }
            return Task.CompletedTask;
        }

        private static string UserLabel(IUserManager userManager, string userId)
        {
            try
            {
                if (Guid.TryParse(userId, out var guid))
                    return userManager.GetUserById(guid)?.Name ?? userId;
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            return userId;
        }

        // The ids of every entry Emby's own section editor lists under "Libraries" for this user.
        // An items-section has no "include only" field — the selection is stored as ExcludedFolders
        // (every view NOT ticked) — so a view missing here stays ticked. The editor loads
        // Users/{id}/Views?IncludeHidden=true&AllowDynamicChildren=false, which also contains
        // libraries the user hid from My Media and channels; mirror that exactly. Both the GUID
        // and the internal id of each view are returned so either form matches.
        internal static List<string> GetSectionEditorViewIds(IUserViewManager userViewManager, IUserManager userManager, string userId)
        {
            var ids = new List<string>();
            try
            {
                var uid = userManager.GetInternalId(userId);
                Guid.TryParse(userId, out var userGuid);
                // Reflect through the interface type to handle explicit interface implementations.
                // Emby 4.10 has three GetUserViews (query; query + data context + token; and one more),
                // so GetMethod(name) is ambiguous: take the one with just the query.
                var ifMethod = typeof(IUserViewManager).GetMethods()
                    .Where(m => m.Name == "GetUserViews")
                    .OrderBy(m => m.GetParameters().Length)
                    .FirstOrDefault();
                if (ifMethod == null) return ids;
                var queryParams = ifMethod.GetParameters();
                object? queryArg = null;
                if (queryParams.Length > 0)
                {
                    try
                    {
                        var qt = queryParams[0].ParameterType;
                        queryArg = Activator.CreateInstance(qt);
                        var uidProp = qt.GetProperty("UserId");
                        if (uidProp?.PropertyType == typeof(long))
                            uidProp.SetValue(queryArg, uid);
                        else
                            uidProp?.SetValue(queryArg, userGuid);
                        SetBool(qt, queryArg, "IncludeHidden", true);
                        SetBool(qt, queryArg, "AllowDynamicChildren", false);
                        SetBool(qt, queryArg, "IncludeExternalContent", true);
                    }
                    catch { queryArg = null; }
                }
                var result = ifMethod.Invoke(userViewManager, new[] { queryArg });
                if (result is System.Collections.IEnumerable views)
                    foreach (var v in views)
                    {
                        if (v is BaseItem bi)
                        {
                            if (bi.Id != Guid.Empty) ids.Add(bi.Id.ToString("N"));
                            if (bi.InternalId > 0) ids.Add(bi.InternalId.ToString());
                            continue;
                        }
                        var idProp = v?.GetType().GetProperty("Id");
                        if (idProp?.GetValue(v) is Guid vid && vid != Guid.Empty)
                            ids.Add(vid.ToString("N"));
                    }
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            return ids.Select(s => s.ToLowerInvariant()).Distinct().ToList();
        }

        private static void SetBool(Type t, object target, string name, bool value)
        {
            var p = t.GetProperty(name);
            if (p == null || !p.CanWrite) return;
            if (p.PropertyType == typeof(bool) || p.PropertyType == typeof(bool?))
                p.SetValue(target, value);
        }

        // All id forms of a top-list's own library (as stored, GUID, internal id), lowercase, so
        // it is never put on its own exclusion list whichever form a view list reports.
        internal static HashSet<string> OwnLibraryIds(string libId, ILibraryManager libraryManager)
        {
            var own = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            if (string.IsNullOrWhiteSpace(libId)) return own;
            var raw = libId.Trim().ToLowerInvariant();
            own.Add(raw);
            own.Add(raw.Replace("-", ""));
            try
            {
                BaseItem? lib = null;
                if (long.TryParse(raw, out var internalId))
                    lib = libraryManager.GetItemById(internalId);
                else if (Guid.TryParse(raw, out var g))
                    lib = libraryManager.GetItemById(g);
                if (lib != null)
                {
                    own.Add(lib.Id.ToString("N"));
                    own.Add(lib.InternalId.ToString());
                }
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            return own;
        }

        // Every id form of every top-list's own library.
        internal static HashSet<string> TopListLibraryIds(PluginConfiguration? config, ILibraryManager libraryManager)
        {
            var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var tl in config?.TopLists ?? new List<TopListHomeSection>())
                if (!string.IsNullOrEmpty(tl.HomeSectionLibraryId) && tl.HomeSectionLibraryId != "auto")
                    ids.UnionWith(OwnLibraryIds(tl.HomeSectionLibraryId, libraryManager));
            // Lists switched off in their source keep their library until the run removes it: its
            // rows are still top-list rows (a source row with the same title must not take them over).
            foreach (var libId in SourceTopLists.QueuedLibraryIds())
                ids.UnionWith(OwnLibraryIds(libId, libraryManager));
            return ids;
        }

        // A top-list's row: an items row on the list's own library (libraryIds: OwnLibraryIds of one
        // list, or TopListLibraryIds of all). Found by its library, never by its title — a source's
        // row can have the same title.
        internal static bool IsTopListRow(ContentSection s, HashSet<string> libraryIds)
            => string.Equals(s.SectionType, "items", StringComparison.OrdinalIgnoreCase)
               && !string.IsNullOrEmpty(s.ParentId)
               && libraryIds.Contains(s.ParentId.Trim().Replace("-", ""));

        // Row-position memory key of a top-list row (kept apart from the source rows' keys).
        internal static string RowPositionKey(TopListHomeSection tl) => "tl\u001F" + (tl.TagName ?? "").Trim();

        private static string NormId(string? id) => (id ?? "").Replace("-", "").Trim().ToLowerInvariant();

        // A user removed from Emby can still be on a list's users: such ids are left out.
        internal static bool UserExists(IUserManager userManager, string userId)
        {
            try { return Guid.TryParse(userId, out var g) ? userManager.GetUserById(g) != null : userManager.GetInternalId(userId) > 0; }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); return false; }
        }

        private static bool LibraryExists(string libId, ILibraryManager libraryManager)
        {
            try
            {
                var raw = (libId ?? "").Trim();
                if (long.TryParse(raw, out var internalId)) return libraryManager.GetItemById(internalId) != null;
                if (Guid.TryParse(raw, out var g)) return libraryManager.GetItemById(g) != null;
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            return false;
        }

        private static string NameList(List<string> names)
            => names.Count <= 10 ? string.Join(", ", names) : string.Join(", ", names.Take(10)) + $" and {names.Count - 10} more";

        // The list's other users (reference home screens for a row's place).
        internal static IEnumerable<string> OtherUsers(TopListHomeSection tl, string userId)
            => (tl.HomeSectionUserIds ?? new List<string>()).Where(u => NormId(u) != NormId(userId));

        // Where a row added again goes: where it was when Copy & Sync last moved it (remembered), else
        // at the list's Position, else below the row that is above it on the home screen of another of
        // the list's users, else at the bottom (where Emby puts a new row).
        private static void PlaceAddedRow(IUserManager userManager, IJsonSerializer json, TopListHomeSection tl, HashSet<string> ownIds,
            string userId, long userInternalId, string sectionId, string position, string? ownTitle, CancellationToken cancellationToken)
        {
            if (HomeScreenCompanionTask.RestoreRowPosition(userManager, json, RowPositionKey(tl), userId, userInternalId, sectionId, cancellationToken)) return;
            if (!string.IsNullOrEmpty(position))
            {
                HomeScreenCompanionTask.PlaceNewRow(userManager, userInternalId, sectionId, position, cancellationToken, OtherUsers(tl, userId), ownTitle);
                return;
            }
            foreach (var other in tl.HomeSectionUserIds ?? new List<string>())
            {
                if (NormId(other) == NormId(userId)) continue;
                try
                {
                    var secs = userManager.GetHomeSections(userManager.GetInternalId(other), cancellationToken)?.Sections ?? Array.Empty<ContentSection>();
                    int at = Array.FindIndex(secs, s => IsTopListRow(s, ownIds));
                    if (at < 0) continue;
                    HomeScreenCompanionTask.PlaceNewRow(userManager, userInternalId, sectionId,
                        at == 0 ? "top" : "after:" + HomeScreenCompanionTask.SectionKey(secs[at - 1]), cancellationToken);
                    return;
                }
                catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            }
        }

        private static void PersistLog() { Progress.Clear(); LogStore.Save(LogStore.TopLists, ExecutionLog, LastRunStatus, LastStartedUtc); }

        // Brings back the last run's log and status after a server restart.
        internal static void RestoreLog()
        {
            var saved = LogStore.Load(LogStore.TopLists);
            if (saved == null || IsRunning) return;
            lock (ExecutionLog)
            {
                if (ExecutionLog.Count > 0) return;
                ExecutionLog.AddRange(saved.Lines ?? new List<string>());
            }
            LastRunStatus = LogStore.RestoredStatus(saved.Status);
            LastStartedUtc = saved.StartedUtc;
        }

        internal static (int updated, string message) SyncAll(
            ILibraryManager libraryManager,
            IUserViewManager userViewManager,
            IUserManager userManager,
            IJsonSerializer jsonSerializer,
            PluginLogger logger,
            CancellationToken cancellationToken,
            RunLog? log = null,
            string? onlyTag = null)
        {
            // Inside the main sync the lines belong to that run's log, which it saves itself.
            if (log != null)
                return SyncAllCore(libraryManager, userViewManager, userManager, jsonSerializer, logger, cancellationToken, log, onlyTag);

            // Standalone (scheduled task or the UI): this task's own log — keep it across restarts.
            // One run at a time (see RunGate): a running sync / Run Group finishes first.
            if (RunGate.Busy) Progress.Set($"Waiting for {RunGate.Holder} to finish");
            using var gate = RunGate.Enter("Top-list section sync", logger, cancellationToken);
            Progress.Clear();
            try
            {
                var result = SyncAllCore(libraryManager, userViewManager, userManager, jsonSerializer, logger, cancellationToken, null);
                LastRunStatus = result.message;
                return result;
            }
            catch (Exception ex)
            {
                LastRunStatus = $"Error: {ex.Message}";
                throw;
            }
            finally { PersistLog(); }
        }

        private static (int updated, string message) SyncAllCore(
            ILibraryManager libraryManager,
            IUserViewManager userViewManager,
            IUserManager userManager,
            IJsonSerializer jsonSerializer,
            PluginLogger logger,
            CancellationToken cancellationToken,
            RunLog? log,
            string? onlyTag = null)
        {
            var config = Plugin.Instance?.Configuration;
            var startTime = DateTime.Now;
            // When called from the main sync, lines go into that run's log (under its "» Top-lists"
            // heading). On its own (scheduled task / UI button) this task keeps its own log.
            bool standalone = log == null;
            if (standalone)
            {
                lock (ExecutionLog) { ExecutionLog.Clear(); }
                LastStartedUtc = DateTime.UtcNow;
                _log = new RunLog(ExecutionLog, null, "", config?.ExtendedConsoleOutput ?? false, Progress, "Top-lists");
                _log.Rule();
                _log.Info($"Top-list section sync  ·  {startTime:yyyy-MM-dd HH:mm}");
                _log.Rule();
            }
            else
            {
                _log = log!;
            }
            if (config == null) { _log.Error("Plugin configuration could not be loaded"); return (0, "No config."); }

            var topLists = config.TopLists ?? new List<TopListHomeSection>();
            if (topLists.Count == 0) { _log.Skip("No top-lists configured — nothing to do"); return (0, "No top-lists configured."); }

            if (standalone) { _log.Blank(); _log.Info($"» Top-list sections  ·  {RunLog.Plural(topLists.Count, "top-list")}"); }
            int totalUpdated = 0, totalErrors = 0, totalAdded = 0;
            var userStatus = UserStatus.Read(userManager);

            // Collect all configured top-list library IDs so each top-list always excludes
            // its siblings, even if their libraries haven't been discovered via GetVirtualFolders yet.
            var allTlLibIds = topLists
                .Where(t => !string.IsNullOrEmpty(t.HomeSectionLibraryId) && t.HomeSectionLibraryId != "auto")
                .Select(t => t.HomeSectionLibraryId.Trim().ToLowerInvariant())
                .Distinct().ToList();

            foreach (var tl in topLists)
            {
                // Run Group on one top-list: only that list's sections.
                if (onlyTag != null && !string.Equals(tl.TagName, onlyTag, StringComparison.OrdinalIgnoreCase)) continue;
                string tlName = tl.TagName ?? "(unnamed)";
                _log.Step($"Top-lists — {tlName}");
                _log.Section($"Top-list '{tlName}'");
                if (string.IsNullOrEmpty(tl.HomeSectionLibraryId) || tl.HomeSectionLibraryId == "auto") { _log.Skip($"Top-list '{tlName}': skipped — no library has been created for it yet"); continue; }
                int tlUpdated = 0, tlRemoved = 0;

                var ownIds = OwnLibraryIds(tl.HomeSectionLibraryId, libraryManager);
                var safeTag = new string((tl.TagName ?? "").Select(c => char.IsLetterOrDigit(c) ? c : '_').ToArray());
                var sectionMarker = "hsc__tl__" + safeTag;

                // Remove home sections for users no longer assigned to this top-list
                if (tl.HomeSectionTracked != null)
                {
                    var assignedNorm = new HashSet<string>(
                        (tl.HomeSectionUserIds ?? new List<string>()).Select(id => id.Replace("-", "").ToLowerInvariant()),
                        StringComparer.OrdinalIgnoreCase);

                    var unassignedTracked = tl.HomeSectionTracked
                        .Where(t => !string.IsNullOrEmpty(t.UserId) && !string.IsNullOrEmpty(t.SectionId))
                        .Where(t => !assignedNorm.Contains(t.UserId.Replace("-", "").ToLowerInvariant()))
                        .ToList();

                    foreach (var t in unassignedTracked)
                    {
                        try
                        {
                            var uid = userManager.GetInternalId(t.UserId);
                            userManager.DeleteHomeSections(uid, new[] { t.SectionId }, cancellationToken);
                            tl.HomeSectionTracked.Remove(t);
                            tlRemoved++;
                            _log.Debug($"  {UserLabel(userManager, t.UserId)}: section removed (user no longer selected)");
                        }
                        catch (Exception ex) { totalErrors++; _log.Warn($"Top-list '{tlName}': could not remove section for {UserLabel(userManager, t.UserId)} — {ex.Message}"); }
                    }
                }

                // Every user on the list gets the list's row: the tracked one; else a row on the list's
                // own library the user already has (e.g. one Copy & Sync put there), taken over; else a
                // new one. A row can go missing (removed in Emby's home screen settings, or by an older
                // version when a source with the same title lost its row); skipping it, as this sync
                // used to, left that user without the row for good.
                tl.HomeSectionTracked ??= new List<HomeSectionTracking>();
                bool libraryFound = LibraryExists(tl.HomeSectionLibraryId, libraryManager);
                var added = new List<string>();
                var noLibrary = new List<string>();   // no row and none can be made: the library is gone
                // Where the list's row goes; changed since last put into effect: the rows already there move.
                var posNow = RowPlacement.PositionIn(tl.HomeSectionSettings);
                var posKey = RowPlacement.TopListKey(tl);
                bool posMove = RowPlacement.ShouldMove(posKey, posNow, true);
                int posMoved = 0;
                foreach (var userId in (tl.HomeSectionUserIds ?? new List<string>())
                    .Where(u => !string.IsNullOrWhiteSpace(u)).GroupBy(NormId).Select(g => g.First()).ToList())
                {
                    string who = UserLabel(userManager, userId);
                    try
                    {
                        if (!UserExists(userManager, userId)) { _log.Debug($"  {who}: skipped (no such user)"); continue; }
                        // Disabled in Emby: the row and its tracking stay as they are.
                        if (userStatus.Skip(userId)) { _log.Debug($"  {who}: skipped — disabled in Emby (the row is kept)"); continue; }
                        var uid = userManager.GetInternalId(userId);
                        var sections = userManager.GetHomeSections(uid, cancellationToken)?.Sections
                            ?? Array.Empty<ContentSection>();

                        var tracking = tl.HomeSectionTracked.FirstOrDefault(t => NormId(t.UserId) == NormId(userId));
                        var owned = !string.IsNullOrEmpty(tracking?.SectionId)
                            ? sections.FirstOrDefault(s => s.Id == tracking!.SectionId) : null;
                        if (owned == null)
                        {
                            owned = sections.FirstOrDefault(s => IsTopListRow(s, ownIds));
                            if (owned != null)
                                _log.Debug($"  {who}: section {owned.Id} on the list's library taken over (the saved one, {(string.IsNullOrEmpty(tracking?.SectionId) ? "none" : tracking!.SectionId)}, was gone)");
                        }
                        if (owned == null && !libraryFound) { noLibrary.Add(who); continue; }

                        // Every view this user's section editor lists (hidden libraries, channels
                        // and Live TV included).
                        var allViewIds = GetSectionEditorViewIds(userViewManager, userManager, userId);

                        // Always also include virtual folder IDs (ensures regular libraries are covered)
                        foreach (var f in libraryManager.GetVirtualFolders())
                            if (!string.IsNullOrEmpty(f.ItemId))
                                allViewIds.Add(f.ItemId.Trim().ToLowerInvariant());

                        allViewIds = allViewIds.Distinct().ToList();

                        // Final fallback: only virtual folders if nothing else worked
                        if (allViewIds.Count == 0)
                        {
                            allViewIds = libraryManager.GetVirtualFolders()
                                .Where(f => !string.IsNullOrEmpty(f.ItemId))
                                .Select(f => f.ItemId.Trim().ToLowerInvariant())
                                .ToList();
                        }

                        var settingsDict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                        try
                        {
                            if (!string.IsNullOrEmpty(tl.HomeSectionSettings) && tl.HomeSectionSettings != "{}")
                                settingsDict = jsonSerializer.DeserializeFromString<Dictionary<string, string>>(tl.HomeSectionSettings) ?? settingsDict;
                        }
                        catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }

                        // Merge: union of newly discovered IDs and any previously stored exclusions
                        // (preserves IDs like Live TV that are only captured at initial setup time).
                        var storedExclude = (settingsDict.TryGetValue("_queryExcludeViewIds", out var storedEv) ? storedEv : "")
                            .Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries)
                            .Select(s => s.Trim().ToLowerInvariant()).Where(s => s.Length > 0);
                        var mergedExcludeIds = allViewIds
                            .Concat(storedExclude)
                            .Concat(allTlLibIds)
                            .Where(id => !ownIds.Contains(id))
                            .Distinct().ToList();
                        var excludeStr = string.Join(",", mergedExcludeIds);

                        settingsDict["_queryExcludeViewIds"] = excludeStr;
                        settingsDict["ExcludedFolders"] = excludeStr;
                        tl.HomeSectionSettings = jsonSerializer.SerializeToString(settingsDict);

                        string rowId;
                        if (owned != null)
                        {
                            var updated = HomeScreenCompanionTask.BuildContentSection(
                                jsonSerializer, settingsDict, tl.HomeSectionLibraryId, owned);
                            typeof(ContentSection).GetProperty("Id")?.SetValue(updated, owned.Id);
                            userManager.UpdateHomeSection(uid, updated, cancellationToken);
                            rowId = owned.Id;
                            bool moved = posMove && HomeScreenCompanionTask.MoveToPosition(userManager, uid, rowId, posNow, cancellationToken,
                                OtherUsers(tl, userId), settingsDict.TryGetValue("CustomName", out var ownName) ? ownName : null);
                            if (moved) posMoved++;
                            _log.Debug($"  {who}: section updated{(moved ? ", moved to its new position" : "")}");
                            totalUpdated++;
                            tlUpdated++;
                        }
                        else
                        {
                            var beforeIds = new HashSet<string>(sections.Where(s => !string.IsNullOrEmpty(s.Id)).Select(s => s.Id));
                            userManager.AddHomeSection(uid, HomeScreenCompanionTask.BuildContentSection(
                                jsonSerializer, settingsDict, tl.HomeSectionLibraryId), cancellationToken);
                            var after = userManager.GetHomeSections(uid, cancellationToken)?.Sections ?? Array.Empty<ContentSection>();
                            rowId = after.Where(s => !string.IsNullOrEmpty(s.Id) && !beforeIds.Contains(s.Id)).Select(s => s.Id).FirstOrDefault()
                                ?? after.FirstOrDefault(s => IsTopListRow(s, ownIds))?.Id ?? "";
                            if (rowId.Length == 0)
                            {
                                totalErrors++;
                                _log.Warn($"Top-list '{tlName}': the row for {who} was added but not found again — the next sync takes it over");
                                continue;
                            }
                            PlaceAddedRow(userManager, jsonSerializer, tl, ownIds, userId, uid, rowId, posNow,
                                settingsDict.TryGetValue("CustomName", out var newName) ? newName : null, cancellationToken);
                            _log.Debug($"  {who}: section added (it was missing)");
                            totalAdded++;
                            added.Add(who);
                        }
                        // One entry per user, pointing at the row.
                        tl.HomeSectionTracked.RemoveAll(t => NormId(t.UserId) == NormId(userId));
                        tl.HomeSectionTracked.Add(new HomeSectionTracking { UserId = userId, SectionId = rowId });
                    }
                    catch (Exception ex) { totalErrors++; _log.Warn($"Top-list '{tlName}': section could not be updated for {who} — {ex.Message}"); }
                }
                if (posMove)
                {
                    RowPlacement.MarkApplied(posKey, posNow);
                    if (posMoved > 0) _log.Ok($"Top-list '{tlName}': row moved to its new position ({HomeScreenCompanionTask.PositionLabel(posNow)}) for {RunLog.Plural(posMoved, "user")}");
                }
                if (noLibrary.Count > 0)
                {
                    totalErrors++;
                    _log.Warn($"Top-list '{tlName}': its library ({tl.HomeSectionLibraryId}) is gone, so no row could be made for {NameList(noLibrary)} — open the top-list and click Save to make the library again");
                }
                if (tlUpdated > 0 || tlRemoved > 0 || added.Count > 0)
                    _log.Ok($"Top-list '{tlName}': {RunLog.Plural(tlUpdated, "section")} updated"
                        + (added.Count > 0 ? $", {added.Count} added — the row was missing for {NameList(added)}" : "")
                        + (tlRemoved > 0 ? $", {tlRemoved} removed" : ""));
                else
                    _log.Skip($"Top-list '{tlName}': no home sections to update");
            }

            if (onlyTag != null)
            {
                if (userStatus.SummaryLine() is string disabledOne) _log.Skip(disabledOne);
                // Run Group on one top-list: the sweep over every other row below belongs to the full sync.
                GrantTopListLibraryAccess(topLists.Where(t => string.Equals(t.TagName, onlyTag, StringComparison.OrdinalIgnoreCase)).ToList(), userManager, libraryManager, logger);
                // A source's list got a new library from this Run Group (SourceTopLists.EnsureLibraries):
                // hidden from My Media and Latest now, not only at the next full sync.
                if (SourceTopLists.LibraryAdded)
                {
                    SourceTopLists.LibraryAdded = false;
                    var hiddenForOne = HideLibraries(config, userManager, libraryManager, logger, m => _log.Warn(m));
                    if (hiddenForOne > 0)
                        _log.Info($"    HSC libraries {(config.HideTopListLibraries ? "hidden from" : "shown again in")} My Media and Latest for {RunLog.Plural(hiddenForOne, "user")}");
                }
                Plugin.Instance?.SaveConfiguration();
                return (totalUpdated, $"Updated {totalUpdated} section(s){(totalAdded > 0 ? $", added {totalAdded}" : "")} of top-list '{onlyTag}'.");
            }

            // Ensure ALL top-list libraries are excluded from every TAG items-type section.
            // This makes the task a complete maintenance sweep, not just a top-list-section refresh.
            var allTopListLibIds = topLists
                .Where(t => !string.IsNullOrEmpty(t.HomeSectionLibraryId) && t.HomeSectionLibraryId != "auto")
                .Select(t => t.HomeSectionLibraryId.Trim().ToLowerInvariant())
                .Distinct().ToList();

            if (allTopListLibIds.Count > 0)
            {
                foreach (var tc in config.Tags ?? new List<TagConfig>())
                {
                    if (!SourceTopLists.PlainRow(tc)) continue;

                    var tcSettings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                    try
                    {
                        if (!string.IsNullOrEmpty(tc.HomeSectionSettings) && tc.HomeSectionSettings != "{}")
                            tcSettings = jsonSerializer.DeserializeFromString<Dictionary<string, string>>(tc.HomeSectionSettings) ?? tcSettings;
                    }
                    catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }

                    if (!tcSettings.TryGetValue("SectionType", out var tcSt) || string.IsNullOrEmpty(tcSt))
                        tcSt = HomeScreenCompanionTask.DefaultSectionType(tc);
                    // Only items rows get the top-list exclusion (boxset and playlist rows show one item list).
                    if (tcSt == "boxset" || tcSt == "playlist") continue;

                    var existingExcluded = (tcSettings.TryGetValue("_queryExcludeViewIds", out var tcEv) ? tcEv : "")
                        .Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries)
                        .Select(s => s.Trim().ToLowerInvariant()).Where(s => s.Length > 0).ToList();

                    var missing = allTopListLibIds
                        .Where(id => !existingExcluded.Contains(id, StringComparer.OrdinalIgnoreCase))
                        .ToList();
                    if (missing.Count == 0) continue;

                    existingExcluded.AddRange(missing);
                    existingExcluded = existingExcluded.Distinct().ToList();
                    var tcExcStr = string.Join(",", existingExcluded);
                    tcSettings["_queryExcludeViewIds"] = tcExcStr;
                    tcSettings["ExcludedFolders"] = tcExcStr;
                    tc.HomeSectionSettings = jsonSerializer.SerializeToString(tcSettings);
                    HomeScreenCompanionTask.ApplyRowTitle(tcSettings, tc, jsonSerializer);

                    if (!string.IsNullOrEmpty(tc.Tag))
                    {
                        try
                        {
                            var tagItem = libraryManager.GetItemList(new InternalItemsQuery
                            {
                                IncludeItemTypes = new[] { "Tag" },
                                Name = tc.Tag,
                                Recursive = true
                            }, cancellationToken).FirstOrDefault();
                            if (tagItem != null) tcSettings["_queryTagId"] = tagItem.InternalId.ToString();
                        }
                        catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
                    }


                    var realTracked = (tc.HomeSectionTracked ?? new List<HomeSectionTracking>())
                        .Where(t => !string.IsNullOrEmpty(t.SectionId) && !t.SectionId.StartsWith("hsc__"))
                        .ToList();

                    foreach (var tracking in realTracked)
                    {
                        try
                        {
                            var uid = userManager.GetInternalId(tracking.UserId);
                            var secs = userManager.GetHomeSections(uid, cancellationToken)?.Sections ?? Array.Empty<ContentSection>();
                            var owned = secs.FirstOrDefault(s => s.Id == tracking.SectionId);
                            if (owned == null) continue;
                            var updatedSec = HomeScreenCompanionTask.BuildContentSection(jsonSerializer, tcSettings, string.Empty, owned);
                            typeof(ContentSection).GetProperty("Id")?.SetValue(updatedSec, owned.Id);
                            userManager.UpdateHomeSection(uid, updatedSec, cancellationToken);
                            totalUpdated++;
                        }
                        catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
                    }
                }
            }

            // Aggressive: also keep all untracked, non-library-scoped sections up to date.
            totalUpdated += HomeScreenCompanionTask.UpdateUntrackedSections(
                jsonSerializer, userManager, config, allTopListLibIds, cancellationToken);

            GrantTopListLibraryAccess(topLists, userManager, libraryManager, logger);

            var hiddenFor = HideLibraries(config, userManager, libraryManager, logger, m => _log.Warn(m));
            if (hiddenFor > 0)
                _log.Info($"    HSC libraries {(config.HideTopListLibraries ? "hidden from" : "shown again in")} My Media and Latest for {RunLog.Plural(hiddenFor, "user")}");

            Plugin.Instance?.SaveConfiguration();
            if (userStatus.SummaryLine() is string disabledLine) _log.Skip(disabledLine);
            var summary = $"Updated {totalUpdated} section(s){(totalAdded > 0 ? $", added {totalAdded}" : "")} across {topLists.Count} top-list(s).";
            _log.Info($"    {RunLog.Plural(totalUpdated, "section")} updated{(totalAdded > 0 ? $", {totalAdded} added" : "")} across {RunLog.Plural(topLists.Count, "top-list")}{(totalErrors > 0 ? $", {RunLog.Plural(totalErrors, "error")}" : "")}  ·  {RunLog.Elapsed(DateTime.Now - startTime)}");
            return (totalUpdated, summary);
        }

        /// <summary>
        /// Keeps the plugin's libraries out of sight for every user: show-list libraries always out of
        /// "Latest" (Emby applies those exclusions to Continue Watching and Next Up too, so a show
        /// list's episode copies stay out of all three, in every app), and with "Hide HSC libraries"
        /// every top-list library out of My Media and Latest (TopListLibraryVisibility). Run by every
        /// top-list section sync and when a user is created (NewUserWatcher). Returns the number of
        /// users whose My Media / Latest settings changed.
        /// </summary>
        internal static int HideLibraries(PluginConfiguration config, IUserManager userManager, ILibraryManager libraryManager, PluginLogger logger, Action<string>? warn = null)
        {
            foreach (var tl in (config.TopLists ?? new List<TopListHomeSection>()).Where(ShowTopList.IsShows))
            {
                if (string.IsNullOrEmpty(tl.HomeSectionLibraryId) || tl.HomeSectionLibraryId == "auto") continue;
                var libGuid = ResolveLibraryGuid(tl.HomeSectionLibraryId.Trim(), libraryManager);
                if (!string.IsNullOrEmpty(libGuid)) SetLatestExclusion(userManager, libGuid!, true, logger);
            }
            return TopListLibraryVisibility.Apply(config, userManager, libraryManager, warn);
        }

        internal static void GrantTopListLibraryAccess(List<TopListHomeSection> topLists, IUserManager userManager, ILibraryManager libraryManager, PluginLogger logger)
        {
            dynamic mgr = userManager;

            foreach (var tl in topLists)
            {
                if (string.IsNullOrEmpty(tl.HomeSectionLibraryId) || tl.HomeSectionLibraryId == "auto") continue;
                if (tl.HomeSectionUserIds == null || tl.HomeSectionUserIds.Count == 0) continue;

                var rawLibId = tl.HomeSectionLibraryId.Trim();

                // HomeSectionLibraryId may be stored as InternalId (numeric) rather than GUID.
                // EnabledFolders in Emby user policy requires the GUID (32 hex chars, no dashes).
                // If numeric, look up the GUID via the library manager.
                var libGuid = ResolveLibraryGuid(rawLibId, libraryManager);
                if (string.IsNullOrEmpty(libGuid))
                {
                    logger.Warn($"[Access] Could not resolve GUID for library '{rawLibId}' — skipping.");
                    continue;
                }

                var assignedNormIds = new HashSet<string>(
                    tl.HomeSectionUserIds.Select(id => id.Replace("-", "").ToLowerInvariant()),
                    StringComparer.OrdinalIgnoreCase);

                var allUsers = userManager.GetUserList(new UserQuery { IsDisabled = false }).ToList();
                var matchedUsers = allUsers
                    .Where(u => assignedNormIds.Contains(u.Id.ToString().Replace("-", "").ToLowerInvariant()))
                    .ToList();

                // Grant: ensure assigned users have access
                foreach (var user in matchedUsers)
                {
                    try
                    {
                        var uid = userManager.GetInternalId(user.Id.ToString());
                        dynamic policy = GetPolicy(mgr, user, uid);
                        if (policy == null) continue;

                        bool enableAll;
                        try { enableAll = (bool)policy.EnableAllFolders; } catch { enableAll = false; }
                        if (enableAll) continue;

                        string[] folders;
                        try { folders = (string[])policy.EnabledFolders ?? Array.Empty<string>(); } catch { folders = Array.Empty<string>(); }

                        if (folders.Any(f => string.Equals(f.Replace("-", ""), libGuid, StringComparison.OrdinalIgnoreCase)))
                            continue;

                        // Remove any stale InternalId entry and add the GUID
                        var cleanedFolders = folders
                            .Where(f => !string.Equals(f.Replace("-", ""), rawLibId.Replace("-", ""), StringComparison.OrdinalIgnoreCase))
                            .ToArray();
                        policy.EnabledFolders = cleanedFolders.Concat(new[] { libGuid }).ToArray();
                        UpdatePolicy(mgr, user, uid, policy);
                        logger.Info($"[Access] Granted library '{tl.TagName}' to '{user.Name}'.");
                    }
                    catch (Exception ex) { logger.Error($"[Access] Grant error for '{user.Name}': {ex.GetBaseException().Message}"); }
                }

                // Revoke: remove access from users NOT assigned to this top-list
                var rawNorm = rawLibId.Replace("-", "").ToLowerInvariant();
                foreach (var user in allUsers.Where(u => !assignedNormIds.Contains(u.Id.ToString().Replace("-", "").ToLowerInvariant())))
                {
                    try
                    {
                        var uid = userManager.GetInternalId(user.Id.ToString());
                        dynamic policy = GetPolicy(mgr, user, uid);
                        if (policy == null) continue;

                        bool enableAll;
                        try { enableAll = (bool)policy.EnableAllFolders; } catch { enableAll = false; }
                        if (enableAll) continue;

                        string[] folders;
                        try { folders = (string[])policy.EnabledFolders ?? Array.Empty<string>(); } catch { folders = Array.Empty<string>(); }

                        var hasEntry = folders.Any(f =>
                            string.Equals(f.Replace("-", ""), libGuid, StringComparison.OrdinalIgnoreCase) ||
                            string.Equals(f.Replace("-", ""), rawNorm, StringComparison.OrdinalIgnoreCase));
                        if (!hasEntry) continue;

                        policy.EnabledFolders = folders
                            .Where(f =>
                                !string.Equals(f.Replace("-", ""), libGuid, StringComparison.OrdinalIgnoreCase) &&
                                !string.Equals(f.Replace("-", ""), rawNorm, StringComparison.OrdinalIgnoreCase))
                            .ToArray();

                        UpdatePolicy(mgr, user, uid, policy);
                        logger.Info($"[Access] Revoked library '{tl.TagName}' from '{user.Name}'.");
                    }
                    catch (Exception ex) { logger.Error($"[Access] Revoke error for '{user.Name}': {ex.GetBaseException().Message}"); }
                }
            }
        }

        // Adds (or removes) a library in every user's "Latest" exclusions (LatestItemsExcludes).
        internal static void SetLatestExclusion(IUserManager userManager, string libGuid, bool exclude, PluginLogger logger)
        {
            var norm = libGuid.Replace("-", "").ToLowerInvariant();
            foreach (var user in userManager.GetUserList(new UserQuery { IsDisabled = false }))
            {
                try
                {
                    var cfg = userManager.GetUserConfiguration(user);
                    if (cfg == null) continue;
                    var current = cfg.LatestItemsExcludes ?? Array.Empty<string>();
                    bool has = current.Any(x => string.Equals((x ?? "").Replace("-", ""), norm, StringComparison.OrdinalIgnoreCase));
                    if (has == exclude) continue;
                    cfg.LatestItemsExcludes = exclude
                        ? current.Concat(new[] { norm }).ToArray()
                        : current.Where(x => !string.Equals((x ?? "").Replace("-", ""), norm, StringComparison.OrdinalIgnoreCase)).ToArray();
                    userManager.UpdateConfiguration(user, cfg);
                }
                catch (Exception ex) { logger.Error($"[Access] Latest exclusion for '{user.Name}' failed: {ex.GetBaseException().Message}"); }
            }
        }

        private static string? ResolveLibraryGuid(string libId, ILibraryManager libraryManager)
        {
            if (string.IsNullOrEmpty(libId)) return null;
            var norm = libId.Replace("-", "").ToLowerInvariant();

            // If it's already a 32-char hex string → it's a GUID
            if (norm.Length == 32 && norm.All(c => "0123456789abcdef".IndexOf(c) >= 0))
                return norm;

            // Otherwise treat as InternalId — find the matching CollectionFolder
            if (long.TryParse(libId.Trim(), out long internalId))
            {
                try
                {
                    var items = libraryManager.GetItemList(new InternalItemsQuery
                    {
                        IncludeItemTypes = new[] { "CollectionFolder" }
                    });
                    var match = items.FirstOrDefault(it => it.InternalId == internalId);
                    if (match != null)
                        return match.Id.ToString("N").ToLowerInvariant();
                }
                catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            }

            return null;
        }

        internal static object? GetPolicy(dynamic mgr, BaseItem user, long uid)
        {
            dynamic? policy = null;
            try { policy = mgr.GetUserPolicy(user); return policy; }
            catch { }
            try { policy = mgr.GetUserPolicy(user.Id); return policy; }
            catch { }
            try { policy = mgr.GetUserPolicy(uid); return policy; }
            catch { }
            return user.GetType().GetProperty("Policy")?.GetValue(user);
        }

        internal static void UpdatePolicy(dynamic mgr, BaseItem user, long uid, dynamic policy)
        {
            try { mgr.UpdateUserPolicy(uid, policy); return; }
            catch { }
            try { mgr.UpdateUserPolicy(user.Id, policy); return; }
            catch { }
            mgr.UpdateUserPolicy(user, policy);
        }

    }
}
