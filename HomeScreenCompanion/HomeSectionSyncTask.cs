using MediaBrowser.Controller.Library;
using MediaBrowser.Model.Entities;
using MediaBrowser.Model.Logging;
using MediaBrowser.Model.Serialization;
using MediaBrowser.Model.Tasks;
using MediaBrowser.Model.Users;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace HomeScreenCompanion
{
    public class HomeSectionSyncTask : IScheduledTask
    {
        private readonly IUserManager _userManager;
        private readonly ILibraryManager _libraryManager;
        private readonly IJsonSerializer _jsonSerializer;
        private readonly PluginLogger _logger;

        public static string LastSyncTime { get; private set; } = "Never";
        public static bool IsRunning { get; private set; } = false;
        public static string LastSyncResult { get; private set; } = "";
        public static int LastSectionsCopied { get; private set; } = 0;
        public static List<string> ExecutionLog { get; } = new List<string>();
        public static DateTime? LastStartedUtc { get; private set; }
        /// <summary>The step a running sync is on, shown next to "Running" on the page.</summary>
        internal static RunProgress Progress { get; } = new RunProgress();
        private RunLog _log;

        private static void PersistLog() => LogStore.Save(LogStore.HomeScreen, ExecutionLog, LastSyncResult, LastStartedUtc,
            new Dictionary<string, string>
            {
                ["LastSyncTime"] = LastSyncTime,
                ["SectionsCopied"] = LastSectionsCopied.ToString(System.Globalization.CultureInfo.InvariantCulture)
            });

        // Brings back the last run's log and status after a server restart.
        internal static void RestoreLog()
        {
            var saved = LogStore.Load(LogStore.HomeScreen);
            if (saved == null || IsRunning) return;
            lock (ExecutionLog)
            {
                if (ExecutionLog.Count > 0) return;
                ExecutionLog.AddRange(saved.Lines ?? new List<string>());
            }
            LastSyncResult = LogStore.RestoredStatus(saved.Status);
            LastStartedUtc = saved.StartedUtc;
            var extra = saved.Extra ?? new Dictionary<string, string>();
            if (extra.TryGetValue("LastSyncTime", out var t) && !string.IsNullOrEmpty(t)) LastSyncTime = t;
            if (extra.TryGetValue("SectionsCopied", out var c) && int.TryParse(c, out var n)) LastSectionsCopied = n;
        }

        public HomeSectionSyncTask(IUserManager userManager, ILibraryManager libraryManager, IJsonSerializer jsonSerializer, ILogManager logManager)
        {
            _userManager = userManager;
            _libraryManager = libraryManager;
            _jsonSerializer = jsonSerializer;
            _logger = PluginLog.Wrap(logManager.GetLogger("HomeScreenCompanion_HSC"), "Home Screen");
            _log = new RunLog(ExecutionLog, _logger, "[Home Screen]", false);
        }

        public string Key => "HomeSectionSyncTask";
        public string Name => "Home Screen Sync";
        public string Description => "Copies home screen sections from each profile's source user to its target users.";
        public string Category => "Home Screen Companion";

        public IEnumerable<TaskTriggerInfo> GetDefaultTriggers()
        {
            return Array.Empty<TaskTriggerInfo>();
        }

        public async Task Execute(CancellationToken cancellationToken, IProgress<double> progress)
        {
            // One run at a time (see RunGate): a running sync / Run Group finishes first. Shown as
            // running ("Waiting for … to finish") meanwhile.
            IsRunning = true;
            if (RunGate.Busy) Progress.Set($"Waiting for {RunGate.Holder} to finish");
            IDisposable gate;
            try { gate = await RunGate.EnterAsync(Name, _logger, cancellationToken).ConfigureAwait(false); }
            catch { IsRunning = false; Progress.Clear(); throw; }
            using (gate) await ExecuteCore(cancellationToken, progress).ConfigureAwait(false);
        }

        private Task ExecuteCore(CancellationToken cancellationToken, IProgress<double> progress)
        {
            IsRunning = true;
            lock (ExecutionLog) { ExecutionLog.Clear(); }
            LastStartedUtc = DateTime.UtcNow;
            try
            {
                var config = Plugin.Instance?.Configuration;
                if (config == null) return Task.CompletedTask;

                bool debug = config.ExtendedConsoleOutput;
                _log = new RunLog(ExecutionLog, _logger, "[Home Screen]", debug, Progress);
                var startTime = DateTime.Now;
                _log.Rule();
                _log.Info($"Home Screen Sync  ·  {startTime:yyyy-MM-dd HH:mm}");
                _log.Rule();

                if (!config.HomeSyncEnabled)
                {
                    _log.Skip("Home Screen Sync is disabled in Settings — nothing to do");
                    return Task.CompletedTask;
                }

                var profiles = config.EffectiveHomeSyncProfiles()
                    .Where(p => p.Active && !string.IsNullOrWhiteSpace(p.SourceUserId)).ToList();
                if (profiles.Count == 0)
                {
                    _log.Warn("No active profile with a source user in Settings — nothing to sync");
                    LastSyncResult = "No source user configured.";
                    return Task.CompletedTask;
                }

                var managed = ManagedRows.Build(config, _libraryManager);
                var userStatus = UserStatus.Read(_userManager);
                // A user gets their home screen from one profile only, and a source user is never
                // overwritten by another profile (the page prevents both; old or hand-edited
                // settings are guarded here).
                var sourceKeys = new HashSet<string>(profiles.Select(p => NormId(p.SourceUserId)));
                var handledTargets = new HashSet<string>();
                var totals = new SyncTotals();
                progress.Report(5);

                for (int pi = 0; pi < profiles.Count; pi++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    SyncProfile(profiles[pi], managed, userStatus, sourceKeys, handledTargets, totals, cancellationToken);
                    progress.Report(5 + (int)(95.0 * (pi + 1) / profiles.Count));
                }
                if (totals.TopListsChanged) Plugin.Instance?.SaveConfiguration();

                int usersDone = totals.Targets - totals.Skipped - totals.Failed;
                LastSectionsCopied = totals.Copied;
                LastSyncTime = DateTime.Now.ToString("yyyy-MM-dd HH:mm");
                LastSyncResult = $"OK — {totals.Copied} section(s) copied to {totals.Targets - totals.Skipped} user(s)"
                    + (profiles.Count > 1 ? $" from {profiles.Count} source users." : ".");
                _log.Rule();
                _log.Info("Summary");
                _log.Info($"  Sections:      {totals.Copied} copied to {RunLog.Plural(usersDone, "user")}{(profiles.Count > 1 ? $" from {RunLog.Plural(profiles.Count, "source user")}" : "")}{(totals.Failed > 0 ? $", {RunLog.Plural(totals.Failed, "user")} failed" : "")}");
                if (userStatus.SummaryLine() is string disabledLine) _log.Info("  " + disabledLine);
                _log.Info($"  Done in {RunLog.Elapsed(DateTime.Now - startTime)}  ·  {(totals.Failed > 0 ? "✖ Completed with " + RunLog.Plural(totals.Failed, "error") : "✔ Completed")}");
                _log.Rule();
                progress.Report(100);
            }
            catch (OperationCanceledException)
            {
                LastSyncResult = "Cancelled.";
                _log.Warn("Sync was cancelled");
            }
            catch (Exception ex)
            {
                LastSyncResult = $"Error: {ex.Message}";
                _log.Error($"Sync aborted: {ex.Message}");
            }
            finally
            {
                IsRunning = false;
                Progress.Clear();
                PersistLog();
            }

            return Task.CompletedTask;
        }

        private sealed class SyncTotals
        {
            public int Copied, Targets, Failed, Skipped;
            public bool TopListsChanged;
        }

        // One profile: the source user's home screen (and library order) to its target users.
        private void SyncProfile(HomeSyncProfile profile, ManagedRows managed, UserStatus userStatus, HashSet<string> sourceKeys,
            HashSet<string> handledTargets, SyncTotals totals, CancellationToken cancellationToken)
        {
            string sourceKey = NormId(profile.SourceUserId);
            string sourceName = UserName(sourceKey);
            _log.Blank();
            _log.Info($"» {sourceName}");

            // Targets this profile may write: not the source user of any profile, not already
            // written by an earlier profile.
            var targets = new List<string>();
            foreach (var t in profile.TargetUserIds ?? new List<string>())
            {
                var key = NormId(t);
                if (key.Length == 0) continue;
                if (sourceKeys.Contains(key)) { _log.Warn($"{UserName(key)}: skipped — is the source user of a profile"); continue; }
                if (!handledTargets.Add(key)) { _log.Warn($"{UserName(key)}: skipped — already gets its home screen from another profile"); continue; }
                targets.Add(t);
            }
            if (targets.Count == 0)
            {
                _log.Warn($"No target users selected for {sourceName} — nothing to copy");
                return;
            }

            var sourceSections = _userManager.GetHomeSections(_userManager.GetInternalId(profile.SourceUserId), cancellationToken);
            if (sourceSections?.Sections == null || sourceSections.Sections.Length == 0)
            {
                _log.Warn($"Master user {sourceName} has no home screen sections — nothing to copy");
                return;
            }

            _log.Info($"  Master: {sourceName}  ·  {RunLog.Plural(sourceSections.Sections.Length, "section")}  ·  {RunLog.Plural(targets.Count, "target user")}");
            _log.Section($"Master sections ({sourceName})");
            foreach (var s in sourceSections.Sections)
                _log.Debug($"  [{s.SectionType}] \"{s.CustomName ?? s.Name}\"");

            for (int i = 0; i < targets.Count; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                _log.Count($"Home Screen Sync — {sourceName}:", i + 1, targets.Count, "users");
                totals.Targets++;

                var targetIdStr = targets[i];
                string targetName = UserName(NormId(targetIdStr));
                // Disabled in Emby: their home screen is left as it is.
                if (userStatus.Skip(targetIdStr))
                {
                    totals.Skipped++;
                    _log.Skip($"{targetName}: skipped — disabled in Emby (home screen left as it is)");
                    continue;
                }
                try
                {
                    var targetInternalId = _userManager.GetInternalId(targetIdStr);

                    var existing = _userManager.GetHomeSections(targetInternalId, cancellationToken);
                    string targetKey = NormId(targetIdStr);

                    // Per-user HSC playlist rows (e.g. Your Next Watch) that an earlier sync copied
                    // here pointing at another user's playlist: say what is re-pointed or removed.
                    // Shared rows (top lists, tag/collection/items rows) are never removed.
                    foreach (var s in existing?.Sections ?? Array.Empty<ContentSection>())
                    {
                        var row = managed.PlaylistRow(s);
                        if (row == null) continue;
                        string label = s.CustomName ?? s.Name ?? row.Name;
                        if (!managed.PlaylistOwners.TryGetValue(s.ParentId, out var owner) || owner == targetKey) continue;
                        string ownerName = UserName(owner);
                        if (row.PlaylistFor(targetKey) != null)
                            _log.Info($"  {targetName}: '{label}' row pointed at {ownerName}'s playlist — now points at {targetName}'s own");
                        else
                            _log.Skip($"{targetName}: removed copied '{label}' row (belongs to {ownerName})");
                    }

                    // Top-list rows: only for the list's own users. A row of a list the master
                    // is not on is kept (put back below at its place); one copied here earlier
                    // for a list this user is not on goes.
                    var keptTopListRows = new Dictionary<TopListHomeSection, ContentSection>();
                    foreach (var s in existing?.Sections ?? Array.Empty<ContentSection>())
                    {
                        var tl = managed.TopListRow(s);
                        if (tl == null) continue;
                        if (!managed.IsTopListUser(tl, targetKey))
                            _log.Skip($"{targetName}: removed copied '{s.CustomName ?? s.Name}' row (top-list for other users)");
                        else if (!managed.MasterHasRow(tl, sourceSections.Sections) && !keptTopListRows.ContainsKey(tl))
                        {
                            keptTopListRows[tl] = s;
                            HomeScreenCompanionTask.RememberRowPosition(_userManager, _jsonSerializer, TopListRowKey(tl), targetIdStr, targetInternalId, s.Id, cancellationToken);
                        }
                    }

                    if (existing?.Sections?.Length > 0)
                    {
                        _log.Debug($"  {targetName}: replacing {existing.Sections.Length} existing sections");
                        var idsToDelete = existing.Sections
                            .Where(s => !string.IsNullOrEmpty(s.Id))
                            .Select(s => s.Id)
                            .ToArray();
                        if (idsToDelete.Length > 0)
                            _userManager.DeleteHomeSections(targetInternalId, idsToDelete, cancellationToken);
                    }

                    int copiedHere = 0;
                    var notCopied = new List<string>();   // rows this user does not get (said in the log)
                    foreach (var section in sourceSections.Sections)
                    {
                        var copy = CopySection(section);
                        // Per-user HSC playlist rows are personalised and top-list rows go only to the
                        // list's own users; every other row (tag/collection/items rows) is copied to all targets.
                        var row = managed.PlaylistRow(section);
                        if (row != null)
                        {
                            // A per-user playlist row: same place, but this user's own playlist.
                            var own = row.PlaylistFor(targetKey);
                            if (own == null)
                            {
                                _log.Debug($"  {targetName}: – '{section.CustomName ?? section.Name}' skipped (no own playlist from that source)");
                                notCopied.Add($"'{section.CustomName ?? section.Name}' (no own playlist from that source)");
                                continue;
                            }
                            copy.ParentId = own;
                        }
                        var topList = managed.TopListRow(section);
                        if (topList != null && !managed.IsTopListUser(topList, targetKey))
                        {
                            _log.Debug($"  {targetName}: – '{section.CustomName ?? section.Name}' skipped (top-list for other users)");
                            notCopied.Add($"'{section.CustomName ?? section.Name}' (top-list: not one of its users)");
                            continue;
                        }
                        _log.Debug($"  {targetName}: + [{section.SectionType}] \"{section.CustomName ?? section.Name}\"");
                        _userManager.AddHomeSection(targetInternalId, copy, cancellationToken);
                        totals.Copied++;
                        copiedHere++;
                    }

                    // This user's top-list rows: the one copied from the master, else their
                    // own put back at its place (a new one at the bottom). Tracked, so the
                    // top-list sync keeps updating it and never adds a second one.
                    foreach (var tl in managed.TopLists)
                    {
                        if (!managed.IsTopListUser(tl, targetKey)) continue;
                        var before = _userManager.GetHomeSections(targetInternalId, cancellationToken)?.Sections ?? Array.Empty<ContentSection>();
                        var row = before.FirstOrDefault(s => managed.TopListRow(s) == tl);
                        if (row == null)
                        {
                            bool ownKept = keptTopListRows.TryGetValue(tl, out var kept);
                            var add = ownKept ? CopySection(kept) : managed.NewTopListRow(tl, _jsonSerializer);
                            _userManager.AddHomeSection(targetInternalId, add, cancellationToken);
                            row = (_userManager.GetHomeSections(targetInternalId, cancellationToken)?.Sections ?? Array.Empty<ContentSection>())
                                .FirstOrDefault(s => !string.IsNullOrEmpty(s.Id) && !before.Any(b => b.Id == s.Id));
                            if (row == null) continue;
                            // Back where it was, else (a new one) at the list's Position.
                            if (!HomeScreenCompanionTask.RestoreRowPosition(_userManager, _jsonSerializer, TopListRowKey(tl), targetIdStr, targetInternalId, row.Id, cancellationToken) && !ownKept)
                                HomeScreenCompanionTask.PlaceNewRow(_userManager, targetInternalId, row.Id, RowPlacement.PositionIn(tl.HomeSectionSettings), cancellationToken,
                                    TopListSyncTask.OtherUsers(tl, targetIdStr), row.CustomName);
                            _log.Debug(ownKept
                                ? $"  {targetName}: = '{row.CustomName ?? row.Name}' kept (top-list row)"
                                : $"  {targetName}: + '{row.CustomName ?? row.Name}' added (top-list row; they had none)");
                            copiedHere++;
                        }
                        var tracked = (tl.HomeSectionTracked ??= new List<HomeSectionTracking>()).FirstOrDefault(t => NormId(t.UserId) == targetKey);
                        if (tracked == null) tl.HomeSectionTracked.Add(new HomeSectionTracking { UserId = targetIdStr, SectionId = row.Id });
                        else tracked.SectionId = row.Id;
                        totals.TopListsChanged = true;
                    }

                    _log.Ok($"{targetName}: {RunLog.Plural(copiedHere, "section")} copied"
                        + (notCopied.Count > 0 ? $"  ·  not copied: {string.Join(", ", notCopied)}" : ""));
                }
                catch (Exception ex)
                {
                    totals.Failed++;
                    _log.Error($"{targetName}: could not copy home screen — {ex.Message}");
                }
            }

            if (profile.LibraryOrder) CopyLibraryOrder(profile.SourceUserId, sourceName, targets, userStatus);
        }

        private void CopyLibraryOrder(string sourceUserId, string sourceName, List<string> targets, UserStatus userStatus)
        {
            _log.Info($"  Library order ({sourceName})");
            try
            {
                var sourceUser = _userManager.GetUserById(sourceUserId);
                if (sourceUser == null) { _log.Warn("Master user could not be loaded — library order not copied"); return; }
                var sourceConf = _userManager.GetUserConfiguration(sourceUser);
                if (sourceConf?.OrderedViews == null || sourceConf.OrderedViews.Length == 0)
                {
                    _log.Skip($"{sourceName} has no custom library order — nothing to copy");
                    return;
                }
                _log.Debug($"  Master order: [{string.Join(", ", sourceConf.OrderedViews)}]");
                foreach (var targetIdStr in targets)
                {
                    if (userStatus.IsDisabled(targetIdStr)) continue;
                    string targetName = UserName(NormId(targetIdStr));
                    try
                    {
                        var targetUser = _userManager.GetUserById(targetIdStr);
                        if (targetUser == null) continue;
                        var targetConf = _userManager.GetUserConfiguration(targetUser);
                        if (targetConf == null) continue;
                        targetConf.OrderedViews = sourceConf.OrderedViews;
                        _userManager.UpdateConfiguration(_userManager.GetInternalId(targetIdStr), targetConf);
                        _log.Ok($"{targetName}: library order copied");
                    }
                    catch (Exception ex)
                    {
                        _log.Warn($"{targetName}: library order could not be copied — {ex.Message}");
                    }
                }
            }
            catch (Exception ex)
            {
                _log.Error($"Library order sync failed: {ex.Message}");
            }
        }

        private static string NormId(string id) => (id ?? "").Replace("-", "").Trim().ToLowerInvariant();

        private string UserName(string normId)
            => Guid.TryParse(normId, out var g) ? (_userManager.GetUserById(g)?.Name ?? normId) : normId;

        // Per-user playlist rows HSC manages (e.g. Your Next Watch): one playlist per user from a
        // source. Copy & Sync must never hand one user's playlist row to another user.
        private sealed class ManagedRow
        {
            public string Name = "";
            public Dictionary<string, string> Playlists = new Dictionary<string, string>(); // user -> playlist InternalId
            public string? PlaylistFor(string user) => Playlists.TryGetValue(user, out var p) ? p : null;
        }

        private sealed class ManagedRows
        {
            public readonly Dictionary<string, string> PlaylistOwners = new Dictionary<string, string>(); // playlist InternalId -> user
            private readonly Dictionary<string, ManagedRow> _byPlaylist = new Dictionary<string, ManagedRow>();

            public static ManagedRows Build(PluginConfiguration config, ILibraryManager libraryManager)
            {
                var m = new ManagedRows();
                foreach (var tc in config.Tags ?? new List<TagConfig>())
                {
                    if (tc.PlaylistMappings == null || tc.PlaylistMappings.Count == 0) continue;
                    var row = new ManagedRow { Name = !string.IsNullOrWhiteSpace(tc.Name) ? tc.Name : (tc.Tag ?? "") };
                    foreach (var pm in tc.PlaylistMappings)
                    {
                        if (!Guid.TryParse(pm.PlaylistId, out var g)) continue;
                        var pl = libraryManager.GetItemById(g);
                        if (pl == null || !pl.GetType().Name.Contains("Playlist")) continue;
                        var iid = pl.InternalId.ToString(System.Globalization.CultureInfo.InvariantCulture);
                        row.Playlists[NormId(pm.UserId)] = iid;
                        m.PlaylistOwners[iid] = NormId(pm.UserId);
                        m._byPlaylist[iid] = row;
                    }
                }
                foreach (var tl in config.TopLists ?? new List<TopListHomeSection>())
                {
                    if (string.IsNullOrEmpty(tl.HomeSectionLibraryId) || tl.HomeSectionLibraryId == "auto") continue;
                    m.TopLists.Add(tl);
                    m._topListUsers[tl] = new HashSet<string>((tl.HomeSectionUserIds ?? new List<string>()).Select(NormId));
                    foreach (var id in TopListSyncTask.OwnLibraryIds(tl.HomeSectionLibraryId, libraryManager))
                        m._byTopListLibrary[id] = tl;
                }
                return m;
            }

            // A per-user HSC playlist row: the section shows one of HSC's per-user playlists.
            public ManagedRow? PlaylistRow(ContentSection s)
                => !string.IsNullOrEmpty(s.ParentId) && _byPlaylist.TryGetValue(s.ParentId, out var pr) ? pr : null;

            // Top-list rows (movies and shows): an items row on the list's own library, only for
            // the list's users.
            public readonly List<TopListHomeSection> TopLists = new List<TopListHomeSection>();
            private readonly Dictionary<TopListHomeSection, HashSet<string>> _topListUsers = new Dictionary<TopListHomeSection, HashSet<string>>();
            private readonly Dictionary<string, TopListHomeSection> _byTopListLibrary = new Dictionary<string, TopListHomeSection>(StringComparer.OrdinalIgnoreCase);

            public TopListHomeSection? TopListRow(ContentSection s)
                => string.Equals(s.SectionType, "items", StringComparison.OrdinalIgnoreCase) && !string.IsNullOrEmpty(s.ParentId)
                   && _byTopListLibrary.TryGetValue(s.ParentId.Trim().Replace("-", ""), out var tl) ? tl : null;

            public bool IsTopListUser(TopListHomeSection tl, string user) => _topListUsers[tl].Contains(user);

            public bool MasterHasRow(TopListHomeSection tl, ContentSection[] masterSections) => masterSections.Any(s => TopListRow(s) == tl);

            // A new row for a list user without one, as the top-list's own setup makes it.
            public ContentSection NewTopListRow(TopListHomeSection tl, IJsonSerializer json)
            {
                var settings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                try
                {
                    if (!string.IsNullOrEmpty(tl.HomeSectionSettings) && tl.HomeSectionSettings != "{}")
                        settings = json.DeserializeFromString<Dictionary<string, string>>(tl.HomeSectionSettings) ?? settings;
                }
                catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
                if (!settings.ContainsKey("SectionType")) settings["SectionType"] = "items";
                return HomeScreenCompanionTask.BuildContentSection(json, settings, tl.HomeSectionLibraryId);
            }
        }

        // Row-position memory key of a top-list row (kept apart from the tag rows' keys).
        private static string TopListRowKey(TopListHomeSection tl) => TopListSyncTask.RowPositionKey(tl);

        private static ContentSection CopySection(ContentSection source)
        {
            var copy = new ContentSection();
            foreach (var prop in typeof(ContentSection).GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                if (prop.Name == "Id") continue;
                if (prop.CanRead && prop.CanWrite)
                    prop.SetValue(copy, prop.GetValue(source));
            }
            return copy;
        }
    }
}
