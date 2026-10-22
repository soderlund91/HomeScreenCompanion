using MediaBrowser.Controller.Entities;
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
    // A row of a source user's home screen, as the Copy selected picker lists it.
    public class HomeSyncRowChoice
    {
        public string Key { get; set; } = "";
        public string Title { get; set; } = "";
        public string Kind { get; set; } = "";   // "source", "playlist", "toplist" or "emby"
    }

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

                var managed = ManagedRows.Build(config, _libraryManager, _jsonSerializer);
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
                    // One profile failing (e.g. its source user was deleted) never stops the others,
                    // nor the save of what they changed.
                    try { SyncProfile(config, profiles[pi], managed, userStatus, sourceKeys, handledTargets, totals, cancellationToken); }
                    catch (OperationCanceledException) { throw; }
                    catch (Exception ex)
                    {
                        totals.Failed++;
                        _log.Error($"Profile of {UserName(NormId(profiles[pi].SourceUserId))} could not be synced — {ex.Message}");
                    }
                    progress.Report(5 + (int)(95.0 * (pi + 1) / profiles.Count));
                }
                RemoveStaleAdditions(config, managed, handledTargets, sourceKeys, totals);
                if (totals.ConfigChanged) Plugin.Instance?.SaveConfiguration();

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
            public bool ConfigChanged;   // top-list or source row tracking updated
        }

        // One profile: the source user's home screen (and library order) to its target users.
        private void SyncProfile(PluginConfiguration config, HomeSyncProfile profile, ManagedRows managed, UserStatus userStatus, HashSet<string> sourceKeys,
            HashSet<string> handledTargets, SyncTotals totals, CancellationToken cancellationToken)
        {
            string sourceKey = NormId(profile.SourceUserId);
            string sourceName = UserName(sourceKey);
            _log.Blank();
            _log.Info($"» {sourceName}");
            if (!Guid.TryParse(profile.SourceUserId, out var srcGuid) || _userManager.GetUserById(srcGuid) == null)
            {
                _log.Warn("The source user of this profile no longer exists in Emby — profile skipped (pick a new source user in Settings)");
                // Its targets are left as they are (what Copy & Sync added for them stays too).
                foreach (var t in profile.TargetUserIds ?? new List<string>()) if (NormId(t).Length > 0) handledTargets.Add(NormId(t));
                return;
            }

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

            // The master's rows by identity (RowKey), and those this profile copies.
            var masterRows = sourceSections.Sections.Select(s => (Section: s, Key: managed.RowKey(config, s, sourceKey))).ToList();
            bool selectedOnly = profile.CopiesSelected();
            bool mirror = !selectedOnly && !profile.KeepOwnRows;
            var selected = new HashSet<string>(profile.SelectedRows ?? new List<string>(), StringComparer.OrdinalIgnoreCase);
            var wanted = selectedOnly ? masterRows.Where(r => selected.Contains(r.Key)).ToList() : masterRows;
            var wantedKeys = new HashSet<string>(wanted.Select(r => r.Key), StringComparer.OrdinalIgnoreCase);
            _log.Info("  " + (selectedOnly ? $"Copies {RunLog.Plural(wanted.Count, "selected row")}"
                : mirror ? "Copies everything (exact copy)" : "Copies everything (keeps each user's own rows)"));
            foreach (var key in selected.Where(k => !masterRows.Any(r => string.Equals(r.Key, k, StringComparison.OrdinalIgnoreCase))))
                _log.Skip($"Selected row {RowKeyLabel(key)} is not on {sourceName}'s home screen — not copied");

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
                    string targetKey = NormId(targetIdStr);

                    // The sources and top-lists of the rows this user gets: on them (Copy & Sync
                    // remembers whom it added), and off what it added for rows no longer copied.
                    // An exact copy also takes the user off every other source and top-list, so
                    // the full sync does not put those rows back.
                    if (UpdateMemberships(config, managed, wanted, wantedKeys, mirror, selectedOnly, targetIdStr, targetName, targetInternalId, sourceName, cancellationToken))
                        totals.ConfigChanged = true;

                    var notCopied = new List<string>();   // rows this user does not get (said in the log)
                    int copiedHere = selectedOnly
                        ? CopySelectedRows(config, managed, profile, sourceSections.Sections, wanted, targetIdStr, targetName, targetInternalId, totals, notCopied, cancellationToken)
                        : CopyAllRows(config, managed, sourceKey, sourceSections.Sections, mirror, targetIdStr, targetName, targetInternalId, totals, notCopied, cancellationToken);

                    // This user's top-list rows: the one copied from the master, else their
                    // own (put back above), else a new one at the list's Position. Tracked, so the
                    // top-list sync keeps updating it and never adds a second one.
                    foreach (var tl in managed.TopLists)
                    {
                        if (!managed.IsTopListUser(tl, targetKey)) continue;
                        var before = _userManager.GetHomeSections(targetInternalId, cancellationToken)?.Sections ?? Array.Empty<ContentSection>();
                        var row = before.FirstOrDefault(s => managed.TopListRow(s) == tl);
                        if (row == null)
                        {
                            _userManager.AddHomeSection(targetInternalId, managed.NewTopListRow(tl, _jsonSerializer), cancellationToken);
                            row = (_userManager.GetHomeSections(targetInternalId, cancellationToken)?.Sections ?? Array.Empty<ContentSection>())
                                .FirstOrDefault(s => !string.IsNullOrEmpty(s.Id) && !before.Any(b => b.Id == s.Id));
                            if (row == null) continue;
                            // Back where it was, else (a new one) at the list's Position.
                            if (!HomeScreenCompanionTask.RestoreRowPosition(_userManager, _jsonSerializer, TopListRowKey(tl), targetIdStr, targetInternalId, row.Id, cancellationToken))
                                HomeScreenCompanionTask.PlaceNewRow(_userManager, targetInternalId, row.Id, RowPlacement.PositionIn(tl.HomeSectionSettings), cancellationToken,
                                    TopListSyncTask.OtherUsers(tl, targetIdStr), row.CustomName);
                            _log.Debug($"  {targetName}: + '{row.CustomName ?? row.Name}' added (top-list row; they had none)");
                            copiedHere++;
                        }
                        var tracked = (tl.HomeSectionTracked ??= new List<HomeSectionTracking>()).FirstOrDefault(t => NormId(t.UserId) == targetKey);
                        if (tracked == null) tl.HomeSectionTracked.Add(new HomeSectionTracking { UserId = targetIdStr, SectionId = row.Id });
                        else tracked.SectionId = row.Id;
                        totals.ConfigChanged = true;
                    }

                    // Source rows tracked for this user, so the full sync updates the row it finds
                    // here (copied or kept) and never adds a second one.
                    if (TrackSourceRows(config, managed, targetIdStr, targetInternalId, cancellationToken))
                        totals.ConfigChanged = true;

                    _log.Ok($"{targetName}: {RunLog.Plural(copiedHere, selectedOnly ? "selected row" : "section")} copied"
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

        // Copy all: the user's home screen replaced by the master's. Rows of sources and top-lists
        // the user is on and the master is not are kept (put back at their place), unless it is
        // an exact copy (mirror: the user is already off those, see UpdateMemberships).
        private int CopyAllRows(PluginConfiguration config, ManagedRows managed, string sourceKey, ContentSection[] masterSections, bool mirror,
            string targetIdStr, string targetName, long targetInternalId, SyncTotals totals, List<string> notCopied, CancellationToken cancellationToken)
        {
            var existing = _userManager.GetHomeSections(targetInternalId, cancellationToken);
            string targetKey = NormId(targetIdStr);

            // Per-user HSC playlist rows (e.g. Your Next Watch) that an earlier sync copied
            // here pointing at another user's playlist: say what is re-pointed or removed.
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
            // A source's row (tag / collection / playlist row) the same way: a user of the
            // source keeps it when the master is not one of its users.
            var kept = new List<KeptRow>();
            var existingSections = existing?.Sections ?? Array.Empty<ContentSection>();
            for (int idx = 0; idx < existingSections.Length; idx++)
            {
                var s = existingSections[idx];
                var tl = managed.TopListRow(s);
                if (tl != null)
                {
                    if (!managed.IsTopListUser(tl, targetKey))
                        _log.Skip($"{targetName}: removed copied '{s.CustomName ?? s.Name}' row (top-list for other users)");
                    else if (!mirror && !managed.MasterHasRow(tl, masterSections) && !kept.Any(k => k.TopList == tl))
                        kept.Add(new KeptRow { Index = idx, Row = s, TopList = tl, PositionKey = TopListRowKey(tl) });
                    continue;
                }
                if (mirror) continue;
                // (Not a copied playlist row that shows another user's playlist.)
                if (managed.PlaylistRow(s) != null && managed.PlaylistOwners.TryGetValue(s.ParentId, out var plOwner) && plOwner != targetKey) continue;
                var tc = managed.SourceRow(config, s, targetKey);
                if (tc != null && !managed.MasterHasSourceRow(config, tc, sourceKey, masterSections) && !kept.Any(k => k.Source == tc))
                    kept.Add(new KeptRow { Index = idx, Row = s, Source = tc, PositionKey = SourceRowKey(tc) });
            }
            foreach (var k in kept)
                HomeScreenCompanionTask.RememberRowPosition(_userManager, _jsonSerializer, k.PositionKey, targetIdStr, targetInternalId, k.Row.Id, cancellationToken);

            if (existingSections.Length > 0)
            {
                _log.Debug($"  {targetName}: replacing {existingSections.Length} existing sections");
                var idsToDelete = existingSections
                    .Where(s => !string.IsNullOrEmpty(s.Id))
                    .Select(s => s.Id)
                    .ToArray();
                if (idsToDelete.Length > 0)
                    _userManager.DeleteHomeSections(targetInternalId, idsToDelete, cancellationToken);
            }

            int copiedHere = 0;
            foreach (var section in masterSections)
            {
                var copy = PrepareCopy(managed, section, targetKey, targetName, notCopied);
                if (copy == null) continue;
                _log.Debug($"  {targetName}: + [{section.SectionType}] \"{section.CustomName ?? section.Name}\"");
                _userManager.AddHomeSection(targetInternalId, copy, cancellationToken);
                totals.Copied++;
                copiedHere++;
            }

            // The kept rows back at their places, top to bottom (each lands at its old index).
            foreach (var k in kept.OrderBy(k => k.Index))
            {
                var before = _userManager.GetHomeSections(targetInternalId, cancellationToken)?.Sections ?? Array.Empty<ContentSection>();
                // (Not when a row copied from the master already is that source's row.)
                ContentSection? row = k.Source != null
                    ? before.FirstOrDefault(s => managed.TopListRow(s) == null && managed.SourceRow(config, s, targetKey) == k.Source)
                    : null;
                if (row == null)
                {
                    _userManager.AddHomeSection(targetInternalId, CopySection(k.Row), cancellationToken);
                    row = (_userManager.GetHomeSections(targetInternalId, cancellationToken)?.Sections ?? Array.Empty<ContentSection>())
                        .FirstOrDefault(s => !string.IsNullOrEmpty(s.Id) && !before.Any(b => b.Id == s.Id));
                    if (row == null) continue;
                    HomeScreenCompanionTask.RestoreRowPosition(_userManager, _jsonSerializer, k.PositionKey, targetIdStr, targetInternalId, row.Id, cancellationToken);
                    copiedHere++;
                }
                _log.Debug($"  {targetName}: = '{row.CustomName ?? row.Name}' kept ({(k.Source != null ? "source row; the master is not one of its users" : "top-list row")})");
            }
            return copiedHere;
        }

        // Copy selected: only the selected rows, each updated where the user has it, else added
        // next to the row above it on the master's home screen. The user's other rows stay.
        private int CopySelectedRows(PluginConfiguration config, ManagedRows managed, HomeSyncProfile profile, ContentSection[] masterSections, List<(ContentSection Section, string Key)> wanted,
            string targetIdStr, string targetName, long targetInternalId, SyncTotals totals, List<string> notCopied, CancellationToken cancellationToken)
        {
            string targetKey = NormId(targetIdStr);
            var additions = config.HomeSyncAdditions ??= new List<HomeSyncAddition>();
            int copiedHere = 0;
            foreach (var (section, key) in wanted)
            {
                var copy = PrepareCopy(managed, section, targetKey, targetName, notCopied);
                if (copy == null) continue;
                string label = section.CustomName ?? section.Name ?? section.SectionType ?? "";
                var current = _userManager.GetHomeSections(targetInternalId, cancellationToken)?.Sections ?? Array.Empty<ContentSection>();
                var match = current.FirstOrDefault(s => string.Equals(managed.RowKey(config, s, targetKey), key, StringComparison.OrdinalIgnoreCase));
                if (match != null)
                {
                    typeof(ContentSection).GetProperty("Id")?.SetValue(copy, match.Id);
                    _userManager.UpdateHomeSection(targetInternalId, copy, cancellationToken);
                    _log.Debug($"  {targetName}: = '{label}' updated");
                }
                else
                {
                    _userManager.AddHomeSection(targetInternalId, copy, cancellationToken);
                    var added = (_userManager.GetHomeSections(targetInternalId, cancellationToken)?.Sections ?? Array.Empty<ContentSection>())
                        .FirstOrDefault(s => !string.IsNullOrEmpty(s.Id) && !current.Any(b => b.Id == s.Id));
                    if (added != null)
                    {
                        // Below the row that is above it on the master's home screen. When this user
                        // does not have that row, PlaceNewRow looks on the master's screen (the
                        // reference user) for the nearest row above it that this user has; else the top.
                        int at = Array.FindIndex(masterSections, s => s.Id == section.Id);
                        string position = at > 0 ? "after:" + HomeScreenCompanionTask.SectionKey(masterSections[at - 1]) : "top";
                        HomeScreenCompanionTask.PlaceNewRow(_userManager, targetInternalId, added.Id, position, cancellationToken,
                            new[] { profile.SourceUserId }, copy.CustomName);
                        // A row of no source or top-list: remembered, so unticking it removes it again.
                        if (key.StartsWith(RowPrefix, StringComparison.Ordinal) && !HasAddition(additions, HomeSyncAddition.Row, key, targetKey))
                        {
                            additions.Add(new HomeSyncAddition { List = HomeSyncAddition.Row, Key = key, UserId = targetIdStr });
                            totals.ConfigChanged = true;
                        }
                    }
                    _log.Debug($"  {targetName}: + [{section.SectionType}] \"{label}\"");
                }
                totals.Copied++;
                copiedHere++;
            }
            return copiedHere;
        }

        // The copy of a master row for this user; null when they do not get it (said in notCopied).
        // Per-user HSC playlist rows show the user's own playlist, top-list rows go only to the
        // list's own users; every other row is copied as it is.
        private ContentSection? PrepareCopy(ManagedRows managed, ContentSection section, string targetKey, string targetName, List<string> notCopied)
        {
            var copy = CopySection(section);
            var row = managed.PlaylistRow(section);
            if (row != null)
            {
                var own = row.PlaylistFor(targetKey);
                if (own == null)
                {
                    _log.Debug($"  {targetName}: – '{section.CustomName ?? section.Name}' skipped (no own playlist from that source yet)");
                    notCopied.Add($"'{section.CustomName ?? section.Name}' (no own playlist yet: made by the next full sync)");
                    return null;
                }
                copy.ParentId = own;
            }
            var topList = managed.TopListRow(section);
            if (topList != null && !managed.IsTopListUser(topList, targetKey))
            {
                _log.Debug($"  {targetName}: – '{section.CustomName ?? section.Name}' skipped (top-list for other users)");
                notCopied.Add($"'{section.CustomName ?? section.Name}' (top-list: not one of its users)");
                return null;
            }
            return copy;
        }

        // The user on the sources and top-lists of the rows they get from the master (remembered
        // in HomeSyncAdditions when Copy & Sync put them there), and off what Copy & Sync added
        // for rows they no longer get. Copy selected also removes those rows (Copy all replaces
        // the whole home screen anyway). An exact copy (mirror) takes the user off every other
        // source and top-list too. True when the settings changed.
        private bool UpdateMemberships(PluginConfiguration config, ManagedRows managed, List<(ContentSection Section, string Key)> wanted, HashSet<string> wantedKeys,
            bool mirror, bool selectedOnly, string targetIdStr, string targetName, long targetInternalId, string sourceName, CancellationToken cancellationToken)
        {
            string targetKey = NormId(targetIdStr);
            var additions = config.HomeSyncAdditions ??= new List<HomeSyncAddition>();
            bool changed = false;

            // Rows no longer copied here: what Copy & Sync added for them goes.
            var gone = additions.Where(a => NormId(a.UserId) == targetKey
                && (!wantedKeys.Contains(a.Key) || (a.List == HomeSyncAddition.Row && !selectedOnly))).ToList();
            if (gone.Count > 0)
            {
                if (selectedOnly)
                {
                    // Found before the user is taken off the source (a source row is known by its users).
                    var goneKeys = new HashSet<string>(gone.Select(a => a.Key), StringComparer.OrdinalIgnoreCase);
                    var rows = (_userManager.GetHomeSections(targetInternalId, cancellationToken)?.Sections ?? Array.Empty<ContentSection>())
                        .Where(s => !string.IsNullOrEmpty(s.Id) && goneKeys.Contains(managed.RowKey(config, s, targetKey))).ToList();
                    if (rows.Count > 0)
                    {
                        _userManager.DeleteHomeSections(targetInternalId, rows.Select(s => s.Id).ToArray(), cancellationToken);
                        foreach (var s in rows) _log.Skip($"{targetName}: removed '{s.CustomName ?? s.Name}' row (no longer selected)");
                    }
                }
                foreach (var a in gone)
                {
                    if (a.List != HomeSyncAddition.Row && RemoveMember(config, managed, a.Key, a.List, targetKey, rowGone: true))
                        _log.Info($"  {targetName}: taken off {ListLabel(a.Key, a.List)} again (no longer gets that row through Copy & Sync)");
                    additions.Remove(a);
                }
                changed = true;
            }

            // On the lists of the rows they get.
            foreach (var (section, key) in wanted)
            {
                if (!key.StartsWith(SrcPrefix, StringComparison.Ordinal) && !key.StartsWith(TlPrefix, StringComparison.Ordinal)) continue;
                changed |= AddMember(config, managed, key, HomeSyncAddition.Home, targetIdStr, targetName, sourceName);
                if (managed.PlaylistRow(section) != null)
                    changed |= AddMember(config, managed, key, HomeSyncAddition.Playlist, targetIdStr, targetName, sourceName);
            }

            // An exact copy: off every source and top-list whose row the master does not have, when
            // the master would have it now if it were on it (see MirrorMayRemove).
            if (mirror)
            {
                // Copy all: every master row. Those known as no source's or top-list's row.
                var unknownMasterRows = wanted.Where(w => w.Key.StartsWith(RowPrefix, StringComparison.Ordinal)).Select(w => w.Section).ToArray();
                foreach (var key in MemberKeys(config, targetKey).Where(k => !wantedKeys.Contains(k)).ToList())
                {
                    if (!MirrorMayRemove(config, managed, key, unknownMasterRows))
                    {
                        _log.Debug($"  {targetName}: stays on {ListLabel(key, HomeSyncAddition.Home)} (switched off, outside its schedule, or the master's row could not be told apart)");
                        continue;
                    }
                    if (RemoveMember(config, managed, key, HomeSyncAddition.Home, targetKey, rowGone: true))
                        _log.Info($"  {targetName}: taken off {ListLabel(key, HomeSyncAddition.Home)} (exact copy of {sourceName}'s home screen)");
                    additions.RemoveAll(a => NormId(a.UserId) == targetKey && string.Equals(a.Key, key, StringComparison.OrdinalIgnoreCase));
                    changed = true;
                }
            }
            return changed;
        }

        // An exact copy takes the user off a source or top-list only when its row would be on the
        // master's home screen now if the master were one of its users. Not for a source switched
        // off, nor one outside its schedule whose row is removed then (a tag row stays, titled
        // [INACTIVE], and is copied like any row), nor when the master has a row with the source's
        // title that is no source's row (it may be this source's row, copied to the master by hand):
        // the user would lose a membership for good.
        private bool MirrorMayRemove(PluginConfiguration config, ManagedRows managed, string key, ContentSection[] unknownMasterRows)
        {
            if (key.StartsWith(SrcPrefix, StringComparison.Ordinal))
            {
                var entries = (config.Tags ?? new List<TagConfig>()).Where(t => string.Equals(SourceKey(t.Name, t.Tag), key, StringComparison.OrdinalIgnoreCase)).ToList();
                if (entries.Count == 0) return false;
                foreach (var tc in entries)
                {
                    if (!tc.Active) return false;
                    if (!HomeScreenCompanionTask.IsScheduleActive(tc.ActiveIntervals, DateTime.Now) && !HomeScreenCompanionTask.IsTagRow(tc, _jsonSerializer)) return false;
                    if (unknownMasterRows.Any(s => managed.CouldBeSourceRow(s, tc))) return false;
                }
                return true;
            }
            if (key.StartsWith(TlPrefix, StringComparison.Ordinal))
            {
                var tag = key.Substring(TlPrefix.Length);
                var tl = (config.TopLists ?? new List<TopListHomeSection>()).FirstOrDefault(t => string.Equals((t.TagName ?? "").Trim(), tag, StringComparison.OrdinalIgnoreCase));
                if (tl == null) return false;
                if (!tl.FromSource) return true;
                var owner = (config.Tags ?? new List<TagConfig>()).FirstOrDefault(t => SourceTopLists.IsOn(t) && string.Equals((t.Tag ?? "").Trim(), tag, StringComparison.OrdinalIgnoreCase));
                return owner != null && owner.Active;
            }
            return false;
        }

        // Every source and top-list key whose home row this user is selected for.
        private static List<string> MemberKeys(PluginConfiguration config, string userKey)
        {
            var keys = new List<string>();
            foreach (var tc in config.Tags ?? new List<TagConfig>())
            {
                if (!tc.EnableHomeSection || !IsOn(tc.HomeSectionUserIds, userKey)) continue;
                keys.Add(SourceTopLists.IsOn(tc) ? TopListKey(tc.Tag) : SourceKey(tc.Name, tc.Tag));
            }
            foreach (var tl in config.TopLists ?? new List<TopListHomeSection>())
                if (!tl.FromSource && IsOn(tl.HomeSectionUserIds, userKey)) keys.Add(TlPrefix + (tl.TagName ?? "").Trim());
            return keys.Distinct(StringComparer.OrdinalIgnoreCase).ToList();
        }

        // Puts the user on the lists of a row key (UserLists); remembered when they were not on it.
        private bool AddMember(PluginConfiguration config, ManagedRows managed, string key, string list, string userId, string targetName, string sourceName)
        {
            var lists = UserLists(config, key, list);
            if (lists.Count == 0) return false;
            string userKey = NormId(userId);
            bool wasOn = IsOn(lists[0], userKey);
            bool changed = false;
            foreach (var l in lists)
                if (!IsOn(l, userKey)) { l.Add(userId); changed = true; }
            if (!wasOn)
            {
                var additions = config.HomeSyncAdditions ??= new List<HomeSyncAddition>();
                if (!HasAddition(additions, list, key, userKey))
                    additions.Add(new HomeSyncAddition { List = list, Key = key, UserId = userId });
                _log.Info($"  {targetName}: added to {ListLabel(key, list)} (gets that row from {sourceName})");
                changed = true;
            }
            if (changed && key.StartsWith(TlPrefix, StringComparison.Ordinal)) managed.SetTopListUser(key.Substring(TlPrefix.Length), userKey, true);
            return changed;
        }

        // Takes the user off the lists of a row key. rowGone: Copy & Sync removes (or replaced) the
        // row itself, so the source's / top-list's tracking of it goes too. Else the full sync
        // would look for the vanished row and, by its title, remove another source's row.
        private static bool RemoveMember(PluginConfiguration config, ManagedRows managed, string key, string list, string userKey, bool rowGone)
        {
            bool changed = false;
            foreach (var l in UserLists(config, key, list))
                changed |= l.RemoveAll(u => NormId(u) == userKey) > 0;
            if (changed && key.StartsWith(TlPrefix, StringComparison.Ordinal)) managed.SetTopListUser(key.Substring(TlPrefix.Length), userKey, false);
            if (rowGone && list == HomeSyncAddition.Home)
            {
                if (key.StartsWith(SrcPrefix, StringComparison.Ordinal))
                {
                    foreach (var tc in (config.Tags ?? new List<TagConfig>()).Where(t => string.Equals(SourceKey(t.Name, t.Tag), key, StringComparison.OrdinalIgnoreCase)))
                        tc.HomeSectionTracked?.RemoveAll(t => NormId(t.UserId) == userKey);
                }
                else if (key.StartsWith(TlPrefix, StringComparison.Ordinal))
                {
                    var tag = key.Substring(TlPrefix.Length);
                    foreach (var tl in (config.TopLists ?? new List<TopListHomeSection>()).Where(t => string.Equals((t.TagName ?? "").Trim(), tag, StringComparison.OrdinalIgnoreCase)))
                        tl.HomeSectionTracked?.RemoveAll(t => NormId(t.UserId) == userKey);
                }
            }
            return changed;
        }

        // The user lists a row key's users are on: "src:<group>" — the group's entries (home:
        // HomeSectionUserIds, playlist: PlaylistUserIds of those making playlists); "tl:<tag>" —
        // the top-list, and for a source's own top-list also that source (the list follows it on
        // every settings save). The first list says whether the user was already on it.
        private static List<List<string>> UserLists(PluginConfiguration config, string key, string list)
        {
            var lists = new List<List<string>>();
            if (key.StartsWith(SrcPrefix, StringComparison.Ordinal))
            {
                foreach (var tc in config.Tags ?? new List<TagConfig>())
                {
                    if (!string.Equals(SourceKey(tc.Name, tc.Tag), key, StringComparison.OrdinalIgnoreCase)) continue;
                    if (list == HomeSyncAddition.Home) lists.Add(tc.HomeSectionUserIds ??= new List<string>());
                    else if (list == HomeSyncAddition.Playlist && tc.EnablePlaylist) lists.Add(tc.PlaylistUserIds ??= new List<string>());
                }
            }
            else if (key.StartsWith(TlPrefix, StringComparison.Ordinal) && list == HomeSyncAddition.Home)
            {
                var tag = key.Substring(TlPrefix.Length);
                var tl = (config.TopLists ?? new List<TopListHomeSection>()).FirstOrDefault(t => string.Equals((t.TagName ?? "").Trim(), tag, StringComparison.OrdinalIgnoreCase));
                if (tl != null)
                {
                    if (tl.FromSource)
                    {
                        var owner = (config.Tags ?? new List<TagConfig>()).FirstOrDefault(tc => SourceTopLists.IsOn(tc) && string.Equals((tc.Tag ?? "").Trim(), tag, StringComparison.OrdinalIgnoreCase));
                        if (owner != null) lists.Add(owner.HomeSectionUserIds ??= new List<string>());
                    }
                    lists.Add(tl.HomeSectionUserIds ??= new List<string>());
                }
            }
            return lists;
        }

        private static bool IsOn(List<string>? users, string userKey) => (users ?? new List<string>()).Any(u => NormId(u) == userKey);

        private static bool HasAddition(List<HomeSyncAddition> additions, string list, string key, string userKey)
            => additions.Any(a => a.List == list && NormId(a.UserId) == userKey && string.Equals(a.Key, key, StringComparison.OrdinalIgnoreCase));

        // Source rows of this user tracked by the row found on their home screen (by its tracked
        // id, else its title), so the full sync updates that row and never adds a second one.
        private bool TrackSourceRows(PluginConfiguration config, ManagedRows managed, string targetIdStr, long targetInternalId, CancellationToken cancellationToken)
        {
            string targetKey = NormId(targetIdStr);
            bool changed = false;
            var seen = new HashSet<TagConfig>();
            foreach (var s in _userManager.GetHomeSections(targetInternalId, cancellationToken)?.Sections ?? Array.Empty<ContentSection>())
            {
                if (managed.TopListRow(s) != null) continue;
                var tc = managed.SourceRow(config, s, targetKey);
                if (tc == null || !seen.Add(tc)) continue;
                var tracked = (tc.HomeSectionTracked ??= new List<HomeSectionTracking>()).FirstOrDefault(t => NormId(t.UserId) == targetKey);
                if (tracked != null && tracked.SectionId == s.Id) continue;
                if (tracked != null) tracked.SectionId = s.Id;
                else tc.HomeSectionTracked.Add(new HomeSectionTracking { UserId = managed.SourceUserId(tc, targetKey) ?? targetIdStr, SectionId = s.Id });
                changed = true;
            }
            return changed;
        }

        // What Copy & Sync added for users it no longer copies to (taken off the profile, or the
        // profile removed): taken away again. Not for users of a paused profile (they keep what
        // they have), nor for source users.
        private void RemoveStaleAdditions(PluginConfiguration config, ManagedRows managed, HashSet<string> handledTargets, HashSet<string> sourceKeys, SyncTotals totals)
        {
            var additions = config.HomeSyncAdditions ?? new List<HomeSyncAddition>();
            if (additions.Count == 0) return;
            var paused = new HashSet<string>(config.EffectiveHomeSyncProfiles().Where(p => !p.Active)
                .SelectMany(p => p.TargetUserIds ?? new List<string>()).Select(NormId));
            var stale = additions.Where(a =>
            {
                var u = NormId(a.UserId);
                return !handledTargets.Contains(u) && !paused.Contains(u) && !sourceKeys.Contains(u);
            }).ToList();
            if (stale.Count == 0) return;
            _log.Blank();
            _log.Info("» No longer copied to");
            foreach (var a in stale)
            {
                var u = NormId(a.UserId);
                if (a.List != HomeSyncAddition.Row && RemoveMember(config, managed, a.Key, a.List, u, rowGone: false))
                    _log.Info($"  {UserName(u)}: taken off {ListLabel(a.Key, a.List)} again (Copy & Sync added them)");
                additions.Remove(a);
            }
            totals.ConfigChanged = true;
        }

        // Row keys (RowKey): a source's row, a top-list's row, any other row by its SectionKey.
        internal const string SrcPrefix = "src:";
        internal const string TlPrefix = "tl:";
        internal const string RowPrefix = "row:";

        // These keys are saved in the settings XML (SelectedRows, HomeSyncAdditions), so they hold
        // no control characters (GroupIssues.Key's separator would make the whole file unreadable).
        internal static string SourceKey(string? name, string? tag)
            => SrcPrefix + Uri.EscapeDataString((name ?? "").Trim()) + "/" + Uri.EscapeDataString((tag ?? "").Trim());

        internal static string TopListKey(string? tag) => TlPrefix + XmlSafe((tag ?? "").Trim());

        private static string XmlSafe(string s) => new string(s.Where(c => c >= ' ' || c == '\t').ToArray());

        // A row key for the log: "source 'Trending'", "top-list 'Top 10'", "'Latest Movies'".
        private static string RowKeyLabel(string key)
        {
            if (key.StartsWith(SrcPrefix, StringComparison.Ordinal))
            {
                var parts = key.Substring(SrcPrefix.Length).Split('/');
                var name = Uri.UnescapeDataString(parts[0]);
                if (name.Length == 0 && parts.Length > 1) name = Uri.UnescapeDataString(parts[1]);
                return $"source '{name}'";
            }
            if (key.StartsWith(TlPrefix, StringComparison.Ordinal)) return $"top-list '{key.Substring(TlPrefix.Length)}'";
            var k = key.StartsWith(RowPrefix, StringComparison.Ordinal) ? key.Substring(RowPrefix.Length) : key;
            return k.StartsWith("name:", StringComparison.Ordinal) ? $"'{k.Substring(5)}'" : k.StartsWith("type:", StringComparison.Ordinal) ? k.Substring(5).Split('|')[0] + " row" : k;
        }

        private static string ListLabel(string key, string list)
            => list == HomeSyncAddition.Playlist ? "the playlist users of " + RowKeyLabel(key) : RowKeyLabel(key);

        // The rows of a user's home screen as Copy selected offers them (the page's row picker).
        internal static List<HomeSyncRowChoice> RowChoices(PluginConfiguration config, IUserManager userManager, ILibraryManager libraryManager, IJsonSerializer json, string userId)
        {
            var managed = ManagedRows.Build(config, libraryManager, json);
            string userKey = NormId(userId);
            var choices = new List<HomeSyncRowChoice>();
            foreach (var s in userManager.GetHomeSections(userManager.GetInternalId(userId), CancellationToken.None)?.Sections ?? Array.Empty<ContentSection>())
            {
                var key = managed.RowKey(config, s, userKey);
                if (choices.Any(c => c.Key == key)) continue;
                string title = !string.IsNullOrWhiteSpace(s.CustomName) ? s.CustomName : !string.IsNullOrWhiteSpace(s.Name) ? s.Name : "";
                if (string.IsNullOrEmpty(title))
                {
                    title = s.SectionType ?? "Row";
                    if (!string.IsNullOrEmpty(s.ParentId) && Guid.TryParse(s.ParentId, out var g) && libraryManager.GetItemById(g) is { } lib)
                        title += " · " + lib.Name;
                }
                choices.Add(new HomeSyncRowChoice
                {
                    Key = key,
                    Title = title,
                    Kind = key.StartsWith(TlPrefix, StringComparison.Ordinal) ? "toplist"
                        : key.StartsWith(SrcPrefix, StringComparison.Ordinal) ? (managed.PlaylistRow(s) != null ? "playlist" : "source") : "emby"
                });
            }
            return choices;
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
            public TagConfig? Source;
            public Dictionary<string, string> Playlists = new Dictionary<string, string>(); // user -> playlist InternalId
            public string? PlaylistFor(string user) => Playlists.TryGetValue(user, out var p) ? p : null;
        }

        private sealed class ManagedRows
        {
            public readonly Dictionary<string, string> PlaylistOwners = new Dictionary<string, string>(); // playlist InternalId -> user
            private readonly Dictionary<string, ManagedRow> _byPlaylist = new Dictionary<string, ManagedRow>();

            public static ManagedRows Build(PluginConfiguration config, ILibraryManager libraryManager, IJsonSerializer json)
            {
                var m = new ManagedRows();
                foreach (var tc in config.Tags ?? new List<TagConfig>())
                {
                    if (tc.PlaylistMappings == null || tc.PlaylistMappings.Count == 0) continue;
                    var row = new ManagedRow { Name = !string.IsNullOrWhiteSpace(tc.Name) ? tc.Name : (tc.Tag ?? ""), Source = tc };
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
                // (A source switched off has no rows: the full sync removes them.)
                foreach (var tc in config.Tags ?? new List<TagConfig>())
                {
                    if (!tc.Active || !SourceTopLists.PlainRow(tc) || (tc.HomeSectionUserIds?.Count ?? 0) == 0) continue;
                    var settings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                    try
                    {
                        if (!string.IsNullOrEmpty(tc.HomeSectionSettings) && tc.HomeSectionSettings != "{}")
                            settings = json.DeserializeFromString<Dictionary<string, string>>(tc.HomeSectionSettings) ?? settings;
                    }
                    catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
                    HomeScreenCompanionTask.FillDefaultCustomName(settings, tc);
                    m._sources.Add(tc);
                    m._sourceTitles[tc] = settings.TryGetValue("CustomName", out var cn) ? cn : "";
                    // What the row shows, as the full sync builds it: a tag row its tag, a collection
                    // row its collection. Rows of several sources can share a title (e.g. every
                    // user's own "Recommended for you"), so this tells them apart.
                    var sectionType = settings.TryGetValue("SectionType", out var st) && !string.IsNullOrEmpty(st) ? st : HomeScreenCompanionTask.DefaultSectionType(tc);
                    try
                    {
                        if (sectionType == "items" && !string.IsNullOrWhiteSpace(tc.Tag))
                        {
                            var tag = libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "Tag" }, Name = tc.Tag.Trim(), Recursive = true }).FirstOrDefault();
                            if (tag != null) m._sourceShows[tc] = new[] { tag.InternalId.ToString(System.Globalization.CultureInfo.InvariantCulture) };
                        }
                        else if (sectionType == "boxset" && tc.EnableCollection && !string.IsNullOrWhiteSpace(tc.CollectionName))
                        {
                            var coll = libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "BoxSet" }, Name = tc.CollectionName, Recursive = true }).FirstOrDefault();
                            if (coll != null) m._sourceShows[tc] = new[] { coll.InternalId.ToString(System.Globalization.CultureInfo.InvariantCulture), coll.Id.ToString("N") };
                        }
                    }
                    catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
                    m._sourceTypes[tc] = sectionType;
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

            // Copy & Sync put the user on (or took them off) the list of this tag.
            public void SetTopListUser(string tag, string user, bool on)
            {
                foreach (var tl in TopLists.Where(t => string.Equals((t.TagName ?? "").Trim(), tag, StringComparison.OrdinalIgnoreCase)))
                {
                    if (on) _topListUsers[tl].Add(user);
                    else _topListUsers[tl].Remove(user);
                }
            }

            // A row's identity across users: a top-list's row "tl:<tag>", a source's row
            // "src:<group>" (its per-user playlist row too), any other row "row:" + its SectionKey
            // (title, else type and library). A source's row is known as such only for its users.
            public string RowKey(PluginConfiguration config, ContentSection s, string user)
            {
                var tl = TopListRow(s);
                if (tl != null) return TopListKey(tl.TagName);
                var pr = PlaylistRow(s);
                if (pr?.Source != null) return SourceKey(pr.Source.Name, pr.Source.Tag);
                var tc = SourceRow(config, s, user);
                if (tc != null) return SourceKey(tc.Name, tc.Tag);
                return RowPrefix + XmlSafe(HomeScreenCompanionTask.SectionKey(s));
            }

            public bool MasterHasRow(TopListHomeSection tl, ContentSection[] masterSections) => masterSections.Any(s => TopListRow(s) == tl);

            // A source's row (tag / collection / playlist row) of this user, when they are one of the
            // source's users. A row that shows a tag or collection is the row of the source that
            // shows it (by the tracked id when several do), whatever its title. Any other row: by
            // the section id the full sync tracks, else by its title when only one source of the
            // user has it (never a row another source or a top-list owns).
            public TagConfig? SourceRow(PluginConfiguration config, ContentSection s, string user)
            {
                if (string.IsNullOrEmpty(s.Id)) return null;
                var users = _sources.Where(tc => SourceUserId(tc, user) != null).ToList();
                bool Tracks(TagConfig tc) => (tc.HomeSectionTracked ?? new List<HomeSectionTracking>()).Any(t => NormId(t.UserId) == user && t.SectionId == s.Id);

                var shows = RowShows(s);
                var byContent = users.Where(tc => _sourceShows.TryGetValue(tc, out var ids) && ids.Any(shows.Contains)
                    && string.Equals(_sourceTypes[tc], s.SectionType, StringComparison.OrdinalIgnoreCase)).ToList();
                if (byContent.Count > 0) return byContent.FirstOrDefault(Tracks) ?? byContent[0];

                // A tag / collection row of a source shows that tag or collection: another row is not its own.
                var rest = users.Where(tc => !_sourceShows.ContainsKey(tc)).ToList();
                var tracked = rest.FirstOrDefault(Tracks);
                if (tracked != null) return tracked;
                var byTitle = rest.Where(tc => HomeScreenCompanionTask.SameTitle(s.CustomName, _sourceTitles[tc])
                    && !HomeScreenCompanionTask.SectionOwnedElsewhere(config, tc, SourceUserId(tc, user)!, s.Id)).ToList();
                return byTitle.Count == 1 ? byTitle[0] : null;
            }

            // The tag ids (Query.TagIds) and the parent (ParentId) a row shows.
            private static HashSet<string> RowShows(ContentSection s)
            {
                var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (var t in s.Query?.TagIds ?? Array.Empty<string>()) if (!string.IsNullOrEmpty(t)) ids.Add(t.Trim());
                if (!string.IsNullOrEmpty(s.ParentId)) ids.Add(s.ParentId.Trim().Replace("-", ""));
                return ids;
            }

            // The user's id as the source's home-row users list has it (null: not one of them).
            public string? SourceUserId(TagConfig tc, string user)
                => (tc.HomeSectionUserIds ?? new List<string>()).FirstOrDefault(u => NormId(u) == user);

            public bool MasterHasSourceRow(PluginConfiguration config, TagConfig tc, string master, ContentSection[] masterSections)
                => SourceUserId(tc, master) != null && masterSections.Any(s => TopListRow(s) == null && SourceRow(config, s, master) == tc);

            // A row that may be this source's row: it shows the source's tag / collection, or (for a
            // source whose row shows neither) it has the source's title.
            public bool CouldBeSourceRow(ContentSection s, TagConfig tc)
            {
                if (_sourceShows.TryGetValue(tc, out var ids))
                    return ids.Any(RowShows(s).Contains) && string.Equals(_sourceTypes[tc], s.SectionType, StringComparison.OrdinalIgnoreCase);
                return _sourceTitles.TryGetValue(tc, out var title) && HomeScreenCompanionTask.SameTitle(s.CustomName, title);
            }

            private readonly List<TagConfig> _sources = new List<TagConfig>();
            private readonly Dictionary<TagConfig, string> _sourceTitles = new Dictionary<TagConfig, string>();
            private readonly Dictionary<TagConfig, string[]> _sourceShows = new Dictionary<TagConfig, string[]>();   // see Build
            private readonly Dictionary<TagConfig, string> _sourceTypes = new Dictionary<TagConfig, string>();

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

        // A row the target keeps through the copy, and put back at its place.
        private sealed class KeptRow
        {
            public int Index;
            public ContentSection Row = new ContentSection();
            public TopListHomeSection? TopList;
            public TagConfig? Source;
            public string PositionKey = "";
        }

        // Row-position memory key of a source's row: the full sync's own (its group).
        private static string SourceRowKey(TagConfig tc) => GroupIssues.Key(tc.Name, tc.Tag);

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
