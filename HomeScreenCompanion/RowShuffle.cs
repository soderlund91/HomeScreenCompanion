using MediaBrowser.Controller.Library;
using MediaBrowser.Model.Entities;
using MediaBrowser.Model.Users;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;

namespace HomeScreenCompanion
{
    /// <summary>
    /// Row shuffle (Home Screen → Row Shuffle): the rows of a set take turns on the places they have
    /// on each user's home screen; every other row stays where it is. Only HSC's own rows (a source's
    /// row "src:…", a top-list's row "tl:…", Copy &amp; Sync's keys), known by the section id HSC
    /// tracks for the user or that Copy &amp; Sync recorded for them, never by a title (rule 1).
    ///
    /// The order is worked out, not saved: from the set's start order (its Rows) and the number of
    /// the period since it started (StartDate, stamped by the server when the set is saved changed),
    /// so a run that makes a row again cannot change it for good. Each user's order is worked out on
    /// the rows they have (UserOrder): users with the same rows get the same order, and a user with
    /// fewer rows takes turns among their own, one step every period (owner 2026-11-22).
    /// Rolling: each period every row climbs one place and the top one goes to the bottom. Random: a
    /// new order each period, never the same row on top two periods running.
    ///
    /// Put into effect only by Schedule Switch (every 15 minutes), never by a sync, Run Group, the
    /// top-list sync or Copy &amp; Sync (owner 2026-11-19: rows change places at the set's interval
    /// only). A set switched off or deleted puts its rows back in the start order once
    /// (row_shuffle_state.txt keeps what is needed for that).
    /// </summary>
    internal static class RowShuffle
    {
        internal const string Rolling = "Rolling";
        internal const string Random = "Random";
        internal const string Hour = "Hour", Day = "Day", Week = "Week", Days = "Days";
        private const string StampFormat = "yyyy-MM-dd'T'HH";

        private static readonly object _lock = new object();
        private static string StateFile => Path.Combine(Plugin.Instance!.DataFolderPath, "row_shuffle_state.txt");

        internal static bool IsRandom(RowShuffleSet s) => string.Equals(s.Mode, Random, StringComparison.OrdinalIgnoreCase);

        internal static int EveryDays(RowShuffleSet s) => Math.Max(1, Math.Min(365, s.EveryDays));

        /// <summary>The set's rows: distinct, in start order.</summary>
        internal static List<string> Rows(RowShuffleSet s)
            => (s.Rows ?? new List<string>()).Where(k => !string.IsNullOrWhiteSpace(k)).Select(k => k.Trim())
                .Distinct(StringComparer.OrdinalIgnoreCase).ToList();

        /// <summary>The sets that take turns: switched on, two rows or more, each row in one set only
        /// (the first set that has it).</summary>
        internal static List<(RowShuffleSet set, List<string> rows)> ActiveSets(PluginConfiguration config)
        {
            var claimed = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var result = new List<(RowShuffleSet, List<string>)>();
            foreach (var s in config.RowShuffleSets ?? new List<RowShuffleSet>())
            {
                if (s == null || !s.Active || string.IsNullOrWhiteSpace(s.Id)) continue;
                var rows = Rows(s).Where(claimed.Add).ToList();
                if (rows.Count >= 2) result.Add((s, rows));
            }
            return result;
        }

        /// <summary>Something to put in effect: a set that takes turns, or one to put back.</summary>
        internal static bool HasWork(PluginConfiguration config)
        {
            if (ActiveSets(config).Count > 0) return true;
            try { lock (_lock) return File.Exists(StateFile); }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); return false; }
        }

        // ── Saving ───────────────────────────────────────────────────────────────

        /// <summary>
        /// Settings saved (Plugin.UpdateConfiguration): row keys follow renamed sources, and a set that is
        /// new or changed (rows, their order, mode, interval, switched on) starts now: its start order is
        /// what the page shows. An unchanged set keeps the server's start, whatever the page sent.
        /// </summary>
        internal static void Saved(PluginConfiguration current, PluginConfiguration incoming, Dictionary<string, string> renamedKeys)
        {
            if (incoming.RowShuffleSets == null) { incoming.RowShuffleSets = new List<RowShuffleSet>(); return; }
            string Follow(string k) => renamedKeys.TryGetValue(k ?? "", out var to) ? to : (k ?? "");
            var now = DateTime.Now;
            foreach (var s in incoming.RowShuffleSets.Where(s => s != null))
            {
                if (string.IsNullOrWhiteSpace(s.Id)) s.Id = Guid.NewGuid().ToString("N");
                s.Name = XmlSafe(s.Name ?? "");
                s.Rows = Rows(s).Select(Follow).Select(XmlSafe).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
                if (!IsRandom(s)) s.Mode = Rolling;
                if (s.Every != Hour && s.Every != Week && s.Every != Days) s.Every = Day;
                s.EveryDays = EveryDays(s);
                var old = (current.RowShuffleSets ?? new List<RowShuffleSet>()).FirstOrDefault(o => o != null && o.Id == s.Id);
                bool same = old != null && old.Active == s.Active && string.Equals(old.Mode, s.Mode, StringComparison.OrdinalIgnoreCase)
                    && old.Every == s.Every && (s.Every != Days || EveryDays(old) == s.EveryDays)
                    && Rows(old).Select(Follow).SequenceEqual(s.Rows, StringComparer.OrdinalIgnoreCase)
                    && ParseStart(old.StartDate).HasValue;
                s.StartDate = same ? old!.StartDate : now.ToString(StampFormat, CultureInfo.InvariantCulture);
            }
            incoming.RowShuffleSets.RemoveAll(s => s == null);
        }

        internal static RowShuffleSet Copy(RowShuffleSet s) => new RowShuffleSet
        {
            Id = s.Id ?? "", Name = s.Name ?? "", Active = s.Active, Mode = s.Mode ?? Rolling, Every = s.Every ?? Day,
            EveryDays = s.EveryDays, StartDate = s.StartDate ?? "", Rows = Rows(s)
        };

        /// <summary>Sets from a backup: cleaned as a Save would, each starting now from its start order.</summary>
        internal static List<RowShuffleSet> Restored(IEnumerable<RowShuffleSet> sets)
        {
            var cfg = new PluginConfiguration { RowShuffleSets = sets.Where(s => s != null).Select(Copy).ToList() };
            foreach (var s in cfg.RowShuffleSets) s.StartDate = "";
            Saved(new PluginConfiguration(), cfg, new Dictionary<string, string>());
            return cfg.RowShuffleSets;
        }

        private static string XmlSafe(string s) => new string((s ?? "").Where(c => c >= ' ' || c == '\t').ToArray()).Trim();

        private static DateTime? ParseStart(string? stamp)
            => DateTime.TryParseExact(stamp ?? "", StampFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out var d) ? d : (DateTime?)null;

        // ── The order ────────────────────────────────────────────────────────────

        /// <summary>The number of the period <paramref name="now"/> is in (0 = the start order).</summary>
        internal static long Period(RowShuffleSet s, DateTime now)
        {
            var start = ParseStart(s.StartDate) ?? now;
            if (s.Every == Hour)
            {
                var startHour = new DateTime(start.Year, start.Month, start.Day, start.Hour, 0, 0);
                return Math.Max(0, (long)Math.Floor((now - startHour).TotalHours));
            }
            long days = Math.Max(0, (long)(now.Date - start.Date).TotalDays);
            return s.Every == Week ? days / 7 : s.Every == Days ? days / EveryDays(s) : days;
        }

        /// <summary>When period <paramref name="k"/> begins.</summary>
        internal static DateTime PeriodStart(RowShuffleSet s, long k, DateTime now)
        {
            var start = ParseStart(s.StartDate) ?? now;
            if (s.Every == Hour) return new DateTime(start.Year, start.Month, start.Day, start.Hour, 0, 0).AddHours(k);
            int step = s.Every == Week ? 7 : s.Every == Days ? EveryDays(s) : 1;
            return start.Date.AddDays(k * step);
        }

        // Random orders worked out so far: set signature → (period, order). Random needs the order
        // before to keep the same row off the top, so it is worked out period by period once.
        private static readonly Dictionary<string, (long k, List<string> order)> _randomMemo = new Dictionary<string, (long, List<string>)>();

        /// <summary>The set's order in period <paramref name="k"/>.</summary>
        internal static List<string> Order(RowShuffleSet s, List<string> rows, long k)
        {
            int n = rows.Count;
            if (n < 2 || k <= 0) return rows.ToList();
            if (!IsRandom(s))
            {
                int shift = (int)(k % n);
                return rows.Skip(shift).Concat(rows.Take(shift)).ToList();
            }
            var sig = s.Id + "\u001F" + s.StartDate + "\u001F" + string.Join("\u001F", rows);
            long from = 0; var order = rows.ToList();
            lock (_lock)
            {
                if (_randomMemo.TryGetValue(sig, out var memo) && memo.k <= k) { from = memo.k; order = memo.order.ToList(); }
            }
            // A very old set (years of hours): the rule only needs the order before, so start close by.
            if (k - from > 200000) { from = k - 1; order = Shuffled(rows, Seed(s, from)); }
            for (long p = from + 1; p <= k; p++)
            {
                var next = Shuffled(rows, Seed(s, p));
                if (string.Equals(next[0], order[0], StringComparison.OrdinalIgnoreCase)) { var t = next[0]; next[0] = next[1]; next[1] = t; }
                order = next;
            }
            lock (_lock)
            {
                if (_randomMemo.Count > 200) _randomMemo.Clear();
                _randomMemo[sig] = (k, order.ToList());
            }
            return order;
        }

        private static ulong Seed(RowShuffleSet s, long period)
        {
            using var sha = SHA256.Create();
            var hash = sha.ComputeHash(Encoding.UTF8.GetBytes(s.Id + "|" + s.StartDate + "|" + period.ToString(CultureInfo.InvariantCulture)));
            ulong seed = BitConverter.ToUInt64(hash, 0);
            return seed == 0 ? 0x9E3779B97F4A7C15UL : seed;
        }

        // Fisher-Yates with xorshift64*: the same seed gives the same order on every server.
        private static List<string> Shuffled(List<string> rows, ulong seed)
        {
            var list = rows.ToList();
            ulong x = seed;
            for (int i = list.Count - 1; i > 0; i--)
            {
                x ^= x >> 12; x ^= x << 25; x ^= x >> 27;
                int j = (int)((x * 0x2545F4914F6CDD1DUL) % (ulong)(i + 1));
                var t = list[i]; list[i] = list[j]; list[j] = t;
            }
            return list;
        }

        // ── Which rows are the set's ─────────────────────────────────────────────

        private static string Norm(string? id) => HomeSectionSyncTask.NormId(id);

        /// <summary>
        /// The section ids each user has for each row key: the ids the source or top-list tracks, and
        /// the rows Copy &amp; Sync recorded for users who are not on it. Never by title. A source whose
        /// row is its top-list has the top-list's row.
        /// </summary>
        internal static Dictionary<string, Dictionary<string, List<string>>> RowsByUser(PluginConfiguration config, IEnumerable<string> keys)
        {
            var result = new Dictionary<string, Dictionary<string, List<string>>>(StringComparer.OrdinalIgnoreCase); // user → key → ids
            void Add(string user, string key, string? sectionId)
            {
                if (string.IsNullOrWhiteSpace(user) || string.IsNullOrWhiteSpace(sectionId)) return;
                if (!result.TryGetValue(user, out var byKey)) result[user] = byKey = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
                if (!byKey.TryGetValue(key, out var ids)) byKey[key] = ids = new List<string>();
                if (!ids.Contains(sectionId!)) ids.Add(sectionId!);
            }
            foreach (var key in keys)
            {
                foreach (var tracked in TrackedOf(config, key))
                    Add(Norm(tracked.UserId), key, tracked.SectionId);
                foreach (var a in config.HomeSyncAdditions ?? new List<HomeSyncAddition>())
                    if (a.List == HomeSyncAddition.Row && string.Equals(a.Key, key, StringComparison.OrdinalIgnoreCase))
                        Add(Norm(a.UserId), key, a.SectionId);
            }
            return result;
        }

        private static IEnumerable<HomeSectionTracking> TrackedOf(PluginConfiguration config, string key)
        {
            if (key.StartsWith(HomeSectionSyncTask.SrcPrefix, StringComparison.Ordinal))
            {
                var sources = (config.Tags ?? new List<TagConfig>())
                    .Where(tc => string.Equals(HomeSectionSyncTask.SourceKey(tc.Name, tc.Tag), key, StringComparison.OrdinalIgnoreCase)).ToList();
                // Its row is its top-list's.
                var withList = sources.FirstOrDefault(SourceTopLists.IsOn);
                if (withList != null)
                    return TrackedOf(config, HomeSectionSyncTask.TopListKey(withList.Tag));
                return sources.Where(tc => tc.Active && tc.EnableHomeSection)
                    .SelectMany(tc => tc.HomeSectionTracked ?? new List<HomeSectionTracking>()).ToList();
            }
            if (key.StartsWith(HomeSectionSyncTask.TlPrefix, StringComparison.Ordinal))
                return (config.TopLists ?? new List<TopListHomeSection>())
                    .Where(tl => string.Equals(HomeSectionSyncTask.TopListKey(tl.TagName), key, StringComparison.OrdinalIgnoreCase))
                    .SelectMany(tl => tl.HomeSectionTracked ?? new List<HomeSectionTracking>()).ToList();
            return Enumerable.Empty<HomeSectionTracking>();
        }

        /// <summary>A row key for the log and the page: the source's or top-list's name.</summary>
        internal static string Label(PluginConfiguration config, string key)
        {
            if (key.StartsWith(HomeSectionSyncTask.SrcPrefix, StringComparison.Ordinal))
            {
                var tc = (config.Tags ?? new List<TagConfig>())
                    .FirstOrDefault(t => string.Equals(HomeSectionSyncTask.SourceKey(t.Name, t.Tag), key, StringComparison.OrdinalIgnoreCase));
                if (tc != null) return !string.IsNullOrWhiteSpace(tc.Name) ? tc.Name.Trim() : (tc.Tag ?? "").Trim();
                var parts = key.Substring(HomeSectionSyncTask.SrcPrefix.Length).Split('/');
                var name = Uri.UnescapeDataString(parts[0]);
                return name.Length == 0 && parts.Length > 1 ? Uri.UnescapeDataString(parts[1]) : name;
            }
            if (key.StartsWith(HomeSectionSyncTask.TlPrefix, StringComparison.Ordinal))
            {
                var tag = key.Substring(HomeSectionSyncTask.TlPrefix.Length);
                var owner = (config.Tags ?? new List<TagConfig>())
                    .FirstOrDefault(t => SourceTopLists.IsOn(t) && string.Equals((t.Tag ?? "").Trim(), tag, StringComparison.OrdinalIgnoreCase));
                return owner != null && !string.IsNullOrWhiteSpace(owner.Name) ? owner.Name.Trim() : tag;
            }
            return key;
        }

        // ── Putting it into effect ───────────────────────────────────────────────

        /// <summary>
        /// The set's rows the user has on the home screen now, in start order (one row per key, as
        /// Arrange). A user takes turns among these (owner 2026-11-22): the order is worked out on them,
        /// so a user with 2 of 14 rows gets a change every period, not now and then. A user with all the
        /// rows gets the set's order. Worked out every time, never saved: a row the user gets or loses
        /// counts from the next check on (the one change then may be uneven, an accepted limit).
        /// </summary>
        internal static List<string> PresentRows(List<string> current, Dictionary<string, List<string>> byKey, List<string> rows)
        {
            var taken = new HashSet<string>();
            var result = new List<string>();
            foreach (var key in rows)
            {
                if (!byKey.TryGetValue(key, out var ids)) continue;
                var id = current.FirstOrDefault(c => ids.Contains(c) && !taken.Contains(c));
                if (id == null) continue;
                taken.Add(id);
                result.Add(key);
            }
            return result;
        }

        /// <summary>The order the user's rows of the set get in period <paramref name="k"/>: their own turns.</summary>
        internal static List<string> UserOrder(RowShuffleSet s, List<string> rows, long k, List<string> current, Dictionary<string, List<string>> byKey)
        {
            var own = PresentRows(current, byKey, rows);
            return own.Count >= 2 ? Order(s, own, k) : own;
        }

        /// <summary>The rows of one user's home screen in the order they get (ids), or null when nothing moves.</summary>
        internal static List<string>? Arrange(List<string> current, Dictionary<string, List<string>> byKey, List<string> order)
        {
            // One row per key: the first one on the screen (a second row of the same source stays put).
            var rowOf = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            var taken = new HashSet<string>();
            foreach (var key in order)
            {
                if (!byKey.TryGetValue(key, out var ids)) continue;
                var id = current.FirstOrDefault(c => ids.Contains(c) && !taken.Contains(c));
                if (id == null) continue;
                rowOf[key] = id; taken.Add(id);
            }
            if (rowOf.Count < 2) return null;
            var slots = current.Select((id, i) => (id, i)).Where(x => taken.Contains(x.id)).Select(x => x.i).ToList();
            var wanted = order.Where(rowOf.ContainsKey).Select(k => rowOf[k]).ToList();
            var result = current.ToList();
            for (int i = 0; i < slots.Count; i++) result[slots[i]] = wanted[i];
            return result.SequenceEqual(current) ? null : result;
        }

        /// <summary>
        /// Puts every set's order into effect for every user who has two of its rows or more, and puts the
        /// rows of sets switched off or deleted back in their start order once. Only rows that are in the
        /// wrong place move (MoveHomeSections; the rows are never written again, so their settings and
        /// exclusions stay). Called by Schedule Switch while it holds RunGate (rule 8).
        /// </summary>
        /// <returns>The number of users whose rows moved (or would move, in a dry run).</returns>
        internal static int Apply(PluginConfiguration config, IUserManager userManager, bool dryRun, RunLog? log, CancellationToken cancellationToken)
        {
            var active = ActiveSets(config);
            var state = LoadState();
            // As the run's schedules: the moment it started (RunGate.ScheduleNow).
            var now = RunGate.ScheduleNow;
            var activeRows = new HashSet<string>(active.SelectMany(a => a.rows), StringComparer.OrdinalIgnoreCase);

            // What each set puts in effect: its order now, each user's worked out on the rows they have
            // (UserOrder; `order` is the set's order, for the log); a set that is gone or off: its start
            // order, only for rows no set that takes turns now has.
            var jobs = new List<(string id, string name, RowShuffleSet? set, List<string> rows, long k, List<string> order, bool restore)>();
            foreach (var (set, rows) in active)
            {
                long k = Period(set, now);
                jobs.Add((set.Id, string.IsNullOrWhiteSpace(set.Name) ? "Row shuffle" : set.Name.Trim(), set, rows, k, Order(set, rows, k), false));
            }
            foreach (var kv in state.Where(kv => !active.Any(a => a.set.Id == kv.Key)))
            {
                var rows = kv.Value.rows.Where(r => !activeRows.Contains(r)).ToList();
                jobs.Add((kv.Key, kv.Value.name, null, rows, 0, rows, true));
            }
            if (jobs.Count == 0) return 0;

            var byUser = RowsByUser(config, jobs.SelectMany(j => j.rows).Distinct(StringComparer.OrdinalIgnoreCase));
            var userStatus = UserStatus.Read(userManager);
            int usersMoved = 0;
            var movedFor = new int[jobs.Count];     // users whose rows of the job moved
            var disabledOf = new int[jobs.Count];   // users left alone: disabled in Emby (rule 10)

            foreach (var entry in byUser)
            {
                var user = entry.Key; var byKey = entry.Value;
                cancellationToken.ThrowIfCancellationRequested();
                long uid;
                try { uid = userManager.GetInternalId(user); } catch (Exception swallowed) { PluginLog.Swallowed(swallowed); continue; }
                ContentSection[] sections;
                try { sections = userManager.GetHomeSections(uid, cancellationToken)?.Sections ?? Array.Empty<ContentSection>(); }
                catch (Exception swallowed) { PluginLog.Swallowed(swallowed); continue; }
                var current = sections.Select(s => s.Id ?? "").ToList();
                bool disabled = userStatus.IsDisabled(user);
                bool moved = false;
                for (int j = 0; j < jobs.Count; j++)
                {
                    var job = jobs[j];
                    var order = job.restore ? job.rows : UserOrder(job.set!, job.rows, job.k, current, byKey);
                    var target = Arrange(current, byKey, order);
                    if (target == null) continue;
                    if (disabled) { disabledOf[j]++; continue; }   // rule 10: left as it is
                    var name = UserName(userManager, user);
                    if (dryRun)
                    {
                        log?.Debug($"[Dry run] {name}: the rows of '{job.name}' would move");
                        current = target;
                        movedFor[j]++;
                        moved = true;
                        continue;
                    }
                    try
                    {
                        // Selection order: index by index, the row that belongs there moves up (as the
                        // Manage tab's reorder). MoveHomeSections takes the row's final index.
                        for (int i = 0; i < target.Count; i++)
                        {
                            if (current[i] == target[i]) continue;
                            userManager.MoveHomeSections(uid, new[] { target[i] }, i, cancellationToken);
                            GentleMode.Unpaced();
                            current.Remove(target[i]);
                            current.Insert(i, target[i]);
                        }
                        log?.Debug($"{name}: the rows of '{job.name}' moved");
                        movedFor[j]++;
                        moved = true;
                    }
                    catch (OperationCanceledException) { throw; }
                    catch (Exception ex)
                    {
                        if (log != null) { Head(log); log.Warn($"{name}: the rows of '{job.name}' could not be moved — {ex.Message}"); }
                        // Read again: some of the moves may have been made.
                        try { current = (userManager.GetHomeSections(uid, cancellationToken)?.Sections ?? Array.Empty<ContentSection>()).Select(s => s.Id ?? "").ToList(); }
                        catch (Exception swallowed) { PluginLog.Swallowed(swallowed); break; }
                    }
                }
                if (moved) usersMoved++;
                // Gentle mode: one user's rows are moved at once, the pause comes after them.
                if (moved && !dryRun) GentleMode.UnitDone(cancellationToken);
            }

            // One line per set that moved something.
            if (log != null)
                for (int j = 0; j < jobs.Count; j++)
                {
                    if (movedFor[j] == 0) continue;
                    var job = jobs[j];
                    var first = job.restore || job.order.Count == 0 ? "" : $", '{Label(config, job.order[0])}' first";
                    var users = RunLog.Plural(movedFor[j], "user");
                    Head(log);
                    log.Ok(dryRun
                        ? $"[Dry run] '{job.name}' would {(job.restore ? "go back to its start order" : "get its new order")} for {users}{first}"
                        : $"'{job.name}' {(job.restore ? "back in its start order" : "in its new order")} for {users}{first}");
                    if (disabledOf[j] > 0) log.Skip($"'{job.name}': {RunLog.Plural(disabledOf[j], "user")} disabled in Emby, left as they are");
                }

            if (!dryRun) SaveState(active);
            return usersMoved;
        }

        // The log's heading, once per log.
        private static readonly System.Runtime.CompilerServices.ConditionalWeakTable<RunLog, object> _headed = new System.Runtime.CompilerServices.ConditionalWeakTable<RunLog, object>();
        private static void Head(RunLog log)
        {
            if (_headed.TryGetValue(log, out _)) return;
            _headed.Add(log, new object());
            log.Blank();
            log.Info("» Row shuffle");
        }

        private static string Title(ContentSection s)
        {
            var t = HomeScreenCompanionTask.StripInactive(s.CustomName ?? "").Trim();
            return t.Length > 0 ? t : (s.Name ?? "").Trim();
        }

        private static string UserName(IUserManager userManager, string user)
        {
            try { return Guid.TryParse(user, out var g) ? (userManager.GetUserById(g)?.Name ?? user) : user; }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); return user; }
        }

        // ── State: the sets in effect, so one switched off or deleted can be put back ──

        // One line per set: id, name, then its rows in start order, separated by \x1F (a data file,
        // never the config: rule 4).
        private static Dictionary<string, (string name, List<string> rows)> LoadState()
        {
            var result = new Dictionary<string, (string, List<string>)>(StringComparer.Ordinal);
            try
            {
                lock (_lock)
                {
                    if (!File.Exists(StateFile)) return result;
                    foreach (var line in File.ReadAllLines(StateFile, Encoding.UTF8))
                    {
                        var parts = line.Split('\u001F');
                        if (parts.Length < 2 || parts[0].Length == 0) continue;
                        result[parts[0]] = (parts[1], parts.Skip(2).Where(p => p.Length > 0).ToList());
                    }
                }
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            return result;
        }

        // After a run: the sets that take turns now; the ones put back are forgotten.
        private static void SaveState(List<(RowShuffleSet set, List<string> rows)> active)
        {
            var lines = active.Select(a => string.Join("\u001F", new[] { a.set.Id, XmlSafe(a.set.Name ?? "") }.Concat(a.rows))).ToList();
            try
            {
                lock (_lock)
                {
                    if (lines.Count == 0) { if (File.Exists(StateFile)) File.Delete(StateFile); return; }
                    var text = string.Join("\n", lines) + "\n";
                    if (File.Exists(StateFile) && File.ReadAllText(StateFile, Encoding.UTF8).Replace("\r\n", "\n") == text) return;
                    Directory.CreateDirectory(Path.GetDirectoryName(StateFile)!);
                    File.WriteAllText(StateFile, text, new UTF8Encoding(false));
                }
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
        }

        // ── The page's preview ──────────────────────────────────────────────────

        /// <summary>
        /// What a set does, for the page: its order now and in the next periods, the users who have two
        /// of its rows or more, and one user's home screen as it is in the current period. A set that is
        /// not saved as it is starts on Save, as the server will stamp it.
        /// </summary>
        internal static RowShufflePreviewResponse Preview(PluginConfiguration config, IUserManager userManager, RowShuffleSet set, string? userId)
        {
            var now = DateTime.Now;
            var saved = (config.RowShuffleSets ?? new List<RowShuffleSet>()).FirstOrDefault(s => s != null && s.Id == set.Id);
            var probe = new RowShuffleSet
            {
                Id = string.IsNullOrWhiteSpace(set.Id) ? "preview" : set.Id, Name = set.Name ?? "", Active = true,
                Mode = IsRandom(set) ? Random : Rolling,
                Every = set.Every == Hour || set.Every == Week || set.Every == Days ? set.Every : Day,
                EveryDays = EveryDays(set), Rows = Rows(set)
            };
            bool same = saved != null && saved.Active && string.Equals(saved.Mode, probe.Mode, StringComparison.OrdinalIgnoreCase)
                && saved.Every == probe.Every && (probe.Every != Days || EveryDays(saved) == probe.EveryDays)
                && Rows(saved).SequenceEqual(probe.Rows, StringComparer.OrdinalIgnoreCase) && ParseStart(saved.StartDate).HasValue;
            probe.StartDate = same ? saved!.StartDate : now.ToString(StampFormat, CultureInfo.InvariantCulture);

            var resp = new RowShufflePreviewResponse { StartsOnSave = !same };
            var rows = probe.Rows;
            long k = Period(probe, now);
            for (long p = k; p < k + 5 && rows.Count >= 2; p++)
                resp.Periods.Add(new RowShufflePeriod
                {
                    Starts = p == k ? "" : PeriodStart(probe, p, now).ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture),
                    Order = Order(probe, rows, p).Select(r => Label(config, r)).ToList()
                });

            var byUser = RowsByUser(config, rows);
            var userStatus = UserStatus.Read(userManager);
            foreach (var kv in byUser)
            {
                var has = rows.Count(r => kv.Value.ContainsKey(r));
                resp.Users.Add(new RowShuffleUser { Id = kv.Key, Name = UserName(userManager, kv.Key), Rows = has, Disabled = userStatus.IsDisabled(kv.Key) });
            }
            resp.Users = resp.Users.OrderBy(u => u.Name, StringComparer.OrdinalIgnoreCase).ToList();

            var pick = !string.IsNullOrWhiteSpace(userId) ? Norm(userId) : resp.Users.FirstOrDefault(u => u.Rows >= 2 && !u.Disabled)?.Id ?? resp.Users.FirstOrDefault()?.Id;
            if (pick != null)
            {
                resp.UserId = pick;
                try
                {
                    var sections = userManager.GetHomeSections(userManager.GetInternalId(pick), CancellationToken.None)?.Sections ?? Array.Empty<ContentSection>();
                    var current = sections.Select(s => s.Id ?? "").ToList();
                    byUser.TryGetValue(pick, out var byKey);
                    byKey ??= new Dictionary<string, List<string>>();
                    // The user's own turns (UserOrder), as Schedule Switch puts them in effect.
                    var order = rows.Count >= 2 ? UserOrder(probe, rows, k, current, byKey) : rows;
                    var own = PresentRows(current, byKey, rows);
                    resp.UserRows = own.Count;
                    if (own.Count >= 2 && own.Count < rows.Count)
                        for (long p = k; p < k + 5; p++)
                            resp.UserPeriods.Add(new RowShufflePeriod
                            {
                                Starts = p == k ? "" : PeriodStart(probe, p, now).ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture),
                                Order = Order(probe, own, p).Select(r => Label(config, r)).ToList()
                            });
                    var arranged = Arrange(current, byKey, order) ?? current;
                    var keyOf = new Dictionary<string, string>();
                    foreach (var kv in byKey) foreach (var id in kv.Value) if (!keyOf.ContainsKey(id)) keyOf[id] = kv.Key;
                    // As Arrange: only the first row of each key takes turns.
                    var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    bool shuffles = rows.Count(r => byKey.ContainsKey(r) && byKey[r].Any(current.Contains)) >= 2;
                    foreach (var id in arranged)
                    {
                        var s = sections.FirstOrDefault(x => x.Id == id);
                        if (s == null) continue;
                        string key = keyOf.TryGetValue(id, out var k2) ? k2 : "";
                        bool member = shuffles && key.Length > 0 && rows.Contains(key, StringComparer.OrdinalIgnoreCase) && seen.Add(key);
                        // A member row without a title (Hide row title): its source's or top-list's name.
                        var title = member && Title(s).Length == 0 ? Label(config, key) + " (no title)" : ScreenTitle(s);
                        resp.Screen.Add(new RowShuffleScreenRow { Title = title, Member = member });
                    }
                }
                catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            }
            return resp;
        }

        private static string ScreenTitle(ContentSection s)
        {
            var t = Title(s);
            if (t.Length > 0) return t;
            return "(" + (string.IsNullOrEmpty(s.SectionType) ? "row" : s.SectionType) + ")";
        }
    }
}
