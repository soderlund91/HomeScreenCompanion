using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Library;
using MediaBrowser.Model.Entities;
using MediaBrowser.Model.Serialization;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace HomeScreenCompanion
{
    /// <summary>
    /// "Rotating from existing" source: one home row that shows one tag, collection or playlist from
    /// the source's list (they can be mixed: the row takes the type of what it shows), and the next
    /// one every X days. It only makes that row: it tags nothing and
    /// creates nothing (TagConfig.AllTags is empty for it).
    /// The turn is shared (a state file, so a Save on the page cannot reset it and a changed list
    /// does not make it jump); each user gets the first item from the turn on that they may see, so
    /// a restricted user does not limit the others. A user with nothing to see has no row.
    /// A row is known by the section id tracked for the user, never by a title: the title changes
    /// with every turn and may be the same as a row of the user's own.
    /// </summary>
    internal static class RotatingRow
    {
        internal const string Type = "RotatingExisting";
        internal const string NamePlaceholder = "{name}";

        internal static bool Is(TagConfig? t) => string.Equals(t?.SourceType, Type, StringComparison.OrdinalIgnoreCase);

        private static bool IsTag(RotationItem i) => string.Equals(i.Kind, "Tag", StringComparison.OrdinalIgnoreCase);
        private static bool IsPlaylist(RotationItem i) => string.Equals(i.Kind, "Playlist", StringComparison.OrdinalIgnoreCase);
        internal static bool Shuffled(TagConfig t) => string.Equals(t.RotationOrder, "Shuffle", StringComparison.OrdinalIgnoreCase);

        // An item's identity in the turn: a tag and a collection can have the same name.
        private static string KeyOf(RotationItem i) => (IsTag(i) ? "Tag" : IsPlaylist(i) ? "Playlist" : "Collection") + ":" + i.Value.Trim();

        internal static bool HasTagItem(TagConfig t) => Items(t).Any(IsTag);

        /// <summary>The source's items, one per kind and value, in list order.</summary>
        internal static List<RotationItem> Items(TagConfig t)
        {
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            return (t.RotationItems ?? new List<RotationItem>())
                .Where(i => i != null && !string.IsNullOrWhiteSpace(i.Value) && seen.Add(KeyOf(i))).ToList();
        }

        // ---- The clock ------------------------------------------------------------------------

        // Server-local "today": the row moves on at midnight, server time.
        private static DateTime Today() => DateTime.Now.Date;

        private static int EveryDays(TagConfig t) => Math.Max(1, t.RotationEveryDays);

        private static string Signature(TagConfig t) => EveryDays(t) + "|" + (t.RotationStartDay ?? "").Trim();

        // The day the count starts: with a weekday its next occurrence (today when it is that day),
        // else today.
        private static DateTime AnchorFor(TagConfig t, DateTime today)
        {
            if (!Enum.TryParse<DayOfWeek>((t.RotationStartDay ?? "").Trim(), true, out var day)) return today;
            return today.AddDays(((int)day - (int)today.DayOfWeek + 7) % 7);
        }

        // Which period the day is in. With a weekday the first turn lasts until that day, and the
        // periods run every X days from then on. Without one they run every X days from the anchor.
        private static long PeriodOf(TagConfig t, DateTime anchor, DateTime today)
        {
            int days = (today - anchor).Days;
            bool weekday = (t.RotationStartDay ?? "").Trim().Length > 0;
            if (days < 0) return 0;
            return days / EveryDays(t) + (weekday ? 1 : 0);
        }

        private static DateTime ParseDate(string? s, DateTime fallback)
            => DateTime.TryParseExact(s, "yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out var d) ? d : fallback;

        private static string DateText(DateTime d) => d.ToString("yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture);

        // ---- The turn -------------------------------------------------------------------------

        public sealed class State
        {
            public string Signature { get; set; } = "";
            public string Anchor { get; set; } = "";
            public long Period { get; set; }
            public bool ShuffleOrder { get; set; }
            public List<string> Order { get; set; } = new List<string>();
            public string Current { get; set; } = "";
            public int Index { get; set; }
            // The settings the rows were last written from (see Applied).
            public string Applied { get; set; } = "";
        }

        // What the rows are written from: when it differs from the last run, they need a new one.
        private static string Applied(TagConfig t)
            => string.Join("\n", new[] { t.HomeSectionSettings ?? "" }
                .Concat(Items(t).Select(i => KeyOf(i) + "\t" + (i.Name ?? "").Trim()))
                .Concat((t.HomeSectionUserIds ?? new List<string>()).Select(HomeSectionSyncTask.NormId).OrderBy(u => u, StringComparer.Ordinal)));

        private static readonly object _stateLock = new object();
        private static string StateFile => Path.Combine(Plugin.Instance!.DataFolderPath, "rotation_state.json");

        private static Dictionary<string, State> LoadStates(IJsonSerializer json)
        {
            try
            {
                if (File.Exists(StateFile))
                    return new Dictionary<string, State>(json.DeserializeFromString<Dictionary<string, State>>(File.ReadAllText(StateFile)) ?? new Dictionary<string, State>(), StringComparer.OrdinalIgnoreCase);
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            return new Dictionary<string, State>(StringComparer.OrdinalIgnoreCase);
        }

        private static void SaveStates(Dictionary<string, State> states, IJsonSerializer json)
        {
            try
            {
                Directory.CreateDirectory(Path.GetDirectoryName(StateFile)!);
                var tmp = StateFile + ".tmp";
                File.WriteAllText(tmp, json.SerializeToString(states));
                if (File.Exists(StateFile)) File.Delete(StateFile);
                File.Move(tmp, StateFile);
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
        }

        private static string StateKey(TagConfig t) => GroupIssues.Key(t.Name, t.Tag);

        /// <summary>Deleted sources start over: one made later with the same name is a new one.</summary>
        internal static void Forget(IEnumerable<TagConfig> deleted)
        {
            var json = SourceTopLists.Json;
            if (json == null) return;
            lock (_stateLock)
            {
                var states = LoadStates(json);
                bool any = false;
                foreach (var t in deleted.Where(Is)) any |= states.Remove(StateKey(t));
                if (any) SaveStates(states, json);
            }
        }

        /// <summary>The source has a turn that is not up to date: new, changed, or a new period has come.</summary>
        internal static bool Due(TagConfig t, IJsonSerializer json)
        {
            lock (_stateLock)
            {
                if (!LoadStates(json).TryGetValue(StateKey(t), out var s) || s.Signature != Signature(t) || s.Applied != Applied(t)) return true;
                var today = Today();
                return PeriodOf(t, ParseDate(s.Anchor, today), today) != s.Period;
            }
        }

        /// <summary>
        /// The items from the current turn on, the current one first. Moves the turn on when a new
        /// period has come, to the next item at least one user can see (usable).
        /// </summary>
        // save false (a dry run): the turn is worked out the same way but not saved, so it is not
        // marked as applied either (see Due).
        internal static List<string> Sequence(TagConfig t, IJsonSerializer json, Func<string, bool> usable, bool save = true)
        {
            var items = Items(t).Select(KeyOf).ToList();
            if (items.Count == 0) return items;
            bool shuffle = Shuffled(t);
            var today = Today();
            lock (_stateLock)
            {
                var states = LoadStates(json);
                var key = StateKey(t);
                if (!states.TryGetValue(key, out var s) || s == null) { s = new State { Period = long.MinValue }; states[key] = s; }
                string before = json.SerializeToString(s);

                var sig = Signature(t);
                if (s.Signature != sig)
                {
                    // A new source, or another X or weekday: the count starts again, the turn stays.
                    s.Signature = sig;
                    s.Anchor = DateText(AnchorFor(t, today));
                    s.Period = PeriodOf(t, ParseDate(s.Anchor, today), today);
                }
                var order = OrderOf(s, items, shuffle);
                int pos = order.FindIndex(v => string.Equals(v, s.Current, StringComparison.OrdinalIgnoreCase));
                bool hasCurrent = pos >= 0;
                if (!hasCurrent) pos = Math.Min(Math.Max(0, s.Index), order.Count - 1);

                long period = PeriodOf(t, ParseDate(s.Anchor, today), today);
                if (!hasCurrent)
                {
                    // The first turn, or the current item left the list: from where it was.
                    s.Current = order.Skip(pos).Concat(order.Take(pos)).FirstOrDefault(usable) ?? order[pos];
                }
                else if (period != s.Period)
                {
                    var rest = order.Skip(pos + 1).ToList();
                    var next = shuffle ? NextShuffle(order) : order.Take(pos + 1).ToList();
                    var pick = rest.FirstOrDefault(usable);
                    if (pick == null)
                    {
                        pick = next.FirstOrDefault(usable);
                        // Every item was shown: a new shuffle starts here.
                        if (pick != null && shuffle) order = next;
                    }
                    if (pick != null) s.Current = pick;
                }
                s.Period = period;
                s.Applied = Applied(t);
                s.ShuffleOrder = shuffle;
                s.Order = order;
                s.Index = order.FindIndex(v => string.Equals(v, s.Current, StringComparison.OrdinalIgnoreCase));
                if (s.Index < 0) s.Index = 0;
                if (save && json.SerializeToString(s) != before) SaveStates(states, json);
                return order.Skip(s.Index).Concat(order.Take(s.Index)).ToList();
            }
        }

        // List order: as the list is now. Shuffle: the saved order, without what left the list, and
        // what is new put somewhere in the part not yet shown.
        private static List<string> OrderOf(State s, List<string> items, bool shuffle)
        {
            if (!shuffle) return items.ToList();
            var random = new Random();
            if (!s.ShuffleOrder || s.Order == null || s.Order.Count == 0) return Shuffle(items, s.Current);
            var order = s.Order.Where(v => items.Contains(v, StringComparer.OrdinalIgnoreCase)).ToList();
            int at = order.FindIndex(v => string.Equals(v, s.Current, StringComparison.OrdinalIgnoreCase));
            foreach (var v in items.Where(v => !order.Contains(v, StringComparer.OrdinalIgnoreCase)))
                order.Insert(random.Next(at + 1, order.Count + 1), v);
            return order;
        }

        // A random order; first: the item that goes first (the one showing now).
        private static List<string> Shuffle(List<string> items, string? first)
        {
            var random = new Random();
            var order = items.OrderBy(_ => random.Next()).ToList();
            int at = order.FindIndex(v => string.Equals(v, first, StringComparison.OrdinalIgnoreCase));
            if (at > 0) { var v = order[at]; order.RemoveAt(at); order.Insert(0, v); }
            return order;
        }

        // The next run through the shuffled list; it does not start with the item the last one ended with.
        private static List<string> NextShuffle(List<string> order)
        {
            var next = Shuffle(order, null);
            if (next.Count > 1 && string.Equals(next[0], order[order.Count - 1], StringComparison.OrdinalIgnoreCase))
            {
                var v = next[0]; next[0] = next[1]; next[1] = v;
            }
            return next;
        }

        // ---- What a user sees -----------------------------------------------------------------

        internal sealed class Pick
        {
            public string Name = "";
            public string SectionType = "boxset";   // a Single Collection, playlist or tag ("items") row
            public string? LibraryId;   // a collection or playlist row's ParentId
            public string? TagId;       // a tag row's tag
        }

        private static User? UserOf(string userId, IUserManager um)
        {
            try { return Guid.TryParse(userId, out var g) ? um.GetUserById(g) : null; }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); return null; }
        }

        private static string[] RowItemTypes(TagConfig t, IJsonSerializer json)
        {
            try
            {
                var s = Settings(t, json);
                if (s.TryGetValue("ItemTypes", out var raw) && !string.IsNullOrWhiteSpace(raw))
                {
                    var types = json.DeserializeFromString<string[]>(raw);
                    if (types != null && types.Length > 0) return types;
                }
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            return new[] { "Movie", "Series" };
        }

        // The item for this user: it exists, they may see it and it has a title they may see.
        // Several collections or playlists can have the same name (e.g. each user's own): the first
        // that works for them.
        private static Pick? ItemFor(TagConfig t, string key, User user, ILibraryManager lm, IJsonSerializer json)
        {
            var entry = Items(t).FirstOrDefault(i => string.Equals(KeyOf(i), key, StringComparison.OrdinalIgnoreCase));
            if (entry == null) return null;
            var value = entry.Value.Trim();
            var label = entry.Name?.Trim();
            try
            {
                if (IsTag(entry))
                {
                    var tag = lm.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "Tag" }, Name = value, Recursive = true }).FirstOrDefault();
                    if (tag == null) return null;
                    var titles = lm.GetItemList(new InternalItemsQuery(user)
                    {
                        Tags = new[] { value }, IncludeItemTypes = RowItemTypes(t, json), Recursive = true, IsVirtualItem = false, Limit = 1
                    });
                    if (!titles.Any()) return null;
                    return new Pick { Name = string.IsNullOrEmpty(label) ? tag.Name : label!, SectionType = "items", TagId = tag.InternalId.ToString(System.Globalization.CultureInfo.InvariantCulture) };
                }
                bool playlist = IsPlaylist(entry);
                var found = lm.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { playlist ? "Playlist" : "BoxSet" }, Name = value, Recursive = true });
                foreach (var item in found)
                {
                    if (!item.IsVisible(user)) continue;
                    var titles = playlist
                        ? lm.GetItemList(new InternalItemsQuery(user) { ListIds = new[] { item.InternalId }, Limit = 1 })
                        : lm.GetItemList(new InternalItemsQuery(user) { CollectionIds = new[] { item.InternalId }, IsVirtualItem = false, Limit = 1 });
                    if (!titles.Any()) continue;
                    return new Pick { Name = string.IsNullOrEmpty(label) ? item.Name : label!, SectionType = playlist ? "playlist" : "boxset", LibraryId = item.InternalId.ToString(System.Globalization.CultureInfo.InvariantCulture) };
                }
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            return null;
        }

        /// <summary>The items from the current turn on (see Sequence); the turn moves to one a user of the row can see.</summary>
        internal static List<string> Turn(TagConfig t, IUserManager um, ILibraryManager lm, IJsonSerializer json, bool save = true)
        {
            var viewers = (t.HomeSectionUserIds ?? new List<string>()).Select(u => UserOf(u, um)).Where(u => u != null && u.Policy?.IsDisabled != true).Select(u => u!).ToList();
            return Sequence(t, json, value => viewers.Count == 0 || viewers.Any(u => ItemFor(t, value, u, lm, json) != null), save);
        }

        /// <summary>What this user sees: the first item from the turn on that works for them; null: nothing.</summary>
        internal static Pick? PickFor(TagConfig t, string userId, List<string> sequence, IUserManager um, ILibraryManager lm, IJsonSerializer json)
        {
            var user = UserOf(userId, um);
            if (user == null) return null;
            foreach (var value in sequence)
            {
                var pick = ItemFor(t, value, user, lm, json);
                if (pick != null) return pick;
            }
            return null;
        }

        // ---- The row --------------------------------------------------------------------------

        /// <summary>The row's title for an item: the Custom Title with {name} filled in, else just the name.</summary>
        internal static string Title(string? customTitle, string name)
        {
            var ct = (customTitle ?? "").Trim();
            if (ct.Length == 0) return name.Trim();
            int at;
            while ((at = ct.IndexOf(NamePlaceholder, StringComparison.OrdinalIgnoreCase)) >= 0)
                ct = ct.Substring(0, at) + name.Trim() + ct.Substring(at + NamePlaceholder.Length);
            return ct;
        }

        private static Dictionary<string, string> Settings(TagConfig t, IJsonSerializer json)
        {
            var settings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            try
            {
                if (!string.IsNullOrEmpty(t.HomeSectionSettings) && t.HomeSectionSettings != "{}")
                    settings = new Dictionary<string, string>(json.DeserializeFromString<Dictionary<string, string>>(t.HomeSectionSettings) ?? settings, StringComparer.OrdinalIgnoreCase);
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            return settings;
        }

        /// <summary>The source's saved row settings with this item's title and content in them.</summary>
        internal static Dictionary<string, string> RowSettings(TagConfig t, Pick pick, PluginConfiguration config, ILibraryManager lm, IJsonSerializer json)
        {
            var settings = Settings(t, json);
            settings.TryGetValue("CustomName", out var custom);
            settings["CustomName"] = Title(custom, pick.Name);
            settings["SectionType"] = pick.SectionType;
            settings.Remove("_queryTagId");
            if (pick.TagId != null)
            {
                settings["_queryTagId"] = pick.TagId;
                // (The row may have been a playlist row, which clears these.)
                if (!settings.ContainsKey("ItemTypes")) settings["ItemTypes"] = "[\"Movie\",\"Series\"]";
                // (The row may have been a playlist row, which clears these.)
                if (!settings.ContainsKey("ItemTypes")) settings["ItemTypes"] = "[\"Movie\",\"Series\"]";
                HomeScreenCompanionTask.AddTopListExclusions(settings, config);
            }
            HomeScreenCompanionTask.NormalizePlaylistSectionSettings(settings);
            return TopListLibraryVisibility.WithRankedExclusions(settings, t.HomeSectionLibraryId, lm, json);
        }

        /// <summary>The section for this item; existing: the row (or a copy of one) it is written over.</summary>
        internal static ContentSection Build(TagConfig t, Pick pick, PluginConfiguration config, ILibraryManager lm, IJsonSerializer json, ContentSection? existing)
        {
            var section = HomeScreenCompanionTask.BuildContentSection(json, RowSettings(t, pick, config, lm, json), pick.LibraryId, existing);
            if (pick.LibraryId == null) section.ParentId = null;
            return section;
        }

        // What a row shows: its tag, else its parent.
        private static string Shows(ContentSection s)
            => (s.Query?.TagIds ?? Array.Empty<string>()).FirstOrDefault(x => !string.IsNullOrEmpty(x))?.Trim()
               ?? (s.ParentId ?? "").Trim().Replace("-", "");

        /// <summary>The source's row among the user's rows: the tracked id, else (the id was lost) the row with exactly what it last wrote.</summary>
        internal static ContentSection? FindRow(IEnumerable<ContentSection> sections, HomeSectionTracking? tracked)
        {
            if (tracked == null || string.IsNullOrEmpty(tracked.SectionId)) return null;
            var all = sections.ToList();
            return all.FirstOrDefault(s => s.Id == tracked.SectionId)
                ?? (string.IsNullOrEmpty(tracked.RowTitle) ? null
                    : all.FirstOrDefault(s => !string.IsNullOrEmpty(s.Id) && s.CustomName == tracked.RowTitle && Shows(s) == tracked.RowShows));
        }

        /// <summary>
        /// The user's rotating rows among sections: section id to the key a Position points at them
        /// with (the source's, HomeSectionSyncTask.SourceKey). The title changes with the turn, so
        /// another row placed before or after one cannot point at that.
        /// </summary>
        internal static Dictionary<string, string> PositionKeys(IUserManager um, long userInternalId, IEnumerable<ContentSection> sections)
        {
            var keys = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            var all = sections.ToList();
            foreach (var t in (Plugin.Instance?.Configuration?.Tags ?? new List<TagConfig>()).Where(Is))
                foreach (var tracked in t.HomeSectionTracked ?? new List<HomeSectionTracking>())
                {
                    try
                    {
                        if (um.GetInternalId(tracked.UserId) != userInternalId) continue;
                        var row = FindRow(all, tracked);
                        if (row != null) keys[row.Id] = HomeSectionSyncTask.SourceKey(t.Name, t.Tag);
                    }
                    catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
                }
            return keys;
        }

        /// <summary>
        /// Rotating sources renamed in this save: old key to new. A source has no id, but a renamed
        /// one still tracks the same rows, whatever else was added, removed or moved in the save.
        /// </summary>
        internal static Dictionary<string, string> Renames(PluginConfiguration old, PluginConfiguration incoming)
        {
            var renames = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            string KeyOfSource(TagConfig t) => HomeSectionSyncTask.SourceKey(t.Name, t.Tag);
            bool SameRow(HomeSectionTracking a, HomeSectionTracking b)
                => !string.IsNullOrEmpty(a.SectionId) && a.SectionId == b.SectionId && HomeSectionSyncTask.NormId(a.UserId) == HomeSectionSyncTask.NormId(b.UserId);
            var was = (old.Tags ?? new List<TagConfig>()).Where(Is).ToList();
            var now = (incoming.Tags ?? new List<TagConfig>()).Where(Is).ToList();
            foreach (var o in was)
            {
                if (now.Any(n => string.Equals(KeyOfSource(n), KeyOfSource(o), StringComparison.OrdinalIgnoreCase))) continue;
                var renamed = now.FirstOrDefault(n =>
                    !was.Any(x => string.Equals(KeyOfSource(x), KeyOfSource(n), StringComparison.OrdinalIgnoreCase))
                    && (n.HomeSectionTracked ?? new List<HomeSectionTracking>()).Any(a => (o.HomeSectionTracked ?? new List<HomeSectionTracking>()).Any(b => SameRow(a, b))));
                if (renamed != null) renames[KeyOfSource(o)] = KeyOfSource(renamed);
            }
            return renames;
        }

        /// <summary>Remembers the user's row (its id, and what it shows now).</summary>
        internal static void Track(TagConfig t, string userId, string sectionId, ContentSection written)
        {
            string key = HomeSectionSyncTask.NormId(userId);
            var list = t.HomeSectionTracked ??= new List<HomeSectionTracking>();
            var tracked = list.FirstOrDefault(x => HomeSectionSyncTask.NormId(x.UserId) == key);
            if (tracked == null) list.Add(tracked = new HomeSectionTracking { UserId = userId });
            tracked.SectionId = sectionId;
            tracked.RowTitle = written.CustomName ?? "";
            tracked.RowShows = Shows(written);
        }

        /// <summary>
        /// A run works on the settings it started with. When they were saved on the page meanwhile,
        /// the rows it made or found stay known: a rotating row has no title to be found by.
        /// </summary>
        internal static void CarryTracking(PluginConfiguration config)
        {
            var live = Plugin.Instance?.Configuration;
            if (live?.Tags == null || ReferenceEquals(live, config)) return;
            foreach (var t in (config.Tags ?? new List<TagConfig>()).Where(Is))
            {
                var key = StateKey(t);
                foreach (var l in live.Tags.Where(x => Is(x) && string.Equals(StateKey(x), key, StringComparison.OrdinalIgnoreCase)))
                    l.HomeSectionTracked = (t.HomeSectionTracked ?? new List<HomeSectionTracking>()).ToList();
            }
        }
    }
}
