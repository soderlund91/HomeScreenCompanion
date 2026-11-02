using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Library;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using MediaBrowser.Model.Entities;
using MediaBrowser.Model.Querying;
using MediaBrowser.Model.Users;

namespace HomeScreenCompanion
{
    /// <summary>
    /// "Rotating collection" source: one home row that shows one collection of a pool, the next one
    /// each day (or week), like a "Now Showing" row. The pick is a pure function of the date, the
    /// pool and the source's settings, so it is the same for every user and every run that day.
    /// Its home row is a "Single Collection" (boxset) row whose ParentId and title the sync updates in
    /// place each period; its titles (for Apply Tag / Create Collection / Playlist) are the members
    /// of the day's collection. A collection is skipped when it is empty or one of the row's users
    /// cannot see it, so the shared row works for everyone.
    /// </summary>
    internal static class RotatingRow
    {
        internal const string Type = "RotatingCollection";
        internal const string NamePlaceholder = "{name}";

        internal static bool Is(TagConfig? t) => string.Equals(t?.SourceType, Type, StringComparison.OrdinalIgnoreCase);

        // ---- Clock ----------------------------------------------------------------------------

        // Server-local "today".
        internal static DateTime Today() => DateTime.Now.Date;

        private static readonly DateTime Epoch = new DateTime(2001, 1, 1); // a Monday: weeks start on Monday

        internal static bool Weekly(TagConfig t) => string.Equals(t.RotationEvery, "Week", StringComparison.OrdinalIgnoreCase);

        /// <summary>The period number of <paramref name="day"/>: days (or weeks) since the epoch.</summary>
        internal static long Period(TagConfig t, DateTime day)
        {
            long days = (long)Math.Floor((day.Date - Epoch).TotalDays);
            return Weekly(t) ? FloorDiv(days, 7) : days;
        }

        private static long FloorDiv(long a, long b) => a >= 0 ? a / b : -((-a + b - 1) / b);
        private static int Mod(long a, int n) => (int)(((a % n) + n) % n);

        // ---- Pool -----------------------------------------------------------------------------

        /// <summary>The pool's collections that exist, one per name, A–Z.</summary>
        internal static List<BaseItem> Pool(TagConfig t, PluginConfiguration? cfg, ILibraryManager lm)
        {
            var all = lm.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "BoxSet" }, Recursive = true })
                .Where(b => !string.IsNullOrWhiteSpace(b.Name)).ToList();
            IEnumerable<BaseItem> chosen;
            switch (t.RotationPool ?? "")
            {
                case "All":
                    chosen = all;
                    break;
                case "Tag":
                    var tag = (t.RotationTag ?? "").Trim();
                    chosen = tag.Length == 0 ? Enumerable.Empty<BaseItem>()
                        : all.Where(b => (b.Tags ?? Array.Empty<string>()).Any(x => string.Equals(x?.Trim(), tag, StringComparison.OrdinalIgnoreCase)));
                    break;
                case "Managed":
                    var managed = new HashSet<string>((cfg?.Tags ?? new List<TagConfig>())
                        .Where(o => o.EnableCollection && !Is(o))
                        .Select(o => (string.IsNullOrWhiteSpace(o.CollectionName) ? (o.Tag ?? "") : o.CollectionName).Trim())
                        .Where(n => n.Length > 0), StringComparer.OrdinalIgnoreCase);
                    chosen = all.Where(b => managed.Contains(b.Name.Trim()));
                    break;
                default:
                    var names = new HashSet<string>((t.RotationCollections ?? new List<string>()).Select(n => (n ?? "").Trim()).Where(n => n.Length > 0),
                        StringComparer.OrdinalIgnoreCase);
                    chosen = all.Where(b => names.Contains(b.Name.Trim()));
                    break;
            }
            // The source's own collection (Create Collection on this source) is never in its pool.
            if (t.EnableCollection)
            {
                var own = (string.IsNullOrWhiteSpace(t.CollectionName) ? (t.Tag ?? "") : t.CollectionName).Trim();
                chosen = chosen.Where(b => !string.Equals(b.Name.Trim(), own, StringComparison.OrdinalIgnoreCase));
            }
            return chosen
                .GroupBy(b => b.Name.Trim(), StringComparer.OrdinalIgnoreCase).Select(g => g.First())
                .OrderBy(b => b.Name.Trim(), StringComparer.OrdinalIgnoreCase).ThenBy(b => b.InternalId)
                .ToList();
        }

        /// <summary>No pool picked yet (the sync skips the source and changes nothing).</summary>
        internal static bool NoPoolYet(TagConfig t) =>
            string.Equals(t.RotationPool, "Tag", StringComparison.OrdinalIgnoreCase) ? string.IsNullOrWhiteSpace(t.RotationTag)
            : !string.Equals(t.RotationPool, "All", StringComparison.OrdinalIgnoreCase) && !string.Equals(t.RotationPool, "Managed", StringComparison.OrdinalIgnoreCase)
              && !(t.RotationCollections ?? new List<string>()).Any(n => !string.IsNullOrWhiteSpace(n));

        // ---- Order ----------------------------------------------------------------------------

        internal static bool Shuffled(TagConfig t) => !string.Equals(t.RotationOrder, "InOrder", StringComparison.OrdinalIgnoreCase);

        /// <summary>
        /// The pool in the order it is shown from <paramref name="period"/> on: the period's own
        /// collection first, then the ones after it (used when one has to be skipped). In order:
        /// A–Z. Shuffle: each run through the pool is its own shuffle (seeded by the source and the
        /// run number, so stable all period long), and no collection comes back before the whole
        /// pool was shown; a new run never starts with the collection the last one ended with.
        /// </summary>
        internal static List<BaseItem> Sequence(TagConfig t, List<BaseItem> pool, long period)
        {
            int n = pool.Count;
            if (n == 0) return new List<BaseItem>();
            List<BaseItem> order;
            if (!Shuffled(t)) order = pool;
            else
            {
                long cycle = FloorDiv(period, n);
                order = Shuffle(t, pool, cycle);
                if (n > 2 && order[0] == Shuffle(t, pool, cycle - 1)[n - 1])
                {
                    order = new List<BaseItem>(order);
                    var first = order[0]; order[0] = order[1]; order[1] = first;
                }
            }
            int start = Mod(period, n);
            return order.Skip(start).Concat(order.Take(start)).ToList();
        }

        private static List<BaseItem> Shuffle(TagConfig t, List<BaseItem> pool, long cycle)
        {
            var seed = GroupIssues.Key(t.Name, t.Tag).ToLowerInvariant() + "\x1F" + cycle.ToString(System.Globalization.CultureInfo.InvariantCulture);
            return pool.OrderBy(b => Fnv(seed + "\x1F" + b.Name.Trim().ToLowerInvariant())).ThenBy(b => b.InternalId).ToList();
        }

        // Stable across restarts (string.GetHashCode is not).
        private static ulong Fnv(string s)
        {
            ulong h = 14695981039346656037UL;
            foreach (var b in Encoding.UTF8.GetBytes(s)) { h ^= b; h *= 1099511628211UL; }
            return h;
        }

        // ---- Titles ---------------------------------------------------------------------------

        /// <summary>
        /// The row's title for a collection: the Custom Title with {name} filled in; an empty Custom
        /// Title is just the collection's name; one without {name} is used as it is.
        /// </summary>
        internal static string Title(string? customTitle, string collectionName)
        {
            var ct = (customTitle ?? "").Trim();
            if (ct.Length == 0) return collectionName.Trim();
            int at = ct.IndexOf(NamePlaceholder, StringComparison.OrdinalIgnoreCase);
            return at < 0 ? ct : ct.Substring(0, at) + collectionName.Trim() + ct.Substring(at + NamePlaceholder.Length);
        }

        /// <summary>The saved Custom Title of the source's row ("" = none).</summary>
        internal static string CustomTitle(TagConfig t, MediaBrowser.Model.Serialization.IJsonSerializer json)
        {
            try
            {
                var s = json.DeserializeFromString<Dictionary<string, string>>(string.IsNullOrEmpty(t.HomeSectionSettings) ? "{}" : t.HomeSectionSettings);
                if (s != null && s.TryGetValue("CustomName", out var cn)) return cn ?? "";
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            return "";
        }

        /// <summary>
        /// Every title the row can have (one per pool collection), to find it by title — except the
        /// titles of other sources' rows (e.g. a pool collection's own collection row with the same
        /// name), so a row of another source is never taken for this one.
        /// </summary>
        internal static HashSet<string> PoolTitles(TagConfig t, string? customTitle, IEnumerable<BaseItem> pool,
            PluginConfiguration? cfg, MediaBrowser.Model.Serialization.IJsonSerializer json)
        {
            var titles = new HashSet<string>(pool.Select(b => Title(customTitle, b.Name)).Where(x => x.Length > 0), StringComparer.OrdinalIgnoreCase);
            var own = GroupIssues.Key(t.Name, t.Tag);
            foreach (var o in cfg?.Tags ?? new List<TagConfig>())
            {
                if (!o.EnableHomeSection || Is(o) || string.Equals(GroupIssues.Key(o.Name, o.Tag), own, StringComparison.OrdinalIgnoreCase)) continue;
                var ct = CustomTitle(o, json).Trim();
                titles.Remove(ct.Length > 0 ? ct : HomeScreenCompanionTask.DefaultRowTitle(o));
            }
            foreach (var tl in cfg?.TopLists ?? new List<TopListHomeSection>())
            {
                try
                {
                    var s = json.DeserializeFromString<Dictionary<string, string>>(string.IsNullOrEmpty(tl.HomeSectionSettings) ? "{}" : tl.HomeSectionSettings);
                    if (s != null && s.TryGetValue("CustomName", out var n) && !string.IsNullOrWhiteSpace(n)) titles.Remove(n.Trim());
                }
                catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
                titles.Remove((tl.TagName ?? "").Trim());
            }
            return titles;
        }

        /// <summary>A collection row whose title is one of <paramref name="titles"/>.</summary>
        internal static bool IsRowTitle(ContentSection s, HashSet<string> titles)
            => string.Equals(s.SectionType, "boxset", StringComparison.OrdinalIgnoreCase)
               && !string.IsNullOrWhiteSpace(s.CustomName)
               && titles.Contains(HomeScreenCompanionTask.StripInactive(s.CustomName).Trim());

        // ---- Today's pick ---------------------------------------------------------------------

        internal sealed class Result
        {
            public List<BaseItem> Pool = new List<BaseItem>();
            public BaseItem? Pick;
            /// <summary>Collections of today's turn that were skipped (empty, or not visible to a user of the row).</summary>
            public List<string> Skipped = new List<string>();
        }

        private static readonly object _cacheLock = new object();
        private static readonly Dictionary<string, (DateTime At, Result R)> _cache = new Dictionary<string, (DateTime, Result)>();

        /// <summary>
        /// Today's collection: the period's turn, else the next one in the sequence that has titles
        /// every (enabled) user of the row can see. Null when the pool has none. Cached for a while
        /// per source, period and settings, so every step of a run agrees.
        /// </summary>
        internal static Result Resolve(TagConfig t, PluginConfiguration? cfg, ILibraryManager lm, IUserManager um, DateTime? day = null)
        {
            var d = (day ?? Today()).Date;
            long period = Period(t, d);
            var users = (t.EnableHomeSection ? (t.HomeSectionUserIds ?? new List<string>()) : new List<string>())
                .Select(u => (u ?? "").Replace("-", "").ToLowerInvariant()).OrderBy(u => u).ToList();
            var key = string.Join("\x1E", GroupIssues.Key(t.Name, t.Tag), period, t.RotationPool, t.RotationTag, t.RotationEvery, t.RotationOrder,
                string.Join("\x1F", (t.RotationCollections ?? new List<string>()).OrderBy(x => x, StringComparer.OrdinalIgnoreCase)),
                t.EnableCollection ? t.CollectionName : "", string.Join(",", users));
            lock (_cacheLock)
            {
                if (_cache.TryGetValue(key, out var hit) && DateTime.UtcNow - hit.At < TimeSpan.FromMinutes(30)) return hit.R;
            }

            var r = new Result { Pool = Pool(t, cfg, lm) };
            var viewers = Viewers(users, um);
            foreach (var c in Sequence(t, r.Pool, period))
            {
                if (Usable(c, viewers, lm)) { r.Pick = c; break; }
                r.Skipped.Add(c.Name);
            }
            lock (_cacheLock)
            {
                if (_cache.Count > 200) _cache.Clear();
                _cache[key] = (DateTime.UtcNow, r);
            }
            return r;
        }

        // The row's users who can log in, one per kind of access (same libraries and parental
        // settings see the same titles).
        private static List<User> Viewers(List<string> userIds, IUserManager um)
        {
            var byPolicy = new Dictionary<string, User>();
            foreach (var id in userIds)
            {
                User? u = null;
                try { if (Guid.TryParse(id, out var g)) u = um.GetUserById(g); } catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
                if (u == null || u.Policy?.IsDisabled == true) continue;
                var p = u.Policy;
                string J(IEnumerable<string>? a) => a == null ? "" : string.Join(",", a.OrderBy(x => x, StringComparer.OrdinalIgnoreCase));
                var k = p == null ? "user:" + u.Id : string.Join("|", p.EnableAllFolders, J(p.EnabledFolders), J(p.ExcludedSubFolders), p.MaxParentalRating,
                    p.BlockUnratedItems == null ? "" : string.Join(",", p.BlockUnratedItems.Select(x => x.ToString()).OrderBy(x => x)),
                    J(p.BlockedTags), J(p.IncludeTags), p.IsTagBlockingModeInclusive, p.AllowTagOrRating);
                if (!byPolicy.ContainsKey(k)) byPolicy[k] = u;
            }
            return byPolicy.Values.ToList();
        }

        // Has titles (not only missing ones), and every viewer sees the collection and a title in it.
        private static bool Usable(BaseItem coll, List<User> viewers, ILibraryManager lm)
        {
            try
            {
                if (viewers.Count == 0)
                    return lm.GetItemList(new InternalItemsQuery { CollectionIds = new[] { coll.InternalId }, IsVirtualItem = false, Limit = 1 }).Any();
                foreach (var u in viewers)
                {
                    if (!coll.IsVisible(u)) return false;
                    if (!lm.GetItemList(new InternalItemsQuery(u) { CollectionIds = new[] { coll.InternalId }, IsVirtualItem = false, Limit = 1 }).Any()) return false;
                }
                return true;
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); return false; }
        }
    }
}
