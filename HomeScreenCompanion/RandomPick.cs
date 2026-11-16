using MediaBrowser.Controller.Entities;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace HomeScreenCompanion
{
    /// <summary>
    /// "Pinned titles" and "Random pick" on a source.
    ///
    /// Pinned titles (PinnedItemIds) are any movies or shows in the library: they always get the
    /// source's tag, collection and playlist, whether or not the source finds them, and the
    /// source's filters and blacklist do not drop them. They come first and take places in Max.
    ///
    /// Max is the total. With Random pick on, the rest of Max is filled with titles picked at
    /// random from everything the source finds (1 pinned and Max 8 picks 7); off, with the
    /// source's first titles. A source with several rows (URLs, collections, playlists) has a Max
    /// per row: the total is their sum, and the pinned titles take their places from the first
    /// rows with a Max (a row with Max 0 takes all its titles).
    ///
    /// The pick is worked out, not stored: it comes from the tag and the period (the hour, the day
    /// or the Monday of the week), so every sync in the same period picks the same titles (no tag
    /// churn, nothing to save, Dry Run changes nothing) and the next period brings a new pick. Each
    /// title gets a rank from a hash of the seed and the title, the lowest ranks are taken: a title
    /// that joins or leaves the source changes at most one pick, and a higher Max only adds titles.
    /// Max counts titles: a film's versions are separate items, all get the tag and take one place
    /// together. Top-list and ranked copies are never picked (TopListCollectionMirror.IsOriginal):
    /// the collection mirror puts them in collections.
    ///
    /// The owner's rules (2026-11-17): the shortest period is an hour; a ranked collection (Keep
    /// the list order) gets at most one new pick a day, as each pick makes and removes its copies;
    /// a source that makes a top-list has no Random pick (its pinned titles still apply).
    /// </summary>
    public static class RandomPick
    {
        public const string EveryHour = "Hour", EveryDay = "Day", EveryWeek = "Week";

        // Pinned titles and Random pick apply to sources that tag titles from what they find: not to
        // Rotating from existing (tags nothing) nor Your Next Watch (per user).
        internal static bool Applies(TagConfig t) => !RotatingRow.Is(t) && !HomeScreenCompanionTask.IsNextWatch(t);

        internal static bool On(TagConfig t) => t.RandomPick && t.Limit > 0 && Applies(t) && !SourceTopLists.IsOn(t);

        internal static bool HasPinned(TagConfig t) => Applies(t) && (t.PinnedItemIds?.Count ?? 0) > 0;

        // The source's titles are shaped here (pinned first, then the pick or the first titles).
        internal static bool Shapes(TagConfig t) => On(t) || HasPinned(t);

        /// <summary>
        /// How often the source gets a new pick: Hour, Day or Week. Anything else is Day (a
        /// "15Minutes" from a build before the owner's rules is an hour). A ranked collection gets
        /// at most one pick a day.
        /// </summary>
        internal static string Every(TagConfig t, PluginConfiguration? config)
        {
            var e = (t.RandomPickEvery ?? "").Trim();
            string every = string.Equals(e, EveryWeek, StringComparison.OrdinalIgnoreCase) ? EveryWeek
                : string.Equals(e, EveryHour, StringComparison.OrdinalIgnoreCase) || string.Equals(e, "15Minutes", StringComparison.OrdinalIgnoreCase) ? EveryHour
                : EveryDay;
            if (every == EveryHour && config != null && RankedCollections.Wants(config, t)) every = EveryDay;
            return every;
        }

        internal static string EveryLabel(TagConfig t, PluginConfiguration? config)
        {
            switch (Every(t, config))
            {
                case EveryHour: return "hour";
                case EveryWeek: return "week";
                default: return "day";
            }
        }

        /// <summary>
        /// The period a pick is for, in server time: "H2026-11-13T10", "D2026-11-13", or
        /// "W2026-11-09" (that week's Monday).
        /// </summary>
        public static string PeriodKey(string every, DateTime now)
        {
            var c = CultureInfo.InvariantCulture;
            switch (every)
            {
                case EveryHour:
                    return "H" + now.ToString("yyyy-MM-dd'T'HH", c);
                case EveryWeek:
                    var d = now.Date.AddDays(-(((int)now.DayOfWeek + 6) % 7));
                    return "W" + d.ToString("yyyy-MM-dd", c);
                default:
                    return "D" + now.Date.ToString("yyyy-MM-dd", c);
            }
        }

        internal static string PeriodKey(TagConfig t, PluginConfiguration? config, DateTime now) => PeriodKey(Every(t, config), now);

        public static string Seed(string tag, string periodKey) => (tag ?? "").Trim().ToLowerInvariant() + "|" + periodKey;

        // Stable across restarts and machines (string.GetHashCode is not).
        public static ulong Rank(string seed, string key)
        {
            using (var sha = SHA256.Create())
            {
                var h = sha.ComputeHash(Encoding.UTF8.GetBytes(seed + "|" + key));
                return BitConverter.ToUInt64(h, 0);
            }
        }

        /// <summary>
        /// The keys picked: lowest rank first, each taking Size of the budget; one that no longer
        /// fits is passed over for a smaller one.
        /// </summary>
        public static HashSet<string> Choose(IEnumerable<KeyValuePair<string, int>> groups, int budget, string seed)
        {
            var chosen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            int left = budget;
            foreach (var g in groups.OrderBy(g => Rank(seed, g.Key)).ThenBy(g => g.Key, StringComparer.Ordinal))
            {
                if (left <= 0) break;
                if (g.Value <= 0 || g.Value > left) continue;
                chosen.Add(g.Key);
                left -= g.Value;
            }
            return chosen;
        }

        /// <summary>
        /// How many of a source's pinned titles take places in this row's Max: the pinned titles fill
        /// the first rows with a Max, in the source's order. Rows with Max 0 take none.
        /// </summary>
        internal static int PinnedShare(IList<TagConfig> rows, TagConfig row, int pinned)
        {
            int left = pinned;
            foreach (var r in rows)
            {
                if (r.Limit <= 0) { if (ReferenceEquals(r, row)) return 0; continue; }
                int take = Math.Min(r.Limit, left);
                if (ReferenceEquals(r, row)) return take;
                left -= take;
            }
            return row.Limit > 0 ? Math.Min(row.Limit, pinned) : 0;
        }

        internal sealed class Result
        {
            public List<BaseItem> Items = new List<BaseItem>();   // the pinned titles first, then the pick
            public int Picked;    // titles picked (or taken in order) from the source's own titles
            public int Pool;      // titles to pick from (pinned ones left out)
            public int Pinned;    // pinned items this row added
            public int Share;     // pinned places this row gave up in its Max
            public string Period = "";
            public bool Random;
            public int Counted => Picked + Share;   // this row's places in its Max
        }

        /// <summary>
        /// One row of a source: the pinned titles (when this row adds them) and its own titles, cut
        /// to Max less the pinned places (share). pool is in the source's order.
        /// </summary>
        internal static Result Apply(TagConfig t, PluginConfiguration? config, List<BaseItem> pool, IList<BaseItem> pinned, int share, bool addPinned, DateTime now)
        {
            pool = pool.Where(TopListCollectionMirror.IsOriginal).ToList();
            var filmOf = PopularityCounter.FilmKeys(pool.Concat(pinned).ToList());
            var pinnedIds = new HashSet<Guid>(pinned.Select(i => i.Id));
            var pinnedKeys = new HashSet<string>(pinned.Select(filmOf), StringComparer.OrdinalIgnoreCase);
            var rest = pool.Where(i => !pinnedIds.Contains(i.Id) && !pinnedKeys.Contains(filmOf(i))).ToList();
            var r = new Result { Share = share, Random = On(t) };
            List<BaseItem> picks;
            if (r.Random)
            {
                var groups = rest.Select(filmOf).Distinct(StringComparer.OrdinalIgnoreCase)
                    .Select(k => new KeyValuePair<string, int>(k, 1)).ToList();
                r.Pool = groups.Count;
                r.Period = PeriodKey(t, config, now);
                var chosen = Choose(groups, Math.Max(0, t.Limit - share), Seed(t.Tag, r.Period));
                picks = rest.Where(i => chosen.Contains(filmOf(i))).ToList();
            }
            else if (t.Limit > 0)
            {
                picks = HomeScreenCompanionTask.FirstTitles(rest, Math.Max(0, t.Limit - share), filmOf);
                r.Pool = rest.Select(filmOf).Distinct(StringComparer.OrdinalIgnoreCase).Count();
            }
            else
            {
                picks = rest;
                r.Pool = rest.Count;
            }
            if (addPinned) r.Items.AddRange(pinned);
            r.Pinned = addPinned ? pinned.Count : 0;
            r.Items.AddRange(picks);
            r.Picked = picks.Select(filmOf).Distinct(StringComparer.OrdinalIgnoreCase).Count();
            return r;
        }
    }

    /// <summary>
    /// The period of the last Random pick each source applied (random-pick-state.txt: one
    /// "group key TAB period" line per source), so the Random pick task only re-runs a source when
    /// its period changed. Written by real syncs and Run Groups, never by a dry run or a preview.
    /// </summary>
    internal static class RandomPickState
    {
        private static readonly object Gate = new object();

        private static string? FilePath => Plugin.Instance == null ? null : Path.Combine(Plugin.Instance.DataFolderPath, "random-pick-state.txt");

        internal static Dictionary<string, string> Load()
        {
            var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            lock (Gate)
            {
                try
                {
                    var path = FilePath;
                    if (path == null || !File.Exists(path)) return result;
                    foreach (var line in File.ReadAllLines(path))
                    {
                        var p = line.Split('\t');
                        if (p.Length == 2 && p[0].Length > 0) result[p[0]] = p[1];
                    }
                }
                catch (Exception ex) { PluginLog.Swallowed(ex); }
            }
            return result;
        }

        internal static void Save(IDictionary<string, string> applied)
        {
            if (applied.Count == 0) return;
            lock (Gate)
            {
                try
                {
                    var path = FilePath;
                    if (path == null) return;
                    var all = Load();
                    foreach (var kv in applied) all[kv.Key] = kv.Value;
                    Directory.CreateDirectory(Path.GetDirectoryName(path)!);
                    File.WriteAllLines(path, all.OrderBy(k => k.Key, StringComparer.OrdinalIgnoreCase).Select(k => k.Key + "\t" + k.Value));
                }
                catch (Exception ex) { PluginLog.Swallowed(ex); }
            }
        }
    }
}
