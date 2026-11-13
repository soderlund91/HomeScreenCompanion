using MediaBrowser.Controller.Entities;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace HomeScreenCompanion
{
    /// <summary>
    /// "Random pick" on a source: each Max is filled with titles picked at random from everything
    /// the source matches, instead of the first ones. The pick is worked out, not stored: it comes
    /// from the tag and the day (or the Monday of the week), so every sync on the same day picks
    /// the same titles (no tag churn, nothing to save, Dry Run changes nothing) and the next day
    /// or week brings a new pick. Each title gets a rank from a hash of the seed and the title, the
    /// lowest ranks are taken: a title that joins or leaves the source changes at most one pick,
    /// and a higher Max only adds titles.
    /// Max counts Emby items here (a film's versions are separate items and all get the tag), so
    /// exactly Max items carry the tag and a home row showing that many shows every one of them.
    /// Pinned titles (Manual sources with "Pinned", same tag) are never in the pick; with
    /// RandomPickCountPinned they are part of the Max: 1 pinned and Max 8 picks 7.
    /// </summary>
    public static class RandomPick
    {
        internal static bool On(TagConfig t) => t.RandomPick && t.Limit > 0 && !RotatingRow.Is(t) && !HomeScreenCompanionTask.IsNextWatch(t);

        internal static bool Weekly(TagConfig t) => string.Equals(t.RandomPickEvery, "Week", StringComparison.OrdinalIgnoreCase);

        // A Manual source marked Pinned whose titles count for random picks of its tag.
        internal static bool IsPinnedSource(TagConfig t) => t.Pinned && string.Equals(t.SourceType, "Manual", StringComparison.OrdinalIgnoreCase);

        /// <summary>The period a pick is for: "D2026-11-13", or "W2026-11-09" (that week's Monday).</summary>
        public static string PeriodKey(bool weekly, DateTime today)
        {
            var d = today.Date;
            if (weekly) d = d.AddDays(-(((int)d.DayOfWeek + 6) % 7));
            return (weekly ? "W" : "D") + d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        }

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

        internal sealed class Result
        {
            public List<BaseItem> Items = new List<BaseItem>();
            public int Pool;      // titles to pick from (pinned ones left out)
            public int Pinned;    // pinned items counted in the Max
            public string Period = "";
            public int Counted => Items.Count + Pinned;
        }

        /// <summary>The source's pick from pool (kept in the pool's order) for today.</summary>
        internal static Result Apply(TagConfig t, List<BaseItem> pool, IList<BaseItem> pinned, DateTime today)
        {
            var filmOf = PopularityCounter.FilmKeys(pool.Concat(pinned).ToList());
            var pinnedKeys = new HashSet<string>(pinned.Select(filmOf), StringComparer.OrdinalIgnoreCase);
            int pinnedCount = t.RandomPickCountPinned ? pinned.Count : 0;
            var groups = pool.Where(i => !pinnedKeys.Contains(filmOf(i)))
                .GroupBy(filmOf, StringComparer.OrdinalIgnoreCase)
                .Select(g => new KeyValuePair<string, int>(g.Key, g.Count()))
                .ToList();
            var period = PeriodKey(Weekly(t), today);
            var chosen = Choose(groups, Math.Max(0, t.Limit - pinnedCount), Seed(t.Tag, period));
            return new Result
            {
                Items = pool.Where(i => chosen.Contains(filmOf(i))).ToList(),
                Pool = groups.Count,
                Pinned = pinnedCount,
                Period = period
            };
        }
    }
}
