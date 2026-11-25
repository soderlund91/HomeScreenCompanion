using MediaBrowser.Controller.Library;
using MediaBrowser.Model.Entities;
using MediaBrowser.Model.Querying;
using MediaBrowser.Model.Serialization;
using MediaBrowser.Model.Users;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace HomeScreenCompanion
{
    /// <summary>
    /// Take over home rows (as Import collection takes over a collection): rows users already have that
    /// show what a source makes (its tag, or its collection) while the source has no home row of its
    /// own. Nothing is saved or changed here: the page fills in the source's Home Screen tab from the
    /// rows (its users, the row's settings, Position "after" the row above), and the next sync finds
    /// those rows by what they show (the order a source's row is found in, ARCHITECTURE.md 4.1) and
    /// keeps their ids and places. From then on they are the source's rows like any HSC made (owner
    /// 2026-11-19): switching the home row off or deleting the source removes them.
    /// Rows another source or a top-list tracks, rows on a top-list's library and Copy &amp; Sync copies
    /// of another source's row are never offered.
    /// </summary>
    internal static class RowTakeOver
    {
        // The row settings a source keeps (HomeSectionSettings), as the source card's form has them.
        private static readonly string[] Fields = { "DisplayMode", "ViewType", "ImageType", "CardSizeOffset", "SortBy", "SortOrder", "ScrollDirection" };

        /// <summary>What a source (saved or not) would take over: one option per row type that has rows.</summary>
        internal static List<RowTakeOverOption> Find(PluginConfiguration config, ILibraryManager lm, IUserManager um, IJsonSerializer json,
            string? name, string? tag, string? collectionName, bool tagRow, bool collectionRow, Snapshot? snap = null, bool listOnly = false)
        {
            var result = new List<RowTakeOverOption>();
            name = (name ?? "").Trim(); tag = (tag ?? "").Trim();
            if (tag.Length == 0) tag = name;
            if (tag.Length == 0) return result;
            snap ??= Snapshot.Take(config, lm, um);
            var ownKey = GroupIssues.Key(name, tag);
            var srcKey = HomeSectionSyncTask.SourceKey(name, tag);

            var types = new List<(string type, HashSet<string> shows, string label)>();
            if (tagRow)
                types.Add(("items", HomeScreenCompanionTask.SourceRowShows(lm, "items", tag, null, "auto", null), $"tag '{tag}'"));
            if (collectionRow && !string.IsNullOrWhiteSpace(collectionName))
                types.Add(("boxset", HomeScreenCompanionTask.SourceRowShows(lm, "boxset", null, collectionName!.Trim(), "auto", null), $"collection '{collectionName!.Trim()}'"));

            foreach (var (type, shows, label) in types)
            {
                if (shows.Count == 0) continue;
                var opt = new RowTakeOverOption { Name = name.Length > 0 ? name : tag, Tag = tag, SourceKey = srcKey, SectionType = type, Shows = label };
                // Another source writes the same tag (e.g. a Priority Override): two sources cannot own one
                // row. Not offered in the lists; on the source card the owner is told.
                if (type == "items")
                {
                    var sharing = (config.Tags ?? new List<TagConfig>())
                        .Where(t => string.Equals((t.Tag ?? "").Trim(), tag, StringComparison.OrdinalIgnoreCase)
                                 && !string.Equals(HomeScreenCompanionTask.GroupKey(t), ownKey, StringComparison.OrdinalIgnoreCase))
                        .Select(t => !string.IsNullOrWhiteSpace(t.Name) ? t.Name.Trim() : (t.Tag ?? "").Trim()).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
                    if (sharing.Count > 0)
                    {
                        if (listOnly) continue;
                        opt.Warnings.Add($"The source{(sharing.Count == 1 ? "" : "s")} {string.Join(", ", sharing.Select(n => "'" + n + "'"))} also write{(sharing.Count == 1 ? "s" : "")} this tag. Only one source should have its home row.");
                    }
                }
                var picks = new List<(UserSections user, ContentSection row, int index)>();
                foreach (var u in snap.Users)
                {
                    for (int i = 0; i < u.Sections.Length; i++)
                    {
                        var s = u.Sections[i];
                        if (!HomeScreenCompanionTask.ShowsSame(s, type, shows)) continue;
                        if (snap.OwnedElsewhere(s.Id, ownKey, srcKey) || TopListSyncTask.IsTopListRow(s, snap.TopListLibraries)) continue;
                        picks.Add((u, s, i));
                        break;   // one row per user (a second one stays the user's own)
                    }
                }
                if (picks.Count == 0) continue;

                // The settings most users' rows have; the others get them too.
                var bySettings = picks.GroupBy(p => SettingsKey(SettingsOf(p.row, type))).OrderByDescending(g => g.Count()).ToList();
                var common = bySettings[0].First();
                var settings = SettingsOf(common.row, type);
                if (bySettings.Count > 1)
                {
                    var others = bySettings.Skip(1).SelectMany(g => g).Select(p => p.user.Name).ToList();
                    opt.Warnings.Add($"{RunLog.Plural(others.Count, "user")} ({string.Join(", ", others)}) {(others.Count == 1 ? "has" : "have")} the row with other settings (title, sorting or look): they get the ones most users have.");
                }
                // Position: after the row above it, as most users have it (the rows stay where they are).
                var position = picks.Select(p => p.index == 0 ? "top"
                        : "after:" + HomeScreenCompanionTask.PositionKeysOf(um, p.user.InternalId, p.user.Sections)(p.user.Sections[p.index - 1]))
                    .GroupBy(x => x).OrderByDescending(g => g.Count()).First().Key;
                settings["_position"] = position;
                opt.Position = HomeScreenCompanionTask.PositionLabel(position);
                opt.Settings = json.SerializeToString(settings);
                opt.Title = settings.TryGetValue("CustomName", out var cn) && cn.Length > 0 ? cn : (settings.ContainsKey("_hideTitle") ? "(no title)" : opt.Name);
                opt.Users = picks.Select(p => new RowTakeOverUser { UserId = p.user.Id, Name = p.user.Name, SectionId = p.row.Id ?? "", Disabled = p.user.Disabled }).ToList();
                result.Add(opt);
            }
            return result;
        }

        /// <summary>Every saved source without a home row whose tag or collection users already have a row of.</summary>
        internal static List<RowTakeOverOption> List(PluginConfiguration config, ILibraryManager lm, IUserManager um, IJsonSerializer json)
        {
            var snap = Snapshot.Take(config, lm, um);
            var result = new List<RowTakeOverOption>();
            foreach (var g in (config.Tags ?? new List<TagConfig>()).GroupBy(HomeScreenCompanionTask.GroupKey))
            {
                var tc = g.First();
                if (!CanHaveRows(tc) || g.Any(t => t.EnableHomeSection)) continue;
                bool tagRow = g.Any(t => t.EnableTag);
                var coll = g.FirstOrDefault(t => t.EnableCollection && !string.IsNullOrWhiteSpace(t.CollectionName));
                result.AddRange(Find(config, lm, um, json, tc.Name, tc.Tag, coll?.CollectionName, tagRow, coll != null, snap, listOnly: true));
            }
            return result;
        }

        // A source whose row could be one of these: not a rotating source, Your Next Watch or a top-list.
        private static bool CanHaveRows(TagConfig tc)
            => !RotatingRow.Is(tc) && !string.Equals(tc.SourceType, "NextWatch", StringComparison.OrdinalIgnoreCase) && !SourceTopLists.IsOn(tc);

        // The source settings that give this row: its title, type, look, sorting, media type and libraries.
        internal static Dictionary<string, string> SettingsOf(ContentSection s, string type)
        {
            var d = new Dictionary<string, string>(StringComparer.Ordinal) { ["SectionType"] = type };
            foreach (var f in Fields)
            {
                var p = typeof(ContentSection).GetProperty(f);
                var v = p?.GetValue(s);
                d[f] = v == null ? (f == "CardSizeOffset" ? "0" : "") : Convert.ToString(v, System.Globalization.CultureInfo.InvariantCulture) ?? "";
            }
            var title = s.CustomName ?? "";
            if (title.Length > 0 && title.Trim().Length == 0) d["_hideTitle"] = "true";   // HiddenTitle
            else
            {
                var t = HomeScreenCompanionTask.StripInactive(title).Trim();
                if (t.Length > 0) d["CustomName"] = t;
            }
            var played = s.Query?.GetType().GetProperty("IsPlayed")?.GetValue(s.Query);
            d["_queryIsPlayed"] = played is bool b ? (b ? "true" : "false") : "";
            var itemTypes = (s.ItemTypes ?? Array.Empty<string>()).Where(x => !string.IsNullOrWhiteSpace(x)).ToArray();
            d["ItemTypes"] = "[" + string.Join(",", (itemTypes.Length > 0 ? itemTypes : new[] { "Movie", "Series" }).Select(x => "\"" + x.Replace("\"", "") + "\"")) + "]";
            // The libraries it leaves out (HSC's own are added again by every run, see 4.2).
            var excluded = string.Join(",", (s.ExcludedFolders ?? Array.Empty<string>()).Where(x => !string.IsNullOrWhiteSpace(x)));
            if (excluded.Length > 0) { d["_queryExcludeViewIds"] = excluded; d["ExcludedFolders"] = excluded; }
            return d;
        }

        private static string SettingsKey(Dictionary<string, string> d)
            => string.Join("\u001F", d.Where(kv => kv.Key != "ExcludedFolders" && kv.Key != "_queryExcludeViewIds").OrderBy(kv => kv.Key, StringComparer.Ordinal).Select(kv => kv.Key + "=" + kv.Value))
               + "\u001F" + string.Join(",", (d.TryGetValue("ExcludedFolders", out var e) ? e : "").Split(',').OrderBy(x => x, StringComparer.Ordinal));

        internal sealed class UserSections
        {
            public string Id = "";
            public string Name = "";
            public long InternalId;
            public bool Disabled;
            public ContentSection[] Sections = Array.Empty<ContentSection>();
        }

        /// <summary>Every user's rows, and which rows HSC already owns, read once.</summary>
        internal sealed class Snapshot
        {
            public List<UserSections> Users = new List<UserSections>();
            public HashSet<string> TopListLibraries = new HashSet<string>();
            private readonly Dictionary<string, string> _trackedBy = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase); // section id → owner key
            private readonly Dictionary<string, string> _copyOf = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);    // section id → Copy & Sync row key

            // A row another source (or a top-list) tracks, or a Copy & Sync copy of another source's row.
            public bool OwnedElsewhere(string? sectionId, string ownGroupKey, string ownSrcKey)
            {
                if (string.IsNullOrEmpty(sectionId)) return true;
                if (_trackedBy.TryGetValue(sectionId!, out var owner) && !string.Equals(owner, ownGroupKey, StringComparison.OrdinalIgnoreCase)) return true;
                return _copyOf.TryGetValue(sectionId!, out var key) && !string.Equals(key, ownSrcKey, StringComparison.OrdinalIgnoreCase);
            }

            public static Snapshot Take(PluginConfiguration config, ILibraryManager lm, IUserManager um)
            {
                var snap = new Snapshot { TopListLibraries = TopListSyncTask.TopListLibraryIds(config, lm) };
                foreach (var tc in config.Tags ?? new List<TagConfig>())
                    foreach (var t in tc.HomeSectionTracked ?? new List<HomeSectionTracking>())
                        if (!string.IsNullOrEmpty(t.SectionId)) snap._trackedBy[t.SectionId] = HomeScreenCompanionTask.GroupKey(tc);
                foreach (var tl in config.TopLists ?? new List<TopListHomeSection>())
                    foreach (var t in tl.HomeSectionTracked ?? new List<HomeSectionTracking>())
                        if (!string.IsNullOrEmpty(t.SectionId)) snap._trackedBy[t.SectionId] = "tl\u001F" + tl.TagName;
                foreach (var a in config.HomeSyncAdditions ?? new List<HomeSyncAddition>())
                    if (a.List == HomeSyncAddition.Row && !string.IsNullOrEmpty(a.SectionId) && (a.Key ?? "").StartsWith(HomeSectionSyncTask.SrcPrefix, StringComparison.Ordinal))
                        snap._copyOf[a.SectionId] = a.Key ?? "";
                var status = UserStatus.Read(um);
                foreach (var u in um.GetUserList(new UserQuery()))
                {
                    try
                    {
                        var id = u.Id.ToString("N");
                        snap.Users.Add(new UserSections
                        {
                            Id = id, Name = u.Name ?? id, InternalId = u.InternalId, Disabled = status.IsDisabled(id),
                            Sections = um.GetHomeSections(u.InternalId, CancellationToken.None)?.Sections ?? Array.Empty<ContentSection>()
                        });
                    }
                    catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
                }
                snap.Users = snap.Users.OrderBy(x => x.Name, StringComparer.OrdinalIgnoreCase).ToList();
                return snap;
            }
        }
    }
}
