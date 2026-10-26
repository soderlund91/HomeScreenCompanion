using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Library;
using MediaBrowser.Model.Serialization;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace HomeScreenCompanion
{
    /// <summary>
    /// A source top-list's titles in list order: toplist_members/&lt;tag&gt;.json in the plugin data
    /// folder (Emby item ids). The top-list is built from it (SyncTopListFolders,
    /// ShowTopList.SeriesForTag), so a source's Top-List does not need Apply Tag. The tag name stays
    /// the list's key (folder, rows, removal queue). Without a file (no run since 4.5) the titles
    /// carrying the tag are used, as before.
    ///
    /// Several groups can write the same tag, so each group's titles are kept on their own
    /// (toplist_members/parts/&lt;tag&gt;/&lt;group&gt;.json) and the list is all of them, in the order of
    /// the groups in the settings (<see cref="Combine"/>). A run only writes the parts of the groups
    /// it ran: a Run Group keeps the other groups' titles, a group that failed with "Preserve…" on
    /// keeps its own, and a group switched off or out of its schedule gets an empty part — as its
    /// tag would be taken off its titles.
    /// </summary>
    internal static class TopListMembers
    {
        private static string Root => Path.Combine(Plugin.Instance!.DataFolderPath, "toplist_members");
        private static string Name(string tag) => ShowTopList.SanitizeName((tag ?? "").Trim());
        private static string PathFor(string tag) => Path.Combine(Root, Name(tag) + ".json");
        private static string PartsDir(string tag) => Path.Combine(Root, "parts", Name(tag));

        private static string PartPath(string tag, string groupKey)
        {
            using var sha = SHA1.Create();
            var hash = sha.ComputeHash(Encoding.UTF8.GetBytes((groupKey ?? "").Trim().ToLowerInvariant()));
            return Path.Combine(PartsDir(tag), string.Concat(hash.Select(b => b.ToString("x2"))) + ".json");
        }

        /// <summary>The groups that write a tag, in settings order (several flat entries make one group).</summary>
        internal static List<string> GroupsOf(PluginConfiguration config, string tag) =>
            (config.Tags ?? new List<TagConfig>())
                .Where(t => string.Equals((t.Tag ?? "").Trim(), (tag ?? "").Trim(), StringComparison.OrdinalIgnoreCase))
                .Select(t => GroupIssues.Key(t.Name, t.Tag))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();

        /// <summary>True when a source top-list is built from this tag (its parts are worth writing).</summary>
        internal static bool Wanted(PluginConfiguration config, string tag) =>
            (config.TopLists ?? new List<TopListHomeSection>()).Any(t => t.FromSource
                && string.Equals((t.TagName ?? "").Trim(), (tag ?? "").Trim(), StringComparison.OrdinalIgnoreCase));

        private static List<string>? ReadIds(string file, IJsonSerializer json)
        {
            try
            {
                if (!File.Exists(file)) return null;
                return json.DeserializeFromFile<List<string>>(file) ?? new List<string>();
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); return null; }
        }

        // Written next to it first: a failed write never leaves a cut-off list behind.
        private static void WriteIds(string file, IEnumerable<string> ids, IJsonSerializer json)
        {
            Directory.CreateDirectory(Path.GetDirectoryName(file)!);
            var tmp = file + ".tmp";
            json.SerializeToFile(ids.ToList(), tmp);
            if (File.Exists(file)) File.Replace(tmp, file, null);
            else File.Move(tmp, file);
        }

        /// <summary>The ids of the saved list, in order (null = no list saved yet).</summary>
        internal static List<string>? LoadIds(string tag, IJsonSerializer json) => ReadIds(PathFor(tag), json);

        /// <summary>The saved list's items, in order; titles removed from Emby since are left out.</summary>
        internal static List<BaseItem>? Load(string tag, ILibraryManager libraryManager, IJsonSerializer json)
        {
            var ids = LoadIds(tag, json);
            if (ids == null) return null;
            var items = new List<BaseItem>();
            foreach (var id in ids)
                if (Guid.TryParse(id, out var g) && libraryManager.GetItemById(g) is BaseItem item) items.Add(item);
            return items;
        }

        /// <summary>
        /// The movies of a source's movie top-list in list order, without top-list copies. Before the
        /// source's first run since 4.5 (no saved list): the movies with its tag, unsorted
        /// (FromList false: the caller sorts them by the rank file).
        /// </summary>
        internal static (List<BaseItem> Items, bool FromList) MoviesFor(string tag, ILibraryManager libraryManager, IJsonSerializer json, string topListsFolder)
        {
            var members = Load(tag, libraryManager, json);
            var items = (members?.Where(i => i is MediaBrowser.Controller.Entities.Movies.Movie && !i.IsVirtualItem)
                ?? libraryManager.GetItemList(new InternalItemsQuery
                {
                    Tags = new[] { tag },
                    IncludeItemTypes = new[] { "Movie" },
                    Recursive = true,
                    IsVirtualItem = false
                })).Where(i => string.IsNullOrEmpty(i.Path) || !i.Path.StartsWith(topListsFolder, StringComparison.OrdinalIgnoreCase))
                .ToList();
            return (items, members != null);
        }

        /// <summary>
        /// One group's titles (duplicates dropped). keepOld: one of the group's sources failed and its
        /// titles were kept, so the group's saved titles not in the new list stay, after it.
        /// False when it could not be written (the list should then not be combined).
        /// </summary>
        internal static bool SavePart(string tag, string groupKey, IEnumerable<BaseItem> items, IJsonSerializer json, bool keepOld = false)
        {
            try
            {
                var file = PartPath(tag, groupKey);
                var ids = new List<string>();
                var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (var item in items)
                {
                    var id = item.Id.ToString("N");
                    if (seen.Add(id)) ids.Add(id);
                }
                if (keepOld)
                    foreach (var id in ReadIds(file, json) ?? new List<string>())
                        if (seen.Add(id)) ids.Add(id);
                WriteIds(file, ids, json);
                return true;
            }
            catch (Exception ex) { PluginLog.Write("WRN", "Top-lists", $"Top-list '{tag}': titles could not be saved — " + PluginLog.Describe(ex)); return false; }
        }

        /// <summary>
        /// The list: the parts of the tag's groups in their order. Parts of groups that no longer write
        /// the tag are removed. Nothing is written while none of the groups has a part yet (the
        /// list stays as it was). False when it could not be written (the list stays as it was).
        /// </summary>
        internal static bool Combine(string tag, IReadOnlyList<string> groupKeys, IJsonSerializer json)
        {
            try
            {
                var wanted = new HashSet<string>(groupKeys.Select(g => PartPath(tag, g)), StringComparer.OrdinalIgnoreCase);
                var dir = PartsDir(tag);
                if (Directory.Exists(dir))
                    foreach (var f in Directory.GetFiles(dir, "*.json"))
                        if (!wanted.Contains(f)) File.Delete(f);

                var ids = new List<string>();
                var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                bool any = false;
                foreach (var g in groupKeys)
                {
                    var part = ReadIds(PartPath(tag, g), json);
                    if (part == null) continue;
                    any = true;
                    foreach (var id in part)
                        if (seen.Add(id)) ids.Add(id);
                }
                if (any) WriteIds(PathFor(tag), ids, json);
                return true;
            }
            catch (Exception ex) { PluginLog.Write("WRN", "Top-lists", $"Top-list '{tag}': titles could not be saved — " + PluginLog.Describe(ex)); return false; }
        }

        /// <summary>The list was removed: its saved titles and parts go too.</summary>
        internal static void Delete(string tag)
        {
            try
            {
                var file = PathFor(tag);
                if (File.Exists(file)) File.Delete(file);
                var dir = PartsDir(tag);
                if (Directory.Exists(dir)) Directory.Delete(dir, true);
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
        }
    }
}
