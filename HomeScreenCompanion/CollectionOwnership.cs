using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Library;
using MediaBrowser.Model.Entities;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace HomeScreenCompanion
{
    /// <summary>
    /// Which collections HSC made. A source finds its collection by name, so it can meet a
    /// collection that was already in Emby (made by Emby's TMDb grouping, another plugin or by hand).
    /// HSC only deletes a collection it made itself (its id is in the created file) or, as before,
    /// never deletes an imported one. In a collection it did not make, HSC adds its titles, takes
    /// out again only the titles it added (kept per collection in the added file), leaves the
    /// collection's other titles, images and details alone, and leaves the collection in Emby when
    /// the source goes.
    ///
    /// Installs from before the created file existed: on first use the file is filled once with the
    /// collections in HSC's name history that have no TMDb / IMDb id (HSC never gives a collection one,
    /// Emby's own TMDb collections always have one) and were not imported. Those are cleaned up as
    /// before; anything else in the history is treated as not made by HSC.
    /// </summary>
    internal static class CollectionOwnership
    {
        private const string CreatedFile = "homescreencompanion_collections_created.txt";
        private const string AddedFile = "homescreencompanion_collections_added.txt";
        private const string HistoryFile = "homescreencompanion_collections.txt";

        private static readonly object Gate = new object();
        private static HashSet<long>? _created;

        private static string? DataDir => Plugin.Instance?.DataFolderPath;

        /// <summary>True when HSC made this collection.</summary>
        internal static bool IsCreated(BaseItem coll, ILibraryManager libraryManager)
        {
            lock (Gate) return Created(libraryManager).Contains(coll.InternalId);
        }

        /// <summary>Ids of every collection HSC made.</summary>
        internal static HashSet<long> CreatedIds(ILibraryManager libraryManager)
        {
            lock (Gate) return new HashSet<long>(Created(libraryManager));
        }

        internal static void RecordCreated(long collId, ILibraryManager libraryManager)
        {
            lock (Gate)
            {
                var set = Created(libraryManager);
                if (set.Add(collId)) SaveCreated(set);
            }
        }

        /// <summary>The titles HSC put in a collection it did not make.</summary>
        internal static HashSet<long> Added(long collId)
        {
            lock (Gate) return LoadAdded().TryGetValue(collId, out var ids) ? new HashSet<long>(ids) : new HashSet<long>();
        }

        internal static void SetAdded(long collId, IEnumerable<long> ids)
        {
            lock (Gate)
            {
                var all = LoadAdded();
                var set = new HashSet<long>(ids);
                if (set.Count == 0) all.Remove(collId); else all[collId] = set;
                SaveAdded(all);
            }
        }

        private static HashSet<long> Created(ILibraryManager libraryManager)
        {
            if (_created != null) return _created;
            var dir = DataDir;
            var set = new HashSet<long>();
            if (string.IsNullOrEmpty(dir)) return set;
            var path = Path.Combine(dir, CreatedFile);
            if (File.Exists(path))
            {
                try { foreach (var l in File.ReadAllLines(path)) if (long.TryParse(l.Trim(), out var id)) set.Add(id); }
                catch (Exception swallowed) { PluginLog.Swallowed(swallowed); return set; }   // not cached: read again next time
            }
            else
            {
                var fromHistory = FromHistory(libraryManager);
                if (fromHistory == null) return set;   // not saved or cached: tried again next time
                set = fromHistory;
                SaveCreated(set);
            }
            return _created = set;
        }

        // First run after the update: the collections HSC's name history says it made. Null when
        // the history or the library could not be read.
        private static HashSet<long>? FromHistory(ILibraryManager libraryManager)
        {
            var set = new HashSet<long>();
            try
            {
                var path = Path.Combine(DataDir!, HistoryFile);
                var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                if (File.Exists(path)) foreach (var l in File.ReadAllLines(path)) if (l.Trim().Length > 0) names.Add(l.Trim());
                if (names.Count == 0) return set;
                var imported = HomeScreenCompanionTask.RecordImportedCollections(Plugin.Instance?.Configuration);
                foreach (var coll in libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "BoxSet" }, Recursive = true }))
                {
                    var name = (coll.Name ?? "").Trim();
                    if (!names.Contains(name) || imported.Contains(name)) continue;
                    if (!string.IsNullOrEmpty(coll.GetProviderId("Tmdb")) || !string.IsNullOrEmpty(coll.GetProviderId("Imdb"))) continue;
                    set.Add(coll.InternalId);
                }
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); return null; }
            return set;
        }

        private static void SaveCreated(HashSet<long> set)
        {
            var dir = DataDir;
            if (string.IsNullOrEmpty(dir)) return;
            try { Directory.CreateDirectory(dir); File.WriteAllLines(Path.Combine(dir, CreatedFile), set.OrderBy(i => i).Select(i => i.ToString())); }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
        }

        // One line per collection: "<collection id> <title id>,<title id>,…"
        private static Dictionary<long, HashSet<long>> LoadAdded()
        {
            var all = new Dictionary<long, HashSet<long>>();
            var dir = DataDir;
            if (string.IsNullOrEmpty(dir)) return all;
            var path = Path.Combine(dir, AddedFile);
            try
            {
                if (!File.Exists(path)) return all;
                foreach (var l in File.ReadAllLines(path))
                {
                    var parts = l.Trim().Split(new[] { ' ' }, 2);
                    if (parts.Length < 2 || !long.TryParse(parts[0], out var collId)) continue;
                    all[collId] = new HashSet<long>(parts[1].Split(',').Select(s => long.TryParse(s.Trim(), out var id) ? id : 0).Where(id => id != 0));
                }
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            return all;
        }

        private static void SaveAdded(Dictionary<long, HashSet<long>> all)
        {
            var dir = DataDir;
            if (string.IsNullOrEmpty(dir)) return;
            try
            {
                Directory.CreateDirectory(dir);
                File.WriteAllLines(Path.Combine(dir, AddedFile),
                    all.OrderBy(kv => kv.Key).Select(kv => kv.Key + " " + string.Join(",", kv.Value.OrderBy(i => i))));
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
        }
    }
}
