using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Library;
using MediaBrowser.Model.Entities;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace HomeScreenCompanion
{
    // What a pinned or hand-picked title is, next to its Emby item id (TagConfig.PinnedItemRefs /
    // ManualItemRefs): Emby makes a new item (a new id) when a film's file or folder is replaced,
    // and the title is then found again by these.
    public class TitleRef
    {
        public string Id { get; set; } = "";
        public string Type { get; set; } = "";      // "Movie" or "Series"
        public string Imdb { get; set; } = "";
        public string Tmdb { get; set; } = "";
        public string Tvdb { get; set; } = "";
        public string Name { get; set; } = "";
        public int? Year { get; set; }
    }

    /// <summary>
    /// Pinned titles (PinnedItemIds) and a Manual List (ManualItemIds) store Emby item ids. Each id
    /// has a TitleRef (IMDb / TMDb / TVDb id, else name + year): an id that is gone, or now is one of
    /// HSC's copies, is looked up by it, always as the original library item. A run puts the new id
    /// in the config (Refresh); a save from the settings page keeps the refs (Carry).
    /// </summary>
    internal static class TitleRefs
    {
        private static ILibraryManager? _libraryManager;
        // Old id → new id of the titles moved this server session: a page that was open before the
        // run still has the old id and saves it.
        private static readonly Dictionary<string, string> _moved = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        internal static void Initialize(ILibraryManager libraryManager) => _libraryManager = libraryManager;

        internal static TitleRef RefFor(BaseItem item) => new TitleRef
        {
            Id = item.InternalId.ToString(),
            Type = item.GetType().Name,
            Imdb = item.GetProviderId("Imdb") ?? "",
            Tmdb = item.GetProviderId("Tmdb") ?? "",
            Tvdb = item.GetProviderId("Tvdb") ?? "",
            Name = item.Name ?? "",
            Year = item.ProductionYear
        };

        internal static BaseItem? ById(string id)
        {
            if (_libraryManager == null || string.IsNullOrWhiteSpace(id)) return null;
            try
            {
                if (long.TryParse(id, out var internalId)) return _libraryManager.GetItemById(internalId);
                if (Guid.TryParse(id, out var guid)) return _libraryManager.GetItemById(guid);
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            return null;
        }

        // A movie or show of the library itself: not a top-list or ranked copy, nothing in HSC's data folder.
        internal static bool IsOriginal(BaseItem? item)
        {
            if (item == null || !HomeScreenCompanionTask.IsTaggableTopLevelItem(item) || !TopListCollectionMirror.IsOriginal(item)) return false;
            string? data = null;
            try { data = Plugin.Instance?.DataFolderPath; } catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            return string.IsNullOrEmpty(data) || string.IsNullOrEmpty(item.Path)
                || !item.Path.StartsWith(data.TrimEnd(Path.DirectorySeparatorChar) + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase);
        }

        // The title's item: by its id, else by its ref (from refs, else from the saved config: the
        // page's Preview and Copy send the card without refs). Null when it is in the library under neither.
        internal static BaseItem? Resolve(string id, IEnumerable<TitleRef>? refs)
        {
            var item = ById(id);
            if (IsOriginal(item)) return item;
            var r = refs?.FirstOrDefault(x => x != null && string.Equals(x.Id, id, StringComparison.OrdinalIgnoreCase));
            if (r == null) AllRefs(Plugin.Instance?.Configuration).TryGetValue(id ?? "", out r);
            return r == null ? null : Find(r);
        }

        // The library's original item for a ref: by IMDb, TMDb, TVDb id (TMDb and TVDb numbers are
        // only unique per type), then by name + year.
        internal static BaseItem? Find(TitleRef r)
        {
            if (_libraryManager == null || r == null) return null;
            var types = string.IsNullOrEmpty(r.Type) ? new[] { "Movie", "Series" } : new[] { r.Type };
            BaseItem? Pick(InternalItemsQuery q)
            {
                q.IncludeItemTypes = types;
                q.Recursive = true;
                q.IsVirtualItem = false;
                try { return _libraryManager.GetItemList(q).Where(IsOriginal).OrderBy(i => i.InternalId).FirstOrDefault(); }
                catch (Exception swallowed) { PluginLog.Swallowed(swallowed); return null; }
            }
            foreach (var (p, v) in new[] { ("Imdb", r.Imdb), ("Tmdb", r.Tmdb), ("Tvdb", r.Tvdb) })
            {
                if (string.IsNullOrWhiteSpace(v)) continue;
                var hit = Pick(new InternalItemsQuery { AnyProviderIdEquals = new List<KeyValuePair<string, string>> { new KeyValuePair<string, string>(p, v) } });
                if (hit != null) return hit;
            }
            if (string.IsNullOrWhiteSpace(r.Name)) return null;
            var q = new InternalItemsQuery { Name = r.Name };
            if (r.Year.HasValue) q.Years = new[] { r.Year.Value };
            var byName = Pick(q);
            return byName != null && string.Equals(byName.Name, r.Name, StringComparison.OrdinalIgnoreCase)
                && (!r.Year.HasValue || byName.ProductionYear == r.Year) ? byName : null;
        }

        // Every ref in the config by id (pinned and hand-picked), and the titles moved this session.
        internal static Dictionary<string, TitleRef> AllRefs(PluginConfiguration? config)
        {
            var map = new Dictionary<string, TitleRef>(StringComparer.OrdinalIgnoreCase);
            foreach (var t in config?.Tags ?? new List<TagConfig>())
                foreach (var r in (t?.PinnedItemRefs ?? new List<TitleRef>()).Concat(t?.ManualItemRefs ?? new List<TitleRef>()))
                    if (r != null && !string.IsNullOrEmpty(r.Id) && !map.ContainsKey(r.Id)) map[r.Id] = r;
            lock (_moved)
                foreach (var kv in _moved)
                    if (!map.ContainsKey(kv.Key) && map.TryGetValue(kv.Value, out var now)) map[kv.Key] = now;
            return map;
        }

        // A save (Plugin.UpdateConfiguration): the settings page does not send the refs, so each id
        // gets the one it had, or a new one from the library for a title just picked. An id of a
        // title this session moved becomes the new id.
        internal static void Carry(PluginConfiguration? current, PluginConfiguration? incoming)
        {
            if (incoming?.Tags == null) return;
            var known = AllRefs(current);
            foreach (var t in incoming.Tags)
            {
                if (t == null) continue;
                t.ManualItemIds = CarryList(t.ManualItemIds, t.ManualItemRefs, known, out var manualRefs);
                t.ManualItemRefs = manualRefs;
                t.PinnedItemIds = CarryList(t.PinnedItemIds, t.PinnedItemRefs, known, out var pinnedRefs);
                t.PinnedItemRefs = pinnedRefs;
            }
        }

        private static List<string> CarryList(List<string>? ids, List<TitleRef>? sent, Dictionary<string, TitleRef> known, out List<TitleRef> refs)
        {
            var outIds = new List<string>();
            refs = new List<TitleRef>();
            foreach (var raw in ids ?? new List<string>())
            {
                if (string.IsNullOrWhiteSpace(raw)) continue;
                var id = raw;
                lock (_moved) if (_moved.TryGetValue(id, out var moved) && !IsOriginal(ById(id))) id = moved;
                if (outIds.Contains(id)) continue;
                outIds.Add(id);
                var r = sent?.FirstOrDefault(x => x != null && string.Equals(x.Id, id, StringComparison.OrdinalIgnoreCase));
                if (r == null && known.TryGetValue(id, out var k)) r = k;
                if (r == null) { var item = ById(id); if (IsOriginal(item)) r = RefFor(item!); }
                if (r != null) refs.Add(Copy(r, id));
            }
            return outIds;
        }

        private static TitleRef Copy(TitleRef r, string id) => new TitleRef
        {
            Id = id, Type = r.Type ?? "", Imdb = r.Imdb ?? "", Tmdb = r.Tmdb ?? "", Tvdb = r.Tvdb ?? "", Name = r.Name ?? "", Year = r.Year
        };

        /// <summary>
        /// At the start of a run: every pinned and hand-picked id that is still in the library gets
        /// its ref (configs from before refs), and one that is gone moves to the item its ref finds
        /// (logged once per title). apply false (dry run): only logs what would change. True when
        /// the config was changed.
        /// </summary>
        internal static bool Refresh(IEnumerable<TagConfig>? tags, bool apply, Action<string> info)
        {
            if (_libraryManager == null || tags == null) return false;
            bool changed = false;
            var logged = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var found = new Dictionary<string, BaseItem?>(StringComparer.OrdinalIgnoreCase);
            foreach (var t in tags)
            {
                if (t == null) continue;
                if ((t.ManualItemIds?.Count ?? 0) > 0)
                {
                    var refs = t.ManualItemRefs ?? new List<TitleRef>();
                    if (RefreshList(t.ManualItemIds!, ref refs, "Hand-picked", apply, info, logged, found)) changed = true;
                    t.ManualItemRefs = refs;
                }
                if ((t.PinnedItemIds?.Count ?? 0) > 0)
                {
                    var refs = t.PinnedItemRefs ?? new List<TitleRef>();
                    if (RefreshList(t.PinnedItemIds!, ref refs, "Pinned", apply, info, logged, found)) changed = true;
                    t.PinnedItemRefs = refs;
                }
            }
            return changed;
        }

        private static bool RefreshList(List<string> ids, ref List<TitleRef> refs, string what, bool apply, Action<string> info,
            HashSet<string> logged, Dictionary<string, BaseItem?> found)
        {
            bool changed = false;
            var newIds = new List<string>();
            var newRefs = new List<TitleRef>();
            foreach (var id in ids)
            {
                if (string.IsNullOrWhiteSpace(id)) { changed = true; continue; }
                var stored = refs.FirstOrDefault(x => x != null && string.Equals(x.Id, id, StringComparison.OrdinalIgnoreCase));
                var item = ById(id);
                if (IsOriginal(item))
                {
                    var now = RefFor(item!);
                    if (stored != null)
                    {
                        // Ids the item lost (a metadata refresh in progress) are kept.
                        if (now.Imdb.Length == 0) now.Imdb = stored.Imdb ?? "";
                        if (now.Tmdb.Length == 0) now.Tmdb = stored.Tmdb ?? "";
                        if (now.Tvdb.Length == 0) now.Tvdb = stored.Tvdb ?? "";
                    }
                    if (!newIds.Contains(id)) { newIds.Add(id); newRefs.Add(now); }
                    if (stored == null || !Same(stored, now)) changed = true;
                    continue;
                }
                BaseItem? hit = null;
                if (stored != null)
                {
                    var cacheKey = what + "|" + id;
                    if (!found.TryGetValue(cacheKey, out hit)) found[cacheKey] = hit = Find(stored);
                }
                if (hit == null)
                {
                    // Not found anywhere: kept, so the settings page lists it as not found.
                    if (!newIds.Contains(id)) { newIds.Add(id); if (stored != null) newRefs.Add(stored); }
                    continue;
                }
                var newId = hit.InternalId.ToString();
                var label = stored!.Name.Length > 0 ? stored.Name : (hit.Name ?? "");
                if (logged.Add(what + "|" + id))
                    info(apply ? $"{what} title '{label}' moved to a new item, updated"
                               : $"{what} title '{label}' moved to a new item, would be updated");
                if (!apply)
                {
                    if (!newIds.Contains(id)) { newIds.Add(id); newRefs.Add(stored); }
                    continue;
                }
                lock (_moved) _moved[id] = newId;
                changed = true;
                if (!newIds.Contains(newId)) { newIds.Add(newId); newRefs.Add(RefFor(hit)); }
            }
            if (refs.Count != newRefs.Count) changed = true;
            if (apply && changed)
            {
                ids.Clear();
                ids.AddRange(newIds);
                refs = newRefs;
            }
            return apply && changed;
        }

        private static bool Same(TitleRef a, TitleRef b) =>
            a.Id == b.Id && a.Type == b.Type && a.Imdb == b.Imdb && a.Tmdb == b.Tmdb && a.Tvdb == b.Tvdb && a.Name == b.Name && a.Year == b.Year;
    }
}
