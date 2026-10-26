using MediaBrowser.Controller.Collections;
using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Entities.Movies;
using MediaBrowser.Controller.Library;
using MediaBrowser.Model.Entities;
using MediaBrowser.Model.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace HomeScreenCompanion
{
    // Optional (TopListMirrorCollections): gives each top-list .strm copy the same collection
    // memberships as its original movie, so "Included in <collection>" shows on the copy too.
    // Membership lives on each item in Emby and MergeItems does not copy it.
    //
    // Trade-off (why it is opt-in): Emby groups the two merged versions into one entry in the
    // collection, and depending on the collection's sort order that entry can be the top-list
    // copy — i.e. the rank-badged poster shows up in the collection.
    //
    // When the setting is off, copies are kept out of all collections instead.
    //
    // All writes are state-based (desired vs. current membership, never replayed deltas) and
    // serialised through one lock, so concurrent or out-of-order collection events cannot
    // leave a copy in the wrong state — the next reconcile always converges.
    internal static class TopListCollectionMirror
    {
        private static ILibraryManager? _libraryManager;
        private static ICollectionManager? _collectionManager;
        private static PluginLogger? _logger;
        private static readonly SemaphoreSlim _mirrorLock = new SemaphoreSlim(1, 1);

        // Collection events are collected and reconciled in one pass once they go quiet — the
        // main sync fires an add and a remove per collection back to back.
        private static readonly TimeSpan DebounceDelay = TimeSpan.FromSeconds(3);
        private static readonly object _pendingLock = new object();
        private static readonly HashSet<long> _pendingCollections = new HashSet<long>();
        private static Timer? _debounceTimer;

        internal static bool Enabled => Plugin.Instance?.Configuration?.TopListMirrorCollections == true;

        internal static void Initialize(ILibraryManager libraryManager, ICollectionManager collectionManager, PluginLogger logger)
        {
            _libraryManager = libraryManager;
            _collectionManager = collectionManager;
            _logger = logger;
        }

        private static string? TopListsFolder
        {
            get
            {
                var dataPath = Plugin.Instance?.DataFolderPath;
                return string.IsNullOrEmpty(dataPath) ? null : Path.Combine(dataPath, "toplists") + Path.DirectorySeparatorChar;
            }
        }

        internal static bool IsTopListItem(BaseItem item)
        {
            var folder = TopListsFolder;
            return folder != null && !string.IsNullOrEmpty(item?.Path)
                && item!.Path.StartsWith(folder, StringComparison.OrdinalIgnoreCase);
        }

        private static long[] CollectionIdsOf(BaseItem item) =>
            (item.Collections ?? Array.Empty<MediaBrowser.Model.Dto.LinkedItemInfo>())
                .Where(c => c != null && c.Id > 0).Select(c => c.Id).Distinct().ToArray();

        private static void AddTo(Dictionary<string, List<BaseItem>> map, string key, BaseItem item)
        {
            if (!map.TryGetValue(key, out var list)) map[key] = list = new List<BaseItem>();
            list.Add(item);
        }

        // Copies and (optionally) their originals, grouped by IMDb id. A film can have several
        // originals (separate 1080p/4K items), so each IMDb id maps to a list. Copies without an
        // IMDb id are grouped under "" (no original ever matches that key).
        //
        // Never loads the whole movie library: copies come from the top-list folder, originals
        // from an IMDb-id query for just those copies. Each query's Where() keeps the result
        // exact even if a server version ignored the narrowing field.
        private static (Dictionary<string, List<BaseItem>> Originals, Dictionary<string, List<BaseItem>> Copies) BuildLookups(bool withOriginals)
        {
            var originals = new Dictionary<string, List<BaseItem>>(StringComparer.OrdinalIgnoreCase);
            var copies = new Dictionary<string, List<BaseItem>>(StringComparer.OrdinalIgnoreCase);
            var folder = TopListsFolder;
            if (folder == null) return (originals, copies);

            foreach (var m in _libraryManager!.GetItemList(new InternalItemsQuery
            {
                IncludeItemTypes = new[] { "Movie" },
                Recursive = true,
                IsVirtualItem = false,
                PathStartsWith = folder
            }).Where(IsTopListItem))
                AddTo(copies, m.GetProviderId("Imdb") ?? "", m);

            if (!withOriginals) return (originals, copies);
            var imdbs = copies.Keys.Where(k => k.Length > 0).ToList();
            for (int i = 0; i < imdbs.Count; i += 100)
            {
                var chunk = imdbs.Skip(i).Take(100).ToList();
                var wanted = new HashSet<string>(chunk, StringComparer.OrdinalIgnoreCase);
                foreach (var m in _libraryManager.GetItemList(new InternalItemsQuery
                {
                    IncludeItemTypes = new[] { "Movie" },
                    Recursive = true,
                    IsVirtualItem = false,
                    AnyProviderIdEquals = chunk.Select(id => new KeyValuePair<string, string>("Imdb", id)).ToList()
                }))
                {
                    var imdb = m.GetProviderId("Imdb") ?? "";
                    if (!IsTopListItem(m) && wanted.Contains(imdb)) AddTo(originals, imdb, m);
                }
            }
            return (originals, copies);
        }

        // Makes one copy's memberships equal to the union of its originals' (or empty when the
        // feature is off or there is no original). Caller must hold _mirrorLock.
        private static async Task<int> SyncCopyCore(BaseItem copy, IEnumerable<BaseItem>? originals)
        {
            var desired = new HashSet<long>();
            if (Enabled && originals != null)
                foreach (var o in originals) desired.UnionWith(CollectionIdsOf(o));
            var current = new HashSet<long>(CollectionIdsOf(copy));
            int changes = 0;

            foreach (var collId in desired.Where(id => !current.Contains(id)))
            {
                try { await _collectionManager!.AddToCollection(collId, new[] { copy.InternalId }).ConfigureAwait(false); changes++; }
                catch (Exception ex) { _logger?.Warn($"[TopList] Could not add '{copy.Name}' to collection {collId}: {ex.Message}"); }
            }
            foreach (var collId in current.Where(id => !desired.Contains(id)))
            {
                try
                {
                    if (_libraryManager!.GetItemById(collId) is BoxSet boxSet)
                    {
                        _collectionManager!.RemoveFromCollection(boxSet, new[] { copy.InternalId });
                        changes++;
                    }
                }
                catch (Exception ex) { _logger?.Warn($"[TopList] Could not remove '{copy.Name}' from collection {collId}: {ex.Message}"); }
            }
            return changes;
        }

        // Single copy (freshly indexed top-list entry). Returns memberships added + removed.
        internal static async Task<int> SyncCopy(BaseItem copy, IEnumerable<BaseItem>? originals)
        {
            if (_collectionManager == null || _libraryManager == null || copy == null) return 0;
            await _mirrorLock.WaitAsync().ConfigureAwait(false);
            try { return await SyncCopyCore(copy, originals).ConfigureAwait(false); }
            finally { _mirrorLock.Release(); }
        }

        // Full pass over every top-list copy. Runs when the setting is toggled, after a backup
        // restore and from the "Merge top-list versions" task.
        internal static async Task<int> SyncAll(CancellationToken cancellationToken)
        {
            if (_libraryManager == null || _collectionManager == null || TopListsFolder == null) return 0;
            await _mirrorLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                var (originals, copies) = BuildLookups(withOriginals: true);
                int changes = 0;
                foreach (var kv in copies)
                {
                    originals.TryGetValue(kv.Key, out var origs);
                    foreach (var copy in kv.Value)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        changes += await SyncCopyCore(copy, origs).ConfigureAwait(false);
                    }
                }
                if (changes > 0)
                    _logger?.Info($"[TopList] Collection mirror ({(Enabled ? "on" : "off")}): {changes} membership change(s).");
                return changes;
            }
            finally { _mirrorLock.Release(); }
        }

        // Run Group on one top-list: only the copies in that list's folder.
        internal static async Task<int> SyncFolder(string folderPath, CancellationToken cancellationToken)
        {
            if (_libraryManager == null || _collectionManager == null || TopListsFolder == null) return 0;
            var prefix = folderPath.TrimEnd(System.IO.Path.DirectorySeparatorChar) + System.IO.Path.DirectorySeparatorChar;
            await _mirrorLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                var (originals, copies) = BuildLookups(withOriginals: true);
                int changes = 0;
                foreach (var kv in copies)
                {
                    originals.TryGetValue(kv.Key, out var origs);
                    foreach (var copy in kv.Value.Where(c => c.Path.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)))
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        changes += await SyncCopyCore(copy, origs).ConfigureAwait(false);
                    }
                }
                return changes;
            }
            finally { _mirrorLock.Release(); }
        }

        internal static void QueueFullSync() =>
            Task.Run(async () =>
            {
                try { await SyncAll(CancellationToken.None).ConfigureAwait(false); }
                catch (Exception ex) { _logger?.Error("[TopList] Collection mirror sync failed: " + ex.Message); }
            });

        // Live follow-up when items are added to / removed from a collection (by the user, by
        // Emby or by this plugin's own collection sync): the collection is queued and, once
        // events go quiet, its copies are reconciled against its current original members.
        // Events that only touch copies are our own changes echoing back and are ignored.
        internal static void OnCollectionChanged(BoxSet collection, IEnumerable<long>? itemIds)
        {
            if (!Enabled || _libraryManager == null || _collectionManager == null || collection == null) return;
            var ids = (itemIds ?? Enumerable.Empty<long>()).ToList();
            if (ids.Count == 0) return;

            bool touchesOriginal;
            try
            {
                touchesOriginal = ids.Any(id =>
                {
                    var item = _libraryManager.GetItemById(id);
                    return item != null && !IsTopListItem(item);
                });
            }
            catch { touchesOriginal = true; }
            if (!touchesOriginal) return;

            lock (_pendingLock)
            {
                _pendingCollections.Add(collection.InternalId);
                if (_debounceTimer == null)
                    _debounceTimer = new Timer(_ => FlushPending(), null, DebounceDelay, Timeout.InfiniteTimeSpan);
                else
                    _debounceTimer.Change(DebounceDelay, Timeout.InfiniteTimeSpan);
            }
        }

        private static void FlushPending()
        {
            long[] collectionIds;
            lock (_pendingLock)
            {
                collectionIds = _pendingCollections.ToArray();
                _pendingCollections.Clear();
            }
            if (collectionIds.Length == 0) return;

            Task.Run(async () =>
            {
                try
                {
                    int changes = await ReconcileCollections(collectionIds).ConfigureAwait(false);
                    if (changes > 0)
                        _logger?.Info($"[TopList] Collection mirror: {changes} membership change(s) in {collectionIds.Length} collection(s).");
                }
                catch (Exception ex)
                {
                    _logger?.Warn($"[TopList] Collection mirror failed: {ex.Message}");
                }
            });
        }

        // For each collection: the copies that belong in it are exactly the copies of the
        // originals currently in it. Adds the missing ones and removes the rest.
        private static async Task<int> ReconcileCollections(long[] collectionIds)
        {
            if (!Enabled || _libraryManager == null || _collectionManager == null) return 0;
            await _mirrorLock.WaitAsync().ConfigureAwait(false);
            try
            {
                if (!Enabled) return 0;
                var (_, copiesByImdb) = BuildLookups(withOriginals: false);
                int changes = 0;
                foreach (var collId in collectionIds)
                {
                    try
                    {
                        if (!(_libraryManager.GetItemById(collId) is BoxSet boxSet)) continue;
                        var members = _libraryManager.GetItemList(new InternalItemsQuery
                        {
                            CollectionIds = new[] { collId },
                            Recursive = true,
                            IsVirtualItem = false
                        });

                        var originalImdbs = new HashSet<string>(
                            members.Where(m => !IsTopListItem(m))
                                   .Select(m => m.GetProviderId("Imdb"))
                                   .Where(s => !string.IsNullOrEmpty(s)),
                            StringComparer.OrdinalIgnoreCase);
                        var desired = new HashSet<long>(originalImdbs
                            .SelectMany(imdb => copiesByImdb.TryGetValue(imdb, out var list) ? list : new List<BaseItem>())
                            .Select(c => c.InternalId));
                        var current = new HashSet<long>(members.Where(IsTopListItem).Select(m => m.InternalId));

                        var toAdd = desired.Where(id => !current.Contains(id)).ToArray();
                        var toRemove = current.Where(id => !desired.Contains(id)).ToArray();
                        if (toAdd.Length > 0)
                            await _collectionManager.AddToCollection(collId, toAdd).ConfigureAwait(false);
                        if (toRemove.Length > 0)
                            _collectionManager.RemoveFromCollection(boxSet, toRemove);
                        changes += toAdd.Length + toRemove.Length;
                    }
                    catch (Exception ex)
                    {
                        _logger?.Warn($"[TopList] Collection mirror for collection {collId} failed: {ex.Message}");
                    }
                }
                return changes;
            }
            finally { _mirrorLock.Release(); }
        }
    }
}
