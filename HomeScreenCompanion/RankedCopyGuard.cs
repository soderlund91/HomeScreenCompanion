using MediaBrowser.Controller.Collections;
using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Entities.Movies;
using MediaBrowser.Controller.Library;
using MediaBrowser.Model.Entities;
using MediaBrowser.Model.Querying;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace HomeScreenCompanion
{
    /// <summary>
    /// Keeps each ranked copy (RankedCollections) in its collection and merged with its film
    /// between syncs. Emby keeps both on the copy itself (its Collections and its
    /// PresentationUniqueKey) and writes the whole item on every save, so a save from an object
    /// that was read before a change undoes that change without any event. On a server, a copy
    /// that had been watched was found out of its collection and split from its film; it stayed
    /// that way until the next full sync, and dropped out again after the next play.
    ///
    /// Triggers: an update of a ranked copy, a play / played mark of a copy or of its film, and a
    /// copy taken out of a collection. They are collected and checked once they go quiet (Emby may
    /// still save the copy shortly after playback stops), and every copy that needed a fix is
    /// checked once more a little later. The check is state-based (is the copy in its collection,
    /// is it a version of its film, does it show its film's image files), so the updates it makes itself end in a check that changes
    /// nothing. It waits while a sync runs: the sync sets the same things itself.
    /// </summary>
    internal static class RankedCopyGuard
    {
        private static ILibraryManager? _libraryManager;
        private static ICollectionManager? _collectionManager;
        private static PluginLogger? _logger;

        private static readonly TimeSpan QuietDelay = TimeSpan.FromSeconds(30);
        private static readonly TimeSpan RecheckDelay = TimeSpan.FromMinutes(2);
        private static readonly object _lock = new object();
        private static readonly HashSet<long> _pendingCopies = new HashSet<long>();
        private static readonly HashSet<string> _pendingImdbs = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        private static Timer? _timer;
        private static int _running;

        internal static void Initialize(ILibraryManager libraryManager, ICollectionManager collectionManager, PluginLogger logger)
        {
            _libraryManager = libraryManager;
            _collectionManager = collectionManager;
            _logger = logger;
        }

        private static bool Enabled => Plugin.Instance?.Configuration?.RankedCollectionsEnabled == true
                                       && RankedCollections.RankedRoot is string root && Directory.Exists(root);

        /// <summary>A ranked copy changed (ItemUpdated, removed from a collection, played).</summary>
        internal static void QueueCopy(BaseItem? item)
        {
            if (item == null || !(item is Movie) || !RankedCollections.IsRankedCopy(item) || !Enabled) return;
            lock (_lock) { _pendingCopies.Add(item.InternalId); Arm(QuietDelay); }
        }

        /// <summary>A real film was played or marked played: check its ranked copies.</summary>
        internal static void QueueFilm(BaseItem? item)
        {
            if (item == null || !(item is Movie) || TopListCollectionMirror.IsTopListItem(item) || !Enabled) return;
            if (RankedCollections.IsRankedCopy(item)) { QueueCopy(item); return; }
            var imdb = item.GetProviderId("Imdb");
            if (string.IsNullOrEmpty(imdb)) return;
            lock (_lock) { _pendingImdbs.Add(imdb!); Arm(QuietDelay); }
        }

        // Caller holds _lock.
        private static void Arm(TimeSpan delay)
        {
            if (_timer == null) _timer = new Timer(_ => Flush(), null, delay, Timeout.InfiniteTimeSpan);
            else _timer.Change(delay, Timeout.InfiniteTimeSpan);
        }

        private static void Flush()
        {
            if (_libraryManager == null || _collectionManager == null) return;
            // A sync running: it merges and fills the collections itself. Look again after it.
            if (RunGate.Busy || Interlocked.CompareExchange(ref _running, 1, 0) != 0)
            {
                lock (_lock) Arm(QuietDelay);
                return;
            }
            long[] copyIds; string[] imdbs;
            lock (_lock)
            {
                copyIds = _pendingCopies.ToArray(); _pendingCopies.Clear();
                imdbs = _pendingImdbs.ToArray(); _pendingImdbs.Clear();
            }
            Task.Run(() =>
            {
                try
                {
                    var ids = new HashSet<long>(copyIds);
                    var root = RankedCollections.RankedRoot;
                    if (imdbs.Length > 0 && root != null)
                    {
                        foreach (var c in _libraryManager.GetItemList(new InternalItemsQuery
                        {
                            IncludeItemTypes = new[] { "Movie" },
                            Recursive = true,
                            PathStartsWith = root + Path.DirectorySeparatorChar,
                            AnyProviderIdEquals = imdbs.Select(i => new KeyValuePair<string, string>("Imdb", i)).ToList()
                        }))
                        {
                            if (RankedCollections.IsRankedCopy(c) && imdbs.Contains(c.GetProviderId("Imdb") ?? "", StringComparer.OrdinalIgnoreCase))
                                ids.Add(c.InternalId);
                        }
                    }
                    var fixedIds = new List<long>();
                    // Dry run: only logged; nothing is fixed, so there is no recheck either.
                    bool dry = DryRun.On;
                    foreach (var id in ids)
                    {
                        try { if (Heal(id, dry)) fixedIds.Add(id); }
                        catch (Exception ex) { _logger?.Warn($"[Ranked] Ranked copy {id} could not be checked: {ex.Message}"); }
                    }
                    if (fixedIds.Count > 0)
                        lock (_lock) { foreach (var id in fixedIds) _pendingCopies.Add(id); Arm(RecheckDelay); }
                }
                catch (Exception ex) { _logger?.Warn($"[Ranked] Ranked copy check failed: {ex.Message}"); }
                finally { Interlocked.Exchange(ref _running, 0); }
            });
        }

        // Puts one copy back in its collection, merges it with its film and gives it the film's
        // images again when needed.
        // True when something was fixed. dry: logs what it would fix and changes nothing (false).
        private static bool Heal(long copyId, bool dry)
        {
            var lm = _libraryManager!;
            var copy = lm.GetItemById(copyId);
            if (!(copy is Video) || !RankedCollections.IsRankedCopy(copy) || !File.Exists(copy.Path)) return false;

            // Only a copy whose source still wants a ranked collection (Cleanup removes the others).
            var dir = Path.GetDirectoryName(copy.Path) ?? "";
            var config = Plugin.Instance?.Configuration;
            if (config == null) return false;
            var folder = Path.GetFileName(dir);
            if (!(config.Tags ?? new List<TagConfig>()).Any(tc => RankedCollections.Wants(config, tc)
                    && string.Equals(RankedCollections.FolderName(tc), folder, StringComparison.OrdinalIgnoreCase)))
                return false;

            bool fixedAny = false;

            // Version merge with the film the .strm points at (else the film with the same IMDb id).
            BaseItem? film = null;
            try { var target = File.ReadAllText(copy.Path).Trim(); if (target.Length > 0) film = lm.FindByPath(target, false); } catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            var imdb = copy.GetProviderId("Imdb");
            if ((film == null || RankedCollections.IsRankedCopy(film)) && !string.IsNullOrEmpty(imdb))
                film = lm.GetItemList(new InternalItemsQuery
                {
                    IncludeItemTypes = new[] { "Movie" },
                    Recursive = true,
                    IsVirtualItem = false,
                    AnyProviderIdEquals = new List<KeyValuePair<string, string>> { new KeyValuePair<string, string>("Imdb", imdb!) }
                }).Where(f => !RankedCollections.IsRankedCopy(f) && !TopListCollectionMirror.IsTopListItem(f)
                           && string.Equals(f.GetProviderId("Imdb"), imdb, StringComparison.OrdinalIgnoreCase))
                  .OrderBy(f => f.InternalId).FirstOrDefault();
            if (film is Video filmVideo && film.Id != copy.Id)
            {
                // The film's top-list copies too: a split of the film's versions takes them apart as well.
                var group = new HashSet<long>(filmVideo.GetAlternateVersionIds());
                var parts = new List<BaseItem> { film };
                if (!group.Contains(copy.InternalId)) parts.Add(copy);
                var filmImdb = film.GetProviderId("Imdb");
                if (!string.IsNullOrEmpty(filmImdb))
                    parts.AddRange(lm.GetItemList(new InternalItemsQuery
                    {
                        IncludeItemTypes = new[] { "Movie" },
                        Recursive = true,
                        IsVirtualItem = false,
                        AnyProviderIdEquals = new List<KeyValuePair<string, string>> { new KeyValuePair<string, string>("Imdb", filmImdb!) }
                    }).Where(t => TopListCollectionMirror.IsTopListItem(t) && !group.Contains(t.InternalId)
                               && string.Equals(t.GetProviderId("Imdb"), filmImdb, StringComparison.OrdinalIgnoreCase)));
                if (parts.Count > 1 && dry)
                    _logger?.Info($"[Ranked] [Dry run] '{copy.Name}': {RunLog.Plural(parts.Count - 1, "version")} would be merged with the film again");
                else if (parts.Count > 1)
                {
                    lm.MergeItems(parts.ToArray());
                    _logger?.Info($"[Ranked] '{copy.Name}': {RunLog.Plural(parts.Count - 1, "version")} merged with the film again");
                    fixedAny = true;
                }
            }

            // Collection membership (read again: the merge saved the copy).
            string? collName = null;
            try { var m = Path.Combine(dir, "collection.name"); if (File.Exists(m)) collName = File.ReadAllText(m).Trim(); } catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            if (!string.IsNullOrEmpty(collName))
            {
                var coll = lm.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "BoxSet" }, Name = collName, Recursive = true })
                    .OfType<BoxSet>().FirstOrDefault();
                var fresh = lm.GetItemById(copyId) ?? copy;
                if (coll != null && dry && !(fresh.Collections ?? Array.Empty<MediaBrowser.Model.Dto.LinkedItemInfo>()).Any(c => c != null && c.Id == coll.InternalId))
                    _logger?.Info($"[Ranked] [Dry run] '{copy.Name}' is not in collection '{collName}': would be added again");
                else if (coll != null && !(fresh.Collections ?? Array.Empty<MediaBrowser.Model.Dto.LinkedItemInfo>()).Any(c => c != null && c.Id == coll.InternalId))
                {
                    _collectionManager!.AddToCollection(coll.InternalId, new[] { copyId }).GetAwaiter().GetResult();
                    _logger?.Info($"[Ranked] '{copy.Name}' was no longer in collection '{collName}': added again");
                    fixedAny = true;
                    // The ranked collection holds the copy instead of the film (as the sync does).
                    if (film != null && !RankedCollections.IsRankedCopy(film)
                        && (lm.GetItemById(film.InternalId)?.Collections ?? Array.Empty<MediaBrowser.Model.Dto.LinkedItemInfo>()).Any(c => c != null && c.Id == coll.InternalId))
                        _collectionManager.RemoveFromCollection(coll, new[] { film.InternalId });
                }
            }
            // Images: the copy shows its film's own image files (as the sync sets them). A save
            // from a stale object or a refresh can leave the copy with none, and a re-merge does
            // not bring them back. Read both again: the steps above saved them.
            if (film != null && !RankedCollections.IsRankedCopy(film))
            {
                var freshCopy = lm.GetItemById(copyId);
                var freshFilm = lm.GetItemById(film.InternalId) ?? film;
                // (MatchFilmImages only changes the object read here; it is saved below, never in a dry run.)
                if (freshCopy != null && dry && RankedCollections.MatchFilmImages(freshCopy, freshFilm))
                    _logger?.Info($"[Ranked] [Dry run] '{copy.Name}': images would be restored from the film");
                else if (freshCopy != null && !dry && RankedCollections.MatchFilmImages(freshCopy, freshFilm))
                {
                    lm.UpdateItem(freshCopy, freshCopy.Parent, ItemUpdateType.MetadataEdit, null);
                    _logger?.Info($"[Ranked] '{copy.Name}': images restored from the film");
                    fixedAny = true;
                }
            }
            return fixedAny;
        }
    }
}
