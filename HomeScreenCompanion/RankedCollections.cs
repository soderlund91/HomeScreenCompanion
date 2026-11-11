using MediaBrowser.Controller.Collections;
using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Entities.Movies;
using MediaBrowser.Controller.Library;
using MediaBrowser.Controller.Providers;
using MediaBrowser.Model.Configuration;
using MediaBrowser.Model.Entities;
using MediaBrowser.Model.IO;
using MediaBrowser.Model.Querying;
using MediaBrowser.Model.Users;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace HomeScreenCompanion
{
    /// <summary>
    /// Experimental "Ranked collections" (Settings → Features, per source "Keep the list order").
    /// For a source with it ticked, every movie of its list gets a .strm copy in
    /// &lt;plugin data&gt;/ranked/&lt;source name&gt;/ ("Title (Year).strm" + .nfo with its ids; the name never
    /// carries the rank, so a reorder keeps every copy),
    /// indexed by one hidden library "&lt;collection&gt; (ranked)". Each copy gets its rank as a
    /// locked SortName, is merged with its real film as a version (one entry in search, shared
    /// watched state) and probed for a runtime. The collection then holds the copies instead of
    /// the real films and sorts by SortName, so it opens in list order. Shows and other non-movie
    /// items stay as they are and sort after the ranked movies.
    ///
    /// Copies live outside the top-list folder on purpose: TopListCollectionMirror keeps top-list
    /// copies out of collections, which would undo this. It never mirrors a ranked copy either,
    /// and the sync takes any top-list copy out of a ranked collection: Emby shows merged versions
    /// as one entry, which would otherwise be the rank-badged top-list copy.
    /// </summary>
    internal static class RankedCollections
    {
        private const string MarkerFile = "collection.name";

        internal static string? RankedRoot
        {
            get
            {
                var dataPath = Plugin.Instance?.DataFolderPath;
                return string.IsNullOrEmpty(dataPath) ? null : Path.Combine(dataPath, "ranked");
            }
        }

        internal static bool IsRankedPath(string? path)
        {
            var root = RankedRoot;
            return root != null && !string.IsNullOrEmpty(path)
                && path!.StartsWith(root + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase);
        }

        internal static bool IsRankedCopy(BaseItem? item) => item != null && IsRankedPath(item.Path);

        // The source wants its collection in list order (feature on, box ticked, collection on).
        internal static bool Wants(PluginConfiguration config, TagConfig? tc) =>
            config != null && config.RankedCollectionsEnabled && tc != null && tc.CollectionKeepOrder && tc.EnableCollection;

        internal static string FolderName(TagConfig tc) => Sanitize(string.IsNullOrWhiteSpace(tc.Name) ? tc.Tag : tc.Name);

        internal static string Sanitize(string name)
        {
            var invalid = Path.GetInvalidFileNameChars();
            var safe = new string((name ?? "unknown").Select(c => Array.IndexOf(invalid, c) >= 0 ? '_' : c).ToArray()).Trim().Trim('.');
            return string.IsNullOrWhiteSpace(safe) ? "unknown" : safe;
        }

        private static string Norm(string? p) => (p ?? "").Replace("\\", "/").TrimEnd('/').ToLowerInvariant();

        internal static VirtualFolderInfo? LibraryFor(ILibraryManager libraryManager, string folderPath) =>
            libraryManager.GetVirtualFolders().FirstOrDefault(f =>
                (f.Locations ?? Array.Empty<string>()).Any(l => Norm(l) == Norm(folderPath)));

        // Internal ids of every ranked library (for hiding them like top-list libraries).
        internal static List<string> LibraryIds(ILibraryManager libraryManager)
        {
            var root = RankedRoot;
            if (root == null) return new List<string>();
            var prefix = Norm(root) + "/";
            return libraryManager.GetVirtualFolders()
                .Where(f => !string.IsNullOrEmpty(f.ItemId)
                         && (f.Locations ?? Array.Empty<string>()).Any(l => Norm(l).StartsWith(prefix, StringComparison.Ordinal)))
                .Select(f => f.ItemId).ToList();
        }

        private static List<BaseItem> CopiesIn(ILibraryManager libraryManager, string folderPath)
        {
            var prefix = folderPath.TrimEnd(Path.DirectorySeparatorChar) + Path.DirectorySeparatorChar;
            return libraryManager.GetItemList(new InternalItemsQuery
            {
                IncludeItemTypes = new[] { "Movie" },
                Recursive = true,
                PathStartsWith = prefix
            }).Where(i => !string.IsNullOrEmpty(i.Path) && i.Path.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)).ToList();
        }

        // Removes every ranked folder (with its copies and library) not in keepFolders, and puts the
        // collection's sort order back. onlyFolder: Run Group — look at that one folder only.
        internal static void Cleanup(HashSet<string> keepFolders, string? onlyFolder, ILibraryManager libraryManager,
            ICollectionManager collectionManager, IUserManager userManager, RunLog log)
        {
            var root = RankedRoot;
            if (root == null || !Directory.Exists(root)) return;
            foreach (var dir in Directory.GetDirectories(root))
            {
                var name = Path.GetFileName(dir);
                if (onlyFolder != null && !string.Equals(name, onlyFolder, StringComparison.OrdinalIgnoreCase)) continue;
                if (keepFolders.Contains(name)) continue;
                Remove(dir, libraryManager, collectionManager, userManager, log);
            }
        }

        private static void Remove(string dir, ILibraryManager libraryManager, ICollectionManager collectionManager, IUserManager userManager, RunLog log)
        {
            var name = Path.GetFileName(dir);
            string? collName = null;
            try { var m = Path.Combine(dir, MarkerFile); if (File.Exists(m)) collName = File.ReadAllText(m).Trim(); } catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            var coll = string.IsNullOrEmpty(collName) ? null
                : libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "BoxSet" }, Name = collName, Recursive = true })
                    .OfType<BoxSet>().FirstOrDefault();
            var copies = CopiesIn(libraryManager, dir);

            // Emby's own order again, before the members change (see SetDisplayOrder).
            if (coll != null && coll.DisplayOrder == CollectionDisplayOrder.SortName)
            {
                coll.DisplayOrder = CollectionDisplayOrder.PremiereDate;
                try { libraryManager.UpdateItem(coll, coll.Parent, ItemUpdateType.MetadataEdit, null); } catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            }

            // The collection gets its real films back (also an imported collection whose source is
            // gone, which no sync fills again); then the copies go.
            if (coll != null && copies.Count > 0)
            {
                try
                {
                    var members = new HashSet<long>(libraryManager.GetItemList(new InternalItemsQuery { CollectionIds = new[] { coll.InternalId }, Recursive = true })
                        .Select(i => i.InternalId));
                    var back = new List<long>();
                    foreach (var copy in copies.Where(c => members.Contains(c.InternalId)))
                    {
                        // The film the .strm points at; else the film with the same IMDb id.
                        BaseItem? film = null;
                        try { var target = File.ReadAllText(copy.Path).Trim(); if (target.Length > 0) film = libraryManager.FindByPath(target, false); } catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
                        var imdb = copy.GetProviderId("Imdb");
                        if (film == null && !string.IsNullOrEmpty(imdb))
                            film = libraryManager.GetItemList(new InternalItemsQuery
                        {
                            IncludeItemTypes = new[] { "Movie" },
                            Recursive = true,
                            IsVirtualItem = false,
                            AnyProviderIdEquals = new List<KeyValuePair<string, string>> { new KeyValuePair<string, string>("Imdb", imdb) }
                        }).Where(f => !IsRankedCopy(f) && !TopListCollectionMirror.IsTopListItem(f)
                                   && string.Equals(f.GetProviderId("Imdb"), imdb, StringComparison.OrdinalIgnoreCase))
                          .OrderBy(f => f.InternalId).FirstOrDefault();
                        if (film != null && !IsRankedCopy(film) && !members.Contains(film.InternalId)) back.Add(film.InternalId);
                    }
                    if (back.Count > 0) collectionManager.AddToCollection(coll.InternalId, back.Distinct().ToArray()).GetAwaiter().GetResult();
                }
                catch (Exception ex) { log.Warn($"Collection '{collName}': real films could not be put back: {ex.Message}"); }
            }

            int removed = 0;
            foreach (var copy in copies)
            {
                try { libraryManager.DeleteItem(copy, new DeleteOptions { DeleteFileLocation = false }); removed++; }
                catch (Exception ex) { log.Warn($"Ranked copy '{copy.Name}' could not be removed: {ex.Message}"); }
            }

            var lib = LibraryFor(libraryManager, dir);
            if (lib != null && long.TryParse(lib.ItemId, out var libId))
            {
                try
                {
                    libraryManager.RemoveVirtualFolder(libId, false);
                    TopListLibraryVisibility.Forget(lib.ItemId, (lib.Guid ?? "").Replace("-", ""), userManager, m => log.Warn(m));
                }
                catch (Exception ex) { log.Warn($"Ranked library '{lib.Name}' could not be removed: {ex.Message}"); }
            }

            try { Directory.Delete(dir, true); }
            catch (Exception ex) { log.Warn($"Ranked folder '{name}' could not be removed: {ex.Message}"); }

            log.Skip($"Ranked collection '{collName ?? name}': ranked copies, folder and library removed ({RunLog.Plural(removed, "copy", "copies")})");
        }

        /// <summary>
        /// Brings the source's ranked copies in line with its list and returns the collection's
        /// members: the copies in rank order, then the non-movie items. A movie whose copy is not
        /// indexed yet is kept as the real film (next sync swaps it). Null when it failed: the
        /// caller then keeps the normal (real film) membership.
        /// </summary>
        internal static async Task<(List<long> Members, List<BaseItem> ToProbe)?> SyncAsync(TagConfig tc, string cName, List<BaseItem> orderedItems,
            ILibraryManager libraryManager, IProviderManager providerManager, IFileSystem fileSystem,
            IUserManager userManager, ICollectionManager collectionManager, PluginConfiguration config, RunLog log, CancellationToken cancellationToken)
        {
            var root = RankedRoot;
            if (root == null) return null;
            var folderPath = Path.Combine(root, FolderName(tc));

            // Movies once each, in list order: the same version the collection would take
            // (lowest id, see HomeScreenCompanionTask.ExtraVersionIds). Copies never count.
            var real = orderedItems.Where(i => !IsRankedCopy(i) && !TopListCollectionMirror.IsTopListItem(i)).ToList();
            var extra = HomeScreenCompanionTask.ExtraVersionIds(real);
            var seenIds = new HashSet<long>();
            var movies = new List<BaseItem>();
            var others = new List<BaseItem>();
            var firstByKey = new Dictionary<string, BaseItem>(StringComparer.OrdinalIgnoreCase);
            foreach (var item in real)
            {
                if (!seenIds.Add(item.InternalId)) continue;
                if (item is Movie)
                {
                    // The list order is that of the first version listed; the version kept is the lowest id.
                    var imdb = item.GetProviderId("Imdb");
                    var key = !string.IsNullOrEmpty(imdb) ? imdb : "id:" + item.InternalId;
                    if (firstByKey.ContainsKey(key)) continue;
                    var keep = extra.Contains(item.InternalId)
                        ? real.Where(r => r is Movie && string.Equals(r.GetProviderId("Imdb"), imdb, StringComparison.OrdinalIgnoreCase) && !extra.Contains(r.InternalId)).FirstOrDefault() ?? item
                        : item;
                    firstByKey[key] = keep;
                    if (!string.IsNullOrEmpty(keep.Path)) movies.Add(keep);
                }
                else if (!extra.Contains(item.InternalId)) others.Add(item);
            }

            // Desired files: one stable "<Title (Year)>.strm" per film (named like the top-list
            // copies), never carrying the rank: a reorder then only changes the copies' SortName and
            // nothing is deleted, recreated, merged or probed again. Films sharing a title and year
            // all get their id appended, so a name never depends on the list order.
            int digits = Math.Max(2, movies.Count.ToString().Length);
            string TitleOf(BaseItem m)
            {
                var t = Sanitize(m.Name);
                if (m.ProductionYear.HasValue && m.ProductionYear > 0) t += $" ({m.ProductionYear})";
                return t;
            }
            var titleCounts = movies.GroupBy(TitleOf, StringComparer.OrdinalIgnoreCase).ToDictionary(g => g.Key, g => g.Count(), StringComparer.OrdinalIgnoreCase);
            var wanted = new List<(string Rank, string BaseName, BaseItem Item)>();
            var usedNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < movies.Count; i++)
            {
                var m = movies[i];
                var baseName = TitleOf(m);
                if (titleCounts[baseName] > 1)
                {
                    var id = m.GetProviderId("Imdb");
                    if (string.IsNullOrEmpty(id)) id = m.GetProviderId("Tmdb");
                    baseName += " [" + Sanitize(string.IsNullOrEmpty(id) ? m.InternalId.ToString() : id!) + "]";
                }
                if (!usedNames.Add(baseName)) continue;
                wanted.Add(((wanted.Count + 1).ToString().PadLeft(digits, '0'), baseName, m));
            }

            Directory.CreateDirectory(folderPath);
            try { File.WriteAllText(Path.Combine(folderPath, MarkerFile), cName); } catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }

            bool filesChanged = false;
            var newFiles = new HashSet<string>(wanted.Where(w => !File.Exists(Path.Combine(folderPath, w.BaseName + ".strm"))).Select(w => w.BaseName), StringComparer.OrdinalIgnoreCase);
            int added = newFiles.Count;
            var keepFiles = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { MarkerFile };
            foreach (var w in wanted)
            {
                keepFiles.Add(w.BaseName + ".strm"); keepFiles.Add(w.BaseName + ".nfo");
                // New copies are written below, a few at a time while someone is watching.
                if (!newFiles.Contains(w.BaseName)) filesChanged |= WriteCopyFiles(folderPath, w.BaseName, w.Item);
            }
            foreach (var f in Directory.GetFiles(folderPath))
            {
                if (keepFiles.Contains(Path.GetFileName(f))) continue;
                try { File.Delete(f); filesChanged = true; } catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            }

            // Copies of films that left the list (or of rank-named files from older builds) are
            // removed straight away.
            var expectedPaths = new HashSet<string>(wanted.Select(w => Path.Combine(folderPath, w.BaseName + ".strm")), StringComparer.OrdinalIgnoreCase);
            int removedCopies = 0;
            foreach (var stale in CopiesIn(libraryManager, folderPath).Where(c => !expectedPaths.Contains(c.Path)))
            {
                try { GentleMode.Timed("copy delete", () => libraryManager.DeleteItem(stale, new DeleteOptions { DeleteFileLocation = false })); removedCopies++; } catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
                await GentleMode.WroteAsync(cancellationToken).ConfigureAwait(false);
            }

            // One hidden library per ranked source.
            var lib = LibraryFor(libraryManager, folderPath);
            bool created = false;
            if (lib == null)
            {
                lib = CreateLibrary(cName + " (ranked)", folderPath, movies, libraryManager, userManager, log);
                if (lib == null) return null;
                created = true;
            }
            if (!long.TryParse(lib.ItemId, out var libInternalId)) return null;
            UseLocalMetadataOnly(libraryManager, libInternalId, lib.Name, log);
            GrantRestrictedUsers(lib, movies, libraryManager, userManager, log);
            // Every sync (not only on creation), so turning "Hide HSC libraries" on or off reaches
            // ranked libraries without a top-list sync (that one stops early when there are no top-lists).
            var visChanged = TopListLibraryVisibility.Apply(config, userManager, libraryManager, m => log.Warn(m), m => log.Info("    " + m));
            if (visChanged > 0)
                log.Info($"    HSC libraries {(config.HideTopListLibraries ? "hidden from" : "shown again in")} My Media and Latest for {RunLog.Plural(visChanged, "user")}");

            // Emby adds the copies when it refreshes this library (never a full scan), a batch per
            // refresh, each waited for: a few while someone is watching, with a pause after it
            // (GentleMode); 25 with nobody watching, so a viewer who starts mid-run is noticed
            // after one batch. A copy is only changed and merged
            // once Emby's refresh of it has finished: merging a copy Emby is still saving fails
            // ("FOREIGN KEY constraint failed").
            var pendingNew = wanted.Where(w => newFiles.Contains(w.BaseName)).ToList();
            int written = wanted.Count - pendingNew.Count;
            int writtenWithImdb = wanted.Count(w => !newFiles.Contains(w.BaseName) && !string.IsNullOrEmpty(w.Item.GetProviderId("Imdb")));
            bool needRefresh = filesChanged || created || CountCopies(libraryManager, folderPath) < written;
            int refreshes = 0;
            bool timedOut = false;
            var phaseStart = DateTime.Now;
            while (pendingNew.Count > 0 || needRefresh)
            {
                cancellationToken.ThrowIfCancellationRequested();
                int take = timedOut ? pendingNew.Count : Math.Min(GentleMode.BatchSize, pendingNew.Count);
                foreach (var w in pendingNew.Take(take))
                {
                    WriteCopyFiles(folderPath, w.BaseName, w.Item);
                    if (!string.IsNullOrEmpty(w.Item.GetProviderId("Imdb"))) writtenWithImdb++;
                }
                pendingNew.RemoveRange(0, take);
                written += take;
                needRefresh = false;
                if (timedOut) break;
                try
                {
                    providerManager.QueueRefresh(libInternalId, new MetadataRefreshOptions(fileSystem)
                    {
                        Recursive = true,
                        MetadataRefreshMode = MetadataRefreshMode.Default,
                        ImageRefreshMode = MetadataRefreshMode.Default
                    }, RefreshPriority.Normal);
                    refreshes++;
                }
                catch (Exception ex) { log.Warn($"Ranked library '{lib.Name}' could not be refreshed: {ex.Message}"); break; }
                // Up to 90 s for Emby to index and finish these copies.
                if (!await WaitForCopiesAsync(providerManager, libraryManager, libInternalId, folderPath, written, writtenWithImdb, TimeSpan.FromSeconds(90), cancellationToken).ConfigureAwait(false))
                    timedOut = true;
                int newDone = added - pendingNew.Count;
                if (added >= 100 && newDone / 50 > (newDone - take) / 50 && pendingNew.Count > 0 && DateTime.Now - phaseStart >= ProgressAfter)
                    log.Info($"    Ranked '{cName}': {newDone}/{added} new copies added  ·  {RunLog.Elapsed(DateTime.Now - phaseStart)}");
                if (pendingNew.Count > 0) await GentleMode.WroteAsync(cancellationToken, take).ConfigureAwait(false);
            }
            if (refreshes > 1) log.Debug($"  Ranked library '{lib.Name}': new copies added in {RunLog.Plural(refreshes, "refresh", "refreshes")}");

            var filmByPath = wanted.ToDictionary(w => Path.Combine(folderPath, w.BaseName + ".strm"), w => w.Item, StringComparer.OrdinalIgnoreCase);
            var byPath = new Dictionary<string, BaseItem>(StringComparer.OrdinalIgnoreCase);
            foreach (var c in CopiesIn(libraryManager, folderPath))
                if (expectedPaths.Contains(c.Path)
                    && (!string.IsNullOrEmpty(c.GetProviderId("Imdb")) || (filmByPath.TryGetValue(c.Path, out var f) && string.IsNullOrEmpty(f.GetProviderId("Imdb")))))
                    byPath[c.Path] = c;

            var members = new List<long>();
            var toProbe = new List<BaseItem>();
            int missing = 0, reranked = 0, prepared = 0;
            var prepareStart = DateTime.Now;
            foreach (var w in wanted)
            {
                cancellationToken.ThrowIfCancellationRequested();
                log.Count($"Ranked collections — {cName}:", ++prepared, wanted.Count, "copies");
                var path = Path.Combine(folderPath, w.BaseName + ".strm");
                if (!byPath.TryGetValue(path, out var copy))
                {
                    members.Add(w.Item.InternalId); // not indexed yet: the real film for now
                    missing++;
                    continue;
                }
                var (rerankedNow, writes, needsProbe) = PrepareCopy(copy, w.Item, w.Rank, libraryManager, log, cancellationToken);
                if (rerankedNow && !newFiles.Contains(w.BaseName)) reranked++;
                members.Add(copy.InternalId);
                if (needsProbe) toProbe.Add(copy);
                await GentleMode.WroteAsync(cancellationToken, writes).ConfigureAwait(false);
                if (wanted.Count >= 100 && prepared % 50 == 0 && prepared < wanted.Count && DateTime.Now - prepareStart >= ProgressAfter)
                    log.Info($"    Ranked '{cName}': {prepared}/{wanted.Count} copies prepared  ·  {RunLog.Elapsed(DateTime.Now - prepareStart)}");
            }
            members.AddRange(others.Select(o => o.InternalId));

            if (missing > 0)
                log.Warn($"Ranked collection '{cName}': {RunLog.Plural(missing, "copy", "copies")} not indexed yet — the real film is used until the next sync");
            log.Ok($"Ranked collection '{cName}': {RunLog.Plural(wanted.Count - missing, "movie")} in list order{(others.Count > 0 ? $", {RunLog.Plural(others.Count, "other item")} after them" : "")}"
                + $" ({added} new, {removedCopies} removed, {reranked} re-ranked)");
            return (members, toProbe);
        }

        // Copies whose film had no media info to copy (never probed) get a probe instead, so they
        // still get a runtime. Called after the collection is updated: a probe still running saves
        // the item it loaded and would drop a membership added meanwhile.
        internal static void Probe(List<BaseItem>? copies, IProviderManager providerManager, IFileSystem fileSystem, CancellationToken cancellationToken = default)
        {
            foreach (var c in copies ?? new List<BaseItem>())
            {
                if (MergeTopListVersionsTask.QueueStrmProbe(providerManager, fileSystem, c))
                    GentleMode.Wrote(cancellationToken);
            }
        }

        // Collections HSC switched to sort by SortName, so it can switch them back.
        private static string? SortedListFile
        {
            get
            {
                var dataPath = Plugin.Instance?.DataFolderPath;
                return string.IsNullOrEmpty(dataPath) ? null : Path.Combine(dataPath, "ranked_display_order.txt");
            }
        }

        private static HashSet<string> LoadSorted()
        {
            var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            try { var f = SortedListFile; if (f != null && File.Exists(f)) foreach (var l in File.ReadAllLines(f)) if (l.Trim().Length > 0) set.Add(l.Trim()); } catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            return set;
        }

        private static void SaveSorted(HashSet<string> set)
        {
            try
            {
                var f = SortedListFile;
                if (f == null) return;
                if (set.Count == 0) { if (File.Exists(f)) File.Delete(f); }
                else File.WriteAllLines(f, set.OrderBy(x => x));
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
        }

        // Sets the collection to sort by SortName (the copies' ranks). Called before the members
        // change (that queues a refresh of the collection, which saves the copy it loaded) and
        // checked again on every sync.
        internal static void SetDisplayOrder(BaseItem? coll, ILibraryManager libraryManager)
        {
            if (!(coll is BoxSet box)) return;
            var sorted = LoadSorted();
            if (sorted.Add(box.Name ?? "")) SaveSorted(sorted);
            if (box.DisplayOrder == CollectionDisplayOrder.SortName) return;
            box.DisplayOrder = CollectionDisplayOrder.SortName;
            try { libraryManager.UpdateItem(box, box.Parent, ItemUpdateType.MetadataEdit, null); } catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
        }

        // Collections that are no longer ranked go back to Emby's default order. A name stays on
        // the list until a sync sees the default order saved (a running refresh of the collection
        // can still write the old value once).
        internal static void RestoreDisplayOrders(IEnumerable<string> stillRanked, ILibraryManager libraryManager)
        {
            var sorted = LoadSorted();
            if (sorted.Count == 0) return;
            var keep = new HashSet<string>(stillRanked, StringComparer.OrdinalIgnoreCase);
            bool changed = false;
            foreach (var name in sorted.ToList())
            {
                if (keep.Contains(name)) continue;
                var box = libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "BoxSet" }, Name = name, Recursive = true })
                    .OfType<BoxSet>().FirstOrDefault();
                if (box == null || box.DisplayOrder != CollectionDisplayOrder.SortName) { sorted.Remove(name); changed = true; continue; }
                box.DisplayOrder = CollectionDisplayOrder.PremiereDate;
                try { libraryManager.UpdateItem(box, box.Parent, ItemUpdateType.MetadataEdit, null); } catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            }
            if (changed) SaveSorted(sorted);
        }

        // Rank as locked SortName, the real film's images, metadata and media info (one save),
        // merged as a version. Returns whether the rank changed, how many writes it made (for
        // GentleMode) and whether the copy still needs a probe (its film has no media info).
        private static (bool Reranked, int Writes, bool NeedsProbe) PrepareCopy(BaseItem copy, BaseItem film, string rank, ILibraryManager libraryManager, RunLog log, CancellationToken cancellationToken)
        {
            bool changed = false, reranked = false;
            int writes = 0;
            if (!string.Equals(copy.SortName, rank, StringComparison.Ordinal))
            {
                copy.SortName = rank;
                changed = reranked = true;
            }
            var locked = (copy.LockedFields ?? Array.Empty<MetadataFields>()).ToList();
            if (!locked.Contains(MetadataFields.SortName))
            {
                locked.Add(MetadataFields.SortName);
                copy.LockedFields = locked.ToArray();
                changed = true;
            }
            // The film's own poster / backdrop / logo, so the collection looks the same as before.
            if (MatchFilmImages(copy, film)) changed = true;
            // The details page shows the film's own text and media info: the ranked library
            // fetches nothing online and Emby never probes a .strm during a scan (a copy without a
            // runtime is marked played when stopped instead of getting a resume point).
            changed |= ShowTopList.SyncMetadata(copy, film, isSeries: true);
            changed |= ShowTopList.SyncVideoInfo(copy, film);
            if (changed)
            {
                try { GentleMode.Timed("copy save", () => libraryManager.UpdateItem(copy, copy.Parent, ItemUpdateType.MetadataEdit, null)); writes++; }
                catch (Exception ex) { log.Warn($"Ranked copy '{copy.Name}' could not be updated: {ex.Message}"); }
            }
            bool needsProbe = false;
            try
            {
                if (GentleMode.TimedIfWrote("media streams", () => ShowTopList.CopyMediaStreams(copy, film))) writes++;
                needsProbe = (copy.RunTimeTicks ?? 0) <= 0;
            }
            catch (Exception ex) { log.Warn($"Ranked copy '{copy.Name}': media info could not be copied: {ex.Message}"); needsProbe = (copy.RunTimeTicks ?? 0) <= 0; }
            try
            {
                if (CopyPeople(copy, film, libraryManager, cancellationToken, "cast", m => log.Debug(m))) writes++;
            }
            catch (OperationCanceledException) { throw; }
            catch (Exception ex) { log.Debug($"  Ranked copy '{copy.Name}': cast could not be copied: {ex.Message}"); }

            // Merge once: merging saves the real film again (and its nfo, when the library writes nfo files).
            try
            {
                bool merged = film is Video v && v.GetAlternateVersionIds().Contains(copy.InternalId);
                if (!merged && copy.Id != film.Id)
                {
                    GentleMode.Timed("merge", () => libraryManager.MergeItems(new[] { film, copy }));
                    writes += 2;
                }
            }
            catch (Exception ex) { log.Warn($"Ranked copy '{copy.Name}' could not be merged with '{film.Name}': {ex.Message}"); }
            return (reranked, writes, needsProbe);
        }

        // The film's cast and crew, once (the copy's library fetches no people itself): the copy's
        // details page shows them (Cast & Crew, Director), and has none without them. Used for
        // ranked and top-list copies; kind names the write in the run summary.
        // Emby looks every new person up when it saves a cast, all inside one database write that
        // holds up every other write (playback progress too); on a large library one lookup can
        // take a long time. The people go with their provider ids: Emby then finds them by id
        // (and the exact person rather than a namesake), and remembers them, so people saved in an
        // earlier step cost next to nothing in the next one (each save replaces the whole list).
        // While someone is watching the cast goes in steps, each its own short write: CastFirstStep
        // new people first, then as many as the last step's time per person says fit in
        // CastStepTarget (at most twice the last step); a step over the target halves the next one
        // (down to 1 person). A step that still took over GentleMode.LongStep is followed by a
        // longer pause (GentleMode.AfterStep). Nobody watching: the whole cast in one write.
        private const int CastFirstStep = 2;
        private const int CastMaxStep = 50;
        private static readonly TimeSpan CastStepTarget = TimeSpan.FromMilliseconds(50);

        // A ranked collection of 100+ copies logs its progress every 50 copies, once it has run this long.
        private static readonly TimeSpan ProgressAfter = TimeSpan.FromSeconds(10);

        internal static bool CopyPeople(BaseItem copy, BaseItem film, ILibraryManager libraryManager, CancellationToken cancellationToken,
            string kind = "cast", Action<string>? debug = null)
        {
            var theirs = libraryManager.GetItemPeople(new InternalPeopleQuery { ItemIds = new[] { film.InternalId }, EnableProviderIds = true }) ?? new List<PersonInfo>();
            if (theirs.Count == 0) return false;
            // Done when the copy has them all; a cast left short (a run stopped between steps) is
            // saved again from the start.
            var mine = libraryManager.GetItemPeople(copy) ?? new List<PersonInfo>();
            if (mine.Count >= theirs.Count) return false;
            int done = 0, step = CastFirstStep;
            while (done < theirs.Count)
            {
                bool watching = GentleMode.Streaming;
                int upTo = watching ? Math.Min(theirs.Count, done + step) : theirs.Count;
                var part = upTo == theirs.Count ? theirs : theirs.Take(upTo).ToList();
                var sw = System.Diagnostics.Stopwatch.StartNew();
                GentleMode.Timed(kind, () => libraryManager.UpdatePeople(copy, part, true));
                var took = sw.Elapsed;
                int added = upTo - done;
                done = upTo;
                if (watching) debug?.Invoke($"  '{copy.Name}': {kind} step {done}/{theirs.Count} ({added} new) {took.TotalMilliseconds:0} ms");
                if (done >= theirs.Count) break;
                if (took > CastStepTarget) step = Math.Max(1, step / 2);
                else
                {
                    double msPerPerson = Math.Max(0.1, took.TotalMilliseconds / added);
                    step = Math.Max(1, Math.Min(Math.Min(CastMaxStep, step * 2), (int)(CastStepTarget.TotalMilliseconds / msPerPerson)));
                }
                GentleMode.AfterStep(took, cancellationToken);
            }
            return true;
        }

        // Writes a copy's .nfo, then its .strm (Emby creates the item when it sees the .strm, the
        // nfo must be there already). True when either changed.
        private static bool WriteCopyFiles(string folderPath, string baseName, BaseItem film)
        {
            bool changed = WriteIfChanged(Path.Combine(folderPath, baseName + ".nfo"), BuildNfo(film));
            changed |= WriteIfChanged(Path.Combine(folderPath, baseName + ".strm"), film.Path);
            return changed;
        }

        // How many copies Emby has indexed in the folder; identified: only those saved with their
        // IMDb id, i.e. whose first refresh is done (ids only: cheap enough to poll).
        private static int CountCopies(ILibraryManager libraryManager, string folderPath, bool identified = false)
        {
            var prefix = folderPath.TrimEnd(Path.DirectorySeparatorChar) + Path.DirectorySeparatorChar;
            var query = new InternalItemsQuery
            {
                IncludeItemTypes = new[] { "Movie" },
                Recursive = true,
                PathStartsWith = prefix
            };
            if (identified) query.HasAnyProviderId = new[] { "Imdb" };
            return libraryManager.GetInternalItemIds(query).Length;
        }

        // Waits until Emby's refresh of the library is no longer running or queued, at least
        // `expected` copies are indexed and `identified` of them saved with their IMDb id (Emby's
        // refresh of the library reports done before the refreshes of its new items are). Polls
        // every 2 s, then 5 s, then every 10 s.
        internal static async Task<bool> WaitForCopiesAsync(IProviderManager providerManager, ILibraryManager libraryManager, long libInternalId,
            string folderPath, int expected, int identified, TimeSpan max, CancellationToken cancellationToken)
        {
            var deadline = DateTime.UtcNow + max;
            var steps = new[] { 2, 5, 10 };
            for (int i = 0; ; i++)
            {
                await Task.Delay(TimeSpan.FromSeconds(steps[Math.Min(i, steps.Length - 1)]), cancellationToken).ConfigureAwait(false);
                bool busy;
                try
                {
                    busy = providerManager.GetRefreshProgress(libInternalId).HasValue
                        || (providerManager.GetRefreshQueue() ?? new List<Tuple<long, MetadataRefreshOptions>>()).Any(q => q.Item1 == libInternalId);
                }
                catch (Exception swallowed) { PluginLog.Swallowed(swallowed); busy = false; }
                if (!busy && CountCopies(libraryManager, folderPath) >= expected
                    && (identified == 0 || CountCopies(libraryManager, folderPath, identified: true) >= identified)) return true;
                if (DateTime.UtcNow > deadline) return false;
            }
        }

        // Ranked and movie top-list copies take everything from their film (nfo with its ids, images,
        // text and media info copied by PrepareCopy / TopListCopies.Prepare), so their library
        // fetches nothing online. Libraries made by older builds fetched TheMovieDb metadata and
        // images for every copy: they are switched over once.
        // Nor does Emby watch the folder (real-time monitoring): HSC writes many files into it at
        // once, and Emby's monitor would refresh the whole library (at high priority) for each of
        // them. HSC refreshes the library itself, once, after it has written.
        private static readonly string[] LocalFetchers = { "Nfo" };
        internal static readonly string[] MovieTypes = { "Movie" };
        internal static readonly string[] ShowTypes = { "Series", "Season", "Episode" };

        private static void UseLocalMetadataOnly(ILibraryManager libraryManager, long libInternalId, string? libName, RunLog log) =>
            UseHscLibraryOptions(libraryManager, libInternalId, libName, MovieTypes, "Ranked library", log);

        internal static void UseHscLibraryOptions(ILibraryManager libraryManager, long libInternalId, string? libName, string[] typeNames, string label, RunLog log)
        {
            try
            {
                if (!(libraryManager.GetItemById(libInternalId) is CollectionFolder folder)) return;
                var options = folder.GetLibraryOptions();
                if (options == null) return;
                var types = (options.TypeOptions ?? Array.Empty<TypeOptions>()).ToList();
                bool fetchers = false;
                foreach (var typeName in typeNames)
                {
                    var t = types.FirstOrDefault(x => string.Equals(x.Type, typeName, StringComparison.OrdinalIgnoreCase));
                    if (t != null && (t.MetadataFetchers ?? Array.Empty<string>()).SequenceEqual(LocalFetchers)
                        && (t.ImageFetchers ?? Array.Empty<string>()).Length == 0)
                        continue;
                    if (t == null) { t = new TypeOptions { Type = typeName }; types.Add(t); }
                    t.MetadataFetchers = LocalFetchers.ToArray();
                    t.MetadataFetcherOrder = LocalFetchers.ToArray();
                    t.ImageFetchers = Array.Empty<string>();
                    t.ImageFetcherOrder = Array.Empty<string>();
                    fetchers = true;
                }
                bool monitor = options.EnableRealtimeMonitor;
                if (!fetchers && !monitor) return;
                options.TypeOptions = types.ToArray();
                options.EnableRealtimeMonitor = false;
                folder.UpdateLibraryOptions(options);
                var what = new List<string>();
                if (fetchers) what.Add("no longer fetches metadata and images online (the copies take them from their films)");
                if (monitor) what.Add("no longer watched for file changes (HSC refreshes it after writing)");
                log.Info($"    {label} '{libName}': {string.Join("; ", what)}");
            }
            catch (Exception ex) { log.Warn($"{label} '{libName}': its metadata settings could not be updated: {ex.Message}"); }
        }

        private static readonly ImageType[] CopyImageTypes = { ImageType.Primary, ImageType.Backdrop, ImageType.Logo, ImageType.Thumb };

        // Points the copy's poster / backdrop / logo / thumb at the film's own image files, so the
        // collection looks the same as before (and the copy gets the film's real-poster badge).
        // Covers a copy with no images at all and one whose image paths differ from the film's.
        // Only changes copy.ImageInfos (the caller saves); true when they differed.
        internal static bool MatchFilmImages(BaseItem copy, BaseItem film)
        {
            var filmImages = (film.ImageInfos ?? Array.Empty<ItemImageInfo>()).Where(i => CopyImageTypes.Contains(i.Type)).ToList();
            if (filmImages.Count == 0) return false;
            var copyImages = (copy.ImageInfos ?? Array.Empty<ItemImageInfo>()).ToList();
            var copyOwn = copyImages.Where(c => CopyImageTypes.Contains(c.Type)).ToList();
            if (copyOwn.Count == filmImages.Count
                && filmImages.All(f => copyOwn.Any(c => c.Type == f.Type && string.Equals(c.Path, f.Path, StringComparison.OrdinalIgnoreCase))))
                return false;
            copyImages.RemoveAll(c => CopyImageTypes.Contains(c.Type));
            copyImages.AddRange(filmImages.Select(f => new ItemImageInfo { Path = f.Path, Type = f.Type, DateModified = f.DateModified, Width = f.Width, Height = f.Height }));
            copy.ImageInfos = copyImages.ToArray();
            return true;
        }

        private static bool WriteIfChanged(string path, string content)
        {
            try
            {
                if (File.Exists(path) && File.ReadAllText(path) == content) return false;
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            File.WriteAllText(path, content);
            return true;
        }

        // Like the top-list nfo (ids make Emby identify the copy for sure), plus title and year, and
        // only SortName locked. No rank: Emby ignores sorttitle for a .strm, the rank is set as
        // SortName (PrepareCopy), and a rank-free nfo never changes when the list is reordered.
        private static string BuildNfo(BaseItem item)
        {
            string Esc(string? s) => System.Security.SecurityElement.Escape(s ?? "") ?? "";
            var sb = new System.Text.StringBuilder();
            sb.Append("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<movie>\n");
            sb.Append("  <title>").Append(Esc(item.Name)).Append("</title>\n");
            if (item.ProductionYear.HasValue && item.ProductionYear > 0)
                sb.Append("  <year>").Append(item.ProductionYear.Value).Append("</year>\n");
            var imdb = item.GetProviderId("Imdb");
            if (!string.IsNullOrWhiteSpace(imdb)) sb.Append("  <imdbid>").Append(Esc(imdb)).Append("</imdbid>\n");
            var tmdb = item.GetProviderId("Tmdb");
            if (!string.IsNullOrWhiteSpace(tmdb)) sb.Append("  <tmdbid>").Append(Esc(tmdb)).Append("</tmdbid>\n");
            sb.Append("  <lockedfields>SortName</lockedfields>\n</movie>");
            return sb.ToString();
        }

        private class PolicyState { public bool EnableAll; public string[] Folders = Array.Empty<string>(); }

        // Creates the library like a top-list library (movies, nfo only). Emby may switch every
        // user to "all libraries" when a library is added: each user's access is put back as it was.
        private static VirtualFolderInfo? CreateLibrary(string name, string folderPath, List<BaseItem> movies,
            ILibraryManager libraryManager, IUserManager userManager, RunLog log)
        {
            var options = new LibraryOptions
            {
                PathInfos = new[] { new MediaPathInfo { Path = folderPath } },
                EnableRealtimeMonitor = false,
                TypeOptions = new[]
                {
                    new TypeOptions
                    {
                        Type = "Movie",
                        MetadataFetchers = LocalFetchers.ToArray(),
                        MetadataFetcherOrder = LocalFetchers.ToArray(),
                        ImageFetchers = Array.Empty<string>(),
                        ImageFetcherOrder = Array.Empty<string>()
                    }
                }
            };
            return CreateLibrary(name, folderPath, "movies", options, "Ranked library", libraryManager, userManager, log);
        }

        // Also used for top-lists made by a source (SourceTopLists): label names it in the log.
        internal static VirtualFolderInfo? CreateLibrary(string name, string folderPath, string collectionType, LibraryOptions options,
            string label, ILibraryManager libraryManager, IUserManager userManager, RunLog log)
        {
            dynamic mgr = userManager;
            var users = userManager.GetUserList(new UserQuery { IsDisabled = false }).ToList();
            var before = new Dictionary<long, PolicyState>();
            foreach (var u in users)
            {
                try
                {
                    dynamic p = TopListSyncTask.GetPolicy(mgr, u, u.InternalId);
                    if (p == null) continue;
                    before[u.InternalId] = new PolicyState { EnableAll = (bool)p.EnableAllFolders, Folders = ((string[])p.EnabledFolders) ?? Array.Empty<string>() };
                }
                catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            }

            try
            {
                // The content type goes in the options (as Emby's own Add Library does): the
                // (name, string, options, bool) overload takes a sort name, not the type.
                options.ContentType = collectionType;
                libraryManager.AddVirtualFolder(name, options, false);
            }
            catch (Exception ex)
            {
                log.Error($"{label} '{name}' could not be created: {ex.Message}");
                return null;
            }

            // Put back any user whose access changed.
            foreach (var u in users)
            {
                if (!before.TryGetValue(u.InternalId, out var was) || was.EnableAll) continue;
                try
                {
                    dynamic p = TopListSyncTask.GetPolicy(mgr, u, u.InternalId);
                    if (p == null) continue;
                    bool nowAll = (bool)p.EnableAllFolders;
                    var nowFolders = ((string[])p.EnabledFolders) ?? Array.Empty<string>();
                    if (!nowAll && nowFolders.SequenceEqual(was.Folders)) continue;
                    p.EnableAllFolders = false;
                    p.EnabledFolders = was.Folders;
                    TopListSyncTask.UpdatePolicy(mgr, u, u.InternalId, p);
                    log.Debug($"  Library access of '{u.Name}' restored after creating '{name}'");
                }
                catch (Exception ex) { log.Warn($"Library access of '{u.Name}' could not be restored: {ex.Message}"); }
            }

            for (int i = 0; i < 10; i++)
            {
                var lib = LibraryFor(libraryManager, folderPath);
                if (lib != null && !string.IsNullOrEmpty(lib.ItemId)) { log.Ok($"{label} '{name}' created"); return lib; }
                Thread.Sleep(1000);
            }
            log.Error($"{label} '{name}' was created but Emby does not list it");
            return null;
        }

        // A user limited to some libraries gets the ranked library when they can see one of the
        // libraries the films come from, otherwise the collection would look empty to them.
        private static void GrantRestrictedUsers(VirtualFolderInfo lib, List<BaseItem> movies, ILibraryManager libraryManager,
            IUserManager userManager, RunLog log)
        {
            var libGuid = (lib.Guid ?? "").Replace("-", "").ToLowerInvariant();
            if (string.IsNullOrEmpty(libGuid)) return;
            var sourceLibs = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var m in movies)
            {
                try
                {
                    foreach (var f in libraryManager.GetCollectionFolders(m))
                        sourceLibs.Add(f.Id.ToString("N"));
                }
                catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            }
            if (sourceLibs.Count == 0) return;

            dynamic mgr = userManager;
            foreach (var u in userManager.GetUserList(new UserQuery { IsDisabled = false }))
            {
                try
                {
                    dynamic p = TopListSyncTask.GetPolicy(mgr, u, u.InternalId);
                    if (p == null || (bool)p!.EnableAllFolders) continue;
                    var folders = ((string[])p!.EnabledFolders) ?? Array.Empty<string>();
                    var norm = folders.Select(f => f.Replace("-", "").ToLowerInvariant()).ToList();
                    if (norm.Contains(libGuid)) continue;
                    if (!norm.Any(sourceLibs.Contains)) continue;
                    p.EnabledFolders = folders.Concat(new[] { libGuid }).ToArray();
                    TopListSyncTask.UpdatePolicy(mgr, u, u.InternalId, p);
                    log.Debug($"  '{u.Name}' given access to '{lib.Name}'");
                }
                catch (Exception ex) { log.Warn($"Access to '{lib.Name}' for '{u.Name}' failed: {ex.Message}"); }
            }
        }
    }
}
