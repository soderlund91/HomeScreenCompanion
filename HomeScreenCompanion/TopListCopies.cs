using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Library;
using MediaBrowser.Controller.Providers;
using MediaBrowser.Model.Entities;
using MediaBrowser.Model.IO;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

namespace HomeScreenCompanion
{
    /// <summary>
    /// Movie top-list copies (.strm in &lt;plugin data&gt;/toplists/&lt;list&gt;/) are made like ranked copies:
    /// their library reads the nfo only (no online metadata or images), and each copy takes its
    /// film's text, runtime, media streams and cast (in steps while someone is watching) and is
    /// merged with it as a version. A copy is only probed when its film has no media info itself.
    /// Every write is timed for gentle mode and the run summary ("top-list …").
    ///
    /// A sync that rewrites a list's folder owns it (<see cref="Begin"/>/<see cref="End"/>) and
    /// prepares its copies itself, after one explicit refresh of the library; the real-time
    /// handler (ServerEntryPoint) leaves copies in an owned folder alone and prepares the others
    /// (a list made or saved on the Top-Lists page) one at a time (<see cref="Gate"/>).
    /// </summary>
    internal static class TopListCopies
    {
        private static readonly object Sync = new object();
        private static readonly Dictionary<string, int> Busy = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        /// <summary>One copy prepared at a time by the real-time handler (each may pause for gentle mode).</summary>
        internal static readonly SemaphoreSlim Gate = new SemaphoreSlim(1, 1);

        private static string Key(string folderPath) => folderPath.TrimEnd(Path.DirectorySeparatorChar) + Path.DirectorySeparatorChar;

        internal static void Begin(string folderPath)
        {
            lock (Sync) { Busy.TryGetValue(Key(folderPath), out var n); Busy[Key(folderPath)] = n + 1; }
        }

        internal static void End(string folderPath)
        {
            lock (Sync)
            {
                if (!Busy.TryGetValue(Key(folderPath), out var n)) return;
                if (n <= 1) Busy.Remove(Key(folderPath)); else Busy[Key(folderPath)] = n - 1;
            }
        }

        // The rank each copy was last given (by path), for the real-time handler: another plugin
        // that probes new .strm items (StrmCompanion) can save a copy it read before the sync set
        // the rank, which puts the title back as its SortName. Lost on restart; the next sync sets
        // every rank again.
        private static readonly Dictionary<string, string> Ranks = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        internal static string? RankOf(string? path)
        {
            if (string.IsNullOrEmpty(path)) return null;
            lock (Sync) return Ranks.TryGetValue(path!, out var r) ? r : null;
        }

        /// <summary>The path is in a folder a sync is working on.</summary>
        internal static bool IsBusy(string? path)
        {
            if (string.IsNullOrEmpty(path)) return false;
            lock (Sync) return Busy.Keys.Any(k => path!.StartsWith(k, StringComparison.OrdinalIgnoreCase));
        }

        private static readonly ImageType[] FilmImageTypes = { ImageType.Backdrop, ImageType.Logo };

        /// <summary>
        /// Brings one copy in line with its film. rank: its SortName (null: the rank a sync last gave
        /// it this session, else left as it is); imagesRedrawn:
        /// its badged images were written again (their date must change, or Emby serves the old ones);
        /// mergeWith: the film version it is merged with (default the film). Returns how many writes it
        /// made (for GentleMode); 0 when the copy was already right (reads only).
        /// </summary>
        internal static int Prepare(BaseItem copy, BaseItem film, BaseItem? mergeWith, string? rank, bool imagesRedrawn,
            ILibraryManager libraryManager, IProviderManager providerManager, IFileSystem fileSystem,
            Action<string> warn, Action<string>? debug, CancellationToken cancellationToken)
        {
            int writes = 0;
            bool changed = false;
            var folder = Path.GetDirectoryName(copy.Path) ?? "";
            if (rank != null) lock (Sync) Ranks[copy.Path ?? ""] = rank;
            else rank = RankOf(copy.Path);
            if (rank != null && !string.Equals(copy.SortName, rank, StringComparison.Ordinal))
            {
                copy.SortName = rank;
                changed = true;
            }
            // Poster and thumb: the list's badged images next to the .strm.
            if ((imagesRedrawn || !HomeScreenCompanionService.RankedImagesApplied(copy, folder))
                && HomeScreenCompanionService.ApplyRankedImages(copy, folder)) changed = true;
            // Backdrop and logo: the film's own, when the copy has none (its library fetches none).
            if (AddFilmImages(copy, film)) changed = true;
            // Text, runtime and video info: the film's (the library reads only the nfo, and Emby
            // never probes a .strm during a scan; a copy without a runtime is marked played when
            // stopped instead of getting a resume point).
            changed |= ShowTopList.SyncMetadata(copy, film, isSeries: true);
            changed |= ShowTopList.SyncVideoInfo(copy, film);
            if (changed)
            {
                try { GentleMode.Timed("top-list copy save", () => libraryManager.UpdateItem(copy, copy.Parent, ItemUpdateType.MetadataEdit, null)); writes++; }
                catch (Exception ex) { warn($"Top-list copy '{copy.Name}' could not be updated: {ex.Message}"); }
            }
            try
            {
                if (GentleMode.TimedIfWrote("top-list media streams", () => ShowTopList.CopyMediaStreams(copy, film))) writes++;
            }
            catch (Exception ex) { warn($"Top-list copy '{copy.Name}': media info could not be copied: {ex.Message}"); }
            try
            {
                if (RankedCollections.CopyPeople(copy, film, libraryManager, cancellationToken, "top-list cast", debug)) writes++;
            }
            catch (OperationCanceledException) { throw; }
            catch (Exception ex) { debug?.Invoke($"  Top-list copy '{copy.Name}': cast could not be copied: {ex.Message}"); }

            // Merged once as a version of the film (one entry in search, shared watched state).
            var target = mergeWith ?? film;
            try
            {
                if (copy.Id != target.Id && !IsMerged(copy, target))
                {
                    GentleMode.Timed("top-list merge", () => libraryManager.MergeItems(new[] { target, copy }));
                    writes += 2;
                }
            }
            catch (Exception ex) { warn($"Top-list copy '{copy.Name}' could not be merged with '{target.Name}': {ex.Message}"); }

            // Its film has no media info to copy (never probed): Emby probes the copy, at low priority.
            if ((copy.RunTimeTicks ?? 0) <= 0 && MergeTopListVersionsTask.QueueStrmProbe(providerManager, fileSystem, copy)) writes++;
            return writes;
        }

        internal static bool IsMerged(BaseItem copy, BaseItem film) =>
            (film is Video fv && fv.GetAlternateVersionIds().Contains(copy.InternalId))
            || (copy is Video cv && cv.GetAlternateVersionIds().Contains(film.InternalId));

        private static bool AddFilmImages(BaseItem copy, BaseItem film)
        {
            var copyImages = (copy.ImageInfos ?? Array.Empty<ItemImageInfo>()).ToList();
            bool changed = false;
            foreach (var type in FilmImageTypes)
            {
                if (copyImages.Any(i => i.Type == type)) continue;
                var theirs = (film.ImageInfos ?? Array.Empty<ItemImageInfo>()).Where(i => i.Type == type).ToList();
                if (theirs.Count == 0) continue;
                copyImages.AddRange(theirs.Select(f => new ItemImageInfo { Path = f.Path, Type = f.Type, DateModified = f.DateModified, Width = f.Width, Height = f.Height }));
                changed = true;
            }
            if (changed) copy.ImageInfos = copyImages.ToArray();
            return changed;
        }

        /// <summary>The film a copy's .strm points at (its first line), when it is in the library.</summary>
        internal static BaseItem? FilmOf(BaseItem copy, ILibraryManager libraryManager)
        {
            try
            {
                if (string.IsNullOrEmpty(copy.Path) || !File.Exists(copy.Path)) return null;
                var target = File.ReadAllText(copy.Path).Trim();
                return target.Length == 0 ? null : libraryManager.FindByPath(target, false);
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); return null; }
        }
    }
}
