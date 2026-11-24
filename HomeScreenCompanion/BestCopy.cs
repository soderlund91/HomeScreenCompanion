using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Entities.Movies;
using MediaBrowser.Controller.Library;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace HomeScreenCompanion
{
    /// <summary>
    /// "Prioritize high quality" (PrioritizeHighQuality, Settings > Advanced Settings, off by
    /// default, owner 2026-11-24). A film can be in the library more than once: versions Emby merged,
    /// or separate items in different libraries (a 4K and a 1080p library). They are one film when
    /// they share the IMDb id (else Emby's PresentationUniqueKey, HomeScreenCompanionTask.VersionKey).
    /// Tags go on every copy. The places that hold one copy per film (a collection, a ranked
    /// collection, a playlist, a movie top-list) take the lowest id or the first one listed; with the
    /// setting on they take the best copy: the higher resolution, then Dolby Vision over HDR over
    /// SDR, then the higher video bitrate (file size when a copy has none), then the lowest id.
    /// Movies only: shows and episodes keep the old choice.
    ///
    /// Known limits (the warning under the setting): a user who cannot open the library with the
    /// best copy does not see the film in a collection or playlist, and a top-list copy plays the
    /// best file for everyone who can open the top-list.
    /// </summary>
    internal static class BestCopy
    {
        // A run uses the value from when it started (Begin, under RunGate), like GentleMode; outside a
        // run (the page's endpoints, the background parts) the current setting.
        private static volatile bool _runEnabled;
        internal static bool Enabled => RunGate.Busy ? _runEnabled : Plugin.Instance?.Configuration?.PrioritizeHighQuality == true;

        // Quality per item for one run (each copy's streams are read once); null outside a run.
        private static ConcurrentDictionary<long, Quality>? _runCache;

        /// <summary>A run starts (RunGate).</summary>
        internal static void Begin()
        {
            _runEnabled = Plugin.Instance?.Configuration?.PrioritizeHighQuality == true;
            _runCache = new ConcurrentDictionary<long, Quality>();
        }

        /// <summary>A run ends (RunGate): the cache is let go.</summary>
        internal static void End() => _runCache = null;

        internal struct Quality
        {
            public int Resolution;   // HomeScreenCompanionTask.ResolutionRank: 0 unknown … 7 = 8K
            public int Range;        // 2 Dolby Vision, 1 HDR, 0 SDR
            public int VideoKbps;    // 0 = not known
            public double SizeMb;    // 0 = not known
        }

        private static Quality QualityOf(BaseItem item)
        {
            var cache = _runCache;
            if (cache != null && cache.TryGetValue(item.InternalId, out var q)) return q;
            q = HomeScreenCompanionTask.QualityOf(item);
            cache?.TryAdd(item.InternalId, q);
            return q;
        }

        // Better first: > 0 when a is the better copy.
        private static int Compare(BaseItem a, BaseItem b)
        {
            var qa = QualityOf(a);
            var qb = QualityOf(b);
            if (qa.Resolution != qb.Resolution) return qa.Resolution.CompareTo(qb.Resolution);
            if (qa.Range != qb.Range) return qa.Range.CompareTo(qb.Range);
            if (qa.VideoKbps > 0 && qb.VideoKbps > 0 && qa.VideoKbps != qb.VideoKbps) return qa.VideoKbps.CompareTo(qb.VideoKbps);
            if (qa.SizeMb > 0 && qb.SizeMb > 0 && Math.Abs(qa.SizeMb - qb.SizeMb) >= 1) return qa.SizeMb.CompareTo(qb.SizeMb);
            return b.InternalId.CompareTo(a.InternalId);   // the lowest id wins a tie
        }

        /// <summary>
        /// The copy a film is held as, from its copies (one film, at least one item): with the
        /// setting on and only movies, the best; otherwise the lowest id.
        /// </summary>
        internal static BaseItem Pick(IList<BaseItem> copies, bool best)
        {
            if (copies.Count == 1) return copies[0];
            if (!best || !copies.All(c => c is Movie)) return copies.OrderBy(c => c.InternalId).First();
            var pick = copies[0];
            for (int i = 1; i < copies.Count; i++)
                if (Compare(copies[i], pick) > 0) pick = copies[i];
            return pick;
        }

        /// <summary>
        /// Movie top-lists: each film's copies (in the order given) are replaced by its best copy at
        /// the place of the first one; anything else stays where it is. Only with the setting on
        /// (callers check Enabled); without it the list keeps its first copy, as before.
        /// </summary>
        internal static List<BaseItem> OnePerFilm(List<BaseItem> items)
        {
            var byKey = new Dictionary<string, List<BaseItem>>(StringComparer.OrdinalIgnoreCase);
            foreach (var item in items)
            {
                if (!(item is Movie)) continue;
                var key = HomeScreenCompanionTask.VersionKey(item);
                if (key == null) continue;
                if (!byKey.TryGetValue(key, out var list)) byKey[key] = list = new List<BaseItem>();
                if (!list.Any(l => l.InternalId == item.InternalId)) list.Add(item);
            }
            var result = new List<BaseItem>();
            var done = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var item in items)
            {
                var key = item is Movie ? HomeScreenCompanionTask.VersionKey(item) : null;
                if (key == null) { result.Add(item); continue; }
                if (!done.Add(key)) continue;
                result.Add(Pick(byKey[key], true));
            }
            return result;
        }

        /// <summary>
        /// Before a copy (top-list or ranked) is merged with its film: when the copy is still a version
        /// of another film that is not one of the target's versions (the film it pointed at before,
        /// e.g. the 1080p item after the setting chose the 4K one), it is given a version key of its
        /// own first. Emby's MergeItems merges every version of every item it is given, so merging the
        /// copy as it is would merge the two films for good. True when the copy was detached (it then
        /// needs merging with the target). Never touches the films.
        /// </summary>
        internal static bool DetachFromOtherFilm(BaseItem copy, BaseItem target, ILibraryManager libraryManager)
        {
            if (!LinkedToOtherFilm(copy, target, libraryManager)) return false;
            try
            {
                // Like Emby's own merge key (a new id + "_": a key ending in "_" is never replaced
                // when Emby reads the file again).
                copy.PresentationUniqueKey = Guid.NewGuid().ToString("N") + "_";
                GentleMode.Timed("copy version split", () => libraryManager.UpdateItem(copy, copy.Parent, MediaBrowser.Controller.Library.ItemUpdateType.MetadataEdit, null));
                PluginLog.Write("INF", "Versions", $"HSC's copy of '{copy.Name}' was unlinked from the copy of the film it used before, so your separate copies stay apart; now linked with {target.Path}");
                return true;
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); return false; }
        }

        /// <summary>The copy is a version of a film other than the target's (see DetachFromOtherFilm), without changing anything.</summary>
        internal static bool LinkedToOtherFilm(BaseItem copy, BaseItem target, ILibraryManager libraryManager)
        {
            if (!(copy is Video cv) || copy.Id == target.Id) return false;
            try
            {
                var targetGroup = new HashSet<long>(target is Video tv ? tv.GetAlternateVersionIds() : new List<long>()) { target.InternalId };
                return cv.GetAlternateVersionIds().Where(id => !targetGroup.Contains(id))
                    .Select(id => libraryManager.GetItemById(id))
                    .Any(i => i != null && TopListCollectionMirror.IsOriginal(i));
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); return false; }
        }
    }
}
