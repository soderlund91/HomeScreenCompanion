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
    /// Keeps top-list libraries out of sight (show top-list libraries are kept out of Latest
    /// by TopListSyncTask, always, so their copies never show in Continue Watching or Next Up;
    /// here they only follow the My Media part) (setting "Hide top-list libraries", on by
    /// default): for every user they are excluded from "My Media" (which is also what Emby's
    /// sidebar lists) and from the "Latest" rows, both in the user configuration and in saved
    /// home rows (each row's own excluded folders). Users keep access, so the top-list row
    /// still shows and plays the items.
    ///
    /// The user configuration matches libraries by GUID (UserViewManager compares
    /// MyMediaExcludes/LatestItemsExcludes with BaseItem.IdString); saved rows' ExcludedFolders
    /// use the internal id. Earlier versions wrote internal ids into the user configuration,
    /// which Emby ignored; those are removed here.
    ///
    /// Ranked-collection libraries follow the same setting for My Media and Latest, and are
    /// also put in the excluded folders of every other saved home row, always: Emby filters a
    /// row by its excluded folders, so a row made before the ranked library existed would show
    /// the copies as duplicates. Not in a row on ranked libraries only (OnRankedOnly, or its
    /// parent), nor in the rows KeepsRankedLibraries names. Every place that rebuilds an HSC row
    /// from its stored settings adds them too (AddRankedExclusions), so no sync undoes another.
    ///
    /// Runs on every top-list section sync, so new users and new top-lists are covered.
    /// Turning the setting off removes the exclusions again (not the always-on ranked ones).
    /// </summary>
    internal static class TopListLibraryVisibility
    {
        // Returns the users whose My Media / Latest visibility changed; ranked rows are reported through info.
        // dry: counts the users and rows that would change, without changing them.
        public static int Apply(PluginConfiguration config, IUserManager userManager, ILibraryManager libraryManager, Action<string>? log = null, Action<string>? info = null, bool dry = false)
        {
            var withLibrary = (config.TopLists ?? new List<TopListHomeSection>())
                .Where(t => !string.IsNullOrEmpty(t.HomeSectionLibraryId) && t.HomeSectionLibraryId != "auto")
                .ToList();
            var topIds = withLibrary
                .Where(t => !ShowTopList.IsShows(t))
                .Select(t => t.HomeSectionLibraryId.Trim())
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();
            var rankedIds = RankedCollections.LibraryIds(libraryManager)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();
            var libIds = topIds.Concat(rankedIds) // ranked-collection libraries are hidden the same way
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();
            // Show-list libraries: My Media only — their "Latest" exclusion is always on (TopListSyncTask).
            var showIds = withLibrary
                .Where(ShowTopList.IsShows)
                .Select(t => t.HomeSectionLibraryId.Trim())
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();
            if (libIds.Count == 0 && showIds.Count == 0) return 0;

            var libGuids = Guids(libIds, libraryManager);
            var showGuids = Guids(showIds, libraryManager);
            var mediaIds = libIds.Concat(showIds).ToList();
            var mediaGuids = libGuids.Concat(showGuids).ToList();
            // A row on a ranked library may name it by internal id or GUID.
            var rankedKeys = rankedIds.ToDictionary(id => id, id => new[] { id }.Concat(Guids(new List<string> { id }, libraryManager)).ToList(), StringComparer.OrdinalIgnoreCase);
            var allLibIds = AllLibraryIds(libraryManager);

            bool hide = config.HideTopListLibraries;
            int changedUsers = 0, rankedRows = 0;

            foreach (var user in userManager.GetUserList(new UserQuery()))
            {
                try
                {
                    bool changed = false;

                    var cfg = userManager.GetUserConfiguration(user);
                    // GUIDs in when hiding; the old internal ids always out.
                    var myMedia = Merge(Merge(cfg.MyMediaExcludes, mediaIds, false) ?? cfg.MyMediaExcludes, mediaGuids, hide)
                                  ?? Merge(cfg.MyMediaExcludes, mediaIds, false);
                    var latest = Merge(Merge(cfg.LatestItemsExcludes, libIds, false) ?? cfg.LatestItemsExcludes, libGuids, hide)
                                 ?? Merge(cfg.LatestItemsExcludes, libIds, false);
                    if ((myMedia != null || latest != null) && dry) changed = true;
                    else if (myMedia != null || latest != null)
                    {
                        if (myMedia != null) cfg.MyMediaExcludes = myMedia;
                        if (latest != null) cfg.LatestItemsExcludes = latest;
                        userManager.UpdateConfiguration(user, cfg);
                        changed = true;
                    }

                    // Saved home rows ignore the user configuration and carry their own exclusions.
                    var sections = userManager.GetHomeSections(user.InternalId, CancellationToken.None)?.Sections
                                   ?? Array.Empty<ContentSection>();
                    foreach (var section in sections)
                    {
                        string[]? excluded = null;
                        if (section.SectionType == "latestmediablock" || section.SectionType == "userviews")
                            excluded = Merge(section.ExcludedFolders, section.SectionType == "userviews" ? mediaIds : topIds, hide);
                        bool visibilityChanged = excluded != null;
                        // Ranked copies are duplicates: out of every other row, whatever "hide" is.
                        if (!KeepsRankedLibraries(section.SectionType) && !OnRankedOnly(excluded ?? section.ExcludedFolders, rankedIds, allLibIds))
                        {
                            var parent = (section.ParentId ?? "").Trim().Replace("-", "");
                            var ranked = rankedIds.Where(id => !rankedKeys[id].Contains(parent, StringComparer.OrdinalIgnoreCase)).ToList();
                            var withRanked = Merge(excluded ?? section.ExcludedFolders, ranked, true);
                            if (withRanked != null) { excluded = withRanked; rankedRows++; }
                        }
                        if (excluded == null) continue;
                        if (visibilityChanged) changed = true;
                        if (dry) continue;
                        section.ExcludedFolders = excluded;
                        userManager.UpdateHomeSection(user.InternalId, section, CancellationToken.None);
                    }

                    if (changed) changedUsers++;
                }
                catch (Exception ex)
                {
                    log?.Invoke($"Hiding top-list libraries for '{user.Name}' failed — {ex.Message}");
                }
            }
            if (rankedRows > 0)
                info?.Invoke(dry ? $"[Dry run] Ranked-collection libraries would be kept out of {RunLog.Plural(rankedRows, "home row")}"
                                 : $"Ranked-collection libraries kept out of {RunLog.Plural(rankedRows, "home row")}");
            return changedUsers;
        }

        // Rows the ranked-collection libraries are never added to: My Media (follows the setting),
        // collection rows (they show the ranked copies) and playlist rows (HSC clears their
        // excluded folders on every Home Screen Sync; a playlist holds the real films).
        internal static bool KeepsRankedLibraries(string? sectionType) =>
            sectionType == "userviews" || sectionType == "boxset" || sectionType == "collections" || sectionType == "playlist";

        // A row on ranked libraries only: every other library is in its excluded folders. Emby
        // ignores ParentId on an items row, so this is how a row "on one library" is stored; that
        // row is meant to show the copies and is left alone.
        internal static bool OnRankedOnly(IEnumerable<string>? excluded, List<string> rankedIds, List<string> allLibIds)
        {
            var ex = (excluded ?? Array.Empty<string>()).ToList();
            var included = allLibIds.Where(id => !ex.Contains(id, StringComparer.OrdinalIgnoreCase)).ToList();
            return included.Count > 0 && included.All(id => rankedIds.Contains(id, StringComparer.OrdinalIgnoreCase));
        }

        // The libraries that decide OnRankedOnly: not the top-list libraries — HSC excludes those from
        // every other row anyway, so a new top-list must not turn a row on a ranked library into one
        // that gets it excluded.
        internal static List<string> AllLibraryIds(ILibraryManager libraryManager)
        {
            var topLists = (Plugin.Instance?.Configuration?.TopLists ?? new List<TopListHomeSection>())
                .Select(t => (t.HomeSectionLibraryId ?? "").Trim()).Where(id => id.Length > 0)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);
            return libraryManager.GetVirtualFolders()
                .Where(f => !string.IsNullOrEmpty(f.ItemId) && !topLists.Contains(f.ItemId))
                .Select(f => f.ItemId).ToList();
        }

        // A copy of a row's stored settings with the ranked-collection libraries added (the stored
        // settings themselves are never changed, so a removed ranked library leaves no trace there).
        internal static Dictionary<string, string> WithRankedExclusions(Dictionary<string, string> settings, string? ownLibraryId,
            ILibraryManager libraryManager, IJsonSerializer json)
        {
            var copy = new Dictionary<string, string>(settings, StringComparer.OrdinalIgnoreCase);
            var rankedIds = RankedCollections.LibraryIds(libraryManager);
            if (rankedIds.Count > 0) AddRankedExclusions(copy, ownLibraryId, rankedIds, AllLibraryIds(libraryManager), json);
            return copy;
        }

        // For an HSC row rebuilt from its stored settings: stored exclusions replace the row's own
        // excluded folders (BuildContentSection), so the ranked-collection libraries go in here too,
        // else every rebuild would take out what Apply put in. Not without stored exclusions (the
        // row then keeps its own), nor in rows Apply leaves alone.
        internal static void AddRankedExclusions(Dictionary<string, string> settings, string? ownLibraryId,
            List<string> rankedIds, List<string> allLibIds, IJsonSerializer json)
        {
            settings.TryGetValue("SectionType", out var sectionType);
            if (rankedIds.Count == 0 || KeepsRankedLibraries(sectionType)) return;
            if (!settings.ContainsKey("ExcludedFolders") && !settings.ContainsKey("_queryExcludeViewIds")) return;
            var storedStr = settings.TryGetValue("ExcludedFolders", out var ef) && !string.IsNullOrWhiteSpace(ef) ? ef
                          : settings.TryGetValue("_queryExcludeViewIds", out var qv) ? qv ?? "" : "";
            string[] storedArr;
            try
            {
                storedArr = storedStr.TrimStart().StartsWith("[")
                    ? (json.DeserializeFromString<string[]>(storedStr) ?? Array.Empty<string>())
                    : storedStr.Split(',');
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); return; } // unreadable: left as it is
            var stored = storedArr.Select(s => s.Trim()).Where(s => s.Length > 0).ToList();
            if (OnRankedOnly(stored, rankedIds, allLibIds)) return;
            var missing = rankedIds.Where(id => !stored.Contains(id, StringComparer.OrdinalIgnoreCase)
                                             && id != (ownLibraryId ?? "").Trim()).ToList(); // a row on that library itself
            if (missing.Count == 0) return;
            var excStr = string.Join(",", stored.Concat(missing));
            settings["_queryExcludeViewIds"] = excStr;
            settings["ExcludedFolders"] = excStr;
        }

        // A removed ranked library: its id and GUID leave every user's exclusions, saved home rows
        // and (users limited to some libraries) library access.
        public static void Forget(string libId, string libGuid, IUserManager userManager, Action<string>? log = null)
        {
            var ids = new List<string> { libId, libGuid }.Where(x => !string.IsNullOrEmpty(x)).ToList();
            if (ids.Count == 0) return;
            dynamic mgr = userManager;
            foreach (var user in userManager.GetUserList(new UserQuery()))
            {
                try
                {
                    var cfg = userManager.GetUserConfiguration(user);
                    var myMedia = Merge(cfg.MyMediaExcludes, ids, false);
                    var latest = Merge(cfg.LatestItemsExcludes, ids, false);
                    if (myMedia != null || latest != null)
                    {
                        if (myMedia != null) cfg.MyMediaExcludes = myMedia;
                        if (latest != null) cfg.LatestItemsExcludes = latest;
                        userManager.UpdateConfiguration(user, cfg);
                    }
                    // Every row type: ranked libraries are excluded from all of them (Apply).
                    foreach (var section in userManager.GetHomeSections(user.InternalId, CancellationToken.None)?.Sections ?? Array.Empty<ContentSection>())
                    {
                        var excluded = Merge(section.ExcludedFolders, ids, false);
                        if (excluded == null) continue;
                        section.ExcludedFolders = excluded;
                        userManager.UpdateHomeSection(user.InternalId, section, CancellationToken.None);
                    }
                    dynamic policy = TopListSyncTask.GetPolicy(mgr, user, user.InternalId);
                    if (policy != null && !(bool)policy!.EnableAllFolders)
                    {
                        var folders = (string[])policy!.EnabledFolders ?? Array.Empty<string>();
                        var kept = folders.Where(f => !ids.Contains(f.Replace("-", ""), StringComparer.OrdinalIgnoreCase)).ToArray();
                        if (kept.Length != folders.Length)
                        {
                            policy.EnabledFolders = kept;
                            TopListSyncTask.UpdatePolicy(mgr, user, user.InternalId, policy);
                        }
                    }
                }
                catch (Exception ex) { log?.Invoke($"Removing library '{libId}' from '{user.Name}' failed — {ex.Message}"); }
            }
        }

        private static List<string> Guids(List<string> libIds, ILibraryManager libraryManager) => libIds
            .Select(id => long.TryParse(id, out var n) ? libraryManager.GetItemById(n)?.Id.ToString("N") : null)
            .Where(g => !string.IsNullOrEmpty(g)).Select(g => g!)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        // The new list, or null when nothing changes.
        private static string[]? Merge(string[]? current, List<string> libIds, bool hide)
        {
            var list = (current ?? Array.Empty<string>()).ToList();
            bool changed = false;
            foreach (var id in libIds)
            {
                bool present = list.Contains(id, StringComparer.OrdinalIgnoreCase);
                if (hide && !present) { list.Add(id); changed = true; }
                if (!hide && present) { list.RemoveAll(x => string.Equals(x, id, StringComparison.OrdinalIgnoreCase)); changed = true; }
            }
            return changed ? list.ToArray() : null;
        }
    }
}
