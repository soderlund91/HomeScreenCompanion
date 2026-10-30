using MediaBrowser.Controller.Library;
using MediaBrowser.Model.Entities;
using MediaBrowser.Model.Querying;
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
    /// the copies as duplicates. Not in a row on that library itself, nor in "boxset" and
    /// "collections" rows (excluding the library there empties the ranked collection).
    ///
    /// Runs on every top-list section sync, so new users and new top-lists are covered.
    /// Turning the setting off removes the exclusions again (not the always-on ranked ones).
    /// </summary>
    internal static class TopListLibraryVisibility
    {
        public static int Apply(PluginConfiguration config, IUserManager userManager, ILibraryManager libraryManager, Action<string>? log = null)
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

            bool hide = config.HideTopListLibraries;
            int changedUsers = 0;

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
                    if (myMedia != null || latest != null)
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
                        // Ranked copies are duplicates: out of every other row, whatever "hide" is.
                        if (section.SectionType != "userviews" && section.SectionType != "boxset" && section.SectionType != "collections")
                        {
                            var parent = (section.ParentId ?? "").Trim().Replace("-", "");
                            var ranked = rankedIds.Where(id => !rankedKeys[id].Contains(parent, StringComparer.OrdinalIgnoreCase)).ToList();
                            excluded = Merge(excluded ?? section.ExcludedFolders, ranked, true) ?? excluded;
                        }
                        if (excluded == null) continue;
                        section.ExcludedFolders = excluded;
                        userManager.UpdateHomeSection(user.InternalId, section, CancellationToken.None);
                        changed = true;
                    }

                    if (changed) changedUsers++;
                }
                catch (Exception ex)
                {
                    log?.Invoke($"Hiding top-list libraries for '{user.Name}' failed — {ex.Message}");
                }
            }
            return changedUsers;
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
