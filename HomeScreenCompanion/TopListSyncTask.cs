using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Library;
using MediaBrowser.Model.Entities;
using MediaBrowser.Model.Serialization;
using MediaBrowser.Model.Tasks;
using MediaBrowser.Model.Users;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace HomeScreenCompanion
{
    public class TopListSyncTask : IScheduledTask
    {
        private readonly ILibraryManager _libraryManager;
        private readonly IUserViewManager _userViewManager;
        private readonly IUserManager _userManager;
        private readonly IJsonSerializer _jsonSerializer;

        public TopListSyncTask(ILibraryManager libraryManager, IUserViewManager userViewManager, IUserManager userManager, IJsonSerializer jsonSerializer)
        {
            _libraryManager = libraryManager;
            _userViewManager = userViewManager;
            _userManager = userManager;
            _jsonSerializer = jsonSerializer;
        }

        public string Key => "TopListSyncTask";
        public string Name => "Top-list section sync";
        public string Description => "Ensures each top-list home section only shows items from its own library, and that regular home sections never include top-list libraries.";
        public string Category => "Home Screen Companion";

        public IEnumerable<TaskTriggerInfo> GetDefaultTriggers() => Array.Empty<TaskTriggerInfo>();

        public Task Execute(CancellationToken cancellationToken, IProgress<double> progress)
        {
            SyncAll(_libraryManager, _userViewManager, _userManager, _jsonSerializer, cancellationToken);
            return Task.CompletedTask;
        }

        internal static (int updated, string message) SyncAll(
            ILibraryManager libraryManager,
            IUserViewManager userViewManager,
            IUserManager userManager,
            IJsonSerializer jsonSerializer,
            CancellationToken cancellationToken)
        {
            var config = Plugin.Instance?.Configuration;
            if (config == null) return (0, "No config.");

            var topLists = config.TopLists ?? new List<TopListHomeSection>();
            if (topLists.Count == 0) return (0, "No top-lists configured.");

            int totalUpdated = 0;

            foreach (var tl in topLists)
            {
                if (string.IsNullOrEmpty(tl.HomeSectionLibraryId) || tl.HomeSectionLibraryId == "auto") continue;

                var ownId = tl.HomeSectionLibraryId.Trim().ToLowerInvariant();
                var safeTag = new string((tl.TagName ?? "").Select(c => char.IsLetterOrDigit(c) ? c : '_').ToArray());
                var sectionMarker = "hsc__tl__" + safeTag;

                foreach (var tracking in tl.HomeSectionTracked ?? new List<HomeSectionTracking>())
                {
                    if (string.IsNullOrEmpty(tracking.UserId) || string.IsNullOrEmpty(tracking.SectionId)) continue;
                    try
                    {
                        Guid.TryParse(tracking.UserId, out var userGuid);
                        var uid = userManager.GetInternalId(tracking.UserId);
                        var sections = userManager.GetHomeSections(uid, cancellationToken)?.Sections
                            ?? Array.Empty<ContentSection>();

                        var owned = sections.FirstOrDefault(s => s.Id == tracking.SectionId)
                            ?? sections.FirstOrDefault(s => s.Subtitle == sectionMarker);
                        if (owned == null) continue;

                        // Get ALL views for this user (includes Live TV) via IUserViewManager.
                        // Reflect through the interface type to handle explicit interface implementations.
                        var allViewIds = new List<string>();
                        try
                        {
                            var ifMethod = typeof(IUserViewManager).GetMethod("GetUserViews");
                            if (ifMethod != null)
                            {
                                var queryParams = ifMethod.GetParameters();
                                object queryArg = null;
                                if (queryParams.Length > 0)
                                {
                                    try
                                    {
                                        queryArg = Activator.CreateInstance(queryParams[0].ParameterType);
                                        var uidProp = queryParams[0].ParameterType.GetProperty("UserId");
                                        if (uidProp?.PropertyType == typeof(long))
                                            uidProp.SetValue(queryArg, uid);
                                        else
                                            uidProp?.SetValue(queryArg, userGuid);
                                    }
                                    catch { queryArg = null; }
                                }
                                var result = ifMethod.Invoke(userViewManager, new[] { queryArg });
                                if (result is System.Collections.IEnumerable views)
                                    foreach (var v in views)
                                    {
                                        var idProp = v?.GetType().GetProperty("Id");
                                        if (idProp?.GetValue(v) is Guid vid && vid != Guid.Empty)
                                            allViewIds.Add(vid.ToString("N").ToLowerInvariant());
                                    }
                            }
                        }
                        catch { }

                        // Always also include virtual folder IDs (ensures regular libraries are covered)
                        foreach (var f in libraryManager.GetVirtualFolders())
                            if (!string.IsNullOrEmpty(f.ItemId))
                                allViewIds.Add(f.ItemId.Trim().ToLowerInvariant());

                        allViewIds = allViewIds.Distinct().ToList();

                        // Final fallback: only virtual folders if nothing else worked
                        if (allViewIds.Count == 0)
                        {
                            allViewIds = libraryManager.GetVirtualFolders()
                                .Where(f => !string.IsNullOrEmpty(f.ItemId))
                                .Select(f => f.ItemId.Trim().ToLowerInvariant())
                                .ToList();
                        }

                        var settingsDict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                        try
                        {
                            if (!string.IsNullOrEmpty(tl.HomeSectionSettings) && tl.HomeSectionSettings != "{}")
                                settingsDict = jsonSerializer.DeserializeFromString<Dictionary<string, string>>(tl.HomeSectionSettings) ?? settingsDict;
                        }
                        catch { }

                        // Merge: union of newly discovered IDs and any previously stored exclusions
                        // (preserves IDs like Live TV that are only captured at initial setup time).
                        var storedExclude = (settingsDict.TryGetValue("_queryExcludeViewIds", out var storedEv) ? storedEv : "")
                            .Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries)
                            .Select(s => s.Trim().ToLowerInvariant()).Where(s => s.Length > 0);
                        var mergedExcludeIds = allViewIds.Where(id => id != ownId)
                            .Concat(storedExclude.Where(id => id != ownId))
                            .Distinct().ToList();
                        var excludeStr = string.Join(",", mergedExcludeIds);

                        settingsDict["_queryExcludeViewIds"] = excludeStr;
                        settingsDict["ExcludedFolders"] = excludeStr;
                        tl.HomeSectionSettings = jsonSerializer.SerializeToString(settingsDict);

                        var updated = HomeScreenCompanionTask.BuildContentSection(
                            jsonSerializer, settingsDict, tl.HomeSectionLibraryId, owned);
                        typeof(ContentSection).GetProperty("Id")?.SetValue(updated, owned.Id);
                        userManager.UpdateHomeSection(uid, updated, cancellationToken);
                        totalUpdated++;
                    }
                    catch { }
                }
            }

            // Ensure ALL top-list libraries are excluded from every TAG items-type section.
            // This makes the task a complete maintenance sweep, not just a top-list-section refresh.
            var allTopListLibIds = topLists
                .Where(t => !string.IsNullOrEmpty(t.HomeSectionLibraryId) && t.HomeSectionLibraryId != "auto")
                .Select(t => t.HomeSectionLibraryId.Trim().ToLowerInvariant())
                .Distinct().ToList();

            if (allTopListLibIds.Count > 0)
            {
                foreach (var tc in config.Tags ?? new List<TagConfig>())
                {
                    if (!tc.EnableHomeSection) continue;

                    var tcSettings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                    try
                    {
                        if (!string.IsNullOrEmpty(tc.HomeSectionSettings) && tc.HomeSectionSettings != "{}")
                            tcSettings = jsonSerializer.DeserializeFromString<Dictionary<string, string>>(tc.HomeSectionSettings) ?? tcSettings;
                    }
                    catch { }

                    tcSettings.TryGetValue("SectionType", out var tcSt);
                    if (tcSt == "boxset") continue;

                    var existingExcluded = (tcSettings.TryGetValue("_queryExcludeViewIds", out var tcEv) ? tcEv : "")
                        .Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries)
                        .Select(s => s.Trim().ToLowerInvariant()).Where(s => s.Length > 0).ToList();

                    var missing = allTopListLibIds
                        .Where(id => !existingExcluded.Contains(id, StringComparer.OrdinalIgnoreCase))
                        .ToList();
                    if (missing.Count == 0) continue;

                    existingExcluded.AddRange(missing);
                    existingExcluded = existingExcluded.Distinct().ToList();
                    var tcExcStr = string.Join(",", existingExcluded);
                    tcSettings["_queryExcludeViewIds"] = tcExcStr;
                    tcSettings["ExcludedFolders"] = tcExcStr;
                    tc.HomeSectionSettings = jsonSerializer.SerializeToString(tcSettings);

                    if (!string.IsNullOrEmpty(tc.Tag))
                    {
                        try
                        {
                            var tagItem = libraryManager.GetItemList(new InternalItemsQuery
                            {
                                IncludeItemTypes = new[] { "Tag" },
                                Name = tc.Tag,
                                Recursive = true
                            }, cancellationToken).FirstOrDefault();
                            if (tagItem != null) tcSettings["_queryTagId"] = tagItem.InternalId.ToString();
                        }
                        catch { }
                    }

                    var tcSafeTag = new string((tc.Name ?? tc.Tag ?? "").Select(c => char.IsLetterOrDigit(c) ? c : '_').ToArray());
                    var tcMarker = "hsc__" + tcSafeTag;

                    var realTracked = (tc.HomeSectionTracked ?? new List<HomeSectionTracking>())
                        .Where(t => !string.IsNullOrEmpty(t.SectionId) && !t.SectionId.StartsWith("hsc__"))
                        .ToList();

                    foreach (var tracking in realTracked)
                    {
                        try
                        {
                            var uid = userManager.GetInternalId(tracking.UserId);
                            var secs = userManager.GetHomeSections(uid, cancellationToken)?.Sections ?? Array.Empty<ContentSection>();
                            var owned = secs.FirstOrDefault(s => s.Id == tracking.SectionId)
                                ?? secs.FirstOrDefault(s => s.Subtitle == tcMarker);
                            if (owned == null) continue;
                            var updatedSec = HomeScreenCompanionTask.BuildContentSection(jsonSerializer, tcSettings, string.Empty, owned);
                            typeof(ContentSection).GetProperty("Id")?.SetValue(updatedSec, owned.Id);
                            userManager.UpdateHomeSection(uid, updatedSec, cancellationToken);
                            totalUpdated++;
                        }
                        catch { }
                    }
                }
            }

            // Aggressive: also keep all untracked, non-library-scoped sections up to date.
            totalUpdated += HomeScreenCompanionTask.UpdateUntrackedSections(
                jsonSerializer, userManager, config, allTopListLibIds, cancellationToken);

            Plugin.Instance?.SaveConfiguration();
            return (totalUpdated, $"Updated {totalUpdated} section(s) across {topLists.Count} top-list(s).");
        }
    }
}
