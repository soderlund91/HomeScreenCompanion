using MediaBrowser.Controller.Library;
using MediaBrowser.Model.Entities;
using MediaBrowser.Model.Logging;
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
        private readonly IUserManager _userManager;
        private readonly IJsonSerializer _jsonSerializer;
        private readonly ILogger _logger;

        public TopListSyncTask(ILibraryManager libraryManager, IUserManager userManager, IJsonSerializer jsonSerializer, ILogManager logManager)
        {
            _libraryManager = libraryManager;
            _userManager = userManager;
            _jsonSerializer = jsonSerializer;
            _logger = logManager.GetLogger("HomeScreenCompanion_TLS");
        }

        public string Key => "TopListSyncTask";
        public string Name => "Top-list section sync";
        public string Description => "Run after manualy creating a library. This ensure each top-list home section only shows items from its own library, and that regular home sections never include top-list libraries.";
        public string Category => "Home Screen Companion";

        public IEnumerable<TaskTriggerInfo> GetDefaultTriggers() => Array.Empty<TaskTriggerInfo>();

        public Task Execute(CancellationToken cancellationToken, IProgress<double> progress)
        {
            SyncAll(_libraryManager, _userManager, _jsonSerializer, cancellationToken);
            return Task.CompletedTask;
        }

        internal static (int updated, string message) SyncAll(
            ILibraryManager libraryManager,
            IUserManager userManager,
            IJsonSerializer jsonSerializer,
            CancellationToken cancellationToken)
        {
            var config = Plugin.Instance?.Configuration;
            if (config == null) return (0, "No config.");

            var topLists = config.TopLists ?? new List<TopListHomeSection>();
            if (topLists.Count == 0) return (0, "No top-lists configured.");

            // Get ALL library IDs currently in the system via GetVirtualFolders
            var allLibIds = libraryManager.GetVirtualFolders()
                .Where(f => !string.IsNullOrEmpty(f.ItemId))
                .Select(f => f.ItemId.Trim().ToLowerInvariant())
                .Distinct()
                .ToList();

            int totalUpdated = 0;

            foreach (var tl in topLists)
            {
                if (string.IsNullOrEmpty(tl.HomeSectionLibraryId) || tl.HomeSectionLibraryId == "auto") continue;

                var ownId = tl.HomeSectionLibraryId.Trim().ToLowerInvariant();

                // Exclude every library except the top-list's own
                var excludeStr = string.Join(",",
                    allLibIds.Where(id => id != ownId));

                var settingsDict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                try
                {
                    if (!string.IsNullOrEmpty(tl.HomeSectionSettings) && tl.HomeSectionSettings != "{}")
                        settingsDict = jsonSerializer.DeserializeFromString<Dictionary<string, string>>(tl.HomeSectionSettings) ?? settingsDict;
                }
                catch { }

                settingsDict["_queryExcludeViewIds"] = excludeStr;
                settingsDict["ExcludedFolders"] = excludeStr;
                tl.HomeSectionSettings = jsonSerializer.SerializeToString(settingsDict);

                var safeTag = new string((tl.TagName ?? "").Select(c => char.IsLetterOrDigit(c) ? c : '_').ToArray());
                var sectionMarker = "hsc__tl__" + safeTag;

                foreach (var tracking in tl.HomeSectionTracked ?? new List<HomeSectionTracking>())
                {
                    if (string.IsNullOrEmpty(tracking.UserId) || string.IsNullOrEmpty(tracking.SectionId)) continue;
                    try
                    {
                        var uid = userManager.GetInternalId(tracking.UserId);
                        var sections = userManager.GetHomeSections(uid, cancellationToken)?.Sections
                            ?? Array.Empty<ContentSection>();

                        var owned = sections.FirstOrDefault(s => s.Id == tracking.SectionId)
                            ?? sections.FirstOrDefault(s => s.Subtitle == sectionMarker);
                        if (owned == null) continue;

                        var updated = HomeScreenCompanionTask.BuildContentSection(
                            jsonSerializer, settingsDict, tl.HomeSectionLibraryId, owned);
                        typeof(ContentSection).GetProperty("Id")?.SetValue(updated, owned.Id);
                        userManager.UpdateHomeSection(uid, updated, cancellationToken);
                        totalUpdated++;
                    }
                    catch { }
                }
            }

            Plugin.Instance?.SaveConfiguration();
            return (totalUpdated, $"Updated {totalUpdated} section(s) across {topLists.Count} top-list(s).");
        }
    }
}
