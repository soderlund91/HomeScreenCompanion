using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Library;
using MediaBrowser.Controller.Providers;
using MediaBrowser.Model.Entities;
using MediaBrowser.Model.IO;
using MediaBrowser.Model.Logging;
using MediaBrowser.Model.Querying;
using MediaBrowser.Model.Tasks;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace HomeScreenCompanion
{
    public class MergeTopListVersionsTask : IScheduledTask, IConfigurableScheduledTask
    {
        private readonly ILibraryManager _libraryManager;
        private readonly IProviderManager _providerManager;
        private readonly IFileSystem _fileSystem;

        public static List<string> ExecutionLog { get; } = new List<string>();
        public static bool IsRunning { get; private set; } = false;
        private static RunLog _log = new RunLog(ExecutionLog, null, "", false);
        public static string LastRunStatus { get; private set; } = "Never";

        private readonly PluginLogger _logger;

        public MergeTopListVersionsTask(ILibraryManager libraryManager, IProviderManager providerManager, IFileSystem fileSystem, ILogManager logManager)
        {
            _libraryManager = libraryManager;
            _providerManager = providerManager;
            _fileSystem = fileSystem;
            _logger = PluginLog.Wrap(logManager.GetLogger("HomeScreenCompanion_TopListMerge"), "Top-list merge");
        }

        // Forces Emby to ffprobe a top-list .strm item so it gets a real RunTimeTicks and
        // MediaStreams. Without this, freshly indexed .strm items have RunTimeTicks = 0, which
        // makes Emby mark the movie fully played (instead of saving a resume point) when stopped
        // mid-playback from the top-list. EnableRemoteContentProbe is the flag that makes the
        // server probe .strm targets (they are skipped during a normal library scan).
        internal static void QueueStrmProbe(IProviderManager providerManager, IFileSystem fileSystem, BaseItem li)
        {
            if (providerManager == null || fileSystem == null || li == null) return;
            // Already has a real runtime → nothing to fix. Makes this safe to call repeatedly
            // (e.g. from the post-creation poll loop) without re-probing healthy items.
            if ((li.RunTimeTicks ?? 0) > 0) return;
            try
            {
                var opts = new MetadataRefreshOptions(fileSystem)
                {
                    MetadataRefreshMode      = MetadataRefreshMode.FullRefresh,
                    EnableRemoteContentProbe = true,
                    ImageRefreshMode         = MetadataRefreshMode.ValidationOnly, // keep our ranked posters
                    ReplaceAllImages         = false,
                    ForceSave                = true
                };
                providerManager.QueueRefresh(li.InternalId, opts, RefreshPriority.High);
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
        }

        public string Key => "MergeTopListVersionsTask";
        public string Name => "Merge top-list versions with library";
        public string Description => "Links each top-list .strm item to its corresponding library movie as an alternate version (like 1080p/4K) to prevent UI duplicates, and syncs their collection memberships when that option is on. Run this after a library scan has indexed newly added top-list entries.";
        public string Category => "Home Screen Companion";

        // Not listed in Emby's Scheduled Tasks (copies are merged as Emby adds them; this repair run is in the Run menu). It still runs on its triggers.
        public bool IsHidden => true;
        public bool IsEnabled => true;
        public bool IsLogged => true;

        public IEnumerable<TaskTriggerInfo> GetDefaultTriggers() => Array.Empty<TaskTriggerInfo>();

        public Task Execute(CancellationToken cancellationToken, IProgress<double> progress)
        {
            IsRunning = true;
            lock (ExecutionLog) { ExecutionLog.Clear(); }
            var startTime = DateTime.Now;
            _log = new RunLog(ExecutionLog, _logger, "[Top-list merge]", Plugin.Instance?.Configuration?.ExtendedConsoleOutput ?? false);
            try
            {
                _log.Info($"» Merging top-list versions with library  ·  {startTime:yyyy-MM-dd HH:mm}");
                var merged = MergeAll(_libraryManager, _providerManager, _fileSystem, cancellationToken);
                // Bring collection memberships of all copies in line with the setting (mirror or clear).
                var collChanges = TopListCollectionMirror.SyncAll(cancellationToken).GetAwaiter().GetResult();
                LastRunStatus = $"Done — {merged} item(s) linked.";
                _log.Ok($"{RunLog.Plural(merged, "top-list item")} linked to library movies  ·  {RunLog.Elapsed(DateTime.Now - startTime)}");
                if (collChanges > 0)
                    _log.Ok($"{RunLog.Plural(collChanges, "collection membership")} updated on top-list items");
            }
            catch (Exception ex)
            {
                LastRunStatus = $"Error: {ex.Message}";
                _log.Error($"Merge aborted: {ex.Message}");
            }
            finally
            {
                IsRunning = false;
            }
            return Task.CompletedTask;
        }

        internal static int MergeAll(ILibraryManager libraryManager, IProviderManager providerManager, IFileSystem fileSystem, CancellationToken cancellationToken)
        {
            var dataPath = Plugin.Instance?.DataFolderPath;
            if (string.IsNullOrEmpty(dataPath)) return 0;

            var topListsFolder = Path.Combine(dataPath, "toplists") + Path.DirectorySeparatorChar;
            if (!Directory.Exists(topListsFolder.TrimEnd(Path.DirectorySeparatorChar))) return 0;

            // Build IMDb → original movie lookup (non-strm items only)
            var origLookup = new Dictionary<string, BaseItem>(StringComparer.OrdinalIgnoreCase);
            foreach (var m in libraryManager.GetItemList(new InternalItemsQuery
            {
                IncludeItemTypes = new[] { "Movie" },
                Recursive = true,
                IsVirtualItem = false
            }))
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (string.IsNullOrEmpty(m.Path)) continue;
                if (m.Path.StartsWith(topListsFolder, StringComparison.OrdinalIgnoreCase)) continue;
                if (RankedCollections.IsRankedPath(m.Path)) continue;
                var imdb = m.GetProviderId("Imdb");
                if (!string.IsNullOrEmpty(imdb) && !origLookup.ContainsKey(imdb))
                    origLookup[imdb] = m;
            }

            _log.Debug($"  {origLookup.Count:N0} library movies with IMDb id");

            // Find all indexed STRM items living under any top-list subfolder
            var strmItems = libraryManager.GetItemList(new InternalItemsQuery
            {
                IncludeItemTypes = new[] { "Movie" },
                Recursive = true,
                IsVirtualItem = false
            }).Where(i => !string.IsNullOrEmpty(i.Path)
                       && i.Path.StartsWith(topListsFolder, StringComparison.OrdinalIgnoreCase))
              .ToList();

            _log.Debug($"  {strmItems.Count} indexed top-list items found");

            int merged = 0;

            foreach (var li in strmItems)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var imdb = li.GetProviderId("Imdb");
                if (string.IsNullOrEmpty(imdb)) continue;
                if (!origLookup.TryGetValue(imdb, out var primary)) continue;
                if (li.Id == primary.Id) continue;

                try
                {
                    libraryManager.MergeItems(new[] { primary, li });
                    QueueStrmProbe(providerManager, fileSystem, li);
                    merged++;
                    _log.Debug($"  '{li.Name}' linked to '{primary.Name}'");
                }
                catch (Exception ex)
                {
                    _log.Warn($"'{li.Name}' could not be linked to '{primary.Name}': {ex.Message}");
                }
            }

            return merged;
        }

        // Merges + probes only the STRM items living under a single top-list folder.
        // Used right after a top-list is created (post library-scan) so RunTimeTicks is filled in
        // automatically without waiting for the daily task. Returns how many STRM items are indexed
        // under the folder and how many were mergeable, so the caller can poll until the async scan
        // has fully indexed the new items.
        internal static (int indexed, int merged) MergeAndProbeFolder(
            ILibraryManager libraryManager, IProviderManager providerManager, IFileSystem fileSystem,
            string folderPath, CancellationToken cancellationToken)
        {
            if (string.IsNullOrEmpty(folderPath)) return (0, 0);
            var folderPrefix = folderPath.TrimEnd(Path.DirectorySeparatorChar) + Path.DirectorySeparatorChar;

            var dataPath = Plugin.Instance?.DataFolderPath;
            var topListsFolder = string.IsNullOrEmpty(dataPath)
                ? null
                : Path.Combine(dataPath, "toplists") + Path.DirectorySeparatorChar;

            var allMovies = libraryManager.GetItemList(new InternalItemsQuery
            {
                IncludeItemTypes = new[] { "Movie" },
                Recursive = true,
                IsVirtualItem = false
            });

            // IMDb → original (non-strm) movie lookup
            var origLookup = new Dictionary<string, BaseItem>(StringComparer.OrdinalIgnoreCase);
            foreach (var m in allMovies)
            {
                if (string.IsNullOrEmpty(m.Path)) continue;
                if (topListsFolder != null && m.Path.StartsWith(topListsFolder, StringComparison.OrdinalIgnoreCase)) continue;
                if (RankedCollections.IsRankedPath(m.Path)) continue;
                var imdb = m.GetProviderId("Imdb");
                if (!string.IsNullOrEmpty(imdb) && !origLookup.ContainsKey(imdb))
                    origLookup[imdb] = m;
            }

            var strmItems = allMovies
                .Where(i => !string.IsNullOrEmpty(i.Path)
                         && i.Path.StartsWith(folderPrefix, StringComparison.OrdinalIgnoreCase))
                .ToList();

            int merged = 0;
            foreach (var li in strmItems)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var imdb = li.GetProviderId("Imdb");
                if (string.IsNullOrEmpty(imdb)) continue;
                if (!origLookup.TryGetValue(imdb, out var primary)) continue;
                if (li.Id == primary.Id) continue;
                try
                {
                    libraryManager.MergeItems(new[] { primary, li });
                    QueueStrmProbe(providerManager, fileSystem, li);
                    merged++;
                }
                catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            }

            return (strmItems.Count, merged);
        }
    }
}
