using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Library;
using MediaBrowser.Model.Entities;
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
    public class MergeTopListVersionsTask : IScheduledTask
    {
        private readonly ILibraryManager _libraryManager;

        public static List<string> ExecutionLog { get; } = new List<string>();
        public static bool IsRunning { get; private set; } = false;
        public static string LastRunStatus { get; private set; } = "Never";

        public MergeTopListVersionsTask(ILibraryManager libraryManager)
        {
            _libraryManager = libraryManager;
        }

        public string Key => "MergeTopListVersionsTask";
        public string Name => "Merge top-list versions with library";
        public string Description => "Links each top-list .strm item to its corresponding library movie as an alternate version (like 1080p/4K) to prevent UI duplicates. Run this after a library scan has indexed newly added top-list entries.";
        public string Category => "Home Screen Companion";

        public IEnumerable<TaskTriggerInfo> GetDefaultTriggers() => Array.Empty<TaskTriggerInfo>();

        public Task Execute(CancellationToken cancellationToken, IProgress<double> progress)
        {
            IsRunning = true;
            lock (ExecutionLog) { ExecutionLog.Clear(); }
            try
            {
                var merged = MergeAll(_libraryManager, cancellationToken);
                LastRunStatus = $"Done — {merged} item(s) linked.";
                Log(LastRunStatus);
            }
            catch (Exception ex)
            {
                LastRunStatus = $"Error: {ex.Message}";
                Log(LastRunStatus);
            }
            finally
            {
                IsRunning = false;
            }
            return Task.CompletedTask;
        }

        private static void Log(string message)
        {
            var msg = $"[{DateTime.Now:HH:mm:ss}] {message}";
            lock (ExecutionLog) { ExecutionLog.Add(msg); }
        }

        internal static int MergeAll(ILibraryManager libraryManager, CancellationToken cancellationToken)
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
                var imdb = m.GetProviderId("Imdb");
                if (!string.IsNullOrEmpty(imdb) && !origLookup.ContainsKey(imdb))
                    origLookup[imdb] = m;
            }

            Log($"Original movies in lookup: {origLookup.Count}");

            // Find all indexed STRM items living under any top-list subfolder
            var strmItems = libraryManager.GetItemList(new InternalItemsQuery
            {
                IncludeItemTypes = new[] { "Movie" },
                Recursive = true,
                IsVirtualItem = false
            }).Where(i => !string.IsNullOrEmpty(i.Path)
                       && i.Path.StartsWith(topListsFolder, StringComparison.OrdinalIgnoreCase))
              .ToList();

            Log($"Indexed top-list STRM items found: {strmItems.Count}");

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
                    merged++;
                    Log($"Merged '{li.Name}' with '{primary.Name}'");
                }
                catch (Exception ex)
                {
                    Log($"Could not merge '{li.Name}': {ex.Message}");
                }
            }

            return merged;
        }
    }
}
