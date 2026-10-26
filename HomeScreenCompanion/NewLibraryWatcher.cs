using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Library;
using MediaBrowser.Model.Logging;
using MediaBrowser.Model.Serialization;
using MediaBrowser.Model.Tasks;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

namespace HomeScreenCompanion
{
    /// <summary>
    /// A library created in Emby after a top-list's home rows were set up is not on their
    /// exclusion lists, so its titles show up in the top-list rows until "Sync top-list rows" runs.
    /// Emby has no "library added" event, so this keeps the last known set of libraries in
    /// &lt;plugin data&gt;/known_libraries.json and compares it with Emby's after a library item is
    /// added, after a scheduled task finishes (creating a library starts a scan) and shortly after
    /// startup. A new library (not a top-list's own) queues the Top-list section sync.
    /// </summary>
    internal sealed class NewLibraryWatcher : IDisposable
    {
        private readonly ILibraryManager _libraryManager;
        private readonly ITaskManager _taskManager;
        private readonly IJsonSerializer _json;
        private readonly PluginLogger _logger;
        private readonly string _path;
        private readonly object _lock = new object();
        private Timer? _timer;

        internal NewLibraryWatcher(ILibraryManager libraryManager, ITaskManager taskManager, IJsonSerializer json, PluginLogger logger, string dataPath)
        {
            _libraryManager = libraryManager;
            _taskManager = taskManager;
            _json = json;
            _logger = logger;
            _path = Path.Combine(dataPath, "known_libraries.json");
        }

        internal void Start()
        {
            _libraryManager.ItemAdded += OnItemAdded;
            _taskManager.TaskCompleted += OnTaskCompleted;
            // Libraries created while the server was down.
            Schedule(TimeSpan.FromMinutes(1));
        }

        private void OnItemAdded(object? sender, ItemChangeEventArgs e)
        {
            if (e.Item is CollectionFolder) Schedule(TimeSpan.FromSeconds(15));
        }

        private void OnTaskCompleted(object? sender, TaskCompletionEventArgs e)
        {
            // Our own run would only find nothing new.
            if (e.Task?.ScheduledTask is TopListSyncTask) return;
            Schedule(TimeSpan.FromSeconds(15));
        }

        // Debounced: a scan raises many events; only the last one runs the check.
        private void Schedule(TimeSpan delay)
        {
            lock (_lock)
            {
                if (_timer == null) _timer = new Timer(_ => Check(), null, delay, Timeout.InfiniteTimeSpan);
                else _timer.Change(delay, Timeout.InfiniteTimeSpan);
            }
        }

        private void Check()
        {
            try
            {
                var current = _libraryManager.GetVirtualFolders()
                    .Select(f => (f.ItemId ?? "").Replace("-", "").Trim().ToLowerInvariant())
                    .Where(id => id.Length > 0)
                    .Distinct().ToList();
                var known = Load();
                Save(current);
                // First run after installing / updating: only remember what is there.
                if (known == null) return;

                var added = current.Where(id => !known.Contains(id)).ToList();
                if (added.Count == 0) return;

                var topLists = Plugin.Instance?.Configuration?.TopLists ?? new List<TopListHomeSection>();
                var topListLibs = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (var tl in topLists)
                    foreach (var id in TopListSyncTask.OwnLibraryIds(tl.HomeSectionLibraryId, _libraryManager))
                        topListLibs.Add(id.Replace("-", ""));
                if (topListLibs.Count == 0) return;   // no top-list rows to keep clean

                // A top-list's own library is set up by HSC itself (which already runs the sync).
                var foreign = added.Where(id => !topListLibs.Contains(id)).ToList();
                if (foreign.Count == 0) return;

                var names = _libraryManager.GetVirtualFolders()
                    .Where(f => foreign.Contains((f.ItemId ?? "").Replace("-", "").Trim().ToLowerInvariant()))
                    .Select(f => f.Name);
                _logger.Info($"[Libraries] New librar{(foreign.Count == 1 ? "y" : "ies")} {string.Join(", ", names.Select(n => "'" + n + "'"))} — running Top-list section sync so top-list rows leave it out.");
                _taskManager.QueueScheduledTask<TopListSyncTask>();
            }
            catch (Exception ex)
            {
                _logger.Warn("[Libraries] Could not check for new libraries: " + ex.Message);
            }
        }

        private HashSet<string>? Load()
        {
            try
            {
                if (!File.Exists(_path)) return null;
                var list = _json.DeserializeFromString<List<string>>(File.ReadAllText(_path));
                return list == null ? null : new HashSet<string>(list, StringComparer.OrdinalIgnoreCase);
            }
            catch { return null; }
        }

        private void Save(List<string> ids)
        {
            try
            {
                Directory.CreateDirectory(Path.GetDirectoryName(_path)!);
                File.WriteAllText(_path, _json.SerializeToString(ids));
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
        }

        public void Dispose()
        {
            _libraryManager.ItemAdded -= OnItemAdded;
            _taskManager.TaskCompleted -= OnTaskCompleted;
            lock (_lock) { _timer?.Dispose(); _timer = null; }
        }
    }
}
