using MediaBrowser.Controller.Entities.Movies;
using MediaBrowser.Controller.Entities.TV;
using MediaBrowser.Controller.Library;
using MediaBrowser.Controller.Plugins;
using MediaBrowser.Model.Logging;
using MediaBrowser.Model.Tasks;
using System;
using System.Threading;

namespace HomeScreenCompanion
{
    /// <summary>
    /// Original-language posters for new titles: once the library has been quiet for a while after
    /// movies or episodes were added, starts OriginalPostersTask (it only does what is new).
    /// </summary>
    public class OriginalPostersEntryPoint : IServerEntryPoint
    {
        // Long enough for Emby to have read the new titles' metadata (their TMDb ids) first.
        private static readonly TimeSpan Debounce = TimeSpan.FromMinutes(10);

        private readonly ILibraryManager _libraryManager;
        private readonly ITaskManager _taskManager;
        private readonly PluginLogger _logger;
        private readonly object _lock = new object();
        private Timer? _timer;

        public OriginalPostersEntryPoint(ILibraryManager libraryManager, ITaskManager taskManager, ILogManager logManager)
        {
            _libraryManager = libraryManager;
            _taskManager = taskManager;
            _logger = PluginLog.Wrap(logManager.GetLogger("HomeScreenCompanion_OrigPosters"), "Original posters");
        }

        public void Run()
        {
            _libraryManager.ItemAdded += OnItemAdded;
        }

        public void Dispose()
        {
            _libraryManager.ItemAdded -= OnItemAdded;
            lock (_lock)
            {
                _timer?.Dispose();
                _timer = null;
            }
        }

        private void OnItemAdded(object sender, ItemChangeEventArgs e)
        {
            try
            {
                if (!(e.Item is Movie || e.Item is Series || e.Item is Season || e.Item is Episode) || e.Item.IsVirtualItem) return;
                if (Plugin.Instance?.Configuration?.OrigPostersEnabled != true) return;
                if (ShowTopList.IsCopy(e.Item) || RankedCollections.IsRankedCopy(e.Item)) return;
                lock (_lock)
                {
                    if (_timer == null) _timer = new Timer(_ => Start(), null, Debounce, Timeout.InfiniteTimeSpan);
                    else _timer.Change(Debounce, Timeout.InfiniteTimeSpan);
                }
            }
            catch (Exception ex)
            {
                _logger.Error("[Original posters] OnItemAdded error: " + ex.Message);
            }
        }

        private void Start()
        {
            try
            {
                if (Plugin.Instance?.Configuration?.OrigPostersEnabled != true) return;
                _taskManager.QueueScheduledTask<OriginalPostersTask>();
            }
            catch (Exception ex)
            {
                _logger.Error("[Original posters] Could not start the task: " + ex.Message);
            }
        }
    }
}
