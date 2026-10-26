using MediaBrowser.Controller.Entities.TV;
using MediaBrowser.Controller.Library;
using MediaBrowser.Controller.Plugins;
using MediaBrowser.Model.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace HomeScreenCompanion
{
    /// <summary>
    /// Real-time half of the Continue Watching bump: collects series that got new episodes and
    /// processes them once the library has been quiet for a while. ContinueWatchingBumpTask is
    /// the scheduled catch-up for anything missed here.
    /// </summary>
    public class ContinueWatchingBumpEntryPoint : IServerEntryPoint
    {
        // A library scan adds many episodes in a burst, and metadata (season/episode numbers)
        // may not be settled at ItemAdded time — wait until additions have stopped.
        private static readonly TimeSpan Debounce = TimeSpan.FromMinutes(2);

        private readonly ILibraryManager _libraryManager;
        private readonly IUserManager _userManager;
        private readonly IUserDataManager _userDataManager;
        private readonly PluginLogger _logger;

        private readonly object _lock = new object();
        private readonly HashSet<long> _pendingSeries = new HashSet<long>();
        private Timer? _timer;

        public ContinueWatchingBumpEntryPoint(ILibraryManager libraryManager, IUserManager userManager, IUserDataManager userDataManager, ILogManager logManager)
        {
            _libraryManager = libraryManager;
            _userManager = userManager;
            _userDataManager = userDataManager;
            _logger = PluginLog.Wrap(logManager.GetLogger("HomeScreenCompanion_CWBump"), "CW Bump");
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
                if (!(e.Item is Episode episode) || episode.IsVirtualItem) return;
                if (Plugin.Instance?.Configuration?.ContinueWatchingBumpEnabled != true) return;

                var seriesId = episode.SeriesId;
                if (seriesId <= 0) return;

                lock (_lock)
                {
                    _pendingSeries.Add(seriesId);
                    if (_timer == null) _timer = new Timer(_ => Flush(), null, Debounce, Timeout.InfiniteTimeSpan);
                    else _timer.Change(Debounce, Timeout.InfiniteTimeSpan);
                }
            }
            catch (Exception ex)
            {
                _logger.Error("[CW Bump] OnItemAdded error: " + ex.Message);
            }
        }

        private void Flush()
        {
            long[] seriesIds;
            lock (_lock)
            {
                seriesIds = _pendingSeries.ToArray();
                _pendingSeries.Clear();
            }
            if (seriesIds.Length == 0) return;

            // Off the timer thread, like the other library mutations in this plugin.
            Task.Run(() =>
            {
                try
                {
                    var config = Plugin.Instance?.Configuration;
                    if (config == null || !config.ContinueWatchingBumpEnabled) return;

                    var users = ContinueWatchingBumper.ResolveUsers(config, _userManager);
                    if (users.Count == 0) return;

                    var log = new RunLog(ContinueWatchingBumpTask.ExecutionLog, _logger, "[CW Bump]", config.ExtendedConsoleOutput);
                    foreach (var id in seriesIds)
                    {
                        if (_libraryManager.GetItemById(id) is Series series)
                            ContinueWatchingBumper.ProcessSeries(series, users, config, _libraryManager, _userDataManager, log, CancellationToken.None);
                    }
                }
                catch (Exception ex)
                {
                    _logger.Error("[CW Bump] Flush error: " + ex.Message);
                }
            });
        }
    }
}
