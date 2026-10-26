using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Entities.TV;
using MediaBrowser.Controller.Library;
using MediaBrowser.Model.Logging;
using MediaBrowser.Model.Tasks;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace HomeScreenCompanion
{
    /// <summary>
    /// Scheduled catch-up for the Continue Watching bump: re-checks every series that received
    /// episodes recently. Safe to run often — ContinueWatchingBumper is idempotent.
    /// </summary>
    public class ContinueWatchingBumpTask : IScheduledTask, IConfigurableScheduledTask
    {
        private const int LookbackDays = 14;

        private readonly ILibraryManager _libraryManager;
        private readonly IUserManager _userManager;
        private readonly IUserDataManager _userDataManager;
        private readonly PluginLogger _logger;

        public static List<string> ExecutionLog { get; } = new List<string>();

        public ContinueWatchingBumpTask(ILibraryManager libraryManager, IUserManager userManager, IUserDataManager userDataManager, ILogManager logManager)
        {
            _libraryManager = libraryManager;
            _userManager = userManager;
            _userDataManager = userDataManager;
            _logger = PluginLog.Wrap(logManager.GetLogger("HomeScreenCompanion_CWBump"), "CW Bump");
        }

        public string Key => "ContinueWatchingBumpTask";
        public string Name => "Continue Watching Bump";
        public string Description => "Moves series you had finished to the front of Continue Watching when new episodes arrive.";
        public string Category => "Home Screen Companion";

        // Not listed in Emby's Scheduled Tasks (new episodes are handled as they arrive; this sweep keeps its interval and is in the Run menu). It still runs on its triggers.
        public bool IsHidden => true;
        public bool IsEnabled => true;
        public bool IsLogged => true;

        public IEnumerable<TaskTriggerInfo> GetDefaultTriggers()
        {
            return new[]
            {
                new TaskTriggerInfo { Type = TaskTriggerInfo.TriggerInterval, IntervalTicks = TimeSpan.FromHours(6).Ticks }
            };
        }

        public Task Execute(CancellationToken cancellationToken, IProgress<double> progress)
        {
            lock (ExecutionLog) { ExecutionLog.Clear(); }

            var config = Plugin.Instance?.Configuration;
            if (config == null) return Task.CompletedTask;

            var log = new RunLog(ExecutionLog, _logger, "[CW Bump]", config.ExtendedConsoleOutput);
            log.Rule();
            log.Info($"Continue Watching Bump  ·  {DateTime.Now:yyyy-MM-dd HH:mm}");
            log.Rule();

            if (!config.ContinueWatchingBumpEnabled)
            {
                log.Skip("Continue Watching bump is disabled in Settings — nothing to do");
                return Task.CompletedTask;
            }

            var users = ContinueWatchingBumper.ResolveUsers(config, _userManager);
            if (users.Count == 0)
            {
                log.Warn("No users are selected in Settings — nothing to do");
                return Task.CompletedTask;
            }

            var seriesIds = _libraryManager.GetItemList(new InternalItemsQuery
            {
                IncludeItemTypes = new[] { "Episode" },
                Recursive = true,
                IsVirtualItem = false,
                MinDateCreated = DateTimeOffset.UtcNow.AddDays(-LookbackDays)
            })
            .OfType<Episode>()
            .Select(e => e.SeriesId)
            .Where(id => id > 0)
            .Distinct()
            .ToList();

            log.Info($"  {RunLog.Plural(seriesIds.Count, "series", "series")} with episodes added in the last {LookbackDays} days  ·  {RunLog.Plural(users.Count, "user")}");

            int bumped = 0;
            for (int i = 0; i < seriesIds.Count; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (_libraryManager.GetItemById(seriesIds[i]) is Series series)
                    bumped += ContinueWatchingBumper.ProcessSeries(series, users, config, _libraryManager, _userDataManager, log, cancellationToken);
                progress.Report(100.0 * (i + 1) / seriesIds.Count);
            }

            if (bumped == 0) log.Skip("Nothing to bump");
            else log.Ok($"{RunLog.Plural(bumped, "bump")} done");
            return Task.CompletedTask;
        }
    }
}
