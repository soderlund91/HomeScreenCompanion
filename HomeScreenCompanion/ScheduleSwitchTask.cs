using MediaBrowser.Model.Logging;
using MediaBrowser.Model.Tasks;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace HomeScreenCompanion
{
    /// <summary>
    /// Runs the Tag &amp; Collection Sync when a scheduled source starts or stops being active, so
    /// time-of-day schedules (e.g. a 20:00 Spotlight) switch on time instead of at the 04:00 sync.
    /// It only checks the schedules; the sync itself does the switching (overrides, cleanup).
    /// </summary>
    public class ScheduleSwitchTask : IScheduledTask, IConfigurableScheduledTask
    {
        private readonly ITaskManager _taskManager;
        private readonly PluginLogger _logger;

        public ScheduleSwitchTask(ITaskManager taskManager, ILogManager logManager)
        {
            _taskManager = taskManager;
            _logger = PluginLog.Wrap(logManager.GetLogger("HomeScreenCompanion_Schedule"), "Schedule");
        }

        public string Key => "HomeScreenCompanionScheduleSwitch";
        public string Name => "Schedule Switch";
        public string Description => "Runs the Tag & Collection Sync when a source's schedule starts or ends.";
        public string Category => "Home Screen Companion";

        // Not listed in Emby's Scheduled Tasks (the schedule clock: it only starts the Tag & Collection Sync when a schedule starts or ends). It still runs on its triggers.
        public bool IsHidden => true;
        public bool IsEnabled => true;
        public bool IsLogged => true;

        public IEnumerable<TaskTriggerInfo> GetDefaultTriggers()
        {
            return new[]
            {
                new TaskTriggerInfo { Type = TaskTriggerInfo.TriggerInterval, IntervalTicks = TimeSpan.FromMinutes(15).Ticks }
            };
        }

        public Task Execute(CancellationToken cancellationToken, IProgress<double> progress)
        {
            var config = Plugin.Instance?.Configuration;
            if (config == null) return Task.CompletedTask;

            var state = CurrentState(config, DateTime.Now);
            var statePath = Path.Combine(Plugin.Instance!.DataFolderPath, "schedule-state.txt");
            string? previous = null;
            try { if (File.Exists(statePath)) previous = File.ReadAllText(statePath); } catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }

            // First run: remember the state; the daily sync already matches it.
            if (previous == null) { Save(statePath, state); return Task.CompletedTask; }
            if (previous == state) return Task.CompletedTask;

            // Try again on the next check rather than lose the switch.
            if (HomeScreenCompanionTask.IsRunning) return Task.CompletedTask;

            var was = ParseState(previous);
            var now = ParseState(state);
            var changed = now.Where(kv => !was.TryGetValue(kv.Key, out var w) || w != kv.Value)
                .Select(kv => $"{kv.Key} {(kv.Value ? "started" : "ended")}")
                .Concat(was.Keys.Where(k => !now.ContainsKey(k)).Select(k => $"{k} removed")).ToList();
            _logger.Info("Schedule changed ({0}); running the Tag & Collection Sync", string.Join(", ", changed));
            Save(statePath, state);
            _taskManager.QueueScheduledTask<HomeScreenCompanionTask>();
            return Task.CompletedTask;
        }

        // One line per scheduled source: "<name>\t1" when in schedule, "\t0" when not.
        internal static string CurrentState(PluginConfiguration config, DateTime now)
        {
            var lines = config.Tags
                .Where(t => t.Active && t.ActiveIntervals != null && t.ActiveIntervals.Count > 0 && !string.IsNullOrWhiteSpace(t.Tag))
                .Select(t => $"{(string.IsNullOrWhiteSpace(t.Name) ? t.Tag.Trim() : t.Name.Trim())}\t{(HomeScreenCompanionTask.IsScheduleActive(t.ActiveIntervals, now) ? 1 : 0)}")
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(l => l, StringComparer.OrdinalIgnoreCase);
            return string.Join("\n", lines);
        }

        private static Dictionary<string, bool> ParseState(string state)
        {
            var result = new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase);
            foreach (var line in state.Split('\n'))
            {
                var parts = line.Split('\t');
                if (parts.Length == 2) result[parts[0]] = parts[1] == "1";
            }
            return result;
        }

        private void Save(string path, string state)
        {
            try { Directory.CreateDirectory(Path.GetDirectoryName(path)!); File.WriteAllText(path, state); }
            catch (Exception ex) { _logger.Warn("Could not save the schedule state: {0}", ex.Message); }
        }
    }
}
