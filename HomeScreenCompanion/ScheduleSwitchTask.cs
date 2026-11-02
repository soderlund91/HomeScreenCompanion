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
    /// A rotating row (RotatingRow) moving on to its next collection (at midnight, server time)
    /// runs the sync the same way.
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
                .Select(kv => kv.Key.StartsWith(RotationKey, StringComparison.Ordinal)
                    ? $"{kv.Key.Substring(RotationKey.Length)}: next collection"
                    : $"{kv.Key} {(kv.Value == "1" ? "started" : "ended")}")
                .Concat(was.Keys.Where(k => !now.ContainsKey(k)).Select(k => k.StartsWith(RotationKey, StringComparison.Ordinal) ? $"{k.Substring(RotationKey.Length)}: rotation stopped" : $"{k} removed")).ToList();
            // Only rotating rows moved on: just those groups run (Run Group), not the full sync.
            var rotated = now.Where(kv => kv.Key.StartsWith(RotationKey, StringComparison.Ordinal) && (!was.TryGetValue(kv.Key, out var w) || w != kv.Value))
                .Select(kv => kv.Key.Substring(RotationKey.Length)).ToList();
            bool onlyRotation = rotated.Count > 0 && rotated.Count == changed.Count && HomeScreenCompanionTask.Instance != null;
            _logger.Info("Schedule changed ({0}); running {1}", string.Join(", ", changed),
                onlyRotation ? string.Join(", ", rotated.Select(n => $"Run Group '{n}'")) : "the Tag & Collection Sync");
            Save(statePath, state);
            if (onlyRotation)
            {
                var task = HomeScreenCompanionTask.Instance!;
                foreach (var name in rotated)
                    _ = Task.Run(() => task.RunSingleEntryAsync(name, CancellationToken.None));
            }
            else _taskManager.QueueScheduledTask<HomeScreenCompanionTask>();
            return Task.CompletedTask;
        }

        private const string RotationKey = "rotation\u001F";

        // One line per scheduled source: "<name>\t1" when in schedule, "\t0" when not; and one per
        // rotating source in schedule: "rotation\u001F<name>\t<period number>" (see RotatingRow).
        internal static string CurrentState(PluginConfiguration config, DateTime now)
        {
            var today = RotatingRow.Today();
            var lines = config.Tags
                .Where(t => t.Active && t.ActiveIntervals != null && t.ActiveIntervals.Count > 0 && !string.IsNullOrWhiteSpace(t.Tag))
                .Select(t => $"{(string.IsNullOrWhiteSpace(t.Name) ? t.Tag.Trim() : t.Name.Trim())}\t{(HomeScreenCompanionTask.IsScheduleActive(t.ActiveIntervals, now) ? 1 : 0)}")
                .Concat(config.Tags
                    .Where(t => t.Active && RotatingRow.Is(t) && !string.IsNullOrWhiteSpace(t.Tag) && HomeScreenCompanionTask.IsScheduleActive(t.ActiveIntervals, now))
                    .Select(t => $"{RotationKey}{(string.IsNullOrWhiteSpace(t.Name) ? t.Tag.Trim() : t.Name.Trim())}\t{RotatingRow.Period(t, today)}"))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(l => l, StringComparer.OrdinalIgnoreCase);
            return string.Join("\n", lines);
        }

        private static Dictionary<string, string> ParseState(string state)
        {
            var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var line in state.Split('\n'))
            {
                var parts = line.Split('\t');
                if (parts.Length == 2) result[parts[0]] = parts[1];
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
