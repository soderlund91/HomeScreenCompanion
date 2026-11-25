using MediaBrowser.Controller.Library;
using MediaBrowser.Model.Logging;
using MediaBrowser.Model.Serialization;
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
    /// A rotating row (RotatingRow) that is due runs its own Run Group, also when that failed
    /// or was missed before: being due is not lost.
    /// It also puts Row shuffle's sets in the order of their period (RowShuffle), when no other run
    /// holds RunGate; else on the next check.
    /// </summary>
    public class ScheduleSwitchTask : IScheduledTask, IConfigurableScheduledTask
    {
        private readonly ITaskManager _taskManager;
        private readonly PluginLogger _logger;
        private readonly IJsonSerializer _json;
        private readonly IUserManager _userManager;

        public ScheduleSwitchTask(ITaskManager taskManager, ILogManager logManager, IJsonSerializer json, IUserManager userManager)
        {
            _taskManager = taskManager;
            _json = json;
            _userManager = userManager;
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
            if (previous == null) { Save(statePath, state); RunDueRotations(config); ShuffleRows(config, cancellationToken); return Task.CompletedTask; }
            if (previous == state) { RunDueRotations(config); ShuffleRows(config, cancellationToken); return Task.CompletedTask; }

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

        // Rotating rows on their way to the next item, or new or changed: only their Run Groups run.
        private void RunDueRotations(PluginConfiguration config)
        {
            var task = HomeScreenCompanionTask.Instance;
            if (task == null || config.DryRunMode || HomeScreenCompanionTask.IsRunning) return;
            var due = config.Tags.Where(t => RotatingRow.Is(t) && t.Active && t.EnableHomeSection && !string.IsNullOrWhiteSpace(t.Tag)
                    && (t.HomeSectionUserIds?.Count ?? 0) > 0 && RotatingRow.Items(t).Count > 0
                    && HomeScreenCompanionTask.IsScheduleActive(t.ActiveIntervals, DateTime.Now) && RotatingRow.Due(t, _json))
                .Select(t => string.IsNullOrWhiteSpace(t.Name) ? t.Tag : t.Name).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
            if (due.Count == 0) return;
            _logger.Info("Rotating row due; running {0}", string.Join(", ", due.Select(n => $"Run Group '{n}'")));
            foreach (var name in due)
                _ = Task.Run(() => task.RunSingleEntryAsync(name, CancellationToken.None));
        }

        // Row shuffle: a new period (or a set switched off or deleted). Only when no run holds the
        // gate (rule 8); a run puts the rows in order at its end, the next check does the rest.
        private static string _lastDryRunNote = "";
        private void ShuffleRows(PluginConfiguration config, CancellationToken cancellationToken)
        {
            if (!RowShuffle.HasWork(config)) return;
            using var gate = RunGate.TryEnter("Row shuffle");
            if (gate == null) return;
            try
            {
                bool dry = config.DryRunMode;
                // A dry run says it once per change, not every 15 minutes.
                var log = new RunLog(new List<string>(), dry ? null : _logger, "", false);
                int users = RowShuffle.Apply(config, _userManager, dry, log, cancellationToken);
                if (!dry && users > 0) _logger.Info("Row shuffle: rows moved for {0}", RunLog.Plural(users, "user"));
                if (dry)
                {
                    var note = users > 0 ? $"Dry run: Row shuffle would move rows for {RunLog.Plural(users, "user")}" : "";
                    if (note.Length > 0 && note != _lastDryRunNote) _logger.Info(note);
                    _lastDryRunNote = note;
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex) { _logger.Warn("Row shuffle failed: {0}", ex.Message); }
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
