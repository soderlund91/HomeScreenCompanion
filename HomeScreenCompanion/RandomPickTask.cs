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
    /// Renews the Random pick of sources whose period (15 minutes, hour, day or week, see
    /// RandomPick) has moved on since the pick they last applied. Cheap when nothing is due: it
    /// only compares period keys. A due source gets a Run Group limited to its tags and its
    /// collection (HomeScreenCompanionTask.RunRandomPickAsync), through the same paced code;
    /// a Dry Run changes nothing. One log line per source: "Random pick: +7 −7 (Spotlight)".
    /// </summary>
    public class RandomPickTask : IScheduledTask, IConfigurableScheduledTask
    {
        private readonly PluginLogger _logger;

        // The periods already logged for sources this task does not run or a dry run, so they are
        // not repeated every check.
        private static readonly Dictionary<string, string> Shown = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        public RandomPickTask(ILogManager logManager)
        {
            _logger = PluginLog.Wrap(logManager.GetLogger("HomeScreenCompanion_RandomPick"), "Random pick");
        }

        public string Key => "HomeScreenCompanionRandomPick";
        public string Name => "Random pick";
        public string Description => "Makes a new random pick for sources with Random pick when their period (15 minutes, hour, day or week) changes.";
        public string Category => "Home Screen Companion";

        public bool IsHidden => false;
        public bool IsEnabled => true;
        public bool IsLogged => false;

        public IEnumerable<TaskTriggerInfo> GetDefaultTriggers()
        {
            return new[]
            {
                new TaskTriggerInfo { Type = TaskTriggerInfo.TriggerInterval, IntervalTicks = TimeSpan.FromMinutes(15).Ticks }
            };
        }

        public async Task Execute(CancellationToken cancellationToken, IProgress<double> progress)
        {
            var config = Plugin.Instance?.Configuration;
            var task = HomeScreenCompanionTask.Instance;
            if (config == null || task == null) return;
            // A sync or Run Group is busy: the next check picks it up.
            if (HomeScreenCompanionTask.IsRunning) return;

            var now = DateTime.Now;
            var applied = RandomPickState.Load();
            var due = new List<(TagConfig Source, string Period)>();
            foreach (var group in (config.Tags ?? new List<TagConfig>())
                .Where(t => RandomPick.On(t) && t.Active && !string.IsNullOrWhiteSpace(t.Tag)
                    && HomeScreenCompanionTask.IsScheduleActive(t.ActiveIntervals, now))
                .GroupBy(HomeScreenCompanionTask.GroupKey, StringComparer.OrdinalIgnoreCase))
            {
                var t = group.First();
                var period = RandomPick.PeriodKey(t, now);
                if (applied.TryGetValue(group.Key, out var last) && last == period) continue;
                // Not written to the state (a dry run, a source left to the sync): said once a period.
                bool quiet = config.DryRunMode || t.SourceType == "AI" || task.OnlyAddsSharedTag(config, t);
                if (quiet)
                {
                    lock (Shown)
                    {
                        if (Shown.TryGetValue(group.Key, out var shown) && shown == period) continue;
                        Shown[group.Key] = period;
                    }
                }
                due.Add((t, period));
            }
            if (due.Count == 0) { progress.Report(100); return; }

            int done = 0;
            foreach (var (t, period) in due)
            {
                cancellationToken.ThrowIfCancellationRequested();
                string name = string.IsNullOrWhiteSpace(t.Name) ? t.Tag.Trim() : t.Name.Trim();
                // An AI source would ask the AI again (a paid call) each period: its pick is renewed
                // by the Tag & Collection Sync only.
                if (t.SourceType == "AI")
                    _logger.Info("Random pick: skipped ({0}) — an AI source gets its new pick from the Tag & Collection Sync", name);
                // Another kind of source writes the same tag: a single run could only add it, so
                // the old pick would keep it. Left to the Tag & Collection Sync.
                else if (task.OnlyAddsSharedTag(config, t))
                    _logger.Info("Random pick: skipped ({0}) — its tag is also written by another source; the Tag & Collection Sync renews it", name);
                else
                {
                    try
                    {
                        var gs = await task.RunRandomPickAsync(name, cancellationToken).ConfigureAwait(false);
                        if (gs == null)
                            _logger.Info("Random pick: not run ({0})", name);
                        else
                        {
                            string coll = gs.EnableCollection && (gs.CollectionItemsAdded > 0 || gs.CollectionItemsRemoved > 0)
                                ? $", collection +{gs.CollectionItemsAdded} −{gs.CollectionItemsRemoved}" : "";
                            _logger.Info("{0}Random pick: +{1} −{2}{3} ({4})", config.DryRunMode ? "Dry run — " : "",
                                gs.TagsAdded, gs.TagsRemoved, coll, name);
                        }
                    }
                    catch (OperationCanceledException) { throw; }
                    catch (Exception ex) { _logger.Warn("Random pick: failed ({0}): {1}", name, ex.Message); }
                }
                progress.Report(100.0 * ++done / due.Count);
            }
        }
    }
}
