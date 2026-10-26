using MediaBrowser.Model.Logging;
using System;
using System.Collections.Generic;
using System.Threading;

namespace HomeScreenCompanion
{
    /// <summary>
    /// Shared writer for the live execution log shown on the plugin page.
    ///
    /// Every task keeps its own <c>ExecutionLog</c> list (the sink) which the status endpoints
    /// read; this class only decides how lines are formatted and which of them also reach the
    /// Emby server log. Status lines carry a leading symbol that the config page colours on:
    ///   ✔ ok · ⚠ warning · ✖ error · – skipped / informational
    /// Debug lines are written only when Extended log is enabled and never go to the server log.
    ///
    /// Every line, Debug lines included whatever Extended log is set to, also goes to the plugin's
    /// own log file (<see cref="PluginLog"/>), tagged with the run ("[Sync #a1b2]"). A RunLog with
    /// no file source (a preview) writes no file lines.
    /// </summary>
    internal sealed class RunLog
    {
        public const string RuleLine = "══════════════════════════════════════════════════";

        private readonly List<string> _sink;
        private readonly ILogger? _logger;
        private readonly string _serverPrefix;
        private readonly string? _fileSource;

        public bool Extended { get; }

        private readonly RunProgress? _progress;

        /// <param name="progress">The running task's live step (shown next to "Running" on the page);
        /// null for logs that are not a real run (previews, restore).</param>
        /// <param name="fileSource">The run's name in the plugin's log file; by default the logger's,
        /// none without a logger.</param>
        public RunLog(List<string> sink, PluginLogger? logger, string serverLogPrefix, bool extended, RunProgress? progress = null, string? fileSource = null)
        {
            _sink = sink;
            // Lines reach the plugin's log file from here; the logger only takes them to Emby's log.
            _logger = logger?.Emby;
            var source = fileSource ?? logger?.Source;
            _fileSource = source == null ? null : $"{source} #{Guid.NewGuid().ToString("N").Substring(0, 4)}";
            _serverPrefix = string.IsNullOrEmpty(serverLogPrefix) ? "" : serverLogPrefix + " ";
            Extended = extended;
            _progress = progress;
            _progress?.Clear();
        }

        // ── Live step (what the page shows while a run is busy) ─────────────────────

        /// <summary>Sets the current step now, e.g. "Collections — IMDB Top 250".</summary>
        public void Step(string text) => _progress?.Set(text);

        /// <summary>Updates the step from inside a loop, e.g. "Playlists — 34/88 users";
        /// written at most four times a second (the last item of a loop always shows).</summary>
        public void Count(string label, int done, int total, string unit = "") => _progress?.Count(label, done, total, unit);

        // ── Status lines (always written) ────────────────────────────────────────

        /// <summary>Neutral line, no symbol. Used for headers, step lines and plain facts.
        /// A phase header ("» Playlists  ·  …") also becomes the live step ("Playlists").</summary>
        public void Info(string message)
        {
            if (_progress != null && message.StartsWith("» ", StringComparison.Ordinal))
            {
                var phase = message.Substring(2);
                int dot = phase.IndexOf("  ·  ", StringComparison.Ordinal);
                _progress.Set((dot >= 0 ? phase.Substring(0, dot) : phase).Trim());
            }
            Write(message, LogSeverity.Info);
        }

        /// <summary>Something completed successfully.</summary>
        public void Ok(string message) => Write("  ✔ " + message, LogSeverity.Info);

        /// <summary>Something needs the user's attention but the run continued.</summary>
        public void Warn(string message) => Write("  ⚠ " + message, LogSeverity.Warn);

        /// <summary>Something failed.</summary>
        public void Error(string message) => Write("  ✖ " + message, LogSeverity.Error);

        /// <summary>Something was deliberately not done (schedule, disabled, nothing to do).</summary>
        public void Skip(string message) => Write("  – " + message, LogSeverity.Info);

        /// <summary>Indented sub-line under a status line (e.g. a list of titles).</summary>
        public void Detail(string message) => Write("      " + message, LogSeverity.Info);

        public void Blank() => Write("", LogSeverity.None);

        public void Rule() => Write(RuleLine, LogSeverity.Info);

        // ── Debug lines (Extended log only) ───────────────────────────────────────

        public void Debug(string message)
        {
            ToFile("DBG", message);
            if (!Extended) return;
            Add($"[{DateTime.Now:HH:mm:ss}] [DEBUG] {message}");
        }

        /// <summary>Debug section divider, e.g. "── Tags ────────".</summary>
        public void Section(string title)
        {
            const int width = 50;
            var head = "── " + title + " ";
            Debug(head.Length >= width ? head : head + new string('─', width - head.Length));
        }

        // ── Internals ────────────────────────────────────────────────────────────

        private enum LogSeverity { None, Info, Warn, Error }

        private void Write(string line, LogSeverity severity)
        {
            Add($"[{DateTime.Now:HH:mm:ss}] {line}");
            if (severity != LogSeverity.None)
                ToFile(severity == LogSeverity.Error ? "ERR" : severity == LogSeverity.Warn ? "WRN" : "INF", line);
            if (_logger == null || severity == LogSeverity.None) return;
            var server = _serverPrefix + line.Trim();
            switch (severity)
            {
                case LogSeverity.Error: _logger.Error(server); break;
                case LogSeverity.Warn:  _logger.Warn(server);  break;
                default:                _logger.Info(server);  break;
            }
        }

        private void Add(string formatted)
        {
            lock (_sink) { _sink.Add(formatted); }
        }

        private void ToFile(string level, string line)
        {
            if (_fileSource != null) PluginLog.Write(level, _fileSource, line.TrimEnd());
        }

        // ── Small formatting helpers shared by the tasks ─────────────────────────

        public static string Plural(int count, string singular, string? plural = null)
            => count == 1 ? $"{count} {singular}" : $"{count} {plural ?? singular + "s"}";

        public static string Elapsed(TimeSpan span)
        {
            if (span.TotalMinutes >= 1) return $"{(int)span.TotalMinutes}m {span.Seconds}s";
            if (span.TotalSeconds >= 10) return $"{(int)span.TotalSeconds}s";
            return $"{span.TotalSeconds:0.0}s";
        }
    }

    /// <summary>
    /// The step a running task is on, read by the Status endpoints while a run is busy. One per
    /// task (see each task's static <c>Progress</c>). Set from the run's <see cref="RunLog"/>:
    /// phase headers set it, long loops update the count. Cleared when the run's log is saved.
    /// </summary>
    internal sealed class RunProgress
    {
        private volatile string _step = "";
        private long _lastCountTicks;

        public string Step => _step;

        public void Set(string text) { _step = text ?? ""; Interlocked.Exchange(ref _lastCountTicks, DateTime.UtcNow.Ticks); }

        public void Clear() => _step = "";

        public void Count(string label, int done, int total, string unit = "")
        {
            long now = DateTime.UtcNow.Ticks;
            if (done < total && now - Interlocked.Read(ref _lastCountTicks) < TimeSpan.TicksPerMillisecond * 250) return;
            Interlocked.Exchange(ref _lastCountTicks, now);
            _step = $"{label} {done}/{total}{(unit.Length > 0 ? " " + unit : "")}";
        }
    }
}
