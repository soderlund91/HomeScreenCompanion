using MediaBrowser.Model.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;

namespace HomeScreenCompanion
{
    /// <summary>
    /// The plugin's own log file, for troubleshooting ("Download logs" on the log modal):
    /// &lt;plugin data&gt;/logs/hsc-YYYY-MM-DD.log, one file per day, kept for <see cref="KeepDays"/> days
    /// and capped at <see cref="MaxBytesPerDay"/> a day.
    ///
    /// It gets everything: every run log line (also the Extended-log lines the page only shows
    /// with Extended log on), every line the plugin writes to Emby's log (through
    /// <see cref="Wrap"/>) and the exceptions caught and otherwise ignored (<see cref="Swallowed"/>).
    /// The page's log is not changed by it.
    ///
    /// Lines are queued and appended by a timer once a second, so a busy sync never waits on the
    /// disk. Failures are swallowed: logging must never break a sync.
    /// </summary>
    internal static class PluginLog
    {
        internal const int KeepDays = 7;
        internal const long MaxBytesPerDay = 25L * 1024 * 1024;
        private const int MaxQueued = 100000;
        // A swallowed exception from one place is written at most this often a day.
        private const int MaxSwallowedPerSite = 20;

        private static readonly ConcurrentQueue<string> _queue = new ConcurrentQueue<string>();
        private static readonly object _writeLock = new object();
        private static readonly ConcurrentDictionary<string, int> _swallowedToday = new ConcurrentDictionary<string, int>();
        private static string _swallowedDay = "";
        private static string? _dir;
        private static Timer? _timer;
        private static string _prunedDay = "";
        private static readonly HashSet<string> _fullDays = new HashSet<string>();

        internal static string? LogDirectory => _dir;

        internal static void Initialize(string dataPath)
        {
            if (_dir != null) return;
            try
            {
                _dir = Path.Combine(dataPath, "logs");
                System.IO.Directory.CreateDirectory(_dir);
                _timer ??= new Timer(_ => Flush(), null, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1));
                Write("INF", "Plugin", $"Home Screen Companion {Plugin.Instance?.Version} started");
            }
            catch { }
        }

        /// <summary>One line: "2026-10-16 14:03:22.123 [INF] [Sync #a1b2] message".</summary>
        internal static void Write(string level, string source, string message)
        {
            if (_queue.Count >= MaxQueued) return;
            var text = (message ?? "").Replace("\r\n", "\n").Replace("\n", "\n    ");
            _queue.Enqueue($"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} [{level}] [{source}] {text}");
        }

        /// <summary>
        /// An exception caught on purpose and otherwise ignored. Written with where it happened, at
        /// most <see cref="MaxSwallowedPerSite"/> times a day per place, so a loop over thousands of
        /// items cannot fill the log.
        /// </summary>
        internal static void Swallowed(Exception ex,
            [CallerFilePath] string file = "", [CallerMemberName] string member = "", [CallerLineNumber] int line = 0)
        {
            try
            {
                var day = DateTime.Now.ToString("yyyy-MM-dd");
                if (_swallowedDay != day) { _swallowedToday.Clear(); _swallowedDay = day; }
                var site = $"{Path.GetFileNameWithoutExtension(file.Replace('\\', '/').Split('/').Last())}.{member}:{line}";
                int n = _swallowedToday.AddOrUpdate(site, 1, (_, c) => c + 1);
                if (n > MaxSwallowedPerSite + 1) return;
                if (n == MaxSwallowedPerSite + 1)
                {
                    Write("CAT", "Caught", $"{site} — more of these today are not logged");
                    return;
                }
                Write("CAT", "Caught", $"{site} — {Describe(ex)}");
            }
            catch { }
        }

        /// <summary>"Type: message", with the inner exception and the details some exceptions keep in
        /// properties of their own (a SQLiteException's message says nothing: its text and code are in
        /// ErrorMessage / ExtendedErrorCode).</summary>
        internal static string Describe(Exception ex)
        {
            var inner = ex.GetBaseException();
            var what = inner == ex ? $"{ex.GetType().Name}: {ex.Message}" : $"{ex.GetType().Name}: {ex.Message} ← {inner.GetType().Name}: {inner.Message}";
            var details = new List<string>();
            foreach (var name in new[] { "ErrorMessage", "ExtendedErrorCode", "ErrorCode" })
            {
                try
                {
                    var value = inner.GetType().GetProperty(name)?.GetValue(inner)?.ToString();
                    if (!string.IsNullOrEmpty(value) && !details.Any(d => d.EndsWith(" " + value))) details.Add($"{name} {value}");
                }
                catch { }
            }
            return details.Count == 0 ? what : $"{what} ({string.Join(", ", details)})";
        }

        /// <summary>Wraps an Emby logger so whatever the plugin writes to it also lands in the plugin's own log.</summary>
        internal static PluginLogger Wrap(ILogger inner, string source) => new PluginLogger(inner, source);

        /// <summary>Writes what is queued. Also called before the logs are downloaded.</summary>
        internal static void Flush()
        {
            if (_dir == null || _queue.IsEmpty) return;
            lock (_writeLock)
            {
                try
                {
                    var byDay = new Dictionary<string, StringBuilder>();
                    while (_queue.TryDequeue(out var line))
                    {
                        var day = line.Length >= 10 ? line.Substring(0, 10) : DateTime.Now.ToString("yyyy-MM-dd");
                        if (!byDay.TryGetValue(day, out var sb)) byDay[day] = sb = new StringBuilder();
                        sb.Append(line).Append('\n');
                    }
                    foreach (var kv in byDay)
                    {
                        var path = Path.Combine(_dir, "hsc-" + kv.Key + ".log");
                        long size = File.Exists(path) ? new FileInfo(path).Length : 0;
                        if (size >= MaxBytesPerDay)
                        {
                            if (_fullDays.Add(kv.Key))
                                File.AppendAllText(path, $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} [WRN] [Plugin] The log for this day reached {MaxBytesPerDay / (1024 * 1024)} MB — later lines of the day are not written\n", Encoding.UTF8);
                            continue;
                        }
                        File.AppendAllText(path, kv.Value.ToString(), Encoding.UTF8);
                    }
                    Prune();
                }
                catch { }
            }
        }

        /// <summary>The log files kept, oldest first.</summary>
        internal static List<string> Files()
        {
            try
            {
                if (_dir == null || !System.IO.Directory.Exists(_dir)) return new List<string>();
                return System.IO.Directory.GetFiles(_dir, "hsc-*.log").OrderBy(f => f, StringComparer.Ordinal).ToList();
            }
            catch { return new List<string>(); }
        }

        // Once a day: the files older than KeepDays go.
        private static void Prune()
        {
            var today = DateTime.Now.ToString("yyyy-MM-dd");
            if (_prunedDay == today || _dir == null) return;
            _prunedDay = today;
            var oldest = DateTime.Now.Date.AddDays(-(KeepDays - 1)).ToString("yyyy-MM-dd");
            foreach (var f in System.IO.Directory.GetFiles(_dir, "hsc-*.log"))
            {
                var day = Path.GetFileNameWithoutExtension(f).Substring(4);
                if (string.CompareOrdinal(day, oldest) < 0)
                    try { File.Delete(f); } catch { }
            }
        }
    }

    /// <summary>
    /// The plugin's logger: each line goes to Emby's log and to the plugin's own log file.
    /// Not an implementation of Emby's ILogger on purpose: Emby adds members to that interface
    /// between versions, and a class missing one fails to load.
    /// </summary>
    internal sealed class PluginLogger
    {
        /// <summary>Emby's own logger (lines written to it reach Emby's log only).</summary>
        public readonly ILogger Emby;
        /// <summary>The name lines carry in the plugin's log file.</summary>
        public readonly string Source;

        public PluginLogger(ILogger emby, string source) { Emby = emby; Source = source; }

        public void Info(string message, params object[] args) { var m = Format(message, args); PluginLog.Write("INF", Source, m); Emby.Info(m); }
        public void Warn(string message, params object[] args) { var m = Format(message, args); PluginLog.Write("WRN", Source, m); Emby.Warn(m); }
        public void Error(string message, params object[] args) { var m = Format(message, args); PluginLog.Write("ERR", Source, m); Emby.Error(m); }
        public void ErrorException(string message, Exception exception, params object[] args)
        {
            var m = Format(message, args);
            PluginLog.Write("ERR", Source, m + Environment.NewLine + exception);
            Emby.ErrorException(m, exception);
        }

        // Emby formats with string.Format only when arguments are given; a message with braces and no
        // arguments is written as it is.
        private static string Format(string message, object[] args)
        {
            if (args == null || args.Length == 0) return message ?? "";
            try { return string.Format(message ?? "", args); } catch { return message ?? ""; }
        }
    }
}
