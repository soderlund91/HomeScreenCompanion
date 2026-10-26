using MediaBrowser.Model.Serialization;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace HomeScreenCompanion
{
    /// <summary>
    /// The last run of a task as stored on disk, so the log modal and "Last run" survive a
    /// server restart.
    /// </summary>
    public class PersistedRunLog
    {
        public List<string> Lines { get; set; } = new List<string>();
        public string Status { get; set; } = "";
        public DateTime? StartedUtc { get; set; }
        public Dictionary<string, string> Extra { get; set; } = new Dictionary<string, string>();
    }

    /// <summary>
    /// Keeps each task's latest run log in &lt;plugin data&gt;/logs/&lt;key&gt;.json. Only the latest
    /// run is kept (same as the in-memory log), capped so an extended log can't grow unbounded.
    /// Failures are swallowed: persisting the log must never break a sync.
    /// </summary>
    internal static class LogStore
    {
        internal const string Sync = "sync";
        internal const string HomeScreen = "hsc";
        internal const string TopLists = "toplists";

        // Status saved while a run was in progress (the server stopped mid-run).
        internal const string InterruptedStatus = "Interrupted (server restarted)";

        private const int MaxLines = 20000;
        private static readonly object _ioLock = new object();
        private static string? _dir;
        private static IJsonSerializer? _json;

        internal static void Initialize(string dataPath, IJsonSerializer json)
        {
            _dir = Path.Combine(dataPath, "logs");
            _json = json;
        }

        internal static void Save(string key, List<string> sink, string status, DateTime? startedUtc,
            Dictionary<string, string>? extra = null)
        {
            if (_dir == null || _json == null) return;
            try
            {
                List<string> lines;
                lock (sink) { lines = sink.Count > MaxLines ? sink.Skip(sink.Count - MaxLines).ToList() : sink.ToList(); }
                var record = new PersistedRunLog
                {
                    Lines = lines,
                    Status = status ?? "",
                    StartedUtc = startedUtc,
                    Extra = extra ?? new Dictionary<string, string>()
                };
                var text = _json.SerializeToString(record);
                lock (_ioLock)
                {
                    Directory.CreateDirectory(_dir);
                    var path = Path.Combine(_dir, key + ".json");
                    var temp = path + ".tmp";
                    File.WriteAllText(temp, text);
                    // Write-then-swap so a crash mid-write never leaves a half-written file.
                    if (File.Exists(path)) File.Replace(temp, path, null);
                    else File.Move(temp, path);
                }
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
        }

        internal static PersistedRunLog? Load(string key)
        {
            if (_dir == null || _json == null) return null;
            try
            {
                var path = Path.Combine(_dir, key + ".json");
                if (!File.Exists(path)) return null;
                string text;
                lock (_ioLock) { text = File.ReadAllText(path); }
                return _json.DeserializeFromString<PersistedRunLog>(text);
            }
            catch { return null; }
        }

        // Status to show after a restart: a run that never finished is reported as interrupted.
        internal static string RestoredStatus(string status) =>
            string.Equals(status, "Running...", StringComparison.Ordinal) ? InterruptedStatus : status;
    }
}
