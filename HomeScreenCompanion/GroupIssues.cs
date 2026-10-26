using MediaBrowser.Model.Serialization;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace HomeScreenCompanion
{
    /// <summary>A group's warnings or error from the last run that included it.</summary>
    public class GroupIssue
    {
        public string Name { get; set; } = "";
        public string Tag { get; set; } = "";
        public string Level { get; set; } = "";   // "warning" or "error"
        public List<string> Messages { get; set; } = new List<string>();
        public DateTime RunUtc { get; set; }
    }

    /// <summary>
    /// The warnings / errors each group got on its last run (full sync or Run Group), shown as a
    /// flag on the group's card. Kept in &lt;plugin data&gt;/logs/group-issues.json so it survives a
    /// restart. A group that runs cleanly loses its entry; a skipped group keeps the one it had.
    /// </summary>
    internal static class GroupIssues
    {
        private static readonly object _lock = new object();
        private static Dictionary<string, GroupIssue> _byKey = new Dictionary<string, GroupIssue>(StringComparer.OrdinalIgnoreCase);
        private static string? _path;
        private static IJsonSerializer? _json;

        internal static string Key(string? name, string? tag) => (name ?? "").Trim() + "\x1F" + (tag ?? "").Trim();

        internal static void Initialize(string dataPath, IJsonSerializer json)
        {
            _path = Path.Combine(dataPath, "logs", "group-issues.json");
            _json = json;
            try
            {
                if (!File.Exists(_path)) return;
                var list = json.DeserializeFromString<List<GroupIssue>>(File.ReadAllText(_path)) ?? new List<GroupIssue>();
                lock (_lock)
                {
                    _byKey = new Dictionary<string, GroupIssue>(StringComparer.OrdinalIgnoreCase);
                    foreach (var i in list) _byKey[Key(i.Name, i.Tag)] = i;
                }
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
        }

        internal static List<GroupIssue> All()
        {
            lock (_lock) return _byKey.Values.ToList();
        }

        /// <summary>
        /// Records one run's results. <paramref name="results"/> holds the groups that ran (null =
        /// ran cleanly); groups not in it keep their entry. <paramref name="liveKeys"/>, when given,
        /// drops entries of groups that no longer exist.
        /// </summary>
        internal static void Record(IEnumerable<(string Name, string Tag, GroupIssue? Issue)> results, IEnumerable<string>? liveKeys = null)
        {
            lock (_lock)
            {
                foreach (var (name, tag, issue) in results)
                {
                    var key = Key(name, tag);
                    // Remove first so a renamed group's entry takes the new casing of its key.
                    _byKey.Remove(key);
                    if (issue != null) { issue.Name = (name ?? "").Trim(); issue.Tag = (tag ?? "").Trim(); _byKey[key] = issue; }
                }
                if (liveKeys != null)
                {
                    var live = new HashSet<string>(liveKeys, StringComparer.OrdinalIgnoreCase);
                    foreach (var key in _byKey.Keys.Where(k => !live.Contains(k)).ToList()) _byKey.Remove(key);
                }
                Save();
            }
        }

        private static void Save()
        {
            if (_path == null || _json == null) return;
            try
            {
                Directory.CreateDirectory(Path.GetDirectoryName(_path)!);
                var temp = _path + ".tmp";
                File.WriteAllText(temp, _json.SerializeToString(_byKey.Values.ToList()));
                if (File.Exists(_path)) File.Replace(temp, _path, null);
                else File.Move(temp, _path);
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
        }
    }
}
