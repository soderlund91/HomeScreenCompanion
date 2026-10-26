using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace HomeScreenCompanion
{
    /// <summary>
    /// When a home row's Position setting is changed, the rows already there move to it; otherwise a
    /// row keeps the place it has (a user may have moved it in Emby). Kept per source and top-list:
    /// the position last put into effect. A settings save notes the old position of a changed one
    /// (<see cref="NoteChanges"/>); the next run moves the rows and records the new one.
    /// Plugin data file home_row_position_settings.txt, one "key TAB position" per line.
    /// </summary>
    internal static class RowPlacement
    {
        private static readonly object _lock = new object();
        private static Dictionary<string, string>? _applied;

        private static string FilePath => Path.Combine(Plugin.Instance!.DataFolderPath, "home_row_position_settings.txt");

        internal static string SourceKey(TagConfig tc) => "src\u001F" + GroupIssues.Key(tc.Name, tc.Tag);
        internal static string TopListKey(TopListHomeSection tl) => "tl\u001F" + (tl.TagName ?? "").Trim().ToLowerInvariant();

        /// <summary>The "_position" in a row's settings JSON ("" when none).</summary>
        internal static string PositionIn(string? settingsJson)
        {
            var json = settingsJson ?? "";
            int at = json.IndexOf("\"_position\"", StringComparison.Ordinal);
            if (at < 0) return "";
            int colon = json.IndexOf(':', at);
            int q1 = colon < 0 ? -1 : json.IndexOf('"', colon + 1);
            if (q1 < 0) return "";
            var sb = new StringBuilder();
            for (int i = q1 + 1; i < json.Length; i++)
            {
                char c = json[i];
                if (c == '\\' && i + 1 < json.Length) { sb.Append(json[++i]); continue; }
                if (c == '"') break;
                sb.Append(c);
            }
            return sb.ToString();
        }

        private static Dictionary<string, string> Applied()
        {
            if (_applied != null) return _applied;
            _applied = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            try
            {
                if (File.Exists(FilePath))
                    foreach (var line in File.ReadAllLines(FilePath, Encoding.UTF8))
                    {
                        int tab = line.IndexOf('\t');
                        if (tab > 0) _applied[line.Substring(0, tab)] = line.Substring(tab + 1);
                    }
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            return _applied;
        }

        private static void Save()
        {
            try
            {
                File.WriteAllLines(FilePath, Applied().Select(kv => kv.Key + "\t" + kv.Value.Replace("\t", " ").Replace("\n", " ")), new UTF8Encoding(false));
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
        }

        /// <summary>
        /// Settings saved: a source or top-list whose Position changed and has no position recorded yet
        /// gets the old one recorded, so the next run sees the change and moves its rows.
        /// </summary>
        internal static void NoteChanges(PluginConfiguration? old, PluginConfiguration? saved)
        {
            if (old == null || saved == null) return;
            try
            {
                var before = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                foreach (var t in old.Tags ?? new List<TagConfig>()) before[SourceKey(t)] = PositionIn(t.HomeSectionSettings);
                foreach (var t in old.TopLists ?? new List<TopListHomeSection>()) before[TopListKey(t)] = PositionIn(t.HomeSectionSettings);
                bool changed = false;
                lock (_lock)
                {
                    var applied = Applied();
                    void Note(string key, string now)
                    {
                        if (applied.ContainsKey(key) || !before.TryGetValue(key, out var was) || was == now) return;
                        applied[key] = was;
                        changed = true;
                    }
                    foreach (var t in saved.Tags ?? new List<TagConfig>()) Note(SourceKey(t), PositionIn(t.HomeSectionSettings));
                    foreach (var t in saved.TopLists ?? new List<TopListHomeSection>()) Note(TopListKey(t), PositionIn(t.HomeSectionSettings));
                    if (changed) Save();
                }
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
        }

        /// <summary>
        /// True when the rows already there should move to <paramref name="position"/>: the setting
        /// changed since it was last put into effect. The first time a source or top-list is seen
        /// nothing moves (its rows may have been moved by hand); a top-list never had a position
        /// before, so one set now counts as a change from the bottom.
        /// </summary>
        internal static bool ShouldMove(string key, string position, bool isTopList)
        {
            lock (_lock)
            {
                var applied = Applied();
                if (applied.TryGetValue(key, out var was)) return was != position;
                return isTopList && position.Length > 0;
            }
        }

        /// <summary>Records the position as put into effect (after the run's rows were placed).</summary>
        internal static void MarkApplied(string key, string position)
        {
            lock (_lock)
            {
                var applied = Applied();
                if (applied.TryGetValue(key, out var was) && was == position) return;
                applied[key] = position;
                Save();
            }
        }
    }
}
