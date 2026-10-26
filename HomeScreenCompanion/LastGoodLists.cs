using MediaBrowser.Model.Serialization;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace HomeScreenCompanion
{
    /// <summary>
    /// "Preserve tags and collections on empty result", per URL: the entries each External list URL
    /// gave on its last read that had any (plugin data folder, last_good_lists/). When a URL comes
    /// back empty (unreachable, removed, made private) its last entries stand in for it, so only
    /// that URL's titles are kept: the group's other URLs are still synced in full, and titles that
    /// left them are removed. Without this, one dead URL froze its whole group (nothing was ever
    /// removed from the tag, collection or playlist again).
    /// </summary>
    internal static class LastGoodLists
    {
        private sealed class Entry
        {
            public string Url { get; set; } = "";
            public DateTime SavedUtc { get; set; }
            public List<ExternalItemDto> Items { get; set; } = new List<ExternalItemDto>();
        }

        private static readonly object Gate = new object();

        private static string Folder => Path.Combine(Plugin.Instance!.DataFolderPath, "last_good_lists");

        private static string FileFor(string url)
        {
            using (var sha = SHA1.Create())
            {
                var hash = sha.ComputeHash(Encoding.UTF8.GetBytes(url.Trim()));
                return Path.Combine(Folder, string.Concat(hash.Select(b => b.ToString("x2"))) + ".json");
            }
        }

        /// <summary>Remembers a read that returned entries.</summary>
        internal static void Save(string url, List<ExternalItemDto> items, IJsonSerializer json)
        {
            if (string.IsNullOrWhiteSpace(url) || items == null || items.Count == 0) return;
            try
            {
                lock (Gate)
                {
                    Directory.CreateDirectory(Folder);
                    json.SerializeToFile(new Entry { Url = url.Trim(), SavedUtc = DateTime.UtcNow, Items = items.ToList() }, FileFor(url));
                }
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
        }

        /// <summary>The entries of the URL's last read that had any; null when there is none.</summary>
        internal static (List<ExternalItemDto> Items, DateTime SavedUtc)? Load(string url, IJsonSerializer json)
        {
            if (string.IsNullOrWhiteSpace(url)) return null;
            try
            {
                lock (Gate)
                {
                    var file = FileFor(url);
                    if (!File.Exists(file)) return null;
                    var entry = json.DeserializeFromFile<Entry>(file);
                    if (entry?.Items == null || entry.Items.Count == 0) return null;
                    return (entry.Items, entry.SavedUtc);
                }
            }
            catch { return null; }
        }

        /// <summary>Drops the files of URLs no source uses any more.</summary>
        internal static void Prune(IEnumerable<string> urlsInUse)
        {
            try
            {
                lock (Gate)
                {
                    if (!Directory.Exists(Folder)) return;
                    var keep = new HashSet<string>(urlsInUse.Where(u => !string.IsNullOrWhiteSpace(u)).Select(FileFor), StringComparer.OrdinalIgnoreCase);
                    foreach (var f in Directory.GetFiles(Folder, "*.json"))
                        if (!keep.Contains(f)) File.Delete(f);
                }
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
        }
    }
}
