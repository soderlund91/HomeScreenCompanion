using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Library;
using System;
using System.Collections.Generic;
using System.Linq;

namespace HomeScreenCompanion
{
    /// <summary>
    /// The libraries a top-list takes its titles from (TagConfig.TopListLibraryIds, copied to
    /// TopListHomeSection.LibraryIds; empty = all, also libraries added later). A source's list is
    /// filtered by every run before Max items, and the saved titles (TopListMembers) stay unfiltered,
    /// so changing the choice needs no new fetch. A manual list's choice only narrows the page's search.
    /// A title belongs to a library by its path: asking Emby per title is too slow for the search list.
    /// </summary>
    internal sealed class TopListLibraries
    {
        private readonly List<string>? _roots;

        /// <summary>Libraries were chosen but none of them exists (or they could not be read): the list keeps what it has.</summary>
        internal bool Gone { get; }

        private TopListLibraries(List<string>? roots, bool gone)
        {
            _roots = roots;
            Gone = gone;
        }

        private static string Norm(string path) => path.Replace('\\', '/').TrimEnd('/') + "/";

        internal static TopListLibraries For(IEnumerable<string>? ids, ILibraryManager libraryManager)
        {
            var wanted = new HashSet<string>((ids ?? Enumerable.Empty<string>()).Where(i => !string.IsNullOrWhiteSpace(i)).Select(i => i.Trim()), StringComparer.OrdinalIgnoreCase);
            if (wanted.Count == 0) return new TopListLibraries(null, false);
            try
            {
                var folders = libraryManager.GetVirtualFolders().Where(f => !string.IsNullOrEmpty(f.ItemId) && wanted.Contains(f.ItemId)).ToList();
                var roots = folders.SelectMany(f => f.Locations ?? Array.Empty<string>()).Where(l => !string.IsNullOrEmpty(l)).Select(Norm).ToList();
                return new TopListLibraries(roots, folders.Count == 0);
            }
            catch (Exception swallowed)
            {
                PluginLog.Swallowed(swallowed);
                return new TopListLibraries(new List<string>(), true);
            }
        }

        /// <summary>What a run filters by: a source's list only, a manual list is what was picked for it.</summary>
        internal static TopListLibraries ForRun(TopListHomeSection tl, ILibraryManager libraryManager) =>
            For(tl.FromSource ? tl.LibraryIds : null, libraryManager);

        /// <summary>Whether the title is in the chosen libraries. Pass a movie or series: seasons can lack a path.</summary>
        internal bool Contains(BaseItem item)
        {
            if (_roots == null) return true;
            if (string.IsNullOrEmpty(item.Path)) return false;
            var path = Norm(item.Path);
            return _roots.Any(r => path.StartsWith(r, StringComparison.OrdinalIgnoreCase));
        }
    }
}
