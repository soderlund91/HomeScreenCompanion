using MediaBrowser.Controller.Library;
using MediaBrowser.Model.Querying;
using MediaBrowser.Model.Users;
using System;
using System.Collections.Generic;

namespace HomeScreenCompanion
{
    /// <summary>
    /// Users switched off in Emby (disabled). Everything HSC does per user — Next Watch picks,
    /// playlists, home rows, top-list rows, Copy &amp; Sync — leaves them out. They stay selected and
    /// nothing of theirs is changed or removed, so switching one on again brings it back on the
    /// next sync.
    /// </summary>
    internal sealed class UserStatus
    {
        private readonly HashSet<string> _disabled = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, string> _names = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        private readonly SortedSet<string> _skipped = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);

        private UserStatus() { }

        /// <summary>The disabled users right now (read once per run).</summary>
        internal static UserStatus Read(IUserManager userManager)
        {
            var s = new UserStatus();
            try
            {
                foreach (var u in userManager.GetUserList(new UserQuery { IsDisabled = true }))
                {
                    s._disabled.Add(u.Id.ToString("N"));
                    s._names[u.Id.ToString("N")] = u.Name ?? "";
                }
            }
            catch (Exception ex) { PluginLog.Swallowed(ex); }
            return s;
        }

        private static string Norm(string? userId) => Guid.TryParse(userId, out var g) ? g.ToString("N") : (userId ?? "").Trim();

        internal bool IsDisabled(string? userId) => _disabled.Contains(Norm(userId));

        /// <summary>True for a disabled user, who is then noted for <see cref="Skipped"/>.</summary>
        internal bool Skip(string? userId)
        {
            var id = Norm(userId);
            if (!_disabled.Contains(id)) return false;
            _skipped.Add(_names.TryGetValue(id, out var n) && n.Length > 0 ? n : id);
            return true;
        }

        /// <summary>The disabled users skipped so far, by name.</summary>
        internal IReadOnlyCollection<string> Skipped => _skipped;

        /// <summary>The summary line for a run's log; null when no disabled user was skipped.</summary>
        internal string? SummaryLine()
            => _skipped.Count == 0 ? null : $"Disabled in Emby, so skipped (nothing of theirs was changed): {string.Join(", ", _skipped)}";
    }
}
