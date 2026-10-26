using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Library;
using MediaBrowser.Controller.Plugins;
using MediaBrowser.Model.Events;
using MediaBrowser.Model.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace HomeScreenCompanion
{
    /// <summary>
    /// A user created in Emby sees the plugin's libraries (top-list libraries in My Media and
    /// Latest, a show list's episode copies in Continue Watching and Next Up) until the next
    /// top-list section sync hides them. This hides them as soon as the user is made: shortly after
    /// UserCreated, and once more a minute later, as Emby's new-user page saves the user's settings
    /// in steps after creating it. Only hiding is done here; library access stays with the sync.
    /// </summary>
    public sealed class NewUserWatcher : IServerEntryPoint
    {
        private static readonly TimeSpan FirstDelay = TimeSpan.FromSeconds(5);
        private static readonly TimeSpan SecondDelay = TimeSpan.FromMinutes(1);

        private readonly IUserManager _userManager;
        private readonly ILibraryManager _libraryManager;
        private readonly PluginLogger _logger;
        private readonly object _lock = new object();
        private readonly List<Timer> _timers = new List<Timer>();

        public NewUserWatcher(IUserManager userManager, ILibraryManager libraryManager, ILogManager logManager)
        {
            _userManager = userManager;
            _libraryManager = libraryManager;
            _logger = PluginLog.Wrap(logManager.GetLogger("HomeScreenCompanion_NewUser"), "New user");
        }

        public void Run() => _userManager.UserCreated += OnUserCreated;

        private void OnUserCreated(object? sender, GenericEventArgs<User> e)
        {
            var name = e?.Argument?.Name ?? "?";
            Schedule(name, FirstDelay);
            Schedule(name, SecondDelay);
        }

        private void Schedule(string userName, TimeSpan delay)
        {
            lock (_lock)
            {
                Timer? timer = null;
                timer = new Timer(_ =>
                {
                    lock (_lock) { _timers.Remove(timer!); }
                    timer!.Dispose();
                    Hide(userName);
                }, null, Timeout.Infinite, Timeout.Infinite);
                _timers.Add(timer);
                timer.Change(delay, Timeout.InfiniteTimeSpan);
            }
        }

        private void Hide(string userName)
        {
            try
            {
                var config = Plugin.Instance?.Configuration;
                if (config == null) return;
                bool any = (config.TopLists ?? new List<TopListHomeSection>())
                    .Any(t => !string.IsNullOrEmpty(t.HomeSectionLibraryId) && t.HomeSectionLibraryId != "auto")
                    || RankedCollections.LibraryIds(_libraryManager).Any();
                if (!any) return;
                int changed = TopListSyncTask.HideLibraries(config, _userManager, _libraryManager, _logger, m => _logger.Warn(m));
                if (changed > 0)
                    _logger.Info($"New user '{userName}': HSC libraries {(config.HideTopListLibraries ? "hidden from" : "shown in")} My Media and Latest");
            }
            catch (Exception ex)
            {
                _logger.ErrorException($"New user '{userName}': HSC libraries could not be hidden", ex);
            }
        }

        public void Dispose()
        {
            _userManager.UserCreated -= OnUserCreated;
            lock (_lock)
            {
                foreach (var t in _timers) t.Dispose();
                _timers.Clear();
            }
        }
    }
}
