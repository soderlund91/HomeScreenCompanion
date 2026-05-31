using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Library;
using MediaBrowser.Controller.Plugins;
using MediaBrowser.Model.Logging;
using System;
using System.Threading.Tasks;

namespace HomeScreenCompanion
{
    public class LibraryAccessManager : IServerEntryPoint
    {
        private readonly ILibraryManager _libraryManager;
        private readonly IUserManager _userManager;
        private readonly ILogger _logger;

        public LibraryAccessManager(
            ILibraryManager libraryManager,
            IUserManager userManager,
            ILogManager logManager)
        {
            _libraryManager = libraryManager;
            _userManager = userManager;
            _logger = logManager.GetLogger("HomeScreenCompanion_LibraryAccess");
        }

        public void Run()
        {
            _libraryManager.ItemAdded += OnItemAdded;
        }

        public void Dispose()
        {
            _libraryManager.ItemAdded -= OnItemAdded;
        }

        private void OnItemAdded(object sender, ItemChangeEventArgs e)
        {
            if (!(e.Item is CollectionFolder)) return;

            var libName = e.Item.Name;
            _logger.Info($"[LibraryAccess] New library '{libName}' (InternalId={e.Item.InternalId}) detected. Scheduling access sync in 30s (waiting for Emby async policy restore to complete)...");

            // Emby runs an async policy restoration after library creation.
            // We wait 30 seconds to ensure Emby has finished before we apply
            // our selective grant/revoke logic.
            Task.Delay(30000).ContinueWith(_ =>
            {
                var config = Plugin.Instance?.Configuration;
                if (config?.TopLists == null || config.TopLists.Count == 0) return;
                try
                {
                    TopListSyncTask.GrantTopListLibraryAccess(config.TopLists, _userManager, _libraryManager, _logger);
                }
                catch (Exception ex)
                {
                    _logger.Error($"[LibraryAccess] Access sync failed: {ex.GetBaseException().Message}");
                }
            });
        }
    }
}
