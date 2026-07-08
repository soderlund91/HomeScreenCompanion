using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Library;
using MediaBrowser.Controller.Plugins;
using MediaBrowser.Controller.Providers;
using MediaBrowser.Model.Entities;
using MediaBrowser.Model.IO;
using MediaBrowser.Model.Logging;
using MediaBrowser.Model.Serialization;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace HomeScreenCompanion
{
    public class ServerEntryPoint : IServerEntryPoint
    {
        private readonly ILibraryManager _libraryManager;
        private readonly ILogger _logger;
        private readonly IJsonSerializer _jsonSerializer;
        private readonly IProviderManager _providerManager;
        private readonly IFileSystem _fileSystem;

        private readonly object _strmLock = new object();
        private readonly HashSet<Guid> _processedStrmIds = new HashSet<Guid>();

        public ServerEntryPoint(ILibraryManager libraryManager, ILogManager logManager, IJsonSerializer jsonSerializer, IProviderManager providerManager, IFileSystem fileSystem)
        {
            _libraryManager = libraryManager;
            _logger = logManager.GetLogger("HomeScreenCompanion_RealTime");
            _jsonSerializer = jsonSerializer;
            _providerManager = providerManager;
            _fileSystem = fileSystem;
        }

        public void Run()
        {
            if (Plugin.Instance == null) return;
            RunAutoMigration();
            TagCacheManager.Instance.Initialize(Plugin.Instance.DataFolderPath, _jsonSerializer);

            _libraryManager.ItemAdded += OnItemChanged;
            _libraryManager.ItemUpdated += OnItemChanged;
        }

        private void RunAutoMigration()
        {
            try
            {
                var configDir = Path.GetDirectoryName(Plugin.Instance?.ConfigurationFilePath);
                if (configDir == null) return;
                var oldConfigPath = Path.Combine(configDir, "AutoTag.xml");
                if (!File.Exists(oldConfigPath)) return;

                _logger.Info("[Migration] AutoTag.xml found, starting automatic migration...");

                var oldConfig = Plugin.XmlSerializer.DeserializeFromFile(typeof(PluginConfiguration), oldConfigPath) as PluginConfiguration;
                if (oldConfig == null)
                {
                    _logger.Warn("[Migration] Could not parse AutoTag.xml — skipping migration.");
                    return;
                }

                var cfg = Plugin.Instance!.Configuration;
                cfg.TraktClientId         = oldConfig.TraktClientId;
                cfg.MdblistApiKey         = oldConfig.MdblistApiKey;
                cfg.TmdbApiKey            = oldConfig.TmdbApiKey;
                cfg.ExtendedConsoleOutput = oldConfig.ExtendedConsoleOutput;
                cfg.DryRunMode            = oldConfig.DryRunMode;
                if (oldConfig.Tags?.Count > 0) cfg.Tags = oldConfig.Tags;
                Plugin.Instance.SaveConfiguration();

                var newDataPath = Plugin.Instance.DataFolderPath;
                var oldDataPath = Path.Combine(Path.GetDirectoryName(newDataPath) ?? "", "AutoTag");
                if (Directory.Exists(oldDataPath))
                {
                    Directory.CreateDirectory(newDataPath);
                    var fileRenames = new System.Collections.Generic.Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                    {
                        { "autotag_cache.json",       "homescreencompanion_cache.json" },
                        { "autotag_history.txt",      "homescreencompanion_history.txt" },
                        { "autotag_collections.txt",  "homescreencompanion_collections.txt" }
                    };
                    foreach (var file in Directory.GetFiles(oldDataPath))
                    {
                        var oldName = Path.GetFileName(file);
                        var newName = fileRenames.TryGetValue(oldName, out var renamed) ? renamed : oldName;
                        var dest = Path.Combine(newDataPath, newName);
                        if (!File.Exists(dest)) File.Copy(file, dest);
                    }
                    var oldImages = Path.Combine(oldDataPath, "collection_images");
                    if (Directory.Exists(oldImages))
                    {
                        var newImages = Path.Combine(newDataPath, "collection_images");
                        Directory.CreateDirectory(newImages);
                        foreach (var file in Directory.GetFiles(oldImages))
                        {
                            var dest = Path.Combine(newImages, Path.GetFileName(file));
                            if (!File.Exists(dest)) File.Copy(file, dest);
                        }
                    }
                    Directory.Delete(oldDataPath, true);
                }

                File.Move(oldConfigPath, oldConfigPath + ".old");
                _logger.Info("[Migration] AutoTag migration completed successfully.");
            }
            catch (Exception ex)
            {
                _logger.Error("[Migration] Migration failed: " + ex.Message);
            }
        }

        private void OnItemChanged(object sender, ItemChangeEventArgs e)
        {
            ProcessItem(e.Item);
        }

        private void ProcessItem(BaseItem item)
        {
            if (!(item is MediaBrowser.Controller.Entities.Movies.Movie) && !(item is MediaBrowser.Controller.Entities.TV.Series))
                return;

            if (item.IsVirtualItem) return;

            // Items under the top-list folder are .strm virtual copies. Never tag them, but DO
            // link each one to its original movie (as an alternate version) and force a media
            // probe so it gets a real RunTimeTicks — otherwise resume breaks and the movie is
            // marked fully-watched on stop. This fires as soon as Emby indexes the .strm.
            if (Plugin.Instance != null && !string.IsNullOrEmpty(item.Path))
            {
                var topListsFolder = Path.Combine(Plugin.Instance.DataFolderPath, "toplists") + Path.DirectorySeparatorChar;
                if (item.Path.StartsWith(topListsFolder, StringComparison.OrdinalIgnoreCase))
                {
                    ProcessTopListStrmItem(item);
                    return;
                }
            }

            var ids = item.ProviderIds;
            if (ids == null || ids.Count == 0) return;

            var tagsFound = TagCacheManager.Instance.GetTagsForIds(ids);

            if (tagsFound.Count > 0)
            {
                bool changed = false;
                foreach (var tag in tagsFound)
                {
                    if (!item.Tags.Contains(tag, System.StringComparer.OrdinalIgnoreCase))
                    {
                        item.AddTag(tag);
                        changed = true;
                        _logger.Info($"[Real-Time] Automatically tagged '{item.Name}' with '{tag}'");
                    }
                }

                if (changed)
                {
                    _libraryManager.UpdateItem(item, item.Parent, ItemUpdateType.MetadataEdit, null);
                }
            }
        }

        // Links a freshly indexed top-list .strm item to its original movie (alternate version)
        // and queues a real ffprobe so it gets a valid RunTimeTicks + MediaStreams. Runs at most
        // once per item per server session, on the first event where its IMDb id is resolved.
        private void ProcessTopListStrmItem(BaseItem item)
        {
            try
            {
                if (!(item is MediaBrowser.Controller.Entities.Movies.Movie)) return;

                var imdb = item.GetProviderId("Imdb");
                if (string.IsNullOrEmpty(imdb)) return; // metadata not resolved yet — a later ItemUpdated will carry it

                // Process each .strm item once (guards against re-entrancy from our own MergeItems/probe events).
                lock (_strmLock)
                {
                    if (!_processedStrmIds.Add(item.Id)) return;
                }

                var itemId   = item.Id;
                var itemName = item.Name;

                // Run the library mutations OFF the event thread. Emby raises ItemAdded/ItemUpdated
                // synchronously during a library scan, so calling MergeItems here directly would
                // re-enter the library mid-scan and can hang. A background task keeps this a true
                // "finishing touches in the background" operation.
                Task.Run(() =>
                {
                    try
                    {
                        var current = _libraryManager.GetItemById(itemId);
                        if (current == null) return;

                        var topListsFolder = Path.Combine(Plugin.Instance.DataFolderPath, "toplists") + Path.DirectorySeparatorChar;

                        // Find the original (non-top-list) movie that shares this IMDb id.
                        var primary = _libraryManager.GetItemList(new InternalItemsQuery
                        {
                            IncludeItemTypes = new[] { "Movie" },
                            Recursive = true,
                            IsVirtualItem = false
                        }).FirstOrDefault(m => m.Id != itemId
                            && !string.IsNullOrEmpty(m.Path)
                            && !m.Path.StartsWith(topListsFolder, StringComparison.OrdinalIgnoreCase)
                            && string.Equals(m.GetProviderId("Imdb"), imdb, StringComparison.OrdinalIgnoreCase));

                        if (primary != null)
                        {
                            try { _libraryManager.MergeItems(new[] { primary, current }); }
                            catch (Exception ex) { _logger.Warn("[TopList] Merge failed for '" + itemName + "': " + ex.Message); }
                        }

                        // Idempotent — skips if the item already has a runtime.
                        MergeTopListVersionsTask.QueueStrmProbe(_providerManager, _fileSystem, current);
                        _logger.Info("[TopList] Linked + probed '" + itemName + "'");
                    }
                    catch (Exception ex)
                    {
                        _logger.Error("[TopList] ProcessTopListStrmItem(bg) error: " + ex.Message);
                    }
                });
            }
            catch (Exception ex)
            {
                _logger.Error("[TopList] ProcessTopListStrmItem error: " + ex.Message);
            }
        }

        public void Dispose()
        {
            _libraryManager.ItemAdded -= OnItemChanged;
            _libraryManager.ItemUpdated -= OnItemChanged;
        }
    }
}