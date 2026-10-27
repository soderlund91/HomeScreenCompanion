using MediaBrowser.Controller.Collections;
using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Library;
using MediaBrowser.Controller.Persistence;
using MediaBrowser.Controller.Plugins;
using MediaBrowser.Controller.Providers;
using MediaBrowser.Model.Entities;
using MediaBrowser.Model.IO;
using MediaBrowser.Model.Logging;
using MediaBrowser.Model.Serialization;
using MediaBrowser.Model.Tasks;
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
        private readonly PluginLogger _logger;
        private readonly IJsonSerializer _jsonSerializer;
        private readonly IProviderManager _providerManager;
        private readonly IFileSystem _fileSystem;
        private readonly ICollectionManager _collectionManager;
        private readonly IItemRepository _itemRepository;
        private readonly IMediaSourceManager _mediaSourceManager;
        private readonly ITaskManager _taskManager;
        private NewLibraryWatcher? _libraryWatcher;

        private readonly object _strmLock = new object();
        // Keyed by InternalId, not Id: Emby derives the Guid from the path, so a top-list entry that
        // is deleted and re-created at the same path (list rebuilt / re-created under the same name)
        // keeps its Guid but gets a new InternalId — and must be linked again.
        private readonly HashSet<long> _processedStrmIds = new HashSet<long>();
        private readonly HashSet<long> _showCopiesInFlight = new HashSet<long>();

        public ServerEntryPoint(ILibraryManager libraryManager, ILogManager logManager, IJsonSerializer jsonSerializer, IProviderManager providerManager, IFileSystem fileSystem, ICollectionManager collectionManager, IItemRepository itemRepository, IMediaSourceManager mediaSourceManager, ITaskManager taskManager)
        {
            _libraryManager = libraryManager;
            _logger = PluginLog.Wrap(logManager.GetLogger("HomeScreenCompanion_RealTime"), "Real-time");
            _jsonSerializer = jsonSerializer;
            _providerManager = providerManager;
            _fileSystem = fileSystem;
            _collectionManager = collectionManager;
            _itemRepository = itemRepository;
            _mediaSourceManager = mediaSourceManager;
            _taskManager = taskManager;
        }

        public void Run()
        {
            if (Plugin.Instance == null) return;
            // First, so the plugin's own log file is written (lines from before wait in its queue).
            PluginLog.Initialize(Plugin.Instance.DataFolderPath);
            RunAutoMigration();
            // Old tag top-lists move to their source's Create as top-list (once; see SourceTopLists).
            SourceTopLists.Json = _jsonSerializer;
            try { if (SourceTopLists.Migrate(Plugin.Instance.Configuration)) Plugin.Instance.SaveConfiguration(); }
            catch (Exception ex) { _logger.Warn($"[Migration] Top-lists could not be moved to their sources: {ex.Message}"); }
            TagCacheManager.Instance.Initialize(Plugin.Instance.DataFolderPath, _jsonSerializer);
            TopListCollectionMirror.Initialize(_libraryManager, _collectionManager, _logger);
            ShowTopList.Initialize(_itemRepository, _mediaSourceManager);

            // Last run logs survive a restart (shown in the log modal and "Last run").
            LogStore.Initialize(Plugin.Instance.DataFolderPath, _jsonSerializer);
            GroupIssues.Initialize(Plugin.Instance.DataFolderPath, _jsonSerializer);
            HomeScreenCompanionTask.RestoreLog();
            HomeSectionSyncTask.RestoreLog();
            TopListSyncTask.RestoreLog();

            _libraryManager.ItemAdded += OnItemChanged;
            _libraryManager.ItemUpdated += OnItemChanged;
            _collectionManager.CollectionCreated += OnCollectionCreated;
            _collectionManager.ItemsAddedToCollection += OnItemsAddedToCollection;
            _collectionManager.ItemsRemovedFromCollection += OnItemsRemovedFromCollection;

            // A library created in Emby later runs the Top-list section sync by itself.
            _libraryWatcher = new NewLibraryWatcher(_libraryManager, _taskManager, _jsonSerializer, _logger, Plugin.Instance.DataFolderPath);
            _libraryWatcher.Start();

            // "Keep the list in a file" (Missing tab): written again when the library changes.
            MissingTitlesExport.Initialize(_libraryManager, _jsonSerializer);
        }

        private void OnCollectionCreated(object sender, CollectionCreatedEventArgs e) =>
            TopListCollectionMirror.OnCollectionChanged(e.Collection, e.Options?.ItemIdList);

        private void OnItemsAddedToCollection(object sender, CollectionModifiedEventArgs e) =>
            TopListCollectionMirror.OnCollectionChanged(e.Collection, e.ItemsChanged);

        private void OnItemsRemovedFromCollection(object sender, CollectionModifiedEventArgs e) =>
            TopListCollectionMirror.OnCollectionChanged(e.Collection, e.ItemsChanged);

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
            if (item.IsVirtualItem) return;

            // Show top-list copies (series, seasons, episodes): rank, ranked images and the
            // originals' images, as soon as Emby indexes them.
            if ((item is MediaBrowser.Controller.Entities.TV.Series || item is MediaBrowser.Controller.Entities.TV.Season
                 || item is MediaBrowser.Controller.Entities.TV.Episode) && ShowTopList.IsCopy(item))
            {
                ProcessShowCopy(item);
                return;
            }

            if (!(item is MediaBrowser.Controller.Entities.Movies.Movie) && !(item is MediaBrowser.Controller.Entities.TV.Series))
                return;

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
                // Ranked-collection copies: never tagged; the sync links and probes them.
                if (RankedCollections.IsRankedPath(item.Path)) return;
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
                    if (!_processedStrmIds.Add(item.InternalId)) return;
                }

                var itemId   = item.Id;
                var itemName = item.Name;

                // Run the library mutations OFF the event thread. Emby raises ItemAdded/ItemUpdated
                // synchronously during a library scan, so calling MergeItems here directly would
                // re-enter the library mid-scan and can hang. A background task keeps this a true
                // "finishing touches in the background" operation.
                Task.Run(async () =>
                {
                    try
                    {
                        var current = _libraryManager.GetItemById(itemId);
                        if (current == null) return;

                        var topListsFolder = Path.Combine(Plugin.Instance.DataFolderPath, "toplists") + Path.DirectorySeparatorChar;

                        // Find the original (non-top-list) movies that share this IMDb id — several
                        // when versions are separate items; the first is the merge target.
                        // Runs once per indexed .strm, so query by IMDb id rather than loading
                        // every movie; the Where() below keeps the result exact regardless.
                        var originals = _libraryManager.GetItemList(new InternalItemsQuery
                        {
                            IncludeItemTypes = new[] { "Movie" },
                            Recursive = true,
                            IsVirtualItem = false,
                            AnyProviderIdEquals = new List<KeyValuePair<string, string>> { new KeyValuePair<string, string>("Imdb", imdb) }
                        }).Where(m => m.Id != itemId
                            && !string.IsNullOrEmpty(m.Path)
                            && !m.Path.StartsWith(topListsFolder, StringComparison.OrdinalIgnoreCase)
                            && !RankedCollections.IsRankedPath(m.Path)
                            && string.Equals(m.GetProviderId("Imdb"), imdb, StringComparison.OrdinalIgnoreCase))
                          .ToList();

                        if (originals.Count > 0)
                        {
                            var primary = originals[0];
                            try { _libraryManager.MergeItems(new[] { primary, current }); }
                            catch (Exception ex) { _logger.Warn("[TopList] Merge failed for '" + itemName + "': " + ex.Message); }

                            // Optional: same collection memberships as the originals.
                            if (TopListCollectionMirror.Enabled)
                                await TopListCollectionMirror.SyncCopy(_libraryManager.GetItemById(itemId) ?? current, originals);
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

        // Runs off the event thread (events fire synchronously during a library scan). Our own
        // UpdateItem raises ItemUpdated again; the in-flight set and Refresh's "nothing to do"
        // check keep that from looping.
        private void ProcessShowCopy(BaseItem item)
        {
            var internalId = item.InternalId;
            lock (_strmLock)
            {
                if (!_showCopiesInFlight.Add(internalId)) return;
            }
            Task.Run(() =>
            {
                try
                {
                    var current = _libraryManager.GetItemById(internalId);
                    if (current != null) ShowTopList.Refresh(current, _libraryManager, _jsonSerializer);
                }
                catch (Exception ex)
                {
                    _logger.Warn("[TopList] Show copy '" + item.Name + "' could not be updated: " + ex.Message);
                }
                finally
                {
                    lock (_strmLock) { _showCopiesInFlight.Remove(internalId); }
                }
            });
        }

        public void Dispose()
        {
            _libraryManager.ItemAdded -= OnItemChanged;
            _libraryManager.ItemUpdated -= OnItemChanged;
            _collectionManager.CollectionCreated -= OnCollectionCreated;
            _collectionManager.ItemsAddedToCollection -= OnItemsAddedToCollection;
            _collectionManager.ItemsRemovedFromCollection -= OnItemsRemovedFromCollection;
            _libraryWatcher?.Dispose();
            MissingTitlesExport.Dispose();
            PluginLog.Flush();
        }
    }
}