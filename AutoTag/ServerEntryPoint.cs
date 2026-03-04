using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Library;
using MediaBrowser.Controller.Plugins;
using MediaBrowser.Model.Entities;
using MediaBrowser.Model.Logging;
using MediaBrowser.Model.Serialization;
using System;
using System.IO;
using System.Linq;
using System.Text.Json;

namespace HomeScreenCompanion
{
    public class ServerEntryPoint : IServerEntryPoint
    {
        private readonly ILibraryManager _libraryManager;
        private readonly ILogger _logger;
        private readonly IJsonSerializer _jsonSerializer;

        public ServerEntryPoint(ILibraryManager libraryManager, ILogManager logManager, IJsonSerializer jsonSerializer)
        {
            _libraryManager = libraryManager;
            _logger = logManager.GetLogger("HomeScreenCompanion_RealTime");
            _jsonSerializer = jsonSerializer;
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

        public void Dispose()
        {
            _libraryManager.ItemAdded -= OnItemChanged;
            _libraryManager.ItemUpdated -= OnItemChanged;
        }
    }
}