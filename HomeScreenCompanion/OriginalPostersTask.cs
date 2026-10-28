using MediaBrowser.Common.Net;
using MediaBrowser.Controller.Library;
using MediaBrowser.Controller.Providers;
using MediaBrowser.Model.IO;
using MediaBrowser.Model.Logging;
using MediaBrowser.Model.Serialization;
using MediaBrowser.Model.Tasks;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace HomeScreenCompanion
{
    /// <summary>
    /// Original-language posters (Home Screen Tweaks): sets them for new titles and puts back the
    /// ones Emby replaced. Runs once a day and shortly after new titles are added
    /// (OriginalPostersEntryPoint). Safe to run often: what is in place is skipped. With the setting
    /// off it puts every original poster back (Restore all).
    /// </summary>
    public class OriginalPostersTask : IScheduledTask, IConfigurableScheduledTask
    {
        private readonly ILibraryManager _libraryManager;
        private readonly IProviderManager _providerManager;
        private readonly IFileSystem _fileSystem;
        private readonly IHttpClient _httpClient;
        private readonly IJsonSerializer _jsonSerializer;
        private readonly PluginLogger _logger;

        public static List<string> ExecutionLog { get; } = new List<string>();

        public OriginalPostersTask(ILibraryManager libraryManager, IProviderManager providerManager, IFileSystem fileSystem,
            IHttpClient httpClient, IJsonSerializer jsonSerializer, ILogManager logManager)
        {
            _libraryManager = libraryManager;
            _providerManager = providerManager;
            _fileSystem = fileSystem;
            _httpClient = httpClient;
            _jsonSerializer = jsonSerializer;
            _logger = PluginLog.Wrap(logManager.GetLogger("HomeScreenCompanion_OrigPosters"), "Original posters");
        }

        public string Key => "OriginalPostersTask";
        public string Name => "Original-language posters";
        public string Description => "Gives titles in a chosen language their poster in that language (Home Screen Tweaks).";
        public string Category => "Home Screen Companion";

        // Not listed in Emby's Scheduled Tasks (it is in the plugin's Run menu); it still runs on its triggers.
        public bool IsHidden => true;
        public bool IsEnabled => true;
        public bool IsLogged => true;

        public IEnumerable<TaskTriggerInfo> GetDefaultTriggers()
        {
            return new[]
            {
                new TaskTriggerInfo { Type = TaskTriggerInfo.TriggerInterval, IntervalTicks = TimeSpan.FromHours(24).Ticks }
            };
        }

        public async Task Execute(CancellationToken cancellationToken, IProgress<double> progress)
        {
            lock (ExecutionLog) { ExecutionLog.Clear(); }

            var config = Plugin.Instance?.Configuration;
            if (config == null) return;

            var log = new RunLog(ExecutionLog, _logger, "[Original posters]", config.ExtendedConsoleOutput);
            log.Rule();
            log.Info($"Original-language posters  ·  {DateTime.Now:yyyy-MM-dd HH:mm}");
            log.Rule();

            // Off: the posters set here go back to the originals (the same as Restore all).
            if (!config.OrigPostersEnabled)
            {
                if (OriginalPosters.Load(_jsonSerializer).Applied.Count == 0)
                {
                    log.Skip("Original-language posters is off in Home Screen Tweaks — nothing to do");
                    return;
                }
                var (restored, _) = await OriginalPosters.RestoreAsync(null, _libraryManager, _providerManager, _fileSystem, _jsonSerializer, _logger, cancellationToken).ConfigureAwait(false);
                log.Ok($"Original-language posters is off: {RunLog.Plural(restored, "original poster")} put back");
                progress.Report(100);
                return;
            }

            var started = DateTime.UtcNow;
            if (await OriginalPosters.RunAsync(config, _libraryManager, _providerManager, _fileSystem, _httpClient, _jsonSerializer, log, progress, cancellationToken).ConfigureAwait(false))
                log.Info($"  Done in {RunLog.Elapsed(DateTime.UtcNow - started)}");
            progress.Report(100);
        }
    }
}
