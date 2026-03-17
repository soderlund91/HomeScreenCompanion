using MediaBrowser.Controller.Library;
using MediaBrowser.Model.Entities;
using MediaBrowser.Model.Logging;
using MediaBrowser.Model.Tasks;
using MediaBrowser.Model.Users;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace HomeScreenCompanion
{
    public class HomeSectionSyncTask : IScheduledTask
    {
        private readonly IUserManager _userManager;
        private readonly ILogger _logger;

        public static string LastSyncTime { get; private set; } = "Never";
        public static bool IsRunning { get; private set; } = false;
        public static string LastSyncResult { get; private set; } = "";
        public static int LastSectionsCopied { get; private set; } = 0;
        public static List<string> ExecutionLog { get; } = new List<string>();

        public HomeSectionSyncTask(IUserManager userManager, ILogManager logManager)
        {
            _userManager = userManager;
            _logger = logManager.GetLogger("HomeScreenCompanion_HSC");
        }

        public string Key => "HomeSectionSyncTask";
        public string Name => "Home Screen Sync";
        public string Description => "Syncs home screen sections from a source user to all selected target users.";
        public string Category => "Home Screen Companion";

        public IEnumerable<TaskTriggerInfo> GetDefaultTriggers()
        {
            return Array.Empty<TaskTriggerInfo>();
        }

        public Task Execute(CancellationToken cancellationToken, IProgress<double> progress)
        {
            IsRunning = true;
            lock (ExecutionLog) { ExecutionLog.Clear(); }
            try
            {
                var config = Plugin.Instance?.Configuration;
                if (config == null) return Task.CompletedTask;

                bool debug = config.ExtendedConsoleOutput;

                if (!config.HomeSyncEnabled)
                {
                    LogSummary("Sync is disabled. Skipping.");
                    return Task.CompletedTask;
                }

                if (string.IsNullOrWhiteSpace(config.HomeSyncSourceUserId))
                {
                    LogSummary("No source user configured. Skipping.", "Warn");
                    LastSyncResult = "No source user configured.";
                    return Task.CompletedTask;
                }

                if (config.HomeSyncTargetUserIds == null || config.HomeSyncTargetUserIds.Count == 0)
                {
                    LogSummary("No target users configured. Skipping.", "Warn");
                    LastSyncResult = "No target users configured.";
                    return Task.CompletedTask;
                }

                var sourceInternalId = _userManager.GetInternalId(config.HomeSyncSourceUserId);
                string sourceName = Guid.TryParse(config.HomeSyncSourceUserId, out var srcGuid)
                    ? (_userManager.GetUserById(srcGuid)?.Name ?? config.HomeSyncSourceUserId)
                    : config.HomeSyncSourceUserId;
                progress.Report(5);

                var sourceSections = _userManager.GetHomeSections(sourceInternalId, cancellationToken);

                if (sourceSections?.Sections == null || sourceSections.Sections.Length == 0)
                {
                    LogSummary($"  ! {sourceName} has no home sections configured. Skipping.", "Warn");
                    LastSyncResult = "Source user has no home sections.";
                    return Task.CompletedTask;
                }

                LogSummary($"  Source: {sourceName}  ·  {sourceSections.Sections.Length} section(s)");
                if (debug)
                {
                    foreach (var s in sourceSections.Sections)
                        LogDebug($"  [HS] [{s.SectionType}] \"{s.CustomName ?? s.Name}\"");
                }

                progress.Report(20);
                int totalCopied = 0;
                int targetCount = config.HomeSyncTargetUserIds.Count;

                for (int i = 0; i < targetCount; i++)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    var targetIdStr = config.HomeSyncTargetUserIds[i];
                    string targetName = Guid.TryParse(targetIdStr, out var tgtGuid)
                        ? (_userManager.GetUserById(tgtGuid)?.Name ?? targetIdStr)
                        : targetIdStr;
                    try
                    {
                        var targetInternalId = _userManager.GetInternalId(targetIdStr);

                        var existing = _userManager.GetHomeSections(targetInternalId, cancellationToken);
                        if (existing?.Sections?.Length > 0)
                        {
                            if (debug) LogDebug($"  [HS] {targetName}: replacing {existing.Sections.Length} existing section(s)");
                            var idsToDelete = existing.Sections
                                .Where(s => !string.IsNullOrEmpty(s.Id))
                                .Select(s => s.Id)
                                .ToArray();
                            if (idsToDelete.Length > 0)
                                _userManager.DeleteHomeSections(targetInternalId, idsToDelete, cancellationToken);
                        }

                        foreach (var section in sourceSections.Sections)
                        {
                            if (debug) LogDebug($"  [HS] {targetName}: + [{section.SectionType}] \"{section.CustomName ?? section.Name}\"");
                            _userManager.AddHomeSection(targetInternalId, CopySection(section), cancellationToken);
                            totalCopied++;
                        }

                        LogSummary($"  + {targetName}  ·  {sourceSections.Sections.Length} section(s) synced");
                    }
                    catch (Exception ex)
                    {
                        LogSummary($"  ! {targetName}  ·  {ex.Message}", "Error");
                    }

                    progress.Report(20 + (int)(80.0 * (i + 1) / targetCount));
                }

                if (config.HomeSyncLibraryOrder)
                {
                    LogSummary("  --------------------------------------------------");
                    LogSummary("  Library order sync");
                    try
                    {
                        var sourceUser = _userManager.GetUserById(config.HomeSyncSourceUserId);
                        if (sourceUser != null)
                        {
                            var sourceConf = _userManager.GetUserConfiguration(sourceUser);
                            if (sourceConf?.OrderedViews != null && sourceConf.OrderedViews.Length > 0)
                            {
                                if (debug)
                                    LogDebug($"  [LibOrder] Source order: [{string.Join(", ", sourceConf.OrderedViews)}]");
                                foreach (var targetIdStr in config.HomeSyncTargetUserIds)
                                {
                                    string targetName2 = Guid.TryParse(targetIdStr, out var tgtGuid2)
                                        ? (_userManager.GetUserById(tgtGuid2)?.Name ?? targetIdStr)
                                        : targetIdStr;
                                    try
                                    {
                                        var targetUser = _userManager.GetUserById(targetIdStr);
                                        if (targetUser == null) continue;
                                        var targetConf = _userManager.GetUserConfiguration(targetUser);
                                        if (targetConf == null) continue;
                                        targetConf.OrderedViews = sourceConf.OrderedViews;
                                        _userManager.UpdateConfiguration(_userManager.GetInternalId(targetIdStr), targetConf);
                                        LogSummary($"  + {targetName2}  ·  library order synced");
                                    }
                                    catch (Exception ex)
                                    {
                                        LogSummary($"  ! {targetName2}  ·  library order failed: {ex.Message}", "Warn");
                                    }
                                }
                            }
                            else
                            {
                                LogSummary($"  ! {sourceName} has no custom library order. Skipping.", "Warn");
                            }
                        }
                        else
                        {
                            LogSummary($"  ! Could not load source user. Skipping.", "Warn");
                        }
                    }
                    catch (Exception ex)
                    {
                        LogSummary($"  ! Library order sync failed: {ex.Message}", "Error");
                    }
                }

                LastSectionsCopied = totalCopied;
                LastSyncTime = DateTime.Now.ToString("yyyy-MM-dd HH:mm");
                LastSyncResult = $"OK — {totalCopied} section(s) copied to {targetCount} user(s).";
                LogSummary("  --------------------------------------------------");
                LogSummary($"  Done  ·  {totalCopied} section(s) synced to {targetCount} user(s)");
                progress.Report(100);
            }
            catch (OperationCanceledException)
            {
                LastSyncResult = "Cancelled.";
                LogSummary("Sync was cancelled.", "Warn");
            }
            catch (Exception ex)
            {
                LastSyncResult = $"Error: {ex.Message}";
                LogSummary($"Unexpected error: {ex.Message}", "Error");
            }
            finally
            {
                IsRunning = false;
            }

            return Task.CompletedTask;
        }

        private void LogSummary(string message, string level = "Info")
        {
            var msg = $"[{DateTime.Now:HH:mm:ss}] {message}";
            lock (ExecutionLog) { ExecutionLog.Add(msg); }
            if (level == "Error") _logger.Error($"[Home Screen] {message}");
            else if (level == "Warn") _logger.Warn($"[Home Screen] {message}");
            else _logger.Info($"[Home Screen] {message}");
        }

        private void LogDebug(string message)
        {
            var msg = $"[{DateTime.Now:HH:mm:ss}] [DEBUG] {message}";
            lock (ExecutionLog) { ExecutionLog.Add(msg); }
        }

        private static ContentSection CopySection(ContentSection source)
        {
            var copy = new ContentSection();
            foreach (var prop in typeof(ContentSection).GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                if (prop.Name == "Id") continue;
                if (prop.CanRead && prop.CanWrite)
                    prop.SetValue(copy, prop.GetValue(source));
            }
            return copy;
        }
    }
}
