using MediaBrowser.Common.Net;
using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Entities.Movies;
using MediaBrowser.Controller.Entities.TV;
using MediaBrowser.Controller.Library;
using MediaBrowser.Controller.Providers;
using MediaBrowser.Model.Configuration;
using MediaBrowser.Model.Entities;
using MediaBrowser.Model.IO;
using MediaBrowser.Model.Providers;
using MediaBrowser.Model.Serialization;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace HomeScreenCompanion
{
    /// <summary>
    /// Home Screen Tweaks › Original-language posters: a movie or series whose original language
    /// (TMDb's original_language) is one of the chosen languages, and is not the library's own image
    /// language, gets a poster in that language instead of the library's. Seasons and episodes
    /// follow their series' language. The posters come from Emby's own image providers (TheMovieDb,
    /// TheTVDB, FanArt …) in Emby's order; only one tagged with exactly that language is used, so a
    /// title with none keeps its poster.
    ///
    /// The poster is saved the way Emby's "Download image" saves one. The one it replaced is remembered
    /// (a file is copied to original_posters/backup; a provider URL Emby had not downloaded is kept as
    /// it is), so it can be put back. state.json remembers each poster set here
    /// (path + modified time): when Emby replaces it later (a refresh with "Replace existing images"),
    /// the next run puts it back. A title the user restores is excluded from then on.
    /// </summary>
    internal static class OriginalPosters
    {
        // ---- Stored state ----

        public sealed class StateFile
        {
            public List<Applied> Applied { get; set; } = new List<Applied>();
            public List<LangEntry> Languages { get; set; } = new List<LangEntry>();
            public List<Checked> Checked { get; set; } = new List<Checked>();
            public List<string> Excluded { get; set; } = new List<string>();   // item ids (N); a series covers its seasons and episodes
            public DateTime LastRunUtc { get; set; }
        }

        public sealed class Applied
        {
            public string ItemId { get; set; } = "";       // N
            public string SeriesId { get; set; } = "";     // N, seasons and episodes
            public long TitleInternalId { get; set; }      // the movie or series (for the page; 0 = saved before it was)
            public string Kind { get; set; } = "";         // Movie / Series / Season / Episode
            public string Language { get; set; } = "";
            public string Url { get; set; } = "";
            public string Provider { get; set; } = "";
            public string ImagePath { get; set; } = "";    // what Emby saved it as
            public long ImageTicks { get; set; }
            // The poster it replaced: its path (a file, or the provider's URL when Emby has not downloaded
            // it) and modified time. "" = the item had no poster; null = not known (saved before this was
            // recorded). A file is also copied to backup/ (Backup = its name there), as it is overwritten.
            public string? OriginalPath { get; set; }
            public long OriginalTicks { get; set; }
            public string? Backup { get; set; }
            public DateTime AppliedUtc { get; set; }
        }

        // TMDb's original language of a movie ("movie:603") or show ("tv:1399"). Lang null = TMDb had no answer.
        public sealed class LangEntry
        {
            public string Key { get; set; } = "";
            public string? Lang { get; set; }
            public DateTime CheckedUtc { get; set; }
        }

        // An item whose providers had no poster in the language: asked again after RecheckAfter.
        public sealed class Checked
        {
            public string ItemId { get; set; } = "";
            public string Language { get; set; } = "";
            public DateTime CheckedUtc { get; set; }
        }

        private static readonly TimeSpan RecheckAfter = TimeSpan.FromDays(30);
        private static readonly TimeSpan RetryUnknownLanguage = TimeSpan.FromDays(7);
        private const int MinWidth = 300;

        // One run or restore at a time (the task, the entry point and the page's buttons). A restore
        // asked for during a run stops the run (it keeps what it did) instead of waiting for it.
        private static readonly SemaphoreSlim Gate = new SemaphoreSlim(1, 1);
        private static CancellationTokenSource? _runCts;
        private static volatile bool _interrupted;
        private static readonly object FileGate = new object();
        private static string Folder => Path.Combine(Plugin.Instance!.DataFolderPath, "original_posters");
        private static string StatePath => Path.Combine(Folder, "state.json");
        private static string BackupFolder => Path.Combine(Folder, "backup");

        internal static StateFile Load(IJsonSerializer json)
        {
            lock (FileGate)
            {
                try { if (File.Exists(StatePath)) return json.DeserializeFromFile<StateFile>(StatePath) ?? new StateFile(); }
                catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
                return new StateFile();
            }
        }

        private static void Save(StateFile state, IJsonSerializer json)
        {
            lock (FileGate)
            {
                Directory.CreateDirectory(Folder);
                var tmp = StatePath + ".tmp";
                json.SerializeToFile(state, tmp);
                if (File.Exists(StatePath)) File.Delete(StatePath);
                File.Move(tmp, StatePath);
            }
        }

        // ---- Languages ----

        // ISO 639-1, lower case. The providers spell a few languages in more than one way.
        internal static string Norm(string? lang)
        {
            var l = (lang ?? "").Trim().ToLowerInvariant();
            int dash = l.IndexOfAny(new[] { '-', '_' });
            if (dash > 0) l = l.Substring(0, dash);
            switch (l)
            {
                case "nb": case "nn": return "no";   // Norwegian Bokmål / Nynorsk
                case "cn": return "zh";               // TMDb's Cantonese
                default: return l.Length == 2 ? l : "";
            }
        }

        internal static HashSet<string> Wanted(PluginConfiguration config) =>
            new HashSet<string>((config.OrigPostersLanguages ?? new List<string>()).Select(Norm).Where(l => l.Length > 0), StringComparer.Ordinal);

        private static string? TmdbKey(BaseItem item, string kind)
        {
            var id = item.GetProviderId("Tmdb");
            return string.IsNullOrWhiteSpace(id) ? null : kind + ":" + id.Trim();
        }

        public sealed class TmdbOriginal
        {
            public string? original_language { get; set; }
        }

        // ---- Run ----

        internal sealed class Counts
        {
            public int Applied, Restored, Kept, NotFound, Failed;
        }

        // False when the run was stopped for a restore (it is started again).
        internal static async Task<bool> RunAsync(PluginConfiguration config, ILibraryManager libraryManager, IProviderManager providerManager,
            IFileSystem fileSystem, IHttpClient http, IJsonSerializer json, RunLog log, IProgress<double>? progress, CancellationToken cancellationToken)
        {
            await Gate.WaitAsync(cancellationToken).ConfigureAwait(false);
            var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _runCts = cts;
            _interrupted = false;
            try
            {
                await RunLockedAsync(config, libraryManager, providerManager, fileSystem, http, json, log, progress, cts.Token).ConfigureAwait(false);
                return true;
            }
            catch (OperationCanceledException) when (_interrupted && !cancellationToken.IsCancellationRequested)
            {
                log.Skip("Stopped to put original posters back — it starts again right after");
                return false;
            }
            finally
            {
                _runCts = null;
                cts.Dispose();
                Gate.Release();
            }
        }

        private static async Task RunLockedAsync(PluginConfiguration config, ILibraryManager libraryManager, IProviderManager providerManager,
            IFileSystem fileSystem, IHttpClient http, IJsonSerializer json, RunLog log, IProgress<double>? progress, CancellationToken cancellationToken)
        {
            var wanted = Wanted(config);
            if (wanted.Count == 0) { log.Skip("No languages are chosen — nothing to do"); return; }
            if (!config.OrigPostersMovies && !config.OrigPostersSeries && !config.OrigPostersSeasons && !config.OrigPostersEpisodes)
            { log.Skip("Movies, series, seasons and episodes are all off — nothing to do"); return; }
            var key = (config.TmdbApiKey ?? "").Trim();
            if (key.Length == 0) { log.Warn("Needs a TMDb key in Settings (it tells the original language) — nothing done"); return; }

            var state = Load(json);
            var excluded = new HashSet<string>(state.Excluded, StringComparer.OrdinalIgnoreCase);

            // 1. The titles, and their original language from TMDb (kept for good; asked once).
            var titles = new List<(BaseItem Item, string Kind)>();
            if (config.OrigPostersMovies)
                titles.AddRange(libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "Movie" }, Recursive = true, IsVirtualItem = false })
                    .Where(m => !ShowTopList.IsCopy(m) && !RankedCollections.IsRankedCopy(m)).Select(m => (m, "movie")));
            if (config.OrigPostersSeries || config.OrigPostersSeasons || config.OrigPostersEpisodes)
                titles.AddRange(ShowTopList.AllSeries(libraryManager).Select(s => (s, "tv")));

            var langs = state.Languages.Where(l => !string.IsNullOrEmpty(l.Key))
                .GroupBy(l => l.Key, StringComparer.Ordinal).ToDictionary(g => g.Key, g => g.First(), StringComparer.Ordinal);
            var toAsk = titles.Select(t => TmdbKey(t.Item, t.Kind)).Where(k => k != null).Select(k => k!).Distinct(StringComparer.Ordinal)
                .Where(k => !langs.TryGetValue(k, out var l) || (l.Lang == null && DateTime.UtcNow - l.CheckedUtc > RetryUnknownLanguage))
                .ToList();
            if (toAsk.Count > 0)
            {
                log.Info($"  Reading the original language of {RunLog.Plural(toAsk.Count, "title")} from TMDb (once per title)");
                log.Step("Original language from TMDb");
                var throttle = new SemaphoreSlim(4);
                int done = 0, failed = 0;
                // In batches, so a long first run that is stopped keeps what it read.
                foreach (var batch in toAsk.Select((k, i) => (k, i)).GroupBy(x => x.i / 400, x => x.k))
                {
                    var read = await Task.WhenAll(batch.Select(async k =>
                    {
                        await throttle.WaitAsync(cancellationToken).ConfigureAwait(false);
                        try
                        {
                            var parts = k.Split(':');
                            var r = await MissingTitles.TmdbGet<TmdbOriginal>($"{parts[0]}/{Uri.EscapeDataString(parts[1])}", key, http, json, cancellationToken).ConfigureAwait(false);
                            var l = Norm(r?.original_language);
                            return new LangEntry { Key = k, Lang = l.Length > 0 ? l : null, CheckedUtc = DateTime.UtcNow };
                        }
                        catch (OperationCanceledException) { throw; }
                        catch { Interlocked.Increment(ref failed); return new LangEntry { Key = k, Lang = null, CheckedUtc = DateTime.UtcNow }; }
                        finally
                        {
                            throttle.Release();
                            log.Count("Original language", Interlocked.Increment(ref done), toAsk.Count, "titles");
                        }
                    })).ConfigureAwait(false);
                    foreach (var l in read) langs[l.Key] = l;
                    state.Languages = langs.Values.ToList();
                    Save(state, json);
                    progress?.Report(30.0 * done / toAsk.Count);
                }
                if (failed > 0) log.Debug($"  TMDb did not answer for {RunLog.Plural(failed, "title")} (asked again in {RetryUnknownLanguage.TotalDays:0} days)");
            }

            // 2. The titles to change: original language chosen and not the library's own image language.
            var libLangs = new Dictionary<long, string>();
            var targets = new List<(BaseItem Title, string Lang, LibraryOptions Options)>();
            foreach (var (item, kind) in titles)
            {
                var k = TmdbKey(item, kind);
                if (k == null || !langs.TryGetValue(k, out var le) || le.Lang == null || !wanted.Contains(le.Lang)) continue;
                if (excluded.Contains(item.Id.ToString("N"))) continue;
                var options = libraryManager.GetLibraryOptions(item);
                if (Norm(item.GetPreferredImageLanguage(options)) == le.Lang) continue;
                targets.Add((item, le.Lang, options));
            }
            log.Info($"  {RunLog.Plural(targets.Count, "title")} in a chosen language other than the library's  ·  languages: {string.Join(", ", wanted.OrderBy(l => l))}");

            // 3. Each title, and the seasons / episodes of each series.
            // Four titles at a time (a series with episodes asks the providers once per episode); a title's
            // own seasons and episodes one after the other.
            var applied = new ConcurrentDictionary<string, Applied>(state.Applied.Where(a => !string.IsNullOrEmpty(a.ItemId))
                .GroupBy(a => a.ItemId, StringComparer.OrdinalIgnoreCase).ToDictionary(g => g.Key, g => g.First()), StringComparer.OrdinalIgnoreCase);
            var checkedMap = new ConcurrentDictionary<string, Checked>(state.Checked.Where(c => !string.IsNullOrEmpty(c.ItemId))
                .GroupBy(c => c.ItemId + "|" + c.Language, StringComparer.OrdinalIgnoreCase).ToDictionary(g => g.Key, g => g.First()), StringComparer.OrdinalIgnoreCase);
            var directory = new DirectoryService(fileSystem);
            var counts = new Counts();
            var touched = new ConcurrentDictionary<string, bool>(StringComparer.OrdinalIgnoreCase);
            var workers = new SemaphoreSlim(4);
            int titlesDone = 0;

            async Task DoTitle(BaseItem title, string lang, LibraryOptions options)
            {
                var items = new List<(BaseItem Item, string Kind)>();
                if (title is Series)
                {
                    if (config.OrigPostersSeries) items.Add((title, "Series"));
                    if (config.OrigPostersSeasons || config.OrigPostersEpisodes)
                    {
                        var children = libraryManager.GetItemList(new InternalItemsQuery
                        {
                            AncestorIds = new[] { title.InternalId },
                            IncludeItemTypes = new[] { "Season", "Episode" },
                            Recursive = true,
                            IsVirtualItem = false
                        }).Where(c => !ShowTopList.IsCopy(c) && !excluded.Contains(c.Id.ToString("N")));
                        foreach (var c in children)
                        {
                            if (c is Season && config.OrigPostersSeasons) items.Add((c, "Season"));
                            else if (c is Episode && config.OrigPostersEpisodes) items.Add((c, "Episode"));
                        }
                    }
                }
                else items.Add((title, "Movie"));

                foreach (var (item, kind) in items)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    touched[item.Id.ToString("N")] = true;
                    try
                    {
                        await ApplyOneAsync(item, kind, title, lang, options, applied, checkedMap, providerManager, libraryManager, directory, counts, log, cancellationToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) { throw; }
                    catch (Exception ex)
                    {
                        Interlocked.Increment(ref counts.Failed);
                        log.Debug($"  {Label(item)}: {ex.Message}");
                    }
                }
            }

            // In batches, so a long first run that is stopped keeps what it did (also when it is stopped
            // mid-batch: the finally below saves every poster set so far).
            try
            {
                foreach (var batch in targets.Select((t, i) => (t, i)).GroupBy(x => x.i / 25, x => x.t))
                {
                    await Task.WhenAll(batch.Select(async t =>
                    {
                        await workers.WaitAsync(cancellationToken).ConfigureAwait(false);
                        try { await DoTitle(t.Title, t.Lang, t.Options).ConfigureAwait(false); }
                        finally
                        {
                            workers.Release();
                            int done = Interlocked.Increment(ref titlesDone);
                            log.Count("Posters", done, targets.Count, "titles");
                            progress?.Report(30.0 + 70.0 * done / Math.Max(1, targets.Count));
                        }
                    })).ConfigureAwait(false);
                    state.Applied = applied.Values.ToList();
                    state.Checked = checkedMap.Values.ToList();
                    Save(state, json);
                }

                // 4. Posters set here for titles that no longer qualify (a language or kind turned off, the
                // library's language changed): put the original back.
                foreach (var a in applied.Values.Where(a => !touched.ContainsKey(a.ItemId)).ToList())
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var item = libraryManager.GetItemById(Guid.ParseExact(a.ItemId, "N"));
                    if (item == null) { DropBackup(a); applied.TryRemove(a.ItemId, out _); continue; }
                    if (excluded.Contains(a.ItemId) || excluded.Contains(a.SeriesId)) continue;   // restored by the user already handles it
                    try
                    {
                        if (await RestoreOneAsync(item, a, providerManager, libraryManager, directory, cancellationToken).ConfigureAwait(false))
                        {
                            counts.Restored++;
                            log.Detail($"{Label(item)}: original poster put back (no longer chosen)");
                        }
                        applied.TryRemove(a.ItemId, out _);
                    }
                    catch (OperationCanceledException) { throw; }
                    catch (Exception ex) { counts.Failed++; log.Debug($"  {Label(item)}: could not restore: {ex.Message}"); }
                }
                state.LastRunUtc = DateTime.UtcNow;
            }
            finally
            {
                state.Applied = applied.Values.ToList();
                state.Checked = checkedMap.Values.ToList();
                Save(state, json);
            }

            if (counts.Applied > 0) log.Ok($"{RunLog.Plural(counts.Applied, "poster")} changed to the original language");
            if (counts.Restored > 0) log.Ok($"{RunLog.Plural(counts.Restored, "original poster")} put back");
            log.Info($"  {RunLog.Plural(counts.Kept, "poster")} already in place  ·  {counts.NotFound} with no poster in the language" +
                     (counts.Failed > 0 ? $"  ·  {counts.Failed} failed (see the debug log)" : ""));
        }

        private static string Label(BaseItem item)
        {
            if (item is Episode e) return $"{e.SeriesName} S{e.ParentIndexNumber ?? 0:00}E{e.IndexNumber ?? 0:00}";
            if (item is Season s) return $"{s.SeriesName} · {s.Name}";
            return item.ProductionYear > 0 ? $"{item.Name} ({item.ProductionYear})" : item.Name;
        }

        private static bool IsOurs(ItemImageInfo? current, Applied a) =>
            current != null && string.Equals(current.Path, a.ImagePath, StringComparison.OrdinalIgnoreCase) && current.DateModified.Ticks == a.ImageTicks;

        private static async Task ApplyOneAsync(BaseItem item, string kind, BaseItem title, string lang, LibraryOptions options,
            ConcurrentDictionary<string, Applied> applied, ConcurrentDictionary<string, Checked> checkedMap,
            IProviderManager providerManager, ILibraryManager libraryManager, IDirectoryService directory,
            Counts counts, RunLog log, CancellationToken cancellationToken)
        {
            var id = item.Id.ToString("N");
            var current = item.GetImageInfo(ImageType.Primary, 0);
            applied.TryGetValue(id, out var had);
            if (had != null && had.Language == lang)
            {
                if (IsOurs(current, had))
                {
                    if (had.TitleInternalId == 0) had.TitleInternalId = title.InternalId;
                    Interlocked.Increment(ref counts.Kept);
                    return;
                }
                // Replaced since (an image refresh): the same poster goes back, without asking the providers again.
                // The image there now becomes the one a restore puts back.
                await SaveAsync(item, had.Url, had.Provider, kind, title, lang, current, options, applied, providerManager, libraryManager, directory, cancellationToken, keepBackup: false).ConfigureAwait(false);
                Interlocked.Increment(ref counts.Applied);
                log.Detail($"{Label(item)}: the {LanguageName(lang)} poster was replaced — put back");
                return;
            }

            var checkKey = id + "|" + lang;
            if (had == null && checkedMap.TryGetValue(checkKey, out var c) && DateTime.UtcNow - c.CheckedUtc < RecheckAfter) { Interlocked.Increment(ref counts.NotFound); return; }

            var remote = await providerManager.GetAvailableRemoteImages(item, options,
                new RemoteImageQuery { ImageType = ImageType.Primary, IncludeAllLanguages = true }, directory, cancellationToken).ConfigureAwait(false);
            var pick = (remote ?? Enumerable.Empty<RemoteImageInfo>())
                .FirstOrDefault(r => !string.IsNullOrEmpty(r.Url) && Norm(r.Language) == lang && (r.Width == null || r.Width >= MinWidth));
            if (pick == null)
            {
                checkedMap[checkKey] = new Checked { ItemId = id, Language = lang, CheckedUtc = DateTime.UtcNow };
                Interlocked.Increment(ref counts.NotFound);
                return;
            }
            checkedMap.TryRemove(checkKey, out _);
            await SaveAsync(item, pick.Url, pick.ProviderName ?? "", kind, title, lang, current, options, applied, providerManager, libraryManager, directory, cancellationToken, keepBackup: had != null).ConfigureAwait(false);
            Interlocked.Increment(ref counts.Applied);
            log.Detail($"{Label(item)}: {LanguageName(lang)} poster ({pick.ProviderName})");
        }

        // Saves url as the item's poster. The poster there now is copied to backup/ first, unless
        // keepBackup (the backup made the first time stays: it is the item's real original).
        private static async Task SaveAsync(BaseItem item, string url, string provider, string kind, BaseItem title, string lang,
            ItemImageInfo? current, LibraryOptions options, ConcurrentDictionary<string, Applied> applied,
            IProviderManager providerManager, ILibraryManager libraryManager, IDirectoryService directory, CancellationToken cancellationToken, bool keepBackup)
        {
            var id = item.Id.ToString("N");
            applied.TryGetValue(id, out var had);
            string? backup = had?.Backup;
            string? originalPath = had?.OriginalPath;
            long originalTicks = had?.OriginalTicks ?? 0;
            if (!keepBackup || had == null)
            {
                DropBackup(had);
                backup = null;
                originalPath = current?.Path ?? "";
                originalTicks = current?.DateModified.Ticks ?? 0;
                if (current != null && !string.IsNullOrEmpty(current.Path) && !IsUrl(current.Path) && File.Exists(current.Path))
                {
                    Directory.CreateDirectory(BackupFolder);
                    var ext = Path.GetExtension(current.Path);
                    backup = id + (string.IsNullOrEmpty(ext) ? ".jpg" : ext.ToLowerInvariant());
                    File.Copy(current.Path, Path.Combine(BackupFolder, backup), true);
                }
            }

            // Not cancelled halfway: a poster that is saved is always recorded (and so can be restored).
            await providerManager.SaveImage(item, options, url, ImageType.Primary, null, Array.Empty<long>(), directory, true, CancellationToken.None).ConfigureAwait(false);
            libraryManager.UpdateItem(item, item.Parent, ItemUpdateType.ImageUpdate, null);
            var now = item.GetImageInfo(ImageType.Primary, 0);
            applied[id] = new Applied
            {
                ItemId = id,
                SeriesId = item is Series || item is Movie ? "" : title.Id.ToString("N"),
                TitleInternalId = title.InternalId,
                Kind = kind, Language = lang, Url = url, Provider = provider,
                OriginalPath = originalPath, OriginalTicks = originalTicks, Backup = backup,
                ImagePath = now?.Path ?? "", ImageTicks = now?.DateModified.Ticks ?? 0, AppliedUtc = DateTime.UtcNow
            };
        }

        private static void DropBackup(Applied? a)
        {
            if (a?.Backup == null) return;
            try { File.Delete(Path.Combine(BackupFolder, a.Backup)); } catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
        }

        private static bool IsUrl(string path) =>
            path.StartsWith("http://", StringComparison.OrdinalIgnoreCase) || path.StartsWith("https://", StringComparison.OrdinalIgnoreCase);

        // Puts the original poster back. A poster that is no longer the one set here (changed by hand
        // since) is left alone. True when the image changed.
        private static async Task<bool> RestoreOneAsync(BaseItem item, Applied a, IProviderManager providerManager, ILibraryManager libraryManager,
            IDirectoryService directory, CancellationToken cancellationToken)
        {
            var current = item.GetImageInfo(ImageType.Primary, 0);
            if (!IsOurs(current, a)) { DropBackup(a); return false; }

            var file = a.Backup == null ? null : Path.Combine(BackupFolder, a.Backup);
            if (file != null && File.Exists(file))
            {
                // A file: written back the way Emby saves an uploaded image.
                var mime = Path.GetExtension(file).ToLowerInvariant() switch
                {
                    ".png" => "image/png", ".webp" => "image/webp", ".gif" => "image/gif", _ => "image/jpeg"
                };
                using (var stream = File.OpenRead(file))
                    await providerManager.SaveImage(item, libraryManager.GetLibraryOptions(item), stream, mime.AsMemory(), ImageType.Primary, null,
                        Array.Empty<long>(), directory, true, CancellationToken.None).ConfigureAwait(false);   // not cut off halfway
            }
            else if (!string.IsNullOrEmpty(a.OriginalPath) && IsUrl(a.OriginalPath!))
            {
                // The provider's URL, as Emby had it: the image entry goes back, and the file saved here goes,
                // so Emby does not pick it up again as a local poster.
                item.ImageInfos = (item.ImageInfos ?? Array.Empty<ItemImageInfo>()).Where(i => i.Type != ImageType.Primary)
                    .Concat(new[] { new ItemImageInfo { Path = a.OriginalPath!, Type = ImageType.Primary, DateModified = new DateTime(a.OriginalTicks, DateTimeKind.Utc) } })
                    .ToArray();
                DeleteSavedFile(a);
            }
            else if (a.OriginalPath == "")
            {
                // It had no poster before.
                item.DeleteImage(ImageType.Primary, 0);
                DeleteSavedFile(a);
            }
            else
            {
                // The original is not known (or its backup file is gone): the poster stays rather than
                // leaving the title without one. Emby does not fall back to a poster in another language
                // than the library's, so removing it could leave the title empty.
                DropBackup(a);
                return false;
            }
            libraryManager.UpdateItem(item, item.Parent, ItemUpdateType.ImageUpdate, null);
            DropBackup(a);
            return true;
        }

        private static void DeleteSavedFile(Applied a)
        {
            if (string.IsNullOrEmpty(a.ImagePath) || IsUrl(a.ImagePath) || string.Equals(a.ImagePath, a.OriginalPath, StringComparison.OrdinalIgnoreCase)) return;
            try { if (File.Exists(a.ImagePath)) File.Delete(a.ImagePath); } catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
        }

        // ---- The page ----

        public sealed class StatusResult
        {
            public bool HasTmdbKey { get; set; }
            public bool Running { get; set; }
            public DateTime? LastRunUtc { get; set; }
            public List<string> LibraryLanguages { get; set; } = new List<string>();   // the movie and TV libraries' image languages
            public List<StatusTitle> Titles { get; set; } = new List<StatusTitle>();
            public List<StatusTitle> Excluded { get; set; } = new List<StatusTitle>();
        }

        // A movie or series with posters set here (a series counts its seasons and episodes).
        public sealed class StatusTitle
        {
            public string Id { get; set; } = "";
            public string InternalId { get; set; } = "";
            public string Name { get; set; } = "";
            public string Type { get; set; } = "";
            public int? Year { get; set; }
            public string Language { get; set; } = "";
            public bool Poster { get; set; }
            public int Seasons { get; set; }
            public int Episodes { get; set; }
            public DateTime AppliedUtc { get; set; }
        }

        internal static StatusResult Status(PluginConfiguration config, ILibraryManager libraryManager, IJsonSerializer json)
        {
            var state = Load(json);
            var result = new StatusResult
            {
                HasTmdbKey = !string.IsNullOrWhiteSpace(config.TmdbApiKey),
                Running = Gate.CurrentCount == 0,
                LastRunUtc = state.LastRunUtc == default ? (DateTime?)null : state.LastRunUtc
            };
            var dataPath = Plugin.Instance?.DataFolderPath ?? "";
            foreach (var f in libraryManager.GetVirtualFolders())
            {
                if (f.CollectionType != "movies" && f.CollectionType != "tvshows") continue;
                if (dataPath.Length > 0 && (f.Locations ?? Array.Empty<string>()).Any(l => l.StartsWith(dataPath, StringComparison.OrdinalIgnoreCase))) continue;   // HSC's own
                var l = Norm(f.LibraryOptions?.PreferredImageLanguage);
                if (l.Length > 0 && !result.LibraryLanguages.Contains(l)) result.LibraryLanguages.Add(l);
            }
            // The titles in one query (one lookup each took seconds for a few hundred).
            var known = state.Applied.Where(a => a.TitleInternalId > 0).Select(a => a.TitleInternalId).Distinct().ToArray();
            var titles = (known.Length == 0 ? Array.Empty<BaseItem>() : libraryManager.GetItemList(new InternalItemsQuery { ItemIds = known }))
                .GroupBy(i => i.Id.ToString("N"), StringComparer.OrdinalIgnoreCase).ToDictionary(g => g.Key, g => g.First(), StringComparer.OrdinalIgnoreCase);
            var byTitle = new Dictionary<string, StatusTitle>(StringComparer.OrdinalIgnoreCase);
            foreach (var a in state.Applied)
            {
                var titleId = string.IsNullOrEmpty(a.SeriesId) ? a.ItemId : a.SeriesId;
                if (!byTitle.TryGetValue(titleId, out var t))
                {
                    if (!titles.TryGetValue(titleId, out var item))
                        item = titles[titleId] = (Guid.TryParseExact(titleId, "N", out var g) ? libraryManager.GetItemById(g) : null)!;
                    if (item == null) continue;
                    t = byTitle[titleId] = new StatusTitle
                    {
                        Id = titleId, InternalId = item.InternalId.ToString(), Name = item.Name, Type = item is Series ? "Series" : "Movie",
                        Year = item.ProductionYear > 0 ? item.ProductionYear : null, Language = a.Language
                    };
                }
                if (a.Kind == "Season") t.Seasons++;
                else if (a.Kind == "Episode") t.Episodes++;
                else t.Poster = true;
                if (a.AppliedUtc > t.AppliedUtc) t.AppliedUtc = a.AppliedUtc;
            }
            result.Titles = byTitle.Values.OrderBy(t => t.Name, StringComparer.CurrentCultureIgnoreCase).ToList();
            foreach (var id in state.Excluded)
            {
                var item = Guid.TryParseExact(id, "N", out var g) ? libraryManager.GetItemById(g) : null;
                if (item == null) continue;
                result.Excluded.Add(new StatusTitle
                {
                    Id = id, InternalId = item.InternalId.ToString(), Name = item is Season s ? $"{s.SeriesName} · {s.Name}" : item.Name,
                    Type = item is Series ? "Series" : item is Season ? "Season" : "Movie",
                    Year = item.ProductionYear > 0 ? item.ProductionYear : null
                });
            }
            result.Excluded = result.Excluded.OrderBy(t => t.Name, StringComparer.CurrentCultureIgnoreCase).ToList();
            return result;
        }

        /// <summary>
        /// Puts the original posters back: of one movie or series (with its seasons and episodes), which is
        /// then excluded so the next run leaves it alone, or of every title (ids null; nothing is excluded,
        /// the page turns the setting off). Returns how many images changed, and whether a run was
        /// stopped for it (the caller starts it again).
        /// </summary>
        internal static async Task<(int Changed, bool Interrupted)> RestoreAsync(IList<string>? ids, ILibraryManager libraryManager, IProviderManager providerManager,
            IFileSystem fileSystem, IJsonSerializer json, PluginLogger logger, CancellationToken cancellationToken)
        {
            bool interrupted = false;
            var running = _runCts;
            if (running != null)
            {
                _interrupted = interrupted = true;
                try { running.Cancel(); } catch (ObjectDisposedException) { }
            }
            await Gate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                var state = Load(json);
                var only = ids == null ? null : new HashSet<string>(ids.Where(i => !string.IsNullOrWhiteSpace(i)).Select(i => i.Trim()), StringComparer.OrdinalIgnoreCase);
                var directory = new DirectoryService(fileSystem);
                int changed = 0;
                var keep = new List<Applied>();
                foreach (var a in state.Applied)
                {
                    if (only != null && !only.Contains(a.ItemId) && !only.Contains(a.SeriesId)) { keep.Add(a); continue; }
                    var item = Guid.TryParseExact(a.ItemId, "N", out var g) ? libraryManager.GetItemById(g) : null;
                    if (item == null) { DropBackup(a); continue; }
                    try
                    {
                        if (await RestoreOneAsync(item, a, providerManager, libraryManager, directory, cancellationToken).ConfigureAwait(false)) changed++;
                    }
                    catch (OperationCanceledException) { throw; }
                    catch (Exception ex)
                    {
                        logger.Warn($"[Original posters] {Label(item)}: could not put the original poster back: {ex.Message}");
                        keep.Add(a);
                    }
                }
                state.Applied = keep;
                if (only != null)
                    foreach (var id in only)
                        if (!state.Excluded.Contains(id, StringComparer.OrdinalIgnoreCase)) state.Excluded.Add(id);
                Save(state, json);
                logger.Info($"[Original posters] Restored {changed} original poster(s){(only == null ? " (all)" : "")}");
                return (changed, interrupted);
            }
            finally { Gate.Release(); }
        }

        /// <summary>Lets the next run change an excluded title again.</summary>
        internal static void Include(string id, IJsonSerializer json)
        {
            var state = Load(json);
            state.Excluded.RemoveAll(e => string.Equals(e, (id ?? "").Trim(), StringComparison.OrdinalIgnoreCase));
            Save(state, json);
        }

        private static string LanguageName(string lang)
        {
            try { return new System.Globalization.CultureInfo(lang).EnglishName; }
            catch { return lang; }
        }
    }
}
