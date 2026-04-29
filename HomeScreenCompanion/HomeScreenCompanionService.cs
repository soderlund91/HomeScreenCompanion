using MediaBrowser.Common.Net;
using SkiaSharp;
using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Library;
using MediaBrowser.Model.Entities;
using MediaBrowser.Model.Querying;
using MediaBrowser.Model.Serialization;
using MediaBrowser.Model.Services;
using MediaBrowser.Model.Tasks;
using MediaBrowser.Model.Users;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace HomeScreenCompanion
{
    [Route("/HomeScreenCompanion/TestUrl", "GET")]
    public class TestUrlRequest : IReturn<TestUrlResponse>
    {
        public string Url { get; set; } = string.Empty;
        public int Limit { get; set; } = 10;
    }

    [Route("/HomeScreenCompanion/Status", "GET")]
    public class GetStatusRequest : IReturn<StatusResponse> { }

    [Route("/HomeScreenCompanion/Version", "GET")]
    public class VersionRequest : IReturn<VersionResponse> { }

    public class VersionResponse
    {
        public string Version { get; set; } = "";
    }

    [Route("/HomeScreenCompanion/UploadCollectionImage", "POST")]
    public class UploadCollectionImageRequest : IReturn<UploadCollectionImageResponse>
    {
        public string FileName { get; set; } = "";
        public string Base64Data { get; set; } = "";
        public string OldFilePath { get; set; } = "";
    }

    [Route("/HomeScreenCompanion/FetchCollectionImageFromUrl", "POST")]
    public class FetchCollectionImageFromUrlRequest : IReturn<UploadCollectionImageResponse>
    {
        public string Url { get; set; } = "";
        public string OldFilePath { get; set; } = "";
    }

    public class UploadCollectionImageResponse
    {
        public bool Success { get; set; }
        public string FilePath { get; set; } = "";
        public string Message { get; set; } = "";
    }

    public class TestUrlResponse
    {
        public bool Success { get; set; }
        public string Message { get; set; } = string.Empty;
        public int Count { get; set; }
    }

    public class StatusResponse
    {
        public string LastRunStatus { get; set; } = string.Empty;
        public List<string> Logs { get; set; } = new List<string>();
        public bool IsRunning { get; set; }
    }

    [Route("/HomeScreenCompanion/RunEntry", "POST")]
    public class RunEntryRequest : IReturn<RunEntryResponse>
    {
        public string EntryName { get; set; } = "";
    }

    public class RunEntryResponse
    {
        public bool Success { get; set; }
        public string Message { get; set; } = "";
    }

    [Route("/HomeScreenCompanion/Hsc/Status", "GET")]
    public class HscGetStatusRequest : IReturn<HscSyncStatusResponse> { }

    [Route("/HomeScreenCompanion/DebugSections", "GET")]
    public class DebugSectionsRequest : IReturn<string>
    {
        public string UserId { get; set; } = string.Empty;
    }

    [Route("/HomeScreenCompanion/Manage/Tags", "GET")]
    public class GetManagedTagsRequest : IReturn<GetManagedTagsResponse> { }
    public class ManagedTagInfo { public string Id { get; set; } = ""; public string Name { get; set; } = ""; public int ItemCount { get; set; } public int MovieCount { get; set; } public List<string> ItemTypes { get; set; } = new List<string>(); }
    public class GetManagedTagsResponse { public List<ManagedTagInfo> Tags { get; set; } = new List<ManagedTagInfo>(); }

    [Route("/HomeScreenCompanion/Manage/Collections", "GET")]
    public class GetManagedCollectionsRequest : IReturn<GetManagedCollectionsResponse> { }
    public class ManagedCollectionInfo { public string Id { get; set; } = ""; public string Name { get; set; } = ""; public int ItemCount { get; set; } }
    public class GetManagedCollectionsResponse { public List<ManagedCollectionInfo> Collections { get; set; } = new List<ManagedCollectionInfo>(); }

    [Route("/HomeScreenCompanion/Manage/Playlists", "GET")]
    public class GetManagedPlaylistsRequest : IReturn<GetManagedPlaylistsResponse> { }
    public class ManagedPlaylistInfo { public string Id { get; set; } = ""; public string Name { get; set; } = ""; public int ItemCount { get; set; } }
    public class GetManagedPlaylistsResponse { public List<ManagedPlaylistInfo> Playlists { get; set; } = new List<ManagedPlaylistInfo>(); }

    [Route("/HomeScreenCompanion/Manage/DeleteTag", "POST")]
    public class DeleteManagedTagRequest : IReturn<DeleteManagedTagResponse> { public string TagName { get; set; } = ""; }
    public class DeleteManagedTagResponse { public bool Success { get; set; } public string Message { get; set; } = ""; public int ItemsUpdated { get; set; } }

    [Route("/HomeScreenCompanion/Manage/DeleteTags", "POST")]
    public class DeleteManagedTagsBatchRequest : IReturn<DeleteManagedTagsResponse> { public List<string> TagNames { get; set; } = new List<string>(); }
    public class DeleteManagedTagsResponse { public bool Success { get; set; } public int ItemsUpdated { get; set; } }

    [Route("/HomeScreenCompanion/Manage/DeleteCollection", "POST")]
    public class DeleteManagedCollectionRequest : IReturn<DeleteManagedCollectionResponse> { public string CollectionId { get; set; } = ""; }
    public class DeleteManagedCollectionResponse { public bool Success { get; set; } public string Message { get; set; } = ""; }

    [Route("/HomeScreenCompanion/TopList/Status", "GET")]
    public class GetTopListStatusRequest : IReturn<TopListStatusResponse> { }
    public class TopListStatusResponse
    {
        public bool IsRunning { get; set; }
        public string LastRunStatus { get; set; } = "";
        public List<string> Logs { get; set; } = new List<string>();
    }

    [Route("/HomeScreenCompanion/TopList/PrepareFolder", "POST")]
    public class PrepareTopListFolderRequest : IReturn<PrepareTopListFolderResponse>
    {
        public string TagName { get; set; } = "";
        public int MaxItems { get; set; } = 0;
        public string BadgeStyle { get; set; } = "neutral";
    }
    public class PrepareTopListFolderResponse
    {
        public bool Success { get; set; }
        public string FolderPath { get; set; } = "";
        public string Message { get; set; } = "";
        public int FilesCreated { get; set; }
    }

    [Route("/HomeScreenCompanion/TopList/List", "GET")]
    public class GetTopListsRequest : IReturn<GetTopListsResponse> { }
    public class GetTopListsResponse
    {
        public List<string> FolderNames { get; set; } = new List<string>();
        public Dictionary<string, int> MovieCounts { get; set; } = new Dictionary<string, int>();
    }

    [Route("/HomeScreenCompanion/TopList/ManualItems", "GET")]
    public class GetManualTopListItemsRequest : IReturn<GetManualTopListItemsResponse>
    {
        public string ListName { get; set; } = "";
    }
    public class GetManualTopListItemsResponse
    {
        public bool Success { get; set; }
        public List<MovieItem> Movies { get; set; } = new List<MovieItem>();
        public string CustomName { get; set; } = "";
        public string DisplayMode { get; set; } = "";
        public string ImageType { get; set; } = "";
        public string BadgeStyle { get; set; } = "neutral";
        public List<string> UserIds { get; set; } = new List<string>();
        public string Message { get; set; } = "";
    }

    [Route("/HomeScreenCompanion/TopList/Delete", "POST")]
    public class DeleteTopListRequest : IReturn<DeleteTopListResponse>
    {
        public string TagName { get; set; } = "";
    }
    public class DeleteTopListResponse
    {
        public bool Success { get; set; }
        public string FolderPath { get; set; } = "";
        public string Message { get; set; } = "";
    }

    [Route("/HomeScreenCompanion/TopList/SyncHomeSections", "POST")]
    public class PrepareTopListHomeSectionsRequest : IReturn<PrepareTopListHomeSectionsResponse>
    {
        public string TagName { get; set; } = "";
    }

    public class PrepareTopListHomeSectionsResponse
    {
        public bool Success { get; set; }
        public int UsersCreated { get; set; }
        public int UsersUpdated { get; set; }
        public string Message { get; set; } = "";
    }

    [Route("/HomeScreenCompanion/TopList/SyncAllSections", "POST")]
    public class SyncAllTopListSectionsRequest : IReturn<SyncAllTopListSectionsResponse> { }
    public class SyncAllTopListSectionsResponse
    {
        public bool Success { get; set; }
        public int UpdatedSections { get; set; }
        public string Message { get; set; } = "";
    }

    [Route("/HomeScreenCompanion/TopList/AllMovies", "GET")]
    public class GetAllMoviesRequest : IReturn<GetAllMoviesResponse> { }
    public class MovieItem
    {
        public string Name   { get; set; } = "";
        public int?   Year   { get; set; }
        public string ImdbId { get; set; } = "";
        public string ItemId { get; set; } = "";
    }
    public class GetAllMoviesResponse
    {
        public List<MovieItem> Movies { get; set; } = new List<MovieItem>();
    }

    [Route("/HomeScreenCompanion/TopList/GrantLibraryAccess", "POST")]
    public class GrantTopListLibraryAccessRequest : IReturn<GrantTopListLibraryAccessResponse>
    {
        public string LibraryId { get; set; } = "";
    }
    public class GrantTopListLibraryAccessResponse
    {
        public bool Success { get; set; }
        public int UsersUpdated { get; set; }
        public string Message { get; set; } = "";
    }

    [Route("/HomeScreenCompanion/TopList/SnapshotPolicies", "POST")]
    public class SnapshotPoliciesRequest : IReturn<SnapshotPoliciesResponse> { }
    public class SnapshotPoliciesResponse
    {
        public bool Success { get; set; }
        public string SnapshotId { get; set; } = "";
        public int UserCount { get; set; }
        public string Message { get; set; } = "";
    }

    [Route("/HomeScreenCompanion/TopList/RestoreAndGrantAccess", "POST")]
    public class RestoreAndGrantAccessRequest : IReturn<RestoreAndGrantAccessResponse>
    {
        public string SnapshotId { get; set; } = "";
        public string LibraryId { get; set; } = "";
    }
    public class RestoreAndGrantAccessResponse
    {
        public bool Success { get; set; }
        public int UsersUpdated { get; set; }
        public string Message { get; set; } = "";
    }

    [Route("/HomeScreenCompanion/TopList/PrepareManualFolder", "POST")]
    public class PrepareManualTopListFolderRequest : IReturn<PrepareTopListFolderResponse>
    {
        public string ListName { get; set; } = "";
        public List<ManualTopListItem> Items { get; set; } = new List<ManualTopListItem>();
        public string BadgeStyle { get; set; } = "neutral";
    }
    public class ManualTopListItem
    {
        public string ImdbId { get; set; } = "";
        public string ItemId { get; set; } = "";
    }

public class HomeScreenCompanionService : IService
    {
        private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, List<PolicySnapshot>> _policySnapshots
            = new System.Collections.Concurrent.ConcurrentDictionary<string, List<PolicySnapshot>>();

        private class PolicySnapshot
        {
            public string UserId { get; set; } = "";
            public bool EnableAllFolders { get; set; }
            public string[] EnabledFolders { get; set; } = Array.Empty<string>();
        }

        private readonly IHttpClient _httpClient;
        private readonly IJsonSerializer _jsonSerializer;
        private readonly IUserManager _userManager;
        private readonly ILibraryManager _libraryManager;
        private readonly IUserDataManager _userDataManager;
        private readonly IUserViewManager _userViewManager;
        private readonly ITaskManager _taskManager;

        public HomeScreenCompanionService(IHttpClient httpClient, IJsonSerializer jsonSerializer, IUserManager userManager, ILibraryManager libraryManager, IUserDataManager userDataManager, IUserViewManager userViewManager, ITaskManager taskManager)
        {
            _httpClient = httpClient;
            _jsonSerializer = jsonSerializer;
            _userManager = userManager;
            _libraryManager = libraryManager;
            _userDataManager = userDataManager;
            _userViewManager = userViewManager;
            _taskManager = taskManager;
        }

        public object Get(VersionRequest request)
        {
            return new VersionResponse { Version = Plugin.Instance?.Version.ToString() ?? "0.0.0" };
        }

        public object Get(GetStatusRequest request)
        {
            List<string> logs;
            lock (HomeScreenCompanionTask.ExecutionLog) { logs = HomeScreenCompanionTask.ExecutionLog.ToList(); }
            return new StatusResponse
            {
                LastRunStatus = HomeScreenCompanionTask.LastRunStatus,
                Logs = logs,
                IsRunning = HomeScreenCompanionTask.IsRunning
            };
        }

        private static string GetItemTypeKey(BaseItem item)
        {
            try
            {
                dynamic d = item;
                var extraType = d.ExtraType;
                if (extraType != null)
                {
                    var s = extraType.ToString();
                    if (!string.IsNullOrEmpty(s) && s != "0" && s != "None")
                        return s; // ThemeSong, ThemeVideo, Trailer, BehindTheScenes, etc.
                }
            }
            catch { }
            return item.GetType().Name;
        }

        private static void DeleteOldImage(string oldFilePath, string imagesDir)
        {
            if (string.IsNullOrWhiteSpace(oldFilePath)) return;
            var fullImagesDir = Path.GetFullPath(imagesDir);
            var fullOldPath = Path.GetFullPath(oldFilePath);
            if (fullOldPath.StartsWith(fullImagesDir + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase) && File.Exists(fullOldPath))
                File.Delete(fullOldPath);
        }

        public object Post(UploadCollectionImageRequest request)
        {
            try
            {
                var dataPath = Plugin.Instance?.DataFolderPath;
                if (dataPath == null) return new UploadCollectionImageResponse { Success = false, Message = "Plugin not initialized" };

                var imagesDir = Path.Combine(dataPath, "collection_images");
                Directory.CreateDirectory(imagesDir);

                DeleteOldImage(request.OldFilePath, imagesDir);

                var ext = Path.GetExtension(request.FileName ?? "").ToLowerInvariant();
                if (!new[] { ".jpg", ".jpeg", ".png", ".webp", ".gif" }.Contains(ext)) ext = ".jpg";

                var fileName = $"{Guid.NewGuid():N}{ext}";
                var filePath = Path.Combine(imagesDir, fileName);

                File.WriteAllBytes(filePath, Convert.FromBase64String(request.Base64Data));

                return new UploadCollectionImageResponse { Success = true, FilePath = filePath };
            }
            catch (Exception ex)
            {
                return new UploadCollectionImageResponse { Success = false, Message = ex.Message };
            }
        }

        public async Task<object> Post(FetchCollectionImageFromUrlRequest request)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(request.Url) || !request.Url.StartsWith("http", StringComparison.OrdinalIgnoreCase))
                    return new UploadCollectionImageResponse { Success = false, Message = "Invalid URL." };

                var dataPath = Plugin.Instance?.DataFolderPath;
                if (dataPath == null) return new UploadCollectionImageResponse { Success = false, Message = "Plugin not initialized." };

                var imagesDir = Path.Combine(dataPath, "collection_images");
                Directory.CreateDirectory(imagesDir);

                DeleteOldImage(request.OldFilePath, imagesDir);

                var ext = Path.GetExtension(new Uri(request.Url).AbsolutePath).ToLowerInvariant();
                if (!new[] { ".jpg", ".jpeg", ".png", ".webp", ".gif" }.Contains(ext)) ext = ".jpg";

                var fileName = $"{Guid.NewGuid():N}{ext}";
                var filePath = Path.Combine(imagesDir, fileName);

                using (var stream = await _httpClient.Get(new HttpRequestOptions { Url = request.Url, CancellationToken = CancellationToken.None }))
                using (var fs = File.Create(filePath))
                {
                    await stream.CopyToAsync(fs);
                }

                return new UploadCollectionImageResponse { Success = true, FilePath = filePath };
            }
            catch (Exception ex)
            {
                return new UploadCollectionImageResponse { Success = false, Message = ex.Message };
            }
        }

        public object Get(DebugSectionsRequest request)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(request.UserId))
                    return "{\"error\":\"UserId is required\"}";
                var internalId = _userManager.GetInternalId(request.UserId);
                var result = _userManager.GetHomeSections(internalId, CancellationToken.None);
                return _jsonSerializer.SerializeToString(result);
            }
            catch (Exception ex)
            {
                return $"{{\"error\":\"{ex.Message}\"}}";
            }
        }

        public async Task<object> Get(TestUrlRequest request)
        {
            var config = Plugin.Instance?.Configuration;
            if (config == null) return new TestUrlResponse { Success = false, Message = "Config not found" };

            var fetcher = new ListFetcher(_httpClient, _jsonSerializer);
            try
            {
                var items = await fetcher.FetchItems(request.Url, request.Limit, config.TraktClientId, config.MdblistApiKey, config.TmdbApiKey, CancellationToken.None);

                if (items == null || items.Count == 0)
                {
                    return new TestUrlResponse { Success = false, Message = "No items found. Check URL and API Keys." };
                }

                return new TestUrlResponse
                {
                    Success = true,
                    Count = items.Count,
                    Message = $"Successfully found {items.Count} items."
                };
            }
            catch (Exception ex)
            {
                return new TestUrlResponse { Success = false, Message = $"Error: {ex.Message}" };
            }
        }

        public async Task<object> Post(RunEntryRequest request)
        {
            if (string.IsNullOrWhiteSpace(request.EntryName))
                return new RunEntryResponse { Success = false, Message = "No entry name provided" };
            var task = HomeScreenCompanionTask.Instance;
            if (task == null)
                return new RunEntryResponse { Success = false, Message = "Task not initialized" };
            var (success, message) = await task.RunSingleEntryAsync(request.EntryName, CancellationToken.None);
            return new RunEntryResponse { Success = success, Message = message };
        }

        public async Task<object> Post(TestAiSourceRequest request)
        {
            var config = Plugin.Instance?.Configuration;
            if (config == null)
                return new TestAiSourceResponse { Success = false, Message = "Plugin config not found." };

            if (string.IsNullOrWhiteSpace(request.Prompt))
                return new TestAiSourceResponse { Success = false, Message = "Prompt is required." };

            string recentlyWatchedContext = "";
            if (request.IncludeRecentlyWatched && !string.IsNullOrWhiteSpace(request.RecentlyWatchedUserId))
            {
                try
                {
                    if (Guid.TryParse(request.RecentlyWatchedUserId, out var userGuid))
                    {
                        var user = _userManager.GetUserById(userGuid);
                        if (user != null)
                        {
                            int maxCount = request.RecentlyWatchedCount > 0 ? request.RecentlyWatchedCount : 20;
                            var allLibItems = _libraryManager.GetItemList(new MediaBrowser.Controller.Entities.InternalItemsQuery
                            {
                                IncludeItemTypes = new[] { "Movie", "Series" },
                                Recursive = true,
                                IsVirtualItem = false
                            });

                            var playedItems = allLibItems
                                .Select(item => new { item, ud = _userDataManager?.GetUserData(user, item) })
                                .Where(x => x.ud?.Played == true)
                                .OrderByDescending(x => x.ud?.LastPlayedDate ?? System.DateTimeOffset.MinValue)
                                .Take(maxCount)
                                .Select(x => x.item)
                                .ToList();

                            if (playedItems.Count > 0)
                            {
                                var sb = new System.Text.StringBuilder("The user has recently watched these movies and TV shows (most recent first):\n");
                                foreach (var item in playedItems)
                                {
                                    var yearStr = item.ProductionYear.HasValue ? $" ({item.ProductionYear})" : "";
                                    var typeStr = item.GetType().Name.Contains("Series") ? "show" : "movie";
                                    sb.AppendLine($"- {item.Name}{yearStr} [{typeStr}]");
                                }
                                sb.AppendLine("Use this to personalize your recommendations.");
                                recentlyWatchedContext = sb.ToString();
                            }
                        }
                    }
                }
                catch { }
            }

            var fetcher = new ListFetcher(_httpClient, _jsonSerializer);
            try
            {
                var aiItems = await fetcher.FetchAiList(
                    request.Provider,
                    request.Prompt,
                    config.OpenAiApiKey,
                    config.GeminiApiKey,
                    config.OllamaBaseUrl,
                    config.OllamaModel,
                    config.AiSystemPrompt,
                    recentlyWatchedContext,
                    20,
                    CancellationToken.None);

                if (aiItems == null || aiItems.Count == 0)
                    return new TestAiSourceResponse { Success = false, Message = "No items returned. Check your API key and prompt." };

                var preview = aiItems.Take(5)
                    .Select(i => string.IsNullOrEmpty(i.imdb_id) ? i.title : $"{i.title} — {i.imdb_id}")
                    .ToList();

                return new TestAiSourceResponse
                {
                    Success = true,
                    Count = aiItems.Count,
                    Message = $"AI returned {aiItems.Count} items.",
                    Preview = preview
                };
            }
            catch (Exception ex)
            {
                return new TestAiSourceResponse { Success = false, Message = $"Error: {ex.Message}" };
            }
        }

        public object Get(HscGetStatusRequest request)
        {
            List<string> logs;
            lock (HomeSectionSyncTask.ExecutionLog) { logs = HomeSectionSyncTask.ExecutionLog.ToList(); }
            return new HscSyncStatusResponse
            {
                LastSyncTime = HomeSectionSyncTask.LastSyncTime,
                IsRunning = HomeSectionSyncTask.IsRunning,
                LastSyncResult = HomeSectionSyncTask.LastSyncResult,
                SectionsCopied = HomeSectionSyncTask.LastSectionsCopied,
                Logs = logs
            };
        }

        public object Get(GetTopListStatusRequest request)
        {
            List<string> logs;
            lock (TopListSyncTask.ExecutionLog) { logs = TopListSyncTask.ExecutionLog.ToList(); }
            return new TopListStatusResponse
            {
                IsRunning = TopListSyncTask.IsRunning,
                LastRunStatus = TopListSyncTask.LastRunStatus,
                Logs = logs
            };
        }

        public object Get(HscGetUserSectionsRequest request)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(request.UserId))
                    return new HscUserSectionsResponse();

                var internalId = _userManager.GetInternalId(request.UserId);
                var result = _userManager.GetHomeSections(internalId, CancellationToken.None);
                return new HscUserSectionsResponse
                {
                    Sections = result?.Sections ?? Array.Empty<ContentSection>()
                };
            }
            catch (Exception ex)
            {
                return new HscSaveUserSectionsResponse { Success = false, Message = ex.Message };
            }
        }



        public object Get(HscDebugMethodsRequest request)
        {
            var lines = new System.Text.StringBuilder();
            lines.AppendLine($"Runtime type: {_userManager.GetType().FullName}");
            lines.AppendLine();

            var seen = new HashSet<Type>();
            var queue = new Queue<Type>();
            queue.Enqueue(_userManager.GetType());
            while (queue.Count > 0)
            {
                var t = queue.Dequeue();
                if (!seen.Add(t)) continue;
                var relevant = t.GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.DeclaredOnly)
                    .Where(m => m.Name.IndexOf("Section", StringComparison.OrdinalIgnoreCase) >= 0
                             || m.Name.IndexOf("Move", StringComparison.OrdinalIgnoreCase) >= 0
                             || m.Name.IndexOf("Home", StringComparison.OrdinalIgnoreCase) >= 0);
                foreach (var m in relevant)
                {
                    var ps = string.Join(", ", m.GetParameters().Select(p => p.ParameterType.Name + " " + p.Name));
                    lines.AppendLine($"  [{t.Name}] {m.ReturnType.Name} {m.Name}({ps})");
                }
                if (t.BaseType != null) queue.Enqueue(t.BaseType);
                foreach (var iface in t.GetInterfaces()) queue.Enqueue(iface);
            }
            return lines.ToString();
        }

        public object Post(HscSaveUserSectionsRequest request)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(request.UserId))
                    return new HscSaveUserSectionsResponse { Success = false, Message = "No user specified." };

                var internalId = _userManager.GetInternalId(request.UserId);
                var requestedSections = request.Sections ?? Array.Empty<ContentSection>();
                var requestedIds = new HashSet<string>(
                    requestedSections.Where(s => !string.IsNullOrEmpty(s.Id)).Select(s => s.Id),
                    StringComparer.OrdinalIgnoreCase);

                // 1. Radera bara sektioner som faktiskt togs bort från listan
                var existing = _userManager.GetHomeSections(internalId, CancellationToken.None);
                var toDelete = (existing?.Sections ?? Array.Empty<ContentSection>())
                    .Where(s => !string.IsNullOrEmpty(s.Id) && !requestedIds.Contains(s.Id))
                    .Select(s => s.Id)
                    .ToArray();
                if (toDelete.Length > 0)
                    _userManager.DeleteHomeSections(internalId, toDelete, CancellationToken.None);

                // 2. Ordna om kvarvarande sektioner via MoveHomeSections — IDs förändras inte,
                //    så HomeSectionTracked behöver inte uppdateras för omordning.
                var orderedIds = requestedSections
                    .Where(s => !string.IsNullOrEmpty(s.Id))
                    .Select(s => s.Id)
                    .ToArray();

                string moveDebug = "MoveHomeSections: ok";
                try
                {
                    dynamic mgr = _userManager;
                    for (int i = 0; i < orderedIds.Length; i++)
                        mgr.MoveHomeSections(internalId, new[] { orderedIds[i] }, i, CancellationToken.None);
                }
                catch (Microsoft.CSharp.RuntimeBinder.RuntimeBinderException)
                {
                    // Fallback: prova utan CancellationToken
                    try
                    {
                        dynamic mgr = _userManager;
                        for (int i = 0; i < orderedIds.Length; i++)
                            mgr.MoveHomeSections(internalId, new[] { orderedIds[i] }, i);
                    }
                    catch (Exception ex) { moveDebug = $"MoveHomeSections fallback error: {ex.Message}"; }
                }
                catch (Exception ex) { moveDebug = $"MoveHomeSections error: {ex.Message}"; }

                // 3. Ta bort tracking för raderade sektioner så att task re-skapar dem vid behov
                if (toDelete.Length > 0)
                {
                    var deletedSet = new HashSet<string>(toDelete, StringComparer.OrdinalIgnoreCase);
                    var pluginConfig = Plugin.Instance?.Configuration;
                    if (pluginConfig != null)
                    {
                        bool changed = false;
                        foreach (var tag in pluginConfig.Tags)
                        {
                            var toRemove = tag.HomeSectionTracked
                                .Where(t => !string.IsNullOrEmpty(t.SectionId) && deletedSet.Contains(t.SectionId))
                                .ToList();
                            foreach (var t in toRemove) { tag.HomeSectionTracked.Remove(t); changed = true; }
                        }
                        if (changed)
                            Plugin.Instance?.SaveConfiguration();
                    }
                }

                return new HscSaveUserSectionsResponse { Success = true, Message = moveDebug };
            }
            catch (Exception ex)
            {
                return new HscSaveUserSectionsResponse { Success = false, Message = ex.Message };
            }
        }

        public object Post(HscApplyTagHomeSectionsRequest request)
        {
            try
            {
                var config = Plugin.Instance?.Configuration;
                if (config == null)
                    return new HscApplyTagHomeSectionsResponse { Success = false, Message = "Plugin configuration not available." };

                var tc = config.Tags?.FirstOrDefault(t =>
                    string.Equals(t.Name, request.TagName, StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(t.Tag,  request.TagName, StringComparison.OrdinalIgnoreCase));

                if (tc == null)
                    return new HscApplyTagHomeSectionsResponse { Success = false, Message = $"Tag '{request.TagName}' not found." };

                if (!tc.EnableHomeSection)
                    return new HscApplyTagHomeSectionsResponse { Success = true, Message = "Home section not enabled for this tag." };

                var realTracked = (tc.HomeSectionTracked ?? new System.Collections.Generic.List<HomeSectionTracking>())
                    .Where(t => !string.IsNullOrEmpty(t.SectionId) && !t.SectionId.StartsWith("hsc__"))
                    .ToList();
                if (realTracked.Count == 0)
                    return new HscApplyTagHomeSectionsResponse { Success = true, Message = "No existing tracked sections — nothing to apply." };

                // Deserialize settings
                var settingsDict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                try
                {
                    if (!string.IsNullOrEmpty(tc.HomeSectionSettings) && tc.HomeSectionSettings != "{}")
                        settingsDict = _jsonSerializer.DeserializeFromString<Dictionary<string, string>>(tc.HomeSectionSettings) ?? settingsDict;
                }
                catch { }

                if (!settingsDict.ContainsKey("SectionType"))
                    settingsDict["SectionType"] = (tc.EnableCollection && !string.IsNullOrEmpty(tc.CollectionName)) ? "boxset" : "items";

                settingsDict.TryGetValue("SectionType", out var sectionType);

                // Resolve library ID
                string resolvedLibraryId = null;
                if (sectionType == "boxset")
                {
                    if (tc.HomeSectionLibraryId == "auto")
                    {
                        if (tc.EnableCollection && !string.IsNullOrEmpty(tc.CollectionName))
                        {
                            var coll = _libraryManager.GetItemList(new MediaBrowser.Controller.Entities.InternalItemsQuery
                            {
                                IncludeItemTypes = new[] { "BoxSet" },
                                Name = tc.CollectionName,
                                Recursive = true
                            }).FirstOrDefault();
                            if (coll != null) resolvedLibraryId = coll.InternalId.ToString();
                        }
                    }
                    else if (!string.IsNullOrEmpty(tc.HomeSectionLibraryId))
                    {
                        resolvedLibraryId = tc.HomeSectionLibraryId;
                    }
                    if (string.IsNullOrEmpty(resolvedLibraryId))
                        return new HscApplyTagHomeSectionsResponse { Success = false, Message = "Collection not found — cannot apply." };
                }

                // Look up tag ID for query (items type)
                if (sectionType == "items" && !string.IsNullOrEmpty(tc.Tag))
                {
                    var tagItem = _libraryManager.GetItemList(new MediaBrowser.Controller.Entities.InternalItemsQuery
                    {
                        IncludeItemTypes = new[] { "Tag" },
                        Name = tc.Tag,
                        Recursive = true
                    }).FirstOrDefault();
                    if (tagItem != null) settingsDict["_queryTagId"] = tagItem.InternalId.ToString();
                }

                // Build section marker (same pattern as the task)
                var safeTag = new string((tc.Name ?? tc.Tag ?? "").Select(c => char.IsLetterOrDigit(c) ? c : '_').ToArray());
                var sectionMarker = "hsc__" + safeTag;

                int updated = 0;
                foreach (var userId in (tc.HomeSectionUserIds ?? new System.Collections.Generic.List<string>()))
                {
                    var tracking = realTracked.FirstOrDefault(t => t.UserId == userId);
                    if (tracking == null) continue; // no real tracked section for this user

                    try
                    {
                        var userInternalId = _userManager.GetInternalId(userId);
                        var currentSections = _userManager.GetHomeSections(userInternalId, CancellationToken.None);
                        var allSections = currentSections?.Sections ?? Array.Empty<ContentSection>();

                        ContentSection ownedSection =
                            allSections.FirstOrDefault(s => s.Id == tracking.SectionId) ??
                            allSections.FirstOrDefault(s => s.Subtitle == sectionMarker);

                        if (ownedSection == null) continue;

                        var updatedSection = HomeScreenCompanionTask.BuildContentSection(_jsonSerializer, settingsDict, resolvedLibraryId, ownedSection);
                        typeof(ContentSection).GetProperty("Id")?.SetValue(updatedSection, ownedSection.Id);
                        _userManager.UpdateHomeSection(userInternalId, updatedSection, CancellationToken.None);
                        updated++;
                    }
                    catch { /* skip this user on error */ }
                }

                return new HscApplyTagHomeSectionsResponse { Success = true, UsersUpdated = updated, Message = $"Applied to {updated} user(s)." };
            }
            catch (Exception ex)
            {
                return new HscApplyTagHomeSectionsResponse { Success = false, Message = ex.Message };
            }
        }

        private static ContentSection CopySectionWithoutId(ContentSection source)
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

        public object Get(HscGetSectionSchemaRequest request)
        {
            var fields = typeof(ContentSection)
                .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.CanRead && p.CanWrite && p.Name != "Id")
                .Select(p => new HscSectionField { Name = p.Name, Type = GetSimpleTypeName(p.PropertyType) })
                .Where(f => f.Type != null)
                .ToList();
            return new HscSectionSchemaResponse { Fields = fields };
        }

        private static string GetSimpleTypeName(Type t)
        {
            if (t == typeof(string)) return "string";
            if (t == typeof(bool) || t == typeof(bool?)) return "bool";
            if (t == typeof(int) || t == typeof(int?)) return "int";
            if (t == typeof(long) || t == typeof(long?)) return "long";
            if (t == typeof(DateTime) || t == typeof(DateTime?)) return "datetime";
            return null;
        }

        public object Get(GetManagedTagsRequest request)
        {
            var allItems = _libraryManager.GetItemList(new InternalItemsQuery
            {
                Recursive = true,
                IsVirtualItem = false,
                IncludeItemTypes = new[] { "Movie", "Series", "Episode", "Season", "Audio", "MusicVideo", "MusicAlbum", "MusicArtist", "Book", "Game", "Trailer", "Video", "Person", "BoxSet", "Photo", "PhotoAlbum", "Playlist", "Recording", "Studio" }
            }).ToList();

            // Also fetch extras (ExtraType = ThemeSong, BehindTheScenes, etc.) which are excluded by default
            try
            {
                var extraQuery = new InternalItemsQuery { Recursive = true, IsVirtualItem = false };
                var extraTypesProp = typeof(InternalItemsQuery).GetProperty("ExtraTypes");
                if (extraTypesProp != null)
                {
                    var elemType = extraTypesProp.PropertyType.GetElementType();
                    if (elemType != null && elemType.IsEnum)
                    {
                        var all = System.Enum.GetValues(elemType);
                        var arr = System.Array.CreateInstance(elemType, all.Length);
                        all.CopyTo(arr, 0);
                        extraTypesProp.SetValue(extraQuery, arr);
                    }
                }
                var seenIds = new HashSet<Guid>(allItems.Select(i => i.Id));
                foreach (var extra in _libraryManager.GetItemList(extraQuery))
                    if (seenIds.Add(extra.Id)) allItems.Add(extra);
            }
            catch { }

            var tagCount = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            var tagMovieKeys = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
            var tagTypes = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
            foreach (var item in allItems)
            {
                if (item.Tags == null) continue;
                var typeKey = GetItemTypeKey(item);
                var isMovie = item is MediaBrowser.Controller.Entities.Movies.Movie;
                string movieKey = null;
                if (isMovie)
                {
                    var imdb = item.GetProviderId("Imdb");
                    movieKey = !string.IsNullOrEmpty(imdb)
                        ? imdb
                        : (item.Name ?? "") + "_" + (item.ProductionYear?.ToString() ?? "");
                }
                foreach (var tag in item.Tags)
                {
                    if (string.IsNullOrWhiteSpace(tag)) continue;
                    tagCount.TryGetValue(tag, out var c);
                    tagCount[tag] = c + 1;
                    if (isMovie && movieKey != null)
                    {
                        if (!tagMovieKeys.TryGetValue(tag, out var seen))
                            tagMovieKeys[tag] = seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                        seen.Add(movieKey);
                    }
                    if (!tagTypes.TryGetValue(tag, out var typeSet))
                        tagTypes[tag] = typeSet = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    typeSet.Add(typeKey);
                }
            }
            var tagItems = _libraryManager.GetItemList(new InternalItemsQuery
            {
                IncludeItemTypes = new[] { "Tag" },
                Recursive = true
            });
            var tagIdMap = tagItems.ToDictionary(
                t => t.Name ?? "",
                t => t.Id.ToString("N"),
                StringComparer.OrdinalIgnoreCase);

            var tags = tagCount
                .Select(kv => new ManagedTagInfo
                {
                    Name = kv.Key,
                    ItemCount = kv.Value,
                    MovieCount = tagMovieKeys.TryGetValue(kv.Key, out var movieSet) ? movieSet.Count : 0,
                    Id = tagIdMap.TryGetValue(kv.Key, out var tid) ? tid : "",
                    ItemTypes = tagTypes.TryGetValue(kv.Key, out var typeSet2) ? typeSet2.ToList() : new List<string>()
                })
                .OrderBy(t => t.Name)
                .ToList();
            return new GetManagedTagsResponse { Tags = tags };
        }

        public object Get(GetManagedCollectionsRequest request)
        {
            var collections = _libraryManager.GetItemList(new InternalItemsQuery
            {
                IncludeItemTypes = new[] { "BoxSet" },
                Recursive = true
            });
            var result = new List<ManagedCollectionInfo>();
            foreach (var c in collections)
            {
                var childCount = _libraryManager.GetItemList(new InternalItemsQuery { CollectionIds = new[] { c.InternalId }, IsVirtualItem = false }).Count();
                result.Add(new ManagedCollectionInfo
                {
                    Id = c.Id.ToString("N"),
                    Name = c.Name ?? "",
                    ItemCount = childCount
                });
            }
            result.Sort((a, b) => string.Compare(a.Name, b.Name, StringComparison.OrdinalIgnoreCase));
            return new GetManagedCollectionsResponse { Collections = result };
        }

        public object Get(GetManagedPlaylistsRequest request)
        {
            var playlists = _libraryManager.GetItemList(new InternalItemsQuery
            {
                IncludeItemTypes = new[] { "Playlist" },
                Recursive = true
            });
            var result = new List<ManagedPlaylistInfo>();
            foreach (var p in playlists)
            {
                var childCount = _libraryManager.GetItemList(new InternalItemsQuery { ListIds = new[] { p.InternalId } }).Count();
                result.Add(new ManagedPlaylistInfo
                {
                    Id = p.Id.ToString("N"),
                    Name = p.Name ?? "",
                    ItemCount = childCount
                });
            }
            result.Sort((a, b) => string.Compare(a.Name, b.Name, StringComparison.OrdinalIgnoreCase));
            return new GetManagedPlaylistsResponse { Playlists = result };
        }

        public object Post(DeleteManagedTagRequest request)
        {
            if (string.IsNullOrWhiteSpace(request.TagName))
                return new DeleteManagedTagResponse { Success = false, Message = "TagName is required." };

            var tagName = request.TagName.Trim();

            // Remove from real-time cache first — otherwise UpdateItem fires ItemUpdated
            // which triggers ProcessItem in ServerEntryPoint and immediately re-adds the tag.
            TagCacheManager.Instance.RemoveTagFromAllEntries(tagName);
            TagCacheManager.Instance.Save();

            var allItems = _libraryManager.GetItemList(new InternalItemsQuery
            {
                Recursive = true,
                IsVirtualItem = false,
                Tags = new[] { tagName }
            });
            int updated = 0;
            foreach (var item in allItems)
            {
                if (item.Tags == null) continue;
                item.RemoveTag(tagName);
                try { _libraryManager.UpdateItem(item, item.Parent, ItemUpdateType.MetadataEdit, null); updated++; }
                catch { /* best effort */ }
            }
            return new DeleteManagedTagResponse { Success = true, ItemsUpdated = updated };
        }

        public object Post(DeleteManagedTagsBatchRequest request)
        {
            var tagNames = (request.TagNames ?? new List<string>())
                .Select(t => t?.Trim()).Where(t => !string.IsNullOrEmpty(t)).ToList();

            if (tagNames.Count == 0)
                return new DeleteManagedTagsResponse { Success = true };

            foreach (var tag in tagNames)
                TagCacheManager.Instance.RemoveTagFromAllEntries(tag);
            TagCacheManager.Instance.Save();

            var allItems = _libraryManager.GetItemList(new InternalItemsQuery
            {
                Recursive = true,
                IsVirtualItem = false,
                IncludeItemTypes = new[] { "Movie", "Series", "Episode", "Season", "Audio", "MusicVideo", "MusicAlbum", "MusicArtist", "Book", "Game", "Trailer", "Video", "Person", "BoxSet", "Photo", "PhotoAlbum", "Playlist", "Recording", "Studio" }
            }).ToList();

            // Also fetch extras (ExtraType = ThemeSong, BehindTheScenes, etc.) which are excluded by default
            try
            {
                var extraQuery = new InternalItemsQuery { Recursive = true, IsVirtualItem = false };
                var extraTypesProp = typeof(InternalItemsQuery).GetProperty("ExtraTypes");
                if (extraTypesProp != null)
                {
                    var elemType = extraTypesProp.PropertyType.GetElementType();
                    if (elemType != null && elemType.IsEnum)
                    {
                        var all = System.Enum.GetValues(elemType);
                        var arr = System.Array.CreateInstance(elemType, all.Length);
                        all.CopyTo(arr, 0);
                        extraTypesProp.SetValue(extraQuery, arr);
                    }
                }
                var seenIds = new HashSet<Guid>(allItems.Select(i => i.Id));
                foreach (var extra in _libraryManager.GetItemList(extraQuery))
                    if (seenIds.Add(extra.Id)) allItems.Add(extra);
            }
            catch { }

            int updated = 0;
            foreach (var item in allItems)
            {
                if (item.Tags == null || item.Tags.Length == 0) continue;
                bool changed = false;
                foreach (var tag in tagNames)
                {
                    if (item.Tags.Any(t => string.Equals(t, tag, StringComparison.OrdinalIgnoreCase)))
                    {
                        item.RemoveTag(tag);
                        changed = true;
                    }
                }
                if (!changed) continue;
                try { _libraryManager.UpdateItem(item, item.Parent, ItemUpdateType.MetadataEdit, null); updated++; }
                catch { /* best effort */ }
            }
            return new DeleteManagedTagsResponse { Success = true, ItemsUpdated = updated };
        }

        public object Post(DeleteManagedCollectionRequest request)
        {
            if (string.IsNullOrWhiteSpace(request.CollectionId))
                return new DeleteManagedCollectionResponse { Success = false, Message = "CollectionId is required." };
            if (!Guid.TryParse(request.CollectionId, out var guid))
                return new DeleteManagedCollectionResponse { Success = false, Message = "Invalid CollectionId." };

            var item = _libraryManager.GetItemById(guid);
            if (item == null)
                return new DeleteManagedCollectionResponse { Success = false, Message = "Collection not found." };

            try
            {
                _libraryManager.DeleteItem(item, new DeleteOptions { DeleteFileLocation = true });
                return new DeleteManagedCollectionResponse { Success = true };
            }
            catch (Exception ex)
            {
                return new DeleteManagedCollectionResponse { Success = false, Message = ex.Message };
            }
        }

        public object Post(PrepareTopListFolderRequest request)
        {
            try
            {
                var dataPath = Plugin.Instance.DataFolderPath;
                var sanitized = SanitizeFolderName(request.TagName);
                var folderPath = Path.Combine(dataPath, "toplists", sanitized);
                Directory.CreateDirectory(folderPath);

                // Remove stale files from a previous run
                foreach (var f in Directory.GetFiles(folderPath, "*.strm"))
                    File.Delete(f);
                foreach (var f in Directory.GetFiles(folderPath, "*.nfo"))
                    File.Delete(f);
                foreach (var f in Directory.GetFiles(folderPath, "*.jpg"))
                    File.Delete(f);

                // Write one .strm file per movie that carries this tag
                var items = _libraryManager.GetItemList(new InternalItemsQuery
                {
                    Tags = new[] { request.TagName },
                    IncludeItemTypes = new[] { "Movie" },
                    Recursive = true,
                    IsVirtualItem = false
                }).ToList();

                // Sort by saved rank order from the last task run (preserves external list order)
                var rankFile = Path.Combine(Plugin.Instance.DataFolderPath, "tag_ranks", sanitized + ".json");
                if (File.Exists(rankFile))
                {
                    try
                    {
                        var rankIds = _jsonSerializer.DeserializeFromFile<List<string>>(rankFile);
                        if (rankIds != null && rankIds.Count > 0)
                        {
                            var rankMap = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                            for (int i = 0; i < rankIds.Count; i++)
                                if (!string.IsNullOrEmpty(rankIds[i])) rankMap[rankIds[i]] = i;
                            items = items.OrderBy(item =>
                            {
                                var imdb = item.GetProviderId("Imdb");
                                return (!string.IsNullOrEmpty(imdb) && rankMap.TryGetValue(imdb, out var rank)) ? rank : int.MaxValue;
                            }).ToList();
                        }
                    }
                    catch { }
                }

                // First pass: deduplicate and preserve query order
                var seenKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                var selected = new List<(string BaseName, string FilePath, string? PosterPath)>();
                foreach (var item in items)
                {
                    if (string.IsNullOrEmpty(item.Path)) continue;
                    var baseName = SanitizeFolderName(item.Name);
                    if (item.ProductionYear.HasValue && item.ProductionYear > 0)
                        baseName += $" ({item.ProductionYear})";
                    if (!seenKeys.Add(baseName)) continue;
                    var posterPath = item.ImageInfos?.FirstOrDefault(i => i.Type == ImageType.Primary)?.Path;
                    selected.Add((baseName, item.Path, posterPath));
                }

                // Apply max-items limit before writing
                if (request.MaxItems > 0 && selected.Count > request.MaxItems)
                    selected = selected.Take(request.MaxItems).ToList();

                // Second pass: write .strm, .nfo and ranked poster
                int digits = Math.Max(2, selected.Count.ToString().Length);
                int count = 0;
                foreach (var entry in selected)
                {
                    count++;
                    var sortPrefix = count.ToString().PadLeft(digits, '0');
                    var fileName = entry.BaseName;
                    File.WriteAllText(Path.Combine(folderPath, fileName + ".strm"), entry.FilePath);
                    var nfo = $"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<movie>\n  <sorttitle>{sortPrefix}</sorttitle>\n  <lockedfields>SortName|Images</lockedfields>\n</movie>";
                    File.WriteAllText(Path.Combine(folderPath, fileName + ".nfo"), nfo);
                    if (!string.IsNullOrEmpty(entry.PosterPath) && File.Exists(entry.PosterPath))
                        try { CreateRankedPoster(entry.PosterPath, count, Path.Combine(folderPath, fileName + ".jpg"), request.BadgeStyle); }
                        catch { }
                }

                return new PrepareTopListFolderResponse { Success = true, FolderPath = folderPath, FilesCreated = count };
            }
            catch (Exception ex)
            {
                return new PrepareTopListFolderResponse { Success = false, Message = ex.Message };
            }
        }

        public object Get(GetAllMoviesRequest request)
        {
            try
            {
                var items = _libraryManager.GetItemList(new InternalItemsQuery
                {
                    IncludeItemTypes = new[] { "Movie" },
                    Recursive = true,
                    IsVirtualItem = false
                });
                var toplistsFolder = Path.Combine(Plugin.Instance.DataFolderPath, "toplists");

                // Deduplicate: movies with multiple versions (1080p + 4K) appear as separate
                // library items but share the same IMDB ID — keep only one per unique title.
                var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                var movies = items
                    .Where(i => !string.IsNullOrEmpty(i.Path)
                             && !i.Path.StartsWith(toplistsFolder, StringComparison.OrdinalIgnoreCase))
                    .OrderBy(i => i.Name, StringComparer.OrdinalIgnoreCase)
                    .ThenBy(i => i.ProductionYear)
                    .Select(i => new MovieItem
                    {
                        Name   = i.Name ?? "",
                        Year   = i.ProductionYear,
                        ImdbId = i.GetProviderId("Imdb") ?? "",
                        ItemId = i.Id.ToString("N")
                    })
                    .Where(m =>
                    {
                        var key = !string.IsNullOrEmpty(m.ImdbId)
                            ? m.ImdbId
                            : $"{m.Name}|{m.Year}";
                        return seen.Add(key);
                    })
                    .ToList();

                return new GetAllMoviesResponse { Movies = movies };
            }
            catch { return new GetAllMoviesResponse { Movies = new List<MovieItem>() }; }
        }

        public object Post(GrantTopListLibraryAccessRequest request)
        {
            var libraryId = (request.LibraryId ?? "").Trim();
            if (string.IsNullOrEmpty(libraryId))
                return new GrantTopListLibraryAccessResponse { Success = false, Message = "LibraryId required." };

            var bf = System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic
                   | System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.FlattenHierarchy;
            var mgrType = _userManager.GetType();

            // Locate UpdateUserPolicy and GetUserPolicy on the concrete manager type (catches
            // both interface and implementation methods, and handles any number of overloads).
            var updateMethod = mgrType.GetMethods(bf)
                .Where(m => m.Name == "UpdateUserPolicy")
                .OrderBy(m => m.GetParameters().Length)
                .FirstOrDefault()
                ?? typeof(IUserManager).GetMethods().FirstOrDefault(m => m.Name == "UpdateUserPolicy");

            var getPolicyMethod = mgrType.GetMethods(bf)
                .Where(m => m.Name == "GetUserPolicy")
                .OrderBy(m => m.GetParameters().Length)
                .FirstOrDefault()
                ?? typeof(IUserManager).GetMethods().FirstOrDefault(m => m.Name == "GetUserPolicy");

            int updated = 0;
            var errors = new List<string>();

            try
            {
                var users = _userManager.GetUserList(new UserQuery { IsDisabled = false });

                foreach (var user in users)
                {
                    try
                    {
                        // Prefer GetUserPolicy (fresh from store) over the cached User.Policy property.
                        object policy = null;
                        if (getPolicyMethod != null)
                        {
                            try
                            {
                                var gpParams = getPolicyMethod.GetParameters();
                                var gpArg0   = BuildUserArg(gpParams[0].ParameterType, user);
                                var gpArgs   = BuildArgList(gpParams, gpArg0, null);
                                policy = getPolicyMethod.Invoke(_userManager, gpArgs);
                            }
                            catch { }
                        }
                        if (policy == null)
                            policy = user.GetType().GetProperty("Policy")?.GetValue(user);
                        if (policy == null) { errors.Add($"no policy for {user.Name}"); continue; }

                        var enableAllProp = policy.GetType().GetProperty("EnableAllFolders");
                        if (enableAllProp?.GetValue(policy) is true) continue;

                        var foldersProp = policy.GetType().GetProperty("EnabledFolders");
                        var folders = foldersProp?.GetValue(policy) as string[] ?? Array.Empty<string>();
                        if (folders.Any(f => string.Equals(f, libraryId, StringComparison.OrdinalIgnoreCase)))
                            continue;

                        foldersProp?.SetValue(policy, folders.Concat(new[] { libraryId }).ToArray());

                        if (updateMethod == null) { errors.Add("UpdateUserPolicy not found"); break; }

                        var upParams = updateMethod.GetParameters();
                        var upArg0   = BuildUserArg(upParams[0].ParameterType, user);
                        var upArgs   = BuildArgList(upParams, upArg0, policy);
                        updateMethod.Invoke(_userManager, upArgs);
                        updated++;
                    }
                    catch (Exception ex) { errors.Add($"{user.Name}: {ex.GetBaseException().Message}"); }
                }
            }
            catch (Exception ex)
            {
                return new GrantTopListLibraryAccessResponse { Success = false, Message = ex.Message };
            }

            var msg = $"Updated {updated} user(s)";
            if (errors.Count > 0) msg += $" — errors: {string.Join("; ", errors.Take(5))}";
            return new GrantTopListLibraryAccessResponse { Success = true, UsersUpdated = updated, Message = msg };
        }

        public object Post(SnapshotPoliciesRequest request)
        {
            var bf = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.FlattenHierarchy;
            var mgrType = _userManager.GetType();
            var getPolicyMethod = mgrType.GetMethods(bf)
                .Where(m => m.Name == "GetUserPolicy").OrderBy(m => m.GetParameters().Length).FirstOrDefault()
                ?? typeof(IUserManager).GetMethods().FirstOrDefault(m => m.Name == "GetUserPolicy");

            var snapshots = new List<PolicySnapshot>();
            try
            {
                var users = _userManager.GetUserList(new UserQuery { IsDisabled = false });
                foreach (var user in users)
                {
                    try
                    {
                        object policy = null;
                        if (getPolicyMethod != null)
                        {
                            try
                            {
                                var gp = getPolicyMethod.GetParameters();
                                policy = getPolicyMethod.Invoke(_userManager, BuildArgList(gp, BuildUserArg(gp[0].ParameterType, user), null));
                            }
                            catch { }
                        }
                        if (policy == null) policy = user.GetType().GetProperty("Policy")?.GetValue(user);
                        if (policy == null) continue;

                        var enableAllProp = policy.GetType().GetProperty("EnableAllFolders");
                        var foldersProp = policy.GetType().GetProperty("EnabledFolders");
                        snapshots.Add(new PolicySnapshot
                        {
                            UserId = user.Id.ToString(),
                            EnableAllFolders = enableAllProp?.GetValue(policy) is true,
                            EnabledFolders = foldersProp?.GetValue(policy) as string[] ?? Array.Empty<string>()
                        });
                    }
                    catch { }
                }
            }
            catch (Exception ex)
            {
                return new SnapshotPoliciesResponse { Success = false, Message = ex.Message };
            }

            var snapshotId = Guid.NewGuid().ToString("N");
            _policySnapshots[snapshotId] = snapshots;

            // Prune old snapshots to avoid unbounded growth (keep max 20)
            if (_policySnapshots.Count > 20)
            {
                foreach (var key in _policySnapshots.Keys.OrderBy(k => k).Take(_policySnapshots.Count - 20).ToList())
                    _policySnapshots.TryRemove(key, out _);
            }

            return new SnapshotPoliciesResponse { Success = true, SnapshotId = snapshotId, UserCount = snapshots.Count };
        }

        public object Post(RestoreAndGrantAccessRequest request)
        {
            if (string.IsNullOrEmpty(request.SnapshotId) || string.IsNullOrEmpty(request.LibraryId))
                return new RestoreAndGrantAccessResponse { Success = false, Message = "SnapshotId and LibraryId required." };

            if (!_policySnapshots.TryRemove(request.SnapshotId, out var snapshots) || snapshots == null)
                return new RestoreAndGrantAccessResponse { Success = false, Message = "Snapshot not found or already used." };

            var libraryId = request.LibraryId.Trim();
            var bf = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.FlattenHierarchy;
            var mgrType = _userManager.GetType();
            var getPolicyMethod = mgrType.GetMethods(bf)
                .Where(m => m.Name == "GetUserPolicy").OrderBy(m => m.GetParameters().Length).FirstOrDefault()
                ?? typeof(IUserManager).GetMethods().FirstOrDefault(m => m.Name == "GetUserPolicy");
            var updateMethod = mgrType.GetMethods(bf)
                .Where(m => m.Name == "UpdateUserPolicy").OrderBy(m => m.GetParameters().Length).FirstOrDefault()
                ?? typeof(IUserManager).GetMethods().FirstOrDefault(m => m.Name == "UpdateUserPolicy");

            if (updateMethod == null)
                return new RestoreAndGrantAccessResponse { Success = false, Message = "UpdateUserPolicy not found." };

            var users = _userManager.GetUserList(new UserQuery { IsDisabled = false });
            var userDict = users.ToDictionary(u => u.Id.ToString(), u => u, StringComparer.OrdinalIgnoreCase);
            int updated = 0;
            var errors = new List<string>();

            foreach (var snap in snapshots)
            {
                // Users who already had EnableAllFolders=true before creation have access to everything — skip.
                if (snap.EnableAllFolders) continue;

                if (!userDict.TryGetValue(snap.UserId, out var user)) continue;
                try
                {
                    object policy = null;
                    if (getPolicyMethod != null)
                    {
                        try
                        {
                            var gp = getPolicyMethod.GetParameters();
                            policy = getPolicyMethod.Invoke(_userManager, BuildArgList(gp, BuildUserArg(gp[0].ParameterType, user), null));
                        }
                        catch { }
                    }
                    if (policy == null) policy = user.GetType().GetProperty("Policy")?.GetValue(user);
                    if (policy == null) continue;

                    // Restore to exact pre-creation state: EnableAllFolders=false + original folders + new library.
                    var enableAllProp = policy.GetType().GetProperty("EnableAllFolders");
                    enableAllProp?.SetValue(policy, false);

                    var foldersProp = policy.GetType().GetProperty("EnabledFolders");
                    var folders = snap.EnabledFolders.ToList();
                    if (!folders.Any(f => string.Equals(f, libraryId, StringComparison.OrdinalIgnoreCase)))
                        folders.Add(libraryId);
                    foldersProp?.SetValue(policy, folders.ToArray());

                    var up = updateMethod.GetParameters();
                    updateMethod.Invoke(_userManager, BuildArgList(up, BuildUserArg(up[0].ParameterType, user), policy));
                    updated++;
                }
                catch (Exception ex) { errors.Add($"{user.Name}: {ex.GetBaseException().Message}"); }
            }

            var msg = $"Restored access for {updated} user(s)";
            if (errors.Count > 0) msg += $" — errors: {string.Join("; ", errors.Take(5))}";
            return new RestoreAndGrantAccessResponse { Success = true, UsersUpdated = updated, Message = msg };
        }

        private object BuildUserArg(Type paramType, BaseItem user)
        {
            if (paramType == typeof(long) || paramType == typeof(Int64))
                return _userManager.GetInternalId(user.Id.ToString());
            if (paramType == typeof(Guid))   return user.Id;
            if (paramType == typeof(string)) return user.Id.ToString();
            return user;
        }

        private static object[] BuildArgList(System.Reflection.ParameterInfo[] parms, object arg0, object arg1)
        {
            var args = new object[parms.Length];
            args[0] = arg0;
            for (int i = 1; i < parms.Length; i++)
            {
                if (i == 1 && arg1 != null)                             args[i] = arg1;
                else if (parms[i].ParameterType == typeof(CancellationToken)) args[i] = CancellationToken.None;
                else if (parms[i].HasDefaultValue)                      args[i] = parms[i].DefaultValue;
                else                                                    args[i] = null;
            }
            return args;
        }

        public object Post(PrepareManualTopListFolderRequest request)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(request.ListName))
                    return new PrepareTopListFolderResponse { Success = false, Message = "ListName is required." };

                var dataPath   = Plugin.Instance.DataFolderPath;
                var sanitized  = SanitizeFolderName(request.ListName);
                var folderPath = Path.Combine(dataPath, "toplists", sanitized);
                Directory.CreateDirectory(folderPath);

                foreach (var f in Directory.GetFiles(folderPath, "*.strm")) File.Delete(f);
                foreach (var f in Directory.GetFiles(folderPath, "*.nfo"))  File.Delete(f);
                foreach (var f in Directory.GetFiles(folderPath, "*.jpg"))  File.Delete(f);

                var items    = request.Items ?? new List<ManualTopListItem>();
                var seenKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                var selected = new List<(string BaseName, string FilePath, string? PosterPath)>();

                foreach (var entry in items)
                {
                    if (!Guid.TryParse(entry.ItemId, out var guid)) continue;
                    var mediaItem = _libraryManager.GetItemById(guid);
                    if (mediaItem == null || string.IsNullOrEmpty(mediaItem.Path)) continue;
                    var baseName = SanitizeFolderName(mediaItem.Name);
                    if (mediaItem.ProductionYear.HasValue && mediaItem.ProductionYear > 0)
                        baseName += $" ({mediaItem.ProductionYear})";
                    if (!seenKeys.Add(baseName)) continue;
                    var posterPath = mediaItem.ImageInfos?.FirstOrDefault(i => i.Type == ImageType.Primary)?.Path;
                    selected.Add((baseName, mediaItem.Path, posterPath));
                }

                int digits = Math.Max(2, selected.Count.ToString().Length);
                int count  = 0;
                foreach (var entry in selected)
                {
                    count++;
                    var sortPrefix = count.ToString().PadLeft(digits, '0');
                    File.WriteAllText(Path.Combine(folderPath, entry.BaseName + ".strm"), entry.FilePath);
                    var nfo = $"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<movie>\n  <sorttitle>{sortPrefix}</sorttitle>\n  <lockedfields>SortName|Images</lockedfields>\n</movie>";
                    File.WriteAllText(Path.Combine(folderPath, entry.BaseName + ".nfo"), nfo);
                    if (!string.IsNullOrEmpty(entry.PosterPath) && File.Exists(entry.PosterPath))
                        try { CreateRankedPoster(entry.PosterPath, count, Path.Combine(folderPath, entry.BaseName + ".jpg"), request.BadgeStyle); }
                        catch { }
                }

                try
                {
                    var rankDir = Path.Combine(dataPath, "tag_ranks");
                    Directory.CreateDirectory(rankDir);
                    var rankIds = items.Where(i => !string.IsNullOrWhiteSpace(i.ImdbId))
                                       .Select(i => i.ImdbId).ToList();
                    _jsonSerializer.SerializeToFile(rankIds, Path.Combine(rankDir, sanitized + ".json"));
                }
                catch { }

                // Update ForcedSortName directly in the library database so the new order applies
                // immediately. MetadataRefreshMode=Default (used in the UI scan) won't override
                // locked SortName fields, so we must push the change through UpdateItem.
                try
                {
                    var sortPaths = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                    for (int si = 0; si < selected.Count; si++)
                        sortPaths[Path.Combine(folderPath, selected[si].BaseName + ".strm")]
                            = (si + 1).ToString().PadLeft(digits, '0');

                    var libItems = _libraryManager.GetItemList(new InternalItemsQuery
                    {
                        Recursive = true,
                        IncludeItemTypes = new[] { "Movie" },
                        IsVirtualItem = false
                    }).Where(i => !string.IsNullOrEmpty(i.Path)
                               && i.Path.StartsWith(folderPath + Path.DirectorySeparatorChar,
                                                    StringComparison.OrdinalIgnoreCase))
                      .ToList();

                    foreach (var li in libItems)
                    {
                        if (!sortPaths.TryGetValue(li.Path, out var newSort)) continue;
                        var prop = li.GetType().GetProperty("SortName");
                        if (prop?.CanWrite == true) prop.SetValue(li, newSort);
                        try { _libraryManager.UpdateItem(li, li.Parent, ItemUpdateType.MetadataEdit, null); }
                        catch { }
                    }
                }
                catch { }

                return new PrepareTopListFolderResponse { Success = true, FolderPath = folderPath, FilesCreated = count };
            }
            catch (Exception ex)
            {
                return new PrepareTopListFolderResponse { Success = false, Message = ex.Message };
            }
        }

        internal static void CreateRankedPoster(string sourcePath, int rank, string outputPath, string badgeStyle = "neutral")
        {
            using var original = SKBitmap.Decode(sourcePath);
            if (original == null) return;

            using var surface = SKSurface.Create(new SKImageInfo(original.Width, original.Height));
            var canvas = surface.Canvas;
            canvas.DrawBitmap(original, 0, 0);

            float radius = original.Width * 0.15f;
            float margin = original.Width * 0.04f;
            float cx = margin + radius;
            float cy = margin + radius;

            SKColor bgColor;
            SKColor textColor;
            switch (badgeStyle?.ToLowerInvariant())
            {
                case "slate-grey":
                    bgColor   = new SKColor(0x41, 0x41, 0x4B, 224);
                    textColor = SKColors.White;
                    break;
                case "emby-green":
                    bgColor   = new SKColor(0x52, 0xB5, 0x4B, 200);
                    textColor = SKColors.White;
                    break;
                case "ocean-blue":
                    bgColor   = new SKColor(0x2E, 0x86, 0xC1, 210);
                    textColor = SKColors.White;
                    break;
                case "soft-red":
                    bgColor   = new SKColor(0xC9, 0x45, 0x45, 210);
                    textColor = SKColors.White;
                    break;
                case "violet":
                    bgColor   = new SKColor(0x7B, 0x52, 0xB5, 210);
                    textColor = SKColors.White;
                    break;
                default:
                    bgColor   = new SKColor(0, 0, 0, 210);
                    textColor = SKColors.White;
                    break;
            }

            using var bgPaint = new SKPaint { Color = bgColor, IsAntialias = true };
            canvas.DrawCircle(cx, cy, radius, bgPaint);

            var text = rank.ToString();
            float fontSize = radius * 1.1f;

            using var fontStream = System.Reflection.Assembly.GetExecutingAssembly()
                .GetManifestResourceStream("HomeScreenCompanion.LemonMilk.otf");
            using var typeface = fontStream != null ? SKTypeface.FromStream(fontStream) : SKTypeface.Default;
            using var textPaint = new SKPaint
            {
                Color = textColor,
                TextSize = fontSize,
                IsAntialias = true,
                Typeface = typeface
            };

            float maxTextWidth = radius * 1.6f;
            while (textPaint.MeasureText(text) > maxTextWidth && textPaint.TextSize > 1f)
                textPaint.TextSize -= 1f;

            float textWidth = textPaint.MeasureText(text);
            var metrics = textPaint.FontMetrics;
            float textX = cx - textWidth / 2;
            float textY = cy - (metrics.Ascent + metrics.Descent) / 2;
            canvas.DrawText(text, textX, textY, textPaint);

            using var image = surface.Snapshot();
            using var data = image.Encode(SKEncodedImageFormat.Jpeg, 92);
            using var stream = File.Create(outputPath);
            data.SaveTo(stream);
        }

        public object Get(GetTopListsRequest request)
        {
            try
            {
                var dataPath = Plugin.Instance.DataFolderPath;
                var topListsPath = Path.Combine(dataPath, "toplists");
                var folderNames = new List<string>();
                var movieCounts = new Dictionary<string, int>();
                if (Directory.Exists(topListsPath))
                {
                    foreach (var dir in Directory.GetDirectories(topListsPath))
                    {
                        var name = Path.GetFileName(dir);
                        folderNames.Add(name);
                        movieCounts[name.ToLowerInvariant()] = Directory.GetFiles(dir, "*.strm").Length;
                    }
                }
                return new GetTopListsResponse { FolderNames = folderNames, MovieCounts = movieCounts };
            }
            catch
            {
                return new GetTopListsResponse { FolderNames = new List<string>() };
            }
        }

        public object Get(GetManualTopListItemsRequest request)
        {
            try
            {
                var sanitized  = SanitizeFolderName(request.ListName);
                var folderPath = Path.Combine(Plugin.Instance.DataFolderPath, "toplists", sanitized);
                if (!Directory.Exists(folderPath))
                    return new GetManualTopListItemsResponse { Success = false, Message = "Folder not found." };

                var config   = Plugin.Instance.Configuration;
                var tlConfig = (config.TopLists ?? new System.Collections.Generic.List<TopListHomeSection>())
                    .FirstOrDefault(t => string.Equals(SanitizeFolderName(t.TagName), sanitized, StringComparison.OrdinalIgnoreCase));

                var customName  = "";
                var displayMode = "";
                var imageType   = "";
                var badgeStyle  = "neutral";
                var userIds     = new List<string>();
                if (tlConfig != null)
                {
                    try
                    {
                        var settings = _jsonSerializer.DeserializeFromString<Dictionary<string, string>>(tlConfig.HomeSectionSettings ?? "{}") ?? new Dictionary<string, string>();
                        customName  = settings.TryGetValue("CustomName",  out var cn) ? cn  : "";
                        displayMode = settings.TryGetValue("DisplayMode", out var dm) ? dm  : "";
                        imageType   = settings.TryGetValue("ImageType",   out var it) ? it  : "";
                        badgeStyle  = settings.TryGetValue("BadgeStyle",  out var bs) ? bs  : "neutral";
                    }
                    catch { }
                    userIds = tlConfig.HomeSectionUserIds ?? new List<string>();
                }

                // Read .strm files sorted by rank from .nfo sorttitle
                var strmFiles = Directory.GetFiles(folderPath, "*.strm");
                var entries   = new List<(int Rank, string MoviePath)>();
                foreach (var strmFile in strmFiles)
                {
                    var baseName = Path.GetFileNameWithoutExtension(strmFile);
                    var nfoFile  = Path.Combine(folderPath, baseName + ".nfo");
                    int rank     = int.MaxValue;
                    if (File.Exists(nfoFile))
                    {
                        try
                        {
                            var nfoContent = File.ReadAllText(nfoFile);
                            var match = System.Text.RegularExpressions.Regex.Match(nfoContent, @"<sorttitle>(\d+)</sorttitle>");
                            if (match.Success) rank = int.Parse(match.Groups[1].Value);
                        }
                        catch { }
                    }
                    entries.Add((rank, File.ReadAllText(strmFile).Trim()));
                }
                entries.Sort((a, b) => a.Rank.CompareTo(b.Rank));

                // Build path→item lookup once
                var allItems = _libraryManager.GetItemList(new InternalItemsQuery
                {
                    IncludeItemTypes = new[] { "Movie" },
                    Recursive = true,
                    IsVirtualItem = false
                })
                .Where(i => !string.IsNullOrEmpty(i.Path))
                .GroupBy(i => i.Path, StringComparer.OrdinalIgnoreCase)
                .ToDictionary(g => g.Key, g => g.First(), StringComparer.OrdinalIgnoreCase);

                var movies = new List<MovieItem>();
                foreach (var entry in entries)
                {
                    if (allItems.TryGetValue(entry.MoviePath, out var item))
                    {
                        movies.Add(new MovieItem
                        {
                            Name   = item.Name ?? "",
                            Year   = item.ProductionYear,
                            ImdbId = item.GetProviderId("Imdb") ?? "",
                            ItemId = item.Id.ToString("N")
                        });
                    }
                }

                return new GetManualTopListItemsResponse
                {
                    Success     = true,
                    Movies      = movies,
                    CustomName  = customName,
                    DisplayMode = displayMode,
                    ImageType   = imageType,
                    BadgeStyle  = badgeStyle,
                    UserIds     = userIds
                };
            }
            catch (Exception ex)
            {
                return new GetManualTopListItemsResponse { Success = false, Message = ex.Message };
            }
        }

        public object Post(DeleteTopListRequest request)
        {
            try
            {
                var dataPath = Plugin.Instance.DataFolderPath;
                var sanitized = SanitizeFolderName(request.TagName);
                var folderPath = Path.Combine(dataPath, "toplists", sanitized);
                if (Directory.Exists(folderPath))
                    Directory.Delete(folderPath, true);

                var config = Plugin.Instance?.Configuration;
                if (config?.TopLists != null)
                {
                    var tl = config.TopLists.FirstOrDefault(t =>
                        string.Equals(SanitizeFolderName(t.TagName), sanitized, StringComparison.OrdinalIgnoreCase));
                    if (tl != null)
                    {
                        foreach (var tracking in tl.HomeSectionTracked ?? new List<HomeSectionTracking>())
                        {
                            if (string.IsNullOrEmpty(tracking.UserId) || string.IsNullOrEmpty(tracking.SectionId)) continue;
                            try
                            {
                                var internalId = _userManager.GetInternalId(tracking.UserId);
                                _userManager.DeleteHomeSections(internalId, new[] { tracking.SectionId }, CancellationToken.None);
                            }
                            catch { }
                        }
                        config.TopLists.Remove(tl);
                        Plugin.Instance.SaveConfiguration();
                    }
                }

                return new DeleteTopListResponse { Success = true, FolderPath = folderPath };
            }
            catch (Exception ex)
            {
                return new DeleteTopListResponse { Success = false, Message = ex.Message };
            }
        }

        public object Post(PrepareTopListHomeSectionsRequest request)
        {
            try
            {
                var config = Plugin.Instance?.Configuration;
                if (config == null)
                    return new PrepareTopListHomeSectionsResponse { Success = false, Message = "Plugin configuration not available." };

                var tl = config.TopLists?.FirstOrDefault(t =>
                    string.Equals(t.TagName, request.TagName, StringComparison.OrdinalIgnoreCase));

                if (tl == null)
                    return new PrepareTopListHomeSectionsResponse { Success = false, Message = $"TopList '{request.TagName}' not found in config." };

                var settingsDict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                try
                {
                    if (!string.IsNullOrEmpty(tl.HomeSectionSettings) && tl.HomeSectionSettings != "{}")
                        settingsDict = _jsonSerializer.DeserializeFromString<Dictionary<string, string>>(tl.HomeSectionSettings) ?? settingsDict;
                }
                catch { }

                if (!settingsDict.ContainsKey("SectionType"))
                    settingsDict["SectionType"] = "items";

                if (string.IsNullOrEmpty(tl.HomeSectionLibraryId) || tl.HomeSectionLibraryId == "auto")
                    return new PrepareTopListHomeSectionsResponse { Success = false, Message = "HomeSectionLibraryId is not set — library may not be ready yet." };

                var resolvedLibraryId = tl.HomeSectionLibraryId;

                // Exclude ALL libraries except this top-list's own; use GetUserViews to capture
                // Live TV's user-view ID (GetVirtualFolders does not include Live TV).
                var allLibIds = _libraryManager.GetVirtualFolders()
                    .Where(f => !string.IsNullOrEmpty(f.ItemId))
                    .Select(f => f.ItemId.Trim().ToLowerInvariant())
                    .ToList();
                var firstUserIdForViews = tl.HomeSectionUserIds?.FirstOrDefault();
                if (!string.IsNullOrEmpty(firstUserIdForViews))
                {
                    try
                    {
                        var uid = _userManager.GetInternalId(firstUserIdForViews);
                        Guid.TryParse(firstUserIdForViews, out var userGuid);
                        var ifMethod = typeof(IUserViewManager).GetMethod("GetUserViews");
                        if (ifMethod != null)
                        {
                            var queryParams = ifMethod.GetParameters();
                            object queryArg = null;
                            if (queryParams.Length > 0)
                            {
                                try
                                {
                                    queryArg = Activator.CreateInstance(queryParams[0].ParameterType);
                                    var uidProp = queryParams[0].ParameterType.GetProperty("UserId");
                                    if (uidProp?.PropertyType == typeof(long))
                                        uidProp.SetValue(queryArg, uid);
                                    else
                                        uidProp?.SetValue(queryArg, userGuid);
                                }
                                catch { queryArg = null; }
                            }
                            var result = ifMethod.Invoke(_userViewManager, new[] { queryArg });
                            if (result is System.Collections.IEnumerable views)
                                foreach (var v in views)
                                {
                                    var idProp = v?.GetType().GetProperty("Id");
                                    if (idProp?.GetValue(v) is Guid vid && vid != Guid.Empty)
                                        allLibIds.Add(vid.ToString("N").ToLowerInvariant());
                                }
                        }
                    }
                    catch { }
                }
                allLibIds = allLibIds.Distinct().ToList();
                var ownIdLower = resolvedLibraryId.Trim().ToLowerInvariant();
                var storedExclude = (settingsDict.TryGetValue("_queryExcludeViewIds", out var storedEv) ? storedEv : "")
                    .Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries)
                    .Select(s => s.Trim().ToLowerInvariant()).Where(s => s.Length > 0);
                var mergedIds = allLibIds.Concat(storedExclude).Where(id => id != ownIdLower).Distinct().ToList();
                var excStr = string.Join(",", mergedIds);
                settingsDict["_queryExcludeViewIds"] = excStr;
                settingsDict["ExcludedFolders"] = excStr;
                tl.HomeSectionSettings = _jsonSerializer.SerializeToString(settingsDict);

                var safeTag = new string((request.TagName ?? "").Select(c => char.IsLetterOrDigit(c) ? c : '_').ToArray());
                var sectionMarker = "hsc__tl__" + safeTag;

                int created = 0, updated = 0;

                foreach (var userId in (tl.HomeSectionUserIds ?? new System.Collections.Generic.List<string>()))
                {
                    try
                    {
                        var userInternalId = _userManager.GetInternalId(userId);
                        var currentSections = _userManager.GetHomeSections(userInternalId, CancellationToken.None);
                        var allSections = currentSections?.Sections ?? Array.Empty<ContentSection>();

                        var tracked = (tl.HomeSectionTracked ?? new System.Collections.Generic.List<HomeSectionTracking>())
                            .FirstOrDefault(t => t.UserId == userId);

                        ContentSection ownedSection = null;
                        if (tracked != null && !string.IsNullOrEmpty(tracked.SectionId) && !tracked.SectionId.StartsWith("hsc__"))
                            ownedSection = allSections.FirstOrDefault(s => s.Id == tracked.SectionId);
                        if (ownedSection == null)
                            ownedSection = allSections.FirstOrDefault(s => s.Subtitle == sectionMarker);

                        string trackId;
                        if (ownedSection != null)
                        {
                            var updatedSection = HomeScreenCompanionTask.BuildContentSection(_jsonSerializer, settingsDict, resolvedLibraryId, ownedSection);
                            typeof(ContentSection).GetProperty("Id")?.SetValue(updatedSection, ownedSection.Id);
                            _userManager.UpdateHomeSection(userInternalId, updatedSection, CancellationToken.None);
                            trackId = ownedSection.Id ?? sectionMarker;
                            updated++;
                        }
                        else
                        {
                            var beforeIds = new HashSet<string>(
                                allSections.Where(s => !string.IsNullOrEmpty(s.Id)).Select(s => s.Id));
                            _userManager.AddHomeSection(userInternalId,
                                HomeScreenCompanionTask.BuildContentSection(_jsonSerializer, settingsDict, resolvedLibraryId),
                                CancellationToken.None);
                            var afterSections = _userManager.GetHomeSections(userInternalId, CancellationToken.None);
                            var newId = (afterSections?.Sections ?? Array.Empty<ContentSection>())
                                .Where(s => !string.IsNullOrEmpty(s.Id) && !beforeIds.Contains(s.Id))
                                .Select(s => s.Id).FirstOrDefault() ?? "";
                            trackId = !string.IsNullOrEmpty(newId) ? newId : sectionMarker;
                            created++;
                        }

                        if (tracked != null)
                            tracked.SectionId = trackId;
                        else
                        {
                            if (tl.HomeSectionTracked == null) tl.HomeSectionTracked = new System.Collections.Generic.List<HomeSectionTracking>();
                            tl.HomeSectionTracked.Add(new HomeSectionTracking { UserId = userId, SectionId = trackId });
                        }
                    }
                    catch { }
                }

                // Inject the new top-list library into _queryExcludeViewIds of every existing
                // TAG & COLLECT items-type section and apply immediately (no task run needed).
                var resolvedLibraryIdLower = resolvedLibraryId.Trim().ToLowerInvariant();
                if (!string.IsNullOrEmpty(resolvedLibraryId))
                {
                    foreach (var tc in (config.Tags ?? new System.Collections.Generic.List<TagConfig>()))
                    {
                        if (!tc.EnableHomeSection) continue;

                        var tcSettings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                        try
                        {
                            if (!string.IsNullOrEmpty(tc.HomeSectionSettings) && tc.HomeSectionSettings != "{}")
                                tcSettings = _jsonSerializer.DeserializeFromString<Dictionary<string, string>>(tc.HomeSectionSettings) ?? tcSettings;
                        }
                        catch { }

                        tcSettings.TryGetValue("SectionType", out var tcSt);
                        if (tcSt == "boxset") continue;

                        var existingExcluded = (tcSettings.TryGetValue("_queryExcludeViewIds", out var ev) ? ev : "")
                            .Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries)
                            .Select(s => s.Trim()).ToList();

                        if (existingExcluded.Contains(resolvedLibraryIdLower, StringComparer.OrdinalIgnoreCase)) continue;

                        existingExcluded.Add(resolvedLibraryIdLower);
                        var tcExcStr = string.Join(",", existingExcluded);
                        tcSettings["_queryExcludeViewIds"] = tcExcStr;
                        tcSettings["ExcludedFolders"] = tcExcStr;
                        tc.HomeSectionSettings = _jsonSerializer.SerializeToString(tcSettings);

                        // Resolve tag ID for items-type query
                        if (!string.IsNullOrEmpty(tc.Tag))
                        {
                            var tagItem = _libraryManager.GetItemList(new MediaBrowser.Controller.Entities.InternalItemsQuery
                            {
                                IncludeItemTypes = new[] { "Tag" },
                                Name = tc.Tag,
                                Recursive = true
                            }).FirstOrDefault();
                            if (tagItem != null) tcSettings["_queryTagId"] = tagItem.InternalId.ToString();
                        }

                        var tcSafeTag = new string((tc.Name ?? tc.Tag ?? "").Select(c => char.IsLetterOrDigit(c) ? c : '_').ToArray());
                        var tcMarker = "hsc__" + tcSafeTag;

                        var realTracked = (tc.HomeSectionTracked ?? new System.Collections.Generic.List<HomeSectionTracking>())
                            .Where(t => !string.IsNullOrEmpty(t.SectionId) && !t.SectionId.StartsWith("hsc__"))
                            .ToList();

                        foreach (var tracking in realTracked)
                        {
                            try
                            {
                                var uid = _userManager.GetInternalId(tracking.UserId);
                                var secs = _userManager.GetHomeSections(uid, CancellationToken.None)?.Sections ?? Array.Empty<ContentSection>();
                                var owned = secs.FirstOrDefault(s => s.Id == tracking.SectionId)
                                    ?? secs.FirstOrDefault(s => s.Subtitle == tcMarker);
                                if (owned == null) continue;
                                var updatedSec = HomeScreenCompanionTask.BuildContentSection(_jsonSerializer, tcSettings, null, owned);
                                typeof(ContentSection).GetProperty("Id")?.SetValue(updatedSec, owned.Id);
                                _userManager.UpdateHomeSection(uid, updatedSec, CancellationToken.None);
                            }
                            catch { }
                        }
                    }
                }

                // Also inject the new top-list library into other existing top-list sections.
                if (!string.IsNullOrEmpty(resolvedLibraryId))
                {
                    foreach (var otherTl in (config.TopLists ?? new System.Collections.Generic.List<TopListHomeSection>()))
                    {
                        if (otherTl == tl) continue;
                        if (string.IsNullOrEmpty(otherTl.HomeSectionLibraryId) || otherTl.HomeSectionLibraryId == "auto") continue;

                        var otherSettings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                        try
                        {
                            if (!string.IsNullOrEmpty(otherTl.HomeSectionSettings) && otherTl.HomeSectionSettings != "{}")
                                otherSettings = _jsonSerializer.DeserializeFromString<Dictionary<string, string>>(otherTl.HomeSectionSettings) ?? otherSettings;
                        }
                        catch { }

                        var otherExisting = (otherSettings.TryGetValue("_queryExcludeViewIds", out var otherEv) ? otherEv : "")
                            .Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries)
                            .Select(s => s.Trim()).ToList();

                        if (otherExisting.Contains(resolvedLibraryIdLower, StringComparer.OrdinalIgnoreCase)) continue;

                        otherExisting.Add(resolvedLibraryIdLower);
                        var otherExcStr = string.Join(",", otherExisting);
                        otherSettings["_queryExcludeViewIds"] = otherExcStr;
                        otherSettings["ExcludedFolders"] = otherExcStr;
                        otherTl.HomeSectionSettings = _jsonSerializer.SerializeToString(otherSettings);

                        var otherSafeTag = new string((otherTl.TagName ?? "").Select(c => char.IsLetterOrDigit(c) ? c : '_').ToArray());
                        var otherMarker = "hsc__tl__" + otherSafeTag;

                        var otherTracked = (otherTl.HomeSectionTracked ?? new System.Collections.Generic.List<HomeSectionTracking>())
                            .Where(t => !string.IsNullOrEmpty(t.SectionId) && !t.SectionId.StartsWith("hsc__"))
                            .ToList();

                        foreach (var tracking in otherTracked)
                        {
                            try
                            {
                                var uid = _userManager.GetInternalId(tracking.UserId);
                                var secs = _userManager.GetHomeSections(uid, CancellationToken.None)?.Sections ?? Array.Empty<ContentSection>();
                                var owned = secs.FirstOrDefault(s => s.Id == tracking.SectionId)
                                    ?? secs.FirstOrDefault(s => s.Subtitle == otherMarker);
                                if (owned == null) continue;
                                var updatedSec = HomeScreenCompanionTask.BuildContentSection(_jsonSerializer, otherSettings, otherTl.HomeSectionLibraryId, owned);
                                typeof(ContentSection).GetProperty("Id")?.SetValue(updatedSec, owned.Id);
                                _userManager.UpdateHomeSection(uid, updatedSec, CancellationToken.None);
                            }
                            catch { }
                        }
                    }
                }

                // Aggressive: exclude the new top-list library from ALL untracked, non-library-scoped sections
                // so top-list content never bleeds into manually created or native Emby sections.
                HomeScreenCompanionTask.UpdateUntrackedSections(
                    _jsonSerializer, _userManager, config,
                    new[] { resolvedLibraryIdLower },
                    CancellationToken.None);

                Plugin.Instance.SaveConfiguration();
                _taskManager.QueueScheduledTask<TopListSyncTask>();
                return new PrepareTopListHomeSectionsResponse { Success = true, UsersCreated = created, UsersUpdated = updated, Message = $"Synced: {created} created, {updated} updated." };
            }
            catch (Exception ex)
            {
                return new PrepareTopListHomeSectionsResponse { Success = false, Message = ex.Message };
            }
        }

        public object Post(SyncAllTopListSectionsRequest request)
        {
            try
            {
                var (updated, msg) = TopListSyncTask.SyncAll(_libraryManager, _userViewManager, _userManager, _jsonSerializer, CancellationToken.None);
                return new SyncAllTopListSectionsResponse { Success = true, UpdatedSections = updated, Message = msg };
            }
            catch (Exception ex)
            {
                return new SyncAllTopListSectionsResponse { Success = false, Message = ex.Message };
            }
        }

        private static string SanitizeFolderName(string name)
        {
            var invalid = Path.GetInvalidFileNameChars();
            var safe = new string((name ?? "unknown").Select(c => Array.IndexOf(invalid, c) >= 0 ? '_' : c).ToArray()).Trim('.');
            return string.IsNullOrWhiteSpace(safe) ? "unknown" : safe;
        }

    }
}