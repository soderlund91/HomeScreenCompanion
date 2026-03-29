using MediaBrowser.Common.Net;
using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Library;
using MediaBrowser.Model.Entities;
using MediaBrowser.Model.Querying;
using MediaBrowser.Model.Serialization;
using MediaBrowser.Model.Services;
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
    public class ManagedTagInfo { public string Name { get; set; } = ""; public int ItemCount { get; set; } }
    public class GetManagedTagsResponse { public List<ManagedTagInfo> Tags { get; set; } = new List<ManagedTagInfo>(); }

    [Route("/HomeScreenCompanion/Manage/Collections", "GET")]
    public class GetManagedCollectionsRequest : IReturn<GetManagedCollectionsResponse> { }
    public class ManagedCollectionInfo { public string Id { get; set; } = ""; public string Name { get; set; } = ""; public int ItemCount { get; set; } }
    public class GetManagedCollectionsResponse { public List<ManagedCollectionInfo> Collections { get; set; } = new List<ManagedCollectionInfo>(); }

    [Route("/HomeScreenCompanion/Manage/DeleteTag", "POST")]
    public class DeleteManagedTagRequest : IReturn<DeleteManagedTagResponse> { public string TagName { get; set; } = ""; }
    public class DeleteManagedTagResponse { public bool Success { get; set; } public string Message { get; set; } = ""; public int ItemsUpdated { get; set; } }

    [Route("/HomeScreenCompanion/Manage/DeleteCollection", "POST")]
    public class DeleteManagedCollectionRequest : IReturn<DeleteManagedCollectionResponse> { public string CollectionId { get; set; } = ""; }
    public class DeleteManagedCollectionResponse { public bool Success { get; set; } public string Message { get; set; } = ""; }

public class HomeScreenCompanionService : IService
    {
        private readonly IHttpClient _httpClient;
        private readonly IJsonSerializer _jsonSerializer;
        private readonly IUserManager _userManager;
        private readonly ILibraryManager _libraryManager;
        private readonly IUserDataManager _userDataManager;

        public HomeScreenCompanionService(IHttpClient httpClient, IJsonSerializer jsonSerializer, IUserManager userManager, ILibraryManager libraryManager, IUserDataManager userDataManager)
        {
            _httpClient = httpClient;
            _jsonSerializer = jsonSerializer;
            _userManager = userManager;
            _libraryManager = libraryManager;
            _userDataManager = userDataManager;
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
                var items = await fetcher.FetchItems(request.Url, request.Limit, config.TraktClientId, config.MdblistApiKey, CancellationToken.None);

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
                IncludeItemTypes = new[] { "Movie", "Series", "Season", "Episode" },
                Recursive = true,
                IsVirtualItem = false
            });
            var tagCount = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            foreach (var item in allItems)
            {
                if (item.Tags == null) continue;
                foreach (var tag in item.Tags)
                {
                    if (string.IsNullOrWhiteSpace(tag)) continue;
                    tagCount.TryGetValue(tag, out var c);
                    tagCount[tag] = c + 1;
                }
            }
            var tags = tagCount
                .Select(kv => new ManagedTagInfo { Name = kv.Key, ItemCount = kv.Value })
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

    }
}