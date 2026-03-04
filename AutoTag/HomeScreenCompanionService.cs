using MediaBrowser.Common.Net;
using MediaBrowser.Model.Serialization;
using MediaBrowser.Model.Services;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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

    [Route("/HomeScreenCompanion/Hsc/Status", "GET")]
    public class HscGetStatusRequest : IReturn<HscSyncStatusResponse> { }

public class HomeScreenCompanionService : IService
    {
        private readonly IHttpClient _httpClient;
        private readonly IJsonSerializer _jsonSerializer;

        public HomeScreenCompanionService(IHttpClient httpClient, IJsonSerializer jsonSerializer)
        {
            _httpClient = httpClient;
            _jsonSerializer = jsonSerializer;
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

    }
}