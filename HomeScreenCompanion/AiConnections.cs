using MediaBrowser.Controller.Net;
using MediaBrowser.Model.Serialization;
using MediaBrowser.Model.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace HomeScreenCompanion
{
    /// <summary>An OpenAI-compatible AI service (Groq, OpenRouter, Mistral, LM Studio, vLLM …).
    /// A source uses it with AiProvider = "Compat:" + Id.</summary>
    public class AiConnection
    {
        public string Id { get; set; } = "";
        public string Name { get; set; } = "";
        public string BaseUrl { get; set; } = "";
        public string ApiKey { get; set; } = "";
        public string Model { get; set; } = "";

        internal const string ProviderPrefix = "Compat:";

        internal static bool IsCompat(string? provider) =>
            provider != null && provider.StartsWith(ProviderPrefix, StringComparison.OrdinalIgnoreCase);

        internal static AiConnection? Find(IEnumerable<AiConnection>? list, string? provider)
        {
            if (!IsCompat(provider) || list == null) return null;
            var id = provider!.Substring(ProviderPrefix.Length);
            return list.FirstOrDefault(c => c != null && string.Equals(c.Id, id, StringComparison.OrdinalIgnoreCase));
        }

        /// <summary>The provider as the log shows it: a connection by its name.</summary>
        internal static string Label(string? provider)
        {
            if (!IsCompat(provider)) return provider ?? "";
            var c = Find(Plugin.Instance?.Configuration?.AiConnections, provider);
            return c == null ? "a removed AI connection" : (string.IsNullOrWhiteSpace(c.Name) ? "AI connection" : c.Name.Trim());
        }
    }

    // The models a provider offers, for the Model dropdowns in Settings. POST, so a typed key
    // never ends up in a URL (and the request log); a blank key or URL uses the saved one.
    [Route("/HomeScreenCompanion/Ai/Models", "POST")]
    [Authenticated(Roles = "Admin")]
    public class AiModelsRequest : IReturn<AiModelsResponse>
    {
        public string Provider { get; set; } = "";      // OpenAI | Gemini | Claude | Ollama | Compat
        public string ConnectionId { get; set; } = "";  // Compat only
        public string BaseUrl { get; set; } = "";
        public string ApiKey { get; set; } = "";
    }

    public class AiModelsResponse
    {
        public bool Success { get; set; }
        public string Message { get; set; } = "";
        public List<string> Models { get; set; } = new List<string>();
    }

    internal class AiModelList
    {
        public List<AiModelEntry> data { get; set; } = new List<AiModelEntry>();     // OpenAI, compatible, Claude
        public List<AiModelEntry> models { get; set; } = new List<AiModelEntry>();   // Gemini, Ollama
    }

    internal class AiModelEntry
    {
        public string id { get; set; } = "";
        public string name { get; set; } = "";
        public List<string> supportedGenerationMethods { get; set; } = new List<string>();
    }

    public class AiModelsService : IService
    {
        private static readonly HttpClient Http = new HttpClient { Timeout = TimeSpan.FromSeconds(20) };
        private readonly IJsonSerializer _json;

        public AiModelsService(IJsonSerializer jsonSerializer) { _json = jsonSerializer; }

        public async Task<object> Post(AiModelsRequest request)
        {
            var c = Plugin.Instance!.Configuration;
            string Pick(string? typed, string? saved) => string.IsNullOrWhiteSpace(typed) ? (saved ?? "").Trim() : typed!.Trim();
            var provider = (request.Provider ?? "").Trim();
            try
            {
                HttpRequestMessage req;
                if (provider.Equals("Gemini", StringComparison.OrdinalIgnoreCase))
                {
                    var key = Pick(request.ApiKey, c.GeminiApiKey);
                    if (key.Length == 0) return Fail("No API key");
                    // The key in a header, not in the URL (a proxy may log URLs).
                    req = new HttpRequestMessage(HttpMethod.Get, "https://generativelanguage.googleapis.com/v1beta/models?pageSize=1000");
                    req.Headers.Add("x-goog-api-key", key);
                }
                else if (provider.Equals("Claude", StringComparison.OrdinalIgnoreCase))
                {
                    var key = Pick(request.ApiKey, c.ClaudeApiKey);
                    if (key.Length == 0) return Fail("No API key");
                    req = new HttpRequestMessage(HttpMethod.Get, "https://api.anthropic.com/v1/models?limit=1000");
                    req.Headers.Add("x-api-key", key);
                    req.Headers.Add("anthropic-version", "2023-06-01");
                }
                else if (provider.Equals("Ollama", StringComparison.OrdinalIgnoreCase))
                {
                    var url = Pick(request.BaseUrl, c.OllamaBaseUrl).TrimEnd('/');
                    if (url.Length == 0) return Fail("No URL");
                    req = new HttpRequestMessage(HttpMethod.Get, url + "/api/tags");
                }
                else if (provider.Equals("Compat", StringComparison.OrdinalIgnoreCase))
                {
                    var saved = (c.AiConnections ?? new List<AiConnection>())
                        .FirstOrDefault(x => x != null && string.Equals(x.Id, request.ConnectionId, StringComparison.OrdinalIgnoreCase));
                    var url = Pick(request.BaseUrl, saved?.BaseUrl).TrimEnd('/');
                    var key = Pick(request.ApiKey, saved?.ApiKey);
                    if (url.Length == 0) return Fail("No base URL");
                    req = new HttpRequestMessage(HttpMethod.Get, url + "/models");
                    if (key.Length > 0) req.Headers.Add("Authorization", "Bearer " + key);
                }
                else
                {
                    var key = Pick(request.ApiKey, c.OpenAiApiKey);
                    if (key.Length == 0) return Fail("No API key");
                    req = new HttpRequestMessage(HttpMethod.Get, "https://api.openai.com/v1/models");
                    req.Headers.Add("Authorization", "Bearer " + key);
                }

                string body;
                using (var res = await Http.SendAsync(req, CancellationToken.None).ConfigureAwait(false))
                {
                    body = await res.Content.ReadAsStringAsync().ConfigureAwait(false);
                    if (!res.IsSuccessStatusCode)
                        return Fail($"HTTP {(int)res.StatusCode} {res.ReasonPhrase}".Trim());
                }

                var list = _json.DeserializeFromString<AiModelList>(body) ?? new AiModelList();
                IEnumerable<string> ids;
                if (provider.Equals("Gemini", StringComparison.OrdinalIgnoreCase))
                    ids = (list.models ?? new List<AiModelEntry>())
                        .Where(m => m != null && (m.supportedGenerationMethods ?? new List<string>()).Contains("generateContent"))
                        .Select(m => m.name ?? "").Select(n => n.StartsWith("models/") ? n.Substring(7) : n);
                else if (provider.Equals("Ollama", StringComparison.OrdinalIgnoreCase))
                    ids = (list.models ?? new List<AiModelEntry>()).Where(m => m != null).Select(m => m.name ?? "");
                else
                    ids = (list.data ?? new List<AiModelEntry>()).Where(m => m != null).Select(m => m.id ?? "");

                var models = ids.Select(s => s.Trim()).Where(s => s.Length > 0)
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .OrderBy(s => s, StringComparer.OrdinalIgnoreCase).ToList();
                return new AiModelsResponse { Success = true, Models = models, Message = models.Count == 0 ? "No models" : "" };
            }
            catch (TaskCanceledException) { return Fail("Timed out"); }
            catch (HttpRequestException) { return Fail("Unreachable"); }
            catch (Exception ex) { return Fail((ex.GetBaseException().Message ?? ex.Message).Trim()); }
        }

        private static AiModelsResponse Fail(string message) => new AiModelsResponse { Success = false, Message = message };
    }
}
