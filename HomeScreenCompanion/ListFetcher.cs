using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MediaBrowser.Common.Net;
using MediaBrowser.Model.Serialization;

namespace HomeScreenCompanion
{
    public class ListFetcher
    {
        private readonly IHttpClient _httpClient;
        private readonly IJsonSerializer _jsonSerializer;

        private static readonly System.Net.Http.HttpClient _netHttpClient = new System.Net.Http.HttpClient();


        private const string AiSystemPrompt =
            "You are a movie and TV show recommendation assistant. " +
            "Respond ONLY with a valid JSON array. No explanation, no markdown, no code fences. " +
            "Each item must have these fields: " +
            "\"title\" (string, required), " +
            "\"year\" (integer or null), " +
            "\"imdb_id\" (string starting with \"tt\" if known, otherwise null), " +
            "\"type\" (\"movie\" or \"show\"). " +
            "Return exactly the items requested. Do not add any commentary. " +
            "Example: [{\"title\":\"Inception\",\"year\":2010,\"imdb_id\":\"tt1375666\",\"type\":\"movie\"}]";

        public ListFetcher(IHttpClient httpClient, IJsonSerializer jsonSerializer)
        {
            _httpClient = httpClient;
            _jsonSerializer = jsonSerializer;
        }

        public async Task<List<ExternalItemDto>> FetchItems(string url, int limit, string traktClientId, string mdbApiKey, CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(url)) return new List<ExternalItemDto>();

            if (url.Contains("mdblist.com"))
            {
                return await FetchMdblist(url, mdbApiKey, cancellationToken);
            }
            else
            {
                return await FetchTrakt(url, traktClientId, limit, cancellationToken);
            }
        }

        private async Task<List<ExternalItemDto>> FetchMdblist(string listUrl, string apiKey, CancellationToken cancellationToken)
        {
            var cleanUrl = listUrl.Trim().TrimEnd('/');
            if (!cleanUrl.EndsWith("/json")) cleanUrl += "/json";
            var apiUrl = $"{cleanUrl}?apikey={apiKey}&_={DateTimeOffset.UtcNow.ToUnixTimeSeconds()}";
            try
            {
                using (var stream = await _httpClient.Get(new HttpRequestOptions { Url = apiUrl, CancellationToken = cancellationToken }))
                {
                    var result = _jsonSerializer.DeserializeFromStream<List<MdbListItem>>(stream);
                    if (result != null)
                    {
                        return result
                            .Where(x => !string.IsNullOrEmpty(x.imdb_id))
                            .Select(x => new ExternalItemDto
                            {
                                Name = x.title,
                                Imdb = x.imdb_id,
                                Tmdb = null
                            })
                            .ToList();
                    }
                }
            }
            catch { }
            return new List<ExternalItemDto>();
        }

        private async Task<List<ExternalItemDto>> FetchTrakt(string rawUrl, string clientId, int limit, CancellationToken cancellationToken)
        {
            string path = rawUrl.Trim();

            if (Uri.TryCreate(path, UriKind.Absolute, out var uri))
            {
                path = uri.AbsolutePath;
            }
            else
            {
                path = path.Replace("https://trakt.tv", "")
                           .Replace("https://api.trakt.tv", "")
                           .Replace("https://app.trakt.tv", "")
                           .Trim();
            }

            if (path.Contains("?")) path = path.Split('?')[0];
            path = path.Trim();

            if (path.Contains("/users/") && path.Contains("/lists/") && !path.EndsWith("/items"))
            {
                path = path.TrimEnd('/') + "/items";
            }

            if (!path.StartsWith("/")) path = "/" + path;

            var timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            var options = new HttpRequestOptions { Url = $"https://api.trakt.tv{path}?limit={limit}&_={timestamp}", CancellationToken = cancellationToken };

            options.RequestHeaders.Add("trakt-api-version", "2");
            options.RequestHeaders.Add("trakt-api-key", clientId);
            options.UserAgent = "HomeScreenCompanionPlugin/1.0";
            options.RequestHeaders.Add("Accept", "application/json");

            return await FetchTraktRobust(options);
        }

        private async Task<List<ExternalItemDto>> FetchTraktRobust(HttpRequestOptions options)
        {
            try
            {
                using (var stream = await _httpClient.Get(options))
                using (var reader = new StreamReader(stream))
                {
                    string json = await reader.ReadToEndAsync();
                    var list = new List<ExternalItemDto>();

                    try
                    {
                        var wrappedList = _jsonSerializer.DeserializeFromString<List<TraktBaseObject>>(json);
                        if (wrappedList != null && wrappedList.Any(x => x.movie != null || x.show != null))
                        {
                            foreach (var item in wrappedList)
                            {
                                string? title = item.movie?.title ?? item.show?.title;
                                string? imdb = item.movie?.ids?.imdb ?? item.show?.ids?.imdb;

                                if (!string.IsNullOrEmpty(title) && !string.IsNullOrEmpty(imdb))
                                    list.Add(new ExternalItemDto { Name = title, Imdb = imdb, Tmdb = null });
                            }
                            if (list.Count > 0) return list;
                        }
                    }
                    catch { }

                    try
                    {
                        var flatList = _jsonSerializer.DeserializeFromString<List<TraktMovie>>(json);
                        if (flatList != null)
                        {
                            foreach (var item in flatList)
                            {
                                if (!string.IsNullOrEmpty(item.title) && !string.IsNullOrEmpty(item.ids?.imdb))
                                    list.Add(new ExternalItemDto { Name = item.title, Imdb = item.ids?.imdb, Tmdb = null });
                            }
                            if (list.Count > 0) return list;
                        }
                    }
                    catch { }

                    return list;
                }
            }
            catch { return new List<ExternalItemDto>(); }
        }

        public async Task<List<AiListItem>> FetchAiList(
            string provider,
            string prompt,
            string openAiApiKey,
            string geminiApiKey,
            string recentlyWatchedContext,
            int limit,
            CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(prompt))
                return new List<AiListItem>();

            var userMessage = string.IsNullOrWhiteSpace(recentlyWatchedContext)
                ? $"{prompt}\n\nReturn up to {limit} items."
                : $"{recentlyWatchedContext}\n\n{prompt}\n\nReturn up to {limit} items.";

            try
            {
                string rawJson;
                if (string.Equals(provider, "Gemini", StringComparison.OrdinalIgnoreCase))
                {
                    if (string.IsNullOrWhiteSpace(geminiApiKey))
                        throw new InvalidOperationException("Gemini API key is not configured.");
                    rawJson = await CallGemini(geminiApiKey, userMessage, cancellationToken);
                }
                else
                {
                    if (string.IsNullOrWhiteSpace(openAiApiKey))
                        throw new InvalidOperationException("OpenAI API key is not configured.");
                    rawJson = await CallOpenAi(openAiApiKey, userMessage, cancellationToken);
                }

                var cleaned = CleanAiJsonOutput(rawJson);
                var items = _jsonSerializer.DeserializeFromString<List<AiListItem>>(cleaned);
                return items ?? new List<AiListItem>();
            }
            catch
            {
                return new List<AiListItem>();
            }
        }

        private async Task<string> CallOpenAi(string apiKey, string userMessage, CancellationToken cancellationToken)
        {
            var requestBody = $"{{" +
                $"\"model\":\"gpt-4o-mini\"," +
                $"\"messages\":[" +
                $"{{\"role\":\"system\",\"content\":{EscapeJsonString(AiSystemPrompt)}}}," +
                $"{{\"role\":\"user\",\"content\":{EscapeJsonString(userMessage)}}}" +
                $"]}}";

            var request = new System.Net.Http.HttpRequestMessage(System.Net.Http.HttpMethod.Post, "https://api.openai.com/v1/chat/completions");
            request.Headers.Add("Authorization", $"Bearer {apiKey}");
            request.Content = new System.Net.Http.StringContent(requestBody, Encoding.UTF8, "application/json");

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(TimeSpan.FromSeconds(30));

            var response = await _netHttpClient.SendAsync(request, cts.Token);
            var responseBody = await response.Content.ReadAsStringAsync();

            if (!response.IsSuccessStatusCode)
                throw new InvalidOperationException($"OpenAI API error {(int)response.StatusCode}: {responseBody}");

            var parsed = _jsonSerializer.DeserializeFromString<OpenAiResponse>(responseBody);
            return parsed?.choices?.FirstOrDefault()?.message?.content ?? "";
        }

        private async Task<string> CallGemini(string apiKey, string userMessage, CancellationToken cancellationToken)
        {
            var url = $"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={apiKey}";
            var requestBody = $"{{" +
                $"\"systemInstruction\":{{\"parts\":[{{\"text\":{EscapeJsonString(AiSystemPrompt)}}}]}}," +
                $"\"contents\":[{{\"role\":\"user\",\"parts\":[{{\"text\":{EscapeJsonString(userMessage)}}}]}}]" +
                $"}}";

            var request = new System.Net.Http.HttpRequestMessage(System.Net.Http.HttpMethod.Post, url);
            request.Content = new System.Net.Http.StringContent(requestBody, Encoding.UTF8, "application/json");

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(TimeSpan.FromSeconds(30));

            var response = await _netHttpClient.SendAsync(request, cts.Token);
            var responseBody = await response.Content.ReadAsStringAsync();

            if (!response.IsSuccessStatusCode)
                throw new InvalidOperationException($"Gemini API error {(int)response.StatusCode}: {responseBody}");

            var parsed = _jsonSerializer.DeserializeFromString<GeminiResponse>(responseBody);
            return parsed?.candidates?.FirstOrDefault()?.content?.parts?.FirstOrDefault()?.text ?? "";
        }

        private static string CleanAiJsonOutput(string raw)
        {
            var trimmed = raw.Trim();
            if (trimmed.StartsWith("```"))
            {
                var firstNewline = trimmed.IndexOf('\n');
                var lastFence = trimmed.LastIndexOf("```");
                if (firstNewline > 0 && lastFence > firstNewline)
                    trimmed = trimmed.Substring(firstNewline + 1, lastFence - firstNewline - 1).Trim();
            }
            return trimmed;
        }

        private static string EscapeJsonString(string value)
        {
            var sb = new StringBuilder("\"");
            foreach (var c in value)
            {
                switch (c)
                {
                    case '"': sb.Append("\\\""); break;
                    case '\\': sb.Append("\\\\"); break;
                    case '\n': sb.Append("\\n"); break;
                    case '\r': sb.Append("\\r"); break;
                    case '\t': sb.Append("\\t"); break;
                    default: sb.Append(c); break;
                }
            }
            sb.Append('"');
            return sb.ToString();
        }
    }
}