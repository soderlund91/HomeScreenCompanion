using System.Collections.Generic;
using MediaBrowser.Model.Entities;

namespace HomeScreenCompanion
{
    public class ExternalItemDto
    {
        public string Name { get; set; }
        public string Imdb { get; set; }
        public string Tmdb { get; set; }
    }

    public class MdbListItem
    {
        public string title { get; set; }
        public string imdb_id { get; set; }
        public int? id { get; set; }
        public string mediatype { get; set; }
    }

    public class MdbListResponse
    {
        public List<MdbListItem> movies { get; set; } = new List<MdbListItem>();
        public List<MdbListItem> shows { get; set; } = new List<MdbListItem>();
    }

    public class TraktBaseObject
    {
        public int rank { get; set; }
        public string type { get; set; }
        public TraktMovie movie { get; set; }
        public TraktShow show { get; set; }
    }

    public class TraktMovie
    {
        public string title { get; set; }
        public int year { get; set; }
        public TraktIds ids { get; set; }
    }

    public class TraktShow
    {
        public string title { get; set; }
        public int year { get; set; }
        public TraktIds ids { get; set; }
    }

    public class TraktIds
    {
        public int trakt { get; set; }
        public string slug { get; set; }
        public int? tmdb { get; set; }
        public string imdb { get; set; }
    }

    // TMDB
    public class TmdbListResponse
    {
        public List<TmdbListItem> items { get; set; } = new List<TmdbListItem>();
    }

    public class TmdbListItem
    {
        public int id { get; set; }
        public string media_type { get; set; }
        public string title { get; set; }
        public string name { get; set; }
    }

    public class TmdbExternalIds
    {
        public string imdb_id { get; set; }
    }

    // Used for TMDB built-in pages (now-playing, popular, top-rated, etc.) and search
    public class TmdbPagedResponse
    {
        public List<TmdbPagedItem> results { get; set; } = new List<TmdbPagedItem>();
        public int total_pages { get; set; }
        public int page { get; set; }
    }

    public class TmdbPagedItem
    {
        public int id { get; set; }
        public string media_type { get; set; } // present in trending/all, absent in movie/tv-specific endpoints
        public string title { get; set; }
        public string name { get; set; }
    }

    // Used for TMDB collection pages (/collection/{id})
    public class TmdbCollectionResponse
    {
        public string name { get; set; }
        public List<TmdbPagedItem> parts { get; set; } = new List<TmdbPagedItem>();
    }

    public class HscUserDto
    {
        public string Id { get; set; } = "";
        public string Name { get; set; } = "";
    }

    public class HscUsersResponse
    {
        public List<HscUserDto> Users { get; set; } = new List<HscUserDto>();
    }

    public class HscSyncStatusResponse
    {
        public string LastSyncTime { get; set; } = "";
        public bool IsRunning { get; set; }
        public string LastSyncResult { get; set; } = "";
        public int SectionsCopied { get; set; }
        public List<string> Logs { get; set; } = new List<string>();
    }

    [MediaBrowser.Model.Services.Route("/HomeScreenCompanion/Hsc/UserSections", "GET")]
    public class HscGetUserSectionsRequest : MediaBrowser.Model.Services.IReturn<HscUserSectionsResponse>
    {
        public string UserId { get; set; } = "";
    }

    public class HscUserSectionsResponse
    {
        public ContentSection[] Sections { get; set; } = System.Array.Empty<ContentSection>();
    }

    [MediaBrowser.Model.Services.Route("/HomeScreenCompanion/Hsc/UserSections", "POST")]
    public class HscSaveUserSectionsRequest : MediaBrowser.Model.Services.IReturn<HscSaveUserSectionsResponse>
    {
        public string UserId { get; set; } = "";
        public ContentSection[] Sections { get; set; } = System.Array.Empty<ContentSection>();
    }

    public class HscSaveUserSectionsResponse
    {
        public bool Success { get; set; }
        public string Message { get; set; } = "";
    }

    [MediaBrowser.Model.Services.Route("/HomeScreenCompanion/Hsc/SectionSchema", "GET")]
    public class HscGetSectionSchemaRequest : MediaBrowser.Model.Services.IReturn<HscSectionSchemaResponse> { }

    [MediaBrowser.Model.Services.Route("/HomeScreenCompanion/Hsc/DebugMethods", "GET")]
    public class HscDebugMethodsRequest : MediaBrowser.Model.Services.IReturn<string> { }

    public class HscSectionSchemaResponse
    {
        public List<HscSectionField> Fields { get; set; } = new List<HscSectionField>();
    }

    public class HscSectionField
    {
        public string Name { get; set; } = "";
        public string Type { get; set; } = "";
    }

    // AI source DTOs

    public class OpenAiResponse
    {
        public List<OpenAiChoice> choices { get; set; } = new List<OpenAiChoice>();
    }

    public class OpenAiChoice
    {
        public OpenAiMessage message { get; set; } = new OpenAiMessage();
    }

    public class OpenAiMessage
    {
        public string role { get; set; } = "";
        public string content { get; set; } = "";
    }

    public class GeminiResponse
    {
        public List<GeminiCandidate> candidates { get; set; } = new List<GeminiCandidate>();
    }

    public class GeminiCandidate
    {
        public GeminiContent content { get; set; } = new GeminiContent();
    }

    public class GeminiContent
    {
        public List<GeminiPart> parts { get; set; } = new List<GeminiPart>();
    }

    public class GeminiPart
    {
        public string text { get; set; } = "";
    }

    public class AiListItem
    {
        public string title { get; set; } = "";
        public int? year { get; set; }
        public string imdb_id { get; set; }
        public string type { get; set; } = "";
    }

    [MediaBrowser.Model.Services.Route("/HomeScreenCompanion/Hsc/ApplyTagHomeSections", "POST")]
    public class HscApplyTagHomeSectionsRequest : MediaBrowser.Model.Services.IReturn<HscApplyTagHomeSectionsResponse>
    {
        public string TagName { get; set; } = "";
    }

    public class HscApplyTagHomeSectionsResponse
    {
        public bool Success { get; set; }
        public string Message { get; set; } = "";
        public int UsersUpdated { get; set; }
    }

    [MediaBrowser.Model.Services.Route("/HomeScreenCompanion/TestAiSource", "POST")]
    public class TestAiSourceRequest : MediaBrowser.Model.Services.IReturn<TestAiSourceResponse>
    {
        public string Provider { get; set; } = "OpenAI";
        public string Prompt { get; set; } = "";
        public bool IncludeRecentlyWatched { get; set; } = false;
        public string RecentlyWatchedUserId { get; set; } = "";
        public int RecentlyWatchedCount { get; set; } = 20;
    }

    public class TestAiSourceResponse
    {
        public bool Success { get; set; }
        public string Message { get; set; } = "";
        public int Count { get; set; }
        public List<string> Preview { get; set; } = new List<string>();
    }
}