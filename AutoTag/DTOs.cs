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
        public string media_type { get; set; }
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
}