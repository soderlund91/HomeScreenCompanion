using MediaBrowser.Model.Plugins;
using System;
using System.Collections.Generic;

namespace HomeScreenCompanion
{
    public class PluginConfiguration : BasePluginConfiguration
    {
        public string TraktClientId { get; set; } = "";
        public string MdblistApiKey { get; set; } = "";
        public string OpenAiApiKey { get; set; } = "";
        public string GeminiApiKey { get; set; } = "";
        public bool ExtendedConsoleOutput { get; set; } = false;
        public bool DryRunMode { get; set; } = false;
        public List<TagConfig> Tags { get; set; } = new List<TagConfig>();
        public List<SavedMediaInfoFilter> SavedFilters { get; set; } = new List<SavedMediaInfoFilter>();

        public bool HomeSyncEnabled { get; set; } = false;
        public string HomeSyncSourceUserId { get; set; } = "";
        public List<string> HomeSyncTargetUserIds { get; set; } = new List<string>();
        public bool HomeSyncLibraryOrder { get; set; } = false;
    }

    public class TagConfig
    {
        public bool Active { get; set; } = true;
        public string Name { get; set; } = "";
        public string Tag { get; set; } = "";
        public string Url { get; set; } = "";
        public int Limit { get; set; } = 50;

        public string SourceType { get; set; } = "External";
        public string LocalSourceId { get; set; } = "";
        public List<string> LocalSources { get; set; } = new List<string>();
        public List<string> MediaInfoConditions { get; set; } = new List<string>();
        public List<MediaInfoFilter> MediaInfoFilters { get; set; } = new List<MediaInfoFilter>();

        // AI source settings
        public string AiProvider { get; set; } = "OpenAI";
        public string AiPrompt { get; set; } = "";
        public bool AiIncludeRecentlyWatched { get; set; } = false;
        public string AiRecentlyWatchedUserId { get; set; } = "";
        public int AiRecentlyWatchedCount { get; set; } = 20;
        
        public List<string> Blacklist { get; set; } = new List<string>();
        public List<DateInterval> ActiveIntervals { get; set; } = new List<DateInterval>();

        public bool OverrideWhenActive { get; set; } = false;
        public bool EnableTag { get; set; } = true;
        public bool EnableCollection { get; set; } = false;
        public string CollectionName { get; set; } = "";
        public string CollectionDescription { get; set; } = "";
        public string CollectionPosterPath { get; set; } = "";
        public bool OnlyCollection { get; set; } = false;

        public bool EnableHomeSection { get; set; } = false;
        public List<string> HomeSectionUserIds { get; set; } = new List<string>();
        public string HomeSectionLibraryId { get; set; } = "auto";
        public string HomeSectionSettings { get; set; } = "{}";
        public List<HomeSectionTracking> HomeSectionTracked { get; set; } = new List<HomeSectionTracking>();

        public DateTime LastModified { get; set; } = DateTime.MinValue;
    }

    public class HomeSectionTracking
    {
        public string UserId { get; set; } = "";
        public string SectionId { get; set; } = "";
    }

    public class MediaInfoFilter
    {
        public string Operator { get; set; } = "AND";
        public List<string> Criteria { get; set; } = new List<string>();
        public string GroupOperator { get; set; } = "AND";
    }

    public class SavedMediaInfoFilter
    {
        public string Name { get; set; } = "";
        public List<MediaInfoFilter> Filters { get; set; } = new List<MediaInfoFilter>();
    }

    public class DateInterval
    {
        public string Type { get; set; } = "SpecificDate";
        public DateTime? Start { get; set; }
        public DateTime? End { get; set; }
        public string DayOfWeek { get; set; } = "Friday";
    }
}