using MediaBrowser.Model.Plugins;
using System;
using System.Collections.Generic;

namespace HomeScreenCompanion
{
    public class PluginConfiguration : BasePluginConfiguration
    {
        public string TraktClientId { get; set; } = "";
        public string MdblistApiKey { get; set; } = "";
        public string TmdbApiKey { get; set; } = "";
        public string OpenAiApiKey { get; set; } = "";
        public string OpenAiModel { get; set; } = "gpt-4o-mini";
        public string GeminiApiKey { get; set; } = "";
        public string GeminiModel { get; set; } = "gemini-2.5-flash-lite";
        public string ClaudeApiKey { get; set; } = "";
        public string ClaudeModel { get; set; } = "claude-haiku-4-5-20251001";
        public string OllamaBaseUrl { get; set; } = "http://localhost:11434";
        public string OllamaModel { get; set; } = "";
        public string AiSystemPrompt { get; set; } =
            "You are a movie and TV show recommendation assistant. " +
            "Respond ONLY with a valid JSON array. No explanation, no markdown, no code fences. " +
            "Each item must have these fields: " +
            "\"title\" (string, required), " +
            "\"year\" (integer or null), " +
            "\"imdb_id\" (string starting with \"tt\" if known, otherwise null), " +
            "\"type\" (\"movie\" or \"show\"). " +
            "Return exactly the items requested. Do not add any commentary. " +
            "Example: [{\"title\":\"Inception\",\"year\":2010,\"imdb_id\":\"tt1375666\",\"type\":\"movie\"}]";
        public bool ExtendedConsoleOutput { get; set; } = false;
        public bool LogMissingItems { get; set; } = false;
        // Keep movie top-list libraries out of users' "My Media" tiles and "Latest" rows.
        public bool HideTopListLibraries { get; set; } = true;
        public bool DryRunMode { get; set; } = false;
        public bool PreserveTagsOnEmptyResult { get; set; } = true;
        // Last Played / Popular rules and the AI's "recently watched": count only titles that were
        // really played (Emby's activity log), not ones only marked as watched. See PlaybackHistory.
        public bool IgnoreMarkedAsWatched { get; set; } = false;
        // Give top-list copies the same collection memberships as their original movie.
        public bool TopListMirrorCollections { get; set; } = false;
        // Settings > Features: show the Copy / Paste buttons and the Import collection button.
        public bool ShowCopyPasteButtons { get; set; } = false;
        public bool ShowImportCollectionButton { get; set; } = false;
        // Settings > Features (experimental): offers "Keep the list order" on each source's Collection tab.
        public bool RankedCollectionsEnabled { get; set; } = false;
        public List<TagConfig> Tags { get; set; } = new List<TagConfig>();
        public List<TopListHomeSection> TopLists { get; set; } = new List<TopListHomeSection>();
        public List<SavedMediaInfoFilter> SavedFilters { get; set; } = new List<SavedMediaInfoFilter>();

        public bool HomeSyncEnabled { get; set; } = false;
        public string HomeSyncSourceUserId { get; set; } = "";
        public List<string> HomeSyncTargetUserIds { get; set; } = new List<string>();
        public bool HomeSyncLibraryOrder { get; set; } = false;

        // Continue Watching bump: moves a finished series to the front of Next Up when a new episode arrives.
        public bool ContinueWatchingBumpEnabled { get; set; } = false;
        public string ContinueWatchingBumpMode { get; set; } = "AllEpisodes"; // "AllEpisodes" | "NewSeasonsOnly"
        public bool ContinueWatchingBumpAllUsers { get; set; } = false;
        public List<string> ContinueWatchingBumpUserIds { get; set; } = new List<string>();

        // Set once LegacyForkCleanup has run (see there).
        public bool LegacyForkCleanupDone { get; set; } = false;

        // Set once the old Top-Lists tab lists have been moved to their sources (SourceTopLists.Migrate).
        public bool TopListsMovedToSources { get; set; } = false;

        // Legacy, for migration only: settings of the stoep-labs fork (4.2.0.x), read once by
        // LegacyForkCleanup and then cleared.
        public bool? ShowTopListsEnabled { get; set; } = null;
        public bool CollectionSortTitleEnabled { get; set; } = false;
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
        // "Manual" source: Emby item ids (movies and series), in list order.
        public List<string> ManualItemIds { get; set; } = new List<string>();
        public List<string> MediaInfoConditions { get; set; } = new List<string>();
        public List<MediaInfoFilter> MediaInfoFilters { get; set; } = new List<MediaInfoFilter>();

        // AI source settings
        public string AiProvider { get; set; } = "OpenAI";
        public string AiPrompt { get; set; } = "";
        public bool AiIncludeRecentlyWatched { get; set; } = false;
        public string AiRecentlyWatchedUserId { get; set; } = "";
        public int AiRecentlyWatchedCount { get; set; } = 20;
        public int AiRefreshIntervalDays { get; set; } = 0;
        public DateTime AiLastRunDate { get; set; } = DateTime.MinValue;
        
        public List<string> Blacklist { get; set; } = new List<string>();
        public List<DateInterval> ActiveIntervals { get; set; } = new List<DateInterval>();

        public bool OverrideWhenActive { get; set; } = false;
        public bool EnableTag { get; set; } = true;
        // Generated art on the tag's own page (the Emby Tag item), same styles as the collection
        // art. "" = none: Emby's own collage poster and no background (or TagBackgroundPath).
        public string TagPosterStyle { get; set; } = "";
        public string TagBackgroundStyle { get; set; } = "";
        // Text drawn on the tag art; "" = the tag name. {name} and {week} as for CollectionArtTitle.
        public string TagArtTitle { get; set; } = "";
        // Customise options of the generated tag poster / background (ArtOptions JSON; "" = the style's own look).
        public string TagPosterOptions { get; set; } = "";
        public string TagBackgroundOptions { get; set; } = "";
        // Uploaded background (used when TagBackgroundStyle is "", i.e. Custom).
        public string TagBackgroundPath { get; set; } = "";
        public bool EnableCollection { get; set; } = false;
        public string CollectionName { get; set; } = "";
        // Optional sort title for the collection (empty = Emby sorts it by its name).
        public string CollectionSortName { get; set; } = "";
        public string CollectionDescription { get; set; } = "";
        public string CollectionPosterPath { get; set; } = "";
        // Generated collection art ("" = none / the uploaded poster). See CollectionArtRenderer.
        public string CollectionPosterStyle { get; set; } = "";
        public string CollectionBackgroundStyle { get; set; } = "";
        // Text drawn on the generated art; "" = the collection name. {name} and {week} (ISO week
        // number) are filled in, e.g. "{name} {week}" → "Top Movies for the Week 41".
        public string CollectionArtTitle { get; set; } = "";
        // Customise options of the generated poster / background (ArtOptions JSON; "" = the style's own look).
        public string CollectionPosterOptions { get; set; } = "";
        public string CollectionBackgroundOptions { get; set; } = "";
        // Uploaded background (used when CollectionBackgroundStyle is "", i.e. Custom).
        public string CollectionBackgroundPath { get; set; } = "";
        // Thumb (16:9, drawn like the background) and banner of the collection. Thumb: a generated
        // style, "" = Custom (CollectionThumbPath) or "keep". Banner: "" = Custom (CollectionBannerPath)
        // or "keep" — there is no generated banner.
        public string CollectionThumbStyle { get; set; } = "";
        public string CollectionThumbOptions { get; set; } = "";
        public string CollectionThumbPath { get; set; } = "";
        public string CollectionBannerStyle { get; set; } = "";
        public string CollectionBannerPath { get; set; } = "";
        // Experimental (needs RankedCollectionsEnabled): the collection holds ranked .strm copies of
        // its movies so it opens in list order. See RankedCollections.
        public bool CollectionKeepOrder { get; set; } = false;
        // The collection existed before HSC (Import collection) and was taken over. It is never
        // deleted when the source is removed, disabled or out of schedule: HSC just stops managing it.
        public bool CollectionImported { get; set; } = false;
        // The last run's titles against Limit, for the "9 of 10" chip on the group's card:
        // LastRunLimit is the Limit that run had (0 = no run with a limit recorded).
        public int LastRunCount { get; set; } = 0;
        public int LastRunLimit { get; set; } = 0;
        // Imported collection: set by the sync once HSC's own poster / background has replaced the
        // collection's original image; from then on "Keep current image" is no longer offered for it.
        public bool CollectionPosterReplaced { get; set; } = false;
        public bool CollectionBackgroundReplaced { get; set; } = false;
        public bool CollectionThumbReplaced { get; set; } = false;
        public bool CollectionBannerReplaced { get; set; } = false;
        public bool OnlyCollection { get; set; } = false;

        // Legacy, for migration only (stoep-labs fork, see LegacyForkCleanup): "Show at the top of
        // Collections" and its custom sort title.
        public bool CollectionSortToTop { get; set; } = false;
        public string CollectionSortTitle { get; set; } = "";

        // Legacy fields — kept for backwards compat, never written by new code
        public bool MediaInfoSeasonMode { get; set; } = false;
        public string MediaInfoTargetType { get; set; } = "";
        public bool MediaInfoTargetEpisode { get; set; } = false;
        public bool MediaInfoTargetSeason { get; set; } = false;
        public bool MediaInfoTargetSeries { get; set; } = false;

        // Tag output targets (what level to tag when scanning episodes)
        public bool TagTargetEpisode { get; set; } = false;
        public bool TagTargetSeason  { get; set; } = false;
        public bool TagTargetSeries  { get; set; } = false;

        // Collection output targets (what level to add to collection when scanning episodes)
        public bool CollectionTargetEpisode { get; set; } = false;
        public bool CollectionTargetSeason  { get; set; } = false;
        public bool CollectionTargetSeries  { get; set; } = false;

        public bool EnableHomeSection { get; set; } = false;
        public List<string> HomeSectionUserIds { get; set; } = new List<string>();
        public string HomeSectionLibraryId { get; set; } = "auto";
        public string HomeSectionSettings { get; set; } = "{}";
        public List<HomeSectionTracking> HomeSectionTracked { get; set; } = new List<HomeSectionTracking>();

        public bool EnablePlaylist { get; set; } = false;
        public string PlaylistName { get; set; } = "";
        public List<string> PlaylistUserIds { get; set; } = new List<string>();
        public List<PlaylistMapping> PlaylistMappings { get; set; } = new List<PlaylistMapping>();
        // Your Next Watch: true once the one-time move of the Home Screen tab's users to the
        // Playlist tab (HomeScreenCompanionTask.NormalizeNextWatch) has been done, so an emptied
        // Playlist user list stays empty.
        public bool NextWatchUsersMigrated { get; set; } = false;
        // Your Next Watch: the history window in days (titles finished in it are the user's taste),
        // and whether favourites and the user's own ratings count.
        public int NextWatchSeedDays { get; set; } = 90;
        public bool NextWatchUseTaste { get; set; } = true;
        // The look of every playlist this source makes (same for all its users, also Your Next
        // Watch): Overview, and a Primary / Backdrop image, generated in a CollectionArtRenderer
        // style or uploaded (style "" = the uploaded image; none = Emby's own). Title as CollectionArtTitle.
        public string PlaylistDescription { get; set; } = "";
        public string PlaylistPosterPath { get; set; } = "";
        public string PlaylistPosterStyle { get; set; } = "";
        public string PlaylistBackgroundStyle { get; set; } = "";
        public string PlaylistBackgroundPath { get; set; } = "";
        public string PlaylistArtTitle { get; set; } = "";
        // Customise options of the generated playlist poster / background (ArtOptions JSON; "" = the style's own look).
        public string PlaylistPosterOptions { get; set; } = "";
        public string PlaylistBackgroundOptions { get; set; } = "";

        // Create as top-list (Home Screen tab): the source's home row is a ranked top-list (its own hidden library with
        // numbered posters) instead of a plain row. Users, title, position, display mode, image type
        // and card size come from the Home Screen tab; the TopLists entry is derived from this
        // source on every save (see SourceTopLists).
        public bool EnableTopList { get; set; } = false;
        public string TopListContentType { get; set; } = "Movies";
        public int TopListMaxItems { get; set; } = 0;
        public string TopListBadgeStyle { get; set; } = "neutral";
        public string TopListBadgeOptions { get; set; } = "";
        public string TopListRealPoster { get; set; } = "";

        public DateTime LastModified { get; set; } = DateTime.MinValue;
    }

    public class PlaylistMapping
    {
        public string UserId { get; set; } = "";
        public string PlaylistId { get; set; } = ""; // Guid of the playlist BaseItem
        public List<long> LastSyncedItemIds { get; set; } = new List<long>(); // media InternalIds from last sync
    }

    public class TopListHomeSection
    {
        public string TagName { get; set; } = "";
        public int MaxItems { get; set; } = 0;
        public List<string> HomeSectionUserIds { get; set; } = new List<string>();
        public string HomeSectionLibraryId { get; set; } = "auto";
        public string HomeSectionSettings { get; set; } = "{}";
        public List<HomeSectionTracking> HomeSectionTracked { get; set; } = new List<HomeSectionTracking>();
        // "Movies" (default) or "Shows" — see ShowTopList.
        public string ContentType { get; set; } = "Movies";
        // Made by a source's Create as top-list (SourceTopLists): edited there, not in the Top-Lists tab.
        public bool FromSource { get; set; } = false;

        // Legacy, for migration only (stoep-labs fork, see LegacyForkCleanup): the fork's tag-based
        // show lists.
        public List<ShowTopListEntry> ShowEntries { get; set; } = new List<ShowTopListEntry>();
        public string ShowSourceTag { get; set; } = "";
        public List<string> ShowParkedSeriesIds { get; set; } = new List<string>();
    }

    // Legacy, for migration only: one ranked series of a fork show list and the tag it was given.
    public class ShowTopListEntry
    {
        public string SeriesId { get; set; } = "";
        public string TagName { get; set; } = "";
    }

    public class HomeSectionTracking
    {
        public string UserId { get; set; } = "";
        public string SectionId { get; set; } = "";
    }

    // What groups deleted on the settings page left behind, waiting for the next full run to
    // remove it (deleted_groups.xml in the plugin data folder).
    public class DeletedGroupCleanup
    {
        public List<RemovedHomeRow> HomeRows { get; set; } = new List<RemovedHomeRow>();
        public List<RemovedPlaylist> Playlists { get; set; } = new List<RemovedPlaylist>();
    }

    public class RemovedPlaylist
    {
        public string GroupName { get; set; } = "";
        public string UserId { get; set; } = "";
        public string PlaylistId { get; set; } = "";
    }

    public class RemovedHomeRow
    {
        public string GroupName { get; set; } = "";
        public string UserId { get; set; } = "";
        public string SectionId { get; set; } = "";
        public string SectionMarker { get; set; } = "";
        public string HomeSectionSettings { get; set; } = "{}";
        // The title the row got when its Custom Title was empty (the group's name, else its tag).
        public string DefaultTitle { get; set; } = "";
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
        // "TimeOfDay" only: active from FromTime to ToTime ("HH:mm", server time) on the days in
        // DayOfWeek (empty = every day). A window past midnight (22:00 → 02:00) is allowed.
        public string FromTime { get; set; } = "";
        public string ToTime { get; set; } = "";
        // "MovingDate" (Yearly weekday) and "Easter": every year around a date that moves — the
        // Occurrence-th ("1".."4" or "last") DayOfWeek (one weekday name) of Month, or for "Easter"
        // (or a MovingDate with Occurrence "easter") Western Easter Sunday, weekday and month ignored;
        // active from DaysBefore days before to DaysAfter days after. "Advent" (first Sunday of
        // Advent through Christmas Day) and "Friday13" (every Friday the 13th) use the same
        // DaysBefore / DaysAfter.
        public string Occurrence { get; set; } = "1";
        public int Month { get; set; } = 1;
        public int DaysBefore { get; set; } = 0;
        public int DaysAfter { get; set; } = 0;
    }
}