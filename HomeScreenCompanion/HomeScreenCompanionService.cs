using MediaBrowser.Common.Net;
using HttpRequestOptions = MediaBrowser.Common.Net.HttpRequestOptions;
using SkiaSharp;
using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Library;
using MediaBrowser.Controller.Net;
using MediaBrowser.Controller.Providers;
using MediaBrowser.Model.Entities;
using MediaBrowser.Model.IO;
using MediaBrowser.Model.Logging;
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
    [Authenticated(Roles = "Admin")]
    public class TestUrlRequest : IReturn<TestUrlResponse>
    {
        public string Url { get; set; } = string.Empty;
        public int Limit { get; set; } = 10;
    }

    [Route("/HomeScreenCompanion/Status", "GET")]
    [Authenticated(Roles = "Admin")]
    public class GetStatusRequest : IReturn<StatusResponse> { }

    [Route("/HomeScreenCompanion/Version", "GET")]
    [Authenticated(Roles = "Admin")]
    public class VersionRequest : IReturn<VersionResponse> { }

    public class VersionResponse
    {
        public string Version { get; set; } = "";
        // "beta" for a beta build (ReleaseChannel in the .csproj), "" for a release.
        public string Channel { get; set; } = "";
    }

    // Download logs: the zip (see LogBundle), base64 in JSON like the plugin's other downloads.
    [Route("/HomeScreenCompanion/Logs/Download", "GET")]
    [Authenticated(Roles = "Admin")]
    public class DownloadLogsRequest : IReturn<DownloadLogsResponse>
    {
        public bool Anonymize { get; set; } = true;
    }

    public class DownloadLogsResponse
    {
        public bool Success { get; set; }
        public string Message { get; set; } = "";
        public string FileName { get; set; } = "";
        public string Data { get; set; } = "";
    }

    // Download logs: the number each user gets in an anonymised zip ("User 3" = …). Shown only in
    // the download window, so the one sending the zip can say which user they mean; never in the zip.
    [Route("/HomeScreenCompanion/Logs/UserNumbers", "GET")]
    [Authenticated(Roles = "Admin")]
    public class LogUserNumbersRequest : IReturn<LogUserNumbersResponse> { }

    public class LogUserNumbersResponse
    {
        public List<LogUserNumber> Users { get; set; } = new List<LogUserNumber>();
    }

    public class LogUserNumber
    {
        public int Number { get; set; }
        public string Name { get; set; } = "";
    }

    [Route("/HomeScreenCompanion/UploadCollectionImage", "POST")]
    [Authenticated(Roles = "Admin")]
    public class UploadCollectionImageRequest : IReturn<UploadCollectionImageResponse>
    {
        public string FileName { get; set; } = "";
        public string Base64Data { get; set; } = "";
        public string OldFilePath { get; set; } = "";
    }

    [Route("/HomeScreenCompanion/FetchCollectionImageFromUrl", "POST")]
    [Authenticated(Roles = "Admin")]
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
        public int InLibraryCount { get; set; }
        public List<SourceTestItem> Items { get; set; } = new List<SourceTestItem>();
    }

    public class SourceTestItem
    {
        public string Name { get; set; } = "";
        public int? Year { get; set; }
        public string Imdb { get; set; } = "";
        public bool InLibrary { get; set; }
        public bool MatchedByTitle { get; set; }
        public string LibraryItemId { get; set; } = "";
        public string LibraryName { get; set; } = "";
        public int? LibraryYear { get; set; }
        public string ItemType { get; set; } = "";
    }

    public class StatusResponse
    {
        public string LastRunStatus { get; set; } = string.Empty;
        public List<string> Logs { get; set; } = new List<string>();
        public bool IsRunning { get; set; }
        public string StartedUtc { get; set; } = string.Empty;
        public string Step { get; set; } = string.Empty;   // what a running sync is doing now
        public string Waiting { get; set; } = string.Empty; // runs waiting for this one to finish (see RunGate)
        public List<GroupIssue> GroupIssues { get; set; } = new List<GroupIssue>(); // each group's warnings / error from its last run
    }

    [Route("/HomeScreenCompanion/RunEntry", "POST")]
    [Authenticated(Roles = "Admin")]
    public class RunEntryRequest : IReturn<RunEntryResponse>
    {
        public string EntryName { get; set; } = "";
    }

    public class RunEntryResponse
    {
        public bool Success { get; set; }
        public string Message { get; set; } = "";
        /// <summary>Another run was busy: this one waits for it in the background (see RunGate).</summary>
        public bool Queued { get; set; }
    }

    // The settings page's Run menu: starts one of HSC's tasks, also those hidden from Emby's
    // Scheduled Tasks (IConfigurableScheduledTask.IsHidden).
    [Route("/HomeScreenCompanion/RunTask", "POST")]
    [Authenticated(Roles = "Admin")]
    public class RunTaskRequest : IReturn<RunEntryResponse>
    {
        public string Key { get; set; } = "";
    }

    // Run Group on a top-list card: rebuilds just that list (folder, tiles, home sections, collection mirror).
    [Route("/HomeScreenCompanion/TopList/RunOne", "POST")]
    [Authenticated(Roles = "Admin")]
    public class RunOneTopListRequest : IReturn<RunEntryResponse>
    {
        public string TagName { get; set; } = "";
    }

    // Small examples of each collection art style for the style picker (drawn from stand-in posters).
    [Route("/HomeScreenCompanion/ArtStyleSamples", "GET")]
    [Authenticated(Roles = "Admin")]
    public class ArtStyleSamplesRequest : IReturn<ArtStyleSamplesResponse> { }

    public class ArtStyleSamplesResponse
    {
        public Dictionary<string, string> Poster { get; set; } = new Dictionary<string, string>();
        public Dictionary<string, string> Background { get; set; } = new Dictionary<string, string>();
    }

    // The Customise popup: the fonts (each name drawn in its own font) and, per style and kind
    // ("poster|wall", "background|grid", ...), what every field is when nothing is set.
    [Route("/HomeScreenCompanion/ArtCustomiseInfo", "GET")]
    [Authenticated(Roles = "Admin")]
    public class ArtCustomiseInfoRequest : IReturn<ArtCustomiseInfoResponse> { }

    public class ArtFontInfo
    {
        public string Id { get; set; } = "";
        public string Name { get; set; } = "";
        public string Group { get; set; } = "";
        public string Sample { get; set; } = "";   // PNG data: URL
    }

    public class ArtCustomiseInfoResponse
    {
        public List<ArtFontInfo> Fonts { get; set; } = new List<ArtFontInfo>();
        public Dictionary<string, ArtOptionDefaults> Defaults { get; set; } = new Dictionary<string, ArtOptionDefaults>();
    }

    // The Customise popup's live preview: one style drawn by the renderer with the popup's
    // options from the stand-in posters of the style tiles. Returns a PNG (data: URL).
    [Route("/HomeScreenCompanion/ArtCustomPreview", "POST")]
    [Authenticated(Roles = "Admin")]
    public class ArtCustomPreviewRequest : IReturn<ArtCustomPreviewResponse>
    {
        public string Style { get; set; } = "";
        public bool Background { get; set; }
        public string Options { get; set; } = "";
        public string Title { get; set; } = "";   // the card's "Title on the art" ({name}, {week})
        public string Name { get; set; } = "";    // the collection / tag / playlist name
    }

    public class ArtCustomPreviewResponse
    {
        public bool Success { get; set; }
        public string Message { get; set; } = "";
        public string Image { get; set; } = "";
        public string Note { get; set; } = "";   // e.g. "16 posters fit best on 4 rows." (Rows is at most when Posters is set)
    }

    // Top-list badge Customise popup: the number fonts, and the live preview (one card, rank 7
    // drawn by the real renderer on a stand-in poster). JPEG data: URL.
    [Route("/HomeScreenCompanion/BadgeCustomiseInfo", "GET")]
    [Authenticated(Roles = "Admin")]
    public class BadgeCustomiseInfoRequest : IReturn<BadgeCustomiseInfoResponse> { }

    public class BadgeCustomiseInfoResponse
    {
        public List<ArtFontInfo> Fonts { get; set; } = new List<ArtFontInfo>();
    }

    [Route("/HomeScreenCompanion/BadgeCustomPreview", "POST")]
    [Authenticated(Roles = "Admin")]
    public class BadgeCustomPreviewRequest : IReturn<ArtCustomPreviewResponse>
    {
        public string BadgeStyle { get; set; } = "";
        public string Options { get; set; } = "";
        public string TagName { get; set; } = "";   // unused (kept so older pages still post)
        public string Variant { get; set; } = "";   // "thumb" = the landscape a Thumb row uses; "real" = a real poster's badge; else the Primary poster
        public string RealPoster { get; set; } = "";   // Variant "real": the Real posters value "label|size[|{"pos":..}]"
        public string Kind { get; set; } = "";   // "shows" / "movies": the list's titles (a sample of Emby's count / ✓ chip on top)
    }

    // The badge popup's shape tiles: one small card per look, each drawn exactly like the preview
    // (BadgeCustomPreview) with the popup's settings and only the shape changed. JPEG data: URLs, same order.
    [Route("/HomeScreenCompanion/BadgeShapeTiles", "POST")]
    [Authenticated(Roles = "Admin")]
    public class BadgeShapeTilesRequest : IReturn<BadgeShapeTilesResponse>
    {
        public string BadgeStyle { get; set; } = "";
        public string Variant { get; set; } = "";
        public List<string> Options { get; set; } = new List<string>();   // one Options JSON per tile
        public string Kind { get; set; } = "";   // as BadgeCustomPreview
    }

    public class BadgeShapeTilesResponse
    {
        public bool Success { get; set; }
        public string Message { get; set; } = "";
        public List<string> Images { get; set; } = new List<string>();
    }

    // The collection art a source would get, from its unsaved settings. Changes nothing.
    [Route("/HomeScreenCompanion/PreviewCollectionArt", "POST")]
    [Authenticated(Roles = "Admin")]
    public class PreviewCollectionArtRequest : IReturn<PreviewCollectionArtResponse>
    {
        public TagConfig Source { get; set; } = new TagConfig();
    }

    public class PreviewCollectionArtResponse
    {
        public bool Success { get; set; }
        public string Message { get; set; } = "";
        public string Poster { get; set; } = "";       // data: URL, empty when not generated
        public string Background { get; set; } = "";
        public string Thumb { get; set; } = "";
        public int Titles { get; set; }
        public string Note { get; set; } = "";         // e.g. "this source has 12 titles; posters repeat"
        [System.Runtime.Serialization.IgnoreDataMember] public string Timing { get; set; } = "";   // for the log only
    }

    // What a source (any type) would tag, from its unsaved settings. Changes nothing.
    [Route("/HomeScreenCompanion/PreviewSource", "POST")]
    [Authenticated(Roles = "Admin")]
    public class PreviewSourceRequest : IReturn<PreviewSourceResponse>
    {
        public TagConfig Source { get; set; } = new TagConfig();
        // Every URL / collection / playlist of the card, one TagConfig each (as saved). Optional.
        public List<TagConfig> Sources { get; set; } = new List<TagConfig>();
        // "Your Next Watch": the user whose picks to show (empty = the first selected user).
        public string PreviewUserId { get; set; } = "";
    }

    public class PreviewSourceResponse
    {
        public bool Success { get; set; }
        public string Message { get; set; } = "";
        public int Scanned { get; set; }
        public bool ShowViewers { get; set; }
        public int Total { get; set; }                 // titles that would be tagged; Items holds at most PreviewMaxItems
        public List<PreviewSourceItem> Items { get; set; } = new List<PreviewSourceItem>();
        public string SourceType { get; set; } = "";
        public int MissingTotal { get; set; }          // list titles not in the library; Missing holds at most PreviewMaxItems
        public List<PreviewMissingItem> Missing { get; set; } = new List<PreviewMissingItem>();
        public List<string> Warnings { get; set; } = new List<string>();
        public string Note { get; set; } = "";         // e.g. AI: asked the provider once
    }

    public class PreviewMissingItem
    {
        public string Title { get; set; } = "";
        public int? Year { get; set; }
        public string Imdb { get; set; } = "";
    }


    public class PreviewSourceItem
    {
        public string Id { get; set; } = "";
        public string Name { get; set; } = "";
        public string Type { get; set; } = "";
        public int? Year { get; set; }
        public int? Viewers { get; set; }
        public string ImageTag { get; set; } = "";
        public string Reason { get; set; } = "";       // "Your Next Watch": why it was picked
    }

    [Route("/HomeScreenCompanion/Hsc/Status", "GET")]
    [Authenticated(Roles = "Admin")]
    public class HscGetStatusRequest : IReturn<HscSyncStatusResponse> { }

    [Route("/HomeScreenCompanion/DebugSections", "GET")]
    [Authenticated(Roles = "Admin")]
    public class DebugSectionsRequest : IReturn<string>
    {
        public string UserId { get; set; } = string.Empty;
    }

    [Route("/HomeScreenCompanion/Manage/Tags", "GET")]
    [Authenticated(Roles = "Admin")]
    public class GetManagedTagsRequest : IReturn<GetManagedTagsResponse> { }
    public class ManagedTagInfo { public string Id { get; set; } = ""; public string Name { get; set; } = ""; public int ItemCount { get; set; } public int MovieCount { get; set; } public int SeriesCount { get; set; } public List<string> ItemTypes { get; set; } = new List<string>(); }
    public class GetManagedTagsResponse { public List<ManagedTagInfo> Tags { get; set; } = new List<ManagedTagInfo>(); }

    [Route("/HomeScreenCompanion/Manage/Collections", "GET")]
    [Authenticated(Roles = "Admin")]
    public class GetManagedCollectionsRequest : IReturn<GetManagedCollectionsResponse> { }
    public class ManagedCollectionInfo { public string Id { get; set; } = ""; public string Name { get; set; } = ""; public int ItemCount { get; set; } }
    public class GetManagedCollectionsResponse { public List<ManagedCollectionInfo> Collections { get; set; } = new List<ManagedCollectionInfo>(); }

    // Every playlist on the server (Cleanup tab), with its owner and the users it is shared with.
    [Route("/HomeScreenCompanion/Manage/Playlists", "GET")]
    [Authenticated(Roles = "Admin")]
    public class GetManagedPlaylistsRequest : IReturn<GetManagedPlaylistsResponse> { }
    public class ManagedPlaylistInfo
    {
        public string Id { get; set; } = "";            // Guid (N), as DeletePlaylist takes it
        public string Name { get; set; } = "";
        public int ItemCount { get; set; }
        public string OwnerId { get; set; } = "";       // "" = no user owns it
        public string OwnerName { get; set; } = "";
        public List<string> SharedWith { get; set; } = new List<string>();   // other users' names
        public bool IsPublic { get; set; }               // every user sees it
        public string ManagedBy { get; set; } = "";     // the HSC source that makes it; "" = none
        public bool ManagedActive { get; set; }
        public int ManagedIndex { get; set; } = -1;     // index in Tags of that source
    }
    public class GetManagedPlaylistsResponse { public List<ManagedPlaylistInfo> Playlists { get; set; } = new List<ManagedPlaylistInfo>(); }

    // ── Import existing collections (Sources tab) ──
    // Every Emby collection, with what stops it from being taken over (HSC already makes it, or
    // another collection has the same name: HSC finds its collection by name).
    [Route("/HomeScreenCompanion/Collections/ImportList", "GET")]
    [Authenticated(Roles = "Admin")]
    public class GetImportCollectionsRequest : IReturn<GetImportCollectionsResponse> { }
    public class ImportCollectionInfo
    {
        public string Id { get; set; } = "";          // InternalId, as the web API shows ids
        public string Name { get; set; } = "";
        public int ItemCount { get; set; }
        public string ManagedBy { get; set; } = "";   // the HSC source that makes it; "" = none
        public bool DuplicateName { get; set; }
    }
    public class GetImportCollectionsResponse
    {
        public List<ImportCollectionInfo> Collections { get; set; } = new List<ImportCollectionInfo>();
    }

    // One unsaved Manual List source per collection, taking the collection over: same name (so
    // the sync finds this collection and never makes a new one), its description and current
    // titles, sort title kept, images kept ("Keep current image"). Nothing is saved or changed here.
    [Route("/HomeScreenCompanion/Collections/Import", "POST")]
    [Authenticated(Roles = "Admin")]
    public class ImportCollectionsRequest : IReturn<ImportSourceResponse>
    {
        public List<string> Ids { get; set; } = new List<string>();
    }

    [Route("/HomeScreenCompanion/Manage/DeleteTag", "POST")]
    [Authenticated(Roles = "Admin")]
    public class DeleteManagedTagRequest : IReturn<DeleteManagedTagResponse> { public string TagName { get; set; } = ""; }
    public class DeleteManagedTagResponse { public bool Success { get; set; } public string Message { get; set; } = ""; public int ItemsUpdated { get; set; } }

    [Route("/HomeScreenCompanion/Manage/DeleteTags", "POST")]
    [Authenticated(Roles = "Admin")]
    public class DeleteManagedTagsBatchRequest : IReturn<DeleteManagedTagsResponse> { public List<string> TagNames { get; set; } = new List<string>(); }
    public class DeleteManagedTagsResponse { public bool Success { get; set; } public int ItemsUpdated { get; set; } }

    [Route("/HomeScreenCompanion/Manage/DeleteCollection", "POST")]
    [Authenticated(Roles = "Admin")]
    public class DeleteManagedCollectionRequest : IReturn<DeleteManagedCollectionResponse> { public string CollectionId { get; set; } = ""; }
    public class DeleteManagedCollectionResponse { public bool Success { get; set; } public string Message { get; set; } = ""; }

    [Route("/HomeScreenCompanion/Manage/DeletePlaylist", "POST")]
    [Authenticated(Roles = "Admin")]
    public class DeleteManagedPlaylistRequest : IReturn<DeleteManagedCollectionResponse> { public string PlaylistId { get; set; } = ""; }

    [Route("/HomeScreenCompanion/TopList/Status", "GET")]
    [Authenticated(Roles = "Admin")]
    public class GetTopListStatusRequest : IReturn<TopListStatusResponse> { }
    public class TopListStatusResponse
    {
        public bool IsRunning { get; set; }
        public string LastRunStatus { get; set; } = "";
        public List<string> Logs { get; set; } = new List<string>();
        public string StartedUtc { get; set; } = "";
        public string Step { get; set; } = "";   // what a running sync is doing now
        public string Waiting { get; set; } = ""; // runs waiting for this one to finish (see RunGate)
    }

    [Route("/HomeScreenCompanion/TopList/PrepareFolder", "POST")]
    [Authenticated(Roles = "Admin")]
    public class PrepareTopListFolderRequest : IReturn<PrepareTopListFolderResponse>
    {
        public string TagName { get; set; } = "";
        public int MaxItems { get; set; } = 0;
        public string BadgeStyle { get; set; } = "neutral";
        public string ContentType { get; set; } = "Movies";
        public string BadgeOptions { get; set; } = "";
        // "Badge the real posters too": "" = off, else "label|size" (see RealPosterBadges).
        public string? RealPosterBadge { get; set; }
    }
    public class PrepareTopListFolderResponse
    {
        public bool Success { get; set; }
        public string FolderPath { get; set; } = "";
        public string Message { get; set; } = "";
        public int FilesCreated { get; set; }
    }

    [Route("/HomeScreenCompanion/TopList/List", "GET")]
    [Authenticated(Roles = "Admin")]
    public class GetTopListsRequest : IReturn<GetTopListsResponse> { }
    public class GetTopListsResponse
    {
        public List<string> FolderNames { get; set; } = new List<string>();
        public Dictionary<string, int> MovieCounts { get; set; } = new Dictionary<string, int>();
        // Top-list names that are existing tags (tag-based lists); the rest are manual.
        public List<string> TagBased { get; set; } = new List<string>();
    }

    [Route("/HomeScreenCompanion/TopList/ManualItems", "GET")]
    [Authenticated(Roles = "Admin")]
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
        public string CardSizeOffset { get; set; } = "0";
        public string BadgeStyle { get; set; } = "neutral";
        public string BadgeOptions { get; set; } = "";
        // "Badge the real posters too": "" = off, else "label|size" (see RealPosterBadges).
        public string? RealPosterBadge { get; set; }
        public List<string> UserIds { get; set; } = new List<string>();
        public string ContentType { get; set; } = "Movies";
        public string Message { get; set; } = "";
    }

    [Route("/HomeScreenCompanion/TopList/Delete", "POST")]
    [Authenticated(Roles = "Admin")]
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
    [Authenticated(Roles = "Admin")]
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
    [Authenticated(Roles = "Admin")]
    public class SyncAllTopListSectionsRequest : IReturn<SyncAllTopListSectionsResponse> { }
    public class SyncAllTopListSectionsResponse
    {
        public bool Success { get; set; }
        public int UpdatedSections { get; set; }
        public bool Queued { get; set; }
        public string Message { get; set; } = "";
    }

    [Route("/HomeScreenCompanion/TopList/MergeVersions", "POST")]
    [Authenticated(Roles = "Admin")]
    public class MergeTopListVersionsRequest : IReturn<MergeTopListVersionsResponse>
    {
        public string TagName { get; set; } = "";
    }
    public class MergeTopListVersionsResponse
    {
        public bool Success { get; set; }
        public int Indexed { get; set; }
        public int Merged { get; set; }
        public string Message { get; set; } = "";
    }

    [Route("/HomeScreenCompanion/TopList/IndexStatus", "GET")]
    [Authenticated(Roles = "Admin")]
    public class GetTopListIndexStatusRequest : IReturn<TopListIndexStatusResponse>
    {
        public string FolderPath { get; set; } = "";
    }
    public class TopListIndexStatusResponse
    {
        public bool Success { get; set; }
        public int Indexed { get; set; }
        public int Identified { get; set; }
        public string Message { get; set; } = "";
    }

    [Route("/HomeScreenCompanion/TopList/AllMovies", "GET")]
    [Authenticated(Roles = "Admin")]
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

    // Series for the manual show top-list picker (same shape as AllMovies).
    [Route("/HomeScreenCompanion/TopList/AllSeries", "GET")]
    [Authenticated(Roles = "Admin")]
    public class GetAllSeriesRequest : IReturn<GetAllMoviesResponse> { }

    [Route("/HomeScreenCompanion/TopList/GrantLibraryAccess", "POST")]
    [Authenticated(Roles = "Admin")]
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
    [Authenticated(Roles = "Admin")]
    public class SnapshotPoliciesRequest : IReturn<SnapshotPoliciesResponse> { }
    public class SnapshotPoliciesResponse
    {
        public bool Success { get; set; }
        public string SnapshotId { get; set; } = "";
        public int UserCount { get; set; }
        public string Message { get; set; } = "";
    }

    [Route("/HomeScreenCompanion/TopList/RestoreAndGrantAccess", "POST")]
    [Authenticated(Roles = "Admin")]
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
    [Authenticated(Roles = "Admin")]
    public class PrepareManualTopListFolderRequest : IReturn<PrepareTopListFolderResponse>
    {
        public string ListName { get; set; } = "";
        public List<ManualTopListItem> Items { get; set; } = new List<ManualTopListItem>();
        public string BadgeStyle { get; set; } = "neutral";
        public string ContentType { get; set; } = "Movies";
        public string BadgeOptions { get; set; } = "";
        // "Badge the real posters too": "" = off, else "label|size" (see RealPosterBadges).
        public string? RealPosterBadge { get; set; }
    }
    public class ManualTopListItem
    {
        public string ImdbId { get; set; } = "";
        public string ItemId { get; set; } = "";
    }

    [Route("/HomeScreenCompanion/Backup/Export", "POST")]
    [Authenticated(Roles = "Admin")]
    public class ExportBackupRequest : IReturn<BackupFile>
    {
        public bool Settings { get; set; } = true;
        public bool ApiKeys { get; set; } = true;
        public bool Tags { get; set; } = true;
        public bool SavedFilters { get; set; } = true;
        public bool TopLists { get; set; } = true;
        public bool HomeSync { get; set; } = true;
    }

    public class BackupFile
    {
        public int BackupVersion { get; set; }
        public string PluginVersion { get; set; } = "";
        public string CreatedUtc { get; set; } = "";
        public List<string> Sections { get; set; } = new List<string>();
        public BackupSettings? Settings { get; set; }
        public BackupApiKeys? ApiKeys { get; set; }
        public List<TagConfig>? Tags { get; set; }
        public List<SavedMediaInfoFilter>? SavedFilters { get; set; }
        public List<BackupTopList>? TopLists { get; set; }
        public BackupHomeSync? HomeSync { get; set; }
    }

    public class BackupSettings
    {
        public string OpenAiModel { get; set; } = "";
        public string GeminiModel { get; set; } = "";
        public string ClaudeModel { get; set; } = "";
        public string OllamaBaseUrl { get; set; } = "";
        public string OllamaModel { get; set; } = "";
        public string AiSystemPrompt { get; set; } = "";
        public bool ExtendedConsoleOutput { get; set; }
        public bool LogMissingItems { get; set; }
        public bool DryRunMode { get; set; }
        public bool PreserveTagsOnEmptyResult { get; set; } = true;
        public bool IgnoreMarkedAsWatched { get; set; }
        public bool TopListMirrorCollections { get; set; }
        public bool ShowCopyPasteButtons { get; set; }
        public bool ShowImportCollectionButton { get; set; }
        public bool RankedCollectionsEnabled { get; set; }
    }

    public class BackupApiKeys
    {
        public string TraktClientId { get; set; } = "";
        public string MdblistApiKey { get; set; } = "";
        public string TmdbApiKey { get; set; } = "";
        public string OpenAiApiKey { get; set; } = "";
        public string GeminiApiKey { get; set; } = "";
        public string ClaudeApiKey { get; set; } = "";
    }

    public class BackupHomeSync
    {
        public bool HomeSyncEnabled { get; set; }
        public string HomeSyncSourceUserId { get; set; } = "";
        public List<string> HomeSyncTargetUserIds { get; set; } = new List<string>();
        public bool HomeSyncLibraryOrder { get; set; }
        // Nullable so backups made before this feature leave the current settings alone.
        public bool? ContinueWatchingBumpEnabled { get; set; }
        public string? ContinueWatchingBumpMode { get; set; }
        public bool? ContinueWatchingBumpAllUsers { get; set; }
        public List<string>? ContinueWatchingBumpUserIds { get; set; }
    }

    public class BackupTopList
    {
        public TopListHomeSection Config { get; set; } = new TopListHomeSection();
        public bool IsManual { get; set; }
        // Manual lists only: movies (or shows) in rank order. Tag-based lists are rebuilt from the tag on the next sync.
        public List<BackupTopListItem> Items { get; set; } = new List<BackupTopListItem>();
    }

    public class BackupTopListItem
    {
        public string ImdbId { get; set; } = "";
        public string ItemId { get; set; } = "";
        public string Name { get; set; } = "";
        public int? Year { get; set; }
    }

    [Route("/HomeScreenCompanion/Backup/Import", "POST")]
    [Authenticated(Roles = "Admin")]
    public class ImportBackupRequest : IReturn<ImportBackupResponse>
    {
        public string BackupJson { get; set; } = "";
        public bool Settings { get; set; } = true;
        public bool ApiKeys { get; set; } = true;
        public bool Tags { get; set; } = true;
        public bool SavedFilters { get; set; } = true;
        public bool TopLists { get; set; } = true;
        public bool HomeSync { get; set; } = true;
    }

    public class ImportBackupResponse
    {
        public bool Success { get; set; }
        public string Message { get; set; } = "";
        public List<string> Applied { get; set; } = new List<string>();
        public List<string> Warnings { get; set; } = new List<string>();
        public List<TopListLibraryInfo> TopListsNeedingLibrary { get; set; } = new List<TopListLibraryInfo>();
    }

    // Top-lists whose Emby library could not be found after an import. The UI can run the
    // normal library-creation flow for each of these using the folder already prepared server-side.
    public class TopListLibraryInfo
    {
        public string TagName { get; set; } = "";
        public string CustomName { get; set; } = "";
        public string DisplayMode { get; set; } = "";
        public string ImageType { get; set; } = "";
        public string CardSizeOffset { get; set; } = "0";
        public string BadgeStyle { get; set; } = "neutral";
        public string BadgeOptions { get; set; } = "";
        // "Badge the real posters too": "" = off, else "label|size" (see RealPosterBadges).
        public string? RealPosterBadge { get; set; }
        public int MaxItems { get; set; }
        public List<string> UserIds { get; set; } = new List<string>();
        public string FolderPath { get; set; } = "";
        public string ContentType { get; set; } = "Movies";
    }

    // ── Copy / paste one source (Tag & Collection tab) ──
    // Export takes the source's flat TagConfig entries as the card shows them (saved or not) and
    // returns them with what another server needs to make sense of them: user, library and item
    // names for the ids, and the uploaded images themselves. The page adds the "hsc-source"
    // header and puts the text on the clipboard.
    [Route("/HomeScreenCompanion/Source/Export", "POST")]
    [Authenticated(Roles = "Admin")]
    public class ExportSourceRequest : IReturn<SourceFile>
    {
        public List<TagConfig> Tags { get; set; } = new List<TagConfig>();
    }

    public class SourceFile
    {
        public int SourceVersion { get; set; }
        public string PluginVersion { get; set; } = "";
        public string CreatedUtc { get; set; } = "";
        public List<TagConfig> Tags { get; set; } = new List<TagConfig>();
        // id on the exporting server → name
        public Dictionary<string, string> Users { get; set; } = new Dictionary<string, string>();
        public Dictionary<string, string> Libraries { get; set; } = new Dictionary<string, string>();
        public Dictionary<string, SourceItemRef> Items { get; set; } = new Dictionary<string, SourceItemRef>();
        // stored image path on the exporting server → the file
        public Dictionary<string, SourceImage> Images { get; set; } = new Dictionary<string, SourceImage>();
        // Export only: things the person copying should know (not part of the pasted text).
        public List<string> Notices { get; set; } = new List<string>();
    }

    public class SourceItemRef
    {
        public string Name { get; set; } = "";
        public int? Year { get; set; }
        public string Type { get; set; } = "";
        public string Imdb { get; set; } = "";
        public string Tmdb { get; set; } = "";
        public string Tvdb { get; set; } = "";
    }

    public class SourceImage
    {
        public string FileName { get; set; } = "";
        public string Base64 { get; set; } = "";
    }

    // Import maps everything to this server and returns the source's flat TagConfig entries,
    // ready for a new (unsaved) card. Nothing is saved here – only images are uploaded.
    [Route("/HomeScreenCompanion/Source/Import", "POST")]
    [Authenticated(Roles = "Admin")]
    public class ImportSourceRequest : IReturn<ImportSourceResponse>
    {
        public string SourceJson { get; set; } = "";
    }

    public class ImportSourceResponse
    {
        public bool Success { get; set; }
        public string Message { get; set; } = "";
        public List<TagConfig> Tags { get; set; } = new List<TagConfig>();
        public List<string> Notices { get; set; } = new List<string>();
    }

    // ── Copy / paste one top-list (Top Lists tab) ──
    // Same idea as a source: the saved list with names for the server-specific ids (users, the
    // ranked titles). The page adds the "hsc-toplist" header. Run state (tracked sections,
    // generated files, the list's own library and the per-show tags) is left out: the other
    // server makes those when the list is created there.
    [Route("/HomeScreenCompanion/TopList/Export", "POST")]
    [Authenticated(Roles = "Admin")]
    public class ExportTopListRequest : IReturn<TopListCopyFile>
    {
        public string TagName { get; set; } = "";
    }

    public class TopListCopyFile
    {
        public int TopListVersion { get; set; }
        public string PluginVersion { get; set; } = "";
        public string CreatedUtc { get; set; } = "";
        public string ListName { get; set; } = "";
        public string ContentType { get; set; } = "Movies";
        // Manual = titles picked by hand (Items); otherwise the list is fed by SourceTag.
        public bool IsManual { get; set; }
        public string SourceTag { get; set; } = "";
        public string CustomName { get; set; } = "";
        public string DisplayMode { get; set; } = "";
        public string ImageType { get; set; } = "";
        public string CardSizeOffset { get; set; } = "0";
        public string BadgeStyle { get; set; } = "";
        public string BadgeOptions { get; set; } = "";
        // "Badge the real posters too": "" = off, else "label|size" (see RealPosterBadges).
        public string? RealPosterBadge { get; set; }
        public int MaxItems { get; set; }
        public List<string> UserIds { get; set; } = new List<string>();
        // Manual lists: the ranked titles, in order.
        public List<SourceItemRef> Items { get; set; } = new List<SourceItemRef>();
        // user id on the exporting server → name
        public Dictionary<string, string> Users { get; set; } = new Dictionary<string, string>();
        // Export only (not part of the pasted text).
        public List<string> Notices { get; set; } = new List<string>();
    }

    // Import maps the list to this server and returns what the page's create dialog needs to
    // open pre-filled. Nothing is saved: the user reviews the dialog and clicks Create.
    [Route("/HomeScreenCompanion/TopList/Import", "POST")]
    [Authenticated(Roles = "Admin")]
    public class ImportTopListRequest : IReturn<ImportTopListResponse>
    {
        public string TopListJson { get; set; } = "";
    }

    public class ImportTopListResponse
    {
        public bool Success { get; set; }
        public string Message { get; set; } = "";
        public string ListName { get; set; } = "";
        public string ContentType { get; set; } = "Movies";
        public bool IsManual { get; set; }
        public string SourceTag { get; set; } = "";
        public bool SourceTagExists { get; set; }
        public string CustomName { get; set; } = "";
        public string DisplayMode { get; set; } = "";
        public string ImageType { get; set; } = "";
        public string CardSizeOffset { get; set; } = "0";
        public string BadgeStyle { get; set; } = "";
        public string BadgeOptions { get; set; } = "";
        // "Badge the real posters too": "" = off, else "label|size" (see RealPosterBadges).
        public string? RealPosterBadge { get; set; }
        public int MaxItems { get; set; }
        public List<string> UserIds { get; set; } = new List<string>();
        // Same shape as the create dialog's own picks (movies: Guid ItemId; shows: internal id).
        public List<MovieItem> Items { get; set; } = new List<MovieItem>();
        public List<string> Notices { get; set; } = new List<string>();
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
        private readonly IProviderManager _providerManager;
        private readonly IFileSystem _fileSystem;
        private readonly PluginLogger _logger;

        private readonly MediaBrowser.Controller.IServerApplicationHost _appHost;
        private readonly MediaBrowser.Controller.Persistence.IItemRepository _itemRepository;

        public HomeScreenCompanionService(IHttpClient httpClient, IJsonSerializer jsonSerializer, IUserManager userManager, ILibraryManager libraryManager, IUserDataManager userDataManager, IUserViewManager userViewManager, ITaskManager taskManager, IProviderManager providerManager, IFileSystem fileSystem, ILogManager logManager, MediaBrowser.Controller.IServerApplicationHost appHost, MediaBrowser.Controller.Persistence.IItemRepository itemRepository)
        {
            _appHost = appHost;
            _itemRepository = itemRepository;
            _httpClient = httpClient;
            _jsonSerializer = jsonSerializer;
            _userManager = userManager;
            _libraryManager = libraryManager;
            _userDataManager = userDataManager;
            _userViewManager = userViewManager;
            _taskManager = taskManager;
            _providerManager = providerManager;
            _fileSystem = fileSystem;
            _logger = PluginLog.Wrap(logManager.GetLogger("HomeScreenCompanion_Access"), "Page");
        }

        public object Get(DownloadLogsRequest request)
        {
            try
            {
                var (fileName, zip) = new LogBundle(_libraryManager, _userManager, _taskManager, _jsonSerializer, _appHost).Build(request.Anonymize);
                _logger.Info($"Logs downloaded ({zip.Length / 1024} KB{(request.Anonymize ? ", anonymised" : "")})");
                return new DownloadLogsResponse { Success = true, FileName = fileName, Data = Convert.ToBase64String(zip) };
            }
            catch (Exception ex)
            {
                _logger.ErrorException("Logs could not be put together", ex);
                return new DownloadLogsResponse { Success = false, Message = ex.Message };
            }
        }

        public object Get(LogUserNumbersRequest request)
        {
            var users = LogBundle.UsersInOrder(_userManager);
            return new LogUserNumbersResponse
            {
                Users = users.Select((u, i) => new LogUserNumber { Number = i + 1, Name = u.Name ?? "" }).ToList()
            };
        }

        public object Get(VersionRequest request)
        {
            return new VersionResponse { Version = Plugin.Instance?.Version.ToString() ?? "0.0.0", Channel = ReleaseChannel() };
        }

        // The build's channel: the part after "-" in the informational version ("4.4.0.0-beta+abc123" → "beta").
        private static string ReleaseChannel()
        {
            var info = typeof(HomeScreenCompanionService).Assembly
                .GetCustomAttributes(typeof(System.Reflection.AssemblyInformationalVersionAttribute), false)
                .OfType<System.Reflection.AssemblyInformationalVersionAttribute>()
                .FirstOrDefault()?.InformationalVersion ?? "";
            var plus = info.IndexOf('+');
            if (plus >= 0) info = info.Substring(0, plus);
            var dash = info.IndexOf('-');
            return dash >= 0 ? info.Substring(dash + 1).Trim() : "";
        }

        public object Get(GetStatusRequest request)
        {
            List<string> logs;
            lock (HomeScreenCompanionTask.ExecutionLog) { logs = HomeScreenCompanionTask.ExecutionLog.ToList(); }
            return new StatusResponse
            {
                LastRunStatus = HomeScreenCompanionTask.LastRunStatus,
                Logs = logs,
                IsRunning = HomeScreenCompanionTask.IsRunning,
                StartedUtc = HomeScreenCompanionTask.LastStartedUtc?.ToString("o") ?? "",
                Step = HomeScreenCompanionTask.Progress.Step,
                Waiting = RunGate.WaitingFor(HomeScreenCompanionTask.Progress.Step, HomeScreenCompanionTask.IsRunning),
                GroupIssues = GroupIssues.All()
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
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
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

                var data = Convert.FromBase64String(request.Base64Data ?? "");
                var ext = ImageExtension(data);
                if (ext == null)
                    return new UploadCollectionImageResponse { Success = false, Message = "The file is not an image (JPEG, PNG, WebP or GIF)." };

                DeleteOldImage(request.OldFilePath, imagesDir);

                var fileName = $"{Guid.NewGuid():N}{ext}";
                var filePath = Path.Combine(imagesDir, fileName);

                File.WriteAllBytes(filePath, data);

                return new UploadCollectionImageResponse { Success = true, FilePath = filePath };
            }
            catch (Exception ex)
            {
                return new UploadCollectionImageResponse { Success = false, Message = ex.Message };
            }
        }

        // The file extension for uploaded / downloaded image bytes, or null when they are not an
        // image Emby can show (checked by decoding the header, whatever the name says).
        private static string? ImageExtension(byte[] data)
        {
            if (data == null || data.Length == 0) return null;
            try
            {
                using var codec = SKCodec.Create(new SKMemoryStream(data));
                if (codec == null) return null;
                switch (codec.EncodedFormat)
                {
                    case SKEncodedImageFormat.Jpeg: return ".jpg";
                    case SKEncodedImageFormat.Png: return ".png";
                    case SKEncodedImageFormat.Webp: return ".webp";
                    case SKEncodedImageFormat.Gif: return ".gif";
                    default: return null;
                }
            }
            catch { return null; }
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

                byte[] data;
                using (var stream = await _httpClient.Get(new MediaBrowser.Common.Net.HttpRequestOptions { Url = request.Url, CancellationToken = CancellationToken.None }))
                using (var ms = new MemoryStream())
                {
                    await stream.CopyToAsync(ms);
                    data = ms.ToArray();
                }
                var ext = ImageExtension(data);
                if (ext == null)
                    return new UploadCollectionImageResponse { Success = false, Message = "The URL did not return an image (JPEG, PNG, WebP or GIF)." };

                DeleteOldImage(request.OldFilePath, imagesDir);

                var fileName = $"{Guid.NewGuid():N}{ext}";
                var filePath = Path.Combine(imagesDir, fileName);
                File.WriteAllBytes(filePath, data);

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
                    return new TestUrlResponse { Success = false, Message = fetcher.LastError != null
                        ? $"No items found ({fetcher.LastError}). Check URL and API Keys."
                        : "No items found. Check URL and API Keys." };
                }

                var library = BuildTestLibraryIndex();
                var results = items.Select(i => MatchTestItem(library, i.Name, null, i.Imdb, false)).ToList();
                int inLibrary = results.Count(r => r.InLibrary);

                return new TestUrlResponse
                {
                    Success = true,
                    Count = items.Count,
                    InLibraryCount = inLibrary,
                    Items = results,
                    Message = $"Successfully found {items.Count} items ({inLibrary} in library)."
                };
            }
            catch (Exception ex)
            {
                return new TestUrlResponse { Success = false, Message = $"Error: {ex.Message}" };
            }
        }

        private sealed class TestLibraryIndex
        {
            public Dictionary<string, BaseItem> ByImdb = new Dictionary<string, BaseItem>(StringComparer.OrdinalIgnoreCase);
            public List<BaseItem> Items = new List<BaseItem>();
        }

        // Same matching rules as the sync task: IMDb id first, top-list .strm copies excluded,
        // and title+year as a fallback for AI sources only.
        private TestLibraryIndex BuildTestLibraryIndex()
        {
            var index = new TestLibraryIndex();
            var topListsFolder = Path.Combine(Plugin.Instance.DataFolderPath, "toplists") + Path.DirectorySeparatorChar;
            var allItems = _libraryManager.GetItemList(new InternalItemsQuery
            {
                IncludeItemTypes = new[] { "Movie", "Series" },
                Recursive = true,
                IsVirtualItem = false
            });
            foreach (var item in allItems)
            {
                if (!string.IsNullOrEmpty(item.Path) &&
                    item.Path.StartsWith(topListsFolder, StringComparison.OrdinalIgnoreCase))
                    continue;
                index.Items.Add(item);
                if (item.LocationType != LocationType.FileSystem) continue;
                var imdb = item.GetProviderId("Imdb");
                if (!string.IsNullOrEmpty(imdb) && !index.ByImdb.ContainsKey(imdb))
                    index.ByImdb[imdb] = item;
            }
            return index;
        }

        private static SourceTestItem MatchTestItem(TestLibraryIndex index, string? name, int? year, string? imdb, bool allowTitleMatch)
        {
            imdb = (imdb ?? "").Trim();
            var result = new SourceTestItem { Name = name ?? "", Year = year, Imdb = imdb };

            BaseItem? match = null;
            if (!string.IsNullOrEmpty(imdb)) index.ByImdb.TryGetValue(imdb, out match);
            if (match == null && allowTitleMatch && !string.IsNullOrWhiteSpace(name))
            {
                match = index.Items.FirstOrDefault(i =>
                    string.Equals(i.Name, name, StringComparison.OrdinalIgnoreCase)
                    && (year == null || !i.ProductionYear.HasValue || i.ProductionYear == year));
                result.MatchedByTitle = match != null;
            }

            if (match != null)
            {
                result.InLibrary = true;
                result.LibraryItemId = match.InternalId.ToString();
                result.LibraryName = match.Name ?? "";
                result.LibraryYear = match.ProductionYear;
                result.ItemType = match.GetType().Name.Contains("Series") ? "Series" : "Movie";
                if (string.IsNullOrEmpty(result.Name)) result.Name = match.Name ?? "";
            }
            return result;
        }

        public async Task<object> Post(RunEntryRequest request)
        {
            if (string.IsNullOrWhiteSpace(request.EntryName))
                return new RunEntryResponse { Success = false, Message = "No entry name provided" };
            var task = HomeScreenCompanionTask.Instance;
            if (task == null)
                return new RunEntryResponse { Success = false, Message = "Task not initialized" };
            // Another run busy: it waits in the background, so the request does not hang until then.
            if (RunGate.Busy)
            {
                var holder = RunGate.Holder;
                _ = Task.Run(() => task.RunSingleEntryAsync(request.EntryName, CancellationToken.None));
                return new RunEntryResponse { Success = true, Queued = true, Message = QueuedMessage(holder) };
            }
            var (success, message) = await task.RunSingleEntryAsync(request.EntryName, CancellationToken.None);
            return new RunEntryResponse { Success = success, Message = message };
        }

        public async Task<object> Post(RunOneTopListRequest request)
        {
            if (string.IsNullOrWhiteSpace(request.TagName))
                return new RunEntryResponse { Success = false, Message = "No top-list name provided" };
            var task = HomeScreenCompanionTask.Instance;
            if (task == null)
                return new RunEntryResponse { Success = false, Message = "Task not initialized" };
            if (RunGate.Busy)
            {
                var holder = RunGate.Holder;
                var name = request.TagName.Trim();
                _ = Task.Run(() => task.RunSingleTopListAsync(name, RebuildManualTopList, CancellationToken.None));
                return new RunEntryResponse { Success = true, Queued = true, Message = QueuedMessage(holder) };
            }
            var (success, message) = await task.RunSingleTopListAsync(request.TagName.Trim(), RebuildManualTopList, CancellationToken.None);
            return new RunEntryResponse { Success = success, Message = message };
        }

        // The tasks the Run menu may start.
        private static readonly HashSet<string> RunMenuTasks = new HashSet<string>(StringComparer.Ordinal)
        {
            "HomeScreenCompanionSyncTask", "HomeSectionSyncTask", "TopListSyncTask", "MergeTopListVersionsTask", "ContinueWatchingBumpTask"
        };

        public object Post(RunTaskRequest request)
        {
            if (!RunMenuTasks.Contains(request.Key ?? ""))
                return new RunEntryResponse { Success = false, Message = "Unknown task." };
            var worker = _taskManager.ScheduledTasks.FirstOrDefault(w => w.ScheduledTask.Key == request.Key);
            if (worker == null)
                return new RunEntryResponse { Success = false, Message = "Task not found." };
            if (worker.State == MediaBrowser.Model.Tasks.TaskState.Running)
                return new RunEntryResponse { Success = true, Message = $"{worker.Name} is already running." };
            _taskManager.QueueScheduledTask(worker.ScheduledTask, new MediaBrowser.Model.Tasks.TaskOptions());
            return new RunEntryResponse { Success = true, Message = $"{worker.Name} started." };
        }

        private static string QueuedMessage(string holder) =>
            $"Queued — {(string.IsNullOrEmpty(holder) ? "another sync" : holder)} is running. This Run Group starts as soon as it has finished; follow it under Last run / the log.";

        // A manual movie list is rebuilt from the entries in its folder, in their current order,
        // with its saved badge look — the same code as saving the list.
        private (bool Ok, string Message) RebuildManualTopList(TopListHomeSection tl)
        {
            var settings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            try { settings = _jsonSerializer.DeserializeFromString<Dictionary<string, string>>(tl.HomeSectionSettings ?? "{}") ?? settings; }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            var badgeStyle = settings.TryGetValue("BadgeStyle", out var bs) && !string.IsNullOrEmpty(bs) ? bs : "neutral";
            var badgeOptions = settings.TryGetValue("BadgeOptions", out var bo) ? bo ?? "" : "";
            var folderPath = Path.Combine(Plugin.Instance.DataFolderPath, "toplists", SanitizeFolderName(tl.TagName));
            var items = ReadTopListMovies(folderPath)
                .Select(m => new ManualTopListItem { ImdbId = m.ImdbId ?? "", ItemId = m.ItemId ?? "" })
                .ToList();
            if (items.Count == 0) return (false, "the list has no movies");
            var r = Post(new PrepareManualTopListFolderRequest { ListName = tl.TagName, BadgeStyle = badgeStyle, BadgeOptions = badgeOptions, Items = items, ContentType = tl.ContentType ?? "Movies" }) as PrepareTopListFolderResponse;
            if (r == null || !r.Success) return (false, r?.Message ?? "unknown error");
            return (true, $"{r.FilesCreated} of {items.Count} movie(s) synced to its library folder");
        }

        private const int PreviewMaxItems = 250;

        public object Get(ArtStyleSamplesRequest request)
        {
            var dir = Path.Combine(Plugin.Instance!.DataFolderPath, "collection_art", "_samples");
            var res = new ArtStyleSamplesResponse();
            foreach (var style in CollectionArtRenderer.Styles)
            {
                try { res.Poster[style] = CollectionArtRenderer.Sample(style, false, dir); } catch (Exception ex) { _logger.Warn($"Art sample '{style}' poster failed: {ex.Message}"); }
                try { res.Background[style] = CollectionArtRenderer.Sample(style, true, dir); } catch (Exception ex) { _logger.Warn($"Art sample '{style}' background failed: {ex.Message}"); }
            }
            return res;
        }

        public object Get(ArtCustomiseInfoRequest request)
        {
            var res = new ArtCustomiseInfoResponse();
            foreach (var f in ArtFonts.All)
            {
                string sample = "";
                try { sample = ArtFonts.Sample(f); } catch (Exception ex) { _logger.Warn($"Font sample '{f.Id}' failed: {ex.Message}"); }
                res.Fonts.Add(new ArtFontInfo { Id = f.Id, Name = f.Name, Group = f.Group, Sample = sample });
            }
            foreach (var style in CollectionArtRenderer.Styles)
            {
                res.Defaults["poster|" + style] = CollectionArtRenderer.Defaults(style, false);
                res.Defaults["background|" + style] = CollectionArtRenderer.Defaults(style, true);
            }
            return res;
        }

        public object Post(ArtCustomPreviewRequest request)
        {
            if (!CollectionArtRenderer.IsStyle(request.Style))
                return new ArtCustomPreviewResponse { Message = "Choose a generated style first." };
            try
            {
                var dir = Path.Combine(Plugin.Instance!.DataFolderPath, "collection_art", "_samples");
                var image = CollectionArtRenderer.Preview(request.Style.Trim().ToLowerInvariant(), request.Background,
                    ArtOptions.Parse(request.Options),
                    CollectionArtRenderer.ArtTitle(request.Title, string.IsNullOrWhiteSpace(request.Name) ? "Collection" : request.Name.Trim()), dir, out var note);
                return new ArtCustomPreviewResponse { Success = true, Image = image, Note = note };
            }
            catch (Exception ex)
            {
                _logger.Warn($"Art preview '{request.Style}' failed: {ex.Message}");
                return new ArtCustomPreviewResponse { Message = "Preview failed: " + ex.Message };
            }
        }

        public object Get(BadgeCustomiseInfoRequest request)
        {
            var res = new BadgeCustomiseInfoResponse();
            foreach (var f in BadgeFonts.All)
            {
                string sample = "";
                try { sample = BadgeFonts.Sample(f.Id); } catch (Exception ex) { _logger.Warn($"Badge font sample '{f.Id}' failed: {ex.Message}"); }
                res.Fonts.Add(new ArtFontInfo { Id = f.Id, Name = f.Name, Group = BadgeFonts.Group(f.Id), Sample = sample });
            }
            return res;
        }

        public object Post(BadgeCustomPreviewRequest request)
        {
            try
            {
                // A stand-in poster only (like the art Customise preview): no library posters, so it is quick.
                var dir = Path.Combine(Plugin.Instance!.DataFolderPath, "collection_art", "_samples");
                Directory.CreateDirectory(dir);
                var standIn = Path.Combine(dir, "stand-in-badge.jpg");
                if (!File.Exists(standIn)) CollectionArtRenderer.DrawBadgeStandInPoster(standIn);
                var variant = (request.Variant ?? "").Trim().ToLowerInvariant();
                var image = variant == "real"
                    ? BadgeRenderer.RealPosterPreview(RealPosterBadges.OptionOf(string.IsNullOrWhiteSpace(request.RealPoster) ? "top10|m" : request.RealPoster), standIn, dir, 400, PreviewKind(request.Kind))
                    : BadgeRenderer.Preview(BadgeLook.Combine(request.BadgeStyle, request.Options), standIn, dir, variant, 0, PreviewKind(request.Kind));
                return new ArtCustomPreviewResponse { Success = true, Image = image };
            }
            catch (Exception ex)
            {
                _logger.Warn($"Badge preview failed: {ex.Message}");
                return new ArtCustomPreviewResponse { Message = "Preview failed: " + ex.Message };
            }
        }

        private static string? PreviewKind(string? kind)
        {
            var k = (kind ?? "").Trim().ToLowerInvariant();
            return k == "shows" || k == "movies" ? k : null;
        }

        // Shape tiles, by look (style|variant|options): the popup asks again after every change, mostly for the same looks.
        private static readonly Dictionary<string, string> ShapeTileCache = new Dictionary<string, string>();
        private const int ShapeTileWidth = 180;

        public object Post(BadgeShapeTilesRequest request)
        {
            try
            {
                var sw = System.Diagnostics.Stopwatch.StartNew();
                var dir = Path.Combine(Plugin.Instance!.DataFolderPath, "collection_art", "_samples");
                Directory.CreateDirectory(dir);
                var standIn = Path.Combine(dir, "stand-in-badge.jpg");
                if (!File.Exists(standIn)) CollectionArtRenderer.DrawBadgeStandInPoster(standIn);
                var variant = (request.Variant ?? "").Trim().ToLowerInvariant();
                var looks = (request.Options ?? new List<string>()).Select(o => BadgeLook.Combine(request.BadgeStyle, o)).ToList();
                var kind = PreviewKind(request.Kind);
                var images = new string[looks.Count];
                int drawn = 0;
                // The same drawing as the preview (BadgeRenderer.Preview), only smaller; new looks drawn side by side.
                Parallel.For(0, looks.Count, i =>
                {
                    var key = variant + "|" + kind + "|" + looks[i];
                    string? img;
                    lock (ShapeTileCache) ShapeTileCache.TryGetValue(key, out img);
                    if (img == null)
                    {
                        img = BadgeRenderer.Preview(looks[i], standIn, dir, variant, ShapeTileWidth, kind);
                        Interlocked.Increment(ref drawn);
                        lock (ShapeTileCache) { if (ShapeTileCache.Count > 400) ShapeTileCache.Clear(); ShapeTileCache[key] = img; }
                    }
                    images[i] = img;
                });
                _logger.Info($"Badge shape tiles: {looks.Count} tile(s), {drawn} drawn, {sw.ElapsedMilliseconds} ms");
                return new BadgeShapeTilesResponse { Success = true, Images = images.ToList() };
            }
            catch (Exception ex)
            {
                _logger.Warn($"Badge shape tiles failed: {ex.Message}");
                return new BadgeShapeTilesResponse { Message = "Shape tiles failed: " + ex.Message };
            }
        }

        // Art previews run one at a time, in the order they came in (several Preview art clicks
        // at once queue instead of failing; the last one asked for finishes last).
        private static readonly SemaphoreSlim ArtPreviewGate = new SemaphoreSlim(1, 1);

        public async Task<object> Post(PreviewCollectionArtRequest request)
        {
            PreviewCollectionArtResponse result;
            var sw = System.Diagnostics.Stopwatch.StartNew();
            await ArtPreviewGate.WaitAsync().ConfigureAwait(false);
            long waited = sw.ElapsedMilliseconds;
            try { result = await PreviewCollectionArt(request.Source ?? new TagConfig()); }
            finally { ArtPreviewGate.Release(); }
            var src = request.Source ?? new TagConfig();
            _logger.Info($"Collection art preview: '{src.Name}' [{src.SourceType}] tag '{src.Tag}' poster '{src.CollectionPosterStyle}' background '{src.CollectionBackgroundStyle}' thumb '{src.CollectionThumbStyle}' -> "
                + (result.Success ? $"drawn from {result.Titles} title(s)" : result.Message)
                + $" in {sw.ElapsedMilliseconds} ms ({(waited > 0 ? $"waited {waited} ms; " : "")}{result.Timing})");
            return result;
        }

        // A show's episode or season is drawn as the show; only movies and shows are drawn, once each.
        private static List<BaseItem> ArtTopLevel(IEnumerable<BaseItem> items)
        {
            var result = new List<BaseItem>();
            var seen = new HashSet<long>();
            foreach (var i in items)
            {
                BaseItem? top = i is MediaBrowser.Controller.Entities.TV.Episode ep ? ep.Series
                    : i is MediaBrowser.Controller.Entities.TV.Season se ? se.Series : i;
                if (top == null || !(top is MediaBrowser.Controller.Entities.Movies.Movie || top is MediaBrowser.Controller.Entities.TV.Series)) continue;
                if (seen.Add(top.InternalId)) result.Add(top);
            }
            return result;
        }

        // A Manual List's titles, in list order.
        private List<BaseItem> ArtTitlesFromManualList(TagConfig source)
        {
            if (source.SourceType != "Manual") return new List<BaseItem>();
            var found = new List<BaseItem>();
            foreach (var id in source.ManualItemIds ?? new List<string>())
            {
                BaseItem? item = null;
                if (long.TryParse(id, out var internalId)) item = _libraryManager.GetItemById(internalId);
                else if (Guid.TryParse(id, out var guid)) item = _libraryManager.GetItemById(guid);
                if (item != null) found.Add(item);
            }
            return ArtTopLevel(found);
        }

        // The members of the collection this source makes, found by name like the sync does
        // (Collection Name, else the tag; the display name when both are empty).
        private List<BaseItem> ArtTitlesFromCollection(TagConfig source)
        {
            var cName = !string.IsNullOrWhiteSpace(source.CollectionName) ? source.CollectionName.Trim()
                : !string.IsNullOrWhiteSpace(source.Tag) ? source.Tag.Trim() : (source.Name ?? "").Trim();
            if (cName.Length == 0) return new List<BaseItem>();
            var coll = _libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "BoxSet" }, Name = cName, Recursive = true })
                .FirstOrDefault(c => string.Equals((c.Name ?? "").Trim(), cName, StringComparison.OrdinalIgnoreCase));
            if (coll == null) return new List<BaseItem>();
            var members = _libraryManager.GetItemList(new InternalItemsQuery { CollectionIds = new[] { coll.InternalId }, Recursive = true, IsVirtualItem = false })
                .Where(i => !TopListCollectionMirror.IsTopListItem(i));
            return ArtTopLevel(members);
        }

        private async Task<PreviewCollectionArtResponse> PreviewCollectionArt(TagConfig source)
        {
            bool poster = CollectionArtRenderer.IsStyle(source.CollectionPosterStyle);
            bool background = CollectionArtRenderer.IsStyle(source.CollectionBackgroundStyle);
            bool thumb = CollectionArtRenderer.IsStyle(source.CollectionThumbStyle);
            if (!poster && !background && !thumb)
                return new PreviewCollectionArtResponse { Message = "Choose \"Generate from the titles\" for the poster, the background or the thumb first." };

            var clock = System.Diagnostics.Stopwatch.StartNew();
            var timing = new List<string>();
            // The titles: a Smart Playlist is matched now (like its Preview), Your Next Watch from the
            // first selected user's picks; other sources: see below.
            List<BaseItem> items;
            if (source.SourceType == "MediaInfo" || source.SourceType == "NextWatch")
            {
                var task = HomeScreenCompanionTask.Instance;
                if (task == null) return new PreviewCollectionArtResponse { Message = "Task not initialized" };
                var probe = _jsonSerializer.DeserializeFromString<TagConfig>(_jsonSerializer.SerializeToString(source));
                if (string.IsNullOrWhiteSpace(probe.Tag)) probe.Tag = string.IsNullOrWhiteSpace(probe.Name) ? "preview" : probe.Name;
                if (string.IsNullOrWhiteSpace(probe.Name)) probe.Name = probe.Tag;
                var preview = await task.PreviewEntryAsync(probe, CancellationToken.None);
                if (!preview.Done) return new PreviewCollectionArtResponse { Message = preview.Message };
                items = preview.Items;
                if (source.SourceType == "NextWatch")   // a show's pick is its first episode: draw the show
                    items = items.Select(i => i is MediaBrowser.Controller.Entities.TV.Episode ep && ep.Series != null ? ep.Series : i).ToList();
            }
            else
            {
                // Other sources: the hand-picked titles (Manual List), else the members of the
                // source's collection in Emby, else the titles with the tag. No tag needed.
                items = ArtTitlesFromManualList(source);
                if (items.Count == 0) items = ArtTitlesFromCollection(source);
                var tag = (source.Tag ?? "").Trim();
                if (items.Count == 0 && tag.Length > 0) items = _libraryManager.GetItemList(new InternalItemsQuery
                {
                    Recursive = true, IsVirtualItem = false, Tags = new[] { tag },
                    IncludeItemTypes = new[] { "Movie", "Series" }
                }).ToList();
            }
            if (items.Count == 0)
                return new PreviewCollectionArtResponse { Message = source.SourceType == "MediaInfo" || source.SourceType == "NextWatch"
                    ? "No titles match this source, so there is nothing to draw."
                    : "This source has no titles yet, so there is nothing to draw." };

            var name = !string.IsNullOrWhiteSpace(source.CollectionName) ? source.CollectionName.Trim()
                : !string.IsNullOrWhiteSpace(source.Name) ? source.Name.Trim() : (source.Tag ?? "").Trim();
            var title = CollectionArtRenderer.ArtTitle(source.CollectionArtTitle, name);
            var tempDir = Path.Combine(Path.GetTempPath(), "hsc_art_" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(tempDir);
            try
            {
                var titles = CollectionArtRenderer.DistinctTitles(items);
                int distinct = titles.Count;
                string repeatNote = "", rowsNote = "";
                // The posters both drawings need, read once and side by side.
                int need = Math.Max(Math.Max(
                    poster ? CollectionArtRenderer.PostersFor(source.CollectionPosterStyle, false, ArtOptions.Parse(source.CollectionPosterOptions)) : 0,
                    background ? CollectionArtRenderer.PostersFor(source.CollectionBackgroundStyle, true, ArtOptions.Parse(source.CollectionBackgroundOptions)) : 0),
                    thumb ? CollectionArtRenderer.PostersFor(source.CollectionThumbStyle!, true, ArtOptions.Parse(source.CollectionThumbOptions)) : 0);
                timing.Add($"titles {clock.ElapsedMilliseconds} ms");
                clock.Restart();
                var paths = ArtPosterPaths(titles.Take(need).ToList(), _httpClient, tempDir);
                timing.Add($"{need} poster files {clock.ElapsedMilliseconds} ms");
                // kind: "poster", "background" or "thumb" (drawn like the background).
                string Draw(string style, string kind, string? options)
                {
                    bool bg = kind != "poster";
                    var label = char.ToUpperInvariant(kind[0]) + kind.Substring(1);
                    var opts = ArtOptions.Parse(options);
                    if (opts.Posters != null && style.Trim().ToLowerInvariant() != CollectionArtRenderer.Ranked && opts.Posters.Value > distinct)
                        repeatNote = $"This source has {distinct} title{(distinct == 1 ? "" : "s")}; the posters repeat to make {opts.Posters.Value} (never next to a copy).";
                    var posters = paths.Take(CollectionArtRenderer.PostersFor(style, bg, opts)).Where(p => p != null).Select(p => p!).ToList();
                    if (posters.Count == 0) return "";
                    var output = Path.Combine(tempDir, kind + ".jpg");
                    clock.Restart();
                    var note = CollectionArtRenderer.RowsNote(opts, CollectionArtRenderer.Render(style, posters, title, bg, output, opts));
                    timing.Add($"{kind} {clock.ElapsedMilliseconds} ms");
                    if (note.Length > 0) rowsNote = (rowsNote.Length == 0 ? "" : rowsNote + " ") + label + ": " + note;
                    return "data:image/jpeg;base64," + Convert.ToBase64String(File.ReadAllBytes(output));
                }
                var res = new PreviewCollectionArtResponse
                {
                    Success = true,
                    Titles = items.Count,
                    Poster = poster ? Draw(source.CollectionPosterStyle, "poster", source.CollectionPosterOptions) : "",
                    Background = background ? Draw(source.CollectionBackgroundStyle, "background", source.CollectionBackgroundOptions) : "",
                    Thumb = thumb ? Draw(source.CollectionThumbStyle!, "thumb", source.CollectionThumbOptions) : ""
                };
                if (res.Poster.Length == 0 && res.Background.Length == 0 && res.Thumb.Length == 0)
                    return new PreviewCollectionArtResponse { Message = "None of this source's titles has a poster image on this server, so there is nothing to draw." };
                res.Note = string.Join(" ", new[] { repeatNote, rowsNote }.Where(n => n.Length > 0));
                res.Timing = string.Join(", ", timing);
                return res;
            }
            catch (Exception ex)
            {
                return new PreviewCollectionArtResponse { Message = "Preview failed: " + ex.Message };
            }
            finally
            {
                try { Directory.Delete(tempDir, true); } catch { }
            }
        }

        public async Task<object> Post(PreviewSourceRequest request)
        {
            var task = HomeScreenCompanionTask.Instance;
            if (task == null)
                return new PreviewSourceResponse { Message = "Task not initialized" };
            var source = request.Source ?? new TagConfig();
            if (string.IsNullOrWhiteSpace(source.Tag)) source.Tag = string.IsNullOrWhiteSpace(source.Name) ? "preview" : source.Name;
            if (string.IsNullOrWhiteSpace(source.Name)) source.Name = source.Tag;
            // The card's other URLs / collections / playlists; they share the card's name and tag.
            var group = (request.Sources ?? new List<TagConfig>())
                .Where(s => s != null && string.Equals(s.SourceType ?? "", source.SourceType ?? "", StringComparison.OrdinalIgnoreCase))
                .ToList();
            foreach (var g in group) { g.Name = source.Name; g.Tag = source.Tag; }

            var preview = await task.PreviewEntryAsync(source, CancellationToken.None, group, request.PreviewUserId ?? "");
            if (!preview.Done)
                return new PreviewSourceResponse { Message = preview.Message, SourceType = source.SourceType ?? "" };
            bool showViewers = preview.Viewers.Count > 0;
            return new PreviewSourceResponse
            {
                Success = true,
                SourceType = source.SourceType ?? "",
                Scanned = preview.Scanned,
                ShowViewers = showViewers,
                Total = preview.Items.Count,
                MissingTotal = preview.Missing.Count,
                Missing = preview.Missing.Take(PreviewMaxItems).Select(m => new PreviewMissingItem { Title = m.Title, Year = m.Year, Imdb = m.Imdb }).ToList(),
                Warnings = preview.Warnings,
                Note = preview.Note,
                // The dialog loads a poster per row; thousands of rows swamp Emby's image server.
                Items = preview.Items.Take(PreviewMaxItems).Select(i => new PreviewSourceItem
                {
                    Id = i.Id.ToString("N"),
                    Name = i.Name ?? "",
                    Type = i.GetType().Name,
                    Year = i.ProductionYear,
                    Viewers = showViewers && preview.Viewers.TryGetValue(i.Id, out var v) ? v : (int?)null,
                    Reason = preview.Reasons.TryGetValue(i.Id, out var why) ? why : "",
                    ImageTag = i.HasImage(ImageType.Primary) ? (i.GetImageInfo(ImageType.Primary, 0)?.DateModified.Ticks.ToString() ?? "") : ""
                }).ToList()
            };
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
                catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            }

            var fetcher = new ListFetcher(_httpClient, _jsonSerializer);
            try
            {
                var aiItems = await fetcher.FetchAiList(
                    request.Provider,
                    request.Prompt,
                    config.OpenAiApiKey,
                    config.OpenAiModel,
                    config.GeminiApiKey,
                    config.GeminiModel,
                    config.ClaudeApiKey,
                    config.ClaudeModel,
                    config.OllamaBaseUrl,
                    config.OllamaModel,
                    config.AiSystemPrompt,
                    recentlyWatchedContext,
                    20,
                    CancellationToken.None);

                if (aiItems == null || aiItems.Count == 0)
                    return new TestAiSourceResponse { Success = false, Message = fetcher.LastError != null
                        ? $"No items returned ({fetcher.LastError}). Check your API key and prompt."
                        : "No items returned. Check your API key and prompt." };

                var preview = aiItems.Take(5)
                    .Select(i => string.IsNullOrEmpty(i.imdb_id) ? i.title : $"{i.title} — {i.imdb_id}")
                    .ToList();

                var library = BuildTestLibraryIndex();
                var results = aiItems.Where(i => !string.IsNullOrWhiteSpace(i.title))
                    .Select(i => MatchTestItem(library, i.title, i.year, i.imdb_id, true)).ToList();
                int inLibrary = results.Count(r => r.InLibrary);

                return new TestAiSourceResponse
                {
                    Success = true,
                    Count = aiItems.Count,
                    InLibraryCount = inLibrary,
                    Items = results,
                    Message = $"AI returned {aiItems.Count} items ({inLibrary} in library).",
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
                Logs = logs,
                StartedUtc = HomeSectionSyncTask.LastStartedUtc?.ToString("o") ?? "",
                Step = HomeSectionSyncTask.Progress.Step,
                Waiting = RunGate.WaitingFor(HomeSectionSyncTask.Progress.Step, HomeSectionSyncTask.IsRunning)
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
                Logs = logs,
                StartedUtc = TopListSyncTask.LastStartedUtc?.ToString("o") ?? "",
                Step = TopListSyncTask.Progress.Step,
                Waiting = RunGate.WaitingFor(TopListSyncTask.Progress.Step, TopListSyncTask.IsRunning)
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

                if (!SourceTopLists.PlainRow(tc))
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
                catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }

                if (!settingsDict.ContainsKey("SectionType"))
                    settingsDict["SectionType"] = HomeScreenCompanionTask.DefaultSectionType(tc);
                // An empty title would clear the row's name in Emby, and a tag row outside its
                // schedule keeps its "[INACTIVE]" title.
                HomeScreenCompanionTask.ApplyRowTitle(settingsDict, tc, _jsonSerializer);
                // Playlist rows keep each user's own playlist (ParentId is left as it is).
                HomeScreenCompanionTask.NormalizePlaylistSectionSettings(settingsDict);

                settingsDict.TryGetValue("SectionType", out var sectionType);

                // Resolve library ID
                string? resolvedLibraryId = null;
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
                var topListLibraries = TopListSyncTask.TopListLibraryIds(config, _libraryManager);
                var userStatus = UserStatus.Read(_userManager);
                foreach (var userId in (tc.HomeSectionUserIds ?? new System.Collections.Generic.List<string>()))
                {
                    var tracking = realTracked.FirstOrDefault(t => t.UserId == userId);
                    if (tracking == null) continue; // no real tracked section for this user
                    if (userStatus.IsDisabled(userId)) continue; // disabled in Emby: their row is left as it is

                    try
                    {
                        var userInternalId = _userManager.GetInternalId(userId);
                        var currentSections = _userManager.GetHomeSections(userInternalId, CancellationToken.None);
                        var allSections = currentSections?.Sections ?? Array.Empty<ContentSection>();

                        // By title only a row no other source or top-list owns, and never a row on a
                        // top-list's library (a top-list can have the same title).
                        ContentSection ownedSection = allSections.FirstOrDefault(s => s.Id == tracking.SectionId);
                        if (ownedSection == null && settingsDict.TryGetValue("CustomName", out var _applyFallbackName) && !string.IsNullOrEmpty(_applyFallbackName))
                            ownedSection = allSections.FirstOrDefault(s => HomeScreenCompanionTask.SameTitle(s.CustomName, _applyFallbackName)
                                && !HomeScreenCompanionTask.SectionOwnedElsewhere(config, tc, userId, s.Id)
                                && !TopListSyncTask.IsTopListRow(s, topListLibraries));

                        if (ownedSection == null) continue;

                        var updatedSection = HomeScreenCompanionTask.BuildContentSection(_jsonSerializer, settingsDict, resolvedLibraryId, ownedSection);
                        typeof(ContentSection).GetProperty("Id")?.SetValue(updatedSection, ownedSection.Id);
                        _userManager.UpdateHomeSection(userInternalId, updatedSection, CancellationToken.None);
                        updated++;
                    }
                    catch (Exception swallowed) { /* skip this user on error */ PluginLog.Swallowed(swallowed); }
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
                .Select(p => new HscSectionField { Name = p.Name, Type = GetSimpleTypeName(p.PropertyType)! })
                .Where(f => f.Type != null)
                .ToList();
            return new HscSectionSchemaResponse { Fields = fields };
        }

        private static string? GetSimpleTypeName(Type t)
        {
            if (t == typeof(string)) return "string";
            if (t == typeof(bool) || t == typeof(bool?)) return "bool";
            if (t == typeof(int) || t == typeof(int?)) return "int";
            if (t == typeof(long) || t == typeof(long?)) return "long";
            if (t == typeof(DateTime) || t == typeof(DateTime?)) return "datetime";
            return null;
        }

        private const int PerTagQueryLimit = 100;

        public object Get(GetManagedTagsRequest request)
        {
            // One small query per tag (Emby filters by tag id), instead of loading every item on
            // the server and reading its tags: same result, but seconds faster on big libraries.
            // With many tags (e.g. TMDb keywords imported as tags) the queries add up, so above
            // PerTagQueryLimit every item is loaded once and its tags are read instead.
            var tagItems = _libraryManager.GetItemList(new InternalItemsQuery
            {
                IncludeItemTypes = new[] { "Tag" },
                Recursive = true
            });
            var tagIdMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var t in tagItems)
                if (!string.IsNullOrEmpty(t.Name) && !tagIdMap.ContainsKey(t.Name)) tagIdMap[t.Name] = t.Id.ToString("N");

            // Extras (ExtraType = ThemeSong, BehindTheScenes, etc.) are excluded by default.
            Array? allExtraTypes = null;
            System.Reflection.PropertyInfo? extraTypesProp = null;
            try
            {
                extraTypesProp = typeof(InternalItemsQuery).GetProperty("ExtraTypes");
                var elemType = extraTypesProp?.PropertyType.GetElementType();
                if (elemType != null && elemType.IsEnum)
                {
                    var all = System.Enum.GetValues(elemType);
                    allExtraTypes = System.Array.CreateInstance(elemType, all.Length);
                    all.CopyTo(allExtraTypes, 0);
                }
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }

            var tagCount = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            var tagMovieKeys = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
            var tagSeriesIds = new Dictionary<string, HashSet<long>>(StringComparer.OrdinalIgnoreCase);
            var tagTypes = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);

            // The items carrying a tag (tagId set), or every tagged item (tagId null).
            List<BaseItem> ItemsWithTag(long? tagId)
            {
                var query = new InternalItemsQuery
                {
                    Recursive = true,
                    IsVirtualItem = false,
                    IncludeItemTypes = new[] { "Movie", "Series", "Episode", "Season", "Audio", "MusicVideo", "MusicAlbum", "MusicArtist", "Book", "Game", "Trailer", "Video", "Person", "BoxSet", "Photo", "PhotoAlbum", "Playlist", "Recording", "Studio" }
                };
                if (tagId.HasValue) query.TagIds = new[] { tagId.Value };
                var items = _libraryManager.GetItemList(query).ToList();
                if (allExtraTypes != null && extraTypesProp != null)
                {
                    try
                    {
                        var extraQuery = new InternalItemsQuery { Recursive = true, IsVirtualItem = false };
                        if (tagId.HasValue) extraQuery.TagIds = new[] { tagId.Value };
                        extraTypesProp.SetValue(extraQuery, allExtraTypes);
                        var seenIds = new HashSet<Guid>(items.Select(i => i.Id));
                        foreach (var extra in _libraryManager.GetItemList(extraQuery))
                            if (seenIds.Add(extra.Id)) items.Add(extra);
                    }
                    catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
                }
                return items;
            }

            if (tagItems.Length <= PerTagQueryLimit)
            {
                foreach (var tagItem in tagItems)
                    if (!string.IsNullOrWhiteSpace(tagItem.Name))
                        foreach (var item in ItemsWithTag(tagItem.InternalId)) CountTag(tagItem.Name, item);
            }
            else
            {
                foreach (var item in ItemsWithTag(null))
                    if (item.Tags != null)
                        foreach (var tag in item.Tags.Where(t => !string.IsNullOrWhiteSpace(t)).Distinct(StringComparer.OrdinalIgnoreCase))
                            CountTag(tag, item);
            }

            void CountTag(string tag, BaseItem item)
            {
                // Show top-lists can be built from a tag on series, seasons or episodes.
                long seriesId = ShowTopList.IsCopy(item) ? 0
                    : item is MediaBrowser.Controller.Entities.TV.Series ? item.InternalId
                    : (item as MediaBrowser.Controller.Entities.TV.Season)?.SeriesId
                      ?? (item as MediaBrowser.Controller.Entities.TV.Episode)?.SeriesId ?? 0;
                tagCount.TryGetValue(tag, out var c);
                tagCount[tag] = c + 1;
                if (item is MediaBrowser.Controller.Entities.Movies.Movie)
                {
                    var imdb = item.GetProviderId("Imdb");
                    var movieKey = !string.IsNullOrEmpty(imdb)
                        ? imdb
                        : (item.Name ?? "") + "_" + (item.ProductionYear?.ToString() ?? "");
                    if (!tagMovieKeys.TryGetValue(tag, out var seen))
                        tagMovieKeys[tag] = seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    seen.Add(movieKey);
                }
                if (seriesId > 0)
                {
                    if (!tagSeriesIds.TryGetValue(tag, out var seriesSet))
                        tagSeriesIds[tag] = seriesSet = new HashSet<long>();
                    seriesSet.Add(seriesId);
                }
                if (!tagTypes.TryGetValue(tag, out var typeSet))
                    tagTypes[tag] = typeSet = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                typeSet.Add(GetItemTypeKey(item));
            }

            var tags = tagCount
                .Select(kv => new ManagedTagInfo
                {
                    Name = kv.Key,
                    ItemCount = kv.Value,
                    MovieCount = tagMovieKeys.TryGetValue(kv.Key, out var movieSet) ? movieSet.Count : 0,
                    SeriesCount = tagSeriesIds.TryGetValue(kv.Key, out var seriesIdSet) ? seriesIdSet.Count : 0,
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
                if (TagAnchor.IsAnchor(c, _jsonSerializer)) continue;   // HSC's own tag holder, not a real collection
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

        // Owner = the user whose share is ManageDelete (Emby gives it to the creator only; users it
        // is shared with get Read or Write). When the owner is deleted the others keep their share,
        // so the playlist has no owner but is still shared.
        public object Get(GetManagedPlaylistsRequest request)
        {
            var playlists = _libraryManager.GetItemList(new InternalItemsQuery
            {
                IncludeItemTypes = new[] { "Playlist" },
                Recursive = true
            }).Where(p => p.GetType().Name.Contains("Playlist")).ToArray();

            var usersById = _userManager.GetUserList(new UserQuery()).ToDictionary(u => u.InternalId);
            var sharesByItem = new Dictionary<long, List<MediaBrowser.Controller.Library.UserItemShare>>();
            if (playlists.Length > 0)
            {
                try
                {
                    var shares = _itemRepository.GetUserItemShares(
                        new MediaBrowser.Controller.Library.UserItemShareQuery { ItemIds = playlists.Select(p => p.InternalId).ToArray() },
                        CancellationToken.None) ?? Array.Empty<MediaBrowser.Controller.Library.UserItemShare>();
                    foreach (var s in shares)
                    {
                        if (!sharesByItem.TryGetValue(s.ItemId, out var list)) sharesByItem[s.ItemId] = list = new List<MediaBrowser.Controller.Library.UserItemShare>();
                        list.Add(s);
                    }
                }
                catch (Exception ex) { _logger.Warn($"Cleanup: playlist owners could not be read: {ex.Message}"); }
            }

            // The playlists HSC sources make, by playlist id → (source name, active, index).
            var hsc = new Dictionary<Guid, (string Name, bool Active, int Index)>();
            var tags = Plugin.Instance?.Configuration?.Tags ?? new List<TagConfig>();
            for (int i = 0; i < tags.Count; i++)
            {
                var t = tags[i];
                if (t?.PlaylistMappings == null) continue;
                foreach (var m in t.PlaylistMappings)
                    if (m != null && Guid.TryParse(m.PlaylistId, out var g) && !hsc.ContainsKey(g))
                        hsc[g] = (string.IsNullOrWhiteSpace(t.Name) ? (t.Tag ?? "") : t.Name.Trim(), t.Active, i);
            }

            var result = new List<ManagedPlaylistInfo>();
            foreach (var p in playlists)
            {
                int count = 0;
                try { count = _libraryManager.GetItemList(new InternalItemsQuery { ListIds = new[] { p.InternalId } }).Length; }
                catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
                var info = new ManagedPlaylistInfo { Id = p.Id.ToString("N"), Name = p.Name ?? "", ItemCount = count, IsPublic = p.IsPublic };

                var known = (sharesByItem.TryGetValue(p.InternalId, out var ps) ? ps : new List<MediaBrowser.Controller.Library.UserItemShare>())
                    .Where(s => usersById.ContainsKey(s.UserId)).ToList();
                var ownerShare = known.FirstOrDefault(s => s.ShareLevel == MediaBrowser.Model.Dto.UserItemShareLevel.ManageDelete);
                if (ownerShare != null)
                {
                    var owner = usersById[ownerShare.UserId];
                    info.OwnerId = owner.Id.ToString("N");
                    info.OwnerName = owner.Name ?? "";
                }
                info.SharedWith = known.Where(s => s != ownerShare).Select(s => usersById[s.UserId].Name ?? "")
                    .OrderBy(n => n, StringComparer.OrdinalIgnoreCase).ToList();
                if (hsc.TryGetValue(p.Id, out var src))
                {
                    info.ManagedBy = src.Name;
                    info.ManagedActive = src.Active;
                    info.ManagedIndex = src.Index;
                }
                result.Add(info);
            }
            result.Sort((a, b) => string.Compare(a.Name, b.Name, StringComparison.OrdinalIgnoreCase) is int c && c != 0
                ? c : string.Compare(a.OwnerName, b.OwnerName, StringComparison.OrdinalIgnoreCase));
            return new GetManagedPlaylistsResponse { Playlists = result };
        }

        // Collection names the saved sources make (collection on), name → source name.
        private static Dictionary<string, string> HscCollectionNames()
        {
            var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var t in Plugin.Instance?.Configuration?.Tags ?? new List<TagConfig>())
            {
                if (t == null || !t.EnableCollection) continue;
                var cName = string.IsNullOrWhiteSpace(t.CollectionName) ? (t.Tag ?? "").Trim() : t.CollectionName.Trim();
                if (cName.Length > 0 && !result.ContainsKey(cName))
                    result[cName] = string.IsNullOrWhiteSpace(t.Name) ? (t.Tag ?? "") : t.Name;
            }
            return result;
        }

        public object Get(GetImportCollectionsRequest request)
        {
            var collections = _libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "BoxSet" }, Recursive = true })
                .Where(c => !TagAnchor.IsAnchor(c, _jsonSerializer)).ToArray();   // HSC's own tag holder
            var hsc = HscCollectionNames();
            var nameCounts = collections.GroupBy(c => (c.Name ?? "").Trim(), StringComparer.OrdinalIgnoreCase)
                .ToDictionary(g => g.Key, g => g.Count(), StringComparer.OrdinalIgnoreCase);
            var response = new GetImportCollectionsResponse();
            foreach (var c in collections)
            {
                var name = (c.Name ?? "").Trim();
                response.Collections.Add(new ImportCollectionInfo
                {
                    Id = c.InternalId.ToString(),
                    Name = c.Name ?? "",
                    ItemCount = _libraryManager.GetItemList(new InternalItemsQuery { CollectionIds = new[] { c.InternalId }, IsVirtualItem = false }).Count(),
                    ManagedBy = hsc.TryGetValue(name, out var src) ? src : "",
                    DuplicateName = nameCounts.TryGetValue(name, out var n) && n > 1
                });
            }
            response.Collections.Sort((a, b) => string.Compare(a.Name, b.Name, StringComparison.OrdinalIgnoreCase));
            return response;
        }

        public object Post(ImportCollectionsRequest request)
        {
            var response = new ImportSourceResponse();
            try
            {
                var hsc = HscCollectionNames();
                var all = _libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "BoxSet" }, Recursive = true });
                foreach (var id in (request.Ids ?? new List<string>()).Distinct())
                {
                    if (!long.TryParse(id, out var internalId)) continue;
                    var coll = all.FirstOrDefault(c => c.InternalId == internalId);
                    if (coll == null) { response.Notices.Add($"A collection (id {id}) is no longer in the library and was left out."); continue; }
                    var name = (coll.Name ?? "").Trim();
                    if (name.Length == 0) { response.Notices.Add($"The collection with id {id} has no name and was left out."); continue; }
                    if (hsc.TryGetValue(name, out var src)) { response.Notices.Add($"\"{name}\" was left out: the source \"{src}\" already makes it."); continue; }
                    if (all.Count(c => string.Equals((c.Name ?? "").Trim(), name, StringComparison.OrdinalIgnoreCase)) > 1)
                    { response.Notices.Add($"\"{name}\" was left out: another collection has the same name. Rename one of them in Emby first."); continue; }

                    // The current titles, in the collection's own order (sort title). A Manual List
                    // holds movies and shows (and music); a film's other versions stay out, as on
                    // every HSC collection. Top-list copies are added by the top-list mirror, not here.
                    var members = _libraryManager.GetItemList(new InternalItemsQuery { CollectionIds = new[] { coll.InternalId }, Recursive = true, IsVirtualItem = false })
                        .Where(i => !TopListCollectionMirror.IsTopListItem(i))
                        .OrderBy(i => i.SortName ?? i.Name ?? "", StringComparer.OrdinalIgnoreCase).ThenBy(i => i.InternalId)
                        .ToList();
                    var usable = members.Where(HomeScreenCompanionTask.IsTaggableTopLevelItem).ToList();
                    var extra = HomeScreenCompanionTask.ExtraVersionIds(usable);
                    var kept = usable.Where(i => !extra.Contains(i.InternalId)).ToList();
                    var other = members.Where(i => !HomeScreenCompanionTask.IsTaggableTopLevelItem(i)).ToList();
                    string Names(IEnumerable<BaseItem> items) =>
                        string.Join(", ", items.Take(5).Select(i => i.Name + (i.ProductionYear.HasValue ? $" ({i.ProductionYear})" : ""))) + (items.Count() > 5 ? $" and {items.Count() - 5} more" : "");
                    if (extra.Count > 0)
                        response.Notices.Add($"\"{name}\": {extra.Count} extra version(s) of a film leave the collection on the next sync (HSC keeps one copy per film): {Names(usable.Where(i => extra.Contains(i.InternalId)))}.");
                    if (other.Count > 0)
                        response.Notices.Add($"\"{name}\": {other.Count} item(s) that are not a movie or show (e.g. episodes or seasons) cannot be in a Manual List and leave the collection on the next sync: {Names(other)}.");

                    var sortName = coll.SortName ?? "";
                    response.Tags.Add(new TagConfig
                    {
                        Active = true,
                        Name = name,
                        Tag = name,
                        SourceType = "Manual",
                        ManualItemIds = kept.Select(i => i.InternalId.ToString()).ToList(),
                        Limit = 0,
                        EnableTag = false,
                        EnableCollection = true,
                        CollectionName = name,
                        CollectionDescription = coll.Overview ?? "",
                        CollectionPosterStyle = CollectionArtRenderer.KeepStyle,
                        CollectionBackgroundStyle = CollectionArtRenderer.KeepStyle,
                        CollectionThumbStyle = CollectionArtRenderer.KeepStyle,
                        CollectionBannerStyle = CollectionArtRenderer.KeepStyle,
                        // A sort title set in Emby (locked) is kept as the source's sort title.
                        CollectionSortName = (coll.LockedFields ?? Array.Empty<MetadataFields>()).Contains(MetadataFields.SortName) ? sortName : "",
                        CollectionImported = true,
                        LastModified = DateTime.UtcNow
                    });
                }
                response.Success = true;
                if (response.Tags.Count == 0 && response.Notices.Count == 0) response.Message = "No collection was chosen.";
            }
            catch (Exception ex)
            {
                _logger.ErrorException("Import collections failed", ex);
                response.Success = false;
                response.Message = ex.Message;
            }
            return response;
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
                catch (Exception swallowed) { /* best effort */ PluginLog.Swallowed(swallowed); }
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
                TagCacheManager.Instance.RemoveTagFromAllEntries(tag!);
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
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }

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
                catch (Exception swallowed) { /* best effort */ PluginLog.Swallowed(swallowed); }
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

        // Removed with its folder (userplaylists\<name> [playlist]\): left behind, Emby indexes the
        // .m3u again as a new playlist that no user owns.
        public object Post(DeleteManagedPlaylistRequest request)
        {
            if (!Guid.TryParse(request.PlaylistId ?? "", out var guid))
                return new DeleteManagedCollectionResponse { Success = false, Message = "Invalid PlaylistId." };
            var item = _libraryManager.GetItemById(guid);
            if (item == null || !item.GetType().Name.Contains("Playlist"))
                return new DeleteManagedCollectionResponse { Success = false, Message = "Playlist not found." };
            try
            {
                _libraryManager.DeleteItem(item, new DeleteOptions { DeleteFileLocation = true });
                _logger.Info($"Cleanup: playlist \"{item.Name}\" removed");
                return new DeleteManagedCollectionResponse { Success = true };
            }
            catch (Exception ex)
            {
                return new DeleteManagedCollectionResponse { Success = false, Message = ex.Message };
            }
        }

        public object Post(MergeTopListVersionsRequest request)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(request.TagName))
                    return new MergeTopListVersionsResponse { Success = false, Message = "TagName is required." };

                var dataPath = Plugin.Instance.DataFolderPath;
                var sanitized = SanitizeFolderName(request.TagName);
                var folderPath = Path.Combine(dataPath, "toplists", sanitized);

                var (indexed, merged) = MergeTopListVersionsTask.MergeAndProbeFolder(
                    _libraryManager, _providerManager, _fileSystem, folderPath, CancellationToken.None);

                return new MergeTopListVersionsResponse { Success = true, Indexed = indexed, Merged = merged };
            }
            catch (Exception ex)
            {
                return new MergeTopListVersionsResponse { Success = false, Message = ex.Message };
            }
        }

        // Read-only: how many .strm files in a top-list folder Emby has indexed as movies, and how
        // many of those are identified (IMDb id resolved). The creation flow polls this after
        // triggering the library scan so the home section is only finalised once items exist.
        public object Get(GetTopListIndexStatusRequest request)
        {
            try
            {
                var topListsFolder = Path.Combine(Plugin.Instance.DataFolderPath, "toplists") + Path.DirectorySeparatorChar;
                var folderPrefix = Path.GetFullPath(request.FolderPath ?? "").TrimEnd(Path.DirectorySeparatorChar) + Path.DirectorySeparatorChar;
                if (!folderPrefix.StartsWith(topListsFolder, StringComparison.OrdinalIgnoreCase))
                    return new TopListIndexStatusResponse { Success = false, Message = "FolderPath is not a top-list folder." };

                // Polled every 2 s — let the database narrow to the folder instead of loading every
                // movie. The Where() stays as a safety net and keeps the result exact.
                // Show lists count their series copies; those are identified by the nfo the
                // plugin writes, so any provider id means done.
                var items = _libraryManager.GetItemList(new InternalItemsQuery
                {
                    IncludeItemTypes = new[] { "Movie", "Series" },
                    Recursive = true,
                    IsVirtualItem = false,
                    PathStartsWith = folderPrefix
                }).Where(i => !string.IsNullOrEmpty(i.Path)
                           && i.Path.StartsWith(folderPrefix, StringComparison.OrdinalIgnoreCase))
                  .ToList();

                return new TopListIndexStatusResponse
                {
                    Success = true,
                    Indexed = items.Count,
                    Identified = items.Count(i => i is MediaBrowser.Controller.Entities.TV.Series
                        ? (i.ProviderIds?.Count ?? 0) > 0
                        : !string.IsNullOrEmpty(i.GetProviderId("Imdb")))
                };
            }
            catch (Exception ex)
            {
                return new TopListIndexStatusResponse { Success = false, Message = ex.Message };
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

                if (ShowTopList.IsShows(request.ContentType))
                {
                    var series = ShowTopList.SeriesForTag(_libraryManager, _jsonSerializer, request.TagName);
                    if (request.MaxItems > 0) series = series.Take(request.MaxItems).ToList();
                    var written = WriteShowFolder(request.TagName, folderPath, series, BadgeLook.Combine(request.BadgeStyle, request.BadgeOptions));
                    return new PrepareTopListFolderResponse { Success = true, FolderPath = folderPath, FilesCreated = written };
                }
                ShowTopList.ClearShowFolder(folderPath);

                // Remove stale files from a previous run
                foreach (var f in Directory.GetFiles(folderPath, "*.strm"))
                    File.Delete(f);
                foreach (var f in Directory.GetFiles(folderPath, "*.nfo"))
                    File.Delete(f);
                foreach (var f in Directory.GetFiles(folderPath, "*.jpg"))
                    File.Delete(f);

                // Write one .strm file per movie of the source's list (TopListMembers), or before its
                // first run since 4.5 per movie that carries this tag. Top-list copies are skipped:
                // one carrying the tag would otherwise become a .strm pointing at a .strm.
                var topListsFolder = Path.Combine(dataPath, "toplists") + Path.DirectorySeparatorChar;
                var (items, fromList) = TopListMembers.MoviesFor(request.TagName, _libraryManager, _jsonSerializer, topListsFolder);

                // Without a saved list: sort by saved rank order from the last task run (preserves external list order)
                var rankFile = Path.Combine(Plugin.Instance.DataFolderPath, "tag_ranks", sanitized + ".json");
                if (!fromList && File.Exists(rankFile))
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
                    catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
                }

                // First pass: deduplicate and preserve query order
                var seenKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                var selected = new List<(string BaseName, string FilePath, BaseItem Item)>();
                foreach (var item in items)
                {
                    if (string.IsNullOrEmpty(item.Path)) continue;
                    var baseName = SanitizeFolderName(item.Name);
                    if (item.ProductionYear.HasValue && item.ProductionYear > 0)
                        baseName += $" ({item.ProductionYear})";
                    if (!seenKeys.Add(baseName)) continue;
                    selected.Add((baseName, item.Path, item));
                }

                // Apply max-items limit before writing
                if (request.MaxItems > 0 && selected.Count > request.MaxItems)
                    selected = selected.Take(request.MaxItems).ToList();

                // Second pass: write .strm, .nfo and ranked poster
                int digits = Math.Max(2, selected.Count.ToString().Length);
                int count = 0;
                var look = BadgeLook.Combine(request.BadgeStyle, request.BadgeOptions);
                var rankExtras = TopListHistory.Update(request.TagName, selected.Select(x => x.Item).ToList(), look,
                    _jsonSerializer, _libraryManager, _userManager, _userDataManager, m => _logger.Warn(m));
                var tempDir = Path.Combine(Path.GetTempPath(), "hsc_toplist_" + Guid.NewGuid().ToString("N"));
                Directory.CreateDirectory(tempDir);
                try
                {
                foreach (var entry in selected)
                {
                    count++;
                    var sortPrefix = count.ToString().PadLeft(digits, '0');
                    var fileName = entry.BaseName;
                    File.WriteAllText(Path.Combine(folderPath, fileName + ".nfo"), BuildTopListNfo(entry.Item, sortPrefix));
                    WriteRankedImages(entry.Item, count, Path.Combine(folderPath, fileName), look, tempDir, rankExtras.TryGetValue(entry.Item.Id, out var rx) ? rx : null);
                    // .strm last: the folder is a watched library, and Emby creates the item the
                    // moment it sees the .strm — the nfo and badged images must already be there.
                    File.WriteAllText(Path.Combine(folderPath, fileName + ".strm"), entry.FilePath);
                }
                }
                finally { try { Directory.Delete(tempDir, true); } catch { } }

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

        public object Get(GetAllSeriesRequest request)
        {
            try
            {
                var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                var series = ShowTopList.AllSeries(_libraryManager)
                    .OrderBy(i => i.Name, StringComparer.OrdinalIgnoreCase)
                    .ThenBy(i => i.ProductionYear)
                    .Select(i => new MovieItem
                    {
                        Name   = i.Name ?? "",
                        Year   = i.ProductionYear,
                        ImdbId = i.GetProviderId("Imdb") ?? "",
                        ItemId = i.Id.ToString("N")
                    })
                    // The same show in several libraries is offered once.
                    .Where(m => seen.Add(!string.IsNullOrEmpty(m.ImdbId) ? m.ImdbId : $"{m.Name}|{m.Year}"))
                    .ToList();
                return new GetAllMoviesResponse { Movies = series };
            }
            catch { return new GetAllMoviesResponse { Movies = new List<MovieItem>() }; }
        }

        // Writes a show list's folder and updates the copies Emby has already indexed.
        private int WriteShowFolder(string listName, string folderPath, List<BaseItem> series, string look)
        {
            var written = ShowTopList.WriteFolder(folderPath, series, look,
                _libraryManager, _jsonSerializer, _httpClient, _providerManager, _fileSystem, m => _logger.Info(m),
                ranked => TopListHistory.Update(listName, ranked, look, _jsonSerializer, _libraryManager, _userManager, _userDataManager, m => _logger.Warn(m)));
            ShowTopList.ApplyIndexed(folderPath, _libraryManager, _jsonSerializer);
            return written;
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
                        object? policy = null;
                        if (getPolicyMethod != null)
                        {
                            try
                            {
                                var gpParams = getPolicyMethod.GetParameters();
                                var gpArg0   = BuildUserArg(gpParams[0].ParameterType, user);
                                var gpArgs   = BuildArgList(gpParams, gpArg0, null);
                                policy = getPolicyMethod.Invoke(_userManager, gpArgs);
                            }
                            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
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
                        object? policy = null;
                        if (getPolicyMethod != null)
                        {
                            try
                            {
                                var gp = getPolicyMethod.GetParameters();
                                policy = getPolicyMethod.Invoke(_userManager, BuildArgList(gp, BuildUserArg(gp[0].ParameterType, user), null));
                            }
                            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
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
                    catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
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
                    object? policy = null;
                    if (getPolicyMethod != null)
                    {
                        try
                        {
                            var gp = getPolicyMethod.GetParameters();
                            policy = getPolicyMethod.Invoke(_userManager, BuildArgList(gp, BuildUserArg(gp[0].ParameterType, user), null));
                        }
                        catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
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

        private static object?[] BuildArgList(System.Reflection.ParameterInfo[] parms, object arg0, object? arg1)
        {
            var args = new object?[parms.Length];
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

        private void EnforceTopListLibraryPermissions(string libId, IEnumerable<string>? assignedUserIds)
        {
            var normalizedLibId = (libId ?? "").Trim().Replace("-", "").ToLowerInvariant();
            if (string.IsNullOrEmpty(normalizedLibId)) return;

            var assignedSet = new HashSet<string>(
                (assignedUserIds ?? Enumerable.Empty<string>()).Select(id => id.Replace("-", "").ToLowerInvariant()),
                StringComparer.OrdinalIgnoreCase);

            var bf = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.FlattenHierarchy;
            var mgrType = _userManager.GetType();

            var getPolicyMethod = mgrType.GetMethods(bf)
                .Where(m => m.Name == "GetUserPolicy").OrderBy(m => m.GetParameters().Length).FirstOrDefault()
                ?? typeof(IUserManager).GetMethods().FirstOrDefault(m => m.Name == "GetUserPolicy");

            var updateMethod = mgrType.GetMethods(bf)
                .Where(m => m.Name == "UpdateUserPolicy").OrderBy(m => m.GetParameters().Length).FirstOrDefault()
                ?? typeof(IUserManager).GetMethods().FirstOrDefault(m => m.Name == "UpdateUserPolicy");

            if (updateMethod == null) return;

            foreach (var user in _userManager.GetUserList(new UserQuery { IsDisabled = false }))
            {
                try
                {
                    object? policy = null;
                    if (getPolicyMethod != null)
                    {
                        try
                        {
                            var gp = getPolicyMethod.GetParameters();
                            policy = getPolicyMethod.Invoke(_userManager, BuildArgList(gp, BuildUserArg(gp[0].ParameterType, user), null));
                        }
                        catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
                    }
                    if (policy == null) policy = user.GetType().GetProperty("Policy")?.GetValue(user);
                    if (policy == null) continue;

                    var enableAllProp = policy.GetType().GetProperty("EnableAllFolders");
                    if (enableAllProp?.GetValue(policy) is true) continue;

                    var foldersProp = policy.GetType().GetProperty("EnabledFolders");
                    var folders = foldersProp?.GetValue(policy) as string[] ?? Array.Empty<string>();

                    var userNormId = user.Id.ToString().Replace("-", "").ToLowerInvariant();
                    var hasAccess = folders.Any(f => string.Equals(f.Replace("-", ""), normalizedLibId, StringComparison.OrdinalIgnoreCase));
                    var shouldHaveAccess = assignedSet.Contains(userNormId);

                    if (shouldHaveAccess == hasAccess) continue;

                    string[] newFolders;
                    if (shouldHaveAccess)
                        newFolders = folders.Concat(new[] { normalizedLibId }).ToArray();
                    else
                        newFolders = folders.Where(f => !string.Equals(f.Replace("-", ""), normalizedLibId, StringComparison.OrdinalIgnoreCase)).ToArray();

                    foldersProp?.SetValue(policy, newFolders);

                    var up = updateMethod.GetParameters();
                    updateMethod.Invoke(_userManager, BuildArgList(up, BuildUserArg(up[0].ParameterType, user), policy));
                }
                catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            }
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

                if (ShowTopList.IsShows(request.ContentType))
                {
                    var series = ShowTopList.ResolveSeries(_libraryManager,
                        (request.Items ?? new List<ManualTopListItem>()).Select(i => (i.ItemId ?? "", i.ImdbId ?? "")));
                    var written = WriteShowFolder(request.ListName, folderPath, series, BadgeLook.Combine(request.BadgeStyle, request.BadgeOptions));
                    return new PrepareTopListFolderResponse { Success = true, FolderPath = folderPath, FilesCreated = written };
                }
                ShowTopList.ClearShowFolder(folderPath);

                foreach (var f in Directory.GetFiles(folderPath, "*.strm")) File.Delete(f);
                foreach (var f in Directory.GetFiles(folderPath, "*.nfo"))  File.Delete(f);
                foreach (var f in Directory.GetFiles(folderPath, "*.jpg"))  File.Delete(f);

                var items    = request.Items ?? new List<ManualTopListItem>();
                var seenKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                var selected = new List<(string BaseName, string FilePath, BaseItem Item)>();
                var strmToOriginal = new Dictionary<string, BaseItem>(StringComparer.OrdinalIgnoreCase);

                Dictionary<string, BaseItem>? imdbLookup = null;
                foreach (var entry in items)
                {
                    BaseItem? mediaItem = null;
                    if (Guid.TryParse(entry.ItemId, out var guid))
                        mediaItem = _libraryManager.GetItemById(guid);
                    // Fallback for restored backups: ItemIds are server-specific, IMDb ids are not.
                    if ((mediaItem == null || string.IsNullOrEmpty(mediaItem.Path)) && !string.IsNullOrWhiteSpace(entry.ImdbId))
                    {
                        imdbLookup ??= BuildImdbLookup();
                        imdbLookup.TryGetValue(entry.ImdbId.Trim(), out mediaItem);
                    }
                    if (mediaItem == null || string.IsNullOrEmpty(mediaItem.Path)) continue;
                    var baseName = SanitizeFolderName(mediaItem.Name);
                    if (mediaItem.ProductionYear.HasValue && mediaItem.ProductionYear > 0)
                        baseName += $" ({mediaItem.ProductionYear})";
                    if (!seenKeys.Add(baseName)) continue;
                    selected.Add((baseName, mediaItem.Path, mediaItem));
                    strmToOriginal[Path.Combine(folderPath, baseName + ".strm")] = mediaItem;
                }

                int digits = Math.Max(2, selected.Count.ToString().Length);
                int count  = 0;
                var look = BadgeLook.Combine(request.BadgeStyle, request.BadgeOptions);
                var rankExtras = TopListHistory.Update(request.ListName, selected.Select(x => x.Item).ToList(), look,
                    _jsonSerializer, _libraryManager, _userManager, _userDataManager, m => _logger.Warn(m));
                var tempDir2 = Path.Combine(Path.GetTempPath(), "hsc_toplist_" + Guid.NewGuid().ToString("N"));
                Directory.CreateDirectory(tempDir2);
                try
                {
                foreach (var entry in selected)
                {
                    count++;
                    var sortPrefix = count.ToString().PadLeft(digits, '0');
                    File.WriteAllText(Path.Combine(folderPath, entry.BaseName + ".nfo"), BuildTopListNfo(entry.Item, sortPrefix));
                    WriteRankedImages(entry.Item, count, Path.Combine(folderPath, entry.BaseName), look, tempDir2, rankExtras.TryGetValue(entry.Item.Id, out var rx) ? rx : null);
                    // .strm last — see PrepareTopListFolderRequest handler.
                    File.WriteAllText(Path.Combine(folderPath, entry.BaseName + ".strm"), entry.FilePath);
                }
                }
                finally { try { Directory.Delete(tempDir2, true); } catch { } }

                try
                {
                    var rankDir = Path.Combine(dataPath, "tag_ranks");
                    Directory.CreateDirectory(rankDir);
                    var rankIds = items.Where(i => !string.IsNullOrWhiteSpace(i.ImdbId))
                                       .Select(i => i.ImdbId).ToList();
                    _jsonSerializer.SerializeToFile(rankIds, Path.Combine(rankDir, sanitized + ".json"));
                }
                catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }

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
                        if (string.IsNullOrEmpty(li.Path)) continue;

                        if (sortPaths.TryGetValue(li.Path, out var newSort))
                        {
                            var prop = li.GetType().GetProperty("SortName");
                            if (prop?.CanWrite == true) prop.SetValue(li, newSort);
                        }

                        // Point poster and thumb at our local ranked images
                        ApplyRankedImages(li, folderPath);

                        // Merge STRM as an alternate version of the original library movie.
                        try
                        {
                            if (strmToOriginal.TryGetValue(li.Path, out var origItem) && li.Id != origItem.Id)
                            {
                                _libraryManager.MergeItems(new[] { origItem, li });
                                MergeTopListVersionsTask.QueueStrmProbe(_providerManager, _fileSystem, li);
                            }
                        }
                        catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }

                        try { _libraryManager.UpdateItem(li, li.Parent, ItemUpdateType.MetadataEdit, null); }
                        catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
                    }
                }
                catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }

                return new PrepareTopListFolderResponse { Success = true, FolderPath = folderPath, FilesCreated = count };
            }
            catch (Exception ex)
            {
                return new PrepareTopListFolderResponse { Success = false, Message = ex.Message };
            }
        }

        // Builds the .nfo for a top-list .strm entry. Carries the original movie's IMDb/TMDb ids
        // so Emby identifies the .strm item deterministically instead of guessing from the
        // "Title (Year)" file name — a wrong guess means no merge (duplicates in the UI) or a
        // merge with the wrong film.
        internal static string BuildTopListNfo(BaseItem item, string sortPrefix)
        {
            var sb = new System.Text.StringBuilder();
            sb.Append("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<movie>\n");
            // The original's own title, locked: the list's library may fetch metadata in another
            // language, and the copy would then show another name than the film it stands for.
            if (!string.IsNullOrWhiteSpace(item.Name))
                sb.Append("  <title>").Append(System.Security.SecurityElement.Escape(item.Name)).Append("</title>\n");
            sb.Append("  <sorttitle>").Append(sortPrefix).Append("</sorttitle>\n");

            var imdb = item.GetProviderId("Imdb");
            if (!string.IsNullOrWhiteSpace(imdb))
                sb.Append("  <imdbid>").Append(System.Security.SecurityElement.Escape(imdb)).Append("</imdbid>\n");
            var tmdb = item.GetProviderId("Tmdb");
            if (!string.IsNullOrWhiteSpace(tmdb))
                sb.Append("  <tmdbid>").Append(System.Security.SecurityElement.Escape(tmdb)).Append("</tmdbid>\n");

            sb.Append("  <lockedfields>Name|SortName|Images</lockedfields>\n</movie>");
            return sb.ToString();
        }

        // Renders <outputBase>.jpg and <outputBase>-thumb.jpg for a top-list entry, badged with
        // its rank. Fetches (and if needed refreshes) the source images first. badgeStyle is the
        // list's look: its BadgeStyle, or "BadgeStyle|BadgeOptions" when it is customised.
        internal static void WriteRankedImages(
            BaseItem item, int rank, string outputBase, string badgeStyle, string tempDir,
            IHttpClient httpClient, IProviderManager providerManager, ILibraryManager libraryManager,
            IFileSystem fileSystem, Action<string>? log = null, RankExtras? extras = null)
        {
            var (poster, thumb) = FetchImageSources(item, httpClient, tempDir, providerManager, libraryManager, fileSystem, log);
            // A series card always gets Emby's unplayed count in its top-right corner.
            if (item is MediaBrowser.Controller.Entities.TV.Series) { extras ??= new RankExtras(); extras.Show = true; }
            BadgeRenderer.RenderRanked(poster, thumb, rank, outputBase, badgeStyle, extras,
                (what, ex) => log?.Invoke($"Top-list: {what} failed for '{item.Name}' — {ex.Message}"));
        }

        private void WriteRankedImages(BaseItem item, int rank, string outputBase, string badgeStyle, string tempDir, RankExtras? extras = null)
            => WriteRankedImages(item, rank, outputBase, badgeStyle, tempDir,
                _httpClient, _providerManager, _libraryManager, _fileSystem, m => _logger.Info(m), extras);

        // Produces the local source files a top-list entry's ranked poster/thumb are rendered
        // from. Returned paths are files that exist on disk (or null when nothing is available).
        //
        // Thumb rows on the home screen fall back to Backdrop when an item has no Thumb, so we
        // do the same here — otherwise Emby shows its own unnumbered backdrop instead of our
        // badged image.
        //
        // The decision to refresh is based on whether the image could actually be obtained,
        // not on whether ImageInfos has a path: a path may point at a file that is gone, or at
        // a remote URL that fails to download. Only when the first attempt comes up short do
        // we run a targeted image refresh on that single item and try once more.
        internal static (string? Poster, string? Thumb) FetchImageSources(
            BaseItem item, IHttpClient httpClient, string tempDir,
            IProviderManager providerManager, ILibraryManager libraryManager, IFileSystem fileSystem,
            Action<string>? log = null)
        {
            var first = TryFetchImages(item, httpClient, tempDir);
            if (first.Poster != null && first.Thumb != null)
                return first;

            try
            {
                log?.Invoke($"Top-list: could not get {(first.Poster == null ? "poster" : "thumb/backdrop")} for '{item.Name}' — refreshing images and retrying");
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                providerManager.RefreshFullItem(item, new MetadataRefreshOptions(fileSystem)
                {
                    MetadataRefreshMode = MetadataRefreshMode.ValidationOnly,
                    ImageRefreshMode    = MetadataRefreshMode.FullRefresh,
                    ReplaceAllImages    = false,
                    ForceSave           = true
                }, cts.Token).GetAwaiter().GetResult();

                // Re-read from the library so we see the ImageInfos the refresh persisted.
                var refreshed = libraryManager.GetItemById(item.InternalId) ?? item;
                var second = TryFetchImages(refreshed, httpClient, tempDir);
                var result = (second.Poster ?? first.Poster, second.Thumb ?? first.Thumb);
                if (result.Item1 == null || result.Item2 == null)
                    log?.Invoke($"Top-list: still no {(result.Item1 == null ? "poster" : "thumb/backdrop")} for '{item.Name}' after refresh — check the library's image providers");
                return result;
            }
            catch (Exception ex)
            {
                log?.Invoke($"Top-list: image refresh for '{item.Name}' failed — {ex.Message}");
                return first;
            }
        }

        /// <summary>
        /// The poster file of each title for generated art, in order (null where a title has
        /// none): the item's own Primary image file on disk, read as it is. Only the poster is
        /// looked at (no thumb), nothing is refreshed; a poster Emby only knows by URL is
        /// downloaded. Done a few titles at a time, so a hundred posters take a moment.
        /// </summary>
        internal static List<string?> ArtPosterPaths(IList<BaseItem> items, IHttpClient httpClient, string tempDir)
        {
            var result = new string?[items.Count];
            System.Threading.Tasks.Parallel.For(0, items.Count, new System.Threading.Tasks.ParallelOptions { MaxDegreeOfParallelism = 6 }, i =>
            {
                try
                {
                    var path = (items[i].ImageInfos ?? Array.Empty<ItemImageInfo>()).FirstOrDefault(im => im.Type == ImageType.Primary)?.Path;
                    result[i] = EnsureLocalImagePath(httpClient, path, tempDir);
                }
                catch { result[i] = null; }
            });
            return result.ToList();
        }

        private static (string? Poster, string? Thumb) TryFetchImages(BaseItem item, IHttpClient httpClient, string tempDir)
        {
            var images = item.ImageInfos ?? Array.Empty<ItemImageInfo>();
            var poster = EnsureLocalImagePath(httpClient, images.FirstOrDefault(i => i.Type == ImageType.Primary)?.Path, tempDir);
            var thumb  = EnsureLocalImagePath(httpClient, images.FirstOrDefault(i => i.Type == ImageType.Thumb)?.Path, tempDir)
                      ?? EnsureLocalImagePath(httpClient, images.FirstOrDefault(i => i.Type == ImageType.Backdrop)?.Path, tempDir);
            return (poster, thumb);
        }

        // True when the item's poster and thumb already point at its ranked images in the folder.
        internal static bool RankedImagesApplied(BaseItem li, string folderPath)
        {
            var baseName = Path.GetFileNameWithoutExtension(li.Path);
            var images = li.ImageInfos ?? Array.Empty<ItemImageInfo>();
            foreach (var (type, suffix) in new[] { (ImageType.Primary, ".jpg"), (ImageType.Thumb, "-thumb.jpg") })
            {
                var path = Path.Combine(folderPath, baseName + suffix);
                if (File.Exists(path) && !images.Any(i => i.Type == type && string.Equals(i.Path, path, StringComparison.OrdinalIgnoreCase)))
                    return false;
            }
            return true;
        }

        // Points an already-indexed top-list .strm item at the ranked images on disk so the
        // new badge shows up without waiting for a library scan. When the files were rewritten,
        // DateModified must be refreshed too or Emby keeps serving the previously cached
        // (stale-numbered) image.
        internal static bool ApplyRankedImages(BaseItem li, string folderPath)
        {
            var baseName = Path.GetFileNameWithoutExtension(li.Path);
            var images = (li.ImageInfos ?? Array.Empty<ItemImageInfo>()).ToList();
            bool changed = false;

            foreach (var (type, suffix) in new[] { (ImageType.Primary, ".jpg"), (ImageType.Thumb, "-thumb.jpg") })
            {
                var path = Path.Combine(folderPath, baseName + suffix);
                if (!File.Exists(path)) continue;
                images.RemoveAll(i => i.Type == type);
                images.Add(new ItemImageInfo
                {
                    Path = path,
                    Type = type,
                    DateModified = File.GetLastWriteTimeUtc(path)
                });
                changed = true;
            }

            if (changed) li.ImageInfos = images.ToArray();
            return changed;
        }

        internal static string? EnsureLocalImagePath(IHttpClient httpClient, string? path, string tempDir)
        {
            if (string.IsNullOrEmpty(path)) return null;
            if (!path!.StartsWith("http", StringComparison.OrdinalIgnoreCase))
                return File.Exists(path) ? path : null;
            try
            {
                var tempPath = Path.Combine(tempDir, Guid.NewGuid().ToString("N") + ".jpg");
                using var stream = httpClient.Get(new MediaBrowser.Common.Net.HttpRequestOptions
                {
                    Url = path,
                    CancellationToken = CancellationToken.None
                }).GetAwaiter().GetResult();
                using var fs = File.Create(tempPath);
                stream.CopyTo(fs);
                return File.Exists(tempPath) ? tempPath : null;
            }
            catch
            {
                return null;
            }
        }

        internal static void CreateRankedPoster(string sourcePath, int rank, string outputPath, string badgeStyle = "neutral")
            => BadgeRenderer.CreateRankedPoster(sourcePath, rank, outputPath, badgeStyle);

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
                        movieCounts[name.ToLowerInvariant()] = ShowTopList.IsShowFolder(dir)
                            ? ShowTopList.ReadManifest(dir, _jsonSerializer).Count
                            : Directory.GetFiles(dir, "*.strm").Length;
                    }
                }

                // One cheap lookup per list instead of the full tag scan the page used to wait for.
                var tagBased = (Plugin.Instance.Configuration.TopLists ?? new List<TopListHomeSection>())
                    .Where(t => !string.IsNullOrWhiteSpace(t.TagName))
                    .Where(t => _libraryManager.GetItemList(new InternalItemsQuery { Tags = new[] { t.TagName }, Recursive = true, Limit = 1 }).Length > 0)
                    .Select(t => t.TagName)
                    .ToList();
                return new GetTopListsResponse { FolderNames = folderNames, MovieCounts = movieCounts, TagBased = tagBased };
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
                var cardSize    = "0";
                var badgeStyle  = "neutral";
                var badgeOptions = "";
                var realPoster  = "";
                var userIds     = new List<string>();
                if (tlConfig != null)
                {
                    try
                    {
                        var settings = _jsonSerializer.DeserializeFromString<Dictionary<string, string>>(tlConfig.HomeSectionSettings ?? "{}") ?? new Dictionary<string, string>();
                        customName  = settings.TryGetValue("CustomName",  out var cn) ? cn  : "";
                        displayMode = settings.TryGetValue("DisplayMode", out var dm) ? dm  : "";
                        imageType   = settings.TryGetValue("ImageType",   out var it) ? it  : "";
                        cardSize    = settings.TryGetValue("CardSizeOffset", out var cs) && !string.IsNullOrEmpty(cs) ? cs : "0";
                        badgeStyle  = settings.TryGetValue("BadgeStyle",  out var bs) ? bs  : "neutral";
                        badgeOptions = settings.TryGetValue("BadgeOptions", out var bo) ? bo ?? "" : "";
                        realPoster = settings.TryGetValue("RealPosterBadge", out var rp) ? rp ?? "" : "";
                    }
                    catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
                    userIds = tlConfig.HomeSectionUserIds ?? new List<string>();
                }

                var isShows = ShowTopList.IsShows(tlConfig) || ShowTopList.IsShowFolder(folderPath);
                var movies = isShows ? ReadTopListShows(folderPath) : ReadTopListMovies(folderPath);

                return new GetManualTopListItemsResponse
                {
                    Success     = true,
                    Movies      = movies,
                    CustomName  = customName,
                    DisplayMode = displayMode,
                    ImageType   = imageType,
                    CardSizeOffset = cardSize,
                    BadgeStyle  = badgeStyle,
                    BadgeOptions = badgeOptions,
                    RealPosterBadge = realPoster,
                    UserIds     = userIds,
                    ContentType = isShows ? ShowTopList.ContentTypeShows : "Movies"
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

                // The list's library was kept out of users' "My Media" and "Latest" — undo that while
                // the library can still be found by its folder (the UI removes it afterwards).
                var libGuid = FindLibraryGuidByPath(folderPath);
                if (!string.IsNullOrEmpty(libGuid))
                    TopListLibraryVisibility.Forget(_libraryManager.GetItemById(Guid.Parse(libGuid))?.InternalId.ToString() ?? "",
                        libGuid!, _userManager, m => _logger.Warn(m));

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
                            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
                        }
                        config.TopLists.Remove(tl);
                        // Made with a source's Create as top-list: switched off there too (else the next
                        // settings save would make it again).
                        // A source with nothing else to show (Apply Tag went off with the top-list) gets
                        // no plain row instead: it would have nothing in it.
                        if (tl.FromSource)
                            foreach (var tc in (config.Tags ?? new List<TagConfig>()).Where(t => string.Equals((t.Tag ?? "").Trim(), (tl.TagName ?? "").Trim(), StringComparison.OrdinalIgnoreCase)))
                            {
                                tc.EnableTopList = false;
                                if (!tc.EnableTag && !tc.EnableCollection && !tc.EnablePlaylist) tc.EnableHomeSection = false;
                            }
                        TopListMembers.Delete(tl.TagName);
                        Plugin.Instance!.SaveConfiguration();
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
                catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }

                if (!settingsDict.ContainsKey("SectionType"))
                    settingsDict["SectionType"] = "items";

                if (string.IsNullOrEmpty(tl.HomeSectionLibraryId) || tl.HomeSectionLibraryId == "auto")
                    return new PrepareTopListHomeSectionsResponse { Success = false, Message = "HomeSectionLibraryId is not set — library may not be ready yet." };

                var resolvedLibraryId = tl.HomeSectionLibraryId;

                // Exclude everything except this top-list's own library. The section is shown to
                // several users and each one's editor lists their own views (hidden libraries,
                // channels, Live TV), so collect the views of every selected user.
                var allLibIds = _libraryManager.GetVirtualFolders()
                    .Where(f => !string.IsNullOrEmpty(f.ItemId))
                    .Select(f => f.ItemId.Trim().ToLowerInvariant())
                    .ToList();
                // Users removed from Emby can still be on the list, and disabled ones are left as they
                // are (see UserStatus): both left out here and below.
                var userStatus = UserStatus.Read(_userManager);
                var listUsers = (tl.HomeSectionUserIds ?? new System.Collections.Generic.List<string>())
                    .Where(u => TopListSyncTask.UserExists(_userManager, u) && !userStatus.IsDisabled(u)).ToList();
                foreach (var viewUserId in listUsers)
                    allLibIds.AddRange(TopListSyncTask.GetSectionEditorViewIds(_userViewManager, _userManager, viewUserId));
                var ownIds = TopListSyncTask.OwnLibraryIds(resolvedLibraryId, _libraryManager);
                var storedExclude = (settingsDict.TryGetValue("_queryExcludeViewIds", out var storedEv) ? storedEv : "")
                    .Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries)
                    .Select(s => s.Trim().ToLowerInvariant()).Where(s => s.Length > 0);
                var mergedIds = allLibIds.Concat(storedExclude).Where(id => !ownIds.Contains(id)).Distinct().ToList();
                var excStr = string.Join(",", mergedIds);
                settingsDict["_queryExcludeViewIds"] = excStr;
                settingsDict["ExcludedFolders"] = excStr;
                tl.HomeSectionSettings = _jsonSerializer.SerializeToString(settingsDict);

                var safeTag = new string((request.TagName ?? "").Select(c => char.IsLetterOrDigit(c) ? c : '_').ToArray());
                var sectionMarker = "hsc__tl__" + safeTag;

                int created = 0, updated = 0;

                foreach (var userId in listUsers)
                {
                    try
                    {
                        var userInternalId = _userManager.GetInternalId(userId);
                        var currentSections = _userManager.GetHomeSections(userInternalId, CancellationToken.None);
                        var allSections = currentSections?.Sections ?? Array.Empty<ContentSection>();

                        var tracked = (tl.HomeSectionTracked ?? new System.Collections.Generic.List<HomeSectionTracking>())
                            .FirstOrDefault(t => t.UserId == userId);

                        ContentSection? ownedSection = null;
                        if (tracked != null && !string.IsNullOrEmpty(tracked.SectionId) && !tracked.SectionId.StartsWith("hsc__"))
                            ownedSection = allSections.FirstOrDefault(s => s.Id == tracked.SectionId);
                        // Its id is gone: the user's row on the list's own library. Never a row found by
                        // its title — a source's row can have the same title, and would be made over.
                        if (ownedSection == null)
                            ownedSection = allSections.FirstOrDefault(s => TopListSyncTask.IsTopListRow(s, ownIds));

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
                            // At the list's Position (a new row is at the bottom).
                            if (!string.IsNullOrEmpty(newId))
                                HomeScreenCompanionTask.PlaceNewRow(_userManager, userInternalId, newId, RowPlacement.PositionIn(tl.HomeSectionSettings), CancellationToken.None,
                                    TopListSyncTask.OtherUsers(tl, userId), settingsDict.TryGetValue("CustomName", out var _tlOwnName) ? _tlOwnName : null);
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
                    catch (Exception ex)
                    {
                        // The top-list sync queued below makes or updates the row again.
                        var who = Guid.TryParse(userId, out var whoGuid) ? (_userManager.GetUserById(whoGuid)?.Name ?? userId) : userId;
                        PluginLog.Write("WRN", "Page", $"Top-list '{tl.TagName}': the row for {who} could not be saved — {PluginLog.Describe(ex)}");
                    }
                }

                // Inject the new top-list library into _queryExcludeViewIds of every existing
                // TAG & COLLECT items-type section and apply immediately (no task run needed).
                var resolvedLibraryIdLower = resolvedLibraryId.Trim().ToLowerInvariant();
                if (!string.IsNullOrEmpty(resolvedLibraryId))
                {
                    foreach (var tc in (config.Tags ?? new System.Collections.Generic.List<TagConfig>()))
                    {
                        if (!SourceTopLists.PlainRow(tc)) continue;

                        var tcSettings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                        try
                        {
                            if (!string.IsNullOrEmpty(tc.HomeSectionSettings) && tc.HomeSectionSettings != "{}")
                                tcSettings = _jsonSerializer.DeserializeFromString<Dictionary<string, string>>(tc.HomeSectionSettings) ?? tcSettings;
                        }
                        catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }

                        tcSettings.TryGetValue("SectionType", out var tcSt);
                        // Only items rows get the top-list exclusion (boxset and playlist rows show one item list).
                        if (tcSt == "boxset" || tcSt == "playlist") continue;

                        var existingExcluded = (tcSettings.TryGetValue("_queryExcludeViewIds", out var ev) ? ev : "")
                            .Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries)
                            .Select(s => s.Trim()).ToList();

                        if (existingExcluded.Contains(resolvedLibraryIdLower, StringComparer.OrdinalIgnoreCase)) continue;

                        existingExcluded.Add(resolvedLibraryIdLower);
                        var tcExcStr = string.Join(",", existingExcluded);
                        tcSettings["_queryExcludeViewIds"] = tcExcStr;
                        tcSettings["ExcludedFolders"] = tcExcStr;
                        tc.HomeSectionSettings = _jsonSerializer.SerializeToString(tcSettings);
                        HomeScreenCompanionTask.ApplyRowTitle(tcSettings, tc, _jsonSerializer);

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
                            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
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
                        catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }

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
                            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
                        }
                    }
                }

                // Aggressive: exclude the new top-list library from ALL untracked, non-library-scoped sections
                // so top-list content never bleeds into manually created or native Emby sections.
                HomeScreenCompanionTask.UpdateUntrackedSections(
                    _jsonSerializer, _userManager, config,
                    new[] { resolvedLibraryIdLower },
                    CancellationToken.None);

                EnforceTopListLibraryPermissions(resolvedLibraryId, tl.HomeSectionUserIds);

                Plugin.Instance!.SaveConfiguration();
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
            // Another run busy: SyncAll waits for it in the background, so the request does not hang until then.
            if (RunGate.Busy)
            {
                var holder = RunGate.Holder;
                _ = Task.Run(() =>
                {
                    try { TopListSyncTask.SyncAll(_libraryManager, _userViewManager, _userManager, _jsonSerializer, _logger, CancellationToken.None); }
                    catch (Exception ex) { _logger.ErrorException("Top-list section sync failed", ex); }
                });
                return new SyncAllTopListSectionsResponse { Success = true, Queued = true,
                    Message = $"Queued — {(string.IsNullOrEmpty(holder) ? "another sync" : holder)} is running. The top-list sections are synced as soon as it has finished." };
            }
            try
            {
                var (updated, msg) = TopListSyncTask.SyncAll(_libraryManager, _userViewManager, _userManager, _jsonSerializer, _logger, CancellationToken.None);
                return new SyncAllTopListSectionsResponse { Success = true, UpdatedSections = updated, Message = msg };
            }
            catch (Exception ex)
            {
                return new SyncAllTopListSectionsResponse { Success = false, Message = ex.Message };
            }
        }

        // ── Backup & Restore ──────────────────────────────────────────────────────────────
        // Only configuration ("preconditions") is exported. Everything the sync tasks generate
        // (tags, collections, playlists, top-list strm/nfo/jpg files, caches, rank files) is
        // rebuilt on the next run and deliberately left out.

        private const int BackupFormatVersion = 2;

        public object Post(ExportBackupRequest request)
        {
            var config = Plugin.Instance?.Configuration;
            if (config == null) return new BackupFile();

            var file = new BackupFile
            {
                BackupVersion = BackupFormatVersion,
                PluginVersion = Plugin.Instance?.Version.ToString() ?? "0.0.0",
                CreatedUtc    = DateTime.UtcNow.ToString("o")
            };

            if (request.Settings)
            {
                file.Settings = new BackupSettings
                {
                    OpenAiModel               = config.OpenAiModel ?? "",
                    GeminiModel               = config.GeminiModel ?? "",
                    ClaudeModel               = config.ClaudeModel ?? "",
                    OllamaBaseUrl             = config.OllamaBaseUrl ?? "",
                    OllamaModel               = config.OllamaModel ?? "",
                    AiSystemPrompt            = config.AiSystemPrompt ?? "",
                    ExtendedConsoleOutput     = config.ExtendedConsoleOutput,
                    LogMissingItems           = config.LogMissingItems,
                    DryRunMode                = config.DryRunMode,
                    PreserveTagsOnEmptyResult = config.PreserveTagsOnEmptyResult,
                    IgnoreMarkedAsWatched     = config.IgnoreMarkedAsWatched,
                    TopListMirrorCollections  = config.TopListMirrorCollections,
                    ShowCopyPasteButtons      = config.ShowCopyPasteButtons,
                    ShowImportCollectionButton = config.ShowImportCollectionButton,
                    RankedCollectionsEnabled  = config.RankedCollectionsEnabled
                };
                file.Sections.Add("Settings");
            }

            if (request.ApiKeys)
            {
                file.ApiKeys = new BackupApiKeys
                {
                    TraktClientId = config.TraktClientId ?? "",
                    MdblistApiKey = config.MdblistApiKey ?? "",
                    TmdbApiKey    = config.TmdbApiKey ?? "",
                    OpenAiApiKey  = config.OpenAiApiKey ?? "",
                    GeminiApiKey  = config.GeminiApiKey ?? "",
                    ClaudeApiKey  = config.ClaudeApiKey ?? ""
                };
                file.Sections.Add("ApiKeys");
            }

            if (request.Tags)
            {
                file.Tags = (config.Tags ?? new List<TagConfig>()).ToList();
                file.Sections.Add("Tags");
            }

            if (request.SavedFilters)
            {
                file.SavedFilters = (config.SavedFilters ?? new List<SavedMediaInfoFilter>()).ToList();
                file.Sections.Add("SavedFilters");
            }

            if (request.TopLists)
            {
                // Same rule as the Top Lists tab: a list is manual if no source group manages its tag.
                var managedTags = new HashSet<string>(
                    (config.Tags ?? new List<TagConfig>())
                        .Where(t => !string.IsNullOrWhiteSpace(t.Tag))
                        .Select(t => t.Tag.Trim()),
                    StringComparer.OrdinalIgnoreCase);
                var dataPath = Plugin.Instance!.DataFolderPath;

                file.TopLists = new List<BackupTopList>();
                foreach (var tl in config.TopLists ?? new List<TopListHomeSection>())
                {
                    if (string.IsNullOrWhiteSpace(tl.TagName)) continue;
                    var entry = new BackupTopList { Config = tl, IsManual = !managedTags.Contains(tl.TagName.Trim()) };
                    if (entry.IsManual)
                    {
                        var folderPath = Path.Combine(dataPath, "toplists", SanitizeFolderName(tl.TagName));
                        entry.Items = (ShowTopList.IsShows(tl) ? ReadTopListShows(folderPath) : ReadTopListMovies(folderPath))
                            .Select(m => new BackupTopListItem { ImdbId = m.ImdbId, ItemId = m.ItemId, Name = m.Name, Year = m.Year })
                            .ToList();
                    }
                    file.TopLists.Add(entry);
                }
                file.Sections.Add("TopLists");
            }

            if (request.HomeSync)
            {
                file.HomeSync = new BackupHomeSync
                {
                    HomeSyncEnabled       = config.HomeSyncEnabled,
                    HomeSyncSourceUserId  = config.HomeSyncSourceUserId ?? "",
                    HomeSyncTargetUserIds = (config.HomeSyncTargetUserIds ?? new List<string>()).ToList(),
                    HomeSyncLibraryOrder  = config.HomeSyncLibraryOrder,
                    ContinueWatchingBumpEnabled  = config.ContinueWatchingBumpEnabled,
                    ContinueWatchingBumpMode     = config.ContinueWatchingBumpMode ?? ContinueWatchingBumper.ModeAllEpisodes,
                    ContinueWatchingBumpAllUsers = config.ContinueWatchingBumpAllUsers,
                    ContinueWatchingBumpUserIds  = (config.ContinueWatchingBumpUserIds ?? new List<string>()).ToList()
                };
                file.Sections.Add("HomeSync");
            }

            return file;
        }

        public object Post(ImportBackupRequest request)
        {
            var response = new ImportBackupResponse();
            try
            {
                if (string.IsNullOrWhiteSpace(request.BackupJson))
                    return Fail(response, "No backup data received.");

                var plugin = Plugin.Instance;
                if (plugin == null)
                    return Fail(response, "Plugin not initialized.");
                var config = plugin.Configuration;
                // The sources as they were before the restore (they may be replaced first): the source
                // a list that leaves with the restore came from (SourceTopLists.QueueVanished).
                var tagsBefore = (config.Tags ?? new List<TagConfig>()).ToList();
                var listsBefore = (config.TopLists ?? new List<TopListHomeSection>()).ToList();

                var backup = ParseBackup(request.BackupJson);
                if (backup == null)
                    return Fail(response, "The file could not be parsed as a Home Screen Companion backup.");

                bool Has(string section) => backup.Sections.Contains(section, StringComparer.OrdinalIgnoreCase);
                var knownUsers = LoadKnownUserIds();
                bool mirrorChanged = false;

                if (request.Settings && Has("Settings") && backup.Settings != null)
                {
                    var s = backup.Settings;
                    config.OpenAiModel               = s.OpenAiModel ?? "";
                    config.GeminiModel               = s.GeminiModel ?? "";
                    config.ClaudeModel               = s.ClaudeModel ?? "";
                    config.OllamaBaseUrl             = s.OllamaBaseUrl ?? "";
                    config.OllamaModel               = s.OllamaModel ?? "";
                    config.AiSystemPrompt            = s.AiSystemPrompt ?? "";
                    config.ExtendedConsoleOutput     = s.ExtendedConsoleOutput;
                    config.LogMissingItems           = s.LogMissingItems;
                    config.DryRunMode                = s.DryRunMode;
                    config.PreserveTagsOnEmptyResult = s.PreserveTagsOnEmptyResult;
                    config.IgnoreMarkedAsWatched     = s.IgnoreMarkedAsWatched;
                    mirrorChanged = config.TopListMirrorCollections != s.TopListMirrorCollections;
                    config.TopListMirrorCollections  = s.TopListMirrorCollections;
                    config.ShowCopyPasteButtons      = s.ShowCopyPasteButtons;
                    config.ShowImportCollectionButton = s.ShowImportCollectionButton;
                    config.RankedCollectionsEnabled  = s.RankedCollectionsEnabled;
                    response.Applied.Add("Settings");
                }

                if (request.ApiKeys && Has("ApiKeys") && backup.ApiKeys != null)
                {
                    var k = backup.ApiKeys;
                    config.TraktClientId = k.TraktClientId ?? "";
                    config.MdblistApiKey = k.MdblistApiKey ?? "";
                    config.TmdbApiKey    = k.TmdbApiKey ?? "";
                    config.OpenAiApiKey  = k.OpenAiApiKey ?? "";
                    config.GeminiApiKey  = k.GeminiApiKey ?? "";
                    config.ClaudeApiKey  = k.ClaudeApiKey ?? "";
                    response.Applied.Add("API keys");
                }

                if (request.Tags && Has("Tags") && backup.Tags != null)
                {
                    config.Tags = backup.Tags.Where(t => t != null).Select(NormalizeTag).ToList();
                    foreach (var t in config.Tags)
                    {
                        var label = string.IsNullOrEmpty(t.Name) ? t.Tag : t.Name;
                        PruneUnknownUsers(t.HomeSectionUserIds, knownUsers, response.Warnings, $"Group '{label}' home section");
                        PruneUnknownUsers(t.PlaylistUserIds, knownUsers, response.Warnings, $"Group '{label}' playlist");
                        t.HomeSectionTracked.RemoveAll(x => !IsKnownUser(knownUsers, x.UserId));
                        t.PlaylistMappings.RemoveAll(x => !IsKnownUser(knownUsers, x.UserId));
                        if (!string.IsNullOrEmpty(t.AiRecentlyWatchedUserId) && !IsKnownUser(knownUsers, t.AiRecentlyWatchedUserId))
                        {
                            t.AiRecentlyWatchedUserId = "";
                            var msg = $"Group '{label}': the user for AI recently-watched context does not exist on this server – pick a new user.";
                            if (!response.Warnings.Contains(msg)) response.Warnings.Add(msg);
                        }
                    }
                    // Same grouping key as the UI (Name + unit separator + Tag)
                    var groups = config.Tags.Select(t => string.IsNullOrEmpty(t.Name) ? t.Tag : t.Name + (char)31 + t.Tag).Distinct().Count();
                    response.Applied.Add($"Tag & collection groups ({groups})");
                }

                if (request.SavedFilters && Has("SavedFilters") && backup.SavedFilters != null)
                {
                    config.SavedFilters = backup.SavedFilters.Where(f => f != null).ToList();
                    foreach (var f in config.SavedFilters) f.Filters ??= new List<MediaInfoFilter>();
                    response.Applied.Add($"Saved filters ({config.SavedFilters.Count})");
                }

                if (request.HomeSync && Has("HomeSync") && backup.HomeSync != null)
                {
                    var h = backup.HomeSync;
                    config.HomeSyncEnabled       = h.HomeSyncEnabled;
                    config.HomeSyncSourceUserId  = h.HomeSyncSourceUserId ?? "";
                    config.HomeSyncTargetUserIds = (h.HomeSyncTargetUserIds ?? new List<string>()).ToList();
                    config.HomeSyncLibraryOrder  = h.HomeSyncLibraryOrder;
                    if (!string.IsNullOrEmpty(config.HomeSyncSourceUserId) && !IsKnownUser(knownUsers, config.HomeSyncSourceUserId))
                    {
                        config.HomeSyncSourceUserId = "";
                        if (config.HomeSyncEnabled)
                        {
                            config.HomeSyncEnabled = false;
                            response.Warnings.Add("Home screen sync: the source user does not exist on this server – sync has been disabled until you pick a new source user.");
                        }
                    }
                    PruneUnknownUsers(config.HomeSyncTargetUserIds, knownUsers, response.Warnings, "Home screen sync targets");
                    if (h.ContinueWatchingBumpEnabled.HasValue)  config.ContinueWatchingBumpEnabled  = h.ContinueWatchingBumpEnabled.Value;
                    if (!string.IsNullOrEmpty(h.ContinueWatchingBumpMode)) config.ContinueWatchingBumpMode = h.ContinueWatchingBumpMode!;
                    if (h.ContinueWatchingBumpAllUsers.HasValue) config.ContinueWatchingBumpAllUsers = h.ContinueWatchingBumpAllUsers.Value;
                    if (h.ContinueWatchingBumpUserIds != null)
                    {
                        config.ContinueWatchingBumpUserIds = h.ContinueWatchingBumpUserIds.ToList();
                        PruneUnknownUsers(config.ContinueWatchingBumpUserIds, knownUsers, response.Warnings, "Continue Watching bump users");
                    }
                    response.Applied.Add("Home screen sync");
                }

                if (request.TopLists && Has("TopLists") && backup.TopLists != null)
                {
                    var dataPath = plugin.DataFolderPath;
                    var virtualFolders = _libraryManager.GetVirtualFolders().ToList();
                    var restored = new List<TopListHomeSection>();

                    foreach (var entry in backup.TopLists)
                    {
                        var tl = entry?.Config;
                        if (tl == null || string.IsNullOrWhiteSpace(tl.TagName)) continue;
                        tl.HomeSectionUserIds ??= new List<string>();
                        tl.HomeSectionTracked ??= new List<HomeSectionTracking>();
                        if (string.IsNullOrEmpty(tl.HomeSectionSettings)) tl.HomeSectionSettings = "{}";
                        PruneUnknownUsers(tl.HomeSectionUserIds, knownUsers, response.Warnings, $"Top-list '{tl.TagName}'");
                        tl.HomeSectionTracked.RemoveAll(x => !IsKnownUser(knownUsers, x.UserId));

                        var settings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                        try { settings = _jsonSerializer.DeserializeFromString<Dictionary<string, string>>(tl.HomeSectionSettings) ?? settings; }
                        catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
                        var badgeStyle = settings.TryGetValue("BadgeStyle", out var bs) && !string.IsNullOrEmpty(bs) ? bs : "neutral";
                        var badgeOptions = settings.TryGetValue("BadgeOptions", out var bo) ? bo ?? "" : "";
                        var folderPath = Path.Combine(dataPath, "toplists", SanitizeFolderName(tl.TagName));

                        // Rebuild the folder so the library (existing or about to be created) has content.
                        // Tag-based folders are refilled by every sync run anyway; manual folders only exist
                        // through PrepareManualFolder, so the backup's item list is the source of truth.
                        try
                        {
                            if (entry!.IsManual)
                            {
                                var items = (entry.Items ?? new List<BackupTopListItem>())
                                    .Select(i => new ManualTopListItem { ImdbId = i.ImdbId ?? "", ItemId = i.ItemId ?? "" })
                                    .ToList();
                                if (items.Count > 0)
                                {
                                    var r = Post(new PrepareManualTopListFolderRequest { ListName = tl.TagName, BadgeStyle = badgeStyle, BadgeOptions = badgeOptions, Items = items, ContentType = tl.ContentType ?? "Movies" }) as PrepareTopListFolderResponse;
                                    if (r == null || !r.Success)
                                        response.Warnings.Add($"Top-list '{tl.TagName}': files could not be rebuilt ({r?.Message ?? "unknown error"}).");
                                    else if (r.FilesCreated < items.Count)
                                        response.Warnings.Add($"Top-list '{tl.TagName}': {items.Count - r.FilesCreated} of {items.Count} {(ShowTopList.IsShows(tl) ? "show(s)" : "movie(s)")} were not found in the library and were skipped.");
                                }
                                else
                                {
                                    Directory.CreateDirectory(folderPath);
                                    response.Warnings.Add($"Top-list '{tl.TagName}': the backup contains no {(ShowTopList.IsShows(tl) ? "shows" : "movies")} for this manual list.");
                                }
                            }
                            else
                            {
                                var r = Post(new PrepareTopListFolderRequest { TagName = tl.TagName, MaxItems = tl.MaxItems, BadgeStyle = badgeStyle, BadgeOptions = badgeOptions, ContentType = tl.ContentType ?? "Movies" }) as PrepareTopListFolderResponse;
                                if (r == null || !r.Success)
                                    response.Warnings.Add($"Top-list '{tl.TagName}': files could not be rebuilt ({r?.Message ?? "unknown error"}).");
                            }
                        }
                        catch (Exception ex)
                        {
                            response.Warnings.Add($"Top-list '{tl.TagName}': files could not be rebuilt ({ex.Message}).");
                        }

                        var libraryId = ResolveTopListLibraryId(tl.HomeSectionLibraryId, folderPath, virtualFolders);
                        if (libraryId == null)
                        {
                            tl.HomeSectionLibraryId = "auto";
                            response.TopListsNeedingLibrary.Add(new TopListLibraryInfo
                            {
                                TagName     = tl.TagName,
                                CustomName  = settings.TryGetValue("CustomName", out var cn) && !string.IsNullOrEmpty(cn) ? cn : tl.TagName,
                                DisplayMode = settings.TryGetValue("DisplayMode", out var dm) ? dm : "",
                                ImageType   = settings.TryGetValue("ImageType", out var it) ? it : "",
                                CardSizeOffset = settings.TryGetValue("CardSizeOffset", out var cs) && !string.IsNullOrEmpty(cs) ? cs : "0",
                                BadgeStyle  = badgeStyle,
                                BadgeOptions = badgeOptions,
                                MaxItems    = tl.MaxItems,
                                UserIds     = tl.HomeSectionUserIds.ToList(),
                                FolderPath  = folderPath,
                                ContentType = ShowTopList.IsShows(tl) ? ShowTopList.ContentTypeShows : "Movies"
                            });
                        }
                        else
                        {
                            tl.HomeSectionLibraryId = libraryId;
                        }
                        restored.Add(tl);
                    }

                    // Manual lists this server has that the backup does not: removed by the next run (rows,
                    // library, folder) instead of being left behind with nothing tracking them. Source
                    // lists follow their sources below.
                    SourceTopLists.QueueVanished(listsBefore.Where(t => !t.FromSource), restored, tagsBefore, "not in the restored backup", onlyFromSource: false);
                    config.TopLists = restored;
                    response.Applied.Add($"Top lists ({restored.Count})");
                }

                if (response.Applied.Count == 0)
                    return Fail(response, "Nothing selected to restore, or the selected sections are not present in the file.");

                // As a settings save does: each source with Top-List on has its list (one the backup
                // lacks gets the library and rows it has here), and a list whose source is gone or
                // switched off is queued for removal. A list whose source the restore left out is kept.
                SourceTopLists.Derive(new PluginConfiguration { Tags = tagsBefore, TopLists = listsBefore }, config, keepOrphans: true);
                plugin.SaveConfiguration();
                if (mirrorChanged) TopListCollectionMirror.QueueFullSync();
                response.Success = true;
                response.Message = $"Restored {response.Applied.Count} section(s).";
                return response;
            }
            catch (Exception ex)
            {
                return Fail(response, ex.Message);
            }
        }

        // Ids of every user on this server (normalized: no dashes, lower case). Null if the lookup failed,
        // in which case the import keeps user references untouched instead of guessing.
        private HashSet<string>? LoadKnownUserIds()
        {
            try
            {
                var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (var u in _userManager.GetUserList(new UserQuery()))
                    set.Add(u.Id.ToString("N"));
                return set;
            }
            catch { return null; }
        }

        private static bool IsKnownUser(HashSet<string>? known, string? userId)
        {
            if (known == null) return true;
            if (string.IsNullOrWhiteSpace(userId)) return false;
            return known.Contains(userId!.Replace("-", "").Trim());
        }

        // Removes user ids that do not exist on this server and reports them once per context.
        // A backup restored on another server carries that server's user ids; leaving them in place
        // would make every sync run warn (or, for home sync, fail) until the user re-selects them.
        private static void PruneUnknownUsers(List<string> ids, HashSet<string>? known, List<string> warnings, string context)
        {
            if (known == null || ids == null || ids.Count == 0) return;
            var unknown = ids.Where(id => !IsKnownUser(known, id)).Distinct().ToList();
            if (unknown.Count == 0) return;
            ids.RemoveAll(id => !IsKnownUser(known, id));
            var msg = $"{context}: {unknown.Count} selected user(s) do not exist on this server and were removed – re-select users in the UI.";
            if (!warnings.Contains(msg)) warnings.Add(msg);
        }

        private static ImportBackupResponse Fail(ImportBackupResponse response, string message)
        {
            response.Success = false;
            response.Message = message;
            return response;
        }

        // Accepts both the current format (BackupFile) and legacy files, which were a raw
        // PluginConfiguration dump created by the old client-side backup button.
        // The fields that mark a legacy backup (a saved PluginConfiguration) as holding settings / API keys.
        private static readonly string[] LegacySettingKeys =
        {
            "OpenAiModel", "GeminiModel", "ClaudeModel", "OllamaBaseUrl", "OllamaModel", "AiSystemPrompt", "ExtendedConsoleOutput",
            "LogMissingItems", "DryRunMode", "PreserveTagsOnEmptyResult", "TopListMirrorCollections", "ShowCopyPasteButtons",
            "ShowImportCollectionButton", "RankedCollectionsEnabled"
        };
        private static readonly string[] LegacyApiKeyKeys =
            { "TraktClientId", "MdblistApiKey", "TmdbApiKey", "OpenAiApiKey", "GeminiApiKey", "ClaudeApiKey" };

        private BackupFile? ParseBackup(string json)
        {
            bool isCurrentFormat = System.Text.RegularExpressions.Regex.IsMatch(json, "\"BackupVersion\"\\s*:");
            if (isCurrentFormat)
            {
                BackupFile? file = null;
                try { file = _jsonSerializer.DeserializeFromString<BackupFile>(json); } catch { }
                if (file == null) return null;
                file.Sections ??= new List<string>();
                return file;
            }

            PluginConfiguration? legacy = null;
            try { legacy = _jsonSerializer.DeserializeFromString<PluginConfiguration>(json); } catch { }
            if (legacy == null) return null;
            bool HasKey(string key) => System.Text.RegularExpressions.Regex.IsMatch(json, "\"" + key + "\"\\s*:");
            // A legacy backup is a saved plugin configuration. Only the sections whose fields are in
            // the file are offered: any other JSON (a copied source, another plugin's file) would
            // otherwise "restore" the defaults over the current settings and wipe every API key.
            // A copied source or top-list (Copy on a card) is not a backup either: its "Tags" would
            // replace every group.
            if (HasKey("hsc-source") || HasKey("hsc-toplist")) return null;
            bool hasSettings = LegacySettingKeys.Any(HasKey), hasKeys = LegacyApiKeyKeys.Any(HasKey);
            if (!hasSettings && !hasKeys && !HasKey("Tags") && !HasKey("SavedFilters")) return null;
            var result = new BackupFile { BackupVersion = 1 };
            result.Settings = new BackupSettings
            {
                OpenAiModel               = legacy.OpenAiModel ?? "",
                GeminiModel               = legacy.GeminiModel ?? "",
                ClaudeModel               = legacy.ClaudeModel ?? "",
                OllamaBaseUrl             = legacy.OllamaBaseUrl ?? "",
                OllamaModel               = legacy.OllamaModel ?? "",
                AiSystemPrompt            = legacy.AiSystemPrompt ?? "",
                ExtendedConsoleOutput     = legacy.ExtendedConsoleOutput,
                LogMissingItems           = legacy.LogMissingItems,
                DryRunMode                = legacy.DryRunMode,
                PreserveTagsOnEmptyResult = legacy.PreserveTagsOnEmptyResult,
                TopListMirrorCollections  = legacy.TopListMirrorCollections,
                ShowCopyPasteButtons      = legacy.ShowCopyPasteButtons,
                ShowImportCollectionButton = legacy.ShowImportCollectionButton,
                RankedCollectionsEnabled  = legacy.RankedCollectionsEnabled
            };
            if (hasSettings) result.Sections.Add("Settings");
            result.ApiKeys = new BackupApiKeys
            {
                TraktClientId = legacy.TraktClientId ?? "",
                MdblistApiKey = legacy.MdblistApiKey ?? "",
                TmdbApiKey    = legacy.TmdbApiKey ?? "",
                OpenAiApiKey  = legacy.OpenAiApiKey ?? "",
                GeminiApiKey  = legacy.GeminiApiKey ?? "",
                ClaudeApiKey  = legacy.ClaudeApiKey ?? ""
            };
            if (hasKeys) result.Sections.Add("ApiKeys");
            if (HasKey("Tags")) { result.Tags = legacy.Tags ?? new List<TagConfig>(); result.Sections.Add("Tags"); }
            if (HasKey("SavedFilters")) { result.SavedFilters = legacy.SavedFilters ?? new List<SavedMediaInfoFilter>(); result.Sections.Add("SavedFilters"); }
            return result;
        }

        // Backups written by other plugin versions may lack list properties entirely; the rest of
        // the plugin assumes they are never null.
        private static TagConfig NormalizeTag(TagConfig t)
        {
            t.Name                ??= "";
            t.Tag                 ??= "";
            t.Url                 ??= "";
            t.SourceType          ??= "External";
            t.LocalSourceId       ??= "";
            t.LocalSources        ??= new List<string>();
            t.MediaInfoConditions ??= new List<string>();
            t.MediaInfoFilters    ??= new List<MediaInfoFilter>();
            t.Blacklist           ??= new List<string>();
            t.ActiveIntervals     ??= new List<DateInterval>();
            t.HomeSectionUserIds  ??= new List<string>();
            t.HomeSectionTracked  ??= new List<HomeSectionTracking>();
            t.PlaylistUserIds     ??= new List<string>();
            t.PlaylistMappings    ??= new List<PlaylistMapping>();
            t.CollectionPosterStyle     ??= "";
            t.CollectionArtTitle        ??= "";
            t.CollectionBackgroundStyle ??= "";
            t.CollectionBackgroundPath  ??= "";
            t.CollectionThumbStyle      ??= "";
            t.CollectionThumbOptions    ??= "";
            t.CollectionThumbPath       ??= "";
            t.CollectionBannerStyle     ??= "";
            t.CollectionBannerPath      ??= "";
            t.TagPosterStyle            ??= "";
            t.TagArtTitle               ??= "";
            t.TagBackgroundStyle        ??= "";
            t.TagBackgroundPath         ??= "";
            t.CollectionPosterOptions     ??= "";
            t.CollectionBackgroundOptions ??= "";
            t.TagPosterOptions            ??= "";
            t.TagBackgroundOptions        ??= "";
            t.PlaylistPosterOptions       ??= "";
            t.PlaylistBackgroundOptions   ??= "";
            if (string.IsNullOrEmpty(t.HomeSectionLibraryId)) t.HomeSectionLibraryId = "auto";
            if (string.IsNullOrEmpty(t.HomeSectionSettings)) t.HomeSectionSettings = "{}";
            foreach (var m in t.PlaylistMappings) m.LastSyncedItemIds ??= new List<long>();
            foreach (var f in t.MediaInfoFilters) f.Criteria ??= new List<string>();
            return t;
        }

        // Finds the Emby library for a top-list: first by the stored id, then by the folder path
        // (covers ids that changed, or a library created manually for the same folder).
        private static string? ResolveTopListLibraryId(string? storedId, string folderPath, List<VirtualFolderInfo> folders)
        {
            string Norm(string? p) => (p ?? "").Replace("\\", "/").TrimEnd('/').ToLowerInvariant();

            var stored = (storedId ?? "").Trim();
            if (stored.Length > 0 && !string.Equals(stored, "auto", StringComparison.OrdinalIgnoreCase))
            {
                var storedNorm = stored.Replace("-", "").ToLowerInvariant();
                if (folders.Any(f => !string.IsNullOrEmpty(f.ItemId) && f.ItemId.Replace("-", "").ToLowerInvariant() == storedNorm))
                    return stored;
            }

            var target = Norm(folderPath);
            var byPath = folders.FirstOrDefault(f => (f.Locations ?? Array.Empty<string>()).Any(l => Norm(l) == target));
            return string.IsNullOrEmpty(byPath?.ItemId) ? null : byPath!.ItemId;
        }

        // Reads a top-list folder's .strm files in rank order (rank = <sorttitle> in the sibling .nfo)
        // and resolves each one back to its original library movie.
        private List<MovieItem> ReadTopListMovies(string folderPath)
        {
            var movies = new List<MovieItem>();
            if (!Directory.Exists(folderPath)) return movies;

            var entries = new List<(int Rank, string MoviePath)>();
            foreach (var strmFile in Directory.GetFiles(folderPath, "*.strm"))
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
                    catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
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
            return movies;
        }

        // A show list folder's series in rank order, as the manual list editor and backups use them.
        private List<MovieItem> ReadTopListShows(string folderPath) =>
            ShowTopList.ReadManifest(folderPath, _jsonSerializer)
                .Select(e => new MovieItem { Name = e.Name, Year = e.Year, ImdbId = e.ImdbId, ItemId = e.SeriesId })
                .ToList();

        // GUID (no dashes) of the library whose location is this folder, or null.
        private string? FindLibraryGuidByPath(string folderPath)
        {
            static string Norm(string p) => (p ?? "").Replace('\\', '/').TrimEnd('/').ToLowerInvariant();
            var target = Norm(folderPath);
            var folder = _libraryManager.GetVirtualFolders()
                .FirstOrDefault(f => (f.Locations ?? Array.Empty<string>()).Any(l => Norm(l) == target));
            if (folder == null) return null;
            if (Guid.TryParse(folder.Guid, out var g)) return g.ToString("N");
            return long.TryParse(folder.ItemId, out var id) ? _libraryManager.GetItemById(id)?.Id.ToString("N") : null;
        }

        // Movies outside the top-list folder keyed by IMDb id — lets manual top-lists be restored
        // on a server where the ItemIds in the backup no longer match.
        private Dictionary<string, BaseItem> BuildImdbLookup()
        {
            var lookup = new Dictionary<string, BaseItem>(StringComparer.OrdinalIgnoreCase);
            try
            {
                var topListsFolder = Path.Combine(Plugin.Instance!.DataFolderPath, "toplists") + Path.DirectorySeparatorChar;
                foreach (var m in _libraryManager.GetItemList(new InternalItemsQuery
                {
                    IncludeItemTypes = new[] { "Movie" },
                    Recursive = true,
                    IsVirtualItem = false
                }))
                {
                    if (string.IsNullOrEmpty(m.Path)) continue;
                    if (m.Path.StartsWith(topListsFolder, StringComparison.OrdinalIgnoreCase)) continue;
                    if (RankedCollections.IsRankedPath(m.Path)) continue;
                    var imdb = m.GetProviderId("Imdb");
                    if (!string.IsNullOrEmpty(imdb) && !lookup.ContainsKey(imdb))
                        lookup[imdb] = m;
                }
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            return lookup;
        }

        private static string SanitizeFolderName(string name)
        {
            var invalid = Path.GetInvalidFileNameChars();
            var safe = new string((name ?? "unknown").Select(c => Array.IndexOf(invalid, c) >= 0 ? '_' : c).ToArray()).Trim('.');
            return string.IsNullOrWhiteSpace(safe) ? "unknown" : safe;
        }

        // ── Copy / paste one source ───────────────────────────────────────────────────────
        // Same TagConfig format as the backup's Tags section (and the same NormalizeTag on the
        // way in). Ids that only mean something on this server travel with their names; images
        // travel as base64. Run state (tracked home sections, playlist links, AI last run,
        // LastModified) is left out: the pasted source is new on the other server.

        private const int SourceFormatVersion = 1;
        private const int MaxSourceImageBytes = 5 * 1024 * 1024;
        private static readonly string[] UserCriterionProps = { "IsPlayed", "LastPlayed", "PlayCount" };
        private const string SmartFilterUsers = "Smart filter rules";

        private static string NormId(string? id) => (id ?? "").Replace("-", "").Trim().ToLowerInvariant();

        private static void ClearSourceRunState(TagConfig t)
        {
            t.HomeSectionTracked = new List<HomeSectionTracking>();
            t.PlaylistMappings   = new List<PlaylistMapping>();
            t.AiLastRunDate      = DateTime.MinValue;
            t.LastRunCount       = 0; t.LastRunLimit = 0;
            t.LastModified       = DateTime.MinValue;
            // On another server the collection is HSC's own, not one it took over.
            t.CollectionImported = false; t.CollectionPosterReplaced = false; t.CollectionBackgroundReplaced = false;
            t.CollectionThumbReplaced = false; t.CollectionBannerReplaced = false;
        }

        // The user id inside a smart-filter rule (IsPlayed/LastPlayed/PlayCount:<userId>:<op>:<value>,
        // optionally "!"-prefixed), or null when the rule has none ("Any user"/"All users" included).
        private static string? CriterionUserId(string criterion)
        {
            var parts = (criterion ?? "").TrimStart('!').Split(':');
            if (parts.Length != 4 || !UserCriterionProps.Contains(parts[0])) return null;
            var uid = parts[1].Trim();
            return uid.Length == 0 || uid.StartsWith("__") ? null : uid;
        }

        private static string WithCriterionUserId(string criterion, string userId)
        {
            var neg = criterion.StartsWith("!") ? "!" : "";
            var parts = criterion.TrimStart('!').Split(':');
            parts[1] = userId;
            return neg + string.Join(":", parts);
        }

        private Dictionary<string, string> ParseSectionSettings(string? json)
        {
            var d = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            if (string.IsNullOrWhiteSpace(json) || json == "{}") return d;
            try { d = _jsonSerializer.DeserializeFromString<Dictionary<string, string>>(json!) ?? d; } catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            return new Dictionary<string, string>(d, StringComparer.OrdinalIgnoreCase);
        }

        private static readonly string[] ExcludedLibraryKeys = { "_queryExcludeViewIds", "ExcludedFolders" };

        private static IEnumerable<string> SplitIds(string? csv) =>
            (csv ?? "").Split(',').Select(x => x.Trim()).Where(x => x.Length > 0);

        private string? SourceImagesDir()
        {
            var dataPath = Plugin.Instance?.DataFolderPath;
            return dataPath == null ? null : Path.Combine(dataPath, "collection_images");
        }

        public object Post(ExportSourceRequest request)
        {
            var file = new SourceFile
            {
                SourceVersion = SourceFormatVersion,
                PluginVersion = Plugin.Instance?.Version.ToString() ?? "0.0.0",
                CreatedUtc    = DateTime.UtcNow.ToString("o")
            };
            var tags = (request.Tags ?? new List<TagConfig>()).Where(t => t != null).Select(NormalizeTag).ToList();
            file.Tags = tags;
            if (tags.Count == 0) return file;

            var userNames = UserNamesById();
            var libraryNames = new Dictionary<string, string>();
            try
            {
                foreach (var f in _libraryManager.GetVirtualFolders())
                    if (!string.IsNullOrEmpty(f.ItemId)) libraryNames[NormId(f.ItemId)] = f.Name ?? "";
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }

            void AddUser(string? id)
            {
                if (string.IsNullOrWhiteSpace(id) || file.Users.ContainsKey(id!)) return;
                if (userNames.TryGetValue(NormId(id), out var name)) file.Users[id!] = name;
            }
            void AddLibrary(string? id)
            {
                if (string.IsNullOrWhiteSpace(id) || string.Equals(id, "auto", StringComparison.OrdinalIgnoreCase) || file.Libraries.ContainsKey(id!)) return;
                if (libraryNames.TryGetValue(NormId(id), out var name)) file.Libraries[id!] = name;
            }

            var imagesDir = SourceImagesDir();
            var fullImagesDir = imagesDir == null ? null : Path.GetFullPath(imagesDir) + Path.DirectorySeparatorChar;
            void AddImage(string? path, string what)
            {
                if (string.IsNullOrWhiteSpace(path) || file.Images.ContainsKey(path!)) return;
                string? problem = null;
                try
                {
                    var full = Path.GetFullPath(path!);
                    if (fullImagesDir == null || !full.StartsWith(fullImagesDir, StringComparison.OrdinalIgnoreCase) || !File.Exists(full))
                        problem = "the image file was not found on this server";
                    else
                    {
                        var len = new FileInfo(full).Length;
                        if (len > MaxSourceImageBytes)
                            problem = $"the image is too large to copy ({len / 1024.0 / 1024.0:0.0} MB, the limit is {MaxSourceImageBytes / 1024 / 1024} MB) – upload it again on the other server";
                        else
                            file.Images[path!] = new SourceImage { FileName = Path.GetFileName(full), Base64 = Convert.ToBase64String(File.ReadAllBytes(full)) };
                    }
                }
                catch (Exception ex) { problem = "the image could not be read (" + ex.Message + ")"; }
                if (problem != null)
                {
                    var msg = $"{what}: {problem}. The copy has no {what.ToLowerInvariant()}.";
                    if (!file.Notices.Contains(msg)) file.Notices.Add(msg);
                }
            }

            foreach (var t in tags)
            {
                ClearSourceRunState(t);
                foreach (var id in t.HomeSectionUserIds) AddUser(id);
                foreach (var id in t.PlaylistUserIds) AddUser(id);
                AddUser(t.AiRecentlyWatchedUserId);
                foreach (var f in t.MediaInfoFilters)
                    foreach (var c in f.Criteria) AddUser(CriterionUserId(c));

                AddLibrary(t.HomeSectionLibraryId);
                var settings = ParseSectionSettings(t.HomeSectionSettings);
                foreach (var key in ExcludedLibraryKeys)
                    if (settings.TryGetValue(key, out var csv))
                        foreach (var id in SplitIds(csv)) AddLibrary(id);

                foreach (var id in t.ManualItemIds ?? new List<string>())
                {
                    if (string.IsNullOrWhiteSpace(id) || file.Items.ContainsKey(id)) continue;
                    BaseItem? item = null;
                    if (long.TryParse(id, out var internalId)) item = _libraryManager.GetItemById(internalId);
                    else if (Guid.TryParse(id, out var guid)) item = _libraryManager.GetItemById(guid);
                    if (item == null) continue; // already gone here; the import reports it as missing
                    file.Items[id] = ItemRefFor(item);
                }

                AddImage(t.CollectionPosterPath, "Collection poster");
                AddImage(t.CollectionBackgroundPath, "Collection background");
                AddImage(t.CollectionThumbPath, "Collection thumb");
                AddImage(t.CollectionBannerPath, "Collection banner");
                AddImage(t.TagBackgroundPath, "Tag background");
            }

            // Top-lists are configured separately (Top Lists tab) and are not part of a source.
            var tagNames = new HashSet<string>(tags.Select(t => (t.Tag ?? "").Trim()).Where(s => s.Length > 0), StringComparer.OrdinalIgnoreCase);
            foreach (var tl in Plugin.Instance?.Configuration?.TopLists ?? new List<TopListHomeSection>())
            {
                if (tl == null) continue;
                if (tagNames.Contains((tl.TagName ?? "").Trim()))
                    file.Notices.Add($"This source feeds the top-list '{tl.TagName}'. Top-lists are not copied with a source – copy it with Copy on the top-list in the Top Lists tab and paste it there after this source.");
            }
            return file;
        }

        public object Post(ImportSourceRequest request)
        {
            var response = new ImportSourceResponse();
            try
            {
                var json = request.SourceJson ?? "";
                if (string.IsNullOrWhiteSpace(json)) return FailSource(response, "Nothing was pasted.");
                if (!System.Text.RegularExpressions.Regex.IsMatch(json, "\"hsc-source\"\\s*:\\s*\\d"))
                    return FailSource(response, "This is not a copied Home Screen Companion source (the \"hsc-source\" header is missing). Use Copy on a source card and paste that text.");
                SourceFile? file = null;
                try { file = _jsonSerializer.DeserializeFromString<SourceFile>(json); } catch { }
                if (file == null || file.Tags == null || file.Tags.Count(t => t != null) == 0)
                    return FailSource(response, "The pasted text could not be read as a source.");
                file.Users     ??= new Dictionary<string, string>();
                file.Libraries ??= new Dictionary<string, string>();
                file.Items     ??= new Dictionary<string, SourceItemRef>();
                file.Images    ??= new Dictionary<string, SourceImage>();

                var tags = file.Tags.Where(t => t != null).Select(NormalizeTag).ToList();
                var notices = response.Notices;
                void Notice(string msg) { if (!notices.Contains(msg)) notices.Add(msg); }
                string Label(string id, Dictionary<string, string> names) =>
                    names.TryGetValue(id, out var n) && !string.IsNullOrEmpty(n) ? n : "unknown (id " + id + ")";

                // Users: by name, case-insensitive.
                var localUsers = LocalUserIdsByName();
                string? MapUser(string id) =>
                    file.Users.TryGetValue(id, out var name) && !string.IsNullOrEmpty(name) && localUsers.TryGetValue(name, out var local) ? local : null;
                var droppedUsers = new Dictionary<string, SortedSet<string>>();
                void DropUser(string context, string id)
                {
                    if (!droppedUsers.TryGetValue(context, out var set)) droppedUsers[context] = set = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);
                    set.Add(Label(id, file.Users));
                }
                List<string> MapUsers(List<string> ids, string context)
                {
                    var result = new List<string>();
                    foreach (var id in ids.Where(x => !string.IsNullOrWhiteSpace(x)))
                    {
                        var local = MapUser(id);
                        if (local == null) DropUser(context, id);
                        else if (!result.Contains(local)) result.Add(local);
                    }
                    return result;
                }

                // Libraries: by name, case-insensitive.
                var localLibraries = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                foreach (var f in _libraryManager.GetVirtualFolders())
                    if (!string.IsNullOrEmpty(f.Name) && !string.IsNullOrEmpty(f.ItemId) && !localLibraries.ContainsKey(f.Name)) localLibraries[f.Name] = f.ItemId;
                string? MapLibrary(string id) =>
                    file.Libraries.TryGetValue(id, out var name) && !string.IsNullOrEmpty(name) && localLibraries.TryGetValue(name, out var local) ? local : null;
                var droppedLibraries = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);

                // Manual List items: by IMDb / TMDb / TVDb id, then name + year.
                Func<SourceItemRef, BaseItem?>? findItem = null;
                if (tags.Any(t => t.ManualItemIds.Count > 0)) findItem = BuildSourceItemFinder();
                var missingItems = new List<string>();

                // Images: uploaded once per distinct file, like the card's own upload button.
                var newImagePaths = new Dictionary<string, string>();
                string MapImage(string path, string what)
                {
                    if (string.IsNullOrWhiteSpace(path)) return "";
                    if (newImagePaths.TryGetValue(path, out var done)) return done;
                    var newPath = "";
                    if (file.Images.TryGetValue(path, out var img) && img != null && !string.IsNullOrEmpty(img.Base64))
                    {
                        var r = Post(new UploadCollectionImageRequest { FileName = img.FileName ?? "", Base64Data = img.Base64 }) as UploadCollectionImageResponse;
                        if (r != null && r.Success) newPath = r.FilePath;
                        else Notice($"{what}: the copied image could not be saved ({r?.Message ?? "unknown error"}) – upload it again.");
                    }
                    else Notice($"{what}: the copied text has no image (too large or missing on the other server) – upload it again.");
                    newImagePaths[path] = newPath;
                    return newPath;
                }

                // Local collection / playlist sources are stored by name: just check they exist.
                var localSourceNames = new Dictionary<string, HashSet<string>>();
                bool LocalSourceExists(string type, string name)
                {
                    if (!localSourceNames.TryGetValue(type, out var set))
                    {
                        set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                        try
                        {
                            foreach (var i in _libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { type }, Recursive = true }))
                                if (!string.IsNullOrEmpty(i.Name)) set.Add(i.Name);
                        }
                        catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
                        localSourceNames[type] = set;
                    }
                    return set.Contains(name);
                }

                foreach (var t in tags)
                {
                    ClearSourceRunState(t);
                    t.HomeSectionUserIds = MapUsers(t.HomeSectionUserIds, "Home section users");
                    t.PlaylistUserIds    = MapUsers(t.PlaylistUserIds, "Playlist users");
                    if (!string.IsNullOrWhiteSpace(t.AiRecentlyWatchedUserId))
                    {
                        var local = MapUser(t.AiRecentlyWatchedUserId);
                        if (local == null) DropUser("AI recently-watched user", t.AiRecentlyWatchedUserId);
                        t.AiRecentlyWatchedUserId = local ?? "";
                    }
                    foreach (var f in t.MediaInfoFilters)
                    {
                        for (int i = 0; i < f.Criteria.Count; i++)
                        {
                            var uid = CriterionUserId(f.Criteria[i]);
                            if (uid == null) continue;
                            var local = MapUser(uid);
                            if (local == null) DropUser(SmartFilterUsers, uid);
                            f.Criteria[i] = WithCriterionUserId(f.Criteria[i], local ?? "__any__");
                        }
                    }

                    var libId = (t.HomeSectionLibraryId ?? "").Trim();
                    if (libId.Length > 0 && !string.Equals(libId, "auto", StringComparison.OrdinalIgnoreCase))
                    {
                        var local = MapLibrary(libId);
                        if (local == null) droppedLibraries.Add(Label(libId, file.Libraries) + " (home section library, now auto)");
                        t.HomeSectionLibraryId = local ?? "auto";
                    }
                    var settings = ParseSectionSettings(t.HomeSectionSettings);
                    bool settingsChanged = false;
                    foreach (var key in ExcludedLibraryKeys)
                    {
                        if (!settings.TryGetValue(key, out var csv)) continue;
                        var mapped = new List<string>();
                        foreach (var id in SplitIds(csv))
                        {
                            var local = MapLibrary(id);
                            if (local == null) droppedLibraries.Add(Label(id, file.Libraries) + " (excluded library)");
                            else if (!mapped.Contains(local)) mapped.Add(local);
                        }
                        // No key = the page's default (top-list libraries excluded).
                        if (mapped.Count == 0) settings.Remove(key); else settings[key] = string.Join(",", mapped);
                        settingsChanged = true;
                    }
                    if (settingsChanged) t.HomeSectionSettings = _jsonSerializer.SerializeToString(settings);

                    if (t.ManualItemIds.Count > 0)
                    {
                        var mappedItems = new List<string>();
                        foreach (var id in t.ManualItemIds.Where(x => !string.IsNullOrWhiteSpace(x)))
                        {
                            file.Items.TryGetValue(id, out var itemRef);
                            var item = itemRef != null && findItem != null ? findItem(itemRef) : null;
                            if (item == null)
                            {
                                var label = itemRef == null ? "unknown item (id " + id + ")" : ItemRefLabel(itemRef);
                                if (!missingItems.Contains(label)) missingItems.Add(label);
                                continue;
                            }
                            var localId = item.InternalId.ToString();
                            if (!mappedItems.Contains(localId)) mappedItems.Add(localId);
                        }
                        t.ManualItemIds = mappedItems;
                    }

                    t.CollectionPosterPath     = MapImage(t.CollectionPosterPath, "Collection poster");
                    t.CollectionBackgroundPath = MapImage(t.CollectionBackgroundPath, "Collection background");
                    t.CollectionThumbPath      = MapImage(t.CollectionThumbPath, "Collection thumb");
                    t.CollectionBannerPath     = MapImage(t.CollectionBannerPath, "Collection banner");
                    t.TagBackgroundPath        = MapImage(t.TagBackgroundPath, "Tag background");

                    if ((t.SourceType == "LocalCollection" || t.SourceType == "LocalPlaylist") && !string.IsNullOrWhiteSpace(t.LocalSourceId)
                        && !LocalSourceExists(t.SourceType == "LocalPlaylist" ? "Playlist" : "BoxSet", t.LocalSourceId.Trim()))
                        Notice($"{(t.SourceType == "LocalPlaylist" ? "Playlist" : "Collection")} '{t.LocalSourceId}' does not exist on this server – the source finds nothing until it does.");
                }

                foreach (var kv in droppedUsers)
                    Notice(kv.Key == SmartFilterUsers
                        ? $"{kv.Key}: users not on this server, set to Any user: {string.Join(", ", kv.Value)}."
                        : $"{kv.Key} not on this server, left out: {string.Join(", ", kv.Value)}.");
                if (droppedLibraries.Count > 0)
                    Notice($"Libraries not on this server, left out: {string.Join(", ", droppedLibraries)}.");
                if (missingItems.Count > 0)
                    Notice($"Manual List: {missingItems.Count} title(s) not in this server's library, left out: {string.Join(", ", missingItems)}.");

                response.Tags = tags;
                response.Success = true;
                response.Message = "Source ready.";
                return response;
            }
            catch (Exception ex)
            {
                return FailSource(response, ex.Message);
            }
        }

        private static ImportSourceResponse FailSource(ImportSourceResponse response, string message)
        {
            response.Success = false;
            response.Message = message;
            return response;
        }

        // Finds this server's movie or show for a copied Manual List entry. Top-list copies
        // (.strm files in the top-list folders) are skipped, as in BuildImdbLookup.
        private Func<SourceItemRef, BaseItem?> BuildSourceItemFinder()
        {
            var byProvider = new Dictionary<string, BaseItem>(StringComparer.OrdinalIgnoreCase);
            var byNameYear = new Dictionary<string, BaseItem>(StringComparer.OrdinalIgnoreCase);
            string? topListsFolder = null;
            try { topListsFolder = Path.Combine(Plugin.Instance!.DataFolderPath, "toplists") + Path.DirectorySeparatorChar; } catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            foreach (var item in _libraryManager.GetItemList(new InternalItemsQuery
            {
                IncludeItemTypes = new[] { "Movie", "Series" },
                Recursive = true,
                IsVirtualItem = false
            }))
            {
                if (topListsFolder != null && !string.IsNullOrEmpty(item.Path) && item.Path.StartsWith(topListsFolder, StringComparison.OrdinalIgnoreCase)) continue;
                if (RankedCollections.IsRankedPath(item.Path)) continue;
                var type = item.GetType().Name;
                foreach (var p in new[] { "Imdb", "Tmdb", "Tvdb" })
                {
                    var v = item.GetProviderId(p);
                    // TMDb and TVDb numbers are only unique per type.
                    if (!string.IsNullOrEmpty(v)) byProvider.TryAdd(type + ":" + p + ":" + v, item);
                }
                if (!string.IsNullOrEmpty(item.Name)) byNameYear.TryAdd(type + ":" + item.Name + ":" + item.ProductionYear, item);
            }
            return r =>
            {
                foreach (var (p, v) in new[] { ("Imdb", r.Imdb), ("Tmdb", r.Tmdb), ("Tvdb", r.Tvdb) })
                    if (!string.IsNullOrEmpty(v) && byProvider.TryGetValue(r.Type + ":" + p + ":" + v, out var hit)) return hit;
                return !string.IsNullOrEmpty(r.Name) && byNameYear.TryGetValue(r.Type + ":" + r.Name + ":" + r.Year, out var byName) ? byName : null;
            };
        }

        // ── Shared by Copy / Paste of sources and top-lists ──

        private Dictionary<string, string> UserNamesById()
        {
            var names = new Dictionary<string, string>();
            try { foreach (var u in _userManager.GetUserList(new UserQuery())) names[u.Id.ToString("N")] = u.Name ?? ""; } catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            return names;
        }

        private Dictionary<string, string> LocalUserIdsByName()
        {
            var ids = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var u in _userManager.GetUserList(new UserQuery()))
                if (!string.IsNullOrEmpty(u.Name) && !ids.ContainsKey(u.Name)) ids[u.Name] = u.Id.ToString("N");
            return ids;
        }

        private static SourceItemRef ItemRefFor(BaseItem item) => new SourceItemRef
        {
            Name = item.Name ?? "",
            Year = item.ProductionYear,
            Type = item.GetType().Name,
            Imdb = item.GetProviderId("Imdb") ?? "",
            Tmdb = item.GetProviderId("Tmdb") ?? "",
            Tvdb = item.GetProviderId("Tvdb") ?? ""
        };

        private static string ItemRefLabel(SourceItemRef r) =>
            r.Name + (r.Year.HasValue ? " (" + r.Year + ")" : "") + (string.IsNullOrEmpty(r.Imdb) ? "" : " " + r.Imdb);

        // A tag "exists" here when an HSC source makes it or any item carries it.
        private bool TagExistsHere(string tag)
        {
            if (string.IsNullOrWhiteSpace(tag)) return false;
            if ((Plugin.Instance?.Configuration?.Tags ?? new List<TagConfig>()).Any(t => string.Equals((t.Tag ?? "").Trim(), tag.Trim(), StringComparison.OrdinalIgnoreCase)))
                return true;
            try { return _libraryManager.GetItemList(new InternalItemsQuery { Tags = new[] { tag.Trim() }, Recursive = true, Limit = 1 }).Length > 0; }
            catch { return false; }
        }

        // ── Copy / paste one top-list ─────────────────────────────────────────────────────

        private const int TopListFormatVersion = 1;

        public object Post(ExportTopListRequest request)
        {
            var file = new TopListCopyFile
            {
                TopListVersion = TopListFormatVersion,
                PluginVersion = Plugin.Instance?.Version.ToString() ?? "0.0.0",
                CreatedUtc = DateTime.UtcNow.ToString("o")
            };
            var name = (request.TagName ?? "").Trim();
            var tl = (Plugin.Instance?.Configuration?.TopLists ?? new List<TopListHomeSection>())
                .FirstOrDefault(t => t != null && string.Equals((t.TagName ?? "").Trim(), name, StringComparison.OrdinalIgnoreCase));
            if (tl == null)
            {
                file.Notices.Add($"The top-list '{name}' was not found – save it first.");
                return file;
            }

            bool isShows = ShowTopList.IsShows(tl);
            var settings = ParseSectionSettings(tl.HomeSectionSettings);
            string Setting(string key) => settings.TryGetValue(key, out var v) ? v ?? "" : "";

            file.ListName    = tl.TagName ?? "";
            file.ContentType = isShows ? "Shows" : "Movies";
            file.CustomName  = Setting("CustomName");
            file.DisplayMode = Setting("DisplayMode");
            file.ImageType   = Setting("ImageType");
            file.CardSizeOffset = Setting("CardSizeOffset").Length > 0 ? Setting("CardSizeOffset") : "0";
            file.BadgeStyle  = Setting("BadgeStyle");
            file.BadgeOptions = Setting("BadgeOptions");
            file.RealPosterBadge = Setting("RealPosterBadge");
            file.MaxItems    = tl.MaxItems;
            file.UserIds     = (tl.HomeSectionUserIds ?? new List<string>()).Where(u => !string.IsNullOrWhiteSpace(u)).ToList();

            var userNames = UserNamesById();
            foreach (var id in file.UserIds)
                if (!file.Users.ContainsKey(id) && userNames.TryGetValue(NormId(id), out var uname)) file.Users[id] = uname;

            // Same rule as the Top Lists tab: a list is fed by a tag when titles carry its name.
            bool byTag = false;
            try { byTag = _libraryManager.GetItemList(new InternalItemsQuery { Tags = new[] { file.ListName }, Recursive = true, Limit = 1 }).Length > 0; } catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            file.IsManual = !byTag;
            if (byTag) file.SourceTag = file.ListName;
            else
            {
                var folder = Path.Combine(Plugin.Instance!.DataFolderPath, "toplists", SanitizeFolderName(file.ListName));
                foreach (var m in isShows ? ReadTopListShows(folder) : ReadTopListMovies(folder))
                {
                    var item = Guid.TryParse(m.ItemId, out var g) ? _libraryManager.GetItemById(g) : null;
                    if (item != null) file.Items.Add(ItemRefFor(item));
                }
                if (file.Items.Count == 0)
                    file.Notices.Add("This manual list has no titles in its library folder – the copy has an empty list.");
            }
            return file;
        }

        public object Post(ImportTopListRequest request)
        {
            var response = new ImportTopListResponse();
            try
            {
                var json = request.TopListJson ?? "";
                if (string.IsNullOrWhiteSpace(json)) return FailTopList(response, "Nothing was pasted.");
                if (!System.Text.RegularExpressions.Regex.IsMatch(json, "\"hsc-toplist\"\\s*:\\s*\\d"))
                    return FailTopList(response, "This is not a copied Home Screen Companion top-list (the \"hsc-toplist\" header is missing). Use Copy on a top-list and paste that text.");
                TopListCopyFile? file = null;
                try { file = _jsonSerializer.DeserializeFromString<TopListCopyFile>(json); } catch { }
                if (file == null || string.IsNullOrWhiteSpace(file.ListName))
                    return FailTopList(response, "The pasted text could not be read as a top-list.");
                file.Users ??= new Dictionary<string, string>();
                file.Items ??= new List<SourceItemRef>();
                file.UserIds ??= new List<string>();

                var notices = response.Notices;
                void Notice(string msg) { if (!notices.Contains(msg)) notices.Add(msg); }
                var config = Plugin.Instance!.Configuration;
                var topLists = config.TopLists ?? new List<TopListHomeSection>();
                bool isShows = string.Equals(file.ContentType, "Shows", StringComparison.OrdinalIgnoreCase);

                response.ContentType = isShows ? "Shows" : "Movies";
                response.IsManual    = file.IsManual;
                response.SourceTag   = file.IsManual ? "" : (file.SourceTag ?? "").Trim();
                response.CustomName  = file.CustomName ?? "";
                response.DisplayMode = file.DisplayMode ?? "";
                response.ImageType   = file.ImageType ?? "";
                response.CardSizeOffset = string.IsNullOrWhiteSpace(file.CardSizeOffset) ? "0" : file.CardSizeOffset;
                response.BadgeStyle  = file.BadgeStyle ?? "";
                response.BadgeOptions = file.BadgeOptions ?? "";
                response.RealPosterBadge = file.RealPosterBadge ?? "";
                response.MaxItems    = Math.Max(0, file.MaxItems);

                // Users: by name, case-insensitive (as for sources).
                var localUsers = LocalUserIdsByName();
                var dropped = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (var id in file.UserIds.Where(x => !string.IsNullOrWhiteSpace(x)))
                {
                    var local = file.Users.TryGetValue(id, out var uname) && !string.IsNullOrEmpty(uname) && localUsers.TryGetValue(uname, out var l) ? l : null;
                    if (local == null) dropped.Add(file.Users.TryGetValue(id, out var n) && !string.IsNullOrEmpty(n) ? n : "unknown (id " + id + ")");
                    else if (!response.UserIds.Contains(local)) response.UserIds.Add(local);
                }
                if (dropped.Count > 0)
                    Notice($"Target users not on this server, left out: {string.Join(", ", dropped)}." + (response.UserIds.Count == 0 ? " Tick at least one user before you create the list." : ""));

                // Name: a clash gets "(copy)", except for a list fed by a tag – its name is the tag.
                bool NameTaken(string n) => topLists.Any(t => string.Equals(SanitizeFolderName(t.TagName), SanitizeFolderName(n), StringComparison.OrdinalIgnoreCase))
                    || Directory.Exists(Path.Combine(Plugin.Instance.DataFolderPath, "toplists", SanitizeFolderName(n)));
                var listName = file.ListName.Trim();
                if (!file.IsManual)
                {
                    listName = response.SourceTag.Length > 0 ? response.SourceTag : listName;
                    if (NameTaken(listName))
                        Notice($"This server already has a top-list for the tag '{listName}'. A list fed by a tag is named after the tag, so Create updates that list's settings instead of adding a second one.");
                }
                else if (NameTaken(listName))
                {
                    var baseName = listName + " (copy)";
                    listName = baseName;
                    for (int i = 2; NameTaken(listName); i++) listName = baseName + " " + i;
                    if (response.CustomName.Length > 0) response.CustomName += " (copy)";
                }
                response.ListName = listName;

                if (!file.IsManual)
                {
                    response.SourceTagExists = TagExistsHere(response.SourceTag);
                    if (!response.SourceTagExists)
                        Notice($"Create or paste the source '{response.SourceTag}' first: no source and no title on this server has that tag yet, so the list stays empty until one does.");
                }
                else
                {
                    // Ranked titles: by IMDb / TMDb / TVDb id, then name + year (as for Manual List sources).
                    var findItem = file.Items.Count > 0 ? BuildSourceItemFinder() : null;
                    var missing = new List<string>();
                    var seen = new HashSet<string>();
                    foreach (var r in file.Items.Where(x => x != null))
                    {
                        var item = findItem?.Invoke(r);
                        if (item == null || (isShows ? !(item is MediaBrowser.Controller.Entities.TV.Series) : item is MediaBrowser.Controller.Entities.TV.Series))
                        {
                            var label = ItemRefLabel(r);
                            if (!missing.Contains(label)) missing.Add(label);
                            continue;
                        }
                        var id = item.Id.ToString("N");
                        if (!seen.Add(id)) continue;
                        response.Items.Add(new MovieItem { ItemId = id, Name = item.Name ?? "", Year = item.ProductionYear, ImdbId = item.GetProviderId("Imdb") ?? "" });
                    }
                    if (missing.Count > 0)
                        Notice($"{missing.Count} title(s) not in this server's library, left out: {string.Join(", ", missing)}.");
                    if (response.Items.Count == 0)
                        Notice($"None of the list's {(isShows ? "shows" : "movies")} are on this server – add some before you create it.");
                }

                response.Success = true;
                response.Message = "Top-list ready.";
                return response;
            }
            catch (Exception ex)
            {
                return FailTopList(response, ex.Message);
            }
        }

        private static ImportTopListResponse FailTopList(ImportTopListResponse response, string message)
        {
            response.Success = false;
            response.Message = message;
            return response;
        }

    }
}
