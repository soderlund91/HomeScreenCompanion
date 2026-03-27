

<img width="481" height="142" alt="HSC" src="https://github.com/user-attachments/assets/2887420a-7198-472e-8afb-a49eda3231d9" />


# Home Screen Companion

**HomeScreenCompanion** is a plugin for Emby Server that automatically manages tags and collections for your Movies and TV Shows — and keeps your home screen sections in sync across multiple users.

It works by connecting one or more **sources** to a tag and/or collection in your Emby library. Sources can be external lists from Trakt or MDBList, rules-based filters on your own library, AI-generated recommendations, or your existing local collections and playlists. Each source runs on a schedule, and the plugin makes sure your library always reflects the current state — adding and removing tags and collection memberships automatically.

> [!IMPORTANT]
> Plugin version 3.0.0.0 no longer supports Emby server version 4.9.x.x. Requires Emby 4.10+
> 
> Plugin version 3.2.0.0 requires Emby 4.10.0.6+

---

## Key Features

- **Five source types** — External List, Smart Playlist, AI List, Local Collection, Local Playlist
- **Automatic tagging and collections** — items are added and removed as lists change
- **Advanced scheduling** — annual, weekly, or specific date windows
- **Per-entry home screen sections** — automatically managed sections for any tag or collection
- **Home screen sync** — mirror one user's home screen layout to any number of other users

---

## Source Types

### External List

Connect to curated lists on **Trakt** or **MDBList**. The plugin fetches the list on a schedule, matches items to your library by IMDB ID, and keeps tags and collections in sync automatically.

- Supports any Trakt or MDBList URL — trending, popular, user lists, curated sets
- Multiple URLs can be combined into a single tag/collection entry
- Items removed from the remote list are automatically untagged

**Supported services:**
- **Trakt.tv** — Trending, Popular, Watched, User Lists
- **MDBList.com** — Dynamic lists with custom criteria

**Example URLs:**
```
https://trakt.tv/movies/trending
https://trakt.tv/movies/popular
https://trakt.tv/users/username/lists/my-list
https://mdblist.com/lists/user/listname/
```

**API keys required:** Trakt Client ID and/or MDBList API Key (configured in the Settings tab).

---

### Smart Playlist

Build dynamic lists directly from your library using a flexible rule builder. No external service needed — the plugin scans your library and tags items that match your criteria.

Rules can be combined with **AND/OR** logic, grouped into multiple condition groups, and negated with `!`.

**Available filter criteria:**

| Category | Properties |
| :--- | :--- |
| **Video** | Resolution (8K, 4K, 1080p, 720p, SD), Codec (HEVC, AV1, H.264), HDR (Any, Dolby Vision, HDR10) |
| **Audio** | Format (Atmos, TrueHD, DTS-HD MA, DTS, AC3, AAC), Channels (7.1+, 5.1, Stereo, Mono), Language |
| **Content** | Genre, Studio, Actor, Director, Writer, Content Rating, Title, Overview, Tag, IMDB ID |
| **Metrics** | Year, Runtime, Community Rating, File Size, Date Added, Date Modified |
| **Watch status** | Watched/Unwatched, Last Played, Play Count — per user, any user, or all users |

**Example use cases:**
- Tag all 4K Dolby Vision movies with Atmos audio → `4K AND Dolby Vision AND Atmos`
- "Recently added and never watched" → `Date Added <= 30 AND Unwatched`
- Action movies from 2010+ rated above 7 → `Genre: Action AND Year >= 2010 AND Rating >= 7`
- All HEVC content under 90 minutes → `HEVC AND Runtime < 90`

Premade filter templates are available for common use cases (4K, recent additions, unwatched, etc.), and you can save your own filter sets for reuse.

---

### AI List

Generate recommendations using AI. Write a natural language prompt and the plugin calls an AI model to produce a list of movies and shows, then matches them against your library.

**Supported AI providers:**
- **OpenAI** (GPT-4o-mini) — requires an OpenAI API key
- **Google Gemini** (Gemini Flash) — requires a Google Gemini API key

**Personalization:** Optionally include a user's recently watched history as context. The AI uses this to tailor recommendations to that user's taste.

**Configuration:**
| Setting | Description |
| :--- | :--- |
| **AI Provider** | OpenAI or Google Gemini |
| **API Key** | Your key for the selected provider |
| **Prompt** | Natural language description of what you want (e.g., *"Best psychological thrillers from the 90s"*) |
| **Include recently watched** | Adds watch history to the prompt for personalized results |
| **Source user** | Which user's watch history to use |
| **History count** | How many recent items to include (5–100, default 20) |

A **Test** button lets you preview the AI output before saving.

---

### Local Collection

Tag items based on an existing Emby Collection. Combine with scheduling to create time-limited promotions of curated content — no external API needed.

---

### Local Playlist

Tag items based on an existing Emby Playlist. Works the same as Local Collection but uses playlists as the source.

---

## Collections

Any source type can automatically maintain an Emby Collection alongside its tag.

| Setting | Description |
| :--- | :--- |
| **Create Collection** | Automatically create and manage an Emby Collection for this entry |
| **Collection Name** | Optional custom name (defaults to the tag name) |

When a source is disabled or its schedule ends, the plugin automatically removes the collection.

---

## Scheduling

Set any entry to be active only during specific time windows.

| Rule | Description |
| :--- | :--- |
| **Annual** | Active between a recurring start and end date each year (e.g., Dec 1–31 for "Christmas Movies") |
| **Weekly** | Active on selected days of the week (e.g., only on Fridays) |
| **Specific Dates** | Active between a fixed start and end date — for one-time events |

When a schedule window closes, tags and collections are automatically cleaned up.

---

## Home Screen

Two complementary features give you full control over home screen sections.

### Per-entry Home Screen Sections

Add a dedicated home screen section to any tag/collection entry. The plugin creates, updates, and removes the section automatically on each sync — no manual Emby configuration needed.

| Setting | Description |
| :--- | :--- |
| **Section Type** | *Single Collection* — shows the managed collection as a boxset row. *Dynamic Media* — shows items filtered by the entry's tag |
| **Item Types** | Movie, Series, Episode, MusicVideo (Dynamic Media only) |
| **Custom Title** | Override the section name on the home screen |
| **Image Type** | Default, Primary, Backdrop, or Thumb |
| **Sort By** | Default, Rating, Date Added, Name, Runtime, Release Date, Year, or Random |
| **Sort Order** | Ascending, Descending, or Default |
| **Scroll Direction** | Horizontal, Vertical, or Default |
| **Target Users** | Which users get this section |

### Home Screen Sync (Manage tab)

Mirror one user's full home screen layout to any number of other users — automatically, or on a schedule

| Setting | Description |
| :--- | :--- |
| **Enable sync** | Turn the copy feature on or off |
| **Source user** | The user whose layout is used as master |
| **Target users** | Users whose home screens will be overwritten to match the source |

The plugin page shows last sync time, result, and number of sections copied.

---

## Safety & Stability

- **Fail-Safe Cleanup** — if a remote list fails to download, the plugin skips cleanup for that tag/collection to prevent accidental data loss
- **Dry Run Mode** — test your configuration in the logs without modifying anything in your library
- **Live Logging** — view the execution log directly inside the plugin settings with real-time status updates

---

## Installation

1. Download the latest `.dll` from the [Releases](../../releases) page.
2. Shut down your Emby Server.
3. Place the `.dll` file in your Emby plugins folder.
4. Start Emby Server.

---

## Configuration

Go to your Emby Dashboard. You will find **Home Screen Companion** in the sidebar menu.

The plugin page has three tabs: **Tag and Collection**, **Home Screen**, and **Settings**.

### API Keys (Settings tab)

| Key | Required for |
| :--- | :--- |
| **Trakt Client ID** | External List sources using Trakt URLs |
| **MDBList API Key** | External List sources using MDBList URLs |
| **OpenAI API Key** | AI List sources using OpenAI |
| **Google Gemini API Key** | AI List sources using Google Gemini |

---

### Screenshots

<img width="768" height="673" alt="main" src="https://github.com/user-attachments/assets/efc94da5-030a-4380-ab07-5ec9e5ff7151" />


<img width="740" height="936" alt="lmi" src="https://github.com/user-attachments/assets/f2e0f1f4-fdc0-4eea-b9ef-a1b2acd14611" />


<img width="743" height="711" alt="schedule" src="https://github.com/user-attachments/assets/f9efe07b-5974-4485-bbd7-381e6bd071dd" />


<img width="728" height="1272" alt="home screen section" src="https://github.com/user-attachments/assets/8dc7b72b-4272-4527-b505-44fa155d4692" />


<img width="737" height="799" alt="sync" src="https://github.com/user-attachments/assets/29103ea5-fce7-47be-9f75-99ee38b58975" />



---

_**Disclaimer:** This plugin is not affiliated with Emby, Trakt, MDBList, OpenAI, or Google.
This plugin is heavily vibe-coded, tested on my own server — use at your own risk._
