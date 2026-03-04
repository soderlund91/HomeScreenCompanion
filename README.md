

<img width="481" height="142" alt="HSC" src="https://github.com/user-attachments/assets/2887420a-7198-472e-8afb-a49eda3231d9" />


# HomeScreenCompanion — (Former AutoTag)

**HomeScreenCompanion** (formerly AutoTag) is a powerful plugin for Emby Server that automatically manages tags and collections for your Movies and TV Shows—and keeps your home screen sections in sync across multiple users.

> **v3.0.0.0** — Plugin renamed from AutoTag to Home Screen Companion.

---

## 🚀 Key Features

### 🌟 Dynamic Lists from Trakt & MDBList
Make your library feel alive with tags that update automatically every day.
* **Trending Now** — always shows what's currently hot worldwide.
* **Popular & Top Rated** — self-managing "Best of" lists.
* **Personal Curations** — sync your own Trakt/MDBList lists (e.g., "Best of 80s", "Dad's Favorites").
* **Automagical Sync** — link a URL, and the plugin handles the rest. Items are added and removed automatically.

### 📅 Advanced Scheduling
Set tags and collections to be active only during specific time windows. AutoTag handles cleanup when the window closes.
* **Annual** — e.g., "Christmas Movies" active Dec 1–31 every year.
* **Weekly** — e.g., "Friday Movie Night" active only on Fridays.
* **Specific Dates** — for one-time events or marathons.

### 📚 Automated Collection Management
Beyond tagging, the plugin can manage your Emby Collections.
* **Auto-Create** — create a corresponding Emby Collection while a list is active.
* **Dynamic Membership** — items join/leave the collection as the list changes.
* **Only Collection Mode** — enable "Disable Item Tagging" to keep item metadata clean while still enjoying organized collections.
* **Smart Removal** — when a collection is disabled or its schedule ends, the plugin automatically deletes it from Emby.

### 🧩 Local Sources: Collections & Playlists
No external API needed. Use what's already in your library.
* Tag items based on existing **Emby Collections** or **Playlists**.
* Combine with schedules to create time-limited promotions of curated content.

### 🔬 MediaInfo Filtering
Tag items based on their actual technical properties—no manual curation required.
* **Rule-based UI** — build filters from a property dropdown with context-sensitive value inputs.
* **Property categories** — Video (4K, HDR, HEVC, H.264, SD), Audio (Atmos, DTS-HD MA, AC3, AAC, Stereo, Mono, channels), Content (Genre, Studio, Actor/Director, Rating, Language), Metrics (Year, Runtime, Community Rating, Critic Rating).
* **Numeric operators** — `>`, `>=`, `<`, `<=`, `=` for ratings, year, runtime, and channel count.
* **Text matching** — case-insensitive contains for Genre, Studio, Actor, Director, and Language.
* **AND/OR within groups** — combine multiple criteria per group with AND or OR logic.
* **AND/OR between groups** — chain multiple criterion groups together.
* Backward compatible with previously saved configs.

**Example use cases:**
* Tag all 4K Atmos movies → `4K AND Atmos`
* Tag action movies from 2020+ with a rating above 7 → `Genre: Action AND Year >= 2020 AND CommunityRating >= 7`
* Tag content suitable for a specific setup → `DTS-HD MA OR Atmos`

### 🏠 Home Screen Companion
Keep home screen sections in sync across all your Emby users—automatically.
* **Source user** — pick one user whose home screen layout is the "master" configuration.
* **Target users** — select one or more users whose home screens will be updated to mirror the source.
* **Scheduled task** — runs every 30 minutes by default, or manually trigger a sync from the plugin page.
* **Live status** — the plugin page shows last sync time, result, and how many sections were copied.
* **API key support** — configure a dedicated Emby API key for reliable access.

This is ideal for multi-user households or Emby servers with guests, where you want everyone to see the same curated home screen without manually configuring each account.

### 🔄 True Synchronization
AutoTag doesn't just add tags; it **enforces** them.
* Item enters your list → **Tag/Collection Added**.
* Item leaves your list → **Tag/Collection Removed**.
* Your library always reflects the current state of the list.

### 🛡️ Safety & Stability
* **Safety Lock** — prevents saving or starting a new run while a sync is in progress.
* **Fail-Safe Cleanup** — if a remote list fails to download, the plugin skips cleanup for that tag/collection to prevent accidental data loss.
* **Dry Run Mode** — test your configuration in the logs without modifying anything in your library.
* **Live Logging** — view the execution log directly inside the plugin settings with real-time status updates.

### 🧠 Self-Cleaning Memory
Deleted a configuration? On the next run, the plugin automatically removes that tag and/or collection from your entire library. No manual cleanup required.

---

## 📦 Installation

1. Download the latest `.dll` from the [Releases](../../releases) page.
2. Shut down your Emby Server.
3. Place the `.dll` file in your Emby plugins folder.
4. Start Emby Server.

---

## ⚙️ Configuration

Go to your Emby Dashboard. You will see **HomeScreenCompanion** in the sidebar menu.

The plugin page has two tabs: **Tag and Collection** and **Home Screen Companion**.

### API Keys (Tag and Collection tab)
* **Trakt Client ID** — required for Trakt lists. Get one free at [trakt.tv/oauth/applications](https://trakt.tv/oauth/applications).
* **MDBList API Key** — required for MDBList. Get one free at [mdblist.com/developer](https://mdblist.com/developer/).

### Managing Sources

Each source is configured across four tabs:

#### 🏠 Source Tab
| Setting | Description |
| :--- | :--- |
| **Active** | Toggle to enable/disable this source. |
| **Tag Name** | The tag applied to matching media (e.g., `weekly_trending`). |
| **Limit** | Max number of items to tag/collect from this list. |
| **Source URL / Selection** | URL (Trakt/MDBList), or local Collection/Playlist name. |

#### 📅 Schedule Tab
| Setting | Description |
| :--- | :--- |
| **Schedule Rule** | Define when this source should be active. |
| *Specific Date* | Active only between a Start and End date. |
| *Recurring* | Repeats every year (e.g., Dec 1–31). |
| *Week Days* | Active only on selected days of the week. |

#### 📚 Collection Tab
| Setting | Description |
| :--- | :--- |
| **Create Collection** | Automatically maintain an Emby Collection for this list. |
| **Collection Name** | Optional custom name (defaults to Tag Name). |
| **Disable Tagging** | Add items to the collection without tagging them individually. |

#### 🔧 Advanced Tab
| Setting | Description |
| :--- | :--- |
| **Blacklist** | Comma-separated IMDB IDs to ignore (e.g., `tt1234567`). |
| **MediaInfo Filter** | Rule-based filter to restrict which items in the list get tagged. |

### Home Screen Companion Tab
| Setting | Description |
| :--- | :--- |
| **Enable HSC Sync** | Toggle the home section sync feature on or off. |
| **Source User** | The user whose home screen layout will be copied. |
| **Target Users** | Users who will receive the source user's home screen layout. |
| **Server URL** | The internal Emby server URL (e.g., `http://localhost:8096`). |
| **API Key** | An Emby API key with sufficient permissions. |

---

### 🔗 Supported URL Examples

**Trakt:**
* Trending Movies: `https://trakt.tv/movies/trending`
* Popular Movies: `https://trakt.tv/movies/popular`
* Watched (Weekly): `https://trakt.tv/movies/watched/weekly`
* User List: `https://trakt.tv/users/username/lists/my-awesome-list`

**MDBList:**
* Dynamic List: `https://mdblist.com/lists/user/listname/`

---



### Screenshots

(Coming)

---
**Disclaimer:** This plugin is not affiliated with Emby, Trakt, or MDBList.
This plugin is heavily vibe-coded, tested on my own server — use at your own risk.
