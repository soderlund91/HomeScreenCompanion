# 🎬 Home Screen Companion for Emby

**Your Emby home screen, on autopilot.**

https://github.com/user-attachments/assets/74f6d998-6b59-42b6-b335-261ca6cc6645

**Home Screen Companion** turns lists of movies and shows into home screen rows for your Emby users, and keeps them up to date by itself. Trending on Trakt, your own 4K HDR picks, an AI-written "90s sci-fi" list, or a Netflix-style Top 10 with numbered posters: you set it up once, and every sync refreshes it.

- **Rows that fill themselves.** Pull lists from Trakt, MDBList or TMDb, build them from your own files, pick titles by hand, or let an AI write them.
- **Every user gets something personal.** *Your Next Watch* builds each user a private playlist of unwatched titles like the ones they love, worked out on your server, with no AI and no internet.
- **The right rows at the right time.** Christmas movies in December, horror around Halloween, a weekend binge row on Fridays. The rows appear and disappear on their own.
- **Top 10 like the streaming services.** Ranked rows with big numbered tiles, and optional TOP 10 badges on the real posters.
- **One layout for everyone.** Design the home screen once, then copy it to every user.

---

## 📑 Contents

- [Installation](#-installation)
- [Features at a glance](#-features-at-a-glance)
- [Features in depth](#-features-in-depth)
  - [Sources](#sources-where-your-lists-come-from)
  - [Your Next Watch](#your-next-watch-personal-recommendations)
  - [What each source produces](#what-each-source-produces)
  - [Home screen rows](#home-screen-rows)
  - [Scheduling](#scheduling)
  - [Top-lists](#top-lists)
  - [Generated artwork](#generated-artwork)
  - [Home Screen Sync](#home-screen-sync)
  - [Continue Watching bump](#continue-watching-bump)
  - [Safety, logs and tools](#safety-logs-and-tools)
  - [Scheduled tasks](#scheduled-tasks)
- [Settings reference](#-settings-reference)
- [Screenshots](#-screenshots)
- [Help and troubleshooting](#-help-and-troubleshooting)
- [License and disclaimer](#-license-and-disclaimer)

---

## 🚀 Installation

### From the Emby plugin catalog (recommended)

1. In Emby, open **Settings → Plugins → Catalog**.
2. Find **Home Screen Companion** and click **Install**.
3. Restart Emby Server when asked.
4. Open **Home Screen Companion** from the sidebar under Plugins.

### Manual install

> [!IMPORTANT]
> Check each release for the Emby Server version it needs.

1. Download the latest `.dll` from the [Releases](../../releases) page.
2. Stop Emby Server.
3. Copy the `.dll` into Emby's `plugins` folder.
4. Start Emby Server and open **Home Screen Companion** from the sidebar.

### Your first row in three steps

1. **Add a source.** On the **Tag & Collection** tab, click **+ Add New Source** and pick a source type. No ideas yet? Quick Start has ready-made **starter sources**, such as "Popular on this server" and "Recently added movies", that need no API keys.
2. **Put it on the home screen.** In the source's **Home Screen** tab, tick **Add as home screen section** and choose the users who should see it.
3. **Save and run.** Click **Save Settings**, then **Run → Full sync**. After that the plugin syncs every night at 04:00 by itself.

> [!TIP]
> Don't see the row? Refresh the Emby app, or sign out and back in. Apps keep the old home screen until then.

### API keys (only for the services you use)

Enter keys in the **Settings** tab. Smart Playlists, Manual Lists, Local sources, Your Next Watch and public MDBList lists need no keys.

| Service | Needed for | Where to get it |
| --- | --- | --- |
| Trakt | Every Trakt list | Create an app at [trakt.tv/oauth/applications](https://trakt.tv/oauth/applications) with Redirect URI `http://localhost`, then copy the **Client ID** |
| MDBList | Private lists only | **API Access** at [mdblist.com/preferences](https://mdblist.com/preferences) |
| TMDb | Every TMDb list | The long **API Read Access Token** (not the short API Key) at [themoviedb.org/settings/api](https://www.themoviedb.org/settings/api) |
| Anthropic Claude | AI lists | [console.anthropic.com](https://console.anthropic.com) |
| Google Gemini | AI lists | Google AI Studio |
| OpenAI | AI lists | [platform.openai.com/api-keys](https://platform.openai.com/api-keys) |
| Ollama | AI lists, run on your own machine | No key needed, just the URL, e.g. `http://192.168.1.100:11434` |

---

## ✨ Features at a glance

Everything starts with a **source**, a list of movies or shows. The plugin finds those titles in your library and turns them into a **tag**, an **Emby collection** and/or a **playlist per user**. Any of them can become a **home screen row**, shown always or only on the dates you choose.

| Feature | What it gives you |
| --- | --- |
| **External lists** | Trakt, MDBList and TMDb lists, matched to your library and kept in sync |
| **Smart Playlists** | Lists built from your own files: 4K HDR, Atmos, genre, actor, watch status, "popular on this server" and more |
| **Manual lists** | Your own hand-picked titles, in your order |
| **AI lists** | Describe a list in plain words; Claude, Gemini, ChatGPT or a local Ollama model writes it |
| **Local sources** | Reuse an existing Emby collection or playlist as a source |
| **Your Next Watch** | A private, daily playlist per user with unwatched titles like what they watch, worked out on your server |
| **Home screen rows** | Rows created, updated and cleaned up for the users you pick, with full control over look and sorting |
| **Scheduling** | Seasonal and weekly rows: dates, weekdays, Easter, Advent, time of day, holiday presets |
| **Top-lists** | Netflix-style ranked rows with numbered tiles, and optional TOP 10 badges on the real posters |
| **Generated artwork** | Posters and backgrounds for collections, tags and playlists, drawn from the titles in them |
| **Home Screen Sync** | Copy one user's whole home screen layout to everyone else |
| **Continue Watching bump** | A show jumps to the front of Continue Watching when a new episode or season arrives |
| **Safety and tools** | Dry run, protection against failed downloads, live logs, downloadable log bundle, cleanup, backup and restore, copy to another server |

---

## 🔍 Features in depth

### Sources: where your lists come from

Each source becomes one list. Several sources can feed the same tag, and every source can be switched off, duplicated, previewed or run on its own.

#### 🌐 External lists (Trakt, MDBList, TMDb)

Paste a list URL and the plugin downloads it, matches the titles to your library by IMDb id, and keeps the result in sync.

- **Trakt:** public user lists and Trakt's own lists, such as `trakt.tv/movies/trending` or `/shows/popular`.
- **MDBList:** public and official lists without a key; private lists with your API key.
- **TMDb:** your own lists, or direct API endpoints such as popular, top rated, now playing or a collection.
- **Several URLs in one source** are combined under one tag, collection and playlist. Each URL has its own **Max**, which counts titles you actually have: Max 10 on a top-100 list gives the 10 highest-ranked titles in your library.
- **Test** checks that a URL can be read. **Preview** shows which titles you have and which are missing.

#### 🎛️ Smart Playlists (MediaInfo)

Build a list from the properties of your own files, like a saved search that updates itself. Titles that start or stop matching are added or removed on the next sync.

- **Rules** check one property each and combine with AND, OR and NOT, in as many groups as you need.
- **Video:** resolution (SD to 8K), codec (H.264, HEVC, AV1), HDR10, Dolby Vision, HLG.
- **Audio:** Atmos, TrueHD, DTS-HD MA, DTS, AC3, AAC, channels, audio language.
- **Content:** genre, actor, director, writer, studio, title, plot keywords, content rating, existing tags, collections, playlists, IMDb id.
- **Metrics:** community rating, year, runtime, date added, file size.
- **Activity:** watched or unwatched (for anyone, everyone or one user), last played, play count, watched-by count.
- **Popular on this server:** the titles most users watched in the last 7 days up to all time, listed most viewed first.
- **Music:** artist, album, bit rate, sample rate and more for songs and music videos.
- **Premade filters** and **saved filters** give you a head start and reuse your own rule sets.

Examples: *"All unwatched 4K Dolby Vision action movies"*, *"Series rated 7.5+ added this year that someone started but nobody finished"*.

#### ✍️ Manual lists

Search your library, click titles to add them, and order them with the arrow buttons. The order is kept in playlists and top-lists.

#### 🤖 AI lists

Write what you want, such as *"Feel-good comedies from the 80s"*, and the AI answers with a list that the plugin matches to your library.

- **Providers:** Anthropic Claude, Google Gemini, OpenAI (ChatGPT), or Ollama on your own machine (experimental).
- **Personal:** optionally send a user's recently watched titles for "recommended for you" lists.
- **Refresh every N days** keeps the last answer between refreshes, which saves paid calls and keeps the list steady.
- The AI is asked for up to twice as many titles as you need, since some won't be in your library.

#### 📚 Local collections and playlists

Use a collection or playlist that already exists in Emby as the source. This is handy for giving a hand-made collection a home row, generated art or a schedule. You can also show **collections themselves** as a row, instead of the movies inside them.

#### 🧹 Filters on any source

Every source type can use the Smart Playlist rules as extra filters. Examples are a Trakt trending list with only unwatched titles, or an AI list limited to 4K.

### Your Next Watch: personal recommendations

Every user you pick gets a private playlist of the unwatched movies and shows most like what they have been watching. It is refreshed on every sync and worked out entirely on your server, with no AI and no internet.

- **Learns from** what each user finished in the last 90 days (or their last 50 titles), plus their favourites and high ratings. Recent titles weigh more, and low-rated ones are left out.
- **Scores** titles on shared genres, actors, directors, writers, studios and keywords, a similar year and a good rating, plus a "people like you" bonus from users with similar taste.
- **Prefers strong matches:** several kinds of match beat a single shared actor, and the next film of a franchise the user is watching always ranks high.
- **Stays varied:** a few titles per franchise and genre at most, and a movie and show mix that follows the user's habits.
- **Respects access:** only titles the user can open, within their parental rating.
- **New users** with little history get a personal mix of the server's most-watched titles, changing daily.
- Shown on the home screen, every user sees **their own** playlist in the same row. **Preview** shows any user's list with the reason for each pick.

### What each source produces

Every source is set up in tabs, so you only turn on what you need.

| Tab | What it does |
| --- | --- |
| **Tag** | An invisible label on every matched item. Old tags are cleaned up when you rename or delete a source. For shows, choose series, season or episode level. |
| **Collection** | An Emby collection with your own name, description, sort title and artwork. One version per film. |
| **Playlist** | A playlist per chosen user, in the source's order, with an optional shared poster, background and description. |
| **Schedule** | When the source is active. |
| **Blacklist** | IMDb ids that this source never includes. |
| **Home Screen** | The source as a row for the users you tick. |

**Keep the list order (experimental):** collections normally use Emby's own sort order. With this on, a movie collection opens in the order of the list.

### Home screen rows

Turn any source into a home screen row for the users you choose. The plugin creates the row, keeps it up to date and removes it when it is no longer needed.

- **Row types:** a collection, a tag row (dynamic media), or each user's own playlist.
- **Look:** custom title, position (top, bottom or after a chosen row), cards or Emby's large Spotlight banner, poster or landscape images, card size, horizontal or grid layout.
- **Sorting:** list order, rating, date added, release date, name, runtime or random (a new mix on every load).
- **Who sees it:** tick users one by one, or use **Unrestricted** and **Restricted** to give adults and kids different rows. A coloured chip shows each user's parental rating limit.
- **TV or phone:** show a row only in TV apps, only on phones and browsers, or always.

### Scheduling

Limit when a source is active. Outside its schedule, its tag, collection, playlists and rows are removed, and they come back when the schedule starts again. The switch happens within 15 minutes.

- **Specific dates** for one-time events.
- **Recurring dates every year**, such as 1 to 26 December, even across New Year.
- **Weekdays**, such as Friday and Saturday.
- **Yearly weekday**, such as the last Friday of November plus 3 days.
- **Moving holidays:** Easter, Advent and every Friday the 13th, with days before and after.
- **Time of day**, such as 22:00 to 02:00 on Fridays.
- **Holiday presets:** Christmas, New Year, Valentine's Day, Star Wars Day, Halloween, Easter, Advent, Friday the 13th.
- **Priority Override:** a seasonal source takes over a shared tag while it is active. For example, "Spotlight Halloween" replaces the regular "Spotlight" row during October.

### Top-lists

A top-list is a ranked row, like "Top 10 Movies This Week", where each poster shows its position. Build one from any tag, or pick and order the titles by hand. It works for movies and TV shows alike.

- **Badge styles:** a number badge in six colours, no number, or the **Top 10 tile**, a big outlined number beside the poster like on Netflix.
- **Customise the tile:** font, colours, position, medal colours for 1 to 3, a special look for number 1, and extras such as rank movement, weeks in the list, play count and a TOP 10 logo.
- **Badges on the real posters:** optionally put a red TOP 10 badge on each title's own poster, everywhere in Emby. It disappears when the title leaves the list.
- **No duplicates:** movies in a top-list are hidden from your other tag rows.
- **Show lists** share watched status and resume points with the real show, and new episodes are added on every sync.

Each top-list has its own hidden Emby library made of shortcuts. No files are copied or moved.

### Generated artwork

Collections, tag pages and playlists can get a poster and background drawn from their own titles, and redrawn when the titles change.

- **Styles:** Collage, Grid, Fan, Wall, Hero strip, Spotlight split and Ranked (Top 10). You can also upload your own image.
- **Customise:** tilt, rows, number of posters, darkening, and a title in your choice of font, position, size and colour. Themed fonts cover horror, Christmas, sci-fi, western, kids and romance.
- **Dynamic titles:** `{name}` and `{week}` give titles like "Top Movies for the Week 41".
- **Preview art** shows the result with your real titles before you save.

### Home Screen Sync

Set up one user's home screen in Emby, then copy the whole layout to any or all other users with one click, or automatically on every sync. You can also copy the library order in the sidebar. Per-user rows such as Your Next Watch still point to each user's own playlist.

> [!WARNING]
> Target users' own home screen changes are replaced on the next sync.

### Continue Watching bump

You finish a season finale, and the next season arrives a year later. By then the show is buried deep in Continue Watching. With the bump on, the show jumps to the front of Continue Watching and Next Up as soon as a new episode or season is added.

- Choose whether it applies to new seasons only, or to every new episode.
- It happens once per new arrival. If the user doesn't watch, the show moves down again as usual.
- Watched status is never changed.
- A scheduled task catches anything missed, every 6 hours by default.

### Safety, logs and tools

- **Protection against failed downloads:** if a list can't be downloaded, its tags and collections are kept instead of being wiped.
- **Dry run:** a full sync that changes nothing and logs what it would do.
- **Live logs:** follow every run from the settings page, with a clear result per source.
- **Download logs:** one zip with the plugin's 7-day log, recent runs, settings and a server snapshot. API keys are always hidden, and user names can be anonymised.
- **Log missing titles:** see which titles from your External and AI lists you don't have yet.
- **Cleanup tab:** see every tag and collection in Emby, and remove the ones you no longer need.
- **Backup and restore:** export settings, sources, filters, top-lists and Home Screen Sync to a file, and restore any part of it.
- **Copy and paste:** move a single source or top-list to another Emby server.
- **Import collection:** let the plugin take over collections made by hand or by another plugin, without breaking links or rows.
- **Unsaved changes are kept** if you leave the page, and two admins can't overwrite each other's changes.

### Scheduled tasks

| Task | When it runs |
| --- | --- |
| Tag & Collection Sync (full sync) | Every day at 04:00, adjustable in Emby's Scheduled Tasks |
| Schedule Switch | Every 15 minutes, starts a sync when a schedule starts or ends |
| Continue Watching Bump | Every 6 hours, and when new episodes are added |
| Home Screen Sync, Top-list section sync | From the **Run** button, or by hand |

---

## 🔧 Settings reference

Every global setting, where to find it and what it does. Per-source options (Tag, Collection, Playlist, Schedule, Blacklist, Home Screen) are described under [What each source produces](#what-each-source-produces). Click **Save Settings** after any change.

### Settings tab → External list settings

| Setting | What it does |
| --- | --- |
| **Trakt Client ID** | Needed for every Trakt list. Create an app at [trakt.tv/oauth/applications](https://trakt.tv/oauth/applications) with Redirect URI `http://localhost` and copy the Client ID. |
| **MDBList API Key** | Needed for private MDBList lists only. Public and official lists work without it. |
| **TMDb API Read Access Token** | Needed for every TMDb list, your own lists and API endpoints alike. Use the long Read Access Token, not the short API Key. |

### Settings tab → AI Settings

Fill in only the AI you use. A cloud AI is paid per call, and a cheap, fast model is usually enough for lists.

| Setting | What it does |
| --- | --- |
| **Google Gemini: API key and Model** | Key from Google AI Studio. Model e.g. `gemini-2.5-flash-lite`. |
| **OpenAI (ChatGPT): API key and Model** | Key from [platform.openai.com](https://platform.openai.com/api-keys). Model e.g. `gpt-4o-mini`. |
| **Anthropic Claude: API key and Model** | Key from [console.anthropic.com](https://console.anthropic.com). Model e.g. `claude-haiku-4-5-20251001`. |
| **Ollama (Local): URL and Model** | Free, runs on your own computer. The URL as seen from the Emby server, e.g. `http://192.168.1.100:11434`, and a model you have pulled, e.g. `llama3.2`. Experimental: local models know less about new titles. |
| **Edit system prompt** | The hidden instruction sent before every AI request, telling the AI how to format its answer. Only change it if you know what you are doing; **Reset to default** puts it back. |

All key fields are hidden by default. Tick **Show** next to a field to see its value.

### Settings tab → Advanced Settings

| Setting | Default | What it does |
| --- | --- | --- |
| **Extended log** | Off | Shows source details, per-item changes, timings and errors in the log window. The downloadable log always has them. |
| **Dry Run Mode** | Off | Runs a full sync that changes nothing and logs what it would do. Remember to switch it off again. |
| **Preserve tags and collections on empty result** | Off | If an External or AI list comes back empty, for example because a site was down, keeps the old tags and collection instead of removing them. |
| **Ignore titles only marked as watched** | Off | Last Played and Popular rules, and the AI's "recently watched" titles, count only real plays, not titles marked watched by hand. |
| **Log missing movies and shows** | Off | Lists per source the titles from your External and AI lists that are not in your library, with their IMDb id. |
| **Show collections on top-list items** | Off | Adds top-list movies to the same collections as the originals, so "Included in &lt;collection&gt;" also shows when a movie is opened from a top-list. |
| **Copy & paste sources** | Off | Shows **Copy** on sources and top-lists, and the **Paste source** and **Paste top-list** buttons, to move them to another server. |
| **Import collection** | Off | Shows the **Import collection** button, to let the plugin take over collections that already exist in Emby. |
| **Ranked collections, movies only (experimental)** | Off | Shows **Keep the list order** on every source's Collection tab, so a collection can open in list order. Adds a hidden library per ranked collection. |
| **Hide HSC libraries** | On | Keeps the libraries the plugin creates (top-lists, ranked collections) out of the sidebar, My Media and Latest. Their rows and collections still show. |

### Settings tab → Backup & Restore

| Setting | What it does |
| --- | --- |
| **Download Backup** | Saves the parts you pick to a JSON file: general settings, API keys, sources, saved filters, top-lists and Home Screen Sync. The file holds your API keys, so keep it private. |
| **Restore Backup** | Restores the parts you pick from a backup. Each part replaces the current one. Tags, collections, playlists and artwork are not in the backup; the next sync rebuilds them. |

### Home Screen tab → Copy & Sync

| Setting | What it does |
| --- | --- |
| **Enable Home Screen sync** | Turns Home Screen Sync on. When off, it never runs, not even from **Run**. |
| **Source user** | The user whose home screen is copied. Set up their home screen in Emby first. |
| **Sync to** | The users who get the copy. Their own rows are replaced on every sync. |
| **Also copy library order** | Also copies the order of the libraries in the sidebar. |

### Home Screen tab → Home Screen Tweaks

| Setting | What it does |
| --- | --- |
| **Bump series with new episodes to the front of Continue Watching** | Turns the Continue Watching bump on. |
| **A new season arrives / Any new episode arrives** | Whether the bump happens only for new seasons, or for every new episode. |
| **All users / chosen users** | Who gets the bump. **All users** also covers users added later. |

The bump only applies to series a user watched up to the end, with no skipped episodes. A series hidden from Continue Watching stays hidden.

### Run and Logs

The **Run** button sits at the bottom right of the page, next to **Save Settings**. **Logs** is at the top.

| Control | What it does |
| --- | --- |
| **Run → Full sync** | Syncs all sources and Home Screen Sync. |
| **Run → Sync tags and collections** | Syncs every active source: tags, collections, playlists, home rows and top-lists. |
| **Run → Sync home screen** | Runs Home Screen Sync only. |
| **Run → Other** | **Bump Continue Watching**, **Sync top-list rows** and **Merge top-list versions**, which also run by themselves. |
| **Logs** | The latest Sync, Home Screen and Top-list runs, with ✔ / ⚠ / ✖ per step. **Warnings & errors only** filters the view. |
| **Download logs** | One zip for troubleshooting. Tick **Anonymise user names** to replace names with User 1, User 2 and so on. |

---

## 📸 Screenshots

<details>
<summary><b>Click to view screenshots</b></summary>

### Sources

**All your sources at a glance**
<img width="1440" alt="All your sources at a glance" src="https://github.com/user-attachments/assets/44592513-e200-4234-85c2-e010825f3175" />

**Setting up an external list**
<img width="902" alt="Setting up an external list" src="https://github.com/user-attachments/assets/4dcd1345-b116-4435-b82a-acc056a41bb6" />

**Testing an external list against your library**
<img width="1440" alt="Testing an external list against your library" src="https://github.com/user-attachments/assets/2b74e94e-5562-43ef-b60d-7b77faae592a" />

**Writing an AI list**
<img width="902" alt="Writing an AI list" src="https://github.com/user-attachments/assets/7dc3cf0c-61c8-4afd-8bf1-b9e2a23890a8" />

**Building a Smart Playlist from rules**
<img width="902" alt="Building a Smart Playlist from rules" src="https://github.com/user-attachments/assets/a0de75d6-0403-42d4-ab9a-4c99c33ad92b" />

### Collections, scheduling and home rows

**Collection settings and generated artwork**
<img width="902" alt="Collection settings and generated artwork" src="https://github.com/user-attachments/assets/378e107c-daea-4d31-b960-2a303dec777b" />

**A seasonal schedule**
<img width="902" alt="A seasonal schedule" src="https://github.com/user-attachments/assets/b91c2dd7-5616-452b-8042-3561f03c68c8" />

**Turning a source into a home screen row**
<img width="902" alt="Turning a source into a home screen row" src="https://github.com/user-attachments/assets/b7f2e3a3-639a-461c-879d-05b976e698c1" />

**Sorting home screen rows**
<img width="1440" alt="Sorting home screen rows" src="https://github.com/user-attachments/assets/73ffd033-6e91-4bc6-acf8-568918723863" />

### Top-lists

**The Top-Lists tab**
<img width="1440" alt="The Top-Lists tab" src="https://github.com/user-attachments/assets/fb7a374c-5840-405b-a5ed-5f87763d4174" />

**Creating a top-list**
<img width="902" alt="Creating a top-list" src="https://github.com/user-attachments/assets/df55b486-1fb0-4484-b193-3c41315e8712" />

**Customising the Top 10 tile**
<img width="1440" alt="Customising the Top 10 tile" src="https://github.com/user-attachments/assets/42ab2b6b-6602-4879-8924-284894bd47a3" />

### Tools

**The Cleanup tab**
<img width="1440" alt="The Cleanup tab" src="https://github.com/user-attachments/assets/6c84970e-1eb1-496a-8eee-1bee67b4761f" />

</details>

---

## 🛟 Help and troubleshooting

The plugin has a full **Setup Guide** built in: click the help button at the top of the settings page. Start with **Logs**, which show per source how many titles the list had, how many were found and why anything was skipped.

**Common fixes**

- **The row doesn't show:** check that the source is active and inside its schedule, the user is ticked, and a sync has run since you saved. Then refresh the Emby app.
- **Fewer titles than expected:** the missing titles aren't in your library, or lack an IMDb id in Emby. **Preview** lists them.
- **A list came back empty:** the log says why, usually a missing key or a private list. Turn on **Preserve tags and collections on empty result** to keep the old titles.
- **Nothing changes:** check that Dry Run is off.

**Reporting a bug:** click **Report a Bug** in the plugin. Open an issue on [GitHub](../../issues) or post in the [Emby forum thread](https://emby.media/community/topic/146080-plugin-home-screen-companion/), and attach the zip from **Download logs**. Do it soon after the problem, since the log keeps 7 days.

---

## 📄 License and disclaimer

Released under the license in [LICENSE](LICENSE).

*This plugin is not affiliated with Emby, Trakt, MDBList, TMDb, OpenAI, Google, Anthropic or Ollama. It is largely written with AI assistance and tested on my own servers. Use it at your own risk.*
