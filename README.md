# 🎬 Home Screen Companion for Emby

<img width="481" height="142" alt="HSC" src="https://github.com/user-attachments/assets/2887420a-7198-472e-8afb-a49eda3231d9" />

**Home Screen Companion** is the ultimate plugin for your Emby server. It automates the management of tags, collections, and playlists for your movies and TV shows, while keeping everyone's home screens perfectly synchronized and looking great.

Whether you want to pull trending lists from Trakt, build smart dynamic playlists from your own library, get AI-powered recommendations, or effortlessly copy your perfect home screen layout to all your users — Home Screen Companion does the heavy lifting for you. Sit back and let your server manage itself!

> [!IMPORTANT]
> Check each release for required server version.

---

## ✨ Key Features

- **5 Powerful Source Types:** Import from external list services, create rules-based "Smart Playlists", leverage AI recommendations, or use your existing local collections and playlists.
- **Automatic Tagging & Collections:** Items are automatically tagged, untagged, and managed in Emby Collections as your sources update.
- **Advanced Scheduling:** Control exactly when a list is active. Perfect for seasonal content like "Christmas Movies" or weekly "Friday Night Action".
- **Dynamic Home Screen Sections:** Automatically inject dedicated rows onto the home screen based on tags or collections. The plugin creates, updates, and cleans them up automatically.
- **Home Screen Sync:** Created the perfect home screen layout? Sync it to all your other users with a single click—or keep it synced automatically!
- **User Playlists:** Create and sync dynamic Emby Playlists directly to specific users based on any of your sources.

---

## 🚀 Source Types

### 1. External Lists (Trakt & MDBList)
Connect your Emby server to popular and curated lists on **Trakt** or **MDBList**. The plugin fetches the lists regularly, matches the movies/shows to your library, and updates your tags and collections automatically.
* **Supports:** Trakt (Trending, Popular, Watched, User Lists) and MDBList (Dynamic lists).
* *API key required for the respective service.*

### 2. Smart Playlists
Build dynamic lists right from your own library using our flexible, built-in rule engine. No external services required!
* **Filter by:** Resolution (4K, 1080p), Audio Format (Atmos, TrueHD), Genre, Rating, Watch Status (Played/Unplayed), Date Added, and much more.
* **Examples:** *"All Unplayed 4K HDR movies with Atmos"* or *"Action movies from 2020+ rated above 7"*.

### 3. AI-Generated Lists
Let Artificial Intelligence curate content for you! Just write a simple prompt (e.g., *"Best psychological thrillers from the 90s"*) and the AI will generate a list matched perfectly against your library.
* **Supports:** OpenAI (GPT-4o-mini), Google Gemini, and local Ollama models (e.g., Llama 3).
* **Personalized:** You can optionally include a user's watch history so the AI tailors the recommendations just for them!

### 4. Local Collections
Tag items based on an existing Emby Collection. Combine this with scheduling to create time-limited promotions of curated content.

### 5. Local Playlists
Works just like Local Collections, but uses an existing Emby Playlist as the source.

---

## ⏰ Smart Scheduling

Only want to show certain collections at specific times? No problem!
* **Annually:** Make your "Holiday Movies" collection active only from December 1st to 31st every year.
* **Weekly:** Activate a "Weekend Binge" list only on Fridays and Saturdays.
* **Specific Dates:** Perfect for one-time events or themed weeks.

When the schedule ends, the plugin automatically cleans up the tags and collections from your server.

---

## 📺 Home Screen & Syncing

Take total control over how Emby looks for you and your users.

### Per-Entry Home Screen Sections
Add a custom row to the home screen for any specific tag or collection. The plugin handles everything—no manual Emby configuration required. Just select which users should see the section, customize the sorting and display style, and the plugin rolls it out.

### Home Screen Sync
Ever get frustrated trying to give all your users the same great home screen experience?
With **Home Screen Sync**, you choose one user as the "Master" and copy their entire home screen layout to any (or all) other users. You can even set it to sync automatically in the background!

---

## 🛡️ Safety & Reliability

* **Fail-Safe Cleanup:** If an external list fails to download, the plugin skips the cleanup process for that tag, ensuring your library never accidentally loses data.
* **Dry Run Mode:** Want to test your rules without changing anything? Turn on Dry Run to see exactly what *would* happen in the logs.
* **Live Logging:** Follow the execution in real-time directly from the plugin's settings page.

---

## ⚙️ Installation & Setup

1. Download the latest `.dll` file from the [Releases](../../releases) page.
2. Shut down your Emby Server.
3. Place the `.dll` file in your Emby `plugins` folder.
4. Start your Emby Server.
5. Go to your Emby Dashboard—you will find **Home Screen Companion** in the sidebar menu!

*(Don't forget to enter your API keys in the **Settings** tab if you plan to use External Lists or AI features).*

---

### Screenshots

<details>
<summary>Click to view screenshots</summary>

<img width="768" alt="main" src="https://github.com/user-attachments/assets/efc94da5-030a-4380-ab07-5ec9e5ff7151" />
<img width="740" alt="lmi" src="https://github.com/user-attachments/assets/f2e0f1f4-fdc0-4eea-b9ef-a1b2acd14611" />
<img width="743" alt="schedule" src="https://github.com/user-attachments/assets/f9efe07b-5974-4485-bbd7-381e6bd071dd" />
<img width="728" alt="home screen section" src="https://github.com/user-attachments/assets/8dc7b72b-4272-4527-b505-44fa155d4692" />
<img width="737" alt="sync" src="https://github.com/user-attachments/assets/29103ea5-fce7-47be-9f75-99ee38b58975" />

</details>

---

*Disclaimer: This plugin is not affiliated with Emby, Trakt, MDBList, OpenAI, Google, or Ollama. This plugin is heavily vibe-coded, tested on my own server — use at your own risk.*