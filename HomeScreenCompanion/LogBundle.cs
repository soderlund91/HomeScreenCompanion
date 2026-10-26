using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Library;
using MediaBrowser.Model.Entities;
using MediaBrowser.Model.Querying;
using MediaBrowser.Model.Serialization;
using MediaBrowser.Model.Tasks;
using MediaBrowser.Model.Users;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;

namespace HomeScreenCompanion
{
    /// <summary>
    /// "Download logs": one zip with everything needed to troubleshoot.
    ///   README.txt        what is inside and how it was made
    ///   diagnostics.txt   the state right now: versions, tasks, libraries, users and their access,
    ///                     every user's home rows, the top-lists and their folders, the data folder
    ///   settings.json     the plugin settings
    ///   logs/             the plugin's own log, one file per day (see <see cref="PluginLog"/>)
    ///   last-runs/        the latest run of each task as the log modal shows it
    ///
    /// API keys and tokens are always masked. With anonymise on, every user's name and id is
    /// replaced by "User 1", "User 2" … — the same one in every file, so a user can still be followed.
    /// The files on disk are not changed: the masking is done on the copies in the zip.
    /// </summary>
    internal sealed class LogBundle
    {
        private readonly ILibraryManager _libraryManager;
        private readonly IUserManager _userManager;
        private readonly ITaskManager _taskManager;
        private readonly IJsonSerializer _json;
        private readonly object? _appHost;

        public LogBundle(ILibraryManager libraryManager, IUserManager userManager, ITaskManager taskManager, IJsonSerializer json, object? appHost)
        {
            _libraryManager = libraryManager;
            _userManager = userManager;
            _taskManager = taskManager;
            _json = json;
            _appHost = appHost;
        }

        public (string FileName, byte[] Zip) Build(bool anonymise)
        {
            PluginLog.Flush();
            var config = Plugin.Instance?.Configuration ?? new PluginConfiguration();
            var mask = new Masker(config, anonymise ? AllUsers() : new List<User>());

            var entries = new List<(string Name, string Text)>();
            entries.Add(("diagnostics.txt", Diagnostics(config)));
            entries.Add(("settings.json", SafeJson(config)));
            foreach (var f in PluginLog.Files())
                entries.Add(("logs/" + Path.GetFileName(f), ReadShared(f)));
            foreach (var key in new[] { LogStore.Sync, LogStore.HomeScreen, LogStore.TopLists })
            {
                var saved = LogStore.Load(key);
                if (saved == null) continue;
                var sb = new StringBuilder();
                sb.Append("Status:  ").Append(saved.Status).Append('\n');
                sb.Append("Started: ").Append(saved.StartedUtc?.ToLocalTime().ToString("yyyy-MM-dd HH:mm:ss") ?? "-").Append('\n').Append('\n');
                foreach (var l in saved.Lines ?? new List<string>()) sb.Append(l).Append('\n');
                entries.Add(("last-runs/" + key + ".txt", sb.ToString()));
            }
            entries.Insert(0, ("README.txt", Readme(anonymise, mask.UserCount)));

            using var ms = new MemoryStream();
            using (var zip = new ZipArchive(ms, ZipArchiveMode.Create, true))
            {
                foreach (var (name, text) in entries)
                {
                    var entry = zip.CreateEntry(name, CompressionLevel.Optimal);
                    using var w = new StreamWriter(entry.Open(), new UTF8Encoding(false));
                    w.Write(mask.Apply(text));
                }
            }
            var fileName = $"hsc-logs-{DateTime.Now:yyyy-MM-dd-HHmm}{(anonymise ? "-anonymised" : "")}.zip";
            return (fileName, ms.ToArray());
        }

        private static string Readme(bool anonymise, int users)
        {
            var sb = new StringBuilder();
            sb.Append("Home Screen Companion — logs for troubleshooting\n");
            sb.Append($"Made {DateTime.Now:yyyy-MM-dd HH:mm:ss} (server time, UTC{DateTimeOffset.Now:zzz}), plugin {Plugin.Instance?.Version}\n\n");
            sb.Append("diagnostics.txt  the state when this was made: versions, tasks, libraries, users, home rows, top-lists\n");
            sb.Append("settings.json    the plugin settings\n");
            sb.Append($"logs/            the plugin's own log, one file per day, the last {PluginLog.KeepDays} days\n");
            sb.Append("last-runs/       the latest run of each task, as the log window shows it\n\n");
            sb.Append("API keys and tokens are always replaced by ***.\n");
            sb.Append(anonymise
                ? $"User names and ids are replaced by User 1 … User {users}, the same in every file. Names typed into sources, tags or libraries may still contain them.\n"
                : "User names are NOT anonymised.\n");
            return sb.ToString();
        }

        // ── diagnostics.txt ─────────────────────────────────────────────────────

        private string Diagnostics(PluginConfiguration config)
        {
            var d = new Report();
            Section(d, "Versions", () => Versions(d));
            Section(d, "Tasks", () => Tasks(d));
            var libs = Libraries();
            Section(d, "Libraries", () => WriteLibraries(d, libs));
            Section(d, "Users", () => Users(d, libs));
            Section(d, "Home rows", () => HomeRows(d, config, libs));
            Section(d, "Top-lists", () => TopLists(d, config, libs));
            Section(d, "Sources", () => Sources(d, config));
            Section(d, "Plugin data folder", () => DataFolder(d));
            return d.ToString();
        }

        // One part failing never loses the rest of the report.
        private static void Section(Report d, string title, Action write)
        {
            d.Head(title);
            try { write(); }
            catch (Exception ex) { d.Line($"(could not be read: {ex.GetType().Name}: {ex.Message})"); }
        }

        private void Versions(Report d)
        {
            d.Line($"Plugin:   {Plugin.Instance?.Version} {ReleaseChannel()}");
            dynamic? host = _appHost;
            try { d.Line($"Emby:     {host?.ApplicationVersion}"); } catch { d.Line("Emby:     (unknown)"); }
            try { d.Line($"Server OS: {host?.OperatingSystemDisplayName}"); } catch { }
            d.Line($"Runtime:  {System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription}  ·  {System.Runtime.InteropServices.RuntimeInformation.OSDescription}");
            d.Line($"Time:     {DateTime.Now:yyyy-MM-dd HH:mm:ss} (UTC{DateTimeOffset.Now:zzz})");
            d.Line($"Data:     {Plugin.Instance?.DataFolderPath}");
        }

        private static string ReleaseChannel()
        {
            var info = typeof(LogBundle).Assembly.GetCustomAttributes(typeof(AssemblyInformationalVersionAttribute), false)
                .OfType<AssemblyInformationalVersionAttribute>().FirstOrDefault()?.InformationalVersion ?? "";
            var plus = info.IndexOf('+');
            if (plus >= 0) info = info.Substring(0, plus);
            var dash = info.IndexOf('-');
            return dash >= 0 ? info.Substring(dash + 1) : "";
        }

        private void Tasks(Report d)
        {
            d.Line($"Sync:            {(HomeScreenCompanionTask.IsRunning ? "running" : "idle")}  ·  last: {HomeScreenCompanionTask.LastRunStatus}  ·  started {Local(HomeScreenCompanionTask.LastStartedUtc)}");
            d.Line($"Top-list rows:   {(TopListSyncTask.IsRunning ? "running" : "idle")}  ·  last: {TopListSyncTask.LastRunStatus}  ·  started {Local(TopListSyncTask.LastStartedUtc)}");
            d.Line($"Home Screen:     {(HomeSectionSyncTask.IsRunning ? "running" : "idle")}  ·  last: {HomeSectionSyncTask.LastSyncResult}  ·  at {HomeSectionSyncTask.LastSyncTime}");
            d.Line($"Top-list merge:  {(MergeTopListVersionsTask.IsRunning ? "running" : "idle")}  ·  last: {MergeTopListVersionsTask.LastRunStatus}");
            d.Line($"Run gate:        {(RunGate.Busy ? "held by " + RunGate.Holder : "free")}");
            d.Blank();
            d.Line("Emby scheduled tasks of the plugin (name · state · last result · triggers):");
            foreach (var task in _taskManager.ScheduledTasks)
            {
                try
                {
                    if (!(task.Category ?? "").Contains("Home Screen Companion")) continue;
                    var r = task.LastExecutionResult;
                    string last = r == null ? "never"
                        : $"{r.Status} {r.StartTimeUtc.ToLocalTime():yyyy-MM-dd HH:mm} – {r.EndTimeUtc.ToLocalTime():HH:mm}{(string.IsNullOrEmpty(r.ErrorMessage) ? "" : " " + r.ErrorMessage)}";
                    var triggers = new List<string>();
                    foreach (var tr in task.Triggers ?? Array.Empty<TaskTriggerInfo>())
                    {
                        string type = tr.Type ?? "";
                        if (tr.TimeOfDayTicks.HasValue) type += " " + TimeSpan.FromTicks(tr.TimeOfDayTicks.Value).ToString(@"hh\:mm");
                        if (tr.IntervalTicks.HasValue) type += " every " + TimeSpan.FromTicks(tr.IntervalTicks.Value);
                        if (type.IndexOf("Weekly", StringComparison.OrdinalIgnoreCase) >= 0) type += " " + tr.DayOfWeek;
                        triggers.Add(type);
                    }
                    d.Line($"  {task.Name} · {task.State} · {last} · {(triggers.Count == 0 ? "no triggers" : string.Join(", ", triggers))}");
                }
                catch (Exception ex) { PluginLog.Swallowed(ex); }
            }
        }

        private static string Local(DateTime? utc) => utc.HasValue ? utc.Value.ToLocalTime().ToString("yyyy-MM-dd HH:mm") : "-";

        private sealed class Lib
        {
            public string Name = "", Id = "", Guid = "", Type = "";
            public string[] Paths = Array.Empty<string>();
        }

        private List<Lib> Libraries()
        {
            var list = new List<Lib>();
            foreach (var f in _libraryManager.GetVirtualFolders())
            {
                var lib = new Lib { Name = f.Name ?? "", Id = f.ItemId ?? "", Type = f.CollectionType ?? "mixed", Paths = f.Locations ?? Array.Empty<string>() };
                try
                {
                    if (long.TryParse(lib.Id, out var iid)) lib.Guid = _libraryManager.GetItemById(iid)?.Id.ToString("N") ?? "";
                }
                catch (Exception ex) { PluginLog.Swallowed(ex); }
                list.Add(lib);
            }
            return list;
        }

        private static void WriteLibraries(Report d, List<Lib> libs)
        {
            foreach (var l in libs)
                d.Line($"{l.Name}  ·  id {l.Id}  ·  {l.Guid}  ·  {l.Type}  ·  {string.Join("; ", l.Paths)}");
        }

        // "Movies (3)" for a library id or GUID; the id itself when it is no library.
        private static string LibName(List<Lib> libs, string? id)
        {
            if (string.IsNullOrEmpty(id)) return "";
            var norm = id!.Replace("-", "").Trim();
            var l = libs.FirstOrDefault(x => string.Equals(x.Id, norm, StringComparison.OrdinalIgnoreCase) || string.Equals(x.Guid, norm, StringComparison.OrdinalIgnoreCase));
            return l != null ? $"{l.Name} ({l.Id})" : id!;
        }

        private List<User> AllUsers() => UsersInOrder(_userManager);

        // Every user (hidden and disabled ones too), in the order an anonymised zip numbers them:
        // "User 1", "User 2" … by name. The download window's "Who is who" list uses the same order.
        internal static List<User> UsersInOrder(IUserManager userManager)
        {
            try { return userManager.GetUserList(new UserQuery()).OrderBy(u => u.Name, StringComparer.OrdinalIgnoreCase).ToList(); }
            catch (Exception ex) { PluginLog.Swallowed(ex); return new List<User>(); }
        }

        private void Users(Report d, List<Lib> libs)
        {
            dynamic mgr = _userManager;
            foreach (var u in AllUsers())
            {
                d.Line($"{u.Name}  ·  {u.Id:N}");
                try
                {
                    dynamic? policy = TopListSyncTask.GetPolicy(mgr, u, u.InternalId);
                    if (policy != null)
                    {
                        bool all = policy.EnableAllFolders;
                        d.Line($"    admin {(bool)policy.IsAdministrator}  ·  disabled {(bool)policy.IsDisabled}  ·  hidden {(bool)policy.IsHidden}");
                        d.Line("    access: " + (all ? "all libraries" : string.Join(", ", ((string[])policy.EnabledFolders ?? Array.Empty<string>()).Select(x => LibName(libs, x)))));
                        var blocked = (string[])policy.BlockedTags ?? Array.Empty<string>();
                        if (blocked.Length > 0) d.Line("    blocked tags: " + string.Join(", ", blocked));
                        if (policy.MaxParentalRating != null) d.Line($"    max parental rating: {policy.MaxParentalRating}");
                    }
                }
                catch (Exception ex) { d.Line($"    policy could not be read: {ex.Message}"); }
                try
                {
                    var cfg = _userManager.GetUserConfiguration(u);
                    if (cfg != null)
                    {
                        if ((cfg.MyMediaExcludes?.Length ?? 0) > 0) d.Line("    hidden from My Media: " + string.Join(", ", cfg.MyMediaExcludes.Select(x => LibName(libs, x))));
                        if ((cfg.LatestItemsExcludes?.Length ?? 0) > 0) d.Line("    left out of Latest: " + string.Join(", ", cfg.LatestItemsExcludes.Select(x => LibName(libs, x))));
                        if ((cfg.OrderedViews?.Length ?? 0) > 0) d.Line("    library order: " + string.Join(", ", cfg.OrderedViews.Select(x => LibName(libs, x))));
                    }
                }
                catch (Exception ex) { d.Line($"    configuration could not be read: {ex.Message}"); }
            }
        }

        // Who tracks a row: "source 'X'" or "top-list 'Y'".
        private static Dictionary<string, string> RowOwners(PluginConfiguration config)
        {
            var owners = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var t in config.Tags ?? new List<TagConfig>())
                foreach (var r in t.HomeSectionTracked ?? new List<HomeSectionTracking>())
                    if (!string.IsNullOrEmpty(r.SectionId)) owners[Norm(r.UserId) + "|" + r.SectionId] = $"source '{(string.IsNullOrWhiteSpace(t.Name) ? t.Tag : t.Name)}'";
            foreach (var tl in config.TopLists ?? new List<TopListHomeSection>())
                foreach (var r in tl.HomeSectionTracked ?? new List<HomeSectionTracking>())
                    if (!string.IsNullOrEmpty(r.SectionId)) owners[Norm(r.UserId) + "|" + r.SectionId] = $"top-list '{tl.TagName}'";
            return owners;
        }

        private static string Norm(string? id) => (id ?? "").Replace("-", "").Trim().ToLowerInvariant();

        private void HomeRows(Report d, PluginConfiguration config, List<Lib> libs)
        {
            var owners = RowOwners(config);
            foreach (var u in AllUsers())
            {
                ContentSection[] rows;
                try { rows = _userManager.GetHomeSections(u.InternalId, CancellationToken.None)?.Sections ?? Array.Empty<ContentSection>(); }
                catch (Exception ex) { d.Line($"{u.Name}: rows could not be read: {ex.Message}"); continue; }
                d.Line($"{u.Name}  ·  {RunLog.Plural(rows.Length, "row")}");
                for (int i = 0; i < rows.Length; i++)
                {
                    var s = rows[i];
                    owners.TryGetValue(Norm(u.Id.ToString("N")) + "|" + s.Id, out var owner);
                    var title = !string.IsNullOrEmpty(s.CustomName) ? s.CustomName : s.Name;
                    d.Line($"  {i + 1,2}. [{s.SectionType}] \"{title}\"  ·  id {s.Id}  ·  {owner ?? "not tracked by the plugin"}");
                    var facts = new List<string>();
                    if (!string.IsNullOrEmpty(s.ParentId)) facts.Add("library/parent " + ParentName(libs, s.ParentId));
                    foreach (var (k, v) in Props(s, "Id", "Name", "CustomName", "SectionType", "ParentId", "ParentItem", "Query", "ExcludedFolders"))
                        facts.Add($"{k} {v}");
                    var excluded = s.ExcludedFolders ?? Array.Empty<string>();
                    if (excluded.Length > 0) facts.Add($"ExcludedFolders {excluded.Length}");
                    var query = s.GetType().GetProperty("Query")?.GetValue(s);
                    if (query != null)
                        foreach (var (k, v) in Props(query, "ExcludeUserViewIds"))
                            facts.Add($"Query.{k} {(k == "TagIds" ? TagNames(v) : v)}");
                    if (facts.Count > 0) d.Line("      " + string.Join("  ·  ", facts));
                }
            }
        }

        private string ParentName(List<Lib> libs, string parentId)
        {
            var lib = LibName(libs, parentId);
            if (lib != parentId) return lib;
            try
            {
                var item = long.TryParse(parentId, out var iid) ? _libraryManager.GetItemById(iid) : null;
                return item != null ? $"{item.GetType().Name} '{item.Name}' ({parentId})" : parentId + " (NOT FOUND)";
            }
            catch { return parentId; }
        }

        private string TagNames(string ids)
        {
            return string.Join(", ", ids.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries).Select(x =>
            {
                var id = x.Trim();
                try { return long.TryParse(id, out var iid) && _libraryManager.GetItemById(iid) is BaseItem t ? $"'{t.Name}' ({id})" : id; }
                catch { return id; }
            }));
        }

        // The object's properties that hold something (not null, empty, false or 0), as text.
        private static IEnumerable<(string, string)> Props(object o, params string[] skip)
        {
            foreach (var p in o.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                if (skip.Contains(p.Name) || p.GetIndexParameters().Length > 0) continue;
                object? v;
                try { v = p.GetValue(o); } catch { continue; }
                var text = Text(v);
                if (text != null) yield return (p.Name, text);
            }
        }

        private static string? Text(object? v)
        {
            switch (v)
            {
                case null: return null;
                case string s: return s.Length == 0 ? null : s;
                case bool b: return b ? "true" : null;
                case int i: return i == 0 ? null : i.ToString(CultureInfo.InvariantCulture);
                case long l: return l == 0 ? null : l.ToString(CultureInfo.InvariantCulture);
                case double db: return db == 0 ? null : db.ToString(CultureInfo.InvariantCulture);
                case Enum e: return Convert.ToInt64(e) == 0 ? null : e.ToString();
                case DateTime dt: return dt == default ? null : dt.ToString("yyyy-MM-dd HH:mm");
                case IEnumerable seq:
                    var parts = seq.Cast<object?>().Select(x => x?.ToString() ?? "").Where(x => x.Length > 0).ToList();
                    return parts.Count == 0 ? null : string.Join(",", parts);
                default:
                    var t = v.GetType();
                    return t.IsPrimitive || t.IsValueType ? v.ToString() : null;
            }
        }

        private void TopLists(Report d, PluginConfiguration config, List<Lib> libs)
        {
            var root = Path.Combine(Plugin.Instance?.DataFolderPath ?? "", "toplists");
            var userNames = AllUsers().ToDictionary(u => u.Id.ToString("N"), u => u.Name ?? "", StringComparer.OrdinalIgnoreCase);
            string UserName(string id) => userNames.TryGetValue(Norm(id), out var n) ? n : id + " (no such user)";
            foreach (var tl in config.TopLists ?? new List<TopListHomeSection>())
            {
                d.Line($"'{tl.TagName}'  ·  {(string.IsNullOrEmpty(tl.ContentType) ? "Movies" : tl.ContentType)}  ·  max {tl.MaxItems}  ·  library {LibName(libs, tl.HomeSectionLibraryId)}");
                bool libExists = libs.Any(l => string.Equals(l.Id, (tl.HomeSectionLibraryId ?? "").Trim(), StringComparison.OrdinalIgnoreCase));
                if (!libExists) d.Line("    ⚠ its library is not one of Emby's libraries");
                d.Line("    users: " + string.Join(", ", (tl.HomeSectionUserIds ?? new List<string>()).Select(UserName)));
                foreach (var r in tl.HomeSectionTracked ?? new List<HomeSectionTracking>())
                {
                    string state;
                    try
                    {
                        var row = Guid.TryParse(r.UserId, out var g) && _userManager.GetUserById(g) is User u
                            ? (_userManager.GetHomeSections(u.InternalId, CancellationToken.None)?.Sections ?? Array.Empty<ContentSection>()).FirstOrDefault(s => s.Id == r.SectionId)
                            : null;
                        state = row == null ? "ROW NOT FOUND"
                            : string.Equals(Norm(row.ParentId), Norm(tl.HomeSectionLibraryId), StringComparison.OrdinalIgnoreCase) ? "ok"
                            : $"row shows {ParentName(libs, row.ParentId ?? "")}, not the list's library";
                    }
                    catch (Exception ex) { state = "could not be read: " + ex.Message; }
                    d.Line($"    row of {UserName(r.UserId)}: {r.SectionId}  ·  {state}");
                }
                var untracked = (tl.HomeSectionUserIds ?? new List<string>())
                    .Where(id => !(tl.HomeSectionTracked ?? new List<HomeSectionTracking>()).Any(r => Norm(r.UserId) == Norm(id))).ToList();
                if (untracked.Count > 0) d.Line("    no tracked row for: " + string.Join(", ", untracked.Select(UserName)));

                try
                {
                    if (long.TryParse((tl.HomeSectionLibraryId ?? "").Trim(), out var libId) && _libraryManager.GetItemById(libId) is BaseItem lib)
                    {
                        var items = _libraryManager.GetItemList(new InternalItemsQuery { Parent = lib, Recursive = true, IncludeItemTypes = new[] { "Movie", "Series" } });
                        d.Line($"    in its library: {RunLog.Plural(items.Length, "title")}");
                        foreach (var it in items.OrderBy(i => i.SortName, StringComparer.Ordinal))
                            d.Line($"      {it.SortName}  {it.Name}  ·  {it.GetType().Name} {it.InternalId}{(string.IsNullOrEmpty(it.Path) ? "" : "  ·  " + Path.GetFileName(it.Path))}");
                    }
                }
                catch (Exception ex) { d.Line("    library items could not be read: " + ex.Message); }

                var folder = Path.Combine(root, SafeFolderName(tl.TagName));
                if (Directory.Exists(folder))
                {
                    var files = Directory.GetFiles(folder);
                    var dirs = Directory.GetDirectories(folder);
                    d.Line($"    folder: {RunLog.Plural(files.Count(f => f.EndsWith(".strm", StringComparison.OrdinalIgnoreCase)), ".strm file")}, {RunLog.Plural(files.Length, "file")} in all, {RunLog.Plural(dirs.Length, "folder")}");
                    foreach (var f in files.Where(f => f.EndsWith(".strm", StringComparison.OrdinalIgnoreCase)).OrderBy(f => f, StringComparer.OrdinalIgnoreCase))
                        d.Line("      " + Path.GetFileName(f));
                    foreach (var dir in dirs.OrderBy(x => x, StringComparer.OrdinalIgnoreCase))
                        d.Line("      " + Path.GetFileName(dir) + "/");
                }
                else d.Line("    folder: none");
                d.Blank();
            }
        }

        private static string SafeFolderName(string? name)
        {
            var invalid = Path.GetInvalidFileNameChars();
            var safe = new string((name ?? "unknown").Select(c => Array.IndexOf(invalid, c) >= 0 ? '_' : c).ToArray()).Trim('.');
            return string.IsNullOrWhiteSpace(safe) ? "unknown" : safe;
        }

        private static void Sources(Report d, PluginConfiguration config)
        {
            foreach (var t in config.Tags ?? new List<TagConfig>())
            {
                var does = new List<string>();
                if (t.EnableTag) does.Add("tag");
                if (t.EnableCollection) does.Add("collection");
                if (t.EnablePlaylist) does.Add("playlist");
                if (t.EnableHomeSection) does.Add($"home row ({RunLog.Plural(t.HomeSectionUserIds?.Count ?? 0, "user")}, {RunLog.Plural(t.HomeSectionTracked?.Count ?? 0, "tracked row")})");
                d.Line($"{(string.IsNullOrWhiteSpace(t.Name) ? "" : t.Name + " ")}[{t.Tag}]  ·  {(string.IsNullOrEmpty(t.SourceType) ? "External" : t.SourceType)}  ·  {(t.Active ? "active" : "off")}  ·  {(does.Count == 0 ? "nothing" : string.Join(", ", does))}");
            }
        }

        private static void DataFolder(Report d)
        {
            var root = Plugin.Instance?.DataFolderPath;
            if (string.IsNullOrEmpty(root) || !Directory.Exists(root)) { d.Line("(none)"); return; }
            foreach (var f in Directory.GetFiles(root).OrderBy(x => x, StringComparer.OrdinalIgnoreCase))
            {
                var fi = new FileInfo(f);
                d.Line($"{fi.Name}  ·  {Size(fi.Length)}  ·  {fi.LastWriteTime:yyyy-MM-dd HH:mm}");
            }
            foreach (var dir in Directory.GetDirectories(root, "*", SearchOption.AllDirectories).OrderBy(x => x, StringComparer.OrdinalIgnoreCase))
            {
                var files = new DirectoryInfo(dir).GetFiles();
                var rel = dir.Substring(root!.Length).TrimStart(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar).Replace('\\', '/');
                var newest = files.Length == 0 ? "" : $"  ·  newest {files.Max(x => x.LastWriteTime):yyyy-MM-dd HH:mm}";
                d.Line($"{rel}/  ·  {RunLog.Plural(files.Length, "file")}  ·  {Size(files.Sum(x => x.Length))}{newest}");
            }
        }

        private static string Size(long bytes) =>
            bytes >= 1024 * 1024 ? $"{bytes / (1024.0 * 1024):0.0} MB" : bytes >= 1024 ? $"{bytes / 1024.0:0} KB" : $"{bytes} B";

        // ── settings.json ───────────────────────────────────────────────────────

        private string SafeJson(PluginConfiguration config)
        {
            try { return PrettyJson(_json.SerializeToString(config)); }
            catch (Exception ex) { return "(settings could not be written: " + ex.Message + ")"; }
        }

        // Indents a compact JSON text (strings are copied as they are).
        private static string PrettyJson(string json)
        {
            var sb = new StringBuilder(json.Length * 2);
            int indent = 0;
            bool inString = false;
            for (int i = 0; i < json.Length; i++)
            {
                char c = json[i];
                if (inString)
                {
                    sb.Append(c);
                    if (c == '\\' && i + 1 < json.Length) sb.Append(json[++i]);
                    else if (c == '"') inString = false;
                    continue;
                }
                switch (c)
                {
                    case '"': inString = true; sb.Append(c); break;
                    case '{':
                    case '[':
                        // Empty object / array stays on one line.
                        if (i + 1 < json.Length && (json[i + 1] == '}' || json[i + 1] == ']')) { sb.Append(c).Append(json[++i]); break; }
                        sb.Append(c).Append('\n').Append(' ', ++indent * 2); break;
                    case '}':
                    case ']': sb.Append('\n').Append(' ', --indent * 2).Append(c); break;
                    case ',': sb.Append(c).Append('\n').Append(' ', indent * 2); break;
                    case ':': sb.Append(": "); break;
                    default: if (!char.IsWhiteSpace(c)) sb.Append(c); break;
                }
            }
            return sb.ToString();
        }

        private static string ReadShared(string path)
        {
            try
            {
                using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
                using var r = new StreamReader(fs, Encoding.UTF8);
                return r.ReadToEnd();
            }
            catch (Exception ex) { return "(could not be read: " + ex.Message + ")"; }
        }

        private sealed class Report
        {
            private readonly StringBuilder _sb = new StringBuilder();
            public void Head(string title)
            {
                if (_sb.Length > 0) _sb.Append('\n');
                _sb.Append("══ ").Append(title).Append(' ').Append(new string('═', Math.Max(3, 60 - title.Length))).Append('\n');
            }
            public void Line(string text) => _sb.Append(text).Append('\n');
            public void Blank() => _sb.Append('\n');
            public override string ToString() => _sb.ToString();
        }

        // ── Masking ─────────────────────────────────────────────────────────────

        /// <summary>
        /// API keys and tokens out (always), user names and ids out (when anonymising). One instance
        /// per zip, so each user is the same "User n" in every file.
        /// </summary>
        internal sealed class Masker
        {
            private const string Hidden = "***";
            // key=value in URLs and settings texts: apikey, api_key, token, access_token, client_id …
            private static readonly Regex UrlSecret = new Regex(
                @"(?i)(\b(?:api[_-]?key|apikey|key|token|access[_-]?token|client[_-]?id|client[_-]?secret|secret|password|x-emby-token|x-mediabrowser-token)=)[^&\s""'<>]+",
                RegexOptions.Compiled);
            private static readonly Regex Bearer = new Regex(@"(?i)\b(bearer\s+)[A-Za-z0-9._~+/=-]{8,}", RegexOptions.Compiled);

            private readonly List<string> _secrets;
            private readonly List<(Regex Pattern, string Pseudonym)> _users = new List<(Regex, string)>();
            public int UserCount { get; }

            public Masker(PluginConfiguration config, List<User> users)
            {
                // Every setting that holds a key, an id of an API client, a token or a password.
                _secrets = typeof(PluginConfiguration).GetProperties()
                    .Where(p => p.PropertyType == typeof(string) && Regex.IsMatch(p.Name, "(Key|Token|Secret|Password|ClientId)$"))
                    .Select(p => (p.GetValue(config) as string ?? "").Trim())
                    .Concat((config.AiConnections ?? new List<AiConnection>()).Select(c => (c?.ApiKey ?? "").Trim()))
                    .Where(v => v.Length >= 4)
                    .Distinct().OrderByDescending(v => v.Length).ToList();

                UserCount = users.Count;
                var forms = new List<(string Form, string Pseudonym)>();
                for (int i = 0; i < users.Count; i++)
                {
                    var n = (i + 1).ToString(CultureInfo.InvariantCulture);
                    var pseudonym = "User " + n;
                    forms.Add((users[i].Id.ToString("N"), "user-" + n));
                    forms.Add((users[i].Id.ToString("D"), "user-" + n));
                    var name = (users[i].Name ?? "").Trim();
                    if (name.Length < 2) continue;
                    forms.Add((name, pseudonym));
                    var plain = WithoutAccents(name);
                    if (!string.Equals(plain, name, StringComparison.OrdinalIgnoreCase)) forms.Add((plain, pseudonym));
                }
                // Longest first, so "Anna-Lena" goes before "Anna".
                foreach (var (form, pseudonym) in forms.OrderByDescending(f => f.Form.Length))
                    _users.Add((new Regex(@"(?<![\p{L}\p{N}])" + Regex.Escape(form) + @"(?![\p{L}\p{N}])", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant), pseudonym));
            }

            public string Apply(string text)
            {
                if (string.IsNullOrEmpty(text)) return text;
                foreach (var s in _secrets) text = text.Replace(s, Hidden);
                text = UrlSecret.Replace(text, "$1" + Hidden);
                text = Bearer.Replace(text, "$1" + Hidden);
                foreach (var (pattern, pseudonym) in _users)
                    text = pattern.Replace(text, m => InIdentifier(text, m) ? pseudonym.Replace(" ", "").ToLowerInvariant() : pseudonym);
                return text;
            }

            // Part of a tag-like name ("recommended_movies_hakan"): written without the space.
            private static bool InIdentifier(string text, Match m)
            {
                char before = m.Index > 0 ? text[m.Index - 1] : ' ';
                int end = m.Index + m.Length;
                char after = end < text.Length ? text[end] : ' ';
                return before == '_' || after == '_';
            }

            private static string WithoutAccents(string s)
            {
                var decomposed = s.Normalize(NormalizationForm.FormD);
                var sb = new StringBuilder(decomposed.Length);
                foreach (var c in decomposed)
                    if (CharUnicodeInfo.GetUnicodeCategory(c) != UnicodeCategory.NonSpacingMark) sb.Append(c);
                return sb.ToString().Normalize(NormalizationForm.FormC);
            }
        }
    }
}
