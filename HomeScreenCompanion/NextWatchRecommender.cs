using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Entities.Movies;
using MediaBrowser.Controller.Entities.TV;
using MediaBrowser.Controller.Library;
using MediaBrowser.Model.Entities;
using MediaBrowser.Model.Querying;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace HomeScreenCompanion
{
    /// <summary>
    /// "Your Next Watch" (source type NextWatch): per user, the titles most like what they watched
    /// recently. No AI — local "more like this" scoring:
    ///   seed       = titles finished in the last 90 days (the source's history window; or the last 50
    ///                when that is too few; a show counts once an episode was watched), favourites and
    ///                titles rated 7+ (unless the source turns favourites and ratings off);
    ///   candidates = movies and shows the user can open (Emby's per-user query, so library access
    ///                and parental rating apply), not watched or started, one item per film;
    ///   score      = weighted overlap with the seed on genres, top-billed people, studios and
    ///                tags (rare people/studios/tags count more; a genre counts by how much more the
    ///                user watches it than the library carries it, capped at 3x), year closeness, community rating and a
    ///                "people like you" bonus from users who watched the same seed titles;
    ///                Each candidate is also matched against the one seed title it has most in common
    ///                with: several kinds of shared signal (genres, people, studio, keywords) count
    ///                more than one shared actor or one broad genre, and the next/previous film of a
    ///                franchise always counts as strong; single-signal matches score lower;
    ///   list       = best first, with caps per franchise (TMDb collection) and main genre and a
    ///                movie/show mix that follows the seed; no single-signal match in the top 10;
    ///                unwatched favourites at most 3 in the top 10 and ~30% overall, spread out.
    /// Few or no plays: the server's most watched titles of all time (viewers = users who finished
    /// it, any date) the user has not seen, a different mix per user and day drawn from the top 3x
    /// the list size. Everything
    /// shared by all users (features, everyone's watch data) is built once per run;
    /// each user then costs one id query (cached per access policy) and an in-memory scoring pass.
    /// </summary>
    internal sealed class NextWatchRecommender
    {
        internal const int DefaultLimit = 50;
        // SeedDays: the default history window, and the window of the "recently popular" signal.
        internal const int SeedDays = 90, MinSeedDays = 7, MaxSeedDays = 730;
        private const int SeedMin = 30, SeedFallback = 50, SeedMax = 150, MinSeed = 3, SimilarUsers = 25;
        private const int MaxActors = 5, MaxCrew = 3;
        private const int EpisodeBatchShows = 250;   // shows whose episodes are read in one go

        private readonly ILibraryManager _libraryManager;
        private readonly IUserManager _userManager;
        private readonly PopularityCounter _popularity;
        private readonly List<BaseItem> _titles;
        private readonly HashSet<string> _ignoredTags;
        private readonly Action<string> _log;

        private bool _ready;
        private readonly List<Group> _groups = new List<Group>();
        private readonly Dictionary<string, int> _groupByKey = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        private readonly Vocab _genres = new Vocab(), _people = new Vocab(), _studios = new Vocab(), _tags = new Vocab();
        private readonly List<string> _franchiseNames = new List<string>();
        private double[] _genreShare = Array.Empty<double>();   // share of library titles carrying each genre
        private double[] _idfGenre = Array.Empty<double>(), _idfPeople = Array.Empty<double>(), _idfStudio = Array.Empty<double>(), _idfTag = Array.Empty<double>();
        private readonly Dictionary<long, History> _history = new Dictionary<long, History>();
        private int[] _viewers90 = Array.Empty<int>();
        private int[] _viewersAll = Array.Empty<int>();   // users who finished it, any date (cold start)
        private readonly Dictionary<string, HashSet<long>> _accessByPolicy = new Dictionary<string, HashSet<long>>();

        public NextWatchRecommender(ILibraryManager libraryManager, IUserManager userManager, PopularityCounter popularity,
            IEnumerable<BaseItem> titles, IEnumerable<string> ignoredTags, Action<string> log)
        {
            _libraryManager = libraryManager;
            _userManager = userManager;
            _popularity = popularity;
            _titles = titles.Where(t => t is Movie || t is Series).ToList();
            _ignoredTags = new HashSet<string>(ignoredTags ?? Enumerable.Empty<string>(), StringComparer.OrdinalIgnoreCase);
            _log = log ?? (_ => { });
        }

        public string PrepareSummary { get; private set; } = "";

        // ── Shared, once per run ──────────────────────────────────────────────────────────────
        public void EnsurePrepared() => Prepare();

        private void Prepare()
        {
            if (_ready) return;
            _ready = true;
            var total = Stopwatch.StartNew();
            var sw = Stopwatch.StartNew();

            // One group per film/show; versions of a film (separate items) share the IMDb id.
            var byFilmKey = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            foreach (var item in _titles.OrderBy(i => i.InternalId))
            {
                var key = FilmKey(item);
                if (!byFilmKey.TryGetValue(key, out var gi))
                {
                    gi = _groups.Count;
                    byFilmKey[key] = gi;
                    _groups.Add(new Group { Rep = item, IsSeries = item is Series });
                }
                _groups[gi].Members.Add(item);
            }
            var groupByItem = new Dictionary<long, int>();
            for (int g = 0; g < _groups.Count; g++)
                foreach (var m in _groups[g].Members) groupByItem[m.InternalId] = g;

            // Genres, studios, tags, year, rating.
            foreach (var g in _groups)
            {
                var it = g.Rep;
                g.Genres = (it.Genres ?? Array.Empty<string>()).Where(s => !string.IsNullOrWhiteSpace(s)).Select(_genres.Id).Distinct().ToArray();
                g.MainGenre = g.Genres.Length > 0 ? g.Genres[0] : -1;
                g.Studios = (it.Studios ?? Array.Empty<string>()).Where(s => !string.IsNullOrWhiteSpace(s)).Take(3).Select(_studios.Id).Distinct().ToArray();
                g.Tags = (it.Tags ?? Array.Empty<string>()).Where(s => !string.IsNullOrWhiteSpace(s) && !_ignoredTags.Contains(s)).Select(_tags.Id).Distinct().ToArray();
                g.Year = it.ProductionYear ?? 0;
                g.Rating = it.CommunityRating.HasValue ? Math.Min(10, Math.Max(0, it.CommunityRating.Value)) / 10.0 : 0.6;
            }
            long tFeatures = sw.ElapsedMilliseconds; sw.Restart();

            // People: top-billed actors plus directors and writers, read in batches.
            LoadPeople(groupByItem);
            long tPeople = sw.ElapsedMilliseconds; sw.Restart();

            // Franchises: TMDb collections (BoxSets with a TMDb id).
            int franchiseCount = 0;
            try
            {
                var boxSets = _libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "BoxSet" }, Recursive = true })
                    .Where(b => !string.IsNullOrEmpty(b.GetProviderId("Tmdb"))).ToList();
                foreach (var bs in boxSets)
                {
                    int f = _franchiseNames.Count;
                    _franchiseNames.Add(bs.Name ?? "");
                    var ids = _libraryManager.GetInternalItemIds(new InternalItemsQuery { CollectionIds = new[] { bs.InternalId }, IsVirtualItem = false });
                    foreach (var id in ids)
                        if (groupByItem.TryGetValue(id, out var g) && _groups[g].Franchise < 0) _groups[g].Franchise = f;
                }
                franchiseCount = boxSets.Count;
            }
            catch (Exception ex) { _log("Next Watch: franchises could not be read: " + ex.Message); }
            long tFranchise = sw.ElapsedMilliseconds; sw.Restart();

            // Watch-data keys: a film's versions share a key; a show is watched through its episodes.
            var seriesGroup = new Dictionary<long, int>();
            foreach (var g in _groups)
                foreach (var m in g.Members)
                {
                    var key = PopularityCounter.KeyOf(m);
                    if (!string.IsNullOrEmpty(key) && !_groupByKey.ContainsKey(key)) _groupByKey[key] = groupByItem[m.InternalId];
                    if (m is Series) seriesGroup[m.InternalId] = groupByItem[m.InternalId];
                }
            int episodeKeys = 0;
            // Read a few hundred shows at a time: every episode in the library at once held them all
            // as full items (about 400 MB more on a library with 160,000 episodes).
            var seriesIds = seriesGroup.Keys.ToArray();
            for (int i = 0; i < seriesIds.Length; i += EpisodeBatchShows)
            {
                var episodes = _libraryManager.GetItemList(new InternalItemsQuery
                {
                    IncludeItemTypes = new[] { "Episode" },
                    SeriesIds = seriesIds.Skip(i).Take(EpisodeBatchShows).ToArray(),
                    Recursive = true,
                    IsVirtualItem = false
                });
                foreach (var ep in episodes.OfType<Episode>())
                {
                    if (!seriesGroup.TryGetValue(ep.SeriesId, out var g)) continue;
                    var key = PopularityCounter.KeyOf(ep);
                    if (!string.IsNullOrEmpty(key) && !_groupByKey.ContainsKey(key)) { _groupByKey[key] = g; episodeKeys++; }
                }
            }
            long tKeys = sw.ElapsedMilliseconds; sw.Restart();

            // Everyone's watch data (one read per user, shared with the Popular rule).
            var cutoff90 = DateTimeOffset.UtcNow.AddDays(-SeedDays);
            _viewers90 = new int[_groups.Count];
            _viewersAll = new int[_groups.Count];
            int users = 0, records = 0;
            foreach (var user in _userManager.GetUserList(new UserQuery { IsDisabled = false }))
            {
                users++;
                var h = new History();
                foreach (var r in _popularity.RecordsOf(user.InternalId))
                {
                    if (r.Key == null || !_groupByKey.TryGetValue(r.Key, out var g)) continue;
                    records++;
                    h.ByGroup.TryGetValue(g, out var e);
                    // Watched = Emby's Played flag only: PlayCount also goes up on a play stopped at once.
                    bool played = r.Played;
                    if (played) { e.Plays++; if (r.LastPlayedDate.HasValue && (!e.Last.HasValue || r.LastPlayedDate > e.Last)) e.Last = r.LastPlayedDate; }
                    if (r.PlaybackPositionTicks > 0) e.Started = true;
                    if (r.IsFavorite) e.Favourite = true;
                    if (r.Rating.HasValue) e.Rating = Math.Max(e.Rating ?? 0, r.Rating.Value);
                    if (PopularityCounter.Counts(r, cutoff90)) e.Recent = true;
                    h.ByGroup[g] = e;
                }
                foreach (var kv in h.ByGroup)
                {
                    if (kv.Value.Plays > 0) { h.Watched.Add(kv.Key); _viewersAll[kv.Key]++; }
                    if (kv.Value.Recent) _viewers90[kv.Key]++;
                }
                _history[user.InternalId] = h;
            }
            long tUsers = sw.ElapsedMilliseconds;

            _idfGenre = Idf(_genres.Count, g => g.Genres);
            _idfPeople = Idf(_people.Count, g => g.People);
            _idfStudio = Idf(_studios.Count, g => g.Studios);
            _idfTag = Idf(_tags.Count, g => g.Tags);
            _genreShare = new double[_genres.Count];
            foreach (var g in _groups) foreach (var f in g.Genres) _genreShare[f]++;
            for (int i = 0; i < _genreShare.Length; i++) _genreShare[i] /= Math.Max(1, _groups.Count);

PrepareSummary = $"{_groups.Count:N0} titles ({_titles.Count:N0} items), {_genres.Count} genres, {_people.Count:N0} people, {_studios.Count:N0} studios, {_tags.Count:N0} tags, "
                + $"{franchiseCount} franchises, {_groupByKey.Count:N0} watch keys ({episodeKeys:N0} episodes), {users} users / {records:N0} watch records  ·  "
                + $"features {tFeatures} ms, people {tPeople} ms, franchises {tFranchise} ms, keys {tKeys} ms, watch data {tUsers} ms, total {total.ElapsedMilliseconds} ms";
            _log("Next Watch index: " + PrepareSummary);
        }

        private void LoadPeople(Dictionary<long, int> groupByItem)
        {
            var actors = new Dictionary<long, List<string>>();
            var crew = new Dictionary<long, List<string>>();
            var repIds = _groups.Select(g => g.Rep.InternalId).ToArray();
            bool batchWorks = true;
            for (int i = 0; i < repIds.Length && batchWorks; i += 400)
            {
                var chunk = repIds.Skip(i).Take(400).ToArray();
                var chunkSet = new HashSet<long>(chunk);
                List<PersonInfo> a, c;
                try
                {
                    // Top-billed: list order is 0-based, so MaxActors - 1 keeps the first MaxActors.
                    a = _libraryManager.GetItemPeople(new InternalPeopleQuery { ItemIds = chunk, PersonTypes = new[] { PersonType.Actor }, MaxListOrder = MaxActors - 1 }) ?? new List<PersonInfo>();
                    c = _libraryManager.GetItemPeople(new InternalPeopleQuery { ItemIds = chunk, PersonTypes = new[] { PersonType.Director, PersonType.Writer } }) ?? new List<PersonInfo>();
                }
                catch { batchWorks = false; break; }
                // PersonInfo.ItemId must name the title for the batch read to be usable.
                if ((a.Count > 0 && !chunkSet.Contains(a[0].ItemId)) || (c.Count > 0 && !chunkSet.Contains(c[0].ItemId))) { batchWorks = false; break; }
                foreach (var p in a) Add(actors, p.ItemId, p.Name, MaxActors);
                foreach (var p in c) Add(crew, p.ItemId, p.Name, MaxCrew * 2);
            }
            if (!batchWorks)
            {
                actors.Clear(); crew.Clear();
                foreach (var g in _groups)
                {
                    List<PersonInfo> list;
                    try { list = _libraryManager.GetItemPeople(g.Rep) ?? new List<PersonInfo>(); } catch { continue; }
                    foreach (var p in list.Where(p => p.Type == PersonType.Actor)) Add(actors, g.Rep.InternalId, p.Name, MaxActors);
                    foreach (var p in list.Where(p => p.Type == PersonType.Director || p.Type == PersonType.Writer)) Add(crew, g.Rep.InternalId, p.Name, MaxCrew * 2);
                }
            }
            foreach (var g in _groups)
            {
                var names = new List<string>();
                if (actors.TryGetValue(g.Rep.InternalId, out var al)) names.AddRange(al);
                if (crew.TryGetValue(g.Rep.InternalId, out var cl)) names.AddRange(cl);
                g.People = names.Where(n => !string.IsNullOrWhiteSpace(n)).Select(_people.Id).Distinct().ToArray();
            }

            void Add(Dictionary<long, List<string>> d, long itemId, string name, int max)
            {
                if (string.IsNullOrWhiteSpace(name)) return;
                if (!d.TryGetValue(itemId, out var l)) d[itemId] = l = new List<string>();
                if (l.Count < max && !l.Contains(name)) l.Add(name);
            }
        }

        private double[] Idf(int vocab, Func<Group, int[]> features)
        {
            var df = new int[vocab];
            foreach (var g in _groups) foreach (var f in features(g)) df[f]++;
            var idf = new double[vocab];
            double n = Math.Max(1, _groups.Count);
            for (int i = 0; i < vocab; i++) idf[i] = Math.Log(1 + n / (1 + df[i]));
            return idf;
        }

        // ── Per user ──────────────────────────────────────────────────────────────────────────
        /// <param name="allow">Extra filter conditions of the source (null = all).</param>
        /// <param name="seedDays">History window: titles finished in the last this many days are the seed.</param>
        /// <param name="useTaste">Favourites and the user's own ratings count (seed, boosts, "want to watch").</param>
        public NextWatchResult Recommend(User user, int limit, Func<BaseItem, bool>? allow, HashSet<string> blacklist,
            int seedDays = SeedDays, bool useTaste = true)
        {
            Prepare();
            var sw = Stopwatch.StartNew();
            if (limit <= 0) limit = DefaultLimit;
            seedDays = seedDays <= 0 ? SeedDays : Math.Max(MinSeedDays, Math.Min(MaxSeedDays, seedDays));
            var result = new NextWatchResult();
            if (!_history.TryGetValue(user.InternalId, out var hist)) hist = new History();
            if (!useTaste) hist = hist.WithoutTaste();

            // Candidate pool: what this user can open (library access + parental rating).
            var access = AccessFor(user);
            var now = DateTimeOffset.UtcNow;

            // Seed.
            var dated = hist.ByGroup.Where(kv => kv.Value.Plays > 0 && kv.Value.Last.HasValue)
                .OrderByDescending(kv => kv.Value.Last).ToList();
            var recent = dated.Where(kv => kv.Value.Last >= now.AddDays(-seedDays)).ToList();
            // Recent plays first; too few of them (a quiet month) → topped up from older history.
            var seedSource = recent.Count >= SeedMin ? recent.Take(SeedMax).ToList() : dated.Take(SeedFallback).ToList();
            var seed = new Dictionary<int, double>();
            foreach (var kv in seedSource)
            {
                var e = kv.Value;
                if (e.Rating.HasValue && e.Rating.Value > 0 && e.Rating.Value < 5) continue; // rated low: not a taste signal
                double age = (now - e.Last!.Value).TotalDays;
                double w = 0.5 + 0.5 * Math.Exp(-age / 60.0);
                if (_groups[kv.Key].IsSeries) w *= Math.Min(2.0, 1.0 + Math.Log(1 + e.Plays) / 3.0);
                if (e.Favourite) w *= 1.5;
                if (e.Rating >= 7) w *= 1.3;
                seed[kv.Key] = w;
            }
            // Favourites and titles rated 7+ that were watched (any time) are taste too. An unwatched
            // favourite is a "want to watch": a candidate with a boost, not a seed.
            int favourites = 0;
            foreach (var kv in hist.ByGroup.Where(kv => kv.Value.Plays > 0 && (kv.Value.Favourite || kv.Value.Rating >= 7)))
            {
                if (seed.ContainsKey(kv.Key)) continue;
                seed[kv.Key] = 0.8;
                favourites++;
            }
            result.SeedCount = seed.Count;
            result.SeedRecent = recent.Count;

            // Candidates: not watched, not started, one item per film, accessible, allowed.
            var candidates = new List<(int Group, BaseItem Item)>();
            for (int g = 0; g < _groups.Count; g++)
            {
                if (hist.ByGroup.TryGetValue(g, out var e) && (e.Plays > 0 || e.Started)) continue;
                var grp = _groups[g];
                var imdb = grp.Rep.GetProviderId("Imdb");
                if (!string.IsNullOrEmpty(imdb) && blacklist.Contains(imdb)) continue;
                BaseItem? pick = null;
                foreach (var m in grp.Members)
                    if (access.Contains(m.InternalId) && (allow == null || allow(m))) { pick = m; break; }
                if (pick != null) candidates.Add((g, pick));
            }
            result.Candidates = candidates.Count;

            bool cold = seed.Count < MinSeed;
            result.ColdStart = cold;
            var scored = new List<(int Group, BaseItem Item, double Score)>(candidates.Count);
            var weak = new HashSet<int>();   // matched on a single signal only: kept out of the top 10
            _links.Clear();
            _seedIndex = BuildSeedIndex(seed);
            Dictionary<int, double>? pGenre = null, pPeople = null, pStudio = null, pTag = null;
            var cf = new Dictionary<int, double>();

            if (cold)
            {
                foreach (var c in candidates)
                    scored.Add((c.Group, c.Item, _viewersAll[c.Group] + _groups[c.Group].Rating));
            }
            else
            {
                pGenre = Profile(seed, g => g.Genres);
                pPeople = Profile(seed, g => g.People);
                pStudio = Profile(seed, g => g.Studios);
                pTag = Profile(seed, g => g.Tags);
                double wsum = seed.Values.Sum();
                double yMean = 0, yVar = 0, ySum = 0;
                foreach (var kv in seed) if (_groups[kv.Key].Year > 0) { yMean += kv.Value * _groups[kv.Key].Year; ySum += kv.Value; }
                yMean = ySum > 0 ? yMean / ySum : 0;
                foreach (var kv in seed) if (_groups[kv.Key].Year > 0) yVar += kv.Value * Math.Pow(_groups[kv.Key].Year - yMean, 2);
                double ySd = ySum > 0 ? Math.Max(8, Math.Sqrt(yVar / ySum)) : 15;

                // People like you: users whose watched titles overlap the seed most.
                foreach (var other in _history)
                {
                    if (other.Key == user.InternalId || other.Value.Watched.Count == 0) continue;
                    double overlap = 0;
                    foreach (var kv in seed) if (other.Value.Watched.Contains(kv.Key)) overlap += kv.Value;
                    if (overlap <= 0) continue;
                    other.Value.Similarity = overlap / Math.Sqrt(other.Value.Watched.Count + 10);
                }
                foreach (var other in _history.Where(o => o.Key != user.InternalId && o.Value.Similarity > 0)
                             .OrderByDescending(o => o.Value.Similarity).Take(SimilarUsers))
                    foreach (var g in other.Value.Watched)
                        cf[g] = cf.TryGetValue(g, out var v) ? v + other.Value.Similarity : other.Value.Similarity;
                foreach (var o in _history.Values) o.Similarity = 0;

                // Genres count by lift: how much more of the user's watching carries a genre than the
                // library as a whole does (capped), so a few stray episodes of a rare genre don't take over.
                var genreWeight = new double[_genres.Count];
                foreach (var kv in pGenre)
                    if (_genreShare[kv.Key] > 0) genreWeight[kv.Key] = Math.Min(MaxGenreLift, kv.Value / _genreShare[kv.Key]);

                var raw = new List<(int G, BaseItem I, double Ge, double Pe, double St, double Ta, double Yr, double Cf)>(candidates.Count);
                double mGe = 1e-9, mPe = 1e-9, mSt = 1e-9, mTa = 1e-9, mCf = 1e-9;
                foreach (var c in candidates)
                {
                    var grp = _groups[c.Group];
                    double ge = Overlap(pGenre, grp.Genres, genreWeight) / Math.Sqrt(Math.Max(1, grp.Genres.Length));
                    double pe = Overlap(pPeople, grp.People, _idfPeople);
                    double st = Overlap(pStudio, grp.Studios, _idfStudio);
                    double ta = Overlap(pTag, grp.Tags, _idfTag) / Math.Sqrt(Math.Max(1, grp.Tags.Length));
                    double yr = grp.Year > 0 && yMean > 0 ? Math.Exp(-Math.Pow((grp.Year - yMean) / ySd, 2) / 2) : 0.3;
                    double cfv = cf.TryGetValue(c.Group, out var cv) ? cv : 0;
                    raw.Add((c.Group, c.Item, ge, pe, st, ta, yr, cfv));
                    mGe = Math.Max(mGe, ge); mPe = Math.Max(mPe, pe); mSt = Math.Max(mSt, st); mTa = Math.Max(mTa, ta); mCf = Math.Max(mCf, cfv);
                }
                int maxViewers = Math.Max(1, _viewers90.Length > 0 ? _viewers90.Max() : 1);
                var baseScored = new List<(int G, BaseItem I, double S, double Cf)>(raw.Count);
                foreach (var r in raw)
                {
                    var grp = _groups[r.G];
                    bool wanted = hist.ByGroup.TryGetValue(r.G, out var he) && he.Favourite;
                    double s = (wanted ? 0.10 : 0) + 0.26 * r.Ge / mGe + 0.24 * r.Pe / mPe + 0.07 * r.St / mSt + 0.08 * r.Ta / mTa
                             + 0.06 * r.Yr + 0.08 * grp.Rating + 0.17 * r.Cf / mCf + 0.04 * Math.Min(1.0, (double)_viewers90[r.G] / maxViewers);
                    baseScored.Add((r.G, r.I, s, r.Cf / mCf));
                }

                // Match strength against the single seed title it has most in common with: several
                // kinds of shared signal (genres + people + studio/keywords) beat one shared actor or
                // one broad genre; the next/previous film of a franchise is always strong. Worked out
                // for every candidate.
                foreach (var b in baseScored)
                {
                    var link = LinkFor(b.G);
                    double strength = link.Strength + (b.Cf >= 0.5 ? 0.5 : 0); // "people like you" adds a little
                    double s = b.S * (strength >= StrongLink ? 1.0 : strength >= 1.5 ? 0.9 : 0.75)
                        + 0.03 * Math.Min(4.0, strength) + (link.Franchise ? 0.10 : 0);
                    if (strength < StrongLink) weak.Add(b.G);
                    scored.Add((b.G, b.I, s));
                }
            }

            // Best first, with caps for variety.
            var ordered = scored.OrderByDescending(s => s.Score).ThenBy(s => s.Item.SortName, StringComparer.OrdinalIgnoreCase).ToList();
            // Cold start: a different mix of the popular titles per user (and per day), so users with
            // little history don't all get the same list.
            if (cold)
                ordered = ColdShuffle(ordered, s => _viewersAll[s.Group], limit,
                    StableSeed(user.Id.ToString("N") + DateTime.Now.ToString("yyyyMMdd")));
            bool hasMovies = candidates.Any(c => !_groups[c.Group].IsSeries), hasShows = candidates.Any(c => _groups[c.Group].IsSeries);
            double showShare = 0.3;
            if (!cold && seed.Count > 0)
                showShare = seed.Where(kv => _groups[kv.Key].IsSeries).Sum(kv => kv.Value) / seed.Values.Sum();
            showShare = hasMovies && hasShows ? Math.Min(0.6, Math.Max(0.15, showShare)) : (hasShows ? 1 : 0);
            int maxShows = (int)Math.Ceiling(limit * showShare) + 2, maxMovies = limit - (int)Math.Floor(limit * showShare) + 2;
            int maxPerGenre = Math.Max(3, (int)Math.Ceiling(limit * 0.3));
            // Unwatched favourites ("want to watch"): at most ~30% of the list.
            bool IsFav(int g) => hist.ByGroup.TryGetValue(g, out var fe) && fe.Favourite;
            int maxFavourites = Math.Max(1, (int)Math.Round(limit * FavouriteShare));
            int favs = 0;

            var picked = new List<(int Group, BaseItem Item, double Score)>();
            var pickedSet = new HashSet<int>();
            var perFranchise = new Dictionary<int, int>();
            var perGenre = new Dictionary<int, int>();
            int shows = 0, movies = 0;
            for (int pass = 0; pass < 3 && picked.Count < limit; pass++)
            {
                int franchiseCap = pass == 0 ? 1 : pass == 1 ? 2 : 3;
                foreach (var s in ordered)
                {
                    if (picked.Count >= limit) break;
                    if (pickedSet.Contains(s.Group)) continue;
                    var grp = _groups[s.Group];
                    if (grp.Franchise >= 0 && perFranchise.TryGetValue(grp.Franchise, out var fc) && fc >= franchiseCap) continue;
                    bool fav = IsFav(s.Group);
                    if (fav && favs >= maxFavourites) continue;
                    if (pass == 0)
                    {
                        if (grp.MainGenre >= 0 && perGenre.TryGetValue(grp.MainGenre, out var gc) && gc >= maxPerGenre) continue;
                        if (grp.IsSeries ? shows >= maxShows : movies >= maxMovies) continue;
                    }
                    picked.Add(s);
                    pickedSet.Add(s.Group);
                    if (grp.Franchise >= 0) perFranchise[grp.Franchise] = perFranchise.TryGetValue(grp.Franchise, out var f2) ? f2 + 1 : 1;
                    if (grp.MainGenre >= 0) perGenre[grp.MainGenre] = perGenre.TryGetValue(grp.MainGenre, out var g2) ? g2 + 1 : 1;
                    if (grp.IsSeries) shows++; else movies++;
                    if (fav) favs++;
                }
            }
            picked = Arrange(picked, IsFav, weak, cold);

            // Reasons: the seed title with the most in common.
            foreach (var p in picked)
                result.Picks.Add(new NextWatchPick
                {
                    Item = p.Item,
                    Score = p.Score,
                    Reason = hist.ByGroup.TryGetValue(p.Group, out var pe) && pe.Favourite ? "In your favourites — not watched yet"
                        : cold ? ColdReason(p.Group) : Reason(p.Group, seed, cf)
                });

            // Seed summary.
            var topGenres = pGenre == null ? new List<string>() : pGenre.OrderByDescending(kv => kv.Value).Take(4).Select(kv => _genres.Name(kv.Key)).ToList();
            var topPeople = pPeople == null ? new List<string>() : pPeople.OrderByDescending(kv => kv.Value * _idfPeople[kv.Key]).Take(4).Select(kv => _people.Name(kv.Key)).ToList();
            var latest = dated.Take(3).Select(kv => Label(_groups[kv.Key].Rep)).ToList();
            result.SeedSummary = cold
                ? $"Not enough watch history ({hist.Watched.Count} watched) — showing a mix of the server's most watched titles (all time) this user has not seen."
                : $"Based on {seed.Count} titles ({recent.Count} watched in the last {seedDays} days{(recent.Count >= SeedMin ? "" : "; topped up with older history")}{(favourites > 0 ? $", {favourites} older favourites/rated" : "")})."
                  + (latest.Count > 0 ? " Latest: " + string.Join(", ", latest) + "." : "")
                  + (topGenres.Count > 0 ? " Top genres: " + string.Join(", ", topGenres) + "." : "")
                  + (topPeople.Count > 0 ? " People: " + string.Join(", ", topPeople) + "." : "");
            result.ElapsedMs = sw.ElapsedMilliseconds;
            return result;
        }

        // Cold-start order: the top ColdPoolFactor x limit popular titles (all-time viewers) are drawn in a per-user, per-day random order weighted by viewers (weighted sampling
        // without replacement), so the most watched titles are still usually near the top; titles
        // outside that pool follow in their normal order.
        internal static List<T> ColdShuffle<T>(List<T> ordered, Func<T, int> viewers, int limit, int seed)
        {
            var pool = ordered.Where(o => viewers(o) > 0).Take(Math.Max(1, limit) * ColdPoolFactor).ToList();
            if (pool.Count < 2) return ordered;
            var rng = new Random(seed);
            var drawn = pool.Select(o => (Item: o, Key: -Math.Log(1.0 - rng.NextDouble()) / viewers(o)))
                .OrderBy(x => x.Key).Select(x => x.Item).ToList();
            var inPool = new HashSet<T>(pool);
            drawn.AddRange(ordered.Where(o => !inPool.Contains(o)));
            return drawn;
        }

        // String hash that is the same in every run (string.GetHashCode is not).
        private static int StableSeed(string s)
        {
            unchecked
            {
                uint h = 2166136261;
                foreach (char c in s) { h ^= c; h *= 16777619; }
                return (int)h;
            }
        }

        // Final order: best first, but favourites spread out (at most one in any three places and at
        // most MaxFavouritesTop10 in the top 10) and no weak single-signal match in the top 10 while
        // a stronger pick is left.
        private static List<(int Group, BaseItem Item, double Score)> Arrange(List<(int Group, BaseItem Item, double Score)> picked,
            Func<int, bool> isFav, HashSet<int> weak, bool cold)
        {
            var favs = picked.Where(p => isFav(p.Group)).ToList();
            var others = picked.Where(p => !isFav(p.Group)).ToList();
            var final = new List<(int Group, BaseItem Item, double Score)>(picked.Count);
            int lastFav = -FavouriteGap, favTop = 0;
            while (favs.Count > 0 || others.Count > 0)
            {
                int pos = final.Count;
                int oi = -1;
                if (others.Count > 0)
                {
                    oi = 0;
                    if (pos < 10 && !cold)
                    {
                        int k = others.FindIndex(o => !weak.Contains(o.Group));
                        if (k >= 0) oi = k;
                    }
                }
                bool favOk = favs.Count > 0 && pos - lastFav >= FavouriteGap && (pos >= 10 || favTop < MaxFavouritesTop10);
                bool takeFav = favs.Count > 0 && (oi < 0 || (favOk && favs[0].Score >= others[oi].Score));
                if (takeFav)
                {
                    final.Add(favs[0]); favs.RemoveAt(0);
                    lastFav = pos;
                    if (pos < 10) favTop++;
                }
                else { final.Add(others[oi]); others.RemoveAt(oi); }
            }
            return final;
        }

        // ── Match strength between a candidate and one seed title ───────────────────────────────
        private const double StrongLink = 2.0, RareShare = 0.05, FavouriteShare = 0.3, MaxGenreLift = 3.0;
        private const int MaxFavouritesTop10 = 3, FavouriteGap = 3, ColdPoolFactor = 3;

        private struct Link
        {
            public int Seed;
            public double Strength;
            public bool Franchise;
        }
        private readonly Dictionary<int, Link> _links = new Dictionary<int, Link>();
        private SeedIndex? _seedIndex;

        // Per user: which seed titles carry each feature, so a candidate's best seed match is found by
        // looking up its own features instead of comparing it with every seed title one by one.
        private sealed class SeedIndex
        {
            public int[] Seeds = Array.Empty<int>();
            public double[] Weights = Array.Empty<double>();
            // Feature id → positions in Seeds of the seed titles that have it (null = none).
            public List<int>?[] People = Array.Empty<List<int>?>(), Genres = Array.Empty<List<int>?>(), Studios = Array.Empty<List<int>?>(),
                Tags = Array.Empty<List<int>?>(), Franchises = Array.Empty<List<int>?>();
            public int[] PeopleN = Array.Empty<int>(), GenreN = Array.Empty<int>(), StudioN = Array.Empty<int>(), TagN = Array.Empty<int>();
            public bool[] RareGenre = Array.Empty<bool>(), RareStudio = Array.Empty<bool>(), SameFranchise = Array.Empty<bool>();
        }

        private SeedIndex BuildSeedIndex(Dictionary<int, double> seed)
        {
            var ix = new SeedIndex { Seeds = seed.Keys.ToArray(), Weights = seed.Values.ToArray() };
            int n = ix.Seeds.Length;
            ix.PeopleN = new int[n]; ix.GenreN = new int[n]; ix.StudioN = new int[n]; ix.TagN = new int[n];
            ix.RareGenre = new bool[n]; ix.RareStudio = new bool[n]; ix.SameFranchise = new bool[n];
            ix.People = new List<int>?[_people.Count]; ix.Genres = new List<int>?[_genres.Count]; ix.Studios = new List<int>?[_studios.Count];
            ix.Tags = new List<int>?[_tags.Count]; ix.Franchises = new List<int>?[_franchiseNames.Count];
            for (int i = 0; i < n; i++)
            {
                var s = _groups[ix.Seeds[i]];
                Index(ix.People, s.People, i); Index(ix.Genres, s.Genres, i); Index(ix.Studios, s.Studios, i); Index(ix.Tags, s.Tags, i);
                if (s.Franchise >= 0) Index(ix.Franchises, new[] { s.Franchise }, i);
            }
            return ix;

            static void Index(List<int>?[] d, int[] features, int i)
            {
                foreach (var f in features) (d[f] ??= new List<int>()).Add(i);
            }
        }

        private Link LinkFor(int g)
        {
            if (_links.TryGetValue(g, out var cached)) return cached;
            var ix = _seedIndex!;
            var c = _groups[g];
            double rareIdf = Math.Log(1 + 1 / RareShare);
            Count(ix.People, c.People, ix.PeopleN, null, null);
            Count(ix.Genres, c.Genres, ix.GenreN, ix.RareGenre, _idfGenre);
            Count(ix.Studios, c.Studios, ix.StudioN, ix.RareStudio, _idfStudio);
            Count(ix.Tags, c.Tags, ix.TagN, null, null);
            var fl = c.Franchise >= 0 ? ix.Franchises[c.Franchise] : null;
            if (fl != null) foreach (var i in fl) ix.SameFranchise[i] = true;

            var best = new Link { Seed = -1 };
            double bestKey = 0;
            for (int i = 0; i < ix.Seeds.Length; i++)
            {
                if (ix.Seeds[i] == g) continue;
                int people = ix.PeopleN[i], genres = ix.GenreN[i], studios = ix.StudioN[i], tags = ix.TagN[i];
                bool franchise = ix.SameFranchise[i];
                // One kind of signal ≈ 1: one shared actor or one broad genre is half of that.
                double strength = (people >= 3 ? 1.5 : people == 2 ? 1.0 : people * 0.5)
                    + (genres >= 2 || ix.RareGenre[i] ? 1.0 : genres * 0.5)
                    + (ix.RareStudio[i] ? 1.0 : studios > 0 ? 0.5 : 0)
                    + (tags >= 2 ? 1.0 : tags * 0.5)
                    + (franchise ? 2.0 : 0);
                double key = strength + 0.1 * Math.Min(2.0, ix.Weights[i]);
                if (key <= bestKey) continue;
                bestKey = key;
                best = new Link { Seed = ix.Seeds[i], Strength = strength, Franchise = franchise };
            }

            // Reset the counters for the next candidate.
            int len = ix.Seeds.Length;
            Array.Clear(ix.PeopleN, 0, len); Array.Clear(ix.GenreN, 0, len); Array.Clear(ix.StudioN, 0, len); Array.Clear(ix.TagN, 0, len);
            Array.Clear(ix.RareGenre, 0, len); Array.Clear(ix.RareStudio, 0, len); Array.Clear(ix.SameFranchise, 0, len);
            _links[g] = best;
            return best;

            void Count(List<int>?[] d, int[] features, int[] n, bool[]? rare, double[]? idf)
            {
                foreach (var f in features)
                {
                    var l = d[f];
                    if (l == null) continue;
                    bool isRare = idf != null && idf[f] >= rareIdf;
                    foreach (var i in l) { n[i]++; if (isRare) rare![i] = true; }
                }
            }
        }

        private static int[] Shared(int[] a, int[] b)
        {
            if (a.Length == 0 || b.Length == 0) return Array.Empty<int>();
            List<int>? r = null;
            foreach (var x in a) if (Array.IndexOf(b, x) >= 0) (r ??= new List<int>()).Add(x);
            return r == null ? Array.Empty<int>() : r.ToArray();
        }

        // Emby turns a show added to a playlist into all its episodes, so a playlist gets the show's
        // first episode (season 1 onwards; specials last) instead. Same for every user — cached.
        private readonly Dictionary<long, BaseItem?> _firstEpisode = new Dictionary<long, BaseItem?>();
        public BaseItem? FirstEpisode(BaseItem series)
        {
            if (_firstEpisode.TryGetValue(series.InternalId, out var ep)) return ep;
            ep = _libraryManager.GetItemList(new InternalItemsQuery
                {
                    IncludeItemTypes = new[] { "Episode" },
                    SeriesIds = new[] { series.InternalId },
                    Recursive = true,
                    IsVirtualItem = false
                })
                .OrderBy(e => (e.ParentIndexNumber ?? 0) > 0 ? e.ParentIndexNumber!.Value : int.MaxValue)
                .ThenBy(e => e.IndexNumber ?? int.MaxValue)
                .FirstOrDefault();
            _firstEpisode[series.InternalId] = ep;
            return ep;
        }

        private HashSet<long> AccessFor(User user)
        {
            var key = PolicyKey(user);
            if (_accessByPolicy.TryGetValue(key, out var set)) return set;
            var ids = _libraryManager.GetInternalItemIds(new InternalItemsQuery(user)
            {
                IncludeItemTypes = new[] { "Movie", "Series" },
                Recursive = true,
                IsVirtualItem = false
            });
            set = new HashSet<long>(ids ?? Array.Empty<long>());
            _accessByPolicy[key] = set;
            return set;
        }

        // Users with the same library access and parental settings see the same items.
        private static string PolicyKey(User user)
        {
            var p = user.Policy;
            if (p == null) return "user:" + user.Id;
            string J(IEnumerable<string>? a) => a == null ? "" : string.Join(",", a.OrderBy(x => x, StringComparer.OrdinalIgnoreCase));
            return string.Join("|", p.EnableAllFolders, J(p.EnabledFolders), J(p.ExcludedSubFolders), p.MaxParentalRating,
                p.BlockUnratedItems == null ? "" : string.Join(",", p.BlockUnratedItems.Select(x => x.ToString()).OrderBy(x => x)),
                J(p.BlockedTags), J(p.IncludeTags), p.IsTagBlockingModeInclusive, p.AllowTagOrRating);
        }

        private Dictionary<int, double> Profile(Dictionary<int, double> seed, Func<Group, int[]> features)
        {
            var p = new Dictionary<int, double>();
            double total = seed.Values.Sum();
            foreach (var kv in seed)
                foreach (var f in features(_groups[kv.Key]))
                    p[f] = (p.TryGetValue(f, out var v) ? v : 0) + kv.Value / total;
            return p;
        }

        private static double Overlap(Dictionary<int, double> profile, int[] features, double[] idf)
        {
            double s = 0;
            foreach (var f in features) if (profile.TryGetValue(f, out var w)) s += w * idf[f];
            return s;
        }

        private string Reason(int g, Dictionary<int, double> seed, Dictionary<int, double> cf)
        {
            var link = LinkFor(g);
            if (link.Seed < 0 || link.Strength < 1)
                return cf.ContainsKey(g) ? "Watched by people with your taste" : "Highly rated";
            var c = _groups[g];
            var s = _groups[link.Seed];
            var shared = new List<string>();
            if (link.Franchise) shared.Add(_franchiseNames[c.Franchise]);
            shared.AddRange(Shared(c.People, s.People).Take(2).Select(_people.Name));
            shared.AddRange(Shared(c.Genres, s.Genres).Take(2).Select(_genres.Name));
            shared.AddRange(Shared(c.Studios, s.Studios).Take(1).Select(_studios.Name));
            return "Because you watched " + Label(_groups[link.Seed].Rep) + (shared.Count > 0 ? " · " + string.Join(", ", shared.Take(4)) : "");
        }

        private string ColdReason(int g) => _viewersAll[g] > 0
            ? $"Popular on this server ({_viewersAll[g]} viewer{(_viewersAll[g] == 1 ? "" : "s")} of all time)"
            : "Highly rated";

        private static string Label(BaseItem i) => (i.Name ?? "") + (i.ProductionYear.HasValue ? $" ({i.ProductionYear})" : "");

        private static string FilmKey(BaseItem item)
        {
            var imdb = item.GetProviderId("Imdb");
            if (!string.IsNullOrEmpty(imdb)) return "imdb:" + imdb;
            var tmdb = item.GetProviderId("Tmdb");
            if (!string.IsNullOrEmpty(tmdb)) return (item is Series ? "tmdbtv:" : "tmdb:") + tmdb;
            if (!string.IsNullOrEmpty(item.PresentationUniqueKey)) return "puk:" + item.PresentationUniqueKey;
            return "id:" + item.InternalId;
        }

        private sealed class Group
        {
            public BaseItem Rep = null!;
            public List<BaseItem> Members = new List<BaseItem>();
            public bool IsSeries;
            public int[] Genres = Array.Empty<int>(), People = Array.Empty<int>(), Studios = Array.Empty<int>(), Tags = Array.Empty<int>();
            public int MainGenre = -1, Franchise = -1, Year;
            public double Rating;
        }

        private struct Entry
        {
            public int Plays;
            public DateTimeOffset? Last;
            public bool Started, Favourite, Recent;
            public double? Rating;
        }

        private sealed class History
        {
            public readonly Dictionary<int, Entry> ByGroup = new Dictionary<int, Entry>();
            public readonly HashSet<int> Watched = new HashSet<int>();
            public double Similarity;

            // The same history with favourite marks and ratings left out (source setting).
            public History WithoutTaste()
            {
                var h = new History();
                foreach (var kv in ByGroup) { var e = kv.Value; e.Favourite = false; e.Rating = null; h.ByGroup[kv.Key] = e; }
                h.Watched.UnionWith(Watched);
                return h;
            }
        }

        private sealed class Vocab
        {
            private readonly Dictionary<string, int> _ids = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            private readonly List<string> _names = new List<string>();
            public int Count => _names.Count;
            public int Id(string name)
            {
                name = name.Trim();
                if (!_ids.TryGetValue(name, out var id)) { id = _names.Count; _ids[name] = id; _names.Add(name); }
                return id;
            }
            public string Name(int id) => id >= 0 && id < _names.Count ? _names[id] : "";
        }
    }

    internal sealed class NextWatchPick
    {
        public BaseItem Item = null!;
        public double Score;
        public string Reason = "";
    }

    internal sealed class NextWatchResult
    {
        public List<NextWatchPick> Picks { get; } = new List<NextWatchPick>();
        public string SeedSummary = "";
        public int SeedCount, SeedRecent, Candidates;
        public bool ColdStart;
        public long ElapsedMs;
    }
}
