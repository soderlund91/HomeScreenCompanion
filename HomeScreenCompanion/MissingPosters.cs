using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MediaBrowser.Common.Net;
using MediaBrowser.Controller.Entities.Movies;
using MediaBrowser.Controller.Entities.TV;
using MediaBrowser.Controller.Providers;
using MediaBrowser.Model.Entities;
using MediaBrowser.Model.Providers;
using MediaBrowser.Model.Serialization;

namespace HomeScreenCompanion
{
    /// <summary>
    /// Posters, year and type of list titles that are not in the library, for the source test popup.
    /// With a TMDb key in Settings they are asked of TMDb (find by IMDb id, else the movie / tv page of
    /// the TMDb id); without one, Emby's own metadata search (the one Identify uses) is asked by the
    /// title's ids. The browser only gets the image URL, never the key. Answers are kept in memory
    /// for a while, so a second test of the same list asks nothing.
    /// </summary>
    public static class MissingPosters
    {
        private const string ImageBase = "https://image.tmdb.org/t/p/";
        private const int MaxPerRequest = 50, MaxCached = 5000;
        private static readonly TimeSpan KeepFound = TimeSpan.FromDays(7), KeepNotFound = TimeSpan.FromHours(12);
        private static readonly ConcurrentDictionary<string, (Result Result, DateTime Utc)> Cache =
            new ConcurrentDictionary<string, (Result, DateTime)>(StringComparer.OrdinalIgnoreCase);

        public sealed class Title
        {
            public string Imdb { get; set; } = "";
            public string Tmdb { get; set; } = "";
            public string Type { get; set; } = "";   // "movie", "show" or "" (unknown)
            public string Name { get; set; } = "";
        }

        public sealed class Result
        {
            public string Imdb { get; set; } = "";
            public string Tmdb { get; set; } = "";
            public string ImageUrl { get; set; } = "";   // "" = none found
            public int? Year { get; set; }
            public string Type { get; set; } = "";       // "movie", "show" or ""
        }

        public sealed class TmdbTitle
        {
            public int id { get; set; }
            public string? poster_path { get; set; }
            public string? release_date { get; set; }
            public string? first_air_date { get; set; }
        }

        public sealed class TmdbFind
        {
            public List<TmdbTitle>? movie_results { get; set; }
            public List<TmdbTitle>? tv_results { get; set; }
        }

        private static string Clean(string? s) => (s ?? "").Trim();

        private static string CacheKey(Title t)
        {
            var imdb = Clean(t.Imdb);
            if (imdb.Length > 0) return "imdb:" + imdb;
            var tmdb = Clean(t.Tmdb);
            return tmdb.Length > 0 ? "tmdb:" + Clean(t.Type) + ":" + tmdb : "";
        }

        internal static async Task<List<Result>> LookUpAsync(List<Title> titles, string apiKey, IProviderManager providers,
            IHttpClient http, IJsonSerializer json, CancellationToken ct)
        {
            var asked = titles.Where(t => t != null && CacheKey(t).Length > 0)
                .GroupBy(CacheKey, StringComparer.OrdinalIgnoreCase).Select(g => g.First()).Take(MaxPerRequest).ToList();
            var gate = new SemaphoreSlim(4);
            var results = await Task.WhenAll(asked.Select(async t =>
            {
                var key = CacheKey(t);
                if (Cache.TryGetValue(key, out var hit) && DateTime.UtcNow - hit.Utc < (hit.Result.ImageUrl.Length > 0 ? KeepFound : KeepNotFound))
                    return hit.Result;
                await gate.WaitAsync(ct).ConfigureAwait(false);
                try
                {
                    var r = apiKey.Length > 0 ? await AskTmdbAsync(t, apiKey, http, json, ct).ConfigureAwait(false)
                                              : await AskEmbyAsync(t, providers, ct).ConfigureAwait(false);
                    if (r == null) return new Result { Imdb = Clean(t.Imdb), Tmdb = Clean(t.Tmdb) };   // failed: asked again next time
                    if (Cache.Count >= MaxCached) Cache.Clear();
                    Cache[key] = (r, DateTime.UtcNow);
                    return r;
                }
                finally { gate.Release(); }
            })).ConfigureAwait(false);
            return results.ToList();
        }

        private static int? YearOf(string? date) =>
            date != null && date.Length >= 4 && int.TryParse(date.Substring(0, 4), out var y) && y > 1800 ? y : (int?)null;

        private static Result FromTmdb(Title t, TmdbTitle x, bool show) => new Result
        {
            Imdb = Clean(t.Imdb),
            Tmdb = Clean(t.Tmdb).Length > 0 ? Clean(t.Tmdb) : x.id.ToString(),
            ImageUrl = string.IsNullOrEmpty(x.poster_path) ? "" : ImageBase + "w185" + x.poster_path,
            Year = YearOf(show ? x.first_air_date : x.release_date),
            Type = show ? "show" : "movie"
        };

        private static bool IsNotFound(Exception ex)
        {
            try { var code = ex.GetType().GetProperty("StatusCode")?.GetValue(ex); return code != null && Convert.ToInt32(code) == 404; }
            catch { return false; }
        }

        // The answer; one with no ImageUrl when TMDb does not know the title, null when it could not be asked.
        private static async Task<Result?> AskTmdbAsync(Title t, string apiKey, IHttpClient http, IJsonSerializer json, CancellationToken ct)
        {
            var imdb = Clean(t.Imdb);
            var tmdb = Clean(t.Tmdb);
            bool? show = t.Type == "show" ? true : t.Type == "movie" ? false : (bool?)null;
            try
            {
                if (imdb.Length > 0)
                {
                    var find = await MissingTitles.TmdbGet<TmdbFind>($"find/{Uri.EscapeDataString(imdb)}?external_source=imdb_id", apiKey, http, json, ct).ConfigureAwait(false);
                    var movie = find?.movie_results?.FirstOrDefault();
                    var tv = find?.tv_results?.FirstOrDefault();
                    // The list's own TMDb id, when it has one, picks between a movie and a show with the same IMDb id.
                    if (movie != null && tv != null && tmdb.Length > 0)
                    {
                        if (tv.id.ToString() == tmdb && movie.id.ToString() != tmdb) movie = null;
                        else if (movie.id.ToString() == tmdb) tv = null;
                    }
                    if (tv != null && (movie == null || show == true)) return FromTmdb(t, tv, true);
                    if (movie != null) return FromTmdb(t, movie, false);
                }
                if (tmdb.Length > 0)
                {
                    foreach (var asShow in show.HasValue ? new[] { show.Value } : new[] { false, true })
                    {
                        try
                        {
                            var x = await MissingTitles.TmdbGet<TmdbTitle>((asShow ? "tv/" : "movie/") + tmdb, apiKey, http, json, ct).ConfigureAwait(false);
                            if (x != null) return FromTmdb(t, x, asShow);
                        }
                        catch (Exception ex) when (IsNotFound(ex)) { }
                    }
                }
                return new Result { Imdb = imdb, Tmdb = tmdb };
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { throw; }
            catch (Exception ex)
            {
                if (IsNotFound(ex)) return new Result { Imdb = imdb, Tmdb = tmdb };
                PluginLog.Swallowed(ex);
                return null;
            }
        }

        // Emby's metadata search by the title's ids, as a movie and/or a show. Null when it could not be asked.
        private static async Task<Result?> AskEmbyAsync(Title t, IProviderManager providers, CancellationToken ct)
        {
            var imdb = Clean(t.Imdb);
            var tmdb = Clean(t.Tmdb);
            var ids = new ProviderIdDictionary();
            if (imdb.Length > 0) ids[MetadataProviders.Imdb.ToString()] = imdb;
            if (tmdb.Length > 0) ids[MetadataProviders.Tmdb.ToString()] = tmdb;
            if (ids.Count == 0) return new Result();

            // A metadata plugin may answer with titles that are not the one asked for: none whose ids differ.
            bool Agrees(RemoteSearchResult r) => r.ProviderIds == null || ids.All(a =>
                !r.ProviderIds.TryGetValue(a.Key, out var theirs) || string.IsNullOrWhiteSpace(theirs) || string.Equals(theirs.Trim(), a.Value, StringComparison.OrdinalIgnoreCase));

            try
            {
                foreach (var asShow in t.Type == "show" ? new[] { true } : t.Type == "movie" ? new[] { false } : new[] { false, true })
                {
                    IEnumerable<RemoteSearchResult> results = asShow
                        ? await providers.GetRemoteSearchResults<Series, SeriesInfo>(
                            new RemoteSearchQuery<SeriesInfo> { SearchInfo = new SeriesInfo { ProviderIds = ids }, IncludeDisabledProviders = true }, ct).ConfigureAwait(false)
                        : await providers.GetRemoteSearchResults<Movie, MovieInfo>(
                            new RemoteSearchQuery<MovieInfo> { SearchInfo = new MovieInfo { ProviderIds = ids }, IncludeDisabledProviders = true }, ct).ConfigureAwait(false);
                    var hit = (results ?? Enumerable.Empty<RemoteSearchResult>()).Where(Agrees).FirstOrDefault();
                    if (hit == null) continue;
                    var url = hit.ImageUrl ?? "";
                    // TheMovieDb gives the original size: the popup needs far less.
                    const string original = ImageBase + "original/";
                    if (url.StartsWith(original, StringComparison.OrdinalIgnoreCase)) url = ImageBase + "w185/" + url.Substring(original.Length);
                    return new Result
                    {
                        Imdb = imdb, Tmdb = tmdb, ImageUrl = url,
                        Year = hit.ProductionYear > 0 ? hit.ProductionYear : (int?)null,
                        Type = asShow ? "show" : "movie"
                    };
                }
                return new Result { Imdb = imdb, Tmdb = tmdb };
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { throw; }
            catch (Exception ex)
            {
                PluginLog.Swallowed(ex);
                return null;
            }
        }
    }
}
