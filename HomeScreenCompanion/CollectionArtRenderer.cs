using MediaBrowser.Model.Entities;
using SkiaSharp;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using static HomeScreenCompanion.ArtCanvas;

namespace HomeScreenCompanion
{
    /// <summary>
    /// Generated collection art, drawn from the posters of the collection's titles (in list order).
    /// Poster = 1000x1500, background = 1920x1080. Styles (TagConfig.CollectionPosterStyle /
    /// CollectionBackgroundStyle). Fan, wall, hero strip, spotlight and ranked follow the mockups
    /// (stoep-shelves/docs/art-mockups/mock.py); grid follows collectra/artkit:
    ///   collage     — the Collectra/Collectify collage: posters edge to edge in a grid (spare cells
    ///                 filled with repeats, never beside the same poster); the poster gets the name in
    ///                 yellow Roboto Bold on a translucent box at the bottom, the background no text.
    ///   grid        — an even grid of rounded poster cards over a blurred backdrop, title below.
    ///   fan         — cards fanned out like a hand of playing cards, title underneath.
    ///   wall        — a tilted, brick-offset wall of posters with a title band (posters repeat).
    ///   ranked      — numbered Top 10 cards (big outlined numerals with a poster against each).
    ///   hero_strip  — the first poster as a soft hero, the title, then a strip of cards.
    ///   spotlight   — background: text panel left, first poster bleeding in on the right
    ///                 (as a poster it renders hero_strip).
    /// </summary>
    internal static class CollectionArtRenderer
    {
        public const string Collage = "collage", Grid = "grid", Fan = "fan", Wall = "wall", HeroStrip = "hero_strip", Spotlight = "spotlight", Ranked = "ranked";
        public static readonly string[] Styles = { Collage, Grid, Fan, Wall, HeroStrip, Spotlight, Ranked };

        // How many titles a style can show (the most it draws from).
        public static int PostersFor(string style, bool background, ArtOptions? opts = null)
        {
            var st = (style ?? "").Trim().ToLowerInvariant();
            if (opts?.Posters != null && st != Ranked) return Math.Min(MaxPosters, opts.Posters.Value);
            // Rows chosen but no count: the style's own count rounded up to full rows.
            if (opts?.Rows != null)
            {
                // Rows set, Posters on Auto: the rows filled across the width (the same sums the
                // drawing does; with a title assumed, which never asks for fewer cards).
                int r = opts.Rows.Value;
                if (st == Fan) return r >= 3 ? r * FanPerRow(r, background, background ? BackgroundW : PosterW, background ? BackgroundH : PosterH)
                                             : RowsCount(background ? 7 : 5, r, background);
                if (st == HeroStrip || (st == Spotlight && !background)) return 1 + HeroRowsCount(r, background);
                if (st == Spotlight) return 1 + SpotlightRowsCount(r);
                if (st == Grid || !IsStyle(st)) return GridRowsCount(opts, background, TitleAssumed(opts));
                if (r >= 4 && st == Collage)
                {
                    int w = background ? BackgroundW : PosterW, h = background ? BackgroundH : PosterH;
                    return Math.Min(AutoMaxPosters, Math.Max(background ? 40 : 9, r * CollageCols(r, w, h)));
                }
            }
            switch (st)
            {
                case Collage: return background ? 40 : 9;
                case Wall: return 20;
                case Ranked: return background ? 5 : 4;
                case Fan: return background ? 7 : 5;
                case HeroStrip: return background ? 7 : 6;
                case Spotlight: return background ? 5 : 6;
                default: return background ? 14 : 12;
            }
        }

        // Posters: the most a custom count may ask for; the counts worked out for Rows on Auto stop at 100.
        public const int MaxPosters = 250, AutoMaxPosters = 100, MaxRows = 10;

        // The smallest multiple of rows that is at least count (5 cards on 2 rows -> 6).
        private static int FullRowsCount(int count, int rows) => rows <= 1 ? count : rows * (int)Math.Ceiling(count / (double)rows);

        // The count for Rows set and Posters on Auto: the style's own count rounded up to full
        // rows; from 4 rows on at least 3 a row (4 on a background) so a row never shrinks to
        // one or two lonely cards.
        private static int RowsCount(int own, int rows, bool background) =>
            rows <= 3 ? FullRowsCount(own, rows) : rows * Math.Max((int)Math.Ceiling(own / (double)rows), background ? 4 : 3);

        // Collage columns for that many rows: cells about a poster's shape.
        private static int CollageCols(int rows, int w, int h) => Math.Max(1, (int)Math.Round(w / (h / (double)rows / PosterRatio)));

        // How many cards to draw: the asked count when Posters is set (laid out as evenly as
        // possible; repeats fill in when the source has fewer titles), else the available
        // titles up to the wanted count, trimmed to full rows.
        private static int CardsToUse(int available, int wanted, int rows, bool exact)
        {
            if (exact) return Math.Max(0, wanted);
            int n = Math.Min(available, wanted);
            if (!exact && rows > 1 && n >= rows) n -= n % rows;
            return Math.Max(0, n);
        }

        // The cards for rows of the given lengths (each row centred): the posters in order, and
        // when there are fewer posters than cards, repeats that never touch a copy of themselves
        // (beside, above, below or diagonal) and sit as far from their copies as they can.
        private static List<SKBitmap> Spread(List<SKBitmap> posters, int[] counts)
        {
            int total = counts.Sum();
            if (posters.Count >= total || posters.Count == 0) return posters.Take(total).ToList();
            var picks = SpreadPicks(posters.Count, counts);
            return picks.Select(i => posters[i]).ToList();
        }

        internal static int[] SpreadPicks(int available, int[] counts)
        {
            var cells = new List<(int Row, double X)>();
            for (int r = 0; r < counts.Length; r++)
                for (int j = 0; j < counts[r]; j++) cells.Add((r, j - (counts[r] - 1) / 2.0));
            var picks = new int[cells.Count];
            var rng = new Random(available * 7919 + cells.Count);
            for (int i = 0; i < cells.Count; i++)
            {
                if (i < available) { picks[i] = i; continue; }
                var (r0, x0) = cells[i];
                double Dist(int j) => Math.Max(Math.Abs(cells[j].Row - r0), Math.Abs(cells[j].X - x0));
                var touching = new HashSet<int>();
                for (int j = 0; j < i; j++) if (Dist(j) <= 1.01) touching.Add(picks[j]);
                var cands = Enumerable.Range(0, available).Where(p => !touching.Contains(p)).ToList();
                if (cands.Count == 0) cands = Enumerable.Range(0, available).ToList();
                double Far(int p) { double d = double.MaxValue; for (int j = 0; j < i; j++) if (picks[j] == p) d = Math.Min(d, Dist(j)); return d; }
                double far = cands.Max(Far);
                var best = cands.Where(p => Far(p) >= far - 1e-9).ToList();
                picks[i] = best[rng.Next(best.Count)];
            }
            return picks;
        }

        // count cards over rows as evenly as possible, the fuller rows first (10 on 3 -> 4/3/3).
        private static int[] EvenRows(int count, int rows)
        {
            rows = Math.Max(1, Math.Min(rows, Math.Max(1, count)));
            var a = new int[rows];
            for (int i = 0; i < rows; i++) a[i] = count / rows + (i < count % rows ? 1 : 0);
            return a;
        }

        // Rows for a strip of n cards when Posters is set but Rows is auto: one row up to perRow.
        private static int AutoStripRows(int n, int perRow) => Math.Min(MaxRows, Math.Max(1, (int)Math.Ceiling(n / (double)perRow)));

        // Card width for rows of the given lengths in an availW x availH box (at most maxCw).
        private static double RowsCardW(int[] counts, double availW, double availH, double gap, double maxCw = double.MaxValue)
        {
            int most = counts.Max(), rows = counts.Length;
            return Math.Min(maxCw, Math.Min((availW - gap * (most - 1)) / most, (availH - gap * (rows - 1)) / rows / PosterRatio));
        }

        // Cards a row holds across availW when rows rows share availH: the cards as tall as the
        // rows allow, and as many of them as fill the width (rounded, so the last few pixels
        // are taken by cards a little narrower rather than left empty).
        private static int FitPerRow(int rows, double availW, double availH, double gap, double maxCw = double.MaxValue)
        {
            double cw = Math.Min(maxCw, (availH - gap * (rows - 1)) / rows / PosterRatio);
            if (cw <= 1) return 1;
            int perRow = Math.Max(1, (int)Math.Round((availW + gap) / (cw + gap)));
            return Math.Max(1, Math.Min(perRow, AutoMaxPosters / Math.Max(1, rows)));
        }

        // Rows (1..10) that give n evenly spread cards the biggest size in availW x availH.
        private static int BestRows(int n, double availW, double availH, double gap, double maxCw = double.MaxValue, int maxRows = MaxRows)
        {
            int best = 1; double bestW = -1;
            for (int r = 1; r <= Math.Min(Math.Max(1, n), Math.Max(1, maxRows)); r++)
            {
                double cw = RowsCardW(EvenRows(n, r), availW, availH, gap, maxCw);
                if (cw > bestW + 0.5) { bestW = cw; best = r; }
            }
            return best;
        }

        // ── layout boxes shared by PostersFor (how many titles to fetch) and the drawing ──
        private static bool TitleAssumed(ArtOptions? o) => o?.ShowTitle != false;

        // Grid: the box the cards go in (above the title band).
        private static SKRect GridRegion(bool background, int w, int h, bool hasTitle, float scale)
        {
            int m = background ? 72 : 64;
            int titleH = hasTitle ? (int)((background ? 200 : 300) * scale) : 0;
            int titleGap = hasTitle ? (background ? 30 : 40) : 0;
            return SKRect.Create(m, m, w - 2 * m, h - 2 * m - titleH - titleGap);
        }

        private static int GridGap(ArtOptions o) => (o.Posters ?? 0) > 20 || (o.Rows ?? 0) > 5 ? 10 : 22;

        // Grid with Rows set and Posters on Auto: rows x as many cards as fill the width (never
        // fewer than the style's own count gave before).
        private static int GridRowsCount(ArtOptions o, bool background, bool hasTitle)
        {
            int r = o.Rows!.Value, w = background ? BackgroundW : PosterW, h = background ? BackgroundH : PosterH;
            int own = r >= 4 ? RowsCount(background ? 14 : 12, r, background) : background ? 14 : 12;
            var region = GridRegion(background, w, h, hasTitle, o.SizeFactor);
            return Math.Min(AutoMaxPosters, Math.Max(own, r * FitPerRow(r, region.Width, region.Height, GridGap(o))));
        }

        // Hero strip poster: the strip's box (full width less the margins, at most 56% high).
        private static (float W, float H, float Gap) HeroPosterBox(int w, int h)
        {
            float s = MockScale(false, w);
            return (w - 2 * 30 * s, h * 0.56f, 12 * s);
        }

        // Hero strip background in rows: the title keeps at most 40% of the space between its
        // top and the bottom margin, the cards get the rest of it, across the full width.
        private const float HeroTitleShare = 0.40f;
        private static (float W, float Gap, float MaxCw) HeroBackgroundBox(int w)
        {
            float s = MockScale(true, w);
            return (w - 2 * 70 * s, 14 * s, 130 * s);
        }
        private static float HeroBackgroundCardsH(int h, float s, float titleBlock) => h - 60 * s - 150 * s - titleBlock - 30 * s;

        // Spotlight split thumbnails: the left half, under the title; rows reserve at most 36%
        // of the height (the title shrinks to leave them that much).
        private static float SpotlightReserve(int rows, int h, float s) => Math.Min(rows * 120 * s + (rows - 1) * 14 * s, h * 0.36f);

        // Hero strip (poster, or spotlight as a poster) / background with Rows set and Posters on
        // Auto: the cards after the big first one.
        private static int HeroRowsCount(int r, bool background)
        {
            if (!background)
            {
                var (bw, bh, gap) = HeroPosterBox(PosterW, PosterH);
                return Math.Min(AutoMaxPosters - 1, Math.Max(RowsCount(5, r, false), r * FitPerRow(r, bw, bh, gap)));
            }
            var (w, g, maxCw) = HeroBackgroundBox(BackgroundW);
            float s = MockScale(true, BackgroundW);
            float cardsH = HeroBackgroundCardsH(BackgroundH, s, HeroTitleShare * (BackgroundH - 210 * s));
            return Math.Min(AutoMaxPosters - 1, Math.Max(RowsCount(6, r, true), r * FitPerRow(r, w, cardsH, g, maxCw)));
        }

        private static int SpotlightRowsCount(int r)
        {
            float s = MockScale(true, BackgroundW);
            return Math.Min(AutoMaxPosters - 1, Math.Max(RowsCount(4, r, true),
                r * FitPerRow(r, BackgroundW * 0.5f - 70 * s, SpotlightReserve(r, BackgroundH, s), 14 * s, 110 * s)));
        }

        private static int SpotlightAutoRows(int n, int w, int h, float s, int maxRows = MaxRows) =>
            BestRows(n, w * 0.5f - 70 * s, h * 0.36f, 14 * s, 110 * s, maxRows);

        // Rows and Posters both set: Rows is the most (the rows up to it with the biggest cards).
        private static int RowsFor(ArtOptions o, Func<int, int> bestUpTo) =>
            o.Rows != null && o.Posters != null ? bestUpTo(o.Rows.Value) : o.Rows ?? bestUpTo(MaxRows);

        // Fan in rows from 3 rows on (and Auto rows over 20 posters): each row its own fan, as
        // many overlapping cards as fill 90% of the width.
        private const float FanStep = 0.55f;
        private static float FanBandCardW(int rows, bool background, int w, int h)
        {
            float s = MockScale(background, w);
            float titleY = (background ? 590 : 800) * (h / (background ? 720f : 1000f));
            float band = (titleY - 16 * s - 24 * s) / rows;
            // A little shorter than the stacked fans of 2-3 rows (0.82 / 1.1 of a row): the
            // ends of a wide fan drop, and the bottom row must stay clear of the title.
            return band * (background ? 0.95f : 0.74f) / PosterRatio;
        }
        // Fan rows for a count over 20 with Rows on Auto: the rows (2-10) giving the biggest cards.
        private static int FanAutoRows(int n, bool background, int w, int h)
        {
            int best = 2; float bestW = -1;
            for (int r = 2; r <= MaxRows; r++)
            {
                int most = (int)Math.Ceiling(n / (double)r);
                float cw = Math.Min(FanBandCardW(r, background, w, h), w * 0.9f / (1 + FanStep * (most - 1)));
                if (cw > bestW + 0.5f) { bestW = cw; best = r; }
            }
            return best;
        }

        // Fan rows (1..maxRows) for n cards with Rows and Posters both set: the biggest cards
        // (one fan holds at most 20; from 2 rows each row is a wide fan across the picture).
        private static int FanBestRows(int n, int maxRows, bool background, int w, int h, int defN, float s)
        {
            int best = 1; float bestW = -1;
            for (int r = 1; r <= Math.Max(1, Math.Min(maxRows, n)); r++)
            {
                float cw;
                if (r == 1)
                {
                    if (n > 20) continue;
                    cw = (background ? 230 : 260) * s;
                    if (n > defN) cw *= (defN + 1) / (float)(n + 1);
                }
                else
                {
                    int most = (int)Math.Ceiling(n / (double)r);
                    cw = Math.Min(FanBandCardW(r, background, w, h), w * 0.9f / (1 + FanStep * (most - 1)));
                }
                if (cw > bestW + 0.5f) { bestW = cw; best = r; }
            }
            return best;
        }

        private static int FanPerRow(int rows, bool background, int w, int h)
        {
            float cw = FanBandCardW(rows, background, w, h);
            int perRow = (int)Math.Floor((w * 0.9f - cw) / (FanStep * cw)) + 1;
            return Math.Max(1, Math.Min(perRow, AutoMaxPosters / rows));
        }

        // Turns what draw() paints by deg about (cx, cy), shrunk so a bw x bh block still fits
        // inside a fw x fh box (no shrinking when deg is 0, nothing is turned then).
        private static void Tilted(SKCanvas c, float deg, float cx, float cy, float bw, float bh, float fw, float fh, Action draw)
        {
            if (deg == 0) { draw(); return; }
            double rad = Math.Abs(deg) * Math.PI / 180;
            float rw = (float)(bw * Math.Cos(rad) + bh * Math.Sin(rad)), rh = (float)(bw * Math.Sin(rad) + bh * Math.Cos(rad));
            float k = Math.Min(1f, Math.Min(fw / rw, fh / rh));
            c.Save();
            c.Translate(cx, cy);
            c.RotateDegrees(deg);
            c.Scale(k);
            c.Translate(-cx, -cy);
            draw();
            c.Restore();
        }

        public const int PosterW = 1000, PosterH = 1500;
        public const int BackgroundW = 1920, BackgroundH = 1080;

        private const float PosterRatio = 1.5f;   // height / width of a movie poster
        private static readonly SKColor Accent = new SKColor(245, 197, 24);
        private static readonly Lazy<SKTypeface> Display = ArtFonts.All.First(f => f.Id == "bebas").Face;
        private static readonly Lazy<SKTypeface> Heavy = ArtFonts.All.First(f => f.Id == "anton").Face;
        private static readonly Lazy<SKTypeface> Roboto = ArtFonts.All.First(f => f.Id == "roboto").Face;

        public static bool IsStyle(string? style) => Styles.Contains((style ?? "").Trim().ToLowerInvariant());

        /// <summary>"Keep current image": not a style. The item's own image is left exactly as it
        /// is (nothing generated, uploaded or removed). Default for imported collections.</summary>
        public const string KeepStyle = "keep";
        public static bool IsKeep(string? style) => string.Equals((style ?? "").Trim(), KeepStyle, StringComparison.OrdinalIgnoreCase);

        /// <summary>Each title once (by IMDb id, else name + year), in order: a library can hold
        /// the same film twice (versions, copies), and the art must never show a poster twice.</summary>
        public static List<MediaBrowser.Controller.Entities.BaseItem> DistinctTitles(IEnumerable<MediaBrowser.Controller.Entities.BaseItem> items)
        {
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var result = new List<MediaBrowser.Controller.Entities.BaseItem>();
            foreach (var item in items)
            {
                var imdb = item.GetProviderId("Imdb");
                var key = !string.IsNullOrEmpty(imdb) ? imdb : (item.Name ?? "").Trim() + "|" + item.ProductionYear;
                if (seen.Add(key)) result.Add(item);
            }
            return result;
        }

        /// <summary>The text for the art: <paramref name="template"/> ("" = "{name}") with {name}
        /// (also {collectionName}) and {week} (ISO week number of today) filled in.</summary>
        public static string ArtTitle(string? template, string name)
        {
            var t = string.IsNullOrWhiteSpace(template) ? "{name}" : template!.Trim();
            var week = System.Globalization.CultureInfo.InvariantCulture.Calendar.GetWeekOfYear(
                DateTime.Now.AddDays(DateTime.Now.DayOfWeek == DayOfWeek.Sunday ? -3 : 3 - ((int)DateTime.Now.DayOfWeek - 1)),
                System.Globalization.CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday);
            return t.Replace("{collectionName}", name).Replace("{name}", name).Replace("{week}", week.ToString()).Trim();
        }

        private static readonly Dictionary<string, string> SampleCache = new Dictionary<string, string>();

        /// <summary>
        /// A small example of a style (JPEG data: URL) for the style picker, drawn by the real
        /// renderer from plain coloured stand-in posters (no real artwork). Cached per process.
        /// </summary>
        public static string Sample(string style, bool background, string tempDir)
        {
            var key = style + (background ? "|bg" : "|poster");
            lock (SampleCache)
                if (SampleCache.TryGetValue(key, out var cached)) return cached;

            Directory.CreateDirectory(tempDir);
            var posters = new List<string>();
            for (int i = 0; i < 12; i++)
            {
                var path = Path.Combine(tempDir, $"stand-in-{i}.jpg");
                lock (SampleCache) if (!File.Exists(path)) DrawStandInPoster(i, path);
                posters.Add(path);
            }
            var output = Path.Combine(tempDir, $"sample-{style}-{(background ? "bg" : "poster")}.jpg");
            Render(style, posters, "Collection", background, output);
            using var full = SKBitmap.Decode(output);
            int th = background ? 135 : 180, tw = (int)Math.Round(full.Width * th / (double)full.Height);
            using var small = full.Resize(new SKImageInfo(tw, th), SKFilterQuality.High);
            using var img = SKImage.FromBitmap(small);
            using var data = img.Encode(SKEncodedImageFormat.Jpeg, 82);
            var url = "data:image/jpeg;base64," + Convert.ToBase64String(data.ToArray());
            lock (SampleCache) SampleCache[key] = url;
            return url;
        }

        /// <summary>
        /// The Customise popup's live preview (PNG data: URL): the real renderer with the popup's
        /// options, drawn from the same stand-in posters as the style tiles (twelve, or as many as
        /// the poster count asks for), a little bigger than a tile.
        /// </summary>
        public static string Preview(string style, bool background, ArtOptions opts, string title, string tempDir) =>
            Preview(style, background, opts, title, tempDir, out _);

        public static string Preview(string style, bool background, ArtOptions opts, string title, string tempDir, out string note)
        {
            note = "";
            Directory.CreateDirectory(tempDir);
            int count = opts.Posters != null ? Math.Max(1, PostersFor(style, background, opts))
                      : opts.Rows != null ? Math.Max(12, PostersFor(style, background, opts)) : 12;
            var posters = new List<string>();
            for (int i = 0; i < count; i++)
            {
                var path = Path.Combine(tempDir, $"stand-in-{i}.jpg");
                lock (SampleCache) if (!File.Exists(path)) DrawStandInPoster(i, path);
                posters.Add(path);
            }
            var output = Path.Combine(tempDir, "preview-" + Guid.NewGuid().ToString("N") + ".jpg");
            try
            {
                note = RowsNote(opts, Render(style, posters, string.IsNullOrWhiteSpace(title) ? "Collection" : title, background, output, opts));
                using var full = SKBitmap.Decode(output);
                int th = background ? 338 : 450, tw = (int)Math.Round(full.Width * th / (double)full.Height);
                using var small = full.Resize(new SKImageInfo(tw, th), SKFilterQuality.High);
                using var img = SKImage.FromBitmap(small);
                using var data = img.Encode(SKEncodedImageFormat.Png, 100);
                return "data:image/png;base64," + Convert.ToBase64String(data.ToArray());
            }
            finally
            {
                try { File.Delete(output); } catch { }
            }
        }

        /// <summary>The popup's note when Rows and Posters are both set and fewer rows were used
        /// ("16 posters fit best on 4 rows"), else "".</summary>
        public static string RowsNote(ArtOptions opts, int? rowsUsed) =>
            opts.Rows != null && opts.Posters != null && rowsUsed != null && rowsUsed < opts.Rows
                ? $"{opts.Posters} poster{(opts.Posters == 1 ? "" : "s")} fit best on {rowsUsed} row{(rowsUsed == 1 ? "" : "s")}." : "";

        /// <summary>What every Customise field is for <paramref name="style"/> when nothing is set
        /// (today's look), with a short note for each field the style ignores. Tilt, Rows and Posters
        /// work on every style (Ranked keeps its top 4 / 5, so only its tilt applies).</summary>
        public static ArtOptionDefaults Defaults(string style, bool background)
        {
            var st = (style ?? "").Trim().ToLowerInvariant();
            if (!IsStyle(st)) st = Grid;
            // Spotlight as a poster is drawn as hero strip.
            var look = st == Spotlight && !background ? HeroStrip : st;
            var d = new ArtOptionDefaults { AutoPosters = PostersFor(st, background) };
            d.Tilt = st == Wall ? "left" : "straight";
            d.ShowTitle = !(st == Collage && background);
            d.Font = st == Collage ? "roboto" : look == Spotlight ? "anton" : "bebas";
            d.Case = st == Collage || look == Spotlight ? "typed" : "upper";
            d.Colour = st == Collage ? "#f5c518" : "#ffffff";
            d.Pos = st == Wall ? "mc" : look == HeroStrip ? (background ? "tl" : "bc") : look == Spotlight ? "ml"
                  : st == Ranked ? (background ? "tl" : "tc") : "bc";
            d.Darken = st == Grid ? 72 : st == Fan ? 55 : st == Wall ? 35 : look == HeroStrip ? (background ? 35 : 25) : 0;
            if (st == Ranked) d.Hints["rows"] = "Ranked keeps its own layout of the top " + (background ? 5 : 4) + ".";
            if (st == Ranked) d.Hints["posters"] = "Ranked always shows the top " + (background ? 5 : 4) + ".";
            if (st == Ranked) d.Hints["darken"] = "Ranked has a plain dark background.";
            if (st == Wall) d.Hints["pos"] = "The centre spot keeps the gold title band; any other spot puts the title over the wall.";
            return d;
        }

        // The badge previews' stand-in poster: the grey/blue gradient of the Badge Style icons
        // (#6b7a8f to #2c3440, top left to bottom right), with the same disc and "title" bar.
        internal static void DrawBadgeStandInPoster(string path)
        {
            const int w = 300, h = 450;
            using var surface = SKSurface.Create(new SKImageInfo(w, h, SKColorType.Rgba8888, SKAlphaType.Premul));
            var c = surface.Canvas;
            using (var g = new SKPaint
            {
                Shader = SKShader.CreateLinearGradient(new SKPoint(0, 0), new SKPoint(w, h),
                    new[] { new SKColor(0x6B, 0x7A, 0x8F), new SKColor(0x2C, 0x34, 0x40) }, null, SKShaderTileMode.Clamp)
            }) c.DrawRect(0, 0, w, h, g);
            using (var disc = new SKPaint { Color = SKColors.White.WithAlpha(60), IsAntialias = true }) c.DrawCircle(w / 2f, h * 0.42f, w * 0.22f, disc);
            using (var bar = new SKPaint { Color = SKColors.White.WithAlpha(200), IsAntialias = true })
                c.DrawRoundRect(SKRect.Create(w * 0.18f, h * 0.78f, w * 0.64f, h * 0.06f), 6, 6, bar);
            Save(surface, path);
        }

        // A plain poster-shaped gradient in one of twelve hues, with a lighter "title" bar.
        internal static void DrawStandInPoster(int i, string path)
        {
            const int w = 300, h = 450;
            float hue = (i * 47) % 360;
            using var surface = SKSurface.Create(new SKImageInfo(w, h, SKColorType.Rgba8888, SKAlphaType.Premul));
            var c = surface.Canvas;
            using (var g = new SKPaint
            {
                Shader = SKShader.CreateLinearGradient(new SKPoint(0, 0), new SKPoint(0, h),
                    new[] { SKColor.FromHsv(hue, 55, 85), SKColor.FromHsv((hue + 30) % 360, 70, 35) }, null, SKShaderTileMode.Clamp)
            }) c.DrawRect(0, 0, w, h, g);
            using (var disc = new SKPaint { Color = SKColors.White.WithAlpha(60), IsAntialias = true }) c.DrawCircle(w / 2f, h * 0.42f, w * 0.22f, disc);
            using (var bar = new SKPaint { Color = SKColors.White.WithAlpha(200), IsAntialias = true })
                c.DrawRoundRect(SKRect.Create(w * 0.18f, h * 0.78f, w * 0.64f, h * 0.06f), 6, 6, bar);
            Save(surface, path);
        }

        /// <summary>Renders <paramref name="style"/> to a JPEG. Needs at least one poster file.
        /// Returns the rows the cards were drawn on when Rows and Posters are both set (Rows is
        /// "at most" then), else null.</summary>
        public static int? Render(string style, IList<string> posterPaths, string title, bool background, string outputPath, ArtOptions? opts = null)
        {
            var o = opts ?? ArtOptions.None;
            var st = (style ?? "").Trim().ToLowerInvariant();
            var d = Defaults(st, background);
            var x = new Ctx(o, background);
            // Title moved from the style's own spot: the style draws without it and the title goes
            // on top at the chosen spot (DrawFreeTitle).
            bool showTitle = o.ShowTitle ?? d.ShowTitle;
            bool free = showTitle && o.Pos != null && o.Pos != d.Pos && !string.IsNullOrWhiteSpace(title);
            string styleTitle = showTitle && !free ? title : "";
            var posters = new List<SKBitmap>();
            try
            {
                // Over 40 posters the cards are small: each poster is decoded at about 600 px high
                // (JPEG decodes straight to a smaller scale), so 100 posters stay light on memory.
                // Up to 40: no larger than the biggest sharp card a style draws, so a 2000x3000 file
                // is not held at full size (see DecodeForCards).
                // Decoded side by side (a few at a time), kept in list order.
                int w = background ? BackgroundW : PosterW, h = background ? BackgroundH : PosterH;
                bool small = posterPaths.Count > 40;
                int maxH = SmallDecodeHeight(posterPaths.Count);
                int count = posterPaths.Count;
                var decoded = new SKBitmap?[posterPaths.Count];
                System.Threading.Tasks.Parallel.For(0, posterPaths.Count,
                    new System.Threading.Tasks.ParallelOptions { MaxDegreeOfParallelism = DecodeThreads },
                    i => { try { decoded[i] = small ? DecodeSmall(posterPaths[i], maxH) : DecodeForCards(posterPaths[i], w, h, count); } catch { decoded[i] = null; } });
                foreach (var bmp in decoded) if (bmp != null) posters.Add(bmp);
                if (posters.Count == 0) throw new InvalidOperationException("none of the posters could be read");

                using var surface = SKSurface.Create(new SKImageInfo(w, h, SKColorType.Rgba8888, SKAlphaType.Premul))
                    ?? throw new InvalidOperationException($"SkiaSharp could not create a {w}x{h} surface");
                var canvas = surface.Canvas;
                canvas.Clear(Base);

                switch (st)
                {
                    case Fan: DrawFan(canvas, posters, styleTitle, background, w, h, x); break;
                    case Collage: DrawCollage(canvas, posters, styleTitle, w, h, x); break;
                    case Wall: DrawWall(canvas, posters, styleTitle, background, w, h, x); break;
                    case Ranked: DrawRanked(canvas, posters, styleTitle, background, w, h, x); break;
                    case HeroStrip:
                        if (background) DrawHeroStripBackground(canvas, posters, styleTitle, w, h, x);
                        else DrawHeroStripPoster(canvas, posters, styleTitle, w, h, x);
                        break;
                    case Spotlight:
                        if (background) DrawSpotlight(canvas, posters, styleTitle, w, h, x);
                        else DrawHeroStripPoster(canvas, posters, styleTitle, w, h, x);
                        break;
                    default: DrawGrid(canvas, posters, styleTitle, background, w, h, x); break;
                }
                if (free) DrawFreeTitle(canvas, title, o.Pos!, background, w, h, x, d);
                Save(surface, outputPath);
                return st == Ranked ? null : x.RowsUsed;
            }
            finally
            {
                foreach (var p in posters) p.Dispose();
            }
        }

        private static readonly int DecodeThreads = Math.Max(2, Math.Min(6, Environment.ProcessorCount));

        // How high a poster is decoded when there are over 40 (the cards are small then): 600 px
        // up to 100 posters, less beyond that (250 cards on a poster are under 150 px high).
        private static int SmallDecodeHeight(int count) => count <= 100 ? 600 : count <= 160 ? 450 : 340;

        // A poster (up to 40 of them) decoded no larger than the biggest sharp card it can get on a
        // w x h picture. One or two posters: enough to cover the whole picture (a collage cell or a
        // wall card can be that big). Three or more: also at most 1.6 x the picture's height (the
        // Spotlight hero needs 1.35 x on a background, a tilted collage cell about 1.2 x). Blurred
        // backdrops need far less (DrawBackdrop draws at a quarter size). A full decode when the
        // file is already that small or cannot be read ahead.
        private static SKBitmap? DecodeForCards(string path, int w, int h, int count)
        {
            int maxH;
            using (var codec = SKCodec.Create(path))
            {
                if (codec == null || codec.Info.Width <= 0 || codec.Info.Height <= 0) return SKBitmap.Decode(path);
                var info = codec.Info;
                double cover = Math.Max(h, w * (double)info.Height / info.Width);
                maxH = (int)Math.Ceiling(count <= 2 ? cover : Math.Min(cover, h * 1.6));
                if (info.Height <= maxH) return SKBitmap.Decode(codec);
            }
            return DecodeSmall(path, maxH);
        }

        // A poster decoded at about maxH pixels high (never larger than the file).
        private static SKBitmap? DecodeSmall(string path, int maxH)
        {
            try
            {
                using var codec = SKCodec.Create(path);
                if (codec == null) return SKBitmap.Decode(path);
                var info = codec.Info;
                if (info.Height <= maxH * 1.5) return SKBitmap.Decode(codec);
                var dims = codec.GetScaledDimensions(maxH / (float)info.Height);
                if (dims.Height < maxH) dims = info.Size;   // no matching scale: decode then resize
                var bmp = new SKBitmap(new SKImageInfo(dims.Width, dims.Height, SKColorType.Rgba8888, SKAlphaType.Premul));
                if (codec.GetPixels(bmp.Info, bmp.GetPixels()) is var r && r != SKCodecResult.Success && r != SKCodecResult.IncompleteInput)
                { bmp.Dispose(); return SKBitmap.Decode(path); }
                if (bmp.Height > maxH * 1.5)
                {
                    var resized = bmp.Resize(new SKImageInfo((int)Math.Round(bmp.Width * maxH / (double)bmp.Height), maxH, SKColorType.Rgba8888, SKAlphaType.Premul), SKFilterQuality.Medium);
                    bmp.Dispose();
                    return resized;
                }
                return bmp;
            }
            catch { return SKBitmap.Decode(path); }
        }

        // ── grid ────────────────────────────────────────────────────────────────────────
        // Posters: up to 12 (1, 2x1, 3x2, 3x3, 4x3…); backgrounds: up to 14 in at most two rows.
        // A partial last row is centred. The title sits in a dark band at the bottom; a two-line
        // title gets an accent first line.
        private static void DrawGrid(SKCanvas c, List<SKBitmap> posters, string title, bool background, int w, int h, Ctx x)
        {
            DrawBackdrop(c, MostColourful(posters), w, h, x.Dark(0.72f));
            DrawVignette(c, w, h, 0.45f);

            bool hasTitle = !string.IsNullOrWhiteSpace(title);
            int m = background ? 72 : 64;
            int titleH = hasTitle ? (int)((background ? 200 : 300) * x.Scale) : 0;
            int titleGap = hasTitle ? (background ? 30 : 40) : 0;
            var region = GridRegion(background, w, h, hasTitle, x.Scale);
            // Rows set, Posters on Auto: as many cards a row as fill the width at that row height.
            int cap = x.O.Posters ?? (x.O.Rows != null ? GridRowsCount(x.O, background, hasTitle) : background ? 14 : 12);
            // Many small cards (a custom count over 20, or more than 5 rows) get a narrower gap.
            int gridGap = GridGap(x.O);
            // A chosen count is drawn in full: repeats fill in when the source has fewer titles.
            // With a count and Rows on Auto the rows that give the biggest cards are used.
            var g = BestGrid(x.O.Posters != null ? Math.Max(posters.Count, cap) : posters.Count, (int)region.Width, (int)region.Height, gridGap, cap,
                             x.O.Rows ?? (background && x.O.Posters == null ? 2 : (int?)null), x.O.Rows, x.O.Posters != null);
            if (g.RowCounts != null) posters = Spread(posters, g.RowCounts);
            if (x.O.Rows != null && x.O.Posters != null) x.RowsUsed = g.Rows;

            float radius = RadiusFor(g.CardW);
            var cells = g.Cells(region);
            float tilt = x.Tilt(-6, 0);
            if (tilt != 0)
            {
                // The whole block turned about the region's centre, shrunk so it still fits.
                float bw = g.Cols * g.CardW + (g.Cols - 1) * g.Gap, bh = g.Rows * g.CardH + (g.Rows - 1) * g.Gap;
                double rad = Math.Abs(tilt) * Math.PI / 180;
                float rw = (float)(bw * Math.Cos(rad) + bh * Math.Sin(rad)), rh = (float)(bw * Math.Sin(rad) + bh * Math.Cos(rad));
                float k = Math.Min(1f, Math.Min(region.Width / rw, region.Height / rh));
                c.Save();
                c.Translate(region.MidX, region.MidY);
                c.RotateDegrees(tilt);
                c.Scale(k);
                c.Translate(-region.MidX, -region.MidY);
            }
            for (int i = 0; i < cells.Count; i++)
                DrawCard(c, posters[i], SKRect.Create(cells[i].X, cells[i].Y, g.CardW, g.CardH), radius,
                         Math.Max(8, g.CardW / 18f), g.CardW / 30f, 190);
            if (tilt != 0) c.Restore();

            if (!hasTitle) return;
            float bandTop = region.Bottom;
            Scrim(c, SKRect.Create(0, bandTop - 80, w, h - bandTop + 80), Dir.Down, 0, 0.55f, 235);
            var t = LayoutTitle(title, w - 2 * m, titleH, (background ? 190 : 240) * x.Scale, x.Upper(true), x.Face(Display.Value));
            DrawTitle(c, t, w / 2f, bandTop + titleGap + (titleH - t.Height) / 2, TextAlign.Center, colour: x.O.ColourValue);
        }

        // The mockup styles (stoep-shelves/docs/art-mockups/mock.py) were drawn at 680x1000 (poster)
        // and 1280x720 (background); sizes below are those numbers scaled to the output.
        private static float MockScale(bool background, int w) => w / (background ? 1280f : 680f);
        private static readonly SKColor MockBase = new SKColor(8, 8, 12);
        private static readonly SKColor RankRed = new SKColor(229, 9, 20);

        // Cover-fitted image, gaussian-blurred, blended toward the dark base (mock "backdrop").
        private static void MockBackdrop(SKCanvas c, SKBitmap img, int w, int h, float blur, float dark)
        {
            var src = CoverSource(img.Width, img.Height, w, h);
            float scale = w / src.Width;
            var fit = SKMatrix.CreateScaleTranslation(scale, scale, -src.Left * scale, -src.Top * scale);
            DrawClampedBlurred(c, img, fit, blur, w, h);
            using var tint = new SKPaint { Color = MockBase.WithAlpha((byte)Math.Round(255 * dark)) };
            c.DrawRect(0, 0, w, h, tint);
        }

        // Rounded card without the soft drop shadow (mock "rounded"), or with it (mock "shadow").
        private static void MockCard(SKCanvas c, SKBitmap img, float x, float y, float cw, float ch, float r, bool shadow, float s)
        {
            if (shadow) DrawCard(c, img, SKRect.Create(x, y, cw, ch), r, 18 * s, 10 * s, 170);
            else DrawCard(c, img, SKRect.Create(x, y, cw, ch), r, 1, 0, 0);
        }

        // Text whose top edge sits at y (like PIL's text at (x, y)).
        private static float MockText(SKCanvas c, string text, float x, float y, SKTypeface face, float size, SKColor colour,
            SKTextAlign align = SKTextAlign.Left, float stroke = 0, SKColor? strokeColour = null)
        {
            using var paint = new SKPaint { Typeface = face, TextSize = size, IsAntialias = true, TextAlign = align, Color = colour };
            var bounds = new SKRect();
            paint.MeasureText(text, ref bounds);
            float baseline = y - bounds.Top;
            if (stroke > 0)
            {
                using var sp = new SKPaint
                {
                    Typeface = face, TextSize = size, IsAntialias = true, TextAlign = align,
                    Color = strokeColour ?? SKColors.Black, Style = SKPaintStyle.Stroke, StrokeWidth = stroke * 2, StrokeJoin = SKStrokeJoin.Round
                };
                c.DrawText(text, x, baseline, sp);
            }
            c.DrawText(text, x, baseline, paint);
            return bounds.Height;
        }

        // Title split into at most two balanced lines that fit maxW at maxSize (shrinking if needed).
        // preferTwo: always two lines when there is more than one word (the two-tone hero titles).
        private static (string[] lines, float size) MockLines(string title, SKTypeface face, float maxW, float maxSize, bool upper = true, bool preferTwo = false)
        {
            string text = string.Join(" ", (upper ? title.ToUpperInvariant() : title).Trim().Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries));
            using var probe = new SKPaint { Typeface = face, TextSize = 100 };
            float SizeFor(string[] ls) => Math.Min(maxSize, 100 * maxW / Math.Max(1, ls.Max(l => probe.MeasureText(l))));
            var one = new[] { text };
            var words = text.Split(' ');
            string[] best = one;
            if (words.Length > 1)
            {
                string[]? two = null; float twoSize = 0;
                for (int i = 1; i < words.Length; i++)
                {
                    var cand = new[] { string.Join(" ", words.Take(i)), string.Join(" ", words.Skip(i)) };
                    float sz = SizeFor(cand);
                    if (two == null || sz > twoSize) { two = cand; twoSize = sz; }
                }
                if (two != null && (preferTwo || SizeFor(one) < maxSize * 0.8f)) best = two;
            }
            return (best, SizeFor(best));
        }

        // ── collage (Collectra) ─────────────────────────────────────────────────────────
        private static void DrawCollage(SKCanvas c, List<SKBitmap> posters, string title, int w, int h, Ctx x)
        {
            int n = posters.Count, rows, cols;
            int[]? rowCounts = null;
            if (x.O.Posters != null)
            {
                // Customise "Posters": each poster once, rows of even length spanning the width
                // (12 on 3 rows = 4/4/4; 10 on 3 = 4/3/3). Rows auto: the rows whose cells come
                // closest to a poster's shape. A source with fewer titles than asked repeats
                // posters (never beside a copy).
                n = Math.Max(n, x.O.Posters.Value);
                int best = 1; double bestErr = double.MaxValue;
                for (int r = 1; r <= Math.Min(n, MaxRows); r++)
                {
                    if (x.O.Rows != null && r > x.O.Rows.Value) continue;   // Rows set too: at most that many
                    int c0 = (int)Math.Ceiling(n / (double)r);
                    double err = Math.Abs(Math.Log((h / (double)r) / (w / (double)c0) / PosterRatio));
                    if (err < bestErr - 1e-9) { bestErr = err; best = r; }
                }
                rowCounts = EvenRows(n, best);
                rows = rowCounts.Length; cols = rowCounts[0];
                if (x.O.Rows != null) x.RowsUsed = rows;
                posters = Spread(posters, rowCounts);
            }
            else if (x.O.Rows != null)
            {
                rows = x.O.Rows.Value;
                cols = CollageCols(rows, w, h);
            }
            else if (w < h) { rows = Math.Max(1, (int)Math.Sqrt(n)); cols = (n + rows - 1) / rows; }
            else { rows = Math.Max(1, (int)(Math.Sqrt(n) / (w / (double)h))); cols = (n + rows - 1) / rows; }
            int cellW = w / cols, cellH = h / rows;
            var used = rowCounts == null ? CollagePicks(n, rows, cols, new Random(n * 7919 + w)) : new int[0];
            using var paint = new SKPaint { FilterQuality = SKFilterQuality.High };
            float tilt = x.Tilt(-8, 0);
            if (tilt != 0)
            {
                // Turned about the centre and enlarged so no corner shows.
                double rad = Math.Abs(tilt) * Math.PI / 180;
                float k = (float)Math.Max((w * Math.Cos(rad) + h * Math.Sin(rad)) / w, (w * Math.Sin(rad) + h * Math.Cos(rad)) / h);
                c.Save();
                c.Translate(w / 2f, h / 2f);
                c.RotateDegrees(tilt);
                c.Scale(k);
                c.Translate(-w / 2f, -h / 2f);
            }
            if (rowCounts != null)
            {
                int i0 = 0;
                for (int r = 0; r < rowCounts.Length; r++)
                {
                    float y0 = (float)Math.Round(r * h / (double)rowCounts.Length), y1 = (float)Math.Round((r + 1) * h / (double)rowCounts.Length);
                    for (int col = 0; col < rowCounts[r]; col++, i0++)
                    {
                        float x0 = (float)Math.Round(col * w / (double)rowCounts[r]), x1 = (float)Math.Round((col + 1) * w / (double)rowCounts[r]);
                        var p = posters[i0];
                        c.DrawBitmap(p, CoverSource(p.Width, p.Height, (int)(x1 - x0), (int)(y1 - y0)), new SKRect(x0, y0, x1, y1), paint);
                    }
                }
            }
            else
            for (int i = 0; i < rows * cols; i++)
            {
                var p = posters[used[i]];
                c.DrawBitmap(p, CoverSource(p.Width, p.Height, cellW, cellH), SKRect.Create((i % cols) * cellW, (i / cols) * cellH, cellW, cellH), paint);
            }
            if (tilt != 0) c.Restore();
            if (x.O.Darken > 0)
                using (var dim = new SKPaint { Color = SKColors.Black.WithAlpha((byte)Math.Round(255 * x.Dark(0))) }) c.DrawRect(0, 0, w, h, dim);
            if (string.IsNullOrWhiteSpace(title)) return;

            // Collectra's text overlay: wrapped to 90% of the width, at the bottom (30px margin),
            // on a 50% black box (16px padding), yellow with a 2px black shadow.
            // Collectify's settings: Roboto Bold 95px on a 680px poster, #F5C518.
            var face = x.Face(Roboto.Value);
            if (x.Upper(false)) title = title.ToUpperInvariant();
            float size = 95 * w / 680f * x.Scale, margin = 30 * w / 680f, pad = Math.Max(16, 16 * w / 680f), shadowOff = 2 * w / 680f;
            using var text = new SKPaint { Typeface = face, TextSize = size, IsAntialias = true, Color = x.Colour(new SKColor(245, 197, 24)) };
            float maxW = w * 0.9f;
            var lines = new List<string>();
            foreach (var word in title.Trim().Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries))
            {
                if (lines.Count > 0 && text.MeasureText(lines[lines.Count - 1] + " " + word) <= maxW) lines[lines.Count - 1] += " " + word;
                else lines.Add(word);
            }
            var tg = new SKRect(); text.MeasureText("Tg", ref tg);
            float lineH = tg.Height + 5 * w / 680f;
            float total = lineH * lines.Count;
            float startY = h - total - margin;
            float widest = lines.Max(l => text.MeasureText(l));
            using (var box = new SKPaint { Color = SKColors.Black.WithAlpha(128) })
                c.DrawRect(new SKRect((w - widest) / 2 - pad, startY - pad, (w + widest) / 2 + pad, startY + total + pad), box);
            using var shadow = new SKPaint { Typeface = face, TextSize = size, IsAntialias = true, Color = SKColors.Black };
            for (int i = 0; i < lines.Count; i++)
            {
                var lb = new SKRect(); text.MeasureText(lines[i], ref lb);
                float lx = (w - text.MeasureText(lines[i])) / 2;
                float y = startY + i * lineH + (lineH - lb.Height) / 2 - lb.Top;
                c.DrawText(lines[i], lx + shadowOff, y + shadowOff, shadow);
                c.DrawText(lines[i], lx, y, text);
            }
        }

        // The poster for each cell of a rows x cols collage of n posters: the posters in order,
        // then the spare cells filled with repeats (CollageRepeat). Four posters with spare cells
        // use a 2x2 pattern, the only way to keep every copy apart from its own.
        internal static int[] CollagePicks(int n, int rows, int cols, Random rng)
        {
            var used = new int[rows * cols];
            bool pattern = n == 4 && rows >= 2 && cols >= 2 && rows * cols > n;
            for (int i = 0; i < used.Length; i++)
                used[i] = pattern ? 2 * (i / cols % 2) + i % cols % 2 : i < n ? i : CollageRepeat(used, i, cols, n, rng);
            return used;
        }

        // A poster for spare collage cell i (cells before it are filled): never one of the cards
        // around it (beside, above or diagonal), and of the rest the one whose nearest copy is
        // furthest away, so repeats are spread out.
        private static int CollageRepeat(int[] used, int i, int cols, int n, Random rng)
        {
            int r0 = i / cols, c0 = i % cols;
            var touching = new HashSet<int>();
            for (int j = 0; j < i; j++)
                if (Math.Abs(j / cols - r0) <= 1 && Math.Abs(j % cols - c0) <= 1) touching.Add(used[j]);
            var cands = Enumerable.Range(0, n).Where(p => !touching.Contains(p)).ToList();
            if (cands.Count == 0) cands = Enumerable.Range(0, n).ToList();
            int Dist(int p)
            {
                int d = int.MaxValue;
                for (int j = 0; j < i; j++)
                    if (used[j] == p) d = Math.Min(d, Math.Max(Math.Abs(j / cols - r0), Math.Abs(j % cols - c0)));
                return d;
            }
            int far = cands.Max(Dist);
            var best = cands.Where(p => Dist(p) == far).ToList();
            return best[rng.Next(best.Count)];
        }

        // ── fan ─────────────────────────────────────────────────────────────────────────
        // Cards fanned like a hand of playing cards, the middle one on top; title underneath.
        private static void DrawFan(SKCanvas c, List<SKBitmap> posters, string title, bool background, int w, int h, Ctx x)
        {
            float s = MockScale(background, w);
            MockBackdrop(c, posters.Count > 3 ? posters[3] : posters[0], w, h, 34 * s, x.Dark(0.55f));
            int defN = background ? 7 : 5;
            float spread = background ? 7 : 9;
            float cy = (background ? 330 : 440) * (h / (background ? 720f : 1000f));
            float titleY = (background ? 590 : 800) * (h / (background ? 720f : 1000f));
            float fanTilt = x.Tilt(-8, 0);
            // One fan of cards (the middle one on top) centred on (fx, fy).
            void FanRow(List<SKBitmap> cards, float fx, float fy, float cw, float ch, float step = 0.55f, float dropScale = 1f)
            {
                int cnt = cards.Count;
                float mid = (cnt - 1) / 2f;
                // More cards than the style's own: the same overall bend, so the ends stay in view.
                float bend = cnt > defN ? (defN - 1) / (float)(cnt - 1) : 1f;
                foreach (int i in Enumerable.Range(0, cnt).OrderByDescending(i => Math.Abs(i - mid)))
                {
                    float k = i - mid, kb = bend == 1f ? k : k * bend;
                    c.Save();
                    float drop = (float)Math.Pow(Math.Abs(kb), 1.6) * (bend == 1f ? cw : cw / bend) * 0.09f;
                    c.Translate(fx + k * cw * step, fy + (dropScale == 1f ? drop : drop * dropScale));
                    c.RotateDegrees(kb * spread);
                    DrawCard(c, cards[i], SKRect.Create(-cw / 2, -ch / 2, cw, ch), 14 * s, 16 * s, 12 * s, 170);
                    c.Restore();
                }
            }
            // More than 20 cards with Rows on Auto: stacked fans, on the rows that give the biggest cards.
            // Rows and Posters both set: Rows is the most, the rows up to it with the biggest cards.
            bool both = x.O.Rows != null && x.O.Posters != null;
            int fanRows = both ? FanBestRows(x.O.Posters!.Value, x.O.Rows!.Value, background, w, h, defN, s)
                        : x.O.Rows ?? (x.O.Posters > 20 ? FanAutoRows(x.O.Posters.Value, background, w, h) : 1);
            if (both) x.RowsUsed = fanRows;
            if (fanRows <= 1)
            {
                int n = x.O.Posters != null ? Math.Min(x.O.Posters.Value, 20) : Math.Min(posters.Count, defN);
                if (posters.Count < n) posters = Spread(posters, new[] { n });
                float cw = (background ? 230 : 260) * s, ch = cw * PosterRatio;
                if (n > defN) { cw *= (defN + 1) / (float)(n + 1); ch = cw * PosterRatio; }
                if (fanTilt != 0) { c.Save(); c.RotateDegrees(fanTilt, w / 2f, cy); }
                FanRow(posters.Take(n).ToList(), w / 2f, cy, cw, ch);
                if (fanTilt != 0) c.Restore();
            }
            else
            {
                // Customise "Rows": that many smaller fans stacked above the title, the cards
                // spread evenly over them (full rows unless Posters is set). From 3 rows on (and
                // for a big count on Auto rows) each row is a wide fan of overlapping cards that
                // spans the picture: as many cards as fill the width when Posters is on Auto.
                int rows = fanRows;
                bool wide = rows >= 3 || x.O.Rows == null || both;
                int own = rows >= 3 ? rows * FanPerRow(rows, background, w, h)
                                    : RowsCount(defN, rows, background);
                int n = CardsToUse(posters.Count, x.O.Posters ?? own, rows, x.O.Posters != null);
                var counts = EvenRows(n, rows);
                posters = Spread(posters, counts);
                float top = 24 * s, bottom = titleY - 16 * s, band = (bottom - top) / counts.Length;
                int most = counts.Max();
                float cwByH = wide ? FanBandCardW(rows, background, w, h) : band * (background ? 1.1f : 0.82f) / PosterRatio;
                float cwByW = w * 0.9f / (1 + FanStep * (most - 1));
                float cw = Math.Min(cwByH, cwByW), ch = cw * PosterRatio;
                // Too few cards to span the picture at the usual overlap: they spread a little
                // wider, but keep overlapping so the row still reads as a fan.
                float step = wide && most > 1 ? Math.Max(FanStep, Math.Min(0.72f, (w * 0.9f - cw) / ((most - 1) * cw))) : FanStep;
                if (fanTilt != 0) { c.Save(); c.RotateDegrees(fanTilt, w / 2f, (top + bottom) / 2); }
                int at = 0;
                for (int r = 0; r < counts.Length; r++)
                {
                    var row = posters.Skip(at).Take(counts[r]).ToList();
                    float fy = top + band * (r + 0.5f);
                    float bendR = counts[r] > defN ? (defN - 1) / (float)(counts[r] - 1) : 1f;
                    float arc = (float)Math.Pow((counts[r] - 1) / 2f * bendR, 1.6) * cw / bendR * 0.09f;
                    // A wide fan bends as much as the style's own, but its ends drop at most a
                    // quarter of a card so the rows stay apart and the top row stays in the picture.
                    float dropScale = wide && arc > ch / 4 ? ch / 4 / arc : 1f;
                    FanRow(row, w / 2f, fy - arc * dropScale / 2, cw, ch, step, dropScale);
                    at += counts[r];
                }
                if (fanTilt != 0) c.Restore();
            }
            if (string.IsNullOrWhiteSpace(title)) return;
            var face = x.Face(Display.Value);
            var (lines, size) = MockLines(title, face, w - 80 * s, (background ? 96 : 110) * s * x.Scale, x.Upper(true));
            float y = titleY;
            foreach (var line in lines)
            {
                MockText(c, line, w / 2f, y, face, size, x.Colour(SKColors.White), SKTextAlign.Center, 2 * s);
                y += size * Step(face, Display.Value, 0.95f);
            }
        }

        // ── wall ────────────────────────────────────────────────────────────────────────
        // A brick-offset wall of posters tilted 14°, dimmed a little, with a dark title band and
        // gold lines across the middle. Posters repeat so the wall is always full.
        private static void DrawWall(SKCanvas c, List<SKBitmap> posters, string title, bool background, int w, int h, Ctx x)
        {
            float s = MockScale(background, w);
            float cw = 150 * s, ch = (int)(150 * PosterRatio) * s, gap = 10 * s;
            // Rows and Posters both set: Rows is the most. Cards sized for the count alone when
            // that needs no more rows; otherwise the rows keep their height and the cards narrow.
            int? postersOnlyRows = null;
            if (x.O.Rows != null && x.O.Posters != null)
            {
                float pch = ((float)Math.Sqrt(w * (double)h / (x.O.Posters.Value * PosterRatio)) - gap) * PosterRatio;
                int pr = Math.Max(1, (int)Math.Round(h / (pch + gap)));
                if (pr <= x.O.Rows.Value) postersOnlyRows = pr;
                x.RowsUsed = postersOnlyRows ?? x.O.Rows.Value;
            }
            if (x.O.Rows != null && postersOnlyRows == null)   // about that many rows across the height
            {
                ch = h / (float)x.O.Rows.Value - gap;
                cw = ch / PosterRatio;
                if (x.O.Posters != null)
                {
                    // Both set: the rows keep their height and the cards narrow (cover-cropped, down
                    // to a little under half a poster's width) so about that many posters fit across.
                    int perRow = (int)Math.Ceiling(x.O.Posters.Value / (double)x.O.Rows.Value);
                    cw = Math.Max(cw * 0.45f, Math.Min(cw, w / (float)Math.Max(1, perRow) - gap));
                }
            }
            else if (x.O.Posters != null)
            {
                // Customise "Posters": cards sized so about that many fill the picture (4 = four
                // big posters), repeating only at the edges the tilt uncovers.
                cw = (float)Math.Sqrt(w * (double)h / (x.O.Posters.Value * PosterRatio)) - gap;
                ch = cw * PosterRatio;
            }
            float bigW = w * 1.8f, bigH = h * 1.8f;
            int cols = (int)Math.Ceiling(bigW / (cw + gap)) + 1, rows = (int)Math.Ceiling(bigH / (ch + gap)) + 1;
            c.Save();
            c.Translate(w / 2f, h / 2f);
            c.RotateDegrees(x.Tilt(-14, -14));
            c.Translate(-bigW / 2, -bigH / 2);
            var grid = WallPicks(posters.Count, rows, cols);
            for (int r = 0; r < rows; r++)
                for (int col = 0; col < cols; col++)
                    MockCard(c, posters[grid[r, col]], col * (cw + gap) + (r % 2) * (cw / 2), r * (ch + gap), cw, ch, 8 * s, false, s);
            c.Restore();
            byte dimAlpha = x.O.Darken.HasValue ? (byte)Math.Round(255 * x.Dark(0)) : (byte)90;
            using (var dim = new SKPaint { Color = SKColors.Black.WithAlpha(dimAlpha) }) c.DrawRect(0, 0, w, h, dim);

            if (string.IsNullOrWhiteSpace(title)) return;
            float size = (background ? 110 : 104) * s * x.Scale;
            var face = x.Face(Display.Value);
            // One line, or two when the name is long; the band grows with the text.
            var (lines, fitted) = MockLines(title, face, w - 60 * s, size, x.Upper(true));
            using var probe = new SKPaint { Typeface = face, TextSize = fitted };
            var b = new SKRect(); probe.MeasureText("H", ref b);
            float lineStep = fitted * Step(face, Display.Value, 0.95f);
            float textH = b.Height + (lines.Length - 1) * lineStep;
            float bandH = Math.Max(size * 1.5f, textH + size * 0.6f);
            float by = (h - bandH) / 2;
            using (var band = new SKPaint { Color = MockBase.WithAlpha(215) }) c.DrawRect(0, by, w, bandH, band);
            using (var gold = new SKPaint { Color = Accent })
            {
                c.DrawRect(0, by - 2 * s, w, 4 * s, gold);
                c.DrawRect(0, by + bandH - 2 * s, w, 4 * s, gold);
            }
            float ty = by + (bandH - textH) / 2;
            foreach (var line in lines)
            {
                MockText(c, line, w / 2f, ty, face, fitted, x.Colour(SKColors.White), SKTextAlign.Center);
                ty += lineStep;
            }
        }

        // Which poster goes on each card of a wall of rows x cols (odd rows sit half a card to the
        // right). Posters repeat to fill the wall, but never touch a card showing the same poster:
        // not the card beside it, nor the two cards above or below (the diagonal neighbours of a
        // brick wall). With enough films a repeat is also kept off the ring around those. Three
        // films use a fixed three-colour pattern (the only way to keep them apart); with one or
        // two films touching repeats cannot be avoided.
        internal static int[,] WallPicks(int count, int rows, int cols)
        {
            var grid = new int[rows, cols];
            if (count == 3)
            {
                for (int r = 0; r < rows; r++)
                    for (int col = 0; col < cols; col++)
                        grid[r, col] = (((col - r / 2 + 2 * r) % 3) + 3) % 3;
                return grid;
            }
            int next = 0;
            for (int r = 0; r < rows; r++)
                for (int col = 0; col < cols; col++)
                {
                    var touching = new HashSet<int>();
                    var near = new HashSet<int>();
                    void Mark(HashSet<int> set, int rr, int cc) { if (rr >= 0 && cc >= 0 && cc < cols) set.Add(grid[rr, cc]); }
                    Mark(touching, r, col - 1); Mark(near, r, col - 2);
                    if (r > 0)
                    {
                        int a = r % 2 == 1 ? col : col - 1;   // odd rows sit half a card to the right
                        Mark(touching, r - 1, a); Mark(touching, r - 1, a + 1);
                        Mark(near, r - 1, a - 1); Mark(near, r - 1, a + 2);
                    }
                    if (r > 1) { Mark(near, r - 2, col - 1); Mark(near, r - 2, col); Mark(near, r - 2, col + 1); }
                    near.UnionWith(touching);
                    int pick = next % count;
                    var avoid = count > near.Count ? near : touching;
                    for (int tries = 0; tries < count && avoid.Contains(pick); tries++)
                        pick = (pick + 1) % count;
                    grid[r, col] = pick;
                    next = pick + 1;
                }
            return grid;
        }

        // ── hero strip ──────────────────────────────────────────────────────────────────
        // Poster: the first title as a lightly blurred hero fading to dark, a two-tone title,
        // and a strip of five cards along the bottom.
        private static void DrawHeroStripPoster(SKCanvas c, List<SKBitmap> posters, string title, int w, int h, Ctx x)
        {
            float s = MockScale(false, w), sy = h / 1000f;
            MockBackdrop(c, posters[0], w, h, 22 * s, x.Dark(0.25f));
            Scrim(c, SKRect.Create(0, 0, w, h), Dir.Down, 0.35f, 1f, 245);
            float gap = 12 * s, margin = 30 * s, stripTop;
            float tilt = x.Tilt(-5, 0);
            if (x.O.Rows == null && (x.O.Posters == null || x.O.Posters.Value - 1 <= 7))
            {
                int n = x.O.Posters != null ? Math.Max(1, Math.Min(x.O.Posters.Value - 1, 10)) : 5;
                float cw = (w - 2 * margin - gap * (n - 1)) / n, ch = cw * PosterRatio;
                stripTop = h - ch - 34 * sy;
                var strip = posters.Skip(1).Take(n).ToList();
                if (strip.Count == 0) strip = posters.Take(1).ToList();
                if (x.O.Posters != null && strip.Count < n) strip = Spread(strip, new[] { n });
                float x0 = (w - (strip.Count * cw + (strip.Count - 1) * gap)) / 2;
                float top = stripTop;
                Tilted(c, tilt, w / 2f, stripTop + ch / 2, strip.Count * cw + (strip.Count - 1) * gap, ch, w - margin, ch + 68 * sy, () =>
                {
                    for (int i = 0; i < strip.Count; i++)
                        MockCard(c, strip[i], x0 + i * (cw + gap), top, cw, ch, 10 * s, false, s);
                });
            }
            else
            {
                // Rows set, or more cards than one strip holds: the strip in rows (spread evenly;
                // full rows unless Posters is set), taking at most 56% of the height.
                // Rows set, Posters on Auto: as many cards a row as fill the width; a count with
                // Rows on Auto: the rows that give the biggest cards.
                bool exact = x.O.Posters != null;
                int want = exact ? x.O.Posters!.Value - 1 : HeroRowsCount(x.O.Rows!.Value, false);
                var pool = posters.Count > 1 ? posters.Skip(1).ToList() : posters.Take(1).ToList();
                var box = HeroPosterBox(w, h);
                int rows = RowsFor(x.O, m => BestRows(Math.Max(1, want), box.W, box.H, box.Gap, maxRows: m));
                if (x.O.Rows != null && exact) x.RowsUsed = rows;
                int n = Math.Max(1, CardsToUse(pool.Count, Math.Max(1, want), rows, exact));
                var counts = EvenRows(n, rows);
                pool = Spread(pool, counts);
                int most = counts.Max();
                float cw = (w - 2 * margin - gap * (most - 1)) / most;
                float maxH = h * 0.56f;
                if (counts.Length * cw * PosterRatio + (counts.Length - 1) * gap > maxH) cw = (maxH - (counts.Length - 1) * gap) / counts.Length / PosterRatio;
                float ch = cw * PosterRatio, blockH = counts.Length * ch + (counts.Length - 1) * gap;
                stripTop = h - blockH - 34 * sy;
                float top = stripTop;
                Tilted(c, tilt, w / 2f, stripTop + blockH / 2, most * cw + (most - 1) * gap, blockH, w - margin, blockH + 68 * sy, () =>
                {
                    int at = 0;
                    for (int r = 0; r < counts.Length; r++)
                    {
                        float x0 = (w - (counts[r] * cw + (counts[r] - 1) * gap)) / 2;
                        for (int i = 0; i < counts[r]; i++, at++)
                            MockCard(c, pool[at], x0 + i * (cw + gap), top + r * (ch + gap), cw, ch, 10 * s, false, s);
                    }
                });
            }
            if (string.IsNullOrWhiteSpace(title)) return;
            var face = x.Face(Display.Value);
            var (lines, size) = MockLines(title, face, w - 80 * s, 132 * s * x.Scale, x.Upper(true), preferTwo: true);
            float lineH = size * Step(face, Display.Value, 0.83f);
            float y = stripTop - 30 * sy - lines.Length * lineH - (size - lineH);
            for (int i = 0; i < lines.Length; i++)
                MockText(c, lines[i], w / 2f, y + i * lineH, face, size, x.TwoTone(lines.Length > 1 && i == 0), SKTextAlign.Center);
        }

        // Background: blurred hero with a dark left-to-right fade, title on the left, and a strip
        // of six cards with drop shadows underneath.
        private static void DrawHeroStripBackground(SKCanvas c, List<SKBitmap> posters, string title, int w, int h, Ctx x)
        {
            float s = MockScale(true, w);
            MockBackdrop(c, posters[0], w, h, 26 * s, x.Dark(0.35f));
            Scrim(c, SKRect.Create(0, 0, w, h), Dir.Left, 0f, 1f, 235);
            float cw = 130 * s, ch = 195 * s, gap = 14 * s, x0 = 70 * s;
            float tilt = x.Tilt(-4, 0);
            var face = x.Face(Display.Value);
            var (lines, size) = MockLines(string.IsNullOrWhiteSpace(title) ? "X" : title, face, w * 0.55f, 150 * s * x.Scale, x.Upper(true), preferTwo: true);
            if (x.O.Rows == null && (x.O.Posters == null || x.O.Posters.Value - 1 <= 10))
            {
                int take = x.O.Posters != null ? Math.Max(1, Math.Min(x.O.Posters.Value - 1, 14)) : 6;
                var strip = posters.Count > 1 ? posters.Skip(1).Take(take).ToList() : posters.Take(1).ToList();
                if (x.O.Posters != null && strip.Count < take) strip = Spread(strip, new[] { take });
                if (strip.Count > 6) { cw = Math.Min(cw, (w - 2 * x0 - gap * (strip.Count - 1)) / strip.Count); ch = cw * PosterRatio; }
                float stripW = strip.Count * cw + (strip.Count - 1) * gap;
                Tilted(c, tilt, x0 + stripW / 2, h - ch / 2 - 60 * s, stripW, ch, w - x0, ch + 80 * s, () =>
                {
                    for (int i = 0; i < strip.Count; i++)
                        MockCard(c, strip[i], x0 + i * (cw + gap), h - ch - 60 * s, cw, ch, 10 * s, true, s);
                });
            }
            else
            {
                // Rows set, or more cards than one strip holds: the strip in rows under the title
                // (spread evenly; full rows unless Posters is set).
                // The title keeps at most 40% of the height under its top so the rows get room;
                // the rows run across the full width (Rows set, Posters on Auto: as many cards
                // a row as fill it; a count with Rows on Auto: the rows giving the biggest cards).
                bool hasTitle = !string.IsNullOrWhiteSpace(title);
                float lineStep = Step(face, Display.Value, 0.86f), maxTitle = HeroTitleShare * (h - 210 * s);
                if (hasTitle && lines.Length * size * lineStep > maxTitle)
                    (lines, size) = MockLines(title, face, w * 0.55f, maxTitle / (lines.Length * lineStep), x.Upper(true), preferTwo: true);
                float titleBottom = !hasTitle ? 60 * s : 150 * s + lines.Length * size * lineStep;
                float maxH = h - 60 * s - titleBottom - 30 * s;
                var box = HeroBackgroundBox(w);
                bool exact = x.O.Posters != null;
                int want = exact ? x.O.Posters!.Value - 1 : HeroRowsCount(x.O.Rows!.Value, true);
                var pool = posters.Count > 1 ? posters.Skip(1).ToList() : posters.Take(1).ToList();
                int rows = RowsFor(x.O, m => BestRows(Math.Max(1, want), box.W, maxH, box.Gap, box.MaxCw, m));
                if (x.O.Rows != null && exact) x.RowsUsed = rows;
                int n = Math.Max(1, CardsToUse(pool.Count, Math.Max(1, want), rows, exact));
                var counts = EvenRows(n, rows);
                pool = Spread(pool, counts);
                int most = counts.Max();
                cw = Math.Min(cw, (w - 2 * x0 - gap * (most - 1)) / most);
                if (counts.Length * cw * PosterRatio + (counts.Length - 1) * gap > maxH) cw = (maxH - (counts.Length - 1) * gap) / counts.Length / PosterRatio;
                ch = cw * PosterRatio;
                float blockW = most * cw + (most - 1) * gap, blockH = counts.Length * ch + (counts.Length - 1) * gap, top = h - 60 * s - blockH;
                float cwF = cw, chF = ch;
                Tilted(c, tilt, x0 + blockW / 2, top + blockH / 2, blockW, blockH, w - x0, blockH + 80 * s, () =>
                {
                    int at = 0;
                    for (int r = 0; r < counts.Length; r++)
                        for (int i = 0; i < counts[r]; i++, at++)
                            MockCard(c, pool[at], x0 + i * (cwF + gap), top + r * (chF + gap), cwF, chF, 10 * s, true, s);
                });
            }
            if (string.IsNullOrWhiteSpace(title)) return;
            float y = 150 * s;
            for (int i = 0; i < lines.Length; i++)
            {
                MockText(c, lines[i], x0, y, face, size, x.TwoTone(lines.Length > 1 && i == 0));
                y += size * Step(face, Display.Value, 0.86f);
            }
        }

        // ── spotlight split (background) ────────────────────────────────────────────────
        // The first title's poster on the right, fading into dark on the left; a gold
        // "COLLECTION" kicker, the title in Anton (mixed case) and four cards on the left.
        private static void DrawSpotlight(SKCanvas c, List<SKBitmap> posters, string title, int w, int h, Ctx x)
        {
            float s = MockScale(true, w);
            c.Clear(new SKColor(10, 10, 14));
            var hero = posters[0];
            float heroW = h * 0.9f;
            using (var p = new SKPaint { FilterQuality = SKFilterQuality.High })
                c.DrawBitmap(hero, CoverSource(hero.Width, hero.Height, (int)heroW, h), SKRect.Create(w - heroW, 0, heroW, h), p);
            if (x.O.Darken > 0)
                using (var dim = new SKPaint { Color = SKColors.Black.WithAlpha((byte)Math.Round(255 * x.Dark(0))) }) c.DrawRect(w - heroW, 0, heroW, h, dim);
            // Dark from the left edge to 45% of the width, then fading out to the right edge.
            using (var fade = new SKPaint
            {
                Shader = SKShader.CreateLinearGradient(new SKPoint(0, 0), new SKPoint(w, 0),
                    new[] { new SKColor(10, 10, 14, 255), new SKColor(10, 10, 14, 255), new SKColor(10, 10, 14, 0) },
                    new[] { 0f, 0.45f, 1f }, SKShaderTileMode.Clamp)
            }) c.DrawRect(0, 0, w, h, fade);

            float left = 70 * s;
            MockText(c, "COLLECTION", left, 120 * s, Display.Value, 34 * s, Accent);
            float y = 160 * s;
            if (!string.IsNullOrWhiteSpace(title))
            {
                var face = x.Face(Heavy.Value);
                var (lines, size) = MockLines(title, face, w * 0.42f, 120 * s * x.Scale, x.Upper(false), preferTwo: true);
                if (x.O.Rows != null || (x.O.Posters != null && x.O.Posters.Value - 1 > 6))
                {
                    // Thumbnails in rows: the title gives up height so each row keeps a usable size.
                    int want = x.O.Posters != null ? x.O.Posters.Value - 1 : SpotlightRowsCount(x.O.Rows!.Value);
                    int rowsWanted = Math.Min(RowsFor(x.O, m => SpotlightAutoRows(want, w, h, s, m)), Math.Max(1, want));
                    // The rows reserve at most 36% of the height (they shrink to fit), so the title
                    // keeps a readable size with many rows.
                    float limit = h - 80 * s - SpotlightReserve(rowsWanted, h, s);
                    if (y + lines.Length * size * 1.25f > limit)
                        (lines, size) = MockLines(title, face, w * 0.42f, Math.Max(40 * s, (limit - y) / (lines.Length * 1.25f)), x.Upper(false), preferTwo: true);
                }
                foreach (var line in lines)
                {
                    MockText(c, line, left - 4 * s, y, face, size, x.Colour(SKColors.White));
                    y += size * 1.25f;
                }
            }
            float tilt = x.Tilt(-5, 0);
            if (x.O.Rows == null && (x.O.Posters == null || x.O.Posters.Value - 1 <= 6))
            {
                float cardsY = Math.Max(500 * s, y + 30 * s);
                int take = x.O.Posters != null ? Math.Max(1, Math.Min(x.O.Posters.Value - 1, 10)) : 4;
                var strip = posters.Skip(1).Take(take).ToList();
                if (x.O.Posters != null && strip.Count < take) strip = Spread(strip.Count > 0 ? strip : posters.Take(1).ToList(), new[] { take });
                float step = 124 * s, cardW = 110 * s, cardH = 165 * s;
                if (strip.Count > 4) { step = (w * 0.5f - left) / strip.Count; cardW = step * 110 / 124f; cardH = cardW * PosterRatio; }
                float stripW = (strip.Count - 1) * step + cardW;
                Tilted(c, tilt, left + stripW / 2, cardsY + cardH / 2, stripW, cardH, w * 0.5f, cardH + 40 * s, () =>
                {
                    for (int i = 0; i < strip.Count; i++)
                        MockCard(c, strip[i], left + i * step, cardsY, cardW, cardH, 8 * s, false, s);
                });
            }
            else
            {
                // Rows set, or more thumbnails than one row holds: thumbnails in rows under the
                // title within the left half (spread evenly; full rows unless Posters is set).
                // Rows set, Posters on Auto: as many thumbnails a row as fill the half; a count
                // with Rows on Auto: the rows giving the biggest thumbnails.
                bool exact = x.O.Posters != null;
                int want = exact ? x.O.Posters!.Value - 1 : SpotlightRowsCount(x.O.Rows!.Value);
                var pool = posters.Skip(1).ToList();
                if (pool.Count == 0 && exact) pool = posters.Take(1).ToList();
                int rows = RowsFor(x.O, m => SpotlightAutoRows(Math.Max(1, want), w, h, s, m));
                if (x.O.Rows != null && exact) x.RowsUsed = rows;
                int n = CardsToUse(pool.Count, Math.Max(1, want), rows, exact);
                if (n > 0 && pool.Count > 0)
                {
                    var counts = EvenRows(n, rows);
                    pool = Spread(pool, counts);
                    int most = counts.Max();
                    float top = y + 30 * s, availH = h - 50 * s - top, availW = w * 0.5f - left, gap = 14 * s;
                    float cardW = Math.Min(110 * s, (availW - gap * (most - 1)) / most);
                    if (counts.Length * cardW * PosterRatio + (counts.Length - 1) * gap > availH)
                        cardW = (availH - (counts.Length - 1) * gap) / counts.Length / PosterRatio;
                    float cardH = cardW * PosterRatio;
                    float blockW = most * cardW + (most - 1) * gap, blockH = counts.Length * cardH + (counts.Length - 1) * gap;
                    Tilted(c, tilt, left + blockW / 2, top + blockH / 2, blockW, blockH, availW + left * 0.5f, availH + 30 * s, () =>
                    {
                        int at = 0;
                        for (int r = 0; r < counts.Length; r++)
                            for (int i = 0; i < counts[r]; i++, at++)
                                MockCard(c, pool[at], left + i * (cardW + gap), top + r * (cardH + gap), cardW, cardH, 8 * s, false, s);
                    });
                }
            }
        }

        // ── ranked (Top 10) ─────────────────────────────────────────────────────────────
        // Netflix-style numbered cards: big outlined numerals with a poster tucked against each.
        // Poster: a red "TOP 10", the title, and a 2x2 of ranks 1-4. Background: the title and
        // ranks 1-5 in a row.
        private static void DrawRanked(SKCanvas c, List<SKBitmap> posters, string title, bool background, int w, int h, Ctx cx)
        {
            float s = MockScale(background, w);
            c.Clear(new SKColor(14, 14, 16));
            var numFill = new SKColor(14, 14, 16);
            var numStroke = new SKColor(125, 125, 133);
            float cw = 168 * s, ch = 252 * s;

            void Numbered(int rank, float x, float y, float numSize, float tuck, float cardY)
            {
                string num = rank.ToString();
                MockText(c, num, x, y, Heavy.Value, numSize, numFill, SKTextAlign.Left, (background ? 5 : 4) * s, numStroke);
                using var probe = new SKPaint { Typeface = Heavy.Value, TextSize = numSize };
                var b = new SKRect(); probe.MeasureText(num, ref b);
                float right = x + b.Right;
                MockCard(c, posters[(rank - 1) % posters.Count], right - tuck, cardY, cw, ch, 10 * s, true, s);
            }

            if (background)
            {
                if (!string.IsNullOrWhiteSpace(title))
                {
                    var face = cx.Face(Display.Value);
                    var (lines, size) = MockLines(title, face, w - 120 * s, 64 * s * cx.Scale, cx.Upper(true));
                    float ty = 46 * s;
                    foreach (var line in lines) { MockText(c, line, 60 * s, ty, face, size, cx.Colour(SKColors.White)); ty += size * Step(face, Display.Value, 0.95f); }
                }
                Tilted(c, cx.Tilt(-4, 0), w / 2f, 400 * s, w, 330 * s, w, 560 * s, () =>
                {
                    for (int i = 0; i < Math.Min(5, posters.Count); i++)
                        Numbered(i + 1, 30 * s + i * 245 * s, 205 * s, 330 * s, 34 * s, 240 * s);
                });
            }
            else
            {
                float sy = h / 1000f;
                MockText(c, "TOP 10", w / 2f, 34 * sy, Heavy.Value, 120 * s, RankRed, SKTextAlign.Center);
                if (!string.IsNullOrWhiteSpace(title))
                {
                    var face = cx.Face(Display.Value);
                    var (lines, size) = MockLines(title, face, w - 80 * s, 54 * s * cx.Scale, cx.Upper(true));
                    float sz = lines.Length > 1 ? Math.Min(size, 44 * s * cx.Scale) : size, ty = 196 * sy;
                    foreach (var line in lines) { MockText(c, line, w / 2f, ty, face, sz, cx.Colour(SKColors.White), SKTextAlign.Center); ty += sz * Step(face, Display.Value, 0.95f); }
                }
                var pos = new[] { (40f, 290f), (360f, 290f), (40f, 630f), (360f, 630f) };
                Tilted(c, cx.Tilt(-4, 0), w / 2f, 640 * sy, w, 700 * sy, w, 760 * sy, () =>
                {
                    for (int i = 0; i < Math.Min(4, posters.Count); i++)
                        Numbered(i + 1, pos[i].Item1 * s, (pos[i].Item2 - 10) * sy, 230 * s, 28 * s, (pos[i].Item2 + 20) * sy);
                });
            }
        }

        // ── shared pieces ───────────────────────────────────────────────────────────────

        // The Customise options as the drawing code needs them. Every getter returns the
        // style's own value when the option is not set, so art without options is unchanged.
        private sealed class Ctx
        {
            public Ctx(ArtOptions o, bool background) { O = o; Background = background; }
            public ArtOptions O { get; }
            public bool Background { get; }
            // Rows and Posters both set: the rows the cards were actually drawn on (Rows is "at
            // most" then); null when the style did not lay the cards out in rows.
            public int? RowsUsed { get; set; }
            public SKTypeface Face(SKTypeface own) => O.Font != null ? ArtFonts.Face(O.Font) : own;
            public bool Upper(bool own) => O.Case == null ? own : O.Case == "upper";
            public SKColor Colour(SKColor own) => O.ColourValue ?? own;
            // Two-tone titles (gold first line) unless a colour was chosen.
            public SKColor TwoTone(bool accentLine) => O.ColourValue ?? (accentLine ? Accent : SKColors.White);
            public float Scale => O.SizeFactor;
            public float Dark(float own) => O.Darken.HasValue ? O.Darken.Value / 100f : own;
            // Degrees to turn: "left" = leftAngle, "right" = the mirror, "straight" = 0.
            public float Tilt(float leftAngle, float own) => O.Tilt == null ? own : O.Tilt == "left" ? leftAngle : O.Tilt == "right" ? -leftAngle : 0;
        }

        // Line spacing: the style's own for its own font; other fonts get room for their
        // ascenders and descenders.
        private static float Step(SKTypeface used, SKTypeface own, float factor) => used == own ? factor : Math.Max(factor, 1.12f);

        // A title moved by Customise to one of nine spots (pos: t/m/b + l/c/r), over the art, with
        // a soft dark fade behind it so it stays readable.
        private static void DrawFreeTitle(SKCanvas c, string title, string pos, bool background, int w, int h, Ctx x, ArtOptionDefaults d)
        {
            var face = ArtFonts.Face(x.O.Font ?? d.Font);
            bool upper = (x.O.Case ?? d.Case) == "upper";
            var colour = x.O.ColourValue ?? (SKColor.TryParse(d.Colour, out var dc) ? dc : SKColors.White);
            float s = MockScale(background, w);
            float m = (background ? 64 : 46) * s;
            char row = pos[0], col = pos[1];
            float maxW = col == 'c' ? w - 2 * m : w * 0.62f;
            var (lines, size) = MockLines(title, face, maxW, (background ? 120 : 108) * s * x.Scale, upper);
            using var probe = new SKPaint { Typeface = face, TextSize = size, IsAntialias = true };
            float cap = CapHeight(probe);
            float step = size * Step(face, Display.Value, 0.95f);
            float textH = cap + (lines.Length - 1) * step;
            float top = row == 't' ? m : row == 'b' ? h - m - textH : (h - textH) / 2;
            float fade = textH + 2.2f * m;
            if (row == 'b') Scrim(c, SKRect.Create(0, h - fade - m, w, fade + m), Dir.Down, 0, 0.7f, 225, 1.3f);
            else if (row == 't') Scrim(c, SKRect.Create(0, 0, w, fade + m), Dir.Up, 0, 0.7f, 225, 1.3f);
            else using (var band = new SKPaint { Color = MockBase.WithAlpha(165) }) c.DrawRect(0, top - m * 0.6f, w, textH + m * 1.2f, band);

            float tx = col == 'l' ? m : col == 'r' ? w - m : w / 2f;
            var align = col == 'l' ? SKTextAlign.Left : col == 'r' ? SKTextAlign.Right : SKTextAlign.Center;
            float y = top + cap;
            foreach (var line in lines)
            {
                using (var sp = new SKPaint
                {
                    Typeface = face, TextSize = size, IsAntialias = true, TextAlign = align, Color = SKColors.Black.WithAlpha(180),
                    MaskFilter = SKMaskFilter.CreateBlur(SKBlurStyle.Normal, Math.Max(4, size / 14))
                }) c.DrawText(line, tx, y, sp);
                using (var fill = new SKPaint { Typeface = face, TextSize = size, IsAntialias = true, TextAlign = align, Color = colour })
                    c.DrawText(line, tx, y, fill);
                y += step;
            }
        }

        // The most colourful of the first five posters makes the best blurred backdrop
        // (near-white or near-black posters give a muddy grey). Ties go to the earliest.
        private static SKBitmap MostColourful(List<SKBitmap> posters)
        {
            SKBitmap best = posters[0];
            double bestScore = -1;
            foreach (var p in posters.Take(5))
            {
                using var small = p.Resize(new SKImageInfo(32, 48), SKFilterQuality.Low);
                if (small == null) continue;
                double s = 0, v = 0;
                for (int y = 0; y < small.Height; y++)
                    for (int x = 0; x < small.Width; x++)
                    {
                        small.GetPixel(x, y).ToHsv(out _, out var sat, out var val);
                        s += sat / 100; v += val / 100;
                    }
                int n = small.Width * small.Height;
                double score = (s / n) * (1 - Math.Abs(v / n - 0.5));
                if (score > bestScore + 1e-9) { best = p; bestScore = score; }
            }
            return best;
        }

        private enum Dir { Down, Up, Left }

        // Base colour fading in across a box: Down = clear at the top, dense at the bottom (Up and
        // Left likewise). start/end are fractions of the way across; alpha = max * t^curve.
        private static void Scrim(SKCanvas c, SKRect box, Dir dir, float start, float end, byte maxAlpha, float curve = 1f)
        {
            const int stops = 32;
            var colors = new SKColor[stops + 1];
            var pos = new float[stops + 1];
            float span = Math.Max(1e-4f, end - start);
            for (int i = 0; i <= stops; i++)
            {
                float p = i / (float)stops;
                float t = Clamp01((p - start) / span);
                colors[i] = Base.WithAlpha((byte)Math.Round(maxAlpha * Math.Pow(t, curve)));
                pos[i] = p;
            }
            SKPoint from, to;
            switch (dir)
            {
                case Dir.Up: from = new SKPoint(box.Left, box.Bottom); to = new SKPoint(box.Left, box.Top); break;
                case Dir.Left: from = new SKPoint(box.Right, box.Top); to = new SKPoint(box.Left, box.Top); break;
                default: from = new SKPoint(box.Left, box.Top); to = new SKPoint(box.Left, box.Bottom); break;
            }
            using var paint = new SKPaint { Shader = SKShader.CreateLinearGradient(from, to, colors, pos, SKShaderTileMode.Clamp) };
            c.DrawRect(box, paint);
        }

        // Source crop that fills w x h (cover), with the crop's focus at (fx, fy) of the image.
        private static SKRect CoverSourceAt(int srcW, int srcH, int w, int h, float fx, float fy)
        {
            float scale = Math.Max(w / (float)srcW, h / (float)srcH);
            float cw = w / scale, ch = h / scale;
            float x = (srcW - cw) * fx, y = (srcH - ch) * fy;
            return new SKRect(x, y, x + cw, y + ch);
        }

        private enum TextAlign { Center, Left }

        private readonly struct TitleLayout
        {
            public TitleLayout(string[] lines, float size, float cap, float leading, SKTypeface face)
            { Lines = lines; Size = size; Cap = cap; Leading = leading; Face = face; }
            public string[] Lines { get; }
            public float Size { get; }
            public float Cap { get; }
            public float Leading { get; }
            public SKTypeface Face { get; }
            public float Height => Cap + (Lines.Length - 1) * Leading * Size;
        }

        // Fits the title into maxW x maxH on one line or two balanced lines (two only when that
        // makes the type at least 18% larger). Bebas Neue upper case by default; Anton for spotlight.
        private static TitleLayout LayoutTitle(string title, float maxW, float maxH, float maxSize,
            bool upper = true, SKTypeface? face = null, float leading = 1.12f)
        {
            face ??= Display.Value;
            string text = string.Join(" ", (upper ? title.ToUpperInvariant() : title).Trim()
                .Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries));
            using var probe = new SKPaint { Typeface = face, TextSize = 100, IsAntialias = true };
            float capAt100 = CapHeight(probe);

            (string[] lines, float size) Fit(string[] lines)
            {
                float widest = lines.Max(l => probe.MeasureText(l));
                float byWidth = 100 * maxW / Math.Max(1, widest);
                float byHeight = maxH / (capAt100 / 100 + (lines.Length - 1) * leading);
                return (lines, Math.Min(maxSize, Math.Min(byWidth, byHeight)));
            }

            var best = Fit(new[] { text });
            var words = text.Split(' ');
            if (words.Length > 1)
            {
                (string[] lines, float size)? two = null;
                for (int i = 1; i < words.Length; i++)
                {
                    var cand = Fit(new[] { string.Join(" ", words.Take(i)), string.Join(" ", words.Skip(i)) });
                    if (two == null || cand.size > two.Value.size) two = cand;
                }
                if (two != null && two.Value.size >= best.size * 1.18f) best = two.Value;
            }
            return new TitleLayout(best.lines, best.size, capAt100 * best.size / 100, leading, face);
        }

        // Draws a laid-out title with its cap line at top. Two-line titles get an accent first
        // line (twoTone); a soft shadow sits underneath (shadow).
        private static void DrawTitle(SKCanvas c, TitleLayout t, float x, float top, TextAlign align,
            bool twoTone = true, bool shadow = true, SKColor? colour = null)
        {
            var face = t.Face;
            var skAlign = align == TextAlign.Left ? SKTextAlign.Left : SKTextAlign.Center;
            float y = top + t.Cap;
            for (int i = 0; i < t.Lines.Length; i++)
            {
                var lineColour = colour ?? (twoTone && t.Lines.Length > 1 && i == 0 ? Accent : SKColors.White);
                if (shadow)
                {
                    using var sp = new SKPaint
                    {
                        Typeface = face, TextSize = t.Size, IsAntialias = true, TextAlign = skAlign,
                        Color = SKColors.Black.WithAlpha(170),
                        MaskFilter = SKMaskFilter.CreateBlur(SKBlurStyle.Normal, Math.Max(4, t.Size / 14))
                    };
                    c.DrawText(t.Lines[i], x, y, sp);
                }
                using var fill = new SKPaint { Typeface = face, TextSize = t.Size, IsAntialias = true, TextAlign = skAlign, Color = lineColour };
                c.DrawText(t.Lines[i], x, y, fill);
                y += t.Leading * t.Size;
            }
        }

        private static float CapHeight(SKPaint paint)
        {
            var bounds = new SKRect();
            paint.MeasureText("H", ref bounds);
            return bounds.Height;
        }

        private readonly struct GridLayout
        {
            public GridLayout(int count, int cols, int rows, int cardW, int cardH, int gap, int[]? rowCounts = null)
            { Count = count; Cols = cols; Rows = rows; CardW = cardW; CardH = cardH; Gap = gap; RowCounts = rowCounts; }
            public int[]? RowCounts { get; }
            public int Count { get; }
            public int Cols { get; }
            public int Rows { get; }
            public int CardW { get; }
            public int CardH { get; }
            public int Gap { get; }

            // Top-left of each card: block centred in the region, partial last row centred.
            public List<SKPoint> Cells(SKRect region)
            {
                float totalH = Rows * CardH + (Rows - 1) * Gap;
                float oy = region.Top + (region.Height - totalH) / 2;
                var cells = new List<SKPoint>();
                if (RowCounts != null)
                {
                    // Rows of their own lengths (evenly spread), each centred.
                    for (int r = 0; r < RowCounts.Length; r++)
                    {
                        float rowW = RowCounts[r] * CardW + (RowCounts[r] - 1) * Gap;
                        float ox = region.Left + (region.Width - rowW) / 2;
                        for (int col = 0; col < RowCounts[r]; col++)
                            cells.Add(new SKPoint((float)Math.Round(ox + col * (CardW + Gap)), (float)Math.Round(oy + r * (CardH + Gap))));
                    }
                    return cells;
                }
                for (int i = 0; i < Count; i++)
                {
                    int r = i / Cols, col = i % Cols;
                    int inRow = Math.Min(Cols, Count - r * Cols);
                    float rowW = inRow * CardW + (inRow - 1) * Gap;
                    float ox = region.Left + (region.Width - rowW) / 2;
                    cells.Add(new SKPoint((float)Math.Round(ox + col * (CardW + Gap)), (float)Math.Round(oy + r * (CardH + Gap))));
                }
                return cells;
            }
        }

        // How many posters (<= n, <= cap) and which grid fills w x h best: area coverage x
        // count^0.35, so more posters win unless they get much smaller. A partial last row is
        // only allowed when every poster is used.
        private static GridLayout BestGrid(int n, int w, int h, int gap, int cap, int? maxRows, int? exactRows = null, bool exactCount = false)
        {
            if (exactCount)
            {
                // Customise "Posters": exactly that many (or all there are), on the rows that give
                // the biggest cards (or the chosen rows), spread as evenly as possible (10 on 3 = 4/3/3).
                int k = Math.Max(1, Math.Min(n, cap));
                GridLayout? pick = null;
                // With Rows also set, Rows is the most: the rows up to it that give the biggest cards.
                int lo = 1, hi = Math.Min(k, exactRows ?? maxRows ?? k);
                for (int rows = lo; rows <= hi; rows++)
                {
                    var counts = EvenRows(k, rows);
                    int cols = counts[0];
                    double cw = Math.Min((w - gap * (cols - 1)) / (double)cols, (h - gap * (counts.Length - 1)) / (double)counts.Length / PosterRatio);
                    if (cw <= 8) continue;
                    if (pick == null || cw > pick.Value.CardW + 0.5)
                        pick = new GridLayout(k, cols, counts.Length, (int)cw, (int)(cw * PosterRatio), gap, counts);
                }
                if (pick != null) return pick.Value;
            }
            else if (exactRows != null && n >= exactRows)
            {
                // Customise "Rows" without a count: full rows only (cols x rows <= the titles there are).
                GridLayout? full = null;
                double fullScore = -1;
                int r = exactRows.Value;
                for (int cols = 1; cols * r <= Math.Min(n, Math.Max(cap, r)); cols++)
                {
                    int k = cols * r;
                    double cw = Math.Min((w - gap * (cols - 1)) / (double)cols, (h - gap * (r - 1)) / (double)r / PosterRatio);
                    if (cw <= 8) continue;
                    double score = (k * cw * cw * PosterRatio) / ((double)w * h) * Math.Pow(k, 0.35);
                    if (score > fullScore) { fullScore = score; full = new GridLayout(k, cols, r, (int)cw, (int)(cw * PosterRatio), gap); }
                }
                if (full != null) return full.Value;
            }
            if (exactRows != null)
            {
                // Customise "Rows": that many rows when there are enough titles, else the best fit.
                var exact = BestGrid(n, w, h, gap, cap, exactRows, null);
                if (exact.Rows == exactRows || n < exactRows) return exact;
                GridLayout? forced = null;
                int k = Math.Min(n, cap);
                for (int cols = 1; cols <= k; cols++)
                {
                    if ((int)Math.Ceiling(k / (double)cols) != exactRows) continue;
                    double cw = Math.Min((w - gap * (cols - 1)) / (double)cols, (h - gap * (exactRows.Value - 1)) / (double)exactRows.Value / PosterRatio);
                    if (cw <= 8) continue;
                    if (forced == null || cw > forced.Value.CardW) forced = new GridLayout(k, cols, exactRows.Value, (int)cw, (int)(cw * PosterRatio), gap);
                }
                return forced ?? exact;
            }
            GridLayout? best = null;
            double bestScore = -1;
            for (int k = 1; k <= Math.Min(n, cap); k++)
                for (int cols = 1; cols <= k; cols++)
                {
                    int rows = (int)Math.Ceiling(k / (double)cols);
                    if (maxRows.HasValue && rows > maxRows.Value) continue;
                    if (rows * cols != k && k != n) continue;
                    if (rows > 1 && cols * rows - k >= cols) continue;
                    double cw = Math.Min((w - gap * (cols - 1)) / (double)cols, (h - gap * (rows - 1)) / (double)rows / PosterRatio);
                    if (cw <= 8) continue;
                    double ch = cw * PosterRatio;
                    double score = (k * cw * ch) / ((double)w * h) * Math.Pow(k, 0.35);
                    if (score > bestScore) { bestScore = score; best = new GridLayout(k, cols, rows, (int)cw, (int)ch, gap); }
                }
            return best ?? new GridLayout(1, 1, 1, Math.Max(1, w), Math.Max(1, (int)(w * PosterRatio)), gap);
        }
    }
}
