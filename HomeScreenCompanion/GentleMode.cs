using MediaBrowser.Controller.Session;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace HomeScreenCompanion
{
    /// <summary>
    /// Gentle mode: while someone is watching, a run does its many-item writes (item saves,
    /// merges, collection members, playlist changes, home rows for many users) in small slices
    /// with a pause between them, so Emby's database is never busy long enough for a stream to
    /// stall. With nobody watching every slice runs straight on, as fast as before.
    ///
    /// Heavy loops call <see cref="Wrote"/> (or <see cref="WroteAsync"/>) after each write that
    /// really changed something. A slice is full after <see cref="SliceSize"/> writes, or sooner
    /// when the writes timed with <see cref="Timed"/> took <see cref="SliceBudget"/> together
    /// (one slow write fills a slice on its own). When a slice is full the sessions are counted
    /// again: nobody watching means go on, anyone watching (playing or paused) means pause
    /// 2 × the slice's timed write time, at least <see cref="MinPause"/> and at most
    /// <see cref="MaxPause"/>: the database is then free at least two thirds of the time.
    /// A batch call (one Emby call that saves many items) is split with <see cref="Slices"/>.
    ///
    /// The numbers come from the test server (4 saves then 3 s kept segment times at their idle
    /// level with 4 transcoding streams) and from a production server with a ~700 MB library
    /// database, where one ranked copy's full write (save, media streams, cast, merge) could hold
    /// a playback-progress request up for 1–3 s.
    /// </summary>
    internal static class GentleMode
    {
        internal const int SliceSize = 4;
        /// <summary>Batch size with nobody watching: small enough that a viewer who starts mid-run is noticed after one batch.</summary>
        internal const int IdleSliceSize = 25;
        internal static readonly TimeSpan SliceBudget = TimeSpan.FromMilliseconds(1500);
        internal static readonly TimeSpan MinPause = TimeSpan.FromSeconds(3);
        internal static readonly TimeSpan MaxPause = TimeSpan.FromSeconds(15);

        private static ISessionManager? _sessions;
        private static readonly object Sync = new object();
        private static int _pending, _pauses, _streamsAtStart, _maxStreams;
        private static TimeSpan _paused = TimeSpan.Zero, _sliceWork = TimeSpan.Zero, _longestPause = TimeSpan.Zero, _longestWrite = TimeSpan.Zero;
        private static string _longestKind = "";
        private static readonly Dictionary<string, (int Count, TimeSpan Total, TimeSpan Max)> Times = new Dictionary<string, (int, TimeSpan, TimeSpan)>();

        internal static void Initialize(ISessionManager sessionManager) => _sessions = sessionManager;

        /// <summary>
        /// Sessions with something in play, paused ones included: a paused player resumes any
        /// moment and then needs its next segments and progress saves straight away.
        /// </summary>
        internal static int ActiveStreams()
        {
            var sessions = _sessions;
            if (sessions == null) return 0;
            try
            {
                return sessions.Sessions.Count(s => s != null && s.NowPlayingItem != null);
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); return 0; }
        }

        internal static bool Streaming => ActiveStreams() > 0;

        /// <summary>A run starts (RunGate): counters back to zero.</summary>
        internal static void Begin()
        {
            lock (Sync)
            {
                _pending = 0; _pauses = 0; _paused = TimeSpan.Zero; _sliceWork = TimeSpan.Zero; _longestPause = TimeSpan.Zero;
                _longestWrite = TimeSpan.Zero; _longestKind = "";
                Times.Clear();
                _streamsAtStart = ActiveStreams();
                _maxStreams = _streamsAtStart;
            }
        }

        /// <summary>The run's header: one plain line when people are watching as it starts.</summary>
        internal static void Announce(RunLog? log)
        {
            int n;
            lock (Sync) n = _streamsAtStart;
            if (n > 0 && log != null)
                log.Info($"  {(n == 1 ? "1 person is" : $"{n} people are")} watching: the sync runs in gentle mode and takes longer.");
        }

        /// <summary>
        /// For the run's summary: how much gentle mode slowed it down (when it paused) and how
        /// long the timed writes took (when there were any).
        /// </summary>
        internal static IEnumerable<string> SummaryLines()
        {
            var lines = new List<string>();
            lock (Sync)
            {
                if (_pauses > 0)
                    lines.Add($"Gentle mode:   {_pauses + 1} slices, {RunLog.Elapsed(_paused)} of pauses ({RunLog.Plural(_pauses, "pause")}, {MinPause.TotalSeconds:0}–{Math.Max(MinPause.TotalSeconds, Math.Ceiling(_longestPause.TotalSeconds)):0}s each) while up to {(_maxStreams == 1 ? "1 person was" : $"{_maxStreams} people were")} watching");
                if (Times.Count > 0)
                    lines.Add("Write times:   " + string.Join("  ·  ", Times.Select(t =>
                        $"{t.Key} {t.Value.Count}× avg {Ms(TimeSpan.FromTicks(t.Value.Total.Ticks / t.Value.Count))}, max {Ms(t.Value.Max)}"))
                        + $"  ·  longest single write: {_longestKind} {Ms(_longestWrite)}");
            }
            return lines;
        }

        private static string Ms(TimeSpan t) => t.TotalMilliseconds >= 1000 ? $"{t.TotalSeconds:0.0} s" : $"{t.TotalMilliseconds:0} ms";

        /// <summary>
        /// Runs one write and records how long it took: per kind for the summary, and for the
        /// current slice (a slow write fills the slice sooner and makes the pause after it longer).
        /// It does not pause itself: the caller's <see cref="Wrote"/> does.
        /// </summary>
        internal static T Timed<T>(string kind, Func<T> write)
        {
            var sw = Stopwatch.StartNew();
            try { return write(); }
            finally { Record(kind, sw.Elapsed); }
        }

        internal static void Timed(string kind, Action write) => Timed<bool>(kind, () => { write(); return true; });

        /// <summary>Like <see cref="Timed{T}"/> for a call that may find nothing to write: only recorded when it wrote (returned true).</summary>
        internal static bool TimedIfWrote(string kind, Func<bool> write)
        {
            var sw = Stopwatch.StartNew();
            bool wrote = write();
            if (wrote) Record(kind, sw.Elapsed);
            return wrote;
        }

        internal static async Task TimedAsync(string kind, Func<Task> write)
        {
            var sw = Stopwatch.StartNew();
            try { await write().ConfigureAwait(false); }
            finally { Record(kind, sw.Elapsed); }
        }

        internal static async Task<T> TimedAsync<T>(string kind, Func<Task<T>> write)
        {
            var sw = Stopwatch.StartNew();
            try { return await write().ConfigureAwait(false); }
            finally { Record(kind, sw.Elapsed); }
        }

        private static void Record(string kind, TimeSpan took)
        {
            lock (Sync)
            {
                _sliceWork += took;
                Times.TryGetValue(kind, out var t);
                Times[kind] = (t.Count + 1, t.Total + took, took > t.Max ? took : t.Max);
                if (took > _longestWrite) { _longestWrite = took; _longestKind = kind; }
            }
        }

        /// <summary>
        /// Call after a write (or <paramref name="count"/> writes) that changed something. Pauses
        /// when a slice is full and someone is watching. Throws when cancelled.
        /// </summary>
        internal static void Wrote(CancellationToken cancellationToken, int count = 1)
        {
            var wait = Due(count);
            if (wait > TimeSpan.Zero && cancellationToken.WaitHandle.WaitOne(wait))
                cancellationToken.ThrowIfCancellationRequested();
        }

        /// <summary>
        /// Between the steps of one long write (a ranked copy's cast): pauses when the steps'
        /// <see cref="Timed"/> time has filled the slice and someone is watching.
        /// </summary>
        internal static void Step(CancellationToken cancellationToken)
        {
            var wait = SliceDue(0);
            if (wait > TimeSpan.Zero && cancellationToken.WaitHandle.WaitOne(wait))
                cancellationToken.ThrowIfCancellationRequested();
        }

        /// <summary>
        /// After one step of a long write that still took longer than <see cref="LongStep"/> (one
        /// step can not be made smaller, e.g. a single person Emby takes long to look up): while
        /// someone is watching, pauses <see cref="LongStepFactor"/> × that step, within
        /// <see cref="MinPause"/>..<see cref="MaxPause"/>, so the database is free most of the
        /// time around it. Starts a new slice. Otherwise like <see cref="Step"/>.
        /// </summary>
        internal static void AfterStep(TimeSpan took, CancellationToken cancellationToken)
        {
            if (took <= LongStep) { Step(cancellationToken); return; }
            lock (Sync) { _pending = 0; _sliceWork = TimeSpan.Zero; }
            int streams = ActiveStreams();
            if (streams == 0) return;
            var pause = TimeSpan.FromTicks(Math.Min(MaxPause.Ticks, Math.Max(MinPause.Ticks, LongStepFactor * took.Ticks)));
            Paused(pause, streams);
            if (cancellationToken.WaitHandle.WaitOne(pause)) cancellationToken.ThrowIfCancellationRequested();
        }

        internal static readonly TimeSpan LongStep = TimeSpan.FromMilliseconds(300);
        internal const int LongStepFactor = 3;

        private static void Paused(TimeSpan pause, int streams)
        {
            lock (Sync)
            {
                _pauses++;
                _paused += pause;
                if (pause > _longestPause) _longestPause = pause;
                if (streams > _maxStreams) _maxStreams = streams;
            }
        }

        internal static async Task WroteAsync(CancellationToken cancellationToken, int count = 1)
        {
            var wait = Due(count);
            if (wait > TimeSpan.Zero) await Task.Delay(wait, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// The ids of one batch call in slices: <see cref="SliceSize"/> while someone is watching,
        /// <see cref="IdleSliceSize"/> otherwise. Checked again before every slice.
        /// </summary>
        internal static IEnumerable<T[]> Slices<T>(IReadOnlyList<T> items)
        {
            int i = 0;
            while (i < items.Count)
            {
                var slice = new T[Math.Min(BatchSize, items.Count - i)];
                for (int k = 0; k < slice.Length; k++) slice[k] = items[i + k];
                i += slice.Length;
                yield return slice;
            }
        }

        /// <summary>How many items the next batch call may take.</summary>
        internal static int BatchSize => Streaming ? SliceSize : IdleSliceSize;

        // How long to pause now: zero until a slice is full; then the sessions decide.
        private static TimeSpan Due(int count)
        {
            if (count <= 0) return TimeSpan.Zero;
            return SliceDue(count);
        }

        // count 0: only the timed write time fills the slice (a step of one longer write).
        private static TimeSpan SliceDue(int count)
        {
            TimeSpan work;
            lock (Sync)
            {
                _pending += count;
                if (_pending < SliceSize && _sliceWork < SliceBudget) return TimeSpan.Zero;
                _pending = 0;
                work = _sliceWork;
                _sliceWork = TimeSpan.Zero;
            }
            int streams = ActiveStreams();
            if (streams == 0) return TimeSpan.Zero;
            var pause = TimeSpan.FromTicks(Math.Min(MaxPause.Ticks, Math.Max(MinPause.Ticks, 2 * work.Ticks)));
            Paused(pause, streams);
            return pause;
        }
    }
}
