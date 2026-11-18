using MediaBrowser.Model.Logging;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace HomeScreenCompanion
{
    /// <summary>
    /// One run at a time: the full sync, Run Group (sources and top-lists), the top-list section
    /// sync and Home Screen Sync all change the same tags, collections, home sections and settings.
    /// Run side by side they left collections half-updated, so a second run waits here until the
    /// first has finished. Only the outer entry points enter; code a run calls itself (e.g. the
    /// top-list section sync inside the full sync) must not, since the gate is not re-entrant.
    /// </summary>
    internal static class RunGate
    {
        private static readonly SemaphoreSlim Gate = new SemaphoreSlim(1, 1);
        private static volatile string _holder = "";

        /// <summary>The run that holds the gate now ("" when none).</summary>
        public static string Holder => _holder;

        public static bool Busy => Gate.CurrentCount == 0;

        // When the run that holds the gate started (ticks, 0 = no run).
        private static long _startTicks;

        /// <summary>
        /// The time every schedule is judged by: while a run holds the gate, the moment it took it,
        /// so the whole run (tags, collections, home rows, top-lists) agrees on which sources are in
        /// schedule even when a schedule starts or ends during it (a long run in Gentle mode). Else now.
        /// The Schedule Switch notices the change after the run and starts the next one.
        /// </summary>
        public static DateTime ScheduleNow
        {
            get
            {
                long t = Interlocked.Read(ref _startTicks);
                return t != 0 && Busy ? new DateTime(t, DateTimeKind.Local) : DateTime.Now;
            }
        }

        private static void Started(string name)
        {
            _holder = name;
            Interlocked.Exchange(ref _startTicks, DateTime.Now.Ticks);
            GentleMode.Begin();
        }

        /// <summary>Waits (asynchronously) until no other run holds the gate. Dispose the result to release it.</summary>
        public static async Task<IDisposable> EnterAsync(string name, PluginLogger? logger, CancellationToken cancellationToken)
        {
            if (!Gate.Wait(0))
            {
                logger?.Info($"{name}: waiting for the running sync ({_holder}) to finish");
                await Gate.WaitAsync(cancellationToken).ConfigureAwait(false);
                logger?.Info($"{name}: the previous sync has finished — starting now");
            }
            Started(name);
            return new Releaser();
        }

        /// <summary>Takes the gate only if it is free right now; null when another run holds it.</summary>
        public static IDisposable? TryEnter(string name)
        {
            if (!Gate.Wait(0)) return null;
            Started(name);
            return new Releaser();
        }

        /// <summary>Blocking variant for the synchronous entry points.</summary>
        public static IDisposable Enter(string name, PluginLogger? logger, CancellationToken cancellationToken)
            => EnterAsync(name, logger, cancellationToken).GetAwaiter().GetResult();

        private sealed class Releaser : IDisposable
        {
            private int _done;
            public void Dispose()
            {
                if (Interlocked.Exchange(ref _done, 1) != 0) return;
                _holder = "";
                Interlocked.Exchange(ref _startTicks, 0);
                Gate.Release();
                RealPosterBadges.RunFinished();
            }
        }
    }
}
