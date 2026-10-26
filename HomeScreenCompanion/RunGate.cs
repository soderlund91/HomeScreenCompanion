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
        private static readonly List<string> Waiters = new List<string>();
        private static volatile string _holder = "";

        /// <summary>The run that holds the gate now ("" when none).</summary>
        public static string Holder => _holder;

        public static bool Busy => Gate.CurrentCount == 0;

        /// <summary>"Run Group 'X' waiting for Tag &amp; Collection Sync to finish" for each waiting run; "" when none.</summary>
        public static string WaitingText
        {
            get
            {
                string holder = _holder;
                lock (Waiters)
                {
                    if (Waiters.Count == 0) return "";
                    return string.Join(" · ", Waiters.ConvertAll(w => $"{w} waiting for {(holder.Length > 0 ? holder : "the running sync")} to finish"));
                }
            }
        }

        /// <summary>For a Status endpoint: the runs waiting for the gate, shown after a running task's
        /// own step; "" when it is not running or is itself waiting (its step then says so).</summary>
        public static string WaitingFor(string step, bool running)
        {
            if (!running || (step ?? "").StartsWith("Waiting for", StringComparison.Ordinal)) return "";
            return WaitingText;
        }

        /// <summary>Waits (asynchronously) until no other run holds the gate. Dispose the result to release it.</summary>
        public static async Task<IDisposable> EnterAsync(string name, PluginLogger? logger, CancellationToken cancellationToken)
        {
            if (!Gate.Wait(0))
            {
                lock (Waiters) Waiters.Add(name);
                logger?.Info($"{name}: waiting for the running sync ({_holder}) to finish");
                try { await Gate.WaitAsync(cancellationToken).ConfigureAwait(false); }
                finally { lock (Waiters) Waiters.Remove(name); }
                logger?.Info($"{name}: the previous sync has finished — starting now");
            }
            _holder = name;
            return new Releaser();
        }

        /// <summary>Takes the gate only if it is free right now; null when another run holds it.</summary>
        public static IDisposable? TryEnter(string name)
        {
            if (!Gate.Wait(0)) return null;
            _holder = name;
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
                Gate.Release();
                RealPosterBadges.RunFinished();
            }
        }
    }
}
