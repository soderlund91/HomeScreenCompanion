namespace HomeScreenCompanion
{
    /// <summary>
    /// Dry Run Mode (Settings). Every sync and task runs as a simulation that changes nothing in
    /// Emby (full sync, Run Group, Preview, Copy & Sync, the top-list section sync, the merge task,
    /// original posters, Continue Watching bump, schedule switch), and the parts that change the
    /// library on their own in the background (real-time tagging, linking top-list copies, the
    /// top-list collection mirror, RankedCopyGuard, new users and libraries) leave it alone and log
    /// what they would do. What the admin clicks on the settings page (Save, Delete, Restore,
    /// Import) still happens: that is the owner's decision (2026-11-09).
    ///
    /// Background parts read On when they act; a task reads config.DryRunMode once at its start
    /// (a run works on the settings it started with) and passes it down.
    /// </summary>
    internal static class DryRun
    {
        internal static bool On => Plugin.Instance?.Configuration?.DryRunMode == true;

        /// <summary>The header line every simulated task writes first.</summary>
        internal const string Header = "DRY RUN — nothing will be changed, the log shows what would happen";
    }
}
