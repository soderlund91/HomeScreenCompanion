using MediaBrowser.Controller.Collections;
using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Library;
using MediaBrowser.Model.Serialization;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace HomeScreenCompanion
{
    /// <summary>
    /// Keeps the tag of every tag home row on an empty collection that no user sees (Emby hides
    /// empty collections). Emby only keeps a tag while some item has it, and a tag that comes back
    /// gets a new id, so a row whose tag was off all its titles (outside its schedule, or before its
    /// first match) could not be created, or pointed at a tag that no longer existed. With the anchor
    /// the tag, and its id, always exist. (Folders, genres and tag items do not keep a tag: only
    /// indexed items such as collections do.) The anchor's id and the tags HSC put on it are kept
    /// in a plugin data file, so only those tags are ever taken off.
    /// </summary>
    internal static class TagAnchor
    {
        internal const string AnchorName = "Home Screen Companion tags";

        private static readonly object Gate = new object();
        private static string StateFile => Path.Combine(Plugin.Instance!.DataFolderPath, "tag_anchor.json");
        private static long _anchorId;

        private class State
        {
            public long AnchorId { get; set; }
            public List<string> Tags { get; set; } = new List<string>();
        }

        /// <summary>True for the anchor collection: never listed, tagged or removed as a real one.</summary>
        internal static bool IsAnchor(BaseItem? item, IJsonSerializer json)
        {
            if (item == null) return false;
            if (_anchorId == 0) { try { _anchorId = Load(json).AnchorId; } catch (Exception swallowed) { PluginLog.Swallowed(swallowed); } }
            return _anchorId != 0 ? item.InternalId == _anchorId
                : string.Equals(item.Name, AnchorName, StringComparison.Ordinal) && item.GetType().Name == "BoxSet";
        }

        /// <summary>Puts the tag on the anchor (made when missing) when it is not there yet.</summary>
        internal static void Ensure(ILibraryManager libraryManager, ICollectionManager collectionManager, IJsonSerializer json, string tag, Action<string> warn)
        {
            if (string.IsNullOrWhiteSpace(tag)) return;
            tag = tag.Trim();
            lock (Gate)
            {
                try
                {
                    var state = Load(json);
                    var anchor = Anchor(libraryManager, collectionManager, json, state);
                    if (anchor == null) { warn($"Tag '{tag}' could not be kept for its home section: the tag collection could not be made"); return; }
                    bool listed = state.Tags.Contains(tag, StringComparer.OrdinalIgnoreCase);
                    bool onAnchor = (anchor.Tags ?? Array.Empty<string>()).Contains(tag, StringComparer.OrdinalIgnoreCase);
                    if (!onAnchor)
                    {
                        anchor.AddTag(tag);
                        libraryManager.UpdateItem(anchor, anchor.GetParent(), ItemUpdateType.MetadataEdit, null);
                    }
                    if (!listed) { state.Tags.Add(tag); Save(json, state); }
                }
                catch (Exception ex) { warn($"Tag '{tag}' could not be kept for its home section: {ex.Message}"); }
            }
        }

        /// <summary>Takes the tags HSC put on the anchor off it, except <paramref name="keep"/>.</summary>
        internal static void Prune(ILibraryManager libraryManager, IJsonSerializer json, ICollection<string> keep, Action<string> warn)
        {
            lock (Gate)
            {
                try
                {
                    var state = Load(json);
                    var gone = state.Tags.Where(t => !keep.Contains(t)).ToList();
                    if (gone.Count == 0) return;
                    var anchor = state.AnchorId != 0 ? libraryManager.GetItemById(state.AnchorId) : null;
                    if (anchor != null)
                    {
                        bool changed = false;
                        foreach (var t in gone)
                            if ((anchor.Tags ?? Array.Empty<string>()).Contains(t, StringComparer.OrdinalIgnoreCase)) { anchor.RemoveTag(t); changed = true; }
                        if (changed)
                            libraryManager.UpdateItem(anchor, anchor.GetParent(), ItemUpdateType.MetadataEdit, null);
                    }
                    state.Tags = state.Tags.Where(t => keep.Contains(t)).ToList();
                    Save(json, state);
                }
                catch (Exception ex) { warn($"Unused home section tags could not be cleaned up: {ex.Message}"); }
            }
        }

        // The anchor collection: the saved one, else one with AnchorName, else a new one.
        private static BaseItem? Anchor(ILibraryManager libraryManager, ICollectionManager collectionManager, IJsonSerializer json, State state)
        {
            BaseItem? anchor = state.AnchorId != 0 ? libraryManager.GetItemById(state.AnchorId) : null;
            if (anchor != null && anchor.GetType().Name != "BoxSet") anchor = null;
            anchor ??= FindByName(libraryManager);
            if (anchor == null)
            {
                // Emby makes no collection without an item: made with one title, which is then taken
                // out again (an empty collection is hidden from every user).
                var seed = libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "Movie", "Series" }, Recursive = true, IsVirtualItem = false, Limit = 1 }).FirstOrDefault();
                if (seed == null) return null;
                var made = collectionManager.CreateCollection(new CollectionCreationOptions { Name = AnchorName, IsLocked = true, ItemIdList = new[] { seed.InternalId } })
                    .GetAwaiter().GetResult();
                anchor = made ?? FindByName(libraryManager);
                if (anchor is BoxSet box)
                    collectionManager.RemoveFromCollection(box, new[] { seed.InternalId });
            }
            if (anchor != null && anchor.InternalId != state.AnchorId)
            {
                state.AnchorId = anchor.InternalId;
                Save(json, state);
            }
            _anchorId = anchor?.InternalId ?? 0;
            return anchor;
        }

        private static BaseItem? FindByName(ILibraryManager libraryManager)
            => libraryManager.GetItemList(new InternalItemsQuery { IncludeItemTypes = new[] { "BoxSet" }, Name = AnchorName, Recursive = true }).FirstOrDefault();

        private static State Load(IJsonSerializer json)
        {
            try
            {
                if (File.Exists(StateFile))
                    return json.DeserializeFromFile<State>(StateFile) ?? new State();
            }
            catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
            return new State();
        }

        private static void Save(IJsonSerializer json, State state)
        {
            try { json.SerializeToFile(state, StateFile); } catch (Exception swallowed) { PluginLog.Swallowed(swallowed); }
        }
    }
}
