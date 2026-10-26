define(['emby-input', 'emby-button', 'emby-select', 'emby-checkbox'], function () {
    'use strict';

    var pluginId = "7c10708f-43e4-4d69-923c-77d01802315b";
    var statusInterval = null;

    // One status timer at most, for the page on screen. Starting a new one always stops the old
    // one, and a tick stops it once the page has left the DOM, so leaving the page (even without
    // a viewhide event) never leaves a timer polling in the background.
    function stopStatusPolling() {
        if (statusInterval) { clearInterval(statusInterval); statusInterval = null; }
    }
    // Every 5 s when idle; every second while a run is busy, so short steps show too
    // (refreshStatus switches the rate).
    var statusPollMs = 5000;
    function startStatusPolling(view, ms) {
        stopStatusPolling();
        statusPollMs = ms || 5000;
        statusInterval = setInterval(function () {
            if (!document.body.contains(view)) { stopStatusPolling(); return; }
            refreshStatus(view);
        }, statusPollMs);
    }
    function setStatusPollRate(view, running) {
        var ms = running ? 1000 : 5000;
        if (statusInterval && ms !== statusPollMs && document.body.contains(view)) startStatusPolling(view, ms);
    }
    // Live log modal: latest status per task and the tab the user picked (null = automatic)
    var _lastStatus = { sync: null, hsc: null, tl: null };
    var _logTab = null;
    var _logIssuesOnly = false;   // log modal: only warning / error lines (with their group's header)
    var originalConfigState = null;
    var statusRequestId = 0;

    // ---- Save conflict check ----
    // A fingerprint of the server's settings without what runs write on their own (home-section
    // ids, playlist mappings, art-replaced flags, the AI's last-run date, the Next Watch migration
    // flag, top-list exclusions in a home section's settings, the top-lists Save re-reads anyway,
    // Copy & Sync's record of whom it added, which the server always keeps, and a source's user
    // lists, which Save merges, see mergeUserIds) and without secrets.
    // Taken when the page loads; Save compares it with the server's current
    // one, so a change made elsewhere (another tab, another admin, an import) is never overwritten,
    // while a run that finished meanwhile does not count.
    function hscServerStamp(config) {
        function strip(v) {
            if (Array.isArray(v)) return v.map(strip);
            if (!v || typeof v !== 'object') return v;
            var out = {};
            Object.keys(v).forEach(function (k) { if (!/(apikey|clientid|secret|token|password)$/i.test(k)) out[k] = strip(v[k]); });
            return out;
        }
        var c = strip(config || {});
        delete c.TopLists;
        delete c.HomeSyncAdditions;
        c.Tags = (c.Tags || []).map(function (t) {
            var x = Object.assign({}, t);
            delete x.HomeSectionTracked; delete x.PlaylistMappings;
            delete x.CollectionPosterReplaced; delete x.CollectionBackgroundReplaced;
            delete x.CollectionThumbReplaced; delete x.CollectionBannerReplaced;
            delete x.AiLastRunDate; delete x.NextWatchUsersMigrated;
            delete x.LastRunCount; delete x.LastRunLimit;
            delete x.HomeSectionUserIds; delete x.PlaylistUserIds;
            try {
                var s = JSON.parse(x.HomeSectionSettings || '{}') || {};
                delete s._queryExcludeViewIds; delete s.ExcludedFolders;
                var sorted = {};
                Object.keys(s).sort().forEach(function (k) { sorted[k] = s[k]; });
                x.HomeSectionSettings = JSON.stringify(sorted);
            } catch (e) { }
            return x;
        });
        return JSON.stringify(c);
    }
    // Each source's user lists as the server has them (by the card key), the base mergeUserIds
    // compares with.
    function sourceUserLists(config) {
        var out = {};
        (config.Tags || []).forEach(function (t) {
            var key = t.Name ? t.Name + '\x1F' + t.Tag : t.Tag;
            if (!out[key]) out[key] = { Home: (t.HomeSectionUserIds || []).slice(), Playlist: (t.PlaylistUserIds || []).slice() };
        });
        return out;
    }
    // A user list as saved: this page's, plus who was put on it on the server since the page loaded,
    // minus who was taken off there (Copy & Sync, another tab). A user changed both here and there
    // follows this page.
    function mergeUserIds(page, loaded, server) {
        function norm(u) { return String(u || '').replace(/-/g, '').toLowerCase(); }
        function has(list, u) { return (list || []).some(function (x) { return norm(x) === norm(u); }); }
        // Added here, or loaded and still on the server.
        var out = (page || []).filter(function (u) { return !has(loaded, u) || has(server, u); });
        (server || []).forEach(function (u) { if (!has(loaded, u) && !has(out, u)) out.push(u); });
        return out;
    }
    // After this page saved part of the settings itself (saved filters, Clean up): the server's new
    // state is the page's own, so move the fingerprint along — only if nothing else had changed.
    function advanceServerStamp(beforeStamp) {
        var v = activeView();
        if (!v || !v._hscServerStamp || v._hscServerStamp !== beforeStamp) return Promise.resolve();
        return window.ApiClient.getPluginConfiguration(pluginId).then(function (c) {
            if (v._hscServerStamp === beforeStamp) v._hscServerStamp = hscServerStamp(c);
        }).catch(function () { });
    }
    function showSaveConflictDialog(onReload) {
        var modal = buildBackupModalShell();
        modal.renderBox(
            '<h3 style="' + _backupTitleStyle + '">Not saved — settings changed elsewhere</h3>' +
            '<p style="' + _backupHintStyle + '">Settings were changed elsewhere since you opened this page (another tab, another admin, or a run). Saving now would overwrite those changes, so nothing was saved.</p>' +
            '<p style="' + _backupHintStyle + '"><strong>Reload page</strong> shows the current settings and discards your unsaved changes here. <strong>Cancel</strong> keeps editing (note your changes, reload, then make them again).</p>' +
            '<div style="display:flex;gap:10px;justify-content:flex-end;align-items:center;flex-wrap:wrap;">' +
            '<button type="button" class="btnConflictCancel hsc-mbtn hsc-btn-secondary">Cancel</button>' +
            '<button type="button" class="btnConflictReload hsc-mbtn hsc-btn-primary"><i class="md-icon" style="font-size:1em;vertical-align:middle;margin-right:6px;">refresh</i>Reload page</button>' +
            '</div>'
        );
        modal.querySelector('.btnConflictCancel').addEventListener('click', modal.close);
        modal.querySelector('.btnConflictReload').addEventListener('click', function () { modal.close(); onReload(); });
    }

    var DEFAULT_AI_SYSTEM_PROMPT = 'You are a movie and TV show recommendation assistant. Respond ONLY with a valid JSON array. No explanation, no markdown, no code fences. Each item must have these fields: "title" (string, required), "year" (integer or null), "imdb_id" (string starting with "tt" if known, otherwise null), "type" ("movie" or "show"). Return exactly the items requested. Do not add any commentary. Example: [{"title":"Inception","year":2010,"imdb_id":"tt1375666","type":"movie"}]';

    function updateSystemPromptResetBtn(view) {
        var ta = view.querySelector('#txtAiSystemPrompt');
        var btn = view.querySelector('#btnResetAiSystemPrompt');
        if (!ta || !btn) return;
        btn.style.display = ta.value.trim() !== DEFAULT_AI_SYSTEM_PROMPT.trim() ? '' : 'none';
    }

    // The copy of this page the user is on. After moving around inside Emby the DOM can hold an
    // old, hidden copy of the page as well (first in document order), so a plain
    // document.querySelector('#HomeScreenCompanionConfigPage') would find the old one.
    var _activeView = null;
    function activeView() {
        return (_activeView && _activeView.isConnected) ? _activeView : document.querySelector('#HomeScreenCompanionConfigPage');
    }

    // The element that scrolls this page: the nearest scrolling ancestor, else the document.
    function pageScroller(view) {
        for (var el = view; el && el !== document.body && el !== document.documentElement; el = el.parentElement) {
            var oy = getComputedStyle(el).overflowY;
            if ((oy === 'auto' || oy === 'scroll') && el.scrollHeight > el.clientHeight) return el;
        }
        return document.scrollingElement || document.documentElement;
    }

    function isOpaqueBg(c) {
        return !!c && c !== 'transparent' && c !== 'rgba(0, 0, 0, 0)' && !/rgba\([^)]*,\s*0\)$/.test(c);
    }

    function applyPluginTheme() {
        // Decide light/dark from the HSC page itself (the settings page can be light while the
        // home screen / drawer is dark): first non-transparent background up from the page.
        var bg = null;
        var page = activeView();
        for (var a = page; a && a.nodeType === 1; a = a.parentElement) {
            var ac = getComputedStyle(a).backgroundColor;
            if (isOpaqueBg(ac)) { bg = ac; break; }
        }
        var candidates = bg ? [] : ['.skinHeader', '.mainDrawer', '.contentScrollSlider', 'body'];
        for (var i = 0; i < candidates.length; i++) {
            var el = document.querySelector(candidates[i]);
            if (!el) continue;
            var c = getComputedStyle(el).backgroundColor;
            if (c && c !== 'transparent' && c !== 'rgba(0, 0, 0, 0)') { bg = c; break; }
        }
        var isDark = true;
        if (bg) {
            var m = bg.match(/\d+/g);
            if (m) isDark = (parseInt(m[0]) * 0.299 + parseInt(m[1]) * 0.587 + parseInt(m[2]) * 0.114) < 128;
        }
        var root = document.documentElement;
        if (isDark) {
            root.style.setProperty('--plugin-popup-bg',     '#2a2a2a');
            root.style.setProperty('--plugin-popup-bg2',    '#333333');
            root.style.setProperty('--plugin-popup-color',  '#e8e8e8');
            root.style.setProperty('--plugin-popup-muted',  '#aaaaaa');
            root.style.setProperty('--plugin-popup-border', 'rgba(255,255,255,0.12)');
            root.style.setProperty('--plugin-popup-hover',  'rgba(255,255,255,0.08)');
            root.style.setProperty('--plugin-popup-badge',  'rgba(255,255,255,0.1)');
            root.style.setProperty('--plugin-input-border', 'rgba(255,255,255,0.2)');
            root.style.setProperty('--plugin-input-bg',     'rgba(255,255,255,0.08)');
            root.style.setProperty('--plugin-footer-bg',    '#181818');
            root.dataset.pluginTheme = 'dark';
        } else {
            root.style.setProperty('--plugin-popup-bg',     '#f2f2f2');
            root.style.setProperty('--plugin-popup-bg2',    '#e0e0e0');
            root.style.setProperty('--plugin-popup-color',  '#1a1a1a');
            root.style.setProperty('--plugin-popup-muted',  '#555555');
            root.style.setProperty('--plugin-popup-border', 'rgba(0,0,0,0.15)');
            root.style.setProperty('--plugin-popup-hover',  'rgba(0,0,0,0.08)');
            root.style.setProperty('--plugin-popup-badge',  'rgba(0,0,0,0.1)');
            root.style.setProperty('--plugin-input-border', 'rgba(0,0,0,0.28)');
            root.style.setProperty('--plugin-input-bg',     'rgba(0,0,0,0.04)');
            root.style.setProperty('--plugin-footer-bg',    '#c5cad1');
            root.dataset.pluginTheme = 'light';
        }
        var drawer = document.querySelector('.mainDrawer');
        var drawerRight = drawer ? drawer.getBoundingClientRect().right : 0;
        var footerLeft = (drawerRight > 0 && drawerRight < window.innerWidth * 0.5) ? drawerRight : 0;
        root.style.setProperty('--plugin-footer-left', footerLeft + 'px');
    }

    // Names in lists and dropdowns: A–Z, case-insensitive, numbers in natural order ("2" before "10").
    function natCmp(a, b) { return String(a == null ? '' : a).localeCompare(String(b == null ? '' : b), undefined, { numeric: true, sensitivity: 'base' }); }
    function byName(a, b) { return natCmp(a && a.Name, b && b.Name); }

    var cachedCollections = [];
    var cachedPlaylists = [];
    var cachedTags = [];
    var lastHscConfig = {};
    var currentManageSections = [];
    var savedFilters = [];
    var _topListTagNames = new Set();
    // Tag (lower case) → the server's top-list content type ('Movies' / 'Shows').
    var _topListTypes = {};
    function noteTopListTypes(lists) {
        _topListTypes = {};
        (lists || []).forEach(function (tl) { if (tl.TagName) _topListTypes[tl.TagName.trim().toLowerCase()] = tl.ContentType || 'Movies'; });
    }
    // Each group's warnings / error from its last run (Status → GroupIssues), by groupIssueKey.
    var _groupIssues = {};
    var _groupIssuesSig = '';

    var customCss = `
    <style id="homeScreenCompanionCustomCss">
        .day-toggle {
            background: transparent;
            color: var(--theme-text-secondary);
            border: 1px solid var(--hsc-hairline-strong);
            border-radius: 6px;
            padding: 7px 12px;
            cursor: pointer;
            font-family: inherit;
            font-size: 0.88em;
            font-weight: 500;
            transition: background-color 0.15s, border-color 0.15s, color 0.15s;
            flex-grow: 1;
            text-align: center;
        }
        .day-toggle:hover {
            background: var(--hsc-tint-2);
            color: var(--theme-text-primary);
        }
        .day-toggle.active {
            background: rgba(82,181,75,0.15);
            color: var(--hsc-primary-text);
            border-color: rgba(82,181,75,0.5);
        }

        .date-row-container {
            background: var(--hsc-tint-1);
            border: 1px solid var(--hsc-hairline);
            border-radius: 8px;
            padding: 16px;
            margin-bottom: 8px;
        }

        .selectLabel {
            font-size: 0.9em;
            color: var(--theme-text-secondary);
            margin-bottom: 5px;
            font-weight: 500;
            display: block;
        }

        /* Type chips (DESIGN.md): neutral chip, only the icon carries the category colour */
        .tag-indicator {
            margin-left: 8px;
            font-size: 0.78em;
            padding: 2px 7px;
            border-radius: 4px;
            display: inline-flex;
            align-items: center;
            gap: 4px;
            font-weight: 500;
            line-height: 1.4;
            position: relative;
            white-space: nowrap;
            color: var(--theme-text-secondary);
            background: transparent;
            border: 1px solid var(--hsc-hairline-strong);
        }
        .tag-indicator .md-icon { font-size: 1.1em !important; }
        .tag-indicator.schedule .md-icon { color: var(--hsc-activity-text); }
        .tag-indicator.collection .md-icon { color: var(--hsc-cat-collection); }
        .tag-indicator.imported .md-icon { color: var(--hsc-cat-imported); }
        .tag-indicator.homescreen .md-icon { color: var(--hsc-primary-text); }
        .tag-indicator.tag .md-icon { color: var(--hsc-cat-tag); }
        .tag-indicator.playlist .md-icon { color: var(--hsc-cat-playlist); }
        .tag-indicator.toplist .md-icon { color: var(--hsc-cat-toplist); }
        .tag-indicator.ct-movies .md-icon { color: var(--hsc-cat-movies); }
        .tag-indicator.ct-shows .md-icon { color: var(--hsc-cat-shows); }

        /* "25 of 30": a status pill (warning) */
        .tag-indicator.short {
            color: var(--hsc-warning-text);
            background: rgba(230,193,90,0.15);
            border-color: transparent;
            border-radius: 9999px;
            padding: 2px 8px;
        }

        .hsc-run-issue:not(:empty) {
            display: inline-flex;
            align-items: center;
            margin-left: 12px;
        }
        .hsc-run-issue:not(:empty) + .tag-title { margin-left: 6px; }
        .hsc-run-issue i { color: var(--hsc-warning); }
        .hsc-run-issue i.error { color: var(--hsc-danger-strong); }

        /* Last-run warnings / error of a group: callouts (semantic colour at 8%, border at 30%, text in ink) */
        .hsc-run-issue-box > div {
            margin-bottom: 12px;
            padding: 12px;
            border: 1px solid rgba(230,193,90,0.3);
            border-radius: 6px;
            background: rgba(230,193,90,0.08);
            font-size: 0.88em;
            line-height: 1.5;
        }
        .hsc-run-issue-box > div.error {
            border-color: rgba(229,115,115,0.3);
            background: rgba(229,115,115,0.08);
        }
        .hsc-run-issue-box .hsc-run-issue-head {
            display: flex;
            align-items: center;
            gap: 6px;
            font-weight: 600;
            color: var(--theme-text-primary);
            margin-bottom: 4px;
        }
        .hsc-run-issue-box .hsc-run-issue-head .md-icon { color: var(--hsc-warning-text); }
        .hsc-run-issue-box .error .hsc-run-issue-head .md-icon { color: var(--hsc-danger-text); }

        /* "N groups need attention": a status pill that also filters the list */
        .hsc-issue-counter {
            display: flex;
            align-items: center;
            gap: 5px;
            margin-left: 10px;
            padding: 4px 12px;
            border-radius: 9999px;
            border: 1px solid transparent;
            background: rgba(230,193,90,0.15);
            color: var(--hsc-warning-text);
            font-size: 0.85em;
            font-weight: 500;
            font-family: inherit;
            white-space: nowrap;
            cursor: pointer;
            transition: background-color 0.15s, border-color 0.15s;
        }
        .hsc-issue-counter:hover { background: rgba(230,193,90,0.25); }
        .hsc-issue-counter.error { background: rgba(229,115,115,0.15); color: var(--hsc-danger-text); }
        .hsc-issue-counter.error:hover { background: rgba(229,115,115,0.25); }
        .hsc-issue-counter.active { border-color: currentColor; }
        .hsc-run-issue-box ul { margin: 0; padding-left: 20px; }

        .tag-indicator.schedule::after {
            content: '';
            position: absolute;
            top: -3px;
            right: -3px;
            width: 6px;
            height: 6px;
            border-radius: 50%;
            background: var(--hsc-warning);
            box-shadow: 0 0 0 1.5px var(--theme-background-level2);
        }

        .tag-indicator.schedule.schedule-active::after {
            background: var(--hsc-primary);
        }

        .tag-indicator.source {
            padding: 2px 5px;
            margin-left: 0;
            margin-right: 8px;
            background: var(--hsc-tint-1);
        }

        .badge-container {
            display: flex;
            align-items: center;
        }

        .sort-hidden .drag-handle {
            display: none !important;
        }

        /* Dry run: a sticky warning callout (opaque, because the page scrolls under it) */
        .dry-run-warning {
            background: linear-gradient(rgba(230,193,90,0.16), rgba(230,193,90,0.16)), var(--theme-background-level1);
            border: 1px solid rgba(230,193,90,0.45);
            color: var(--theme-text-primary);
            padding: 12px 16px;
            border-radius: 8px;
            margin-bottom: 16px;
            text-align: center;
            font-weight: 600;
            font-size: 0.95em;
            box-shadow: var(--hsc-shadow-float);
            display: none;
            align-items: center;
            justify-content: center;
            gap: 10px;
            position: sticky;
            top: 60px;
            z-index: 10000;
        }

        .drag-handle {
            cursor: grab;
            margin-right: 12px;
            color: var(--theme-text-secondary);
            display: flex;
            align-items: center;
        }

        .drag-handle:active {
            cursor: grabbing;
        }

        /* Manual reordering (makeRowSortable): the moving row floats, the page does not select text */
        .drag-handle { touch-action: none; border-radius: 6px; }
        .drag-handle:hover { color: var(--theme-text-primary); }
        .tag-row.hsc-sorting, .man-section-row.hsc-sorting {
            position: relative;
            z-index: 2;
            border-color: var(--hsc-primary);
            box-shadow: var(--hsc-shadow-float);
        }
        html.hsc-sorting-active, html.hsc-sorting-active * { cursor: grabbing !important; user-select: none !important; }

        .sort-placeholder {
            height: 40px;
            background-color: transparent;
            margin-bottom: 8px;
            border-radius: 8px;
            border: 1px dashed var(--hsc-hairline-strong);
            transition: height 0.2s;
        }

        .tag-row.just-moved {
            animation: moveHighlight 2s ease-out forwards;
        }

        .tag-row.just-added {
            animation: addHighlight 2s ease-out forwards;
        }

        @keyframes moveHighlight {
            0% { border-color: var(--hsc-activity); }
            100% { border-color: var(--hsc-hairline); }
        }

        @keyframes tcDotBounce {
            0%, 80%, 100% { transform: translateY(0); opacity: 0.4; }
            40%            { transform: translateY(-5px); opacity: 1; }
        }
        .tc-dot-loader { display:inline-flex; align-items:center; gap:5px; }
        .tc-dot-loader span { display:inline-block; width:7px; height:7px; border-radius:50%; background:currentColor; animation:tcDotBounce 1.2s ease-in-out infinite; }
        .tc-dot-loader span:nth-child(2) { animation-delay:0.2s; }
        .tc-dot-loader span:nth-child(3) { animation-delay:0.4s; }

        @keyframes addHighlight {
            0% { border-color: var(--hsc-primary); }
            100% { border-color: var(--hsc-hairline); }
        }

        .control-row {
            background: var(--hsc-tint-1);
            padding: 12px 16px;
            border-radius: 8px;
            margin-bottom: 16px;
            border: 1px solid var(--hsc-hairline);
            display: flex;
            flex-direction: column;
            gap: 12px;
        }

        .control-sub-row {
            display: flex;
            align-items: center;
            gap: 20px;
        }

        .control-group {
            display: flex;
            align-items: center;
            gap: 10px;
        }

        .control-label {
            font-size: 0.88em;
            font-weight: 500;
            color: var(--theme-text-secondary);
        }

        .search-input-wrapper {
            position: relative;
            display: flex;
            align-items: center;
            flex-grow: 1;
            max-width: 250px;
        }

        .search-input-wrapper .search-icon {
            position: absolute;
            left: 10px;
            font-size: 1em;
            opacity: 0.5;
            pointer-events: none;
        }

        #btnClearSearch {
            position: absolute;
            right: 8px;
            cursor: pointer;
            opacity: 0.5;
            display: none;
        }

        #btnClearSearch:hover {
            opacity: 1;
            color: var(--hsc-danger-text);
        }

        #txtSearchTags {
            width: 100%;
            background: var(--hsc-tint-1) !important;
            border: 1px solid var(--hsc-hairline-strong) !important;
            border-radius: 6px !important;
            padding: 6px 30px 6px 35px !important;
            color: inherit;
            font-size: 0.95em;
        }

        #txtSearchTags:focus {
            border-color: var(--hsc-primary) !important;
            outline: none;
        }

        .filter-dropdown-wrapper {
            position: relative;
            flex-shrink: 0;
        }

        .filter-dropdown-btn {
            display: flex;
            align-items: center;
            gap: 5px;
            padding: 5px 12px;
            background: var(--hsc-tint-1);
            border: 1px solid var(--hsc-hairline-strong);
            border-radius: 8px;
            font-family: inherit;
            font-size: 0.88em;
            font-weight: 500;
            cursor: pointer;
            color: var(--plugin-popup-color, inherit);
            white-space: nowrap;
            user-select: none;
        }

        .filter-dropdown-btn:hover {
            background: var(--hsc-tint-2);
        }

        html[data-plugin-theme="dark"] select,
        html[data-plugin-theme="dark"] input[type="text"],
        html[data-plugin-theme="dark"] input[type="number"] {
            color-scheme: dark;
        }

        html[data-plugin-theme="light"] select,
        html[data-plugin-theme="light"] input[type="text"],
        html[data-plugin-theme="light"] input[type="number"] {
            color-scheme: light;
        }

        .filter-dropdown-btn.active {
            background: var(--hsc-tint-3);
            border-color: var(--theme-text-secondary);
            color: var(--theme-text-primary);
        }

        .filter-dropdown-panel {
            display: none;
            position: absolute;
            top: calc(100% + 6px);
            left: 0;
            z-index: 9999;
            background: var(--plugin-popup-bg, #2a2a2a);
            color: var(--plugin-popup-color, #e8e8e8);
            border: 1px solid var(--plugin-popup-border, rgba(255,255,255,0.12));
            border-radius: 8px;
            box-shadow: var(--hsc-shadow-float);
            padding: 10px 14px;
            min-width: 200px;
        }

        .filter-dropdown-panel.open {
            display: block;
        }

        .hsc-user-dropdown .filter-dropdown-panel {
            max-height: 260px;
            overflow-y: auto;
        }

        .filter-dropdown-section {
            margin-bottom: 10px;
        }

        .filter-dropdown-section:last-child {
            margin-bottom: 0;
        }

        .filter-dropdown-divider {
            height: 1px;
            background: var(--line-color);
            margin: 8px 0;
        }

        .filter-dropdown-label {
            font-size: 0.75em;
            font-weight: 600;
            opacity: 0.7;
            text-transform: uppercase;
            letter-spacing: 0.6px;
            margin-bottom: 6px;
        }

        .filter-chk-row {
            display: flex;
            align-items: center;
            gap: 7px;
            padding: 3px 0;
            cursor: pointer;
            font-size: 0.9em;
        }

        .filter-chk-row input[type=checkbox] {
            cursor: pointer;
        }

        .btn-row-remove {
            background: transparent !important;
            min-width: 40px;
            width: 40px;
            padding: 0;
            color: var(--hsc-danger-text);
            border-radius: 8px;
            display: flex;
            align-items: center;
            justify-content: center;
            box-shadow: none;
            margin-top: 12px;
        }

        .plugin-footer {
            position: fixed;
            bottom: 0;
            left: var(--plugin-footer-left, 0);
            right: 0;
            z-index: 200;
            background: var(--plugin-footer-bg);
            color: var(--plugin-popup-muted);
            height: 50px;
            padding: 0 28px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 0.9em;
            border-top: 1px solid var(--line-color);
            box-sizing: border-box;
        }
        .plugin-footer .footer-version {
            color: var(--theme-text-secondary);
        }
        .plugin-footer .footer-sep {
            margin: 0 12px;
            opacity: 0.35;
        }
        .plugin-footer .footer-update-link {
            color: var(--hsc-warning-text);
            text-decoration: none;
            font-weight: bold;
        }
        .plugin-footer .simple-link {
            font-size: 1em;
            transition: none;
            transform: none;
        }
        .plugin-footer .simple-link:hover {
            transform: none;
        }
    </style>`;

    function getUrlRowHtml(value, limit) {
        var val = value || '';
        var lim = limit !== undefined ? limit : 0;
        return `
            <div class="url-row" style="display:flex; align-items:center; gap:10px; margin-bottom:10px;">
                <div style="flex-grow:1;">
                    <input is="emby-input" class="txtTagUrl" type="text" label="Trakt/MDBList/TMDb URL" value="${val}" />
                </div>
                <div style="width:110px;">
                    <input is="emby-input" class="txtUrlLimit" type="number" label="Max (0=All)" value="${lim}" min="0" />
                </div>
                <button type="button" is="emby-button" class="raised button-submit btnTestUrl hsc-btn-secondary hsc-btn-sm" style="margin-top:12px;" title="Test Source"><span>Test</span></button>
                <button type="button" is="emby-button" class="raised btnRemoveUrl btn-row-remove" title="Remove URL"><i class="md-icon">remove_circle_outline</i></button>
            </div>`;
    }

    // ---- Settings level (Basic / Advanced) ----
    // A power-user setting is an .hsc-adv element with an "Advanced" badge. Basic hides the ones
    // at their default; one in use stays visible, so nothing that is in force is ever hidden.
    var settingsLevel = 'Basic';
    var ADV_BADGE = '<span class="hsc-adv-badge">Advanced</span>';

    // Whether an .hsc-adv element is set to something other than its default. data-adv says how to
    // tell: check (its checkbox ticked), target (other than Series only), tag-art (a generated or
    // uploaded tag image), blacklist (the card's blacklist has ids); otherwise its select's value
    // is not one of data-adv-def ("|"-separated).
    function advInUse(el) {
        var kind = el.getAttribute('data-adv') || '';
        if (kind === 'check') return !!el.querySelector('input[type="checkbox"]:checked');
        if (kind === 'tag-art') {
            var poster = el.querySelector('.coll-style-picker[data-kind="poster"] input:checked');
            var bg = el.querySelector('.coll-style-picker[data-kind="background"] input:checked');
            return !!((poster && poster.value) || (bg && bg.value) || (el.querySelector('.hiddenBgPath') || {}).value);
        }
        if (kind === 'blacklist') {
            var row = el.closest('.tag-row');
            return !!(row && ((row.querySelector('.txtTagBlacklist') || {}).value || '').trim());
        }
        if (kind === 'target') {
            var c = el.querySelectorAll('input[type="checkbox"]');   // Series, Season, Episode
            return c.length === 3 && !(c[0].checked && !c[1].checked && !c[2].checked);
        }
        var sel = el.querySelector('select');
        return !!sel && (el.getAttribute('data-adv-def') || '').split('|').indexOf(sel.value) < 0;
    }

    // Shows or hides the power-user settings under root for the current level. Run when they are
    // drawn, on a level switch and after Save — not while editing, so one put back to its default
    // stays until Save.
    function refreshAdvanced(root) {
        if (!root) return;
        root.querySelectorAll('.hsc-adv').forEach(function (el) {
            var hide = settingsLevel !== 'Advanced' && !advInUse(el);
            el.classList.toggle('hsc-adv-hidden', hide);
            // A card tab that is hidden while open (Blacklist): show the Source tab instead.
            if (hide && el.classList.contains('tag-tab') && el.classList.contains('active')) {
                var first = el.parentElement.querySelector('.tag-tab[data-tab="general"]');
                if (first) first.click();
            }
        });
    }

    // Whether a saved home row's settings (HomeSectionSettings JSON) use a power-user look setting.
    function hseSettingsUseAdvanced(json) {
        var s = {};
        try { s = JSON.parse(json || '{}') || {}; } catch (e) { }
        return !!(s.DisplayMode || s.ScrollDirection || normCardSize(s.CardSizeOffset) !== '0');
    }

    // The level for a configuration saved before there was one: Advanced when it already uses a
    // power-user setting, else Basic.
    function initialSettingsLevel(config) {
        var used = (config.Tags || []).some(function (t) {
            return t.TagTargetSeason || t.TagTargetEpisode || t.CollectionTargetSeason || t.CollectionTargetEpisode
                || t.MediaInfoTargetSeason || t.MediaInfoTargetEpisode
                || t.EnableCollectionTag || t.EnablePlaylistTag || t.CollectionKeepOrder
                || t.TagPosterStyle || t.TagBackgroundStyle || t.TagBackgroundPath
                || t.OverrideWhenActive || (t.Blacklist || []).length > 0
                || hseSettingsUseAdvanced(t.HomeSectionSettings);
        }) || (config.TopLists || []).some(function (tl) { return hseSettingsUseAdvanced(tl.HomeSectionSettings); })
          || (config.HomeSyncProfiles || []).some(function (p) { return p && p.AddToSources; });
        return used ? 'Advanced' : 'Basic';
    }

    // Sets the level on the page; save = also store it in the configuration right away (it is a
    // view choice, so it does not wait for Save or mark the page as changed).
    function setSettingsLevel(view, level, save) {
        settingsLevel = level === 'Advanced' ? 'Advanced' : 'Basic';
        view.querySelectorAll('#hscLevelSwitch button').forEach(function (b) {
            b.classList.toggle('active', b.getAttribute('data-level') === settingsLevel);
        });
        refreshAdvanced(view);
        if (!save) return;
        window.ApiClient.getPluginConfiguration(pluginId)
            .then(function (currentConfig) {
                var before = hscServerStamp(currentConfig);
                currentConfig.SettingsLevel = settingsLevel;
                return window.ApiClient.updatePluginConfiguration(pluginId, currentConfig)
                    .then(function (r) { return advanceServerStamp(before).then(function () { return r; }); });
            })
            .then(function () {
                if (originalConfigState) {
                    try {
                        var state = JSON.parse(originalConfigState);
                        state.SettingsLevel = settingsLevel;
                        originalConfigState = JSON.stringify(state);
                    } catch (e) {}
                }
                checkFormState();
            });
    }

    // One extra tag of a source (TagConfig.ExtraTags), put on the same titles as its Tag Name.
    function getExtraTagRowHtml(value) {
        var val = String(value || '').replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;');
        return `
            <div class="extra-tag-row" style="display:flex; align-items:center; gap:10px; margin-bottom:10px;">
                <div style="flex-grow:1;">
                    <input is="emby-input" class="txtExtraTag" type="text" label="Extra Tag" value="${val}" />
                </div>
                <button type="button" is="emby-button" class="raised btnRemoveExtraTag btn-row-remove" title="Remove tag"><i class="md-icon">remove_circle_outline</i></button>
            </div>`;
    }

    // The values of tag fields: filled in, trimmed, without repeats (case-insensitive).
    function uniqueTagValues(inputs) {
        var seen = {};
        return Array.from(inputs).map(function (i) { return (i.value || '').trim(); })
            .filter(function (v) { var k = v.toLowerCase(); if (!v || seen[k]) return false; seen[k] = 1; return true; });
    }

    // The extra tags of a source card.
    function readExtraTags(row) {
        return uniqueTagValues(row.querySelectorAll('.txtExtraTag'));
    }

    // "Custom tag on collection / playlist": a tag on the collection or playlists the group makes,
    // not on their titles. kind = 'coll' or 'pl'.
    function getOutputTagHtml(kind, enabled, value, extras) {
        var val = String(value || '').replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;');
        var what = kind === 'coll' ? 'collection' : 'playlist';
        var extraRows = (extras || []).map(function (v) { return getOutExtraTagRowHtml(v); }).join('');
        return `
            <div class="checkboxContainer checkboxContainer-withDescription out-tag-box hsc-adv" data-adv="check" style="margin-top:12px;">
                <label>
                    <input is="emby-checkbox" type="checkbox" class="chkOutTag" data-kind="${kind}" ${enabled ? 'checked' : ''} />
                    <span>Custom tag on ${what}${ADV_BADGE}</span>
                </label>
                <div class="fieldDescription">Puts a tag on the ${what}${kind === 'pl' ? 's' : ''} itself, not on the titles in ${kind === 'pl' ? 'them' : 'it'}.<br>Not sure you need it? Leave it off.</div>
                <div class="inputContainer out-tag-field" style="margin-top:10px; display:${enabled ? 'block' : 'none'};">
                    <input is="emby-input" type="text" class="txtOutTag" data-kind="${kind}" label="Tag on the ${what}" value="${val}" />
                    <div class="out-extra-tag-list" style="margin-top:10px;">${extraRows}</div>
                    <button is="emby-button" type="button" class="raised btnAddOutTag hsc-btn-dashed" style="width:100%;"><i class="md-icon" style="margin-right:5px;">add</i>Add another tag</button>
                </div>
            </div>`;
    }

    // An extra row of a custom collection / playlist tag (removed by the shared btnRemoveExtraTag).
    function getOutExtraTagRowHtml(value) {
        return getExtraTagRowHtml(value).replace('class="txtExtraTag"', 'class="txtOutExtraTag"');
    }

    // { enabled, tag, extras } of a card's custom collection / playlist tag.
    function readOutputTag(row, kind) {
        var chk = row.querySelector('.chkOutTag[data-kind="' + kind + '"]');
        var txt = row.querySelector('.txtOutTag[data-kind="' + kind + '"]');
        var box = chk ? chk.closest('.out-tag-box') : null;
        var extras = box ? uniqueTagValues(box.querySelectorAll('.txtOutExtraTag')) : [];
        return { enabled: !!(chk && chk.checked), tag: txt ? txt.value.trim() : '', extras: extras };
    }

    function getLocalRowHtml(type, selectedName, limit) {
        var options = type === 'LocalCollection' ? cachedCollections : cachedPlaylists;
        var optHtml = '<option value="">-- Select --</option>' + options.map(o => `<option value="${o.Name}" ${selectedName === o.Name ? 'selected' : ''}>${o.Name}</option>`).join('');
        var lim = limit !== undefined ? limit : 0;
        return `
            <div class="local-row" style="display:flex; align-items:center; gap:10px; margin-bottom:10px;">
                <div style="flex-grow:1;">
                    <select is="emby-select" class="selLocalSource" style="width:100%;">
                        ${optHtml}
                    </select>
                </div>
                <div style="width:110px;">
                    <input is="emby-input" class="txtLocalLimit" type="number" label="Max (0=All)" value="${lim}" min="0" />
                </div>
                <button type="button" is="emby-button" class="raised btnRemoveLocal btn-row-remove" title="Remove"><i class="md-icon">remove_circle_outline</i></button>
            </div>`;
    }

    function getMonthOptions(selectedMonth) {
        var months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
        return months.map((m, i) => `<option value="${i + 1}" ${selectedMonth == (i + 1) ? 'selected' : ''}>${m}</option>`).join('');
    }

    function getDayOptions(selectedDay, maxDay) {
        maxDay = maxDay || 31;
        var html = '';
        for (var i = 1; i <= maxDay; i++) html += `<option value="${i}" ${selectedDay == i ? 'selected' : ''}>${i}</option>`;
        return html;
    }

    function getMaxDays(month) {
        return new Date(2001, month, 0).getDate();
    }

    function getWeekButtons(savedDays) {
        var days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];
        var shortDays = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
        var saved = (savedDays || "").toLowerCase();

        return days.map((d, i) => {
            var isActive = saved.includes(d.toLowerCase());
            return `<button type="button" class="day-toggle ${isActive ? 'active' : ''}" data-day="${d}">${shortDays[i]}</button>`;
        }).join('');
    }

    function parseDateYMD(dateStr) {
        if (!dateStr) return null;
        var s = String(dateStr);
        // Fast path: ISO YYYY-MM-DD (with or without time/tz suffix)
        var m = /^(\d{4})-(\d{2})-(\d{2})/.exec(s);
        if (m) return { year: +m[1], month: +m[2], day: +m[3] };
        // Fallback for non-ISO formats: parse with Date and use UTC accessors
        // (date-only strings are UTC midnight per spec, so getUTC* is correct)
        var d = new Date(s);
        if (!isNaN(d.getTime())) return { year: d.getUTCFullYear(), month: d.getUTCMonth() + 1, day: d.getUTCDate() };
        return null;
    }

    // Rule types that are a date (or dates) plus "days before / after".
    function isMovingType(type) {
        return type === 'MovingDate' || type === 'Easter' || type === 'Advent' || type === 'Friday13';
    }

    function movingHint(type) {
        if (type === 'Easter') return 'Around Easter Sunday (Western), every year.';
        if (type === 'Advent') return 'From the first Sunday of Advent through Christmas Day, every year.';
        if (type === 'Friday13') return 'Every Friday the 13th.';
        if (type === 'MovingDate') return 'e.g. the 2nd Sunday of May, every year.';
        return '';
    }

    function getDateRowHtml(interval) {
        var type = interval.Type || 'SpecificDate';
        if (type === 'EveryYear') console.log('[HSC schedule] raw interval from server:', JSON.stringify(interval));
        var sDate = interval.Start ? interval.Start.split('T')[0] : '';
        var eDate = interval.End ? interval.End.split('T')[0] : '';
        var sParts = parseDateYMD(interval.Start);
        var eParts = parseDateYMD(interval.End);
        var sMonth = sParts ? sParts.month : 12;
        var sDay   = sParts ? sParts.day   : 1;
        var eMonth = eParts ? eParts.month : 12;
        var eDay   = eParts ? eParts.day   : 28;
        var sMaxDay = getMaxDays(sMonth);
        var eMaxDay = getMaxDays(eMonth);
        sDay = Math.min(sDay, sMaxDay);
        eDay = Math.min(eDay, eMaxDay);
        var dayOfWeek = interval.DayOfWeek || '';
        // "MovingDate" (Yearly weekday, e.g. last Monday of May) and "Easter": an anchor date plus a
        // window of days around it. A MovingDate saved with Occurrence "easter" shows as Easter.
        var movOcc = String(interval.Occurrence || '1').toLowerCase();
        if (type === 'MovingDate' && movOcc === 'easter') type = 'Easter';
        if (movOcc === 'easter') movOcc = '1';
        var movWeekday = type === 'MovingDate' && dayOfWeek ? dayOfWeek.split(',')[0].trim() : 'Monday';
        var movMonth = +interval.Month >= 1 && +interval.Month <= 12 ? +interval.Month : 1;
        var movBefore = Math.max(0, Math.min(60, +interval.DaysBefore || 0));
        var movAfter = Math.max(0, Math.min(60, +interval.DaysAfter || 0));

        return `
            <div class="date-row date-row-container" style="display: flex; flex-wrap: wrap; align-items: flex-start; gap: 15px;">
                
                <div style="width:160px;">
                    <label class="selectLabel">Rule Type</label>
                    <select is="emby-select" class="selDateType" style="width:100%;">
                        <option value="SpecificDate" ${type === 'SpecificDate' ? 'selected' : ''}>Specific Date</option>
                        <option value="EveryYear" ${type === 'EveryYear' ? 'selected' : ''}>Recurring</option>
                        <option value="MovingDate" ${type === 'MovingDate' ? 'selected' : ''}>Yearly weekday</option>
                        ${type === 'Easter' ? '<option value="Easter" selected>Easter</option>' : ''}
                        ${type === 'Advent' ? '<option value="Advent" selected>Advent</option>' : ''}
                        ${type === 'Friday13' ? '<option value="Friday13" selected>Friday the 13th</option>' : ''}
                        <option value="Weekly" ${type === 'Weekly' ? 'selected' : ''}>Week Days</option>
                        <option value="TimeOfDay" ${type === 'TimeOfDay' ? 'selected' : ''}>Time of Day</option>
                    </select>
                </div>
                
                <div class="inputs-specific" style="display: ${type === 'SpecificDate' ? 'flex' : 'none'}; gap: 8px; flex-grow: 1; align-items: center;">
                    <div style="flex-grow:1;">
                        <input is="emby-input" type="date" class="txtFullStartDate" label="Start Date" value="${sDate}" />
                    </div>
                    <span style="opacity:0.5; padding-top:15px;">to</span>
                    <div style="flex-grow:1;">
                        <input is="emby-input" type="date" class="txtFullEndDate" label="End Date" value="${eDate}" />
                    </div>
                </div>

                <div class="inputs-annual" style="display: ${type === 'EveryYear' ? 'flex' : 'none'}; gap: 8px; flex-grow: 1; align-items: flex-start;">
                    
                    <div style="display:flex; display:flex; gap:5px;">
                        <div style="width:80px;">
                            <label class="selectLabel">Start Month</label>
                            <select is="emby-select" class="selStartMonth" style="width:100%;">${getMonthOptions(sMonth)}</select>
                        </div>
                        <div style="width:70px;">
                            <label class="selectLabel">Day</label>
                            <select is="emby-select" class="selStartDay" style="width:100%;">${getDayOptions(sDay, sMaxDay)}</select>
                        </div>
                    </div>

                    <span style="opacity:0.5; padding-top:32px;">to</span>

                    <div style="display:flex; display:flex; gap:5px;">
                        <div style="width:80px;">
                            <label class="selectLabel">End Month</label>
                            <select is="emby-select" class="selEndMonth" style="width:100%;">${getMonthOptions(eMonth)}</select>
                        </div>
                        <div style="width:70px;">
                            <label class="selectLabel">Day</label>
                            <select is="emby-select" class="selEndDay" style="width:100%;">${getDayOptions(eDay, eMaxDay)}</select>
                        </div>
                    </div>
                </div>

                <div class="inputs-moving" style="display: ${isMovingType(type) ? 'flex' : 'none'}; gap: 8px; flex-grow: 1; align-items: flex-start; flex-wrap: wrap;">
                    <div class="mov-wd-month" style="display:${type === 'MovingDate' ? 'flex' : 'none'}; gap:5px; align-items:flex-start;">
                        <div style="width:80px;">
                            <label class="selectLabel">Which</label>
                            <select is="emby-select" class="selMovOccurrence" style="width:100%;">${[['1', '1st'], ['2', '2nd'], ['3', '3rd'], ['4', '4th'], ['last', 'Last']].map(function (o) { return `<option value="${o[0]}" ${movOcc === o[0] ? 'selected' : ''}>${o[1]}</option>`; }).join('')}</select>
                        </div>
                        <div style="width:120px;">
                            <label class="selectLabel">Day</label>
                            <select is="emby-select" class="selMovWeekday" style="width:100%;">${['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'].map(function (d) { return `<option value="${d}" ${movWeekday === d ? 'selected' : ''}>${d}</option>`; }).join('')}</select>
                        </div>
                        <span style="opacity:0.5; padding-top:32px;">of</span>
                        <div style="width:120px;">
                            <label class="selectLabel">Month</label>
                            <select is="emby-select" class="selMovMonth" style="width:100%;">${['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'].map(function (m, i) { return `<option value="${i + 1}" ${movMonth === i + 1 ? 'selected' : ''}>${m}</option>`; }).join('')}</select>
                        </div>
                    </div>
                    <div style="display:flex; gap:5px; align-items:flex-start;">
                        <div style="width:90px;">
                            <label class="selectLabel">Days before</label>
                            <input type="number" class="txtMovBefore emby-input" min="0" max="60" step="1" style="box-sizing:border-box; width:100%; height:31px; padding:0 8px;" value="${movBefore}" title="Active from this many days before (0–60)" />
                        </div>
                        <div style="width:90px;">
                            <label class="selectLabel">Days after</label>
                            <input type="number" class="txtMovAfter emby-input" min="0" max="60" step="1" style="box-sizing:border-box; width:100%; height:31px; padding:0 8px;" value="${movAfter}" title="Active until this many days after (0–60)" />
                        </div>
                    </div>
                    <div class="mov-hint" style="width:100%; font-size:0.8em; opacity:0.6; margin-top:-4px;">${movingHint(type)}</div>
                </div>

                <div class="inputs-weekly" style="display: ${type === 'Weekly' ? 'flex' : 'none'}; flex-grow: 1; align-items: center; gap: 5px; flex-wrap: wrap;">
                    <div style="width:100%;">
                        <label class="selectLabel">Active On Days</label>
                        <div class="week-btn-container" style="display:flex; gap:5px; margin-top:2px;">
                            ${getWeekButtons(dayOfWeek)}
                        </div>
                    </div>
                </div>

                <div class="inputs-time" style="display: ${type === 'TimeOfDay' ? 'flex' : 'none'}; flex-grow: 1; align-items: flex-start; gap: 8px; flex-wrap: wrap;">
                    <div style="width:110px;">
                        <input is="emby-input" type="time" class="txtFromTime" label="From" value="${interval.FromTime || '20:00'}" />
                    </div>
                    <span style="opacity:0.5; padding-top:32px;">to</span>
                    <div style="width:110px;">
                        <input is="emby-input" type="time" class="txtToTime" label="To" value="${interval.ToTime || '23:00'}" />
                    </div>
                    <div style="width:100%;">
                        <label class="selectLabel">On Days <span style="opacity:0.6;">(none = every day)</span></label>
                        <div class="week-btn-container" style="display:flex; gap:5px; margin-top:2px;">
                            ${getWeekButtons(type === 'TimeOfDay' ? dayOfWeek : '')}
                        </div>
                    </div>
                </div>

                <button type="button" is="emby-button" class="btnRemoveDate hsc-btn-ghost" style="color:var(--hsc-danger-text) !important; min-width:40px; margin-top: 25px;" title="Remove Rule"><i class="md-icon">delete</i></button>
            </div>`;
    }

    // Manual reordering of a list by its drag handle. Pointer events cover mouse, touch and pen with
    // one code path (no HTML5 drag-and-drop): the row moves through the list live while the pointer
    // moves, the scroll area scrolls at its edges, Esc puts the row back. The focused handle also
    // moves its row with ArrowUp / ArrowDown. Rows hidden by a filter keep their place.
    //   opts.list()      -> the list element (the rows' parent)
    //   opts.rowSelector -> selector of the sortable rows (direct children of the list)
    //   opts.canDrag()   -> false while reordering is not allowed (e.g. another sort order)
    //   opts.onStart()   -> optional, called when a drag starts
    //   opts.onMoved()   -> called once when the drag ended with the row in a new place
    function makeRowSortable(row, handle, opts) {
        if (!row || !handle) return;
        handle.setAttribute('tabindex', '0');
        handle.setAttribute('role', 'button');
        handle.setAttribute('aria-label', 'Drag to reorder (or use the arrow keys)');

        function visibleRows(list) {
            return Array.prototype.filter.call(list.children, function (el) {
                return el.matches(opts.rowSelector) && el.offsetParent !== null;
            });
        }
        function scrollParent(el) {
            for (var p = el.parentElement; p && p !== document.body; p = p.parentElement) {
                var oy = getComputedStyle(p).overflowY;
                if ((oy === 'auto' || oy === 'scroll') && p.scrollHeight > p.clientHeight) return p;
            }
            return document.scrollingElement || document.documentElement;
        }
        function finishMove(changed) {
            row.classList.add('just-moved');
            setTimeout(function () { row.classList.remove('just-moved'); }, 2000);
            if (changed && opts.onMoved) opts.onMoved();
        }

        handle.addEventListener('pointerdown', function (e) {
            if (e.button !== 0 || !opts.canDrag()) return;
            var list = opts.list();
            if (!list) return;
            e.preventDefault();
            e.stopPropagation();
            if (opts.onStart) opts.onStart();

            var startNext = row.nextElementSibling;
            var scroller = scrollParent(list);
            var pointerY = e.clientY;
            var rafId = null, done = false;

            row.classList.add('hsc-sorting');
            document.documentElement.classList.add('hsc-sorting-active');
            try { handle.setPointerCapture(e.pointerId); } catch (err) { }

            // Swap the row with its visible neighbour while the pointer is past that neighbour's middle.
            // The neighbour is the one that moves: moving the dragged row itself would detach the
            // handle and end its pointer capture (and with it the drag).
            function neighbour(dir) {
                var rows = visibleRows(list), i = rows.indexOf(row);
                return rows[i + dir] || null;
            }
            function mid(el) { var b = el.getBoundingClientRect(); return b.top + b.height / 2; }
            function place() {
                for (var guard = 0; guard < 500; guard++) {
                    var prev = neighbour(-1), next = neighbour(1);
                    if (prev && pointerY < mid(prev)) list.insertBefore(prev, row.nextElementSibling);
                    else if (next && pointerY > mid(next)) list.insertBefore(next, row);
                    else break;
                }
            }
            // Scroll while the pointer is near the top or bottom edge of the visible area.
            function tick() {
                rafId = null;
                if (done) return;
                var top = 0, bottom = window.innerHeight;
                if (scroller !== document.scrollingElement && scroller !== document.documentElement) {
                    var sb = scroller.getBoundingClientRect();
                    top = Math.max(top, sb.top); bottom = Math.min(bottom, sb.bottom);
                }
                var edge = 60, step = 0;
                if (pointerY < top + edge) step = -Math.ceil((top + edge - pointerY) / 4);
                else if (pointerY > bottom - edge) step = Math.ceil((pointerY - (bottom - edge)) / 4);
                if (step) {
                    var before = scroller.scrollTop;
                    scroller.scrollTop += step;
                    if (scroller.scrollTop !== before) place();
                    rafId = requestAnimationFrame(tick);
                }
            }
            function onMove(ev) {
                if (ev.pointerId !== e.pointerId) return;
                ev.preventDefault();
                pointerY = ev.clientY;
                place();
                if (!rafId) rafId = requestAnimationFrame(tick);
            }
            function end(commit) {
                if (done) return;
                done = true;
                if (rafId) cancelAnimationFrame(rafId);
                document.removeEventListener('pointermove', onMove);
                document.removeEventListener('pointerup', onUp);
                document.removeEventListener('pointercancel', onCancel);
                handle.removeEventListener('lostpointercapture', onUp);
                document.removeEventListener('keydown', onKey, true);
                try { handle.releasePointerCapture(e.pointerId); } catch (err) { }
                row.classList.remove('hsc-sorting');
                document.documentElement.classList.remove('hsc-sorting-active');
                if (!commit) list.insertBefore(row, startNext && startNext.parentNode === list ? startNext : null);
                finishMove(commit && row.nextElementSibling !== startNext);
            }
            function onUp(ev) { if (ev.pointerId === e.pointerId) end(true); }
            function onCancel(ev) { if (ev.pointerId === e.pointerId) end(false); }
            function onKey(ev) { if (ev.key === 'Escape') { ev.preventDefault(); ev.stopPropagation(); end(false); } }

            document.addEventListener('pointermove', onMove, { passive: false });
            document.addEventListener('pointerup', onUp);
            document.addEventListener('pointercancel', onCancel);
            handle.addEventListener('lostpointercapture', onUp);
            document.addEventListener('keydown', onKey, true);
        });

        // A click on the handle must not open or close the row.
        handle.addEventListener('click', function (e) { e.stopPropagation(); });

        handle.addEventListener('keydown', function (e) {
            if (e.key !== 'ArrowUp' && e.key !== 'ArrowDown') return;
            e.preventDefault();
            e.stopPropagation();
            if (!opts.canDrag()) return;
            var list = opts.list();
            var rows = visibleRows(list);
            var i = rows.indexOf(row);
            // Move the neighbour, not the row: the focused handle stays in place in the DOM.
            if (e.key === 'ArrowUp' && i > 0) list.insertBefore(rows[i - 1], row.nextElementSibling);
            else if (e.key === 'ArrowDown' && i >= 0 && i < rows.length - 1) list.insertBefore(rows[i + 1], row);
            else return;
            row.scrollIntoView({ block: 'nearest' });
            finishMove(true);
        });
    }

    // The source card's current (unsaved) settings, as the TagConfig the server would save.
    function sourceConfigOfRow(view, row) {
        var label = row.querySelector('.txtEntryLabel').value;
        var tag = row.querySelector('.txtTagName').value || label;
        var st = row.querySelector('.selSourceType').value;
        var same = function (r) {
            return r.querySelector('.selSourceType').value === st
                && r.querySelector('.txtEntryLabel').value === label
                && (r.querySelector('.txtTagName').value || r.querySelector('.txtEntryLabel').value) === tag;
        };
        var cands = getUiConfig(view, true).Tags.filter(function (t) { return t.SourceType === st && t.Name === label && t.Tag === tag; });
        // Source rows only: the Top Lists tab's rows share the .tag-row class but have no source fields.
        var rows = Array.from(view.querySelectorAll('.tag-row')).filter(function (r) { return r.querySelector('.selSourceType'); });
        return cands[rows.filter(same).indexOf(row)] || cands[0];
    }

    // The preview popup is in the page's HTML but shown from document.body: inside Emby's page
    // container a position:fixed box can be placed relative to that container instead of the
    // window, off screen when the page is scrolled down. Its close buttons keep working (their
    // listeners are on the elements themselves); its styles and colours are document-wide.
    // The art / badge Customise popup is moved out the same way.
    function bodyOverlay(view, id) {
        var all = view._bodyOverlays || (view._bodyOverlays = {});
        if (!all[id] || !all[id].isConnected) {
            var o = view.querySelector('#' + id) || all[id];
            if (o) { document.body.appendChild(o); all[id] = o; }
        }
        return all[id];
    }
    function previewOverlay(view) { return bodyOverlay(view, 'miPreviewModalOverlay'); }

    // Preview art: draws the collection poster/background from the source's current (unsaved)
    // settings on the server and shows them in the preview dialog. Nothing is saved.
    // forTag: the tag art instead (same endpoint; the Tag* settings stand in for the collection's
    // and the art is named after the tag).
    function showCollectionArtPreview(row, btn, forTag) {
        var view = row.closest('#HomeScreenCompanionConfigPage') || activeView();
        var source = sourceConfigOfRow(view, row);
        if (!source) return;
        var forPlaylist = forTag === 'playlist';
        if (forPlaylist) { forTag = false; source = Object.assign({}, source, {
            CollectionName: source.PlaylistName || source.Name || source.Tag, CollectionPosterStyle: source.PlaylistPosterStyle,
            CollectionBackgroundStyle: source.PlaylistBackgroundStyle, CollectionArtTitle: source.PlaylistArtTitle,
            CollectionBackgroundPath: source.PlaylistBackgroundPath,
            CollectionPosterOptions: source.PlaylistPosterOptions, CollectionBackgroundOptions: source.PlaylistBackgroundOptions,
            CollectionThumbStyle: ''
        }); }
        if (forTag) source = Object.assign({}, source, {
            CollectionName: source.Tag, CollectionPosterStyle: source.TagPosterStyle, CollectionBackgroundStyle: source.TagBackgroundStyle,
            CollectionArtTitle: source.TagArtTitle, CollectionBackgroundPath: source.TagBackgroundPath,
            CollectionPosterOptions: source.TagPosterOptions, CollectionBackgroundOptions: source.TagBackgroundOptions,
            CollectionThumbStyle: ''
        });
        var overlay = previewOverlay(view);
        var body = overlay.querySelector('.mi-preview-body');
        overlay.querySelector('.mi-preview-subtitle').textContent = forTag
            ? (source.Tag || source.Name || 'Tag') + ' — tag art'
            : (source.CollectionName || source.Name || source.Tag || 'Collection') + (forPlaylist ? ' — playlist art' : ' — collection art');
        body.innerHTML = '<div style="padding:20px 0; text-align:center; opacity:0.8;">Drawing the art…</div>';
        overlay.classList.add('modal-visible');
        btn.disabled = true;
        fetch(window.ApiClient.getUrl('HomeScreenCompanion/PreviewCollectionArt'), {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'X-MediaBrowser-Token': window.ApiClient.accessToken() },
            body: JSON.stringify({ Source: source })
        }).then(function (r) { return r.json(); })
        .then(function (res) {
            if (!res.Success) { body.innerHTML = '<div style="padding:12px 0;">' + escapeHtml(res.Message || 'Preview failed.') + '</div>'; return; }
            var html = '<div style="display:flex; gap:16px; align-items:flex-start; flex-wrap:wrap;">';
            if (res.Poster) html += '<div><div class="fieldDescription" style="margin-bottom:6px;">Poster</div><img src="' + res.Poster + '" style="width:180px; border-radius:4px; display:block;" /></div>';
            if (res.Background) html += '<div style="flex:1; min-width:260px;"><div class="fieldDescription" style="margin-bottom:6px;">Background</div><img src="' + res.Background + '" style="width:100%; border-radius:4px; display:block;" /></div>';
            if (res.Thumb) html += '<div style="flex:1; min-width:260px;"><div class="fieldDescription" style="margin-bottom:6px;">Thumb</div><img src="' + res.Thumb + '" style="width:100%; border-radius:4px; display:block;" /></div>';
            html += '</div>';
            if (res.Note) html += '<div class="fieldDescription" style="margin-top:8px;">' + escapeHtml(res.Note) + '</div>';
            body.innerHTML = html;
        }).catch(function (err) {
            body.innerHTML = '<div style="padding:12px 0;">Preview failed: ' + escapeHtml(err.message) + '</div>';
        }).finally(function () { btn.disabled = false; });
    }

    // Preview: runs this source's current (unsaved) settings on the server without tagging
    // anything, and lists what it would tag, in the same popup as the list sources' Test.
    // (External and AI sources have Test instead.)
    function showSourcePreview(row, btn) {
        var label = row.querySelector('.txtEntryLabel').value;
        var tag = row.querySelector('.txtTagName').value || label;
        var sources = rowToFlatTags(row, false);
        if (!sources.length) sources = rowToFlatTags(row, true); // no collection picked yet
        var source = sources[0];
        if (!source) return;
        var st = source.SourceType || '';
        var isMi = st === 'MediaInfo';
        var isNw = st === 'NextWatch';

        var modal = openSourceTestModal('Source preview',
            (label || tag || 'This source') + (isNw ? ' — personal picks for one user' : ' — what it would tag right now'),
            isNw ? 'Reading watch history and scoring the library…' : 'Checking your library…');
        var body = modal.body;
        var users = [];

        function loading(text) {
            return '<div class="stm-empty"><i class="md-icon" style="font-size:2em;display:block;margin-bottom:8px;">hourglass_empty</i>' + escapeHtml(text) + '</div>';
        }

        // Your Next Watch: picks are per user — preview one user at a time (picker on top).
        function nwPickerHtml(nwUserId) {
            if (!isNw) return '';
            var selected = source.PlaylistUserIds || [];
            var ordered = users.filter(function (u) { return selected.indexOf(u.Id) !== -1; })
                .concat(users.filter(function (u) { return selected.indexOf(u.Id) === -1; }));
            return '<div style="display:flex; align-items:center; gap:10px; margin-bottom:12px;"><label style="white-space:nowrap; opacity:0.85;">Picks for</label>'
                + '<select class="selNwPreviewUser" style="flex:1; padding:6px 8px; font-size:inherit; color:inherit; background:var(--plugin-input-bg,rgba(255,255,255,0.08)); border:1px solid var(--plugin-input-border,rgba(255,255,255,0.2)); border-radius:3px;">'
                + (nwUserId ? '' : '<option value="">-- Select user --</option>')
                + ordered.map(function (u) {
                    return '<option value="' + escapeHtml(u.Id) + '"' + (u.Id === nwUserId ? ' selected' : '') + '>' + escapeHtml(u.Name + userRatingSuffix(u)) + (selected.indexOf(u.Id) !== -1 ? '  (ticked on Playlist tab)' : '') + '</option>';
                }).join('') + '</select></div>';
        }

        function render(res, nwUserId) {
            var items = res.Items || [];
            var total = res.Total || items.length;
            var headline = isNw
                ? '<strong style="font-size:1.4em;">' + total + '</strong> <span style="opacity:0.7;">pick' + (total === 1 ? '' : 's') + ' for this user, best first</span>'
                : '<strong style="font-size:1.4em;">' + total.toLocaleString() + '</strong> <span style="opacity:0.7;">title' + (total === 1 ? '' : 's') + ' would get the tag</span>';
            var scanned = isNw ? '' : (res.Scanned || 0).toLocaleString() + (isMi ? ' checked' : st === 'Manual' ? ' picked' : ' in the ' + (st === 'LocalPlaylist' ? 'playlist' : 'collection'));

            var html = '<div class="stm-summary">' + nwPickerHtml(nwUserId)
                + '<div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:8px;gap:12px;">'
                + '<span>' + headline + '</span>'
                + (scanned ? '<span style="opacity:0.7;white-space:nowrap;">' + escapeHtml(scanned) + '</span>' : '')
                + '</div>';
            if (res.Note) html += '<div style="margin-bottom:8px; font-size:0.85em; opacity:0.75;"><i class="md-icon" style="font-size:1em; vertical-align:-2px; margin-right:4px;">info</i>' + escapeHtml(res.Note) + '</div>';
            (res.Warnings || []).forEach(function (w) {
                html += '<div style="margin-bottom:8px; font-size:0.85em; color:var(--hsc-warning-text);"><i class="md-icon" style="font-size:1em; vertical-align:-2px; margin-right:4px;">warning</i>' + escapeHtml(w) + '</div>';
            });
            html += '</div><div class="stm-list">';
            if (!items.length) {
                html += '<div class="stm-empty">' + (isNw ? 'No picks — this user has nothing unwatched they can open.' : 'Nothing matches, so nothing would be tagged.') + '</div>';
            }
            items.forEach(function (it, idx) {
                var poster = it.ImageTag
                    ? '<div class="stm-poster" style="background-image:url(&quot;' + escapeHtml(window.ApiClient.getUrl('Items/' + it.Id + '/Images/Primary', { maxHeight: 100, tag: it.ImageTag })) + '&quot;)"></div>'
                    : '<div class="stm-poster"><i class="md-icon">movie</i></div>';
                var meta = [it.Year, it.Type === 'Series' ? 'Series' : it.Type].filter(Boolean);
                if (it.Viewers != null) meta.push(it.Viewers + ' viewer' + (it.Viewers === 1 ? '' : 's'));
                html += '<div class="stm-row found" style="animation-delay:' + Math.min(idx * 15, 400) + 'ms">'
                    + '<div class="stm-rank">' + (idx + 1) + '</div>' + poster
                    + '<div class="stm-info">'
                    + '<div class="stm-title" title="' + escapeHtml(it.Name) + '">' + escapeHtml(it.Name || '(untitled)') + '</div>'
                    + '<div class="stm-meta">' + escapeHtml(meta.join(' · ')) + '</div>'
                    + (it.Reason ? '<div class="stm-meta" style="white-space:normal;">' + escapeHtml(it.Reason) + '</div>' : '')
                    + '</div>'
                    + '<div class="stm-status in" title="In library"><i class="md-icon">check</i></div>'
                    + '</div>';
            });
            html += '</div><div class="stm-footer"><span>'
                + (total > items.length ? 'Showing the first ' + items.length + ' of ' + total.toLocaleString() + '. ' : '')
                + (isNw ? 'The order of their playlist and home row.' : 'Unsaved settings — nothing is tagged.')
                + '</span><button type="button" class="stm-done">Close</button></div>';
            body.innerHTML = html;
            wireNwPicker();
        }

        function fail(message, nwUserId) {
            modal.fail(message);
            if (isNw) { body.insertAdjacentHTML('afterbegin', '<div class="stm-summary">' + nwPickerHtml(nwUserId) + '</div>'); wireNwPicker(); }
        }

        function wireNwPicker() {
            var sel = body.querySelector('.selNwPreviewUser');
            if (sel) sel.addEventListener('change', function () { if (this.value) load(this.value); });
        }

        function load(nwUserId) {
            if (isNw) {
                row.dataset.nwPreviewUser = nwUserId;
                body.innerHTML = '<div class="stm-summary">' + nwPickerHtml(nwUserId) + '</div>' + loading('Reading watch history and scoring the library…');
                wireNwPicker();
            }
            btn.disabled = true;
            fetch(window.ApiClient.getUrl('HomeScreenCompanion/PreviewSource'), {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', 'X-MediaBrowser-Token': window.ApiClient.accessToken() },
                body: JSON.stringify({ Source: source, Sources: sources, PreviewUserId: isNw ? nwUserId : '' })
            }).then(function (r) { return r.json(); })
            .then(function (res) {
                if (res.Success) render(res, nwUserId);
                else fail(res.Message || 'Preview failed.', nwUserId);
            }).catch(function (err) {
                fail('Preview failed: ' + err.message, nwUserId);
            }).finally(function () { btn.disabled = false; });
        }

        (isNw ? getHseUsers() : Promise.resolve([])).then(function (u) {
            users = u || [];
            load(isNw ? (row.dataset.nwPreviewUser || (source.PlaylistUserIds || [])[0] || '') : '');
        }).catch(function (err) { fail('Preview failed: ' + err.message, ''); });
    }

    // Emby's own toast (bottom notification); falls back to nothing if it can't be loaded.
    function showToast(text) {
        try {
            require(['toast'], function (toast) { (toast && toast.default ? toast.default : toast)(text); });
        } catch (e) { }
    }

    // The collection's name as the sync uses it: Collection Name, else the tag (Display Name).
    function effectiveCollName(row) {
        return (((row.querySelector('.txtCollectionName') || {}).value || '').trim()
            || ((row.querySelector('.txtTagName') || {}).value || '').trim()
            || ((row.querySelector('.txtEntryLabel') || {}).value || '').trim());
    }

    // "Sort title" (Settings > Features): saved as typed; the prefilled default ("!!! " + name) is saved as "".
    // Collection art settings of a source card (generated poster / background).
    function readCollectionArt(row) {
        var poster = row.querySelector('.collection-tab .coll-style-picker[data-kind="poster"] input:checked');
        var bg = row.querySelector('.collection-tab .coll-style-picker[data-kind="background"] input:checked');
        var thumb = row.querySelector('.collection-tab .coll-style-picker[data-kind="thumb"] input:checked');
        var banner = row.querySelector('.collection-tab .coll-style-picker[data-kind="banner"] input:checked');
        return {
            CollectionPosterStyle: poster ? poster.value : '',
            CollectionBackgroundStyle: bg ? bg.value : '',
            CollectionBackgroundPath: (row.querySelector('.collection-tab .coll-bg-upload .hiddenBgPath') || {}).value || '',
            CollectionThumbStyle: thumb ? thumb.value : '',
            CollectionThumbPath: (row.querySelector('.collection-tab .coll-thumb-upload .hiddenBgPath') || {}).value || '',
            CollectionThumbOptions: artOptsOf(row.querySelector('.collection-tab .coll-style-picker[data-kind="thumb"]')),
            CollectionBannerStyle: banner ? banner.value : '',
            CollectionBannerPath: (row.querySelector('.collection-tab .coll-banner-upload .hiddenBgPath') || {}).value || '',
            CollectionKeepOrder: !!(row.querySelector('.chkCollKeepOrder') || {}).checked,
            CollectionArtTitle: ((row.querySelector('.txtCollArtTitle') || {}).value || '').trim(),
            CollectionPosterOptions: artOptsOf(row.querySelector('.collection-tab .coll-style-picker[data-kind="poster"]')),
            CollectionBackgroundOptions: artOptsOf(row.querySelector('.collection-tab .coll-style-picker[data-kind="background"]'))
        };
    }

    // Playlist look of a source card: poster / background (generated style or upload) and
    // description, put on every playlist the source makes (also each Your Next Watch playlist).
    function readPlaylistLook(row) {
        var box = row.querySelector('.playlist-tab .pl-look');
        if (!box) return { PlaylistDescription: '', PlaylistPosterPath: '', PlaylistPosterStyle: '', PlaylistBackgroundStyle: '', PlaylistBackgroundPath: '', PlaylistArtTitle: '', PlaylistPosterOptions: '', PlaylistBackgroundOptions: '' };
        var poster = box.querySelector('.coll-style-picker[data-kind="poster"] input:checked');
        var bg = box.querySelector('.coll-style-picker[data-kind="background"] input:checked');
        return {
            PlaylistDescription: (box.querySelector('.txtPlaylistDescription') || {}).value || '',
            PlaylistPosterPath: (box.querySelector('.hiddenPosterPath') || {}).value || '',
            PlaylistPosterStyle: poster ? poster.value : '',
            PlaylistBackgroundStyle: bg ? bg.value : '',
            PlaylistBackgroundPath: (box.querySelector('.hiddenBgPath') || {}).value || '',
            PlaylistArtTitle: ((box.querySelector('.txtPlArtTitle') || {}).value || '').trim(),
            PlaylistPosterOptions: artOptsOf(box.querySelector('.coll-style-picker[data-kind="poster"]')),
            PlaylistBackgroundOptions: artOptsOf(box.querySelector('.coll-style-picker[data-kind="background"]'))
        };
    }

    // Tag art settings of a source card (generated poster / background on the tag's own page).
    function readTagArt(row) {
        var poster = row.querySelector('.tagname-tab .coll-style-picker[data-kind="poster"] input:checked');
        var bg = row.querySelector('.tagname-tab .coll-style-picker[data-kind="background"] input:checked');
        return {
            TagPosterStyle: poster ? poster.value : '',
            TagBackgroundStyle: bg ? bg.value : '',
            TagBackgroundPath: (row.querySelector('.tagname-tab .hiddenBgPath') || {}).value || '',
            TagArtTitle: ((row.querySelector('.txtTagArtTitle') || {}).value || '').trim(),
            TagPosterOptions: artOptsOf(row.querySelector('.tagname-tab .coll-style-picker[data-kind="poster"]')),
            TagBackgroundOptions: artOptsOf(row.querySelector('.tagname-tab .coll-style-picker[data-kind="background"]'))
        };
    }

    // The Customise options kept in a style picker ('' = the style's own look).
    function artOptsOf(picker) {
        var input = picker && picker.querySelector('.coll-art-opts');
        return input ? input.value || '' : '';
    }

    // The "Playlist look" block of the Playlist tab: the same pickers / upload boxes as the
    // collection art, scoped to .pl-look (style pickers with scope 'playlist').
    function buildPlaylistLookHtml(tc) {
        var esc = function (v) { return (v || '').replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;'); };
        var file = function (p) { return p ? esc(p.split(/[\\/]/).pop()) : ''; };
        var gen = isGenArtStyle(tc.PlaylistPosterStyle) || isGenArtStyle(tc.PlaylistBackgroundStyle);
        var orRow = '<div style="display:flex; align-items:center; gap:6px; margin-top:8px; opacity:0.45;"><div style="flex:1; height:1px; background:currentColor;"></div><span style="font-size:0.75em;">or</span><div style="flex:1; height:1px; background:currentColor;"></div></div>';
        return `
                        <div class="pl-look" style="margin-top:18px;">
                            <p style="margin:0 0 4px 0; font-size:0.95em; font-weight:bold;">Playlist look</p>
                            <div class="fieldDescription" style="margin-bottom:10px;">Every playlist this source makes gets the same poster, background and description instead of Emby's automatic collage of four posters. They are put back on every sync; a playlist is only touched when something changed.</div>
                            <div class="inputContainer">
                                <textarea is="emby-textarea" class="txtPlaylistDescription" rows="3" label="Description"
                                    placeholder="Optional description shown on the playlist's page..."
                                    style="width:100%; resize:vertical; box-sizing:border-box;">${esc(tc.PlaylistDescription)}</textarea>
                                <div class="fieldDescription pl-desc-note">Leave empty for no description.</div>
                            </div>
                            <div style="margin-top:15px;">
                                <p style="margin:0 0 8px 0; font-size:0.9em; font-weight:bold; opacity:0.7;">Playlist Poster</p>
                                ${buildCollStylePickerHtml('poster', tc.PlaylistPosterStyle, 'playlist', tc.PlaylistPosterOptions)}
                                <div class="pl-poster-upload" style="display:${tc.PlaylistPosterStyle ? 'none' : 'block'};">
                                    <div class="poster-preview-container" style="margin-bottom:8px; display:${tc.PlaylistPosterPath ? 'block' : 'none'};">
                                        <span class="poster-filename" style="font-size:0.85em; opacity:0.7;">${file(tc.PlaylistPosterPath)}</span>
                                        <button type="button" class="btnRemovePoster" style="margin-left:10px; font-size:0.8em; background:transparent; border:none; color:var(--hsc-danger-text); cursor:pointer; vertical-align:middle;">✕ Remove</button>
                                    </div>
                                    <img class="poster-preview-img" src="" alt="" style="max-width:120px; max-height:180px; border-radius:4px; display:none; margin-bottom:8px;" />
                                    <input type="file" class="inputPosterFile" accept="image/*" style="display:none;" />
                                    <input type="hidden" class="hiddenPosterPath" value="${esc(tc.PlaylistPosterPath)}" />
                                    <button type="button" is="emby-button" class="btnChoosePoster raised hsc-btn-dashed" style="width:100%;"><i class="md-icon" style="margin-right:5px;">image</i>Choose Poster Image</button>
                                    ${orRow}
                                    <div style="display:flex; gap:6px; margin-top:6px;">
                                        <input class="txtPosterUrl" is="emby-input" type="url" placeholder="https://example.com/poster.jpg" style="flex:1;" />
                                        <button type="button" is="emby-button" class="btnLoadPosterUrl raised btn-neutral">Load</button>
                                    </div>
                                    <div class="fieldDescription">An image that never changes. Leave empty to keep Emby's own collage.</div>
                                </div>
                                <div class="fieldDescription pl-poster-gen-note" style="display:${isGenArtStyle(tc.PlaylistPosterStyle) ? 'block' : 'none'};">Drawn from the posters of the first titles in the list (Your Next Watch: the titles picked for the most users), with the playlist name. It is redrawn when the titles change; choose Custom and an image for a poster that never changes.</div>
                            </div>
                            <div style="margin-top:15px;">
                                <p style="margin:0 0 8px 0; font-size:0.9em; font-weight:bold; opacity:0.7;">Playlist Background</p>
                                ${buildCollStylePickerHtml('background', tc.PlaylistBackgroundStyle, 'playlist', tc.PlaylistBackgroundOptions)}
                                <div class="pl-bg-upload" style="display:${tc.PlaylistBackgroundStyle ? 'none' : 'block'};">
                                    <div class="bg-preview-container" style="margin-bottom:8px; display:${tc.PlaylistBackgroundPath ? 'block' : 'none'};">
                                        <span class="bg-filename" style="font-size:0.85em; opacity:0.7;">${file(tc.PlaylistBackgroundPath)}</span>
                                        <button type="button" class="btnRemoveBg" style="margin-left:10px; font-size:0.8em; background:transparent; border:none; color:var(--hsc-danger-text); cursor:pointer; vertical-align:middle;">✕ Remove</button>
                                    </div>
                                    <img class="bg-preview-img" src="" alt="" style="max-width:240px; max-height:135px; border-radius:4px; display:none; margin-bottom:8px;" />
                                    <input type="file" class="inputBgFile" accept="image/*" style="display:none;" />
                                    <input type="hidden" class="hiddenBgPath" value="${esc(tc.PlaylistBackgroundPath)}" />
                                    <button type="button" is="emby-button" class="btnChooseBg raised hsc-btn-dashed" style="width:100%;"><i class="md-icon" style="margin-right:5px;">image</i>Choose Background Image</button>
                                    ${orRow}
                                    <div style="display:flex; gap:6px; margin-top:6px;">
                                        <input class="txtBgUrl" is="emby-input" type="url" placeholder="https://example.com/background.jpg" style="flex:1;" />
                                        <button type="button" is="emby-button" class="btnLoadBgUrl raised btn-neutral">Load</button>
                                    </div>
                                    <div class="fieldDescription">Leave empty for no background.</div>
                                </div>
                            </div>
                            <div class="inputContainer pl-art-title-row" style="margin-top:15px; display:${gen ? 'block' : 'none'};">
                                <input is="emby-input" type="text" class="txtPlArtTitle" label="Title on the art" value="${esc(tc.PlaylistArtTitle)}" placeholder="{name}" />
                                <div class="fieldDescription">Leave empty to use the playlist name. <b>{name}</b> = playlist name, <b>{week}</b> = this week's number.</div>
                            </div>
                            <div class="pl-art-preview-row" style="margin-top:10px; display:${gen ? 'block' : 'none'};">
                                <button type="button" is="emby-button" class="btnPlArtPreview raised" style="background:transparent; border:1px solid rgba(128,128,128,0.35); color:var(--theme-text-secondary); font-size:0.82em; padding:0 10px; min-width:0;"><i class="md-icon" style="font-size:1em; margin-right:4px;">visibility</i><span>Preview art</span></button>
                            </div>
                        </div>`;
    }

    // Collection art style picker (cards like the top-list badge picker). '' = Custom (upload);
    // for the tag poster (scope 'tag') '' = Emby's own collage. 'keep' = Keep current image: the
    // item's own image is never touched (the default of an imported collection).
    var COLLECTION_ART_STYLES = [['', 'Custom'], ['keep', 'Keep current image'], ['collage', 'Collage'], ['grid', 'Grid'], ['fan', 'Fan'], ['wall', 'Wall'],
                                 ['hero_strip', 'Hero strip'], ['spotlight', 'Spotlight split'], ['ranked', 'Ranked (Top 10)']];
    var _collPickerSeq = 0;
    function isKeepArtStyle(v) { return v === 'keep'; }
    // A generated style (drawn by the server): not Custom / Emby's own ('') and not Keep current.
    function isGenArtStyle(v) { return !!v && v !== 'keep'; }
    // Keep current image is offered on an imported collection until a run has replaced that image
    // with HSC's own (TagConfig.CollectionPosterReplaced / CollectionBackgroundReplaced, set by the sync);
    // while 'keep' is selected it always shows, so nothing breaks.
    // kind: 'poster', 'background', 'thumb' (16:9, drawn like the background) or 'banner' (no
    // generated styles: Custom / Keep only, so no picker at all unless Keep is offered).
    function buildCollStylePickerHtml(kind, selected, scope, options, imported) {
        var sel = selected || '';
        var showKeep = (scope === 'collection' && !!imported) || isKeepArtStyle(sel);
        if (kind === 'banner' && !showKeep) return '';
        var optsVal = (options || '').replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;');
        var embyOwn = scope === 'tag' && kind === 'poster';
        var name = 'collStyle_' + kind + '_' + (++_collPickerSeq);
        var cardBase = 'cursor:pointer;display:flex;flex-direction:column;align-items:center;gap:8px;padding:10px 12px;border-radius:6px;border:2px solid transparent;transition:border-color 0.15s;';
        var thumb = kind === 'poster' ? 'width:60px;height:90px;' : kind === 'banner' ? 'width:150px;height:28px;' : 'width:120px;height:68px;';
        return '<div class="coll-style-picker" data-kind="' + kind + '" data-scope="' + (scope || 'collection') + '" style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:10px;">' +
            COLLECTION_ART_STYLES.filter(function (o) { return (!isKeepArtStyle(o[0]) || showKeep) && (kind !== 'banner' || !isGenArtStyle(o[0])); }).map(function (o) {
                var active = o[0] === sel;
                return '<label class="coll-style-opt" style="' + cardBase + 'border-color:' + (active ? 'var(--hsc-primary)' : 'var(--line-color,rgba(255,255,255,0.12))') + ';">' +
                    '<input type="radio" name="' + name + '" value="' + o[0] + '" style="position:absolute;opacity:0;pointer-events:none;"' + (active ? ' checked' : '') + '>' +
                    (isGenArtStyle(o[0])
                        ? '<img class="coll-style-thumb" data-kind="' + kind + '" data-style="' + o[0] + '" alt="" style="' + thumb + 'border-radius:4px;object-fit:cover;background:rgba(128,128,128,0.15);display:block;" />'
                        : '<div' + (isKeepArtStyle(o[0]) && scope === 'collection' ? ' class="coll-keep-thumb" data-kind="' + kind + '"' : '') + ' style="' + thumb + 'border-radius:4px;border:1px dashed rgba(128,128,128,0.5);display:flex;align-items:center;justify-content:center;"><i class="md-icon" style="font-size:1.6em;opacity:0.7;">' + (isKeepArtStyle(o[0]) ? 'lock' : embyOwn ? 'block' : 'upload') + '</i></div>') +
                    '<span style="font-size:0.78em;opacity:0.8;white-space:nowrap;">' + (o[0] || !embyOwn ? o[1] : "Emby's own") + '</span>' +
                    '</label>';
            }).join('') +
            // Customise (generated styles only): opens the art popup; its options live here.
            '<input type="hidden" class="coll-art-opts" value="' + optsVal + '" />' +
            '<div class="coll-art-custom-row" style="flex-basis:100%;display:' + (isGenArtStyle(sel) ? 'flex' : 'none') + ';align-items:center;gap:8px;">' +
                '<button type="button" is="emby-button" class="btnArtCustomise raised btn-neutral"><i class="md-icon" style="margin-right:5px;">settings</i><span>Customise</span></button>' +
                '<span class="coll-art-custom-note fieldDescription" style="margin:0;' + (options ? '' : 'display:none;') + '">Customised</span>' +
            '</div>' +
            '<div class="coll-art-keep-note fieldDescription" style="flex-basis:100%;margin:0;display:' + (isKeepArtStyle(sel) ? 'block' : 'none') + ';">' +
                'Keeps the ' + kind + ' this ' + (scope === 'tag' ? 'tag' : scope === 'playlist' ? 'playlist' : 'collection') +
                ' has now, exactly as it is: nothing is drawn, uploaded or removed.</div>' +
            '</div>';
    }

    // Custom upload box (file or URL) for the collection thumb / banner: the same markup as the
    // background's, so wireBgUpload drives it. Hidden while a generated style or Keep is chosen.
    function collImageUploadHtml(cls, style, path, label, example, note, imgSize) {
        var p = path || '';
        var pAttr = p.replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;');
        var file = p ? p.split(/[\\/]/).pop().replace(/&/g, '&amp;').replace(/</g, '&lt;') : '';
        return '<div class="' + cls + '" style="display:' + (style ? 'none' : 'block') + ';">' +
            '<div class="bg-preview-container" style="margin-bottom:8px; display:' + (p ? 'block' : 'none') + ';">' +
                '<span class="bg-filename" style="font-size:0.85em; opacity:0.7;">' + file + '</span>' +
                '<button type="button" class="btnRemoveBg" style="margin-left:10px; font-size:0.8em; background:transparent; border:none; color:var(--hsc-danger-text); cursor:pointer; vertical-align:middle;">✕ Remove</button>' +
            '</div>' +
            '<img class="bg-preview-img" src="" alt="" style="' + imgSize + ' border-radius:4px; display:none; margin-bottom:8px;" />' +
            '<input type="file" class="inputBgFile" accept="image/*" style="display:none;" />' +
            '<input type="hidden" class="hiddenBgPath" value="' + pAttr + '" />' +
            '<button type="button" is="emby-button" class="btnChooseBg raised hsc-btn-dashed" style="width:100%;">' +
                '<i class="md-icon" style="margin-right:5px;">image</i>Choose ' + label + ' Image' +
            '</button>' +
            '<div style="display:flex; align-items:center; gap:6px; margin-top:8px; opacity:0.45;">' +
                '<div style="flex:1; height:1px; background:currentColor;"></div>' +
                '<span style="font-size:0.75em;">or</span>' +
                '<div style="flex:1; height:1px; background:currentColor;"></div>' +
            '</div>' +
            '<div style="display:flex; gap:6px; margin-top:6px;">' +
                '<input class="txtBgUrl" is="emby-input" type="url" placeholder="https://example.com/' + example + '" style="flex:1;" />' +
                '<button type="button" is="emby-button" class="btnLoadBgUrl raised btn-neutral">Load</button>' +
            '</div>' +
            '<div class="fieldDescription">' + note + '</div>' +
        '</div>';
    }

    // ── Customise popup (generated art) ──────────────────────────────────────────────
    // One screen for every poster and background picker (collection, tag, playlist): the same
    // fields for every style, starting at that style's own look (from the server). The live
    // preview is drawn by the server's renderer; nothing is drawn here. OK writes the changed
    // fields (only those that differ from the style's look) into the picker's .coll-art-opts.
    var _artInfoPromise = null;
    function getArtCustomiseInfo() {
        if (!_artInfoPromise)
            _artInfoPromise = window.ApiClient.getJSON(window.ApiClient.getUrl('HomeScreenCompanion/ArtCustomiseInfo'))
                .catch(function (e) { _artInfoPromise = null; throw e; });
        return _artInfoPromise;
    }

    var ART_STYLE_NAMES = { collage: 'Collage', grid: 'Grid', fan: 'Fan', wall: 'Wall', hero_strip: 'Hero strip', spotlight: 'Spotlight split', ranked: 'Ranked (Top 10)' };
    var ART_LIMITS = { rows: 10, posters: 250 };   // Customise "Custom" boxes (the server allows the same)
    var ART_FIELDS = ['tilt', 'rows', 'posters', 'title', 'font', 'pos', 'size', 'case', 'colour', 'darken'];
    var ART_COLOURS = [['#ffffff', 'White'], ['#f5c518', 'Gold'], ['#e50914', 'Red'], ['#ff7a00', 'Orange'], ['#7fd4ff', 'Ice blue'], ['#ff6fb5', 'Pink'], ['#9be37a', 'Green'], ['#111111', 'Black']];

    // The style's own value of every field (from the server's defaults).
    function artDefaultsToState(d) {
        return { tilt: d.Tilt, rows: d.Rows || 0, posters: d.Posters || 0, title: !!d.ShowTitle, font: d.Font, pos: d.Pos,
                 size: d.Size, 'case': d.Case, colour: (d.Colour || '#ffffff').toLowerCase(), darken: d.Darken || 0 };
    }

    // Changed fields only, as stored ('' when nothing differs from the style's look).
    function artStateToOptions(state, def) {
        var out = {}, any = false;
        ART_FIELDS.forEach(function (k) {
            if (state[k] === def[k]) return;
            if ((k === 'rows' || k === 'posters') && !state[k]) return;
            out[k] = state[k]; any = true;
        });
        return any ? JSON.stringify(out) : '';
    }

    function openArtCustomise(picker) {
        var view = picker.closest('#HomeScreenCompanionConfigPage') || activeView();
        var row = picker.closest('.tag-row');
        var overlay = bodyOverlay(view, 'artCustomiseModalOverlay');
        var checked = picker.querySelector('input[type=radio]:checked');
        var style = checked ? checked.value : '';
        if (!overlay || !style) return;
        var kindName = picker.dataset.kind, scope = picker.dataset.scope || 'collection';
        // A thumb is drawn like the background (same 16:9 canvas, defaults and preview).
        var bg = kindName !== 'poster';
        var kind = bg ? 'background' : 'poster';
        var optsInput = picker.querySelector('.coll-art-opts');
        var body = overlay.querySelector('.artc-body');
        var scopeName = { collection: 'Collection', tag: 'Tag', playlist: 'Playlist' }[scope] || 'Collection';
        overlay.querySelector('.artc-title').textContent = 'Customise ' + kindName;
        overlay.querySelector('.artc-subtitle').textContent = scopeName + ' ' + kindName + ' — ' + (ART_STYLE_NAMES[style] || style);
        body.innerHTML = '<div style="padding:20px 0; text-align:center; opacity:0.8;">Loading…</div>';
        overlay._artcCancel = null;
        overlay._artcOk = function () { overlay.classList.remove('modal-visible'); };   // until loaded: never the last popup's OK
        overlay._artcReset = function () { };
        overlay.classList.add('modal-visible');

        // Title text and name the art would carry (same as the card's own art).
        function q(sel) { return row ? ((row.querySelector(sel) || {}).value || '').trim() : ''; }
        var label = q('.txtEntryLabel');
        var titleTpl = scope === 'tag' ? q('.txtTagArtTitle') : scope === 'playlist' ? q('.txtPlArtTitle') : q('.txtCollArtTitle');
        var name = scope === 'tag' ? (q('.txtTagName') || label) : scope === 'playlist' ? (q('.txtPlaylistName') || label) : (q('.txtCollectionName') || label);

        getArtCustomiseInfo().then(function (info) {
            var def = artDefaultsToState(info.Defaults[kind + '|' + style] || {});
            var hints = (info.Defaults[kind + '|' + style] || {}).Hints || {};
            var autoPosters = (info.Defaults[kind + '|' + style] || {}).AutoPosters || 0;
            var stored = {};
            try { stored = optsInput.value ? JSON.parse(optsInput.value) : {}; } catch (e) { stored = {}; }
            var state = Object.assign({}, def);
            ART_FIELDS.forEach(function (k) { if (stored[k] !== undefined && stored[k] !== null) state[k] = stored[k]; });
            if (typeof state.colour === 'string') state.colour = state.colour.toLowerCase();
            // A saved font the server no longer has draws as the style's own font: show that one.
            if (!info.Fonts.some(function (f) { return f.Id === state.font; })) state.font = def.font;
            render();

            function seg(field, opts) {
                return '<div class="artc-seg" data-field="' + field + '">' + opts.map(function (o) {
                    return '<button type="button" class="artc-segbtn' + (String(state[field]) === String(o[0]) ? ' artc-on' : '') + '" data-value="' + o[0] + '">' + o[1] + '</button>';
                }).join('') + '</div>';
            }
            // Preset buttons plus a "Custom" number box (Rows 1-10, Posters 1-250). A number typed in
            // the box is the value; clicking a preset empties the box.
            function segCustom(field, opts) {
                var preset = opts.some(function (o) { return String(o[0]) === String(state[field]); });
                var v = !preset && state[field] ? state[field] : '';
                return seg(field, opts) + '<label class="artc-segcustom">Custom <input type="number" class="artc-custom' + (v ? ' artc-on' : '') +
                    '" data-field="' + field + '" min="1" max="' + ART_LIMITS[field] + '" step="1" placeholder="1–' + ART_LIMITS[field] + '" value="' + v + '" /></label>';
            }
            // How many titles the source's tag holds now (for the "posters repeat" note); null = unknown.
            var titleCount = null;
            function postersHintText() {
                if (hints.posters) return hints.posters;
                if (titleCount != null && titleCount > 0 && state.posters > titleCount)
                    return 'This source has ' + titleCount + ' title' + (titleCount === 1 ? '' : 's') + '; posters repeat (never next to a copy).';
                return '';
            }
            function updatePostersHint() {
                var h = body.querySelector('.artc-field[data-f="posters"] .artc-hint');
                if (!h) return;
                var t = postersHintText();
                h.textContent = t; h.style.display = t ? '' : 'none';
            }
            (function countTitles() {
                var tag = scope === 'tag' ? q('.txtTagName') : (q('.txtTagName') || label);
                if (!tag || !window.ApiClient.getCurrentUserId) return;
                window.ApiClient.getJSON(window.ApiClient.getUrl('Users/' + window.ApiClient.getCurrentUserId() + '/Items', {
                    Recursive: true, Tags: tag, IncludeItemTypes: 'Movie,Series', Limit: 0
                })).then(function (r) {
                    titleCount = r && typeof r.TotalRecordCount === 'number' ? r.TotalRecordCount : null;
                    updatePostersHint();
                }).catch(function () { });
            })();
            function hint(field, extra) {
                var t = hints[field] || extra || '';
                return '<div class="artc-hint"' + (t ? '' : ' style="display:none;"') + '>' + escapeHtml(t) + '</div>';
            }
            // Only fields that change the art are shown: Ranked keeps its own rows, posters and
            // background (the server's hint for those says so), and the title fields need the title on.
            function shown(f) {
                if ((f === 'rows' || f === 'posters' || f === 'darken') && hints[f]) return false;
                if (!state.title && ['font', 'pos', 'size', 'case', 'colour'].indexOf(f) >= 0) return false;
                // Bebas Neue and Luckiest Guy have capitals only: UPPER / As typed look the same.
                if (f === 'case' && ['bebas', 'luckiestguy'].indexOf(state.font) >= 0) return false;
                return true;
            }
            function field(label, html, f, extra) {
                if (!shown(f)) return '';
                if (f === 'posters') extra = postersHintText();
                return '<div class="artc-field" data-f="' + f + '"><div class="artc-label">' + label + '</div>' + html + hint(f, extra) + '</div>';
            }
            function render() {
                var titleOff = state.title ? '' : 'The title is off.';
                var fonts = fontTilesHtml(info.Fonts, state.font, def.font);
                var dots = '<div class="artc-pos">' + ['tl', 'tc', 'tr', 'ml', 'mc', 'mr', 'bl', 'bc', 'br'].map(function (p) {
                    return '<button type="button" class="artc-dot' + (state.pos === p ? ' artc-on' : '') + '" data-value="' + p + '" title="' + p + '"><span></span></button>';
                }).join('') + '</div>';
                var swatches = '<div class="artc-colours">' + ART_COLOURS.map(function (c) {
                    return '<button type="button" class="artc-swatch' + (state.colour === c[0] ? ' artc-on' : '') + '" data-value="' + c[0] + '" title="' + c[1] + '" style="background:' + c[0] + ';"></button>';
                }).join('') + '<input type="text" class="artc-hex" maxlength="7" value="' + escapeHtml(state.colour) + '" spellcheck="false" /></div>';

                body.innerHTML =
                    '<div class="artc-wrap">' +
                      '<div class="artc-previewcol">' +
                        '<div class="artc-preview ' + (bg ? 'artc-bg' : 'artc-poster') + '"><img class="artc-img" alt="" /><div class="artc-busy">Drawing…</div></div>' +
                        '<div class="fieldDescription" style="margin-top:6px;">Drawn with stand-in posters. <em>Preview art</em> on the card shows your real titles.</div>' +
                      '</div>' +
                      '<div class="artc-fieldscol">' +
                        '<div class="artc-section">Layout</div>' +
                        field('Tilt', seg('tilt', [['straight', 'Straight'], ['left', 'Left'], ['right', 'Right']]), 'tilt') +
                        field('Rows', segCustom('rows', [[0, 'Auto'], [2, '2'], [3, '3'], [4, '4'], [5, '5']]), 'rows') +
                        field('Posters', segCustom('posters', [[0, postersAutoLabel()], [4, '4'], [6, '6'], [9, '9'], [12, '12'], [16, '16'], [20, '20']]), 'posters') +
                        '<div class="artc-section">Title</div>' +
                        field('Show title', seg('title', [[true, 'On'], [false, 'Off']]), 'title') +
                        field('Font', fonts, 'font', titleOff) +
                        field('Position', dots, 'pos', titleOff) +
                        field('Size', seg('size', [['s', 'S'], ['m', 'M'], ['l', 'L']]), 'size', titleOff) +
                        field('Case', seg('case', [['upper', 'UPPER'], ['typed', 'As typed']]), 'case', titleOff) +
                        field('Colour', swatches, 'colour', titleOff) +
                        (shown('darken') ? '<div class="artc-section">Darken</div>' : '') +
                        field('Darken <span class="artc-darkval">' + state.darken + '%</span>', '<input type="range" class="artc-darken" min="0" max="80" step="1" value="' + state.darken + '" />', 'darken') +
                      '</div>' +
                    '</div>';
                wire();
                refreshPreview();
            }
            function set(f, v) { state[f] = v; render(); }
            // The style's own count on Auto; with Rows set the rows fill the width, so no number.
            function postersAutoLabel() { return 'Auto' + (autoPosters && !state.rows ? ' (' + autoPosters + ')' : ''); }
            function wire() {
                body.querySelectorAll('.artc-seg').forEach(function (g) {
                    g.querySelectorAll('.artc-segbtn').forEach(function (b) {
                        b.addEventListener('click', function () {
                            var v = b.dataset.value, f = g.dataset.field;
                            set(f, f === 'rows' || f === 'posters' ? parseInt(v, 10) : f === 'title' ? v === 'true' : v);
                        });
                    });
                });
                body.querySelectorAll('.artc-custom').forEach(function (inp) {
                    inp.addEventListener('input', function () {
                        var f = inp.dataset.field, n = parseInt(inp.value, 10);
                        if (inp.value.trim() === '') n = 0;   // box emptied: back to Auto
                        else if (!(n >= 1 && n <= ART_LIMITS[f]) || String(n) !== inp.value.trim()) return;   // keep the last good value
                        state[f] = n;
                        var g = body.querySelector('.artc-seg[data-field="' + f + '"]');
                        g.querySelectorAll('.artc-segbtn').forEach(function (b) { b.classList.toggle('artc-on', String(b.dataset.value) === String(n)); });
                        inp.classList.toggle('artc-on', !g.querySelector('.artc-segbtn.artc-on'));
                        if (f === 'posters') updatePostersHint();
                        if (f === 'rows') { var a = body.querySelector('.artc-seg[data-field="posters"] .artc-segbtn[data-value="0"]'); if (a) a.textContent = postersAutoLabel(); }
                        refreshPreview();
                    });
                });
                body.querySelectorAll('.artc-font').forEach(function (b) { b.addEventListener('click', function () { set('font', b.dataset.value); }); });
                body.querySelectorAll('.artc-dot').forEach(function (b) { b.addEventListener('click', function () { set('pos', b.dataset.value); }); });
                body.querySelectorAll('.artc-swatch').forEach(function (b) { b.addEventListener('click', function () { set('colour', b.dataset.value); }); });
                var hex = body.querySelector('.artc-hex');
                if (hex) hex.addEventListener('input', function () {
                    var v = hex.value.trim().toLowerCase();
                    if (v && v[0] !== '#') v = '#' + v;
                    if (/^#[0-9a-f]{6}$/.test(v)) {
                        state.colour = v;
                        body.querySelectorAll('.artc-swatch').forEach(function (s) { s.classList.toggle('artc-on', s.dataset.value === v); });
                        refreshPreview();
                    }
                });
                var dark = body.querySelector('.artc-darken');
                if (dark) dark.addEventListener('input', function () {
                    state.darken = parseInt(dark.value, 10) || 0;
                    body.querySelector('.artc-darkval').textContent = state.darken + '%';
                    refreshPreview();
                });
            }
            var timer = null, seq = 0;
            function refreshPreview() {
                clearTimeout(timer);
                var busy = body.querySelector('.artc-busy');
                if (busy) busy.style.display = 'block';
                timer = setTimeout(function () {
                    var mine = ++seq;
                    fetch(window.ApiClient.getUrl('HomeScreenCompanion/ArtCustomPreview'), {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json', 'X-MediaBrowser-Token': window.ApiClient.accessToken() },
                        body: JSON.stringify({ Style: style, Background: bg, Options: artStateToOptions(state, def), Title: titleTpl, Name: name })
                    }).then(function (r) { return r.json(); }).then(function (res) {
                        if (mine !== seq) return;   // a newer drawing is on its way
                        var img = body.querySelector('.artc-img'), b = body.querySelector('.artc-busy');
                        if (res.Success && img) { img.src = res.Image; if (b) b.style.display = 'none'; }
                        // Rows and Posters both set: say when fewer rows fit better ("16 posters fit best on 4 rows.").
                        var rh = body.querySelector('.artc-field[data-f="rows"] .artc-hint');
                        if (rh && !hints.rows) { rh.textContent = (res.Success && res.Note) || ''; rh.style.display = res.Success && res.Note ? '' : 'none'; }
                        else if (b) b.textContent = res.Message || 'Preview failed.';
                    }).catch(function (e) {
                        if (mine !== seq) return;
                        var b = body.querySelector('.artc-busy'); if (b) b.textContent = 'Preview failed: ' + e.message;
                    });
                }, 300);
            }

            overlay._artcReset = function () { state = Object.assign({}, def); render(); };
            overlay._artcOk = function () {
                var val = artStateToOptions(state, def);
                if (optsInput.value !== val) {
                    optsInput.value = val;
                    var note = picker.querySelector('.coll-art-custom-note');
                    if (note) note.style.display = val ? '' : 'none';
                    setTimeout(checkFormState, 0);
                }
                overlay.classList.remove('modal-visible');
            };
        }).catch(function (e) {
            body.innerHTML = '<div style="padding:12px 0;">Could not load the options: ' + escapeHtml(e && e.message || String(e)) + '</div>';
            overlay._artcOk = function () { overlay.classList.remove('modal-visible'); };
            overlay._artcReset = function () { };
        });

        if (!overlay._artcWired) {
            overlay._artcWired = true;
            overlay.querySelector('.btnArtcCancel').addEventListener('click', function () {
                var undo = overlay._artcCancel; overlay._artcCancel = null;
                overlay.classList.remove('modal-visible');
                if (undo) undo();
            });
            overlay.querySelector('.btnArtcOk').addEventListener('click', function () { if (overlay._artcOk) overlay._artcOk(); });
            overlay.querySelector('.btnArtcReset').addEventListener('click', function () { if (overlay._artcReset) overlay._artcReset(); });
        }
    }

    // Keep current image tile of a collection: shows the poster / background the source's Emby
    // collection has now (found by name, as HSC finds it). No collection or no image: the lock stays.
    var _keepBoxSetsPromise = null;
    function fillKeepThumbs() {
        var tiles = document.querySelectorAll('.coll-keep-thumb:not([data-filled])');
        if (!tiles.length) return;
        tiles.forEach(function (t) { t.dataset.filled = '1'; });
        if (!_keepBoxSetsPromise) {
            // One lookup per render pass, dropped afterwards so a re-rendered card shows the image as it is now.
            _keepBoxSetsPromise = window.ApiClient.getJSON(window.ApiClient.getUrl('Users/' + window.ApiClient.getCurrentUserId() + '/Items',
                    { IncludeItemTypes: 'BoxSet', Recursive: true }))
                .then(function (r) { return (r && r.Items) || []; }, function () { return []; });
            _keepBoxSetsPromise.then(function () { setTimeout(function () { _keepBoxSetsPromise = null; }, 2000); });
        }
        _keepBoxSetsPromise.then(function (boxSets) {
            tiles.forEach(function (tile) {
                var row = tile.closest('.tag-row');
                if (!row) return;
                function v(sel) { return ((row.querySelector(sel) || {}).value || '').trim(); }
                var name = (v('.txtCollectionName') || v('.txtTagName') || v('.txtEntryLabel')).toLowerCase();
                var bs = name && boxSets.filter(function (b) { return (b.Name || '').toLowerCase() === name; })[0];
                if (!bs) return;
                var k = tile.dataset.kind;
                var tag = k === 'background' ? (bs.BackdropImageTags || [])[0]
                    : (bs.ImageTags || {})[k === 'thumb' ? 'Thumb' : k === 'banner' ? 'Banner' : 'Primary'];
                if (!tag) return;
                var url = window.ApiClient.getImageUrl(bs.Id, k === 'background' ? { type: 'Backdrop', index: 0, maxWidth: 240, tag: tag }
                    : k === 'thumb' ? { type: 'Thumb', maxWidth: 240, tag: tag }
                    : k === 'banner' ? { type: 'Banner', maxWidth: 300, tag: tag }
                    : { type: 'Primary', maxHeight: 180, tag: tag });
                tile.style.border = 'none';
                tile.innerHTML = '<img alt="" src="' + url + '" style="width:100%;height:100%;border-radius:4px;object-fit:cover;display:block;" />';
            });
        });
    }

    var _artSamplesPromise = null;
    function fillCollStyleThumbs() {
        fillKeepThumbs();
        var imgs = document.querySelectorAll('img.coll-style-thumb:not([src])');
        if (!imgs.length) return;
        if (!_artSamplesPromise)
            _artSamplesPromise = window.ApiClient.getJSON(window.ApiClient.getUrl('HomeScreenCompanion/ArtStyleSamples'))
                .catch(function () { _artSamplesPromise = null; return null; });
        _artSamplesPromise.then(function (samples) {
            if (!samples) return;
            document.querySelectorAll('img.coll-style-thumb:not([src])').forEach(function (img) {
                var set = img.dataset.kind === 'poster' ? samples.Poster : samples.Background;   // a thumb looks like the background
                var url = set && set[img.dataset.style];
                if (url) img.src = url;
            });
        });
    }

    // Asks before a group is deleted and lists what it made that goes with it: its home rows and
    // playlists (as the sync tracked them), its tag and its collection. A tag or collection name
    // another group also uses, or an imported collection, is listed as kept. The server does the
    // clean-up on the next full sync after Save.
    function confirmDeleteGroup(row, onDelete) {
        function esc(s) { return String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;'); }
        function decodeList(v) { try { return JSON.parse(decodeURIComponent(v || '%5B%5D')) || []; } catch (e) { return []; } }
        var cfg = readRowAsConfig(row);
        var label = (cfg.Name || '').trim(), tag = (cfg.Tag || '').trim();
        var title = label || tag;
        var hseTab = row.querySelector('.homescreen-tab'), plTab = row.querySelector('.playlist-tab');
        var tracked = decodeList(hseTab && hseTab.dataset.hseTracked).filter(function (t) { return t && t.UserId && t.SectionId; });
        var mappings = decodeList(plTab && plTab.dataset.plMappings).filter(function (m) { return m && m.UserId && m.PlaylistId; });
        var collName = (cfg.CollectionName || '').trim() || tag;

        // Other groups on the page that share the tag or the collection name keep them.
        var others = Array.from(document.querySelectorAll('.tag-row')).filter(function (r) { return r !== row && r.querySelector('.selSourceType'); });
        function lc(s) { return (s || '').trim().toLowerCase(); }
        var tagShared = others.some(function (r) { return lc((r.querySelector('.txtTagName') || {}).value || (r.querySelector('.txtEntryLabel') || {}).value) === lc(tag); });
        var collShared = others.some(function (r) {
            if (!(r.querySelector('.chkEnableCollection') || {}).checked) return false;
            var n = (r.querySelector('.txtCollectionName') || {}).value || (r.querySelector('.txtTagName') || {}).value;
            return lc(n) === lc(collName);
        });

        getHseUsers().catch(function () { return []; }).then(function (users) {
            function names(ids) {
                var seen = {};
                return ids.filter(function (id) { var k = id.toLowerCase(); if (seen[k]) return false; seen[k] = 1; return true; })
                    .map(function (id) { var u = users.find(function (x) { return (x.Id || '').toLowerCase() === id.toLowerCase(); }); return u ? u.Name : 'a removed user'; })
                    .join(', ');
            }
            var removed = [], kept = [];
            if (tracked.length) {
                var rowName = '';
                try { rowName = (JSON.parse(cfg.HomeSectionSettings || '{}').CustomName || '').trim(); } catch (e) {}
                removed.push(['home', 'Home screen row' + (rowName ? ' "' + esc(rowName) + '"' : ''), 'for ' + esc(names(tracked.map(function (t) { return t.UserId; })))]);
            }
            if (mappings.length) {
                var plName = (cfg.PlaylistName || '').trim() || title;
                removed.push(['queue_music', 'Playlist "' + esc(plName) + '"', 'for ' + esc(names(mappings.map(function (m) { return m.UserId; })))]);
            }
            if (cfg.EnableTag && tag) {
                if (tagShared) kept.push(['label', 'Tag "' + esc(tag) + '"', 'another group uses it too']);
                else removed.push(['label', 'Tag "' + esc(tag) + '"', 'taken off every title that has it']);
            }
            if (row.dataset.tlSaved === '1') {
                var tlTitle = '';
                try { tlTitle = (JSON.parse(cfg.HomeSectionSettings || '{}').CustomName || '').trim(); } catch (e) {}
                removed.push(['format_list_numbered', 'Top-list "' + esc(tlTitle || title) + '"', 'its home rows, its library and the copies in it']);
            }
            if (cfg.EnableCollection && collName) {
                if (cfg.CollectionImported) kept.push(['video_library', 'Collection "' + esc(collName) + '"', 'it was imported, so it stays in Emby']);
                else if (collShared) kept.push(['video_library', 'Collection "' + esc(collName) + '"', 'another group uses it too']);
                else removed.push(['video_library', 'Collection "' + esc(collName) + '"', 'deleted from Emby (the titles in it are not touched)']);
            }

            showDeleteDialog({
                title: 'Delete ' + (title ? '"' + esc(title) + '"' : 'this group') + '?',
                subtitle: label && tag && lc(label) !== lc(tag) ? 'Tag: ' + esc(tag) : '',
                removed: removed, kept: kept,
                empty: 'This group has not made anything in Emby yet, so only its settings are deleted.',
                note: 'Nothing changes until you press Save. ' +
                    (removed.length ? 'The items above are removed at the next full sync. ' : '') +
                    'Your movies and shows themselves are never deleted.',
                button: 'Delete group',
                onDelete: onDelete
            });
        });
    }

    // Confirmation popup for deleting a group or a top-list. removed / kept are lists of
    // [icon, text, detail] (HTML already escaped); empty is shown when nothing is removed.
    function showDeleteDialog(o) {
        function list(items, color) {
            return items.map(function (it) {
                return '<div style="display:flex;gap:10px;align-items:flex-start;padding:6px 0;">' +
                    '<i class="md-icon" style="font-size:1.15em;color:' + color + ';margin-top:1px;">' + it[0] + '</i>' +
                    '<div><div>' + it[1] + '</div><div style="font-size:0.82em;opacity:0.65;">' + it[2] + '</div></div></div>';
            }).join('');
        }
        function heading(text) {
            return '<div class="hsc-eyebrow" style="margin:14px 0 2px;">' + text + '</div>';
        }
        var body = o.removed.length
            ? heading('Will also be removed') + list(o.removed, 'var(--hsc-danger-text)')
            : '<div style="opacity:0.75;margin-top:8px;">' + o.empty + '</div>';
        if (o.kept && o.kept.length) body += heading('Kept') + list(o.kept, 'var(--hsc-primary)');
        body += '<div style="font-size:0.82em;opacity:0.65;margin-top:16px;line-height:1.5;">' + o.note + '</div>';

        var modal = document.createElement('div');
        modal.style.cssText = 'position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.6);z-index:9999;display:flex;align-items:center;justify-content:center;';
        modal.innerHTML =
            '<div style="background:var(--plugin-popup-bg,#2a2a2a);color:var(--plugin-popup-color,#e8e8e8);border:1px solid var(--plugin-popup-border,rgba(255,255,255,0.12));border-radius:12px;box-shadow:var(--hsc-shadow-modal);overscroll-behavior:contain;padding:24px 28px;max-width:520px;width:90%;max-height:80vh;overflow-y:auto;">' +
            '<div style="display:flex;justify-content:space-between;align-items:center;gap:12px;">' +
            '<h3 style="margin:0;font-size:1.05em;font-weight:600;">' + o.title + '</h3>' +
            '<button type="button" class="dgClose hsc-x" aria-label="Close" title="Close" style="background:transparent;border:none;color:inherit;cursor:pointer;padding:2px;opacity:0.6;line-height:1;border-radius:8px;"><i class="md-icon">close</i></button>' +
            '</div>' +
            (o.subtitle ? '<div style="font-size:0.82em;opacity:0.6;margin-top:2px;">' + o.subtitle + '</div>' : '') +
            body +
            '<div style="display:flex;justify-content:flex-end;gap:12px;margin-top:20px;border-top:1px solid var(--line-color);padding-top:16px;">' +
            '<button type="button" class="dgCancel hsc-mbtn hsc-btn-secondary">Cancel</button>' +
            '<button type="button" class="dgDelete hsc-mbtn hsc-btn-danger-fill"><i class="md-icon" style="font-size:1em;vertical-align:middle;margin-right:5px;">delete</i>' + o.button + '</button>' +
            '</div></div>';
        function close() { modal.remove(); document.removeEventListener('keydown', onKey); }
        function onKey(e) { if (e.key === 'Escape') close(); }
        document.addEventListener('keydown', onKey);
        modal.addEventListener('click', function (e) {
            if (e.target === modal || e.target.closest('.dgClose') || e.target.closest('.dgCancel')) close();
            else if (e.target.closest('.dgDelete')) { close(); o.onDelete(); }
        });
        document.body.appendChild(modal);
    }

    // Asks before a top-list is deleted (straight away, not on Save) and lists what goes with it:
    // its home rows, its library with the copies in it, and the badges on the real posters.
    function confirmDeleteTopList(tagName, onDelete) {
        function esc(s) { return String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;'); }
        function norm(p) { return (p || '').replace(/\\/g, '/').replace(/\/+$/, '').toLowerCase(); }
        var folderName = (tagName || '').replace(/[\\/:*?"<>|]/g, '_').trim().toLowerCase();
        Promise.all([
            window.ApiClient.getPluginConfiguration(pluginId).catch(function () { return {}; }),
            window.ApiClient.getJSON(window.ApiClient.getUrl('Library/VirtualFolders')).catch(function () { return []; }),
            getHseUsers().catch(function () { return []; })
        ]).then(function (res) {
            var tl = ((res[0] && res[0].TopLists) || []).find(function (t) { return (t.TagName || '').toLowerCase() === (tagName || '').toLowerCase(); }) || {};
            var users = res[2];
            var settings = {};
            try { settings = JSON.parse(tl.HomeSectionSettings || '{}') || {}; } catch (e) {}
            var isShows = tl.ContentType === 'Shows';
            // The list's own library: by the id it stores, else by its folder name under toplists/.
            var folders = res[1] || [];
            var lib = folders.find(function (f) { return tl.HomeSectionLibraryId && String(f.ItemId) === String(tl.HomeSectionLibraryId); })
                || folders.find(function (f) {
                    return (f.Locations || []).some(function (loc) {
                        var n = norm(loc), last = n.slice(n.lastIndexOf('/') + 1);
                        return n.indexOf('/toplists/') >= 0 && (last === folderName || last === (tagName || '').trim().toLowerCase());
                    });
                });
            var tracked = (tl.HomeSectionTracked || []).filter(function (t) { return t && t.UserId && t.SectionId; });
            var seen = {};
            var names = tracked.map(function (t) { return t.UserId; })
                .filter(function (id) { var k = id.toLowerCase(); if (seen[k]) return false; seen[k] = 1; return true; })
                .map(function (id) { var u = users.find(function (x) { return (x.Id || '').toLowerCase() === id.toLowerCase(); }); return u ? u.Name : 'a removed user'; });

            var removed = [];
            if (names.length)
                removed.push(['home', 'Home screen row' + (settings.CustomName ? ' "' + esc(settings.CustomName) + '"' : ''), 'for ' + esc(names.join(', '))]);
            var copies = (isShows ? 'show' : 'movie') + ' copies';
            if (lib) removed.push(['video_library', 'Library "' + esc(lib.Name) + '"', 'and the ' + copies + ' in it, made for this list']);
            else removed.push(['folder', 'The ' + copies + ' made for this list', 'its library is already gone']);
            if (settings.RealPosterBadge)
                removed.push(['verified', 'TOP 10 badge on the real posters', 'the titles in your library get their plain poster back']);

            showDeleteDialog({
                title: 'Delete top-list "' + esc(tagName) + '"?',
                subtitle: '',
                removed: removed, kept: [],
                empty: '',
                note: 'This happens right away. Your real movies and shows are not touched.',
                button: 'Delete top-list',
                onDelete: onDelete
            });
        });
    }

    // An empty Custom Title means the group's current name (the sync and Apply fill it in), so a
    // renamed group renames its row. A title equal to the name the group had when the page loaded
    // is that default (earlier versions saved it on every save), so it is stored empty too.
    function clearDefaultCustomName(hseSettings, hseTab) {
        var orig = '';
        try { orig = decodeURIComponent((hseTab && hseTab.dataset.hseOrigName) || ''); } catch (ex) {}
        var cn = (hseSettings.CustomName || '').trim();
        if (!cn || (orig && cn === orig)) delete hseSettings.CustomName;
    }

    function readRowAsConfig(row) {
        var entryLabel = row.querySelector('.txtEntryLabel').value;
        var tagName = row.querySelector('.txtTagName').value || entryLabel;
        var active = row.querySelector('.chkTagActive').checked;
        var blInput = row.querySelector('.txtTagBlacklist');
        var bl = blInput ? blInput.value.split(/[\n\r]+/).map(function(s) { return s.trim(); }).filter(function(s) { return s.length > 0; }) : [];
        var enableTag = row.querySelector('.chkEnableTag').checked;
        var enableColl = row.querySelector('.chkEnableCollection').checked;
        var overrideWhenActive = !!(row.querySelector('.chkOverrideWhenActive') || {}).checked;
        var _plTab = row.querySelector('.playlist-tab');
        var _plUserIds = _plTab && _plTab.dataset.plLoaded === '1'
            ? Array.from(_plTab.querySelectorAll('.chkPlaylistUser:checked')).map(function(c) { return c.value; })
            : (function() { try { return JSON.parse(decodeURIComponent((_plTab && _plTab.dataset.plUserids) || '%5B%5D')); } catch { return []; } })();
        var collName = row.querySelector('.txtCollectionName').value;
        var collSortName = row.querySelector('.txtCollectionSortName') ? row.querySelector('.txtCollectionSortName').value.trim() : '';
        var collDesc = row.querySelector('.txtCollectionDescription') ? row.querySelector('.txtCollectionDescription').value : '';
        var collPoster = (row.querySelector('.collection-tab .hiddenPosterPath') || { value: '' }).value;
        var st = row.querySelector('.selSourceType').value;

        var intervals = readIntervalsFromRow(row);

        var miFilters = [];
        row.querySelectorAll('.mediainfo-filter-group').forEach(function(group, gi) {
            var operator = group.dataset.op || 'AND';
            var groupOp = gi === 0 ? 'AND' : (group.dataset.groupOp || 'AND');
            var criteria = [];
            group.querySelectorAll('.mi-rule').forEach(function(rule) {
                var prop = (rule.querySelector('.selMiProperty') || {}).value || '';
                var selVal = rule.querySelector('.selMiValue');
                var txtVal = rule.querySelector('.txtMiValue');
                var selOp  = rule.querySelector('.selMiOp');
                var txtNum = rule.querySelector('.txtMiNum');
                var selUser = rule.querySelector('.selMiUser');
                var selTextOp = rule.querySelector('.selMiTextOp');
                var val = selVal ? selVal.value : (txtVal ? txtVal.value.replace(/\r?\n/g, '\n').trim() : '');
                // MediaType:Episode + "Include parent series" → save as EpisodeIncludeSeries
                if (prop === 'MediaType' && val === 'Episode') {
                    var incParentChk = rule.querySelector('.chkIncludeParentSeries');
                    if (incParentChk && incParentChk.checked) val = 'EpisodeIncludeSeries';
                }
                var op2 = selOp ? selOp.value : '';
                var textMatchOp = selTextOp ? selTextOp.value : '';
                var num = txtNum ? txtNum.value.trim() : '';
                var userId = selUser ? selUser.value : '';
                var finalOp = op2 || textMatchOp;
                var finalVal = op2 ? num : val;
                var notBtn = rule.querySelector('.btnNotToggle');
                var isNot = notBtn && notBtn.dataset.not === '1';
                var crit = buildCriterion(prop, finalOp, finalVal, userId);
                if (crit) criteria.push(isNot ? '!' + crit : crit);
            });
            if (criteria.length > 0) miFilters.push({ Operator: operator, Criteria: criteria, GroupOperator: groupOp });
        });

        var hseTab = row.querySelector('.homescreen-tab');
        var enableHse = hseTab ? !!(hseTab.querySelector('.chkEnableHomeSection') || {}).checked && hseAllowed(row) : false;
        var hseLibraryId = hseTab && hseTab.dataset.hseLoaded === '1'
            ? ((hseTab.querySelector('.selHseLibrary') || {}).value || 'auto')
            : decodeURIComponent((hseTab && hseTab.dataset.hseLibraryid) || 'auto');
        var hseUserIds = hseTab && hseTab.dataset.hseLoaded === '1'
            ? Array.from(hseTab.querySelectorAll('.chkHseUser:checked')).map(function(c) { return c.value; })
            : (function() { try { return JSON.parse(decodeURIComponent((hseTab && hseTab.dataset.hseUserids) || '%5B%5D')); } catch(ex) { return []; } })();
        var hseSettings = {};
        if (hseTab && hseTab.dataset.hseLoaded === '1') {
            hseTab.querySelectorAll('[data-field]').forEach(function(el) {
                var f = el.dataset.field;
                var v = el.type === 'checkbox' ? String(el.checked) : el.value;
                // If field is empty, fall back to placeholder — not for Custom Title, whose empty value means "the group's name"
                if (v === '' && el.placeholder && f !== 'CustomName') v = el.placeholder;
                hseSettings[f] = v;
            });
            var itemTypesVal = (hseTab.querySelector('.selHseItemTypes') || {}).value || 'Movie,Series';
            hseSettings['ItemTypes'] = JSON.stringify(itemTypesVal.split(','));
            // Compute excluded library IDs from unchecked boxes → stored in _queryExcludeViewIds → Query.ExcludeUserViewIds
            var _excludedLibIds = Array.from(hseTab.querySelectorAll('.chkHseLibrary:not(:checked)')).map(function(c) { return c.value; });
            if (_excludedLibIds.length > 0) hseSettings['_queryExcludeViewIds'] = _excludedLibIds.join(',');
            else delete hseSettings['_queryExcludeViewIds'];
        } else {
            try { hseSettings = JSON.parse(decodeURIComponent((hseTab && hseTab.dataset.hseSettings) || '%7B%7D')); } catch(ex) {}
        }
        clearDefaultCustomName(hseSettings, hseTab);

        var urls = [];
        row.querySelectorAll('.url-row').forEach(function(uRow) {
            urls.push({ url: uRow.querySelector('.txtTagUrl').value.trim(), limit: parseInt(uRow.querySelector('.txtUrlLimit').value, 10) || 0 });
        });
        if (urls.length === 0) urls = [{ url: '', limit: 0 }];

        var localSources = [];
        row.querySelectorAll('.local-row').forEach(function(lRow) {
            localSources.push({ id: lRow.querySelector('.selLocalSource').value, limit: parseInt(lRow.querySelector('.txtLocalLimit').value, 10) || 0 });
        });

        var miLimit = parseInt((row.querySelector('.txtMediaInfoLimit') || {}).value, 10) || 0;

        var aiProvider = (row.querySelector('.selAiProvider') || {}).value || 'OpenAI';
        var aiPrompt = (row.querySelector('.txtAiPrompt') || {}).value || '';
        var aiIncludeRecentlyWatched = !!(row.querySelector('.chkAiRecentlyWatched') || {}).checked;
        var aiRecentlyWatchedUserId = (row.querySelector('.selAiWatchedUser') || {}).value || '';
        var aiRecentlyWatchedCount = parseInt((row.querySelector('.txtAiWatchedCount') || {}).value, 10) || 20;
        var aiRefreshIntervalDays = parseInt((row.querySelector('.txtAiRefreshInterval') || {}).value, 10) || 0;
        var collTag = readOutputTag(row, 'coll'), plTag = readOutputTag(row, 'pl');

        return Object.assign({
            Name: entryLabel, Tag: tagName, ExtraTags: readExtraTags(row), Active: active, Blacklist: bl, ActiveIntervals: intervals,
            EnableTag: enableTag, EnableCollection: enableColl, CollectionName: collName, CollectionSortName: collSortName,
            CollectionDescription: collDesc, CollectionPosterPath: collPoster,
            CollectionPosterStyle: readCollectionArt(row).CollectionPosterStyle,
            CollectionBackgroundStyle: readCollectionArt(row).CollectionBackgroundStyle,
            CollectionBackgroundPath: readCollectionArt(row).CollectionBackgroundPath,
            CollectionKeepOrder: readCollectionArt(row).CollectionKeepOrder, CollectionImported: row.dataset.collImported === '1',
            CollectionPosterReplaced: row.dataset.posterReplaced === '1', CollectionBackgroundReplaced: row.dataset.bgReplaced === '1',
            CollectionArtTitle: readCollectionArt(row).CollectionArtTitle,
            CollectionPosterOptions: readCollectionArt(row).CollectionPosterOptions,
            CollectionBackgroundOptions: readCollectionArt(row).CollectionBackgroundOptions,
            CollectionThumbStyle: readCollectionArt(row).CollectionThumbStyle, CollectionThumbOptions: readCollectionArt(row).CollectionThumbOptions,
            CollectionThumbPath: readCollectionArt(row).CollectionThumbPath,
            CollectionBannerStyle: readCollectionArt(row).CollectionBannerStyle, CollectionBannerPath: readCollectionArt(row).CollectionBannerPath,
            CollectionThumbReplaced: row.dataset.thumbReplaced === '1', CollectionBannerReplaced: row.dataset.bannerReplaced === '1',
            TagPosterStyle: readTagArt(row).TagPosterStyle, TagBackgroundStyle: readTagArt(row).TagBackgroundStyle,
            TagBackgroundPath: readTagArt(row).TagBackgroundPath, TagArtTitle: readTagArt(row).TagArtTitle,
            TagPosterOptions: readTagArt(row).TagPosterOptions, TagBackgroundOptions: readTagArt(row).TagBackgroundOptions,
            OverrideWhenActive: overrideWhenActive, SourceType: st,
            Urls: urls, LocalSources: localSources, Limit: miLimit, ManualItemIds: readManualIds(row),
            MediaInfoFilters: miFilters, MediaInfoConditions: [],
            EnableHomeSection: enableHse, HomeSectionLibraryId: hseLibraryId,
            HomeSectionUserIds: hseUserIds, HomeSectionSettings: JSON.stringify(hseSettings),
            HomeSectionTracked: [], LastModified: new Date().toISOString(),
            AiProvider: aiProvider, AiPrompt: aiPrompt,
            AiIncludeRecentlyWatched: aiIncludeRecentlyWatched,
            AiRecentlyWatchedUserId: aiRecentlyWatchedUserId,
            AiRecentlyWatchedCount: aiRecentlyWatchedCount,
            AiRefreshIntervalDays: aiRefreshIntervalDays,
            TagTargetEpisode:        !!(row.querySelector('.chkTagTargetEpisode')  || {}).checked,
            TagTargetSeason:         !!(row.querySelector('.chkTagTargetSeason')   || {}).checked,
            TagTargetSeries:         !!(row.querySelector('.chkTagTargetSeries')   || {}).checked,
            CollectionTargetEpisode: !!(row.querySelector('.chkCollTargetEpisode') || {}).checked,
            CollectionTargetSeason:  !!(row.querySelector('.chkCollTargetSeason')  || {}).checked,
            CollectionTargetSeries:  !!(row.querySelector('.chkCollTargetSeries')  || {}).checked,
            EnablePlaylist:   !!(row.querySelector('.chkEnablePlaylist') || {}).checked,
            EnableCollectionTag: collTag.enabled, CollectionTag: collTag.tag, CollectionExtraTags: collTag.extras,
            EnablePlaylistTag: plTag.enabled, PlaylistTag: plTag.tag, PlaylistExtraTags: plTag.extras,
            PlaylistName:     (row.querySelector('.txtPlaylistName') || { value: '' }).value,
            PlaylistDescription: readPlaylistLook(row).PlaylistDescription, PlaylistPosterPath: readPlaylistLook(row).PlaylistPosterPath,
            PlaylistPosterStyle: readPlaylistLook(row).PlaylistPosterStyle, PlaylistBackgroundStyle: readPlaylistLook(row).PlaylistBackgroundStyle,
            PlaylistBackgroundPath: readPlaylistLook(row).PlaylistBackgroundPath, PlaylistArtTitle: readPlaylistLook(row).PlaylistArtTitle,
            PlaylistPosterOptions: readPlaylistLook(row).PlaylistPosterOptions, PlaylistBackgroundOptions: readPlaylistLook(row).PlaylistBackgroundOptions,
            PlaylistUserIds:  _plUserIds,
            PlaylistMappings: (function() { try { return JSON.parse(decodeURIComponent((_plTab && _plTab.dataset.plMappings) || '%5B%5D')); } catch { return []; } })(),
            NextWatchUsersMigrated: row.dataset.nwMigrated === '1',
            NextWatchSeedDays: readNwSeedDays(row), NextWatchUseTaste: readNwUseTaste(row),
            MediaInfoTargetEpisode: false, MediaInfoTargetSeason: false, MediaInfoTargetSeries: false,
            MediaInfoTargetType: '', MediaInfoSeasonMode: false,
        }, readTopListTab(row));
    }

    var _formAc = null;

    // Edits made while the config is still loading. loadConfig() rewrites every field, so these
    // are put back afterwards instead of being lost (and Save is rechecked then).
    function editPath(view, el) {
        if (!el || !el.matches || !el.matches('input, select, textarea')) return null;
        if (el.id) return '#' + el.id;
        var row = el.closest('#tagListContainer .tag-row');
        var cls = (el.getAttribute('class') || '').split(/\s+/).filter(Boolean)[0];
        if (!row || !cls) return null;
        var rows = Array.from(view.querySelectorAll('#tagListContainer .tag-row'));
        return 'row:' + rows.indexOf(row) + ':' + cls + ':' + Array.from(row.querySelectorAll('.' + cls)).indexOf(el);
    }

    function findByEditPath(view, path) {
        if (path.charAt(0) === '#') return view.querySelector(path);
        var m = path.split(':');
        var row = view.querySelectorAll('#tagListContainer .tag-row')[parseInt(m[1], 10)];
        return row ? row.querySelectorAll('.' + m[2])[parseInt(m[3], 10)] : null;
    }

    function rememberLoadEdit(view, el) {
        var path = editPath(view, el);
        if (!path) return;
        if (!view._loadEdits) view._loadEdits = {};
        view._loadEdits[path] = (el.type === 'checkbox' || el.type === 'radio') ? { checked: el.checked } : { value: el.value };
    }

    function replayLoadEdits(view) {
        var edits = view._loadEdits;
        view._loadEdits = null;
        if (!edits) return;
        Object.keys(edits).forEach(function (path) {
            var el = findByEditPath(view, path);
            if (!el) return;
            if ('checked' in edits[path]) el.checked = edits[path].checked;
            else el.value = edits[path].value;
            el.dispatchEvent(new Event('input', { bubbles: true }));
            el.dispatchEvent(new Event('change', { bubbles: true }));
        });
    }

    var MI_CRITERION_MAP = {
        '4K': { prop: 'Resolution', val: '4K' }, '8K': { prop: 'Resolution', val: '8K' },
        '1080p': { prop: 'Resolution', val: '1080p' }, '720p': { prop: 'Resolution', val: '720p' },
        'SD': { prop: 'Resolution', val: 'SD' },
        'HEVC': { prop: 'VideoCodec', val: 'HEVC' }, 'AV1': { prop: 'VideoCodec', val: 'AV1' },
        'H264': { prop: 'VideoCodec', val: 'H264' },
        'HDR': { prop: 'HDR', val: 'HDR' }, 'DolbyVision': { prop: 'HDR', val: 'DolbyVision' },
        'HDR10': { prop: 'HDR', val: 'HDR10' },
        'Atmos': { prop: 'AudioFormat', val: 'Atmos' }, 'TrueHD': { prop: 'AudioFormat', val: 'TrueHD' },
        'DtsHdMa': { prop: 'AudioFormat', val: 'DtsHdMa' }, 'DTS': { prop: 'AudioFormat', val: 'DTS' },
        'AC3': { prop: 'AudioFormat', val: 'AC3' }, 'AAC': { prop: 'AudioFormat', val: 'AAC' },
        '7.1': { prop: 'AudioChannels', val: '7.1' }, '5.1': { prop: 'AudioChannels', val: '5.1' },
        'Stereo': { prop: 'AudioChannels', val: 'Stereo' }, 'Mono': { prop: 'AudioChannels', val: 'Mono' }
    };
    var MI_REVERSE_MAP = {};
    Object.keys(MI_CRITERION_MAP).forEach(function (k) {
        var m = MI_CRITERION_MAP[k];
        MI_REVERSE_MAP[m.prop + ':' + m.val] = k;
    });

    // Migrate legacy comma-separated values to newline-separated on first load.
    // Only converts if the string contains commas but no newlines.
    function migrateCommaSeparated(val) {
        if (!val || val.indexOf('\n') >= 0) return val;
        if (val.indexOf(',') < 0) return val;
        return val.split(',').map(function (s) { return s.trim(); }).filter(function (s) { return s.length > 0; }).join('\n');
    }

    function parseCriterion(crit) {
        if (!crit) return { prop: 'Resolution', op: '', val: '', userId: '', not: false };
        var not = crit.charAt(0) === '!';
        if (not) crit = crit.slice(1);
        // Handle Collection/Playlist before split(':') — names may contain colons
        var lcrit = crit.toLowerCase();
        if (lcrit.startsWith('collection:') || lcrit.startsWith('playlist:')) {
            var ci = crit.indexOf(':');
            return { prop: crit.substring(0, ci), op: '', val: crit.substring(ci + 1), userId: '', not: not };
        }
        var parts = crit.split(':');
        if (parts.length === 1) {
            var mapped = MI_CRITERION_MAP[crit];
            return mapped ? { prop: mapped.prop, op: '', val: mapped.val, userId: '', not: not } : { prop: '', op: '', val: crit, userId: '', not: not };
        }
        // Legacy "Tag:X" (no operator) is evaluated server-side as contains. Keep it explicit so
        // the exact default below doesn't silently turn it into exact on the next save.
        if (parts.length === 2) return { prop: parts[0], op: parts[0] === 'Tag' ? 'contains' : '', val: parts[1], userId: '', not: not };
        if (parts.length === 3) return { prop: parts[0], op: parts[1], val: parts[2], userId: '', not: not };
        if (parts.length === 4) return { prop: parts[0], userId: parts[1], op: parts[2], val: parts[3], not: not };
        return { prop: 'Resolution', op: '', val: '', userId: '', not: false };
    }

    function buildCriterion(prop, op, val, userId) {
        if (!prop || val === '') return '';
        if (userId) return prop + ':' + userId + ':' + op + ':' + val;
        if (op) return prop + ':' + op + ':' + val;
        var key = prop + ':' + val;
        return MI_REVERSE_MAP[key] || (prop + ':' + val);
    }

    var MI_DROPDOWN_OPTIONS = {
        Resolution: [['8K', '8K (7680p+)'], ['4K', '4K / UHD'], ['1080p', '1080p / FHD'], ['720p', '720p / HD'], ['SD', 'SD (<720p)']],
        VideoCodec: [['HEVC', 'HEVC / H.265'], ['AV1', 'AV1'], ['H264', 'H.264 / AVC']],
        HDR:        [['HDR', 'HDR (any)'], ['DolbyVision', 'Dolby Vision'], ['HDR10', 'HDR10']],
        AudioFormat: [['Atmos', 'Dolby Atmos'], ['TrueHD', 'Dolby TrueHD'], ['DtsHdMa', 'DTS-HD MA'], ['DTS', 'DTS'], ['AC3', 'Dolby Digital / AC3'], ['AAC', 'AAC']],
        AudioChannels: [['7.1', '7.1+ Surround'], ['5.1', '5.1 Surround'], ['Stereo', 'Stereo'], ['Mono', 'Mono']],
        MediaType: [['Movie', 'Movie'], ['Series', 'Show / Series'], ['Episode', 'Episode'], ['Audio', 'Music Track (Audio)'], ['MusicVideo', 'Music Video'], ['MusicAlbum', 'Music Album'], ['MusicArtist', 'Music Artist']],
        IsPlayed: [['Watched', 'Watched'], ['Unwatched', 'Unwatched']]
    };
    var MI_NUMERIC_PROPS = ['CommunityRating', 'Year', 'Runtime', 'DateAdded', 'DateModified', 'FileSize', 'LastPlayed', 'PlayCount', 'BitRate', 'SampleRate', 'BitsPerSample', 'TrackNumber', 'DiscNumber', 'WatchedByCount', 'Popular'];
    var MI_UNIT_LABELS = { DateAdded: 'days ago', DateModified: 'days ago', LastPlayed: 'days ago', FileSize: 'MB', PlayCount: 'plays', BitRate: 'kbps', SampleRate: 'Hz', BitsPerSample: 'bits', WatchedByCount: 'users', Popular: 'users' };
    var MI_USER_PROPS = ['IsPlayed', 'LastPlayed', 'PlayCount'];
    var MI_TEXT_MATCH_PROPS = ['Tag', 'Title', 'EpisodeTitle', 'Overview', 'Studio', 'Genre', 'Actor', 'Director', 'Writer', 'ContentRating', 'AudioLanguage', 'Artist', 'Album', 'FolderPath', 'Country'];
    var MI_TEXT_MATCH_DEFAULT = {
        Title: 'contains', EpisodeTitle: 'contains', Overview: 'contains', Studio: 'contains', Genre: 'contains', Tag: 'exact',
        Actor: 'exact', Director: 'exact', Writer: 'exact', Artist: 'contains', Album: 'contains',
        ContentRating: 'exact', AudioLanguage: 'exact', FolderPath: 'contains', Country: 'contains'
    };
    var _miUsers = null;
    // Holiday presets on the Schedule tab: each adds one ordinary rule that can be edited afterwards.
    // International only (no single-country holidays).
    var SCHEDULE_PRESETS = [
        { name: 'Christmas',        interval: { Type: 'EveryYear', Start: '2000-12-01', End: '2000-12-26', DayOfWeek: '' } },
        { name: 'New Year',         interval: { Type: 'EveryYear', Start: '2000-12-30', End: '2000-01-01', DayOfWeek: '' } },
        { name: "Valentine's Day",  interval: { Type: 'EveryYear', Start: '2000-02-07', End: '2000-02-14', DayOfWeek: '' } },
        { name: 'Star Wars Day',    interval: { Type: 'EveryYear', Start: '2000-05-04', End: '2000-05-04', DayOfWeek: '' } },
        { name: 'Halloween',        interval: { Type: 'EveryYear', Start: '2000-10-01', End: '2000-10-31', DayOfWeek: '' } },
        { name: 'Easter',           interval: { Type: 'Easter', Occurrence: 'easter', DaysBefore: 7, DaysAfter: 1 } },
        { name: 'Advent',           interval: { Type: 'Advent', DaysBefore: 0, DaysAfter: 0 } },
        { name: 'Friday the 13th',  interval: { Type: 'Friday13', DaysBefore: 0, DaysAfter: 0 } }
    ];

    var MI_PRESETS = [
        { label: 'Resolution', presets: [
            { name: 'Movies in 4K',
              build: function() { return [{ Operator:'AND', GroupOperator:'AND', Criteria:['MediaType:Movie', '4K'] }]; } },
            { name: 'Movies in 1080p',
              build: function() { return [{ Operator:'AND', GroupOperator:'AND', Criteria:['MediaType:Movie', '1080p'] }]; } },
            { name: 'Movies below HD (≤720p)',
              build: function() { return [
                  { Operator:'AND', GroupOperator:'AND', Criteria:['MediaType:Movie'] },
                  { Operator:'OR',  GroupOperator:'AND', Criteria:['720p', 'SD'] }
              ]; } }
        ]},
        { label: 'Release Year', presets: [
            { name: 'Movies from the 1990s',
              build: function() { return [{ Operator:'AND', GroupOperator:'AND', Criteria:['MediaType:Movie','Year:>=:1990','Year:<=:1999'] }]; } },
            { name: 'Movies released this year',
              build: function() { var y = new Date().getFullYear(); return [{ Operator:'AND', GroupOperator:'AND', Criteria:['MediaType:Movie','Year:=:'+y] }]; } },
            { name: 'Movies released in the last 5 years',
              build: function() { var y = new Date().getFullYear()-5; return [{ Operator:'AND', GroupOperator:'AND', Criteria:['MediaType:Movie','Year:>=:'+y] }]; } }
        ]},
        { label: 'Recently', presets: [
            { name: 'Recently added (last 30 days)',
              build: function() { return [{ Operator:'AND', GroupOperator:'AND', Criteria:['DateAdded:<=:30'] }]; } },
            { name: 'Recently modified (last 7 days)',
              build: function() { return [{ Operator:'AND', GroupOperator:'AND', Criteria:['DateModified:<=:7'] }]; } },
            { name: 'Recently played (last 7 days)',
              build: function() { return [{ Operator:'AND', GroupOperator:'AND', Criteria:['LastPlayed:__any__:<=:7'] }]; } }
        ]},
        { label: 'Watch Status', presets: [
            { name: 'Unwatched movies',
              build: function() { return [{ Operator:'AND', GroupOperator:'AND', Criteria:['MediaType:Movie','IsPlayed:__any__:=:Unwatched'] }]; } },
            { name: 'Never played by anyone',
              build: function() { return [{ Operator:'AND', GroupOperator:'AND', Criteria:['IsPlayed:__all__:=:Unwatched'] }]; } },
            { name: 'Watched by at least 2 users',
              build: function() { return [{ Operator:'AND', GroupOperator:'AND', Criteria:['WatchedByCount:>=:2'] }]; } },
            { name: 'Unseen by everyone',
              build: function() { return [{ Operator:'AND', GroupOperator:'AND', Criteria:['WatchedByCount:=:0'] }]; } }
        ]},
        { label: 'Music', presets: [
            { name: 'All music tracks',
              build: function() { return [{ Operator:'AND', GroupOperator:'AND', Criteria:['MediaType:Audio'] }]; } },
            { name: 'All music videos',
              build: function() { return [{ Operator:'AND', GroupOperator:'AND', Criteria:['MediaType:MusicVideo'] }]; } },
            { name: 'Lossless audio (≥24-bit)',
              build: function() { return [{ Operator:'AND', GroupOperator:'AND', Criteria:['MediaType:Audio', 'BitsPerSample:>=:24'] }]; } },
            { name: 'Hi-res audio (≥96 kHz)',
              build: function() { return [{ Operator:'AND', GroupOperator:'AND', Criteria:['MediaType:Audio', 'SampleRate:>=:96000'] }]; } },
            { name: 'Music videos in 4K',
              build: function() { return [{ Operator:'AND', GroupOperator:'AND', Criteria:['MediaType:MusicVideo', '4K'] }]; } }
        ]}
    ];
    var MI_TEXT_PLACEHOLDERS = {
        Title: 'e.g. Batman, Dark Knight', EpisodeTitle: 'e.g. Pilot, Finale', Overview: 'e.g. heist, time travel',
        Studio: 'e.g. Warner, Netflix, HBO', Genre: 'e.g. Action, Thriller',
        Actor: 'e.g. Tom Hanks, Idris Elba', Director: 'e.g. Nolan, Tarantino', Writer: 'e.g. Tarantino, Nolan',
        ContentRating: 'e.g. PG-13, R', AudioLanguage: 'e.g. eng, swe', ImdbId: 'e.g. tt1234567, tt7654321',
        TvdbId: 'e.g. 121361',
        FolderPath: 'e.g. /movies/action',
        Country: 'e.g. United States',
        Artist: 'e.g. Radiohead, Pink Floyd', Album: 'e.g. OK Computer, Dark Side of the Moon'
    };

    function propertyOptionsHtml(selected) {
        var groups = [
            { label: 'Video', props: [['Resolution','Resolution'], ['VideoCodec','Video Codec'], ['HDR','HDR']] },
            { label: 'Audio', props: [['AudioFormat','Audio Format'], ['AudioChannels','Audio Channels'], ['AudioLanguage','Audio Language']] },
            { label: 'Content', props: [['MediaType','Media Type'], ['Tag','Tag'], ['Title','Title'], ['EpisodeTitle','Title (Episode)'], ['Overview','Overview'], ['Studio','Studio'], ['Genre','Genre'], ['Actor','Actor / Cast'], ['Director','Director'], ['Writer','Writer'], ['ContentRating','Content Rating'], ['ImdbId','IMDB ID'], ['TvdbId','TVDB ID'], ['Country','Country'], ['Collection','In Collection'], ['Playlist','In Playlist']] },
            { label: 'Music', props: [['Artist','Artist'], ['Album','Album'], ['BitRate','Bit Rate (kbps)'], ['SampleRate','Sample Rate (Hz)'], ['BitsPerSample','Bit Depth'], ['TrackNumber','Track Number'], ['DiscNumber','Disc Number']] },
            { label: 'Metrics', props: [['CommunityRating','Community Rating'], ['Year','Year'], ['Runtime','Runtime (minutes)'], ['DateAdded','Date Added'], ['DateModified','Date Modified'], ['FileSize','File Size (MB)'], ['FolderPath','Folder Path']] },
            { label: 'Activity', props: [['IsPlayed','Watched / Unwatched'], ['LastPlayed','Last Played'], ['PlayCount','Play Count'], ['WatchedByCount','Watched by (user count)'], ['Popular','Popular on this server (viewers)']] }
        ];
        return groups.map(function (g) {
            return '<optgroup label="' + g.label + '">' +
                g.props.map(function (p) {
                    return '<option value="' + p[0] + '"' + (p[0] === selected ? ' selected' : '') + '>' + p[1] + '</option>';
                }).join('') +
                '</optgroup>';
        }).join('');
    }

    function getMiValueHtml(prop, savedOp, savedVal, savedUserId) {
        var userHtml = '';
        if (MI_USER_PROPS.indexOf(prop) >= 0) {
            var specialOpts =
                '<option value="__any__"' + ('__any__' === savedUserId ? ' selected' : '') + '>Any user</option>' +
                '<option value="__all__"' + ('__all__' === savedUserId ? ' selected' : '') + '>All users</option>';
            var uOpts = specialOpts + (_miUsers || []).map(function (u) {
                return '<option value="' + u.Id + '"' + (u.Id === savedUserId ? ' selected' : '') + '>' + escapeHtml(u.Name + userRatingSuffix(u)) + '</option>';
            }).join('');
            userHtml = '<select class="selMiUser" is="emby-select" style="flex:0 0 auto;min-width:110px;">' + uOpts + '</select>';
        }
        if (prop === 'Popular') {
            // The time window sits where the user picker sits for other Activity rules, so the
            // criterion is stored the same way: Popular:<days>:<op>:<viewers> (0 days = all time).
            var days = savedUserId || '30';
            var dayOpts = [['7', 'Last 7 days'], ['14', 'Last 14 days'], ['30', 'Last 30 days'], ['60', 'Last 60 days'],
                           ['90', 'Last 90 days'], ['180', 'Last 6 months'], ['365', 'Last year'], ['0', 'All time']];
            if (!dayOpts.some(function (d) { return d[0] === days; })) dayOpts.push([days, 'Last ' + days + ' days']);
            userHtml = '<select class="selMiUser" is="emby-select" style="flex:0 0 auto;min-width:120px;">' + dayOpts.map(function (d) {
                return '<option value="' + d[0] + '"' + (d[0] === days ? ' selected' : '') + '>' + d[1] + '</option>';
            }).join('') + '</select>';
        }
        var unitLabel = MI_UNIT_LABELS[prop] ? '<span style="margin-left:4px;opacity:.7;white-space:nowrap;">' + MI_UNIT_LABELS[prop] + '</span>' : '';
        if (prop === 'Collection' || prop === 'Playlist') {
            var cpList = prop === 'Collection' ? cachedCollections : cachedPlaylists;
            var cpOpts = cpList.map(function (o) {
                var n = o.Name || '';
                return '<option value="' + n.replace(/"/g, '&quot;') + '"' + (n === savedVal ? ' selected' : '') + '>' + n + '</option>';
            }).join('');
            return '<select class="selMiValue" is="emby-select" style="flex:1;"><option value="">-- Select --</option>' + cpOpts + '</select>';
        }
        if (prop === 'Tag') {
            var tagTextOp = savedOp || MI_TEXT_MATCH_DEFAULT['Tag'];
            var tagTextOpHtml = '<select class="selMiTextOp" is="emby-select" style="flex:0 0 100px;">' +
                '<option value="contains"' + (tagTextOp === 'contains' ? ' selected' : '') + '>Contains</option>' +
                '<option value="exact"' + (tagTextOp === 'exact' ? ' selected' : '') + '>Exact</option>' +
                '</select>';
            if (cachedTags.length > 0) {
                var tagOpts = cachedTags.map(function (t) {
                    return '<option value="' + t.replace(/"/g, '&quot;') + '"' + (t === savedVal ? ' selected' : '') + '>' + t + '</option>';
                }).join('');
                return tagTextOpHtml + '<select class="selMiValue" is="emby-select" style="flex:1;"><option value="">-- Select tag --</option>' + tagOpts + '</select>';
            }
            var migratedTagVal = migrateCommaSeparated(savedVal || '');
            return tagTextOpHtml + '<textarea class="txtMiValue" placeholder="e.g. 4K" rows="1" style="flex:1;resize:none;overflow:hidden;padding:6px 8px;font-size:inherit;font-family:inherit;background:var(--plugin-input-bg,rgba(255,255,255,0.08));border:1px solid var(--plugin-input-border,rgba(255,255,255,0.2));border-radius:3px;color:inherit;line-height:1.4;min-height:32px;max-height:120px;overflow-y:auto;">' + migratedTagVal.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;') + '</textarea>';
        }
        if (MI_DROPDOWN_OPTIONS[prop]) {
            if (prop === 'MediaType') {
                var dispVal = (savedVal === 'EpisodeIncludeSeries') ? 'Episode' : (savedVal || '');
                var mtOpts = MI_DROPDOWN_OPTIONS['MediaType'].map(function (pair) {
                    return '<option value="' + pair[0] + '"' + (pair[0] === dispVal ? ' selected' : '') + '>' + pair[1] + '</option>';
                }).join('');
                var ipChecked = (savedVal === 'EpisodeIncludeSeries') ? 'checked' : '';
                var ipDisplay = (dispVal === 'Episode') ? 'inline-flex' : 'none';
                return '<select class="selMiValue" is="emby-select" style="flex:1;">' + mtOpts + '</select>' +
                    '<label class="mi-include-parent" style="display:' + ipDisplay + '; align-items:center; gap:6px; cursor:pointer; white-space:nowrap; font-size:0.85em; margin:0;">' +
                    '<input type="checkbox" class="chkIncludeParentSeries" ' + ipChecked + ' style="margin:0;">' +
                    '<span>Also include parent series</span>' +
                    '</label>';
            }
            var opts = MI_DROPDOWN_OPTIONS[prop].map(function (pair) {
                return '<option value="' + pair[0] + '"' + (pair[0] === savedVal ? ' selected' : '') + '>' + pair[1] + '</option>';
            }).join('');
            return userHtml + '<select class="selMiValue" is="emby-select" style="flex:1;">' + opts + '</select>';
        }
        if (MI_NUMERIC_PROPS.indexOf(prop) >= 0) {
            var ops = ['=', '>', '>=', '<', '<='];
            var defaultOp = (prop === 'PlayCount' || prop === 'Popular') ? '>=' : '<=';
            var opOpts = ops.map(function (o) {
                return '<option value="' + o + '"' + (o === (savedOp || defaultOp) ? ' selected' : '') + '>' + o + '</option>';
            }).join('');
            var infoTooltip =
                '<div class="mi-op-info">' +
                '<div class="mi-op-info-icon">i</div>' +
                '<div class="mi-op-tooltip"><table>' +
                '<tr><td>=</td><td>Exactly equal</td></tr>' +
                '<tr><td>&gt;</td><td>Greater than</td></tr>' +
                '<tr><td>&gt;=</td><td>Greater than or equal</td></tr>' +
                '<tr><td>&lt;</td><td>Less than</td></tr>' +
                '<tr><td>&lt;=</td><td>Less than or equal</td></tr>' +
                '</table></div></div>';
            var numStep = (prop === 'PlayCount' || prop === 'Popular') ? '1' : '0.01';
            return userHtml +
                '<select class="selMiOp" is="emby-select" style="flex:0 0 64px;">' + opOpts + '</select>' +
                infoTooltip +
                '<input class="txtMiNum" is="emby-input" type="number" step="' + numStep + '" value="' + (savedVal || (prop === 'Popular' ? '3' : '')) + '" style="flex:1;" />' +
                unitLabel;
        }
        if (MI_TEXT_MATCH_PROPS.indexOf(prop) >= 0) {
            var textOp = savedOp || MI_TEXT_MATCH_DEFAULT[prop] || 'contains';
            var textOpHtml = '<select class="selMiTextOp" is="emby-select" style="flex:0 0 100px;">' +
                '<option value="contains"' + (textOp === 'contains' ? ' selected' : '') + '>Contains</option>' +
                '<option value="exact"' + (textOp === 'exact' ? ' selected' : '') + '>Exact</option>' +
                '</select>';
            var ph = MI_TEXT_PLACEHOLDERS[prop] || '';
            var ph = MI_TEXT_PLACEHOLDERS[prop] || '';
            var migratedVal = migrateCommaSeparated(savedVal || '');
            return textOpHtml + '<textarea class="txtMiValue" placeholder="' + ph + '" rows="1" style="flex:1;resize:none;overflow:hidden;padding:6px 8px;font-size:inherit;font-family:inherit;background:var(--plugin-input-bg,rgba(255,255,255,0.08));border:1px solid var(--plugin-input-border,rgba(255,255,255,0.2));border-radius:3px;color:inherit;line-height:1.4;min-height:32px;max-height:120px;overflow-y:auto;">' + migratedVal.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;') + '</textarea>';
        }
        var ph = MI_TEXT_PLACEHOLDERS[prop] || '';
        var migratedVal = migrateCommaSeparated(savedVal || '');
        return '<textarea class="txtMiValue" placeholder="' + ph + '" rows="1" style="flex:1;resize:none;overflow:hidden;padding:6px 8px;font-size:inherit;font-family:inherit;background:var(--plugin-input-bg,rgba(255,255,255,0.08));border:1px solid var(--plugin-input-border,rgba(255,255,255,0.2));border-radius:3px;color:inherit;line-height:1.4;min-height:32px;max-height:120px;overflow-y:auto;">' + migratedVal.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;') + '</textarea>';
    }

    function getMiHintHtml(prop) {
        if (prop === 'Popular') return '<div class="mi-rule-hint" style="font-size:0.75em; opacity:0.5; margin-top:2px; padding-right:32px; text-align:right;">Different users who watched it in that time &mdash; anyone who watched an episode counts for the show. Matches are listed most viewed first, so Max items keeps the most popular.</div>';
        if (MI_TEXT_MATCH_PROPS.indexOf(prop) < 0 && prop !== 'ImdbId' && prop !== 'TvdbId') return '<div class="mi-rule-hint"></div>';
        return '<div class="mi-rule-hint" style="font-size:0.75em; opacity:0.5; margin-top:2px; padding-right:32px; text-align:right;">One value per line &mdash; matches if <em>any</em> line matches (OR)</div>';
    }

    function getMediaInfoRuleHtml(criterion) {
        var parsed = parseCriterion(criterion || '');
        var prop = parsed.prop || 'Resolution';
        var notActive = parsed.not;
        var notBg = notActive ? 'rgba(229,115,115,0.15)' : 'transparent';
        var notColor = notActive ? 'var(--hsc-danger-text)' : 'var(--theme-text-secondary)';
        var notBorder = notActive ? '1px solid rgba(229,115,115,0.5)' : '1px solid var(--hsc-hairline-strong)';
        return '<div class="mi-rule" style="margin-bottom:6px;">' +
            '<div style="display:flex; gap:6px; align-items:center;">' +
            '<button type="button" class="btnNotToggle" data-not="' + (notActive ? '1' : '0') + '"' +
            ' style="border:' + notBorder + '; border-radius:6px; padding:3px 10px; font-family:inherit; font-size:0.78em; font-weight:600; cursor:pointer; letter-spacing:0.3px; flex-shrink:0;' +
            ' background:' + notBg + '; color:' + notColor + ';" title="Negate this rule">NOT</button>' +
            '<select class="selMiProperty" is="emby-select" style="flex:0 0 155px;">' + propertyOptionsHtml(prop) + '</select>' +
            '<div class="mi-value-wrapper" style="flex:1; display:flex; gap:6px; align-items:center;">' + getMiValueHtml(prop, parsed.op, parsed.val, parsed.userId || '') + '</div>' +
            '<button type="button" class="btnRemoveMiRule" style="background:transparent; border:none; border-radius:6px; color:var(--hsc-danger-text); cursor:pointer; padding:2px 8px; font-size:1em; flex-shrink:0;" title="Remove rule">✕</button>' +
            '</div>' +
            getMiHintHtml(prop) +
            '</div>';
    }

    function getMediaInfoFilterGroupHtml(filter, _i, isFirst) {
        var op = (filter && filter.Operator) || 'AND';
        var groupOp = (filter && filter.GroupOperator) || 'AND';
        var criteria = (filter && filter.Criteria) || [];

        var connectorHtml = isFirst ? '' :
            '<div class="mi-group-connector" style="display:flex; align-items:center; gap:10px; margin:-12px -12px 14px; padding:8px 14px; background:rgba(0,0,0,0.12);">' +
                '<div style="flex:1; height:1px; background:rgba(128,128,128,0.25);"></div>' +
                '<div style="display:flex; flex-direction:column; align-items:center; gap:4px;">' +
                    '<span class="hsc-eyebrow">Connect groups with</span>' +
                    '<div style="display:flex; border-radius:8px; overflow:hidden; border:1px solid var(--hsc-hairline-strong); background:var(--hsc-tint-1); padding:2px; gap:2px;">' +
                        '<button type="button" class="btnGroupOpChoice" data-value="AND"' +
                        ' style="border:none; border-radius:6px; padding:4px 14px; font-family:inherit; font-size:0.82em; font-weight:600; cursor:pointer; letter-spacing:0.3px;' +
                        ' background:' + (groupOp === 'AND' ? 'var(--theme-background-level3)' : 'transparent') + ';' +
                        ' color:' + (groupOp === 'AND' ? 'var(--theme-text-primary)' : 'var(--theme-text-secondary)') + ';"' +
                        ' title="Both filter groups must match">AND</button>' +
                        '<button type="button" class="btnGroupOpChoice" data-value="OR"' +
                        ' style="border:none; border-radius:6px; padding:4px 14px; font-family:inherit; font-size:0.82em; font-weight:600; cursor:pointer; letter-spacing:0.3px;' +
                        ' background:' + (groupOp === 'OR' ? 'var(--theme-background-level3)' : 'transparent') + ';' +
                        ' color:' + (groupOp === 'OR' ? 'var(--theme-text-primary)' : 'var(--theme-text-secondary)') + ';"' +
                        ' title="Either filter group is enough">OR</button>' +
                    '</div>' +
                    '<span class="group-op-desc" style="font-size:0.7em; opacity:0.55; white-space:nowrap;">' +
                        (groupOp === 'AND' ? 'Both groups must match' : 'Either group is enough') +
                    '</span>' +
                '</div>' +
                '<div style="flex:1; height:1px; background:rgba(128,128,128,0.25);"></div>' +
            '</div>';

        var rulesHtml = criteria.map(function (c) { return getMediaInfoRuleHtml(c); }).join('');

        return '<div class="mediainfo-filter-group" data-group-op="' + groupOp + '" data-op="' + op + '" style="border:1px solid rgba(128,128,128,0.3); border-radius:6px; padding:12px; margin-bottom:10px; background:rgba(128,128,128,0.03);">' +
            connectorHtml +
            '<div style="display:flex; align-items:center; justify-content:space-between; margin-bottom:10px;">' +
                '<div style="display:flex; align-items:center; gap:10px; flex-wrap:wrap;">' +
                    '<span class="hsc-label">Match rules:</span>' +
                    '<div style="display:flex; flex-direction:column; gap:3px;">' +
                        '<div style="display:flex; border-radius:8px; overflow:hidden; border:1px solid var(--hsc-hairline-strong); background:var(--hsc-tint-1); padding:2px; gap:2px;">' +
                            '<button type="button" class="btnGroupInnerOpChoice" data-value="AND"' +
                            ' style="border:none; border-radius:6px; padding:4px 14px; font-family:inherit; font-size:0.82em; font-weight:600; cursor:pointer; letter-spacing:0.3px;' +
                            ' background:' + (op === 'AND' ? 'var(--theme-background-level3)' : 'transparent') + ';' +
                            ' color:' + (op === 'AND' ? 'var(--theme-text-primary)' : 'var(--theme-text-secondary)') + ';"' +
                            ' title="All rules in this group must match">ALL</button>' +
                            '<button type="button" class="btnGroupInnerOpChoice" data-value="OR"' +
                            ' style="border:none; border-radius:6px; padding:4px 14px; font-family:inherit; font-size:0.82em; font-weight:600; cursor:pointer; letter-spacing:0.3px;' +
                            ' background:' + (op === 'OR' ? 'var(--theme-background-level3)' : 'transparent') + ';' +
                            ' color:' + (op === 'OR' ? 'var(--theme-text-primary)' : 'var(--theme-text-secondary)') + ';"' +
                            ' title="Any rule in this group is enough">ANY</button>' +
                        '</div>' +
                        '<span class="inner-op-desc" style="font-size:0.7em; opacity:0.55;">' +
                            (op === 'AND' ? 'All rules must match' : 'Any rule is enough') +
                        '</span>' +
                    '</div>' +
                '</div>' +
                '<button type="button" class="btnRemoveFilterGroup" style="background:transparent; border:none; color:var(--hsc-danger-text); cursor:pointer; padding:2px 8px; font-size:0.85em; flex-shrink:0;">✕ Remove</button>' +
            '</div>' +
            '<div class="mi-rules-list">' + rulesHtml + '</div>' +
            '<button type="button" is="emby-button" class="btnAddMiRule raised btn-neutral" style="margin-top: 4px;">+ Add Rule</button>' +
            '</div>';
    }

    function escapeHtml(str) {
        return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    }

    function readMiFiltersFromContainer(container) {
        var miFilters = [];
        container.querySelectorAll('.mediainfo-filter-group').forEach(function (group, gi) {
            var operator = group.dataset.op || 'AND';
            var groupOp = gi === 0 ? 'AND' : (group.dataset.groupOp || 'AND');
            var criteria = [];
            group.querySelectorAll('.mi-rule').forEach(function (rule) {
                var prop = (rule.querySelector('.selMiProperty') || {}).value || '';
                var selVal = rule.querySelector('.selMiValue');
                var txtVal = rule.querySelector('.txtMiValue');
                var selOp  = rule.querySelector('.selMiOp');
                var txtNum = rule.querySelector('.txtMiNum');
                var selUser = rule.querySelector('.selMiUser');
                var selTextOp = rule.querySelector('.selMiTextOp');
                var val = selVal ? selVal.value : (txtVal ? txtVal.value.replace(/\r?\n/g, '\n').trim() : '');
                if (prop === 'MediaType' && val === 'Episode') {
                    var incParentChk = rule.querySelector('.chkIncludeParentSeries');
                    if (incParentChk && incParentChk.checked) val = 'EpisodeIncludeSeries';
                }
                var op2 = selOp ? selOp.value : '';
                var textMatchOp = selTextOp ? selTextOp.value : '';
                var num = txtNum ? txtNum.value.trim() : '';
                var userId = selUser ? selUser.value : '';
                var finalOp = op2 || textMatchOp;
                var finalVal = op2 ? num : val;
                var notBtn = rule.querySelector('.btnNotToggle');
                var isNot = notBtn && notBtn.dataset.not === '1';
                var crit = buildCriterion(prop, finalOp, finalVal, userId);
                if (crit) criteria.push(isNot ? '!' + crit : crit);
            });
            if (criteria.length > 0) miFilters.push({ Operator: operator, Criteria: criteria, GroupOperator: groupOp });
        });
        return miFilters;
    }

    function getMySavedFiltersPanelHtml() {
        if (savedFilters.length === 0) {
            return '<div style="font-size:0.82em; color:var(--theme-text-secondary); font-style:italic; margin-bottom:4px;">No saved filters yet.</div>';
        }
        return '<div style="display:flex; flex-wrap:wrap; gap:6px;">' +
            savedFilters.map(function (sf, i) {
                return '<div style="display:flex; align-items:center; gap:0;">' +
                    '<button type="button" class="btnApplyMySavedFilter" data-index="' + i + '"' +
                    ' style="border:1px solid var(--hsc-hairline-strong); border-radius:9999px 0 0 9999px; padding:4px 10px; font-family:inherit; font-size:0.82em; cursor:pointer; background:transparent; color:var(--theme-text-primary);">' +
                    escapeHtml(sf.Name) + '</button>' +
                    '<button type="button" class="btnDeleteMySavedFilter" data-index="' + i + '"' +
                    ' style="border:1px solid var(--hsc-hairline-strong); border-left:none; border-radius:0 9999px 9999px 0; background:transparent; color:var(--hsc-danger-text); cursor:pointer; padding:4px 8px; font-size:0.82em; line-height:1;" title="Delete">✕</button>' +
                    '</div>';
            }).join('') +
            '</div>';
    }

    function refreshMySavedFiltersPanels() {
        var v = activeView();
        if (!v) return;
        var html = getMySavedFiltersPanelHtml();
        v.querySelectorAll('.mi-saved-panel-content').forEach(function (el) {
            el.innerHTML = html;
        });
    }

    function saveSavedFiltersNow() {
        window.ApiClient.getPluginConfiguration(pluginId)
            .then(function (currentConfig) {
                var before = hscServerStamp(currentConfig);
                currentConfig.SavedFilters = savedFilters;
                return window.ApiClient.updatePluginConfiguration(pluginId, currentConfig)
                    .then(function (r) { return advanceServerStamp(before).then(function () { return r; }); });
            })
            .then(function () {
                if (originalConfigState) {
                    try {
                        var state = JSON.parse(originalConfigState);
                        state.SavedFilters = savedFilters;
                        originalConfigState = JSON.stringify(state);
                    } catch (e) {}
                }
                var view = activeView();
                if (view) checkFormState();
            });
    }

    // Same as the server's MovingDateAnchor/MovingDateMatches: the anchor date (Nth/last weekday of
    // a month, or Western Easter Sunday) of last, this and next year, widened by DaysBefore/DaysAfter.
    function westernEaster(y) {
        var a = y % 19, b = Math.floor(y / 100), c = y % 100, d = Math.floor(b / 4), e = b % 4;
        var f = Math.floor((b + 8) / 25), g = Math.floor((b - f + 1) / 3), h = (19 * a + b - d - g + 15) % 30;
        var i = Math.floor(c / 4), k = c % 4, l = (32 + 2 * e + 2 * i - h - k) % 7, m = Math.floor((a + 11 * h + 22 * l) / 451);
        var n = h + l - 7 * m + 114;
        return new Date(y, Math.floor(n / 31) - 1, n % 31 + 1);
    }

    function movingDateAnchor(iv, y) {
        var occ = String(iv.Occurrence || '1').trim().toLowerCase();
        if (iv.Type === 'Easter' || occ === 'easter') return westernEaster(y);
        var month = +iv.Month;
        if (!(month >= 1 && month <= 12)) return null;
        var wd = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'].indexOf((iv.DayOfWeek || '').split(',')[0].trim());
        if (wd < 0) return null;
        if (occ === 'last') {
            var last = new Date(y, month, 0);
            return new Date(y, month - 1, last.getDate() - (last.getDay() - wd + 7) % 7);
        }
        var nth = parseInt(occ, 10);
        if (!(nth >= 1 && nth <= 4)) return null;
        var first = new Date(y, month - 1, 1);
        return new Date(y, month - 1, 1 + (wd - first.getDay() + 7) % 7 + 7 * (nth - 1));
    }

    function movingDateActive(iv, now) {
        var before = Math.max(0, Math.min(60, +iv.DaysBefore || 0)), after = Math.max(0, Math.min(60, +iv.DaysAfter || 0));
        var today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
        for (var y = now.getFullYear() - 1; y <= now.getFullYear() + 1; y++) {
            var a = movingDateAnchor(iv, y);
            if (!a) return false;
            var s = new Date(a.getFullYear(), a.getMonth(), a.getDate() - before);
            var e = new Date(a.getFullYear(), a.getMonth(), a.getDate() + after);
            if (today >= s && today <= e) return true;
        }
        return false;
    }

    function isScheduleCurrentlyActive(intervals) {
        if (!intervals || intervals.length === 0) return false;
        var now = new Date();
        var dowNames = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
        var todayName = dowNames[now.getDay()];
        return intervals.some(function(iv) {
            if (iv.Type === 'TimeOfDay') {
                var fp = (iv.FromTime || '').split(':'), tp = (iv.ToTime || '').split(':');
                if (fp.length < 2 || tp.length < 2) return false;
                var from = +fp[0] * 60 + +fp[1], to = +tp[0] * 60 + +tp[1], t = now.getHours() * 60 + now.getMinutes();
                var dayOk = function (d) { return !iv.DayOfWeek || iv.DayOfWeek.split(',').map(function (x) { return x.trim(); }).indexOf(dowNames[d]) >= 0; };
                if (from < to) return dayOk(now.getDay()) && t >= from && t < to;
                if (from === to) return dayOk(now.getDay());
                return (t >= from && dayOk(now.getDay())) || (t < to && dayOk((now.getDay() + 6) % 7));
            }
            if (iv.Type === 'Weekly') {
                var days = (iv.DayOfWeek || '').split(',').map(function(d) { return d.trim(); });
                return days.indexOf(todayName) >= 0;
            }
            if (iv.Type === 'MovingDate' || iv.Type === 'Easter') return movingDateActive(iv, now);
            if (iv.Type === 'Advent') return adventActive(iv, now);
            if (iv.Type === 'Friday13') return friday13Active(iv, now);
            if (!iv.Start || !iv.End) return false;
            var s = new Date(iv.Start);
            var e = new Date(iv.End);
            if (iv.Type === 'EveryYear') {
                // Same as the server: month/day as stored (day clamped to this year's month length,
                // e.g. Feb 29 → 28); a window that wraps the new year (Dec 27 → Jan 3) is active from
                // its start through Dec 31 and again from Jan 1 through its end.
                var sp = parseDateYMD(iv.Start), ep = parseDateYMD(iv.End);
                if (!sp || !ep) return false;
                var dim = function (m) { return new Date(now.getFullYear(), m, 0).getDate(); };
                var nowMD = (now.getMonth() + 1) * 100 + now.getDate();
                var sMD = sp.month * 100 + Math.min(sp.day, dim(sp.month));
                var eMD = ep.month * 100 + Math.min(ep.day, dim(ep.month));
                return eMD >= sMD ? (sMD <= nowMD && nowMD <= eMD) : (nowMD >= sMD || nowMD <= eMD);
            }
            // SpecificDate
            e.setHours(23, 59, 59, 999);
            return s <= now && now <= e;
        });
    }

    function firstAdvent(y) {
        var christmas = new Date(y, 11, 25);
        var back = christmas.getDay() === 0 ? 7 : christmas.getDay();
        return new Date(y, 11, 25 - back - 21);
    }

    function adventActive(iv, now) {
        var before = Math.max(0, Math.min(60, +iv.DaysBefore || 0)), after = Math.max(0, Math.min(60, +iv.DaysAfter || 0));
        var today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
        for (var y = now.getFullYear() - 1; y <= now.getFullYear(); y++) {
            var a = firstAdvent(y);
            var s = new Date(a.getFullYear(), a.getMonth(), a.getDate() - before);
            var e = new Date(y, 11, 25 + after);
            if (today >= s && today <= e) return true;
        }
        return false;
    }

    function friday13Active(iv, now) {
        var before = Math.max(0, Math.min(60, +iv.DaysBefore || 0)), after = Math.max(0, Math.min(60, +iv.DaysAfter || 0));
        for (var i = -after; i <= before; i++) {
            var d = new Date(now.getFullYear(), now.getMonth(), now.getDate() + i);
            if (d.getDate() === 13 && d.getDay() === 5) return true;
        }
        return false;
    }

    function readIntervalsFromRow(row) {
        var intervals = [];
        row.querySelectorAll('.date-row').forEach(function(dr) {
            var type = (dr.querySelector('.selDateType') || {}).value || 'SpecificDate';
            var s = null, e = null, days = '';
            if (type === 'SpecificDate') {
                s = (dr.querySelector('.txtFullStartDate') || {}).value || null;
                e = (dr.querySelector('.txtFullEndDate') || {}).value || null;
            } else if (type === 'EveryYear') {
                var sM = (dr.querySelector('.selStartMonth') || {}).value || '1';
                var sD = (dr.querySelector('.selStartDay') || {}).value || '1';
                var eM = (dr.querySelector('.selEndMonth') || {}).value || '1';
                var eD = (dr.querySelector('.selEndDay') || {}).value || '1';
                s = '2000-' + sM.padStart(2, '0') + '-' + sD.padStart(2, '0');
                e = '2000-' + eM.padStart(2, '0') + '-' + eD.padStart(2, '0');
            } else if (type === 'Weekly' || type === 'TimeOfDay') {
                var daysBox = dr.querySelector(type === 'Weekly' ? '.inputs-weekly' : '.inputs-time');
                days = Array.from(daysBox.querySelectorAll('.day-toggle.active')).map(function(b) { return b.dataset.day; }).join(',');
            } else if (type === 'MovingDate') {
                days = (dr.querySelector('.selMovWeekday') || {}).value || 'Monday';
            }
            var iv = { Type: type, Start: s, End: e, DayOfWeek: days };
            if (isMovingType(type)) {
                var clampDays = function (sel) { var n = parseInt((dr.querySelector(sel) || {}).value, 10); return isNaN(n) ? 0 : Math.max(0, Math.min(60, n)); };
                iv.Occurrence = type === 'Easter' ? 'easter' : type === 'MovingDate' ? ((dr.querySelector('.selMovOccurrence') || {}).value || '1') : '1';
                iv.Month = type === 'MovingDate' ? (parseInt((dr.querySelector('.selMovMonth') || {}).value, 10) || 1) : 1;
                iv.DaysBefore = clampDays('.txtMovBefore');
                iv.DaysAfter = clampDays('.txtMovAfter');
            }
            if (type === 'TimeOfDay') {
                iv.FromTime = (dr.querySelector('.txtFromTime') || {}).value || '';
                iv.ToTime = (dr.querySelector('.txtToTime') || {}).value || '';
            }
            intervals.push(iv);
        });
        return intervals;
    }

    // ── Manual List source: search the library, add, reorder and remove movies and shows ──
    function readManualIds(row) {
        var box = row.querySelector('.source-manual-container');
        try { return JSON.parse(decodeURIComponent((box && box.dataset.manualIds) || '%5B%5D')); } catch (ex) { return []; }
    }

    function initManualEditor(row) {
        var box = row.querySelector('.source-manual-container');
        if (!box) return;
        var input = box.querySelector('.txtManualSearch');
        var results = box.querySelector('.manualSearchResults');
        var listEl = box.querySelector('.manualItemList');
        var items = readManualIds(row).map(function (id) { return { Id: id, Name: '', Year: null, Type: '' }; });
        var searchTimer = null, searchSeq = 0;

        function esc(s) { return String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;'); }
        function kind(t) { return t === 'Series' ? 'Show' : (t === 'Movie' ? 'Movie' : ''); }
        function save() {
            box.dataset.manualIds = encodeURIComponent(JSON.stringify(items.map(function (i) { return i.Id; })));
            if (typeof checkFormState === 'function') setTimeout(checkFormState, 0);
        }
        function render() {
            if (items.length === 0) { listEl.innerHTML = '<div style="padding:8px 4px;opacity:0.5;font-size:0.9em;">Nothing added yet.</div>'; return; }
            listEl.innerHTML = items.map(function (m, idx) {
                var label = m.Name ? esc(m.Name) + (m.Year ? ' (' + m.Year + ')' : '') : '<span style="opacity:0.5;">Not in the library any more</span>';
                return '<div style="display:flex;align-items:center;gap:5px;padding:5px 2px;border-bottom:1px solid rgba(128,128,128,0.15);">' +
                    '<span style="min-width:22px;font-size:0.8em;opacity:0.55;font-weight:600;text-align:right;">' + (idx + 1) + '.</span>' +
                    '<span style="flex:1;font-size:0.9em;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">' + label + '</span>' +
                    (kind(m.Type) ? '<span style="font-size:0.75em;opacity:0.6;">' + kind(m.Type) + '</span>' : '') +
                    '<button type="button" class="btnManualUp" data-idx="' + idx + '"' + (idx === 0 ? ' disabled' : '') + ' style="cursor:pointer;border:none;background:transparent;color:inherit;padding:2px 4px;opacity:0.7;" title="Move up">▲</button>' +
                    '<button type="button" class="btnManualDown" data-idx="' + idx + '"' + (idx === items.length - 1 ? ' disabled' : '') + ' style="cursor:pointer;border:none;background:transparent;color:inherit;padding:2px 4px;opacity:0.7;" title="Move down">▼</button>' +
                    '<button type="button" class="btnManualRemove" data-idx="' + idx + '" style="cursor:pointer;border:none;background:transparent;color:var(--hsc-danger-text);padding:2px 4px;" title="Remove">✕</button>' +
                    '</div>';
            }).join('');
        }

        render();
        if (items.length > 0) {
            window.ApiClient.getJSON(window.ApiClient.getUrl('Items', {
                Ids: items.map(function (i) { return i.Id; }).join(','), Fields: 'ProductionYear'
            })).then(function (res) {
                var byId = {};
                (res.Items || []).forEach(function (i) { byId[i.Id] = i; });
                items.forEach(function (m) { var i = byId[m.Id]; if (i) { m.Name = i.Name || ''; m.Year = i.ProductionYear || null; m.Type = i.Type || ''; } });
                render();
            }).catch(function () {});
        }

        function search(q) {
            var seq = ++searchSeq;
            if (q.length < 2) { results.style.display = 'none'; results.innerHTML = ''; return; }
            window.ApiClient.getJSON(window.ApiClient.getUrl('Items', {
                SearchTerm: q, IncludeItemTypes: 'Movie,Series', Recursive: true, Limit: 20, Fields: 'ProductionYear'
            })).then(function (res) {
                if (seq !== searchSeq) return;
                var hits = res.Items || [];
                if (hits.length === 0) { results.innerHTML = '<div style="padding:7px 12px;opacity:0.6;font-size:0.9em;">No matches</div>'; results.style.display = 'block'; return; }
                var have = {};
                items.forEach(function (m) { have[m.Id] = true; });
                results.innerHTML = hits.map(function (i) {
                    var added = have[i.Id];
                    return '<div class="manualSearchHit" data-id="' + esc(i.Id) + '" data-name="' + esc(i.Name) + '" data-year="' + esc(i.ProductionYear || '') + '" data-type="' + esc(i.Type) + '"' +
                        ' style="display:flex;gap:8px;padding:7px 12px;cursor:pointer;font-size:0.9em;border-bottom:1px solid rgba(128,128,128,0.12);' + (added ? 'opacity:0.42;pointer-events:none;' : '') + '">' +
                        '<span style="flex:1;">' + esc(i.Name) + (i.ProductionYear ? ' (' + i.ProductionYear + ')' : '') + (added ? ' <span style="font-size:0.8em;">(already added)</span>' : '') + '</span>' +
                        '<span style="font-size:0.8em;opacity:0.6;">' + kind(i.Type) + '</span></div>';
                }).join('');
                results.style.display = 'block';
            }).catch(function () {});
        }

        input.addEventListener('input', function () {
            var q = this.value.trim();
            clearTimeout(searchTimer);
            searchTimer = setTimeout(function () { search(q); }, 250);
        });
        input.addEventListener('blur', function () { setTimeout(function () { results.style.display = 'none'; }, 150); });
        // Enter would submit the settings form.
        input.addEventListener('keydown', function (e) { if (e.key === 'Enter') e.preventDefault(); });
        results.addEventListener('mousedown', function (e) {
            var hit = e.target.closest('.manualSearchHit');
            if (!hit) return;
            e.preventDefault();
            if (items.some(function (m) { return m.Id === hit.dataset.id; })) return;
            items.push({ Id: hit.dataset.id, Name: hit.dataset.name, Year: hit.dataset.year ? parseInt(hit.dataset.year, 10) : null, Type: hit.dataset.type });
            input.value = '';
            results.style.display = 'none';
            render();
            save();
        });
        listEl.addEventListener('click', function (e) {
            var btn = e.target.closest('button');
            if (!btn) return;
            e.preventDefault();
            e.stopPropagation();
            var idx = parseInt(btn.dataset.idx, 10), tmp;
            if (btn.classList.contains('btnManualUp') && idx > 0) { tmp = items[idx - 1]; items[idx - 1] = items[idx]; items[idx] = tmp; }
            else if (btn.classList.contains('btnManualDown') && idx < items.length - 1) { tmp = items[idx + 1]; items[idx + 1] = items[idx]; items[idx] = tmp; }
            else if (btn.classList.contains('btnManualRemove')) items.splice(idx, 1);
            render();
            save();
        });
    }

    // Your Next Watch settings of a source card (defaults as on the server: 90 days, taste on).
    function readNwSeedDays(row) {
        var n = parseInt((row.querySelector('.txtNwSeedDays') || {}).value, 10);
        return n > 0 ? Math.max(7, Math.min(730, n)) : 90;
    }
    function readNwUseTaste(row) {
        var c = row.querySelector('.chkNwUseTaste');
        return c ? c.checked : true;
    }

    // Must match HomeScreenCompanionTask.DefaultNextWatchDescription (used when the description is empty).
    var NEXT_WATCH_DEFAULT_DESCRIPTION = 'Picked just for you from what you\'ve been watching — fresh recommendations every day.';
    var NEXT_WATCH_HINT = 'Each user ticked on the Playlist tab gets their own playlist with their own picks: titles like the ones they watched lately (local scoring, no AI). Show it on their home screen with Section Type "This source\'s playlist" on the Home Screen tab.';
    var NEXT_WATCH_SHARED_WARNING = '⚠ Your Next Watch is personal: each user gets their own list in their own playlist. A collection or tag is shared, so everyone would see the same titles (the ones picked for the most users), not their own picks.';

    // Your Next Watch with a tag or collection on: warn (next to both toggles) that those are shared.
    function updateNextWatchWarning(row) {
        var isNw = (row.querySelector('.selSourceType') || {}).value === 'NextWatch';
        var on = isNw && (!!(row.querySelector('.chkEnableTag') || {}).checked || !!(row.querySelector('.chkEnableCollection') || {}).checked);
        row.querySelectorAll('.nw-shared-warning').forEach(function (el) { el.style.display = on ? 'block' : 'none'; });
    }

    // The playlist description placeholder shows the text used when it is left empty.
    function updatePlaylistDescPlaceholder(row) {
        var isNw = (row.querySelector('.selSourceType') || {}).value === 'NextWatch';
        var desc = row.querySelector('.playlist-tab .txtPlaylistDescription');
        if (desc) desc.setAttribute('placeholder', isNw ? NEXT_WATCH_DEFAULT_DESCRIPTION : "Optional description shown on the playlist's page...");
        var note = row.querySelector('.playlist-tab .pl-desc-note');
        if (note) note.textContent = isNw ? 'Leave empty to use the text shown in the box.' : 'Leave empty for no description.';
    }

    function getSourceBadgeHtml(st) {
        var map = {
            'External':          { icon: 'language',        title: 'External List' },
            'LocalCollection':   { icon: 'folder_special',  title: 'Local Collection' },
            'LocalPlaylist':     { icon: 'playlist_play',   title: 'Local Playlist' },
            'Manual':            { icon: 'format_list_numbered', title: 'Manual List' },
            'NextWatch':         { icon: 'recommend',       title: 'Personal: Your Next Watch' },
            'MediaInfo':         { icon: 'tune',            title: 'Smart Playlist' },
                'AI':              { icon: 'auto_awesome',   title: 'AI created lists' }
        };
        var e = map[st];
        if (!e) return '';
        return `<span class="tag-indicator source" title="${e.title}"><i class="md-icon" style="font-size:1.1em;">${e.icon}</i></span>`;
    }

    function renderTagGroup(tagConfig, container, prepend, index, isNew, afterRef) {
        var isChecked = tagConfig.Active !== false ? 'checked' : '';
        var tagName = tagConfig.Tag || '';
        var labelName = tagConfig.Name || '';
        var urls = tagConfig.Urls || (tagConfig.Url ? [{ url: tagConfig.Url, limit: tagConfig.Limit !== undefined ? tagConfig.Limit : 0 }] : [{ url: '', limit: 0 }]);
        if (!urls.length) urls = [{ url: '', limit: 0 }]; // e.g. a restored draft row with no URL yet
        var blacklist = migrateCommaSeparated((tagConfig.Blacklist || []).join('\n'));
        var intervals = tagConfig.ActiveIntervals || [];
        var idx = typeof index !== 'undefined' ? index : 9999;

        var lastMod = tagConfig.LastModified || new Date().toISOString();

        var enableTag = tagConfig.EnableTag !== false ? 'checked' : '';
        var enableColl = tagConfig.EnableCollection ? 'checked' : '';
        var enablePlaylist = tagConfig.EnablePlaylist ? 'checked' : '';
        var playlistName = tagConfig.PlaylistName || '';
        var playlistUserIds = tagConfig.PlaylistUserIds || [];
        var playlistUserIdsEnc = encodeURIComponent(JSON.stringify(playlistUserIds));
        var playlistMappingsEnc = encodeURIComponent(JSON.stringify(tagConfig.PlaylistMappings || []));
        var overrideChecked = tagConfig.OverrideWhenActive ? 'checked' : '';

        var collName = tagConfig.CollectionName || '';
        var collSortName = String(tagConfig.CollectionSortName || '').replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;');
        var collDescription = tagConfig.CollectionDescription || '';
        var collPosterPath = tagConfig.CollectionPosterPath || '';

        var sourceType = tagConfig.SourceType || "";
        var localSources = tagConfig.LocalSources || [];
        if (localSources.length === 0) localSources = [{ id: "", limit: 0 }];

        var mediaInfoLimit = tagConfig.Limit || (tagConfig.SourceType === 'NextWatch' ? 50 : 0);
        var nwSeedDays = tagConfig.NextWatchSeedDays > 0 ? tagConfig.NextWatchSeedDays : 90;
        var nwUseTaste = tagConfig.NextWatchUseTaste !== false;
        var aiLimit = tagConfig.Limit || 0;
        // Backwards compat: legacy single-target → derive separate tag + collection targets
        var _legacyTarget = tagConfig.MediaInfoTargetType || (tagConfig.MediaInfoSeasonMode ? 'Season' : '');
        // Tag output targets — default Series=true if no target has ever been set
        var _tagAnySet = tagConfig.TagTargetEpisode || tagConfig.TagTargetSeason || tagConfig.TagTargetSeries ||
                         tagConfig.MediaInfoTargetEpisode || tagConfig.MediaInfoTargetSeason || tagConfig.MediaInfoTargetSeries ||
                         _legacyTarget !== '';
        var _tagTargetEp  = tagConfig.TagTargetEpisode  || tagConfig.MediaInfoTargetEpisode || _legacyTarget === 'Episode';
        var _tagTargetSea = tagConfig.TagTargetSeason   || tagConfig.MediaInfoTargetSeason  || _legacyTarget === 'Season';
        var _tagTargetSer = tagConfig.TagTargetSeries   || tagConfig.MediaInfoTargetSeries  || _legacyTarget === 'Series' || !_tagAnySet;
        // Collection output targets — default Series=true if no target has ever been set
        var _collAnySet = tagConfig.CollectionTargetEpisode || tagConfig.CollectionTargetSeason || tagConfig.CollectionTargetSeries ||
                          tagConfig.MediaInfoTargetEpisode || tagConfig.MediaInfoTargetSeason || tagConfig.MediaInfoTargetSeries ||
                          _legacyTarget !== '';
        var _collTargetEp  = tagConfig.CollectionTargetEpisode || tagConfig.MediaInfoTargetEpisode || _legacyTarget === 'Episode';
        var _collTargetSea = tagConfig.CollectionTargetSeason  || tagConfig.MediaInfoTargetSeason  || _legacyTarget === 'Season';
        var _collTargetSer = tagConfig.CollectionTargetSeries  || tagConfig.MediaInfoTargetSeries  || _legacyTarget === 'Series' || !_collAnySet;

        var enableHomeSection = tagConfig.EnableHomeSection ? 'checked' : '';
        var homeSectionLibraryId = encodeURIComponent(tagConfig.HomeSectionLibraryId || 'auto');
        var homeSectionUserIdsEnc = encodeURIComponent(JSON.stringify(tagConfig.HomeSectionUserIds || []));
        var homeSectionSettingsEnc = encodeURIComponent(tagConfig.HomeSectionSettings || '{}');
        // The name the row's title defaults to (see clearDefaultCustomName).
        var homeSectionOrigNameEnc = encodeURIComponent((tagConfig.Name || tagConfig.Tag || '').trim());
        var homeSectionTrackedEnc = encodeURIComponent(JSON.stringify(tagConfig.HomeSectionTracked || []));
        var hsDefaultSectionType = tagConfig.EnableCollection ? 'boxset' : (tagConfig.EnableTag !== false ? 'items' : (tagConfig.EnablePlaylist ? 'playlist' : 'boxset'));
        var topListOn = !!tagConfig.EnableTopList;

        var mediaFilters = (tagConfig.MediaInfoFilters && tagConfig.MediaInfoFilters.length > 0)
            ? tagConfig.MediaInfoFilters
            : ((tagConfig.MediaInfoConditions && tagConfig.MediaInfoConditions.length > 0)
                ? [{ Operator: 'AND', Criteria: tagConfig.MediaInfoConditions }]
                : []);
        var filterGroupsHtml = mediaFilters.map((f, i) => getMediaInfoFilterGroupHtml(f, i, i === 0)).join('');

        var activeText = tagConfig.Active !== false ? "Active" : "Disabled";
        var activeColor = tagConfig.Active !== false ? "var(--hsc-primary-text)" : "var(--hsc-neutral)";

        var sourceBadgeHtml = getSourceBadgeHtml(sourceType);
        var indicatorsHtml = '';
        if (intervals.length > 0) {
            var schedActiveClass = isScheduleCurrentlyActive(intervals) ? ' schedule-active' : '';
            var schedPriorityClass = tagConfig.OverrideWhenActive ? ' priority-active' : '';
            var schedText = tagConfig.OverrideWhenActive ? 'Schedule priority' : 'Schedule';
            indicatorsHtml += `<span class="tag-indicator schedule${schedPriorityClass}${schedActiveClass}"><i class="md-icon" style="font-size:1.1em;">calendar_today</i> ${schedText}</span>`;
        }
        if (tagConfig.EnableCollection) {
            indicatorsHtml += `<span class="tag-indicator collection"><i class="md-icon" style="font-size:1.1em;">library_books</i> Collection</span>`;
        }
        if (tagConfig.EnableCollection && tagConfig.CollectionImported) {
            indicatorsHtml += `<span class="tag-indicator imported"><i class="md-icon" style="font-size:1.1em;">move_to_inbox</i> Imported</span>`;
        }
        if (tagConfig.EnableHomeSection) {
            indicatorsHtml += `<span class="tag-indicator homescreen"><i class="md-icon" style="font-size:1.1em;">home</i> Home Section</span>`;
        }
        if (tagConfig.EnableTag) {
            indicatorsHtml += `<span class="tag-indicator tag"><i class="md-icon" style="font-size:1.1em;">label</i> Tag</span>`;
        }
        if (tagConfig.EnablePlaylist) {
            indicatorsHtml += `<span class="tag-indicator playlist"><i class="md-icon" style="font-size:1.1em;">queue_music</i> Playlist</span>`;
        }
        if (topListOn) {
            indicatorsHtml += `<span class="tag-indicator toplist"><i class="md-icon" style="font-size:1.1em;">format_list_numbered</i> Top-List</span>`;
        }
        var lastRuns = tagConfig.LastRuns || [];
        indicatorsHtml += lastRunChipHtml(lastRuns, lastRuns.map(function (r) { var p = r.k.split('\x1F'); return { Url: p[0], LocalSourceId: p[1], Limit: r.lim }; }));

        var initialStyle = isNew ? 'display:block;' : 'display:none;';
        var initialIcon = isNew ? 'expand_less' : 'expand_more';

        var inactiveClass = tagConfig.Active === false ? "inactive" : "";
        var newClass = isNew ? "just-added" : "";

        var html = `
        <div class="tag-row ${inactiveClass} ${newClass}" data-index="${idx}" data-tag="${tagName.toLowerCase()}" data-last-modified="${lastMod}" data-nw-migrated="${tagConfig.NextWatchUsersMigrated ? '1' : '0'}" data-coll-imported="${tagConfig.CollectionImported ? '1' : '0'}" data-imported-st="${tagConfig.CollectionImported ? (tagConfig.SourceType || '') : ''}" data-poster-replaced="${tagConfig.CollectionPosterReplaced ? '1' : '0'}" data-bg-replaced="${tagConfig.CollectionBackgroundReplaced ? '1' : '0'}" data-thumb-replaced="${tagConfig.CollectionThumbReplaced ? '1' : '0'}" data-banner-replaced="${tagConfig.CollectionBannerReplaced ? '1' : '0'}" data-last-runs="${encodeURIComponent(JSON.stringify(tagConfig.LastRuns || []))}" data-group-key="${encodeURIComponent(groupIssueKey(tagConfig.Name, tagConfig.Tag))}" data-tl-saved="${topListOn && !isNew ? '1' : '0'}" data-tl-saved-tag="${escapeHtml((tagConfig.Tag || '').trim().toLowerCase())}" data-dirty="false">
            <div class="tag-header">
                <div style="display:flex; align-items:center; min-width:0;">
                    <div class="header-actions" style="margin-right:12px; display:flex; align-items:center;" onclick="event.stopPropagation()">
                        <div class="drag-handle">
                            <i class="md-icon">reorder</i>
                        </div>
                        <span class="lblActiveStatus" style="color:${activeColor};">${activeText}</span>
                        <label class="checkboxContainer" style="margin:0;">
                            <input type="checkbox" is="emby-checkbox" class="chkTagActive" ${isChecked} />
                            <span></span>
                        </label>
                    </div>
                    <div class="tag-info" style="display:flex; align-items:center;">
                        <span class="source-badge">${sourceBadgeHtml}</span>
                        <span class="hsc-run-issue">${groupIssueFlagHtml(_groupIssues[groupIssueKey(tagConfig.Name, tagConfig.Tag)])}</span>
                        <span class="tag-title">${labelName || tagName || 'New'}</span>
                        <span class="badge-container" style="display:flex; align-items:center;">${indicatorsHtml}</span>
                    </div>
                </div>
                <i class="md-icon expand-icon">${initialIcon}</i>
            </div>
            <div class="tag-body" style="${initialStyle}">
                <div class="hsc-run-issue-box">${groupIssueBoxHtml(_groupIssues[groupIssueKey(tagConfig.Name, tagConfig.Tag)])}</div>
                <div style="display:flex; justify-content:flex-end; margin-bottom:4px;">
                    <button type="button" is="emby-button" class="btnCopySourceRow raised hsc-btn-ghost hsc-btn-sm" title="Copy this source to the clipboard, to paste it on another server"><i class="md-icon" style="font-size:1em; margin-right:4px;">share</i><span>Copy</span></button>
                    <button type="button" is="emby-button" class="btnDuplicateRow raised hsc-btn-ghost hsc-btn-sm" title="Duplicate this source"><i class="md-icon" style="font-size:1em; margin-right:4px;">content_copy</i><span>Duplicate</span></button>
                </div>
                <div class="tag-tabs">
                    <div class="tag-tab active" data-tab="general">Source</div>
                    <div class="tag-tab" data-tab="tag">Tag</div>
                    <div class="tag-tab" data-tab="collection">Collection</div>
                    <div class="tag-tab" data-tab="playlist">Playlist</div>
                    <div class="tag-tab" data-tab="schedule">Schedule</div>
                    <div class="tag-tab hsc-adv" data-adv="blacklist" data-tab="advanced">Blacklist${ADV_BADGE}</div>
                    <div class="tag-tab" data-tab="homescreen">Home Screen</div>
                </div>
                
                <div class="tab-content general-tab">
                    <div class="inputContainer" style="flex-grow:1;"><input is="emby-input" class="txtEntryLabel" type="text" label="Display Name" value="${labelName}" /></div>
                    
                    <div style="margin-bottom: 15px;">
                        <label class="selectLabel">Source Type</label>
                        <select is="emby-select" class="selSourceType" style="width:100%;">
                            <option value="" ${!sourceType ? 'selected' : ''}>-- Select source type --</option>
                            <option value="AI" ${sourceType === 'AI' ? 'selected' : ''}>AI created lists</option>
                            <option value="External" ${sourceType === 'External' ? 'selected' : ''}>External List (Trakt/MDBList/TMDb)</option>
                            <option value="LocalCollection" ${sourceType === 'LocalCollection' ? 'selected' : ''}>Local Collection</option>
                            <option value="MediaInfo" ${sourceType === 'MediaInfo' ? 'selected' : ''}>Local Media Information (Smart Playlist)</option>
                            <option value="LocalPlaylist" ${sourceType === 'LocalPlaylist' ? 'selected' : ''}>Local Playlist</option>
                            <option value="Manual" ${sourceType === 'Manual' ? 'selected' : ''}>Manual List (pick movies and shows)</option>
                            <option value="NextWatch" ${sourceType === 'NextWatch' ? 'selected' : ''}>Personal: Your Next Watch</option>
                        </select>
                        <p class="source-type-hint" style="margin:6px 0 0 0; font-size:1em; opacity:0.8; line-height:1.4;">${(function(st) {
                            if (st === 'External')         return 'Use an external list to tag, or create a collection, from the items that match your library.';
                            if (st === 'LocalCollection') return 'Every item in the selected collection(s) gets the configured tag or is added to a new collection. You can also use this to create a curated list of selected collections as a home screen section.';
                            if (st === 'LocalPlaylist')   return 'Every item in the selected playlist(s) gets the configured tag or is added to a new collection.';
                            if (st === 'Manual')          return 'Pick movies and shows by hand. Search to add them, use the arrows to set the order, and remove them with ✕.';
                            if (st === 'MediaInfo')       return 'Filter your own library to select which movies or shows to tag or create a collection of. This is also known as a Smart Playlist.';
                            if (st === 'AI')              return 'Use AI to create a list. Write your prompt and the AI will build a list based on it.';
                            if (st === 'NextWatch')       return NEXT_WATCH_HINT;
                            return '';
                        })(sourceType)}</p>
                    </div>

                    <div class="source-external-container" style="display: ${sourceType === 'External' ? 'block' : 'none'};">
                        <div style="display:flex; align-items:baseline; gap:10px; margin:10px 0 10px 0; flex-wrap:wrap;">
                            <p style="margin:0; font-size:0.9em; font-weight:bold; opacity:0.7;">Source URLs</p>
                            <span style="font-size:0.75em; opacity:0.5;">— Find lists: <a href="https://trakt.tv/discover" target="_blank" style="color:inherit; text-decoration:underline;">Trakt</a> &middot; <a href="https://mdblist.com/toplists/" target="_blank" style="color:inherit; text-decoration:underline;">MDBList</a> &middot; <a href="https://www.themoviedb.org/" target="_blank" style="color:inherit; text-decoration:underline;">TMDb</a> &middot; <a href="https://developer.themoviedb.org/reference/getting-started" target="_blank" style="color:inherit; text-decoration:underline;">TMDb API</a></span>
                        </div>
                        <div class="url-list-container">${urls.map(u => getUrlRowHtml(u.url, u.limit)).join('')}</div>
                        <div style="margin-top:10px;"><button is="emby-button" type="button" class="raised btnAddUrl hsc-btn-dashed" style="width:100%;"><i class="md-icon" style="margin-right:5px;">add</i>Add another URL</button></div>
                    </div>

                    <div class="source-local-container" style="display: ${(sourceType === 'LocalCollection' || sourceType === 'LocalPlaylist') ? 'block' : 'none'};">
                        <div style="display:flex; align-items:center; gap:10px; margin:10px 0 10px 0;">
                            <p style="margin:0; font-size:0.9em; font-weight:bold; opacity:0.7;" class="local-type-label">${sourceType === 'LocalPlaylist' ? 'Select Playlists' : 'Select Collections'}</p>
                            <button type="button" is="emby-button" class="raised button-submit btnMiPreview hsc-btn-secondary hsc-btn-sm" style="margin-left:auto;" title="Preview Source"><span>Preview</span></button>
                        </div>
                        <div class="local-list-container">${localSources.map(ls => getLocalRowHtml(sourceType, ls.id, ls.limit)).join('')}</div>
                        <div style="margin-top:10px;"><button is="emby-button" type="button" class="raised btnAddLocal hsc-btn-dashed" style="width:100%;"><i class="md-icon" style="margin-right:5px;">add</i>Add another</button></div>
                    </div>

                    <div class="source-manual-container" data-manual-ids="${encodeURIComponent(JSON.stringify(tagConfig.ManualItemIds || []))}" style="display: ${sourceType === 'Manual' ? 'block' : 'none'};">
                        <div style="display:flex; align-items:center; gap:10px; margin:10px 0 10px 0;">
                            <p style="margin:0; font-size:0.9em; font-weight:bold; opacity:0.7;">Movies and Shows</p>
                            <button type="button" is="emby-button" class="raised button-submit btnMiPreview hsc-btn-secondary hsc-btn-sm" style="margin-left:auto;" title="Preview Source"><span>Preview</span></button>
                        </div>
                        <div style="position:relative; margin-bottom:10px;">
                            <input is="emby-input" type="text" class="txtManualSearch" autocomplete="off" label="Add a movie or show" placeholder="Type to search…" />
                            <div class="manualSearchResults" style="display:none;position:absolute;top:100%;left:0;right:0;z-index:200;background:var(--plugin-popup-bg,#2a2a2a);border:1px solid var(--line-color);border-radius:6px;max-height:240px;overflow-y:auto;margin-top:2px;box-shadow:var(--hsc-shadow-float);"></div>
                        </div>
                        <div class="manualItemList" style="max-height:420px;overflow-y:auto;border:1px solid var(--line-color);border-radius:6px;padding:4px 8px;min-height:44px;"></div>
                    </div>

                    <div class="source-ai-container" style="display: ${sourceType === 'AI' ? 'block' : 'none'};">
                        <div style="margin-bottom: 15px;">
                            <label class="selectLabel">AI Provider</label>
                            <select is="emby-select" class="selAiProvider" style="width:100%;">
                                ${aiProviderOptionsHtml(tagConfig.AiProvider || 'OpenAI')}
                            </select>
                            <div class="ai-provider-missing" style="display:${aiProviderMissing(tagConfig.AiProvider) ? 'block' : 'none'}; font-size:0.9em; color:var(--hsc-danger-text); margin-top:4px;">Needs a provider</div>
                        </div>

                        <div class="ollama-experimental-warning" style="display:${(tagConfig.AiProvider || 'OpenAI') === 'Ollama' ? 'flex' : 'none'}; align-items:flex-start; gap:8px; background:rgba(230,193,90,0.1); border:1px solid rgba(230,193,90,0.35); border-radius:6px; padding:10px 12px; margin-bottom:15px; font-size:0.85em; line-height:1.5;">
                            <i class="md-icon" style="font-size:1.1em; color:var(--hsc-warning-text); flex-shrink:0; margin-top:1px;">warning</i>
                            <span style="opacity:0.85;"><strong>Experimental:</strong> Ollama support is experimental. Local models may return inaccurate IMDB IDs — the plugin will fall back to title matching, but results <strong>WILL</strong> vary depending on the model used. Known limitations with local AI is the lack of new and updated information, and limited ability to research online. Cloud based models seems to be the best options for now. </span>
                        </div>

                        <div class="inputContainer" style="margin-bottom:15px;">
                            <textarea is="emby-textarea" class="txtAiPrompt" rows="3"
                                label="Prompt"
                                style="width:100%; resize:vertical; box-sizing:border-box;"
                                placeholder="e.g. Give me the best thriller movies from the 2000s">${tagConfig.AiPrompt || ''}</textarea>
                            <div class="fieldDescription">Write your intent. The system will automatically format the output as a structured movie/show list. You don't need to specify a format.</div>
                        </div>

                        <div class="checkboxContainer checkboxContainer-withDescription" style="margin-top:12px;">
                            <label>
                                <input type="checkbox" is="emby-checkbox" class="chkAiRecentlyWatched" ${tagConfig.AiIncludeRecentlyWatched ? 'checked' : ''} />
                                <span>Include recently watched for personalization</span>
                            </label>
                            <div class="fieldDescription">Prepends the selected user's watch history to the AI prompt, enabling "Recommended for you" style lists.</div>
                        </div>

                        <div class="ai-recently-watched-options" style="display: ${tagConfig.AiIncludeRecentlyWatched ? 'block' : 'none'}; margin-top:10px; padding-left:10px; border-left:2px solid rgba(128,128,128,0.3);">
                            <div style="margin-bottom:10px;">
                                <label class="selectLabel">User for watch history</label>
                                <select is="emby-select" class="selAiWatchedUser" style="width:100%;">
                                    <option value="">-- Select user --</option>
                                    ${(_miUsers || []).map(function(u) { return '<option value="' + u.Id + '"' + (u.Id === (tagConfig.AiRecentlyWatchedUserId || '') ? ' selected' : '') + '>' + escapeHtml(u.Name + userRatingSuffix(u)) + '</option>'; }).join('')}
                                </select>
                            </div>
                            <div style="display:flex; align-items:center; gap:10px; flex-wrap:wrap;">
                                <label style="font-size:0.9em; white-space:nowrap; margin:0;">Recent items to include</label>
                                <input is="emby-input" class="txtAiWatchedCount" type="number" value="${tagConfig.AiRecentlyWatchedCount || 20}" min="5" max="100" style="width:80px;" />
                            </div>
                        </div>

                        <div style="display:flex; align-items:center; gap:12px; margin-top:15px; margin-bottom:15px;">
                            <label style="font-size:0.9em; white-space:nowrap; margin:0;">Max items</label>
                            <input is="emby-input" class="txtAiLimit" type="number" value="${aiLimit}" min="0" style="width:90px;" />
                            <span style="font-size:0.8em; opacity:0.5;">0 = no limit. Injected into the prompt automatically.</span>
                        </div>

                        <div style="display:flex; align-items:center; gap:12px; margin-top:0; margin-bottom:15px;">
                            <label style="font-size:0.9em; white-space:nowrap; margin:0;">Refresh every</label>
                            <input is="emby-input" class="txtAiRefreshInterval" type="number" value="${tagConfig.AiRefreshIntervalDays || 0}" min="0" style="width:90px;" />
                            <span style="font-size:0.8em; opacity:0.5;">days &nbsp;(0 = run on every full sync)</span>
                        </div>

                        <div style="margin-top:0; display:flex; align-items:center; flex-wrap:wrap; gap:6px;">
                            <button type="button" is="emby-button" class="raised btnTestAiSource btn-neutral" style="background:transparent; border:1px solid rgba(128,128,128,0.4); color:var(--theme-text-secondary);">
                                <i class="md-icon" style="margin-right:5px;">science</i>Test AI Source
                            </button>
                            <span class="ai-test-result" style="margin-left:10px; font-size:0.85em; opacity:0.7;"></span>
                        </div>
                    </div>

                    <div class="source-mediainfo-container" style="display: ${(sourceType && sourceType !== '') ? 'block' : 'none'};">
                        <div class="mi-limit-row" style="display:${sourceType === 'MediaInfo' || sourceType === 'NextWatch' ? 'flex' : 'none'}; align-items:center; gap:12px; margin-bottom:14px; flex-wrap:wrap;">
                            <label style="font-size:0.9em; white-space:nowrap; margin:0;">Max items</label>
                            <input is="emby-input" class="txtMediaInfoLimit" type="number" value="${mediaInfoLimit}" min="0" style="width:90px;" />
                            <button type="button" is="emby-button" class="raised button-submit btnMiPreview hsc-btn-secondary hsc-btn-sm" style="margin-left:auto;" title="Preview Source"><span>Preview</span></button>
                            <button type="button" is="emby-button" class="btnMiHelp btnMiLimitHelp raised" style="display:${sourceType === 'MediaInfo' ? '' : 'none'}; background:transparent; border:1px solid rgba(128,128,128,0.35); color:var(--theme-text-secondary); font-size:0.82em; padding:0 10px; min-width:0;"><i class="md-icon" style="font-size:1em; margin-right:4px;">help_outline</i><span>How to (filter guide)</span></button>
                        </div>
                        <div class="nw-settings" style="display:${sourceType === 'NextWatch' ? 'block' : 'none'}; margin-bottom:14px;">
                            <div style="display:flex; align-items:center; gap:12px; flex-wrap:wrap;">
                                <label style="font-size:0.9em; white-space:nowrap; margin:0;">History window (days)</label>
                                <input is="emby-input" class="txtNwSeedDays" type="number" value="${nwSeedDays}" min="7" max="730" style="width:90px;" />
                            </div>
                            <div class="fieldDescription">Titles each user finished in this many days are their taste (a quiet stretch is topped up with older history). Default 90.</div>
                            <div class="checkboxContainer checkboxContainer-withDescription" style="margin-top:10px; margin-bottom:0;">
                                <label>
                                    <input is="emby-checkbox" type="checkbox" class="chkNwUseTaste" ${nwUseTaste ? 'checked' : ''} />
                                    <span>Use favourites and ratings</span>
                                </label>
                                <div class="fieldDescription">Watched favourites and titles a user rated 7 or higher count as taste, unwatched favourites are suggested, and titles rated low are ignored. Off: only what was watched counts.</div>
                            </div>
                        </div>
                        <div class="mi-toggle-row" style="display:${sourceType === 'MediaInfo' || mediaFilters.length > 0 ? 'none' : 'block'}; padding-top:14px; border-top:1px solid var(--line-color);">
                            <button type="button" is="emby-button" class="btnToggleAdditionalFilters raised" style="background:transparent; border:1px solid rgba(128,128,128,0.35); color:var(--theme-text-secondary); font-size:0.85em;"><i class="md-icon" style="font-size:1em; margin-right:4px;">filter_list</i><span>Add filters (optional)</span></button>
                        </div>
                        <div class="mi-filter-body" style="display:${sourceType === 'MediaInfo' || mediaFilters.length > 0 ? 'block' : 'none'};">
                            <div class="mi-help-btn-row" style="display:${sourceType === 'MediaInfo' ? 'none' : 'flex'}; margin-bottom:14px; padding-top:14px; border-top:1px solid var(--line-color);">
                                <button type="button" is="emby-button" class="btnMiHelp raised" style="margin-left:auto; background:transparent; border:1px solid rgba(128,128,128,0.35); color:var(--theme-text-secondary); font-size:0.82em; padding:0 10px; min-width:0;"><i class="md-icon" style="font-size:1em; margin-right:4px;">help_outline</i><span>How to (filter guide)</span></button>
                            </div>
                            <div class="mediainfo-filter-list">${filterGroupsHtml}</div>
                            <div style="display:flex; gap:8px; margin-top:8px;">
                                <button type="button" is="emby-button" class="btnAddMediaInfoFilter raised hsc-btn-dashed" style="flex:1;"><i class="md-icon" style="margin-right:5px;">add</i>Add Filter Group</button>
                                <button type="button" is="emby-button" class="btnClearAllFilters raised hsc-btn-danger" style="padding:0 14px; min-width:0;" title="Clear all filters"><i class="md-icon" style="font-size:1em;">delete_sweep</i></button>
                            </div>
                            <div class="mi-presets-section" style="margin-top:30px; border-top:1px solid var(--line-color); padding-top:12px; display:${sourceType === 'MediaInfo' ? 'block' : 'none'};">
                            ${true ? `
                                <button type="button" is="emby-button" class="btnPremadeFilters raised btn-neutral" style="width:100%; margin-bottom:6px; background:transparent; border:1px solid var(--line-color); color:var(--theme-text-secondary); display:flex; align-items:center; justify-content:space-between;"><span><i class="md-icon" style="margin-right:5px;">auto_awesome</i>Premade filters</span><i class="md-icon mi-expand-icon" style="transition:transform 0.2s; font-size:1.2em;">expand_more</i></button>
                                <div class="mi-preset-panel" style="display:none; border:1px solid var(--line-color); border-radius:6px; padding:10px 12px; margin-bottom:8px; background:rgba(128,128,128,0.06);">
                                    <div style="font-size:0.8em; color:var(--theme-text-secondary); margin-bottom:10px;">Select a preset to replace the current filters:</div>
                                    ${MI_PRESETS.map(function(cat, ci) {
                                        return '<div style="margin-bottom:10px;">' +
                                            '<div class="hsc-eyebrow" style="margin-bottom:5px;">' + cat.label + '</div>' +
                                            '<div style="display:flex; flex-wrap:wrap; gap:6px;">' +
                                            cat.presets.map(function(p, pi) {
                                                return '<button type="button" class="btnApplyMiPreset" data-preset="' + ci + ',' + pi + '"' +
                                                    ' style="border:1px solid var(--hsc-hairline-strong); border-radius:9999px; padding:4px 12px; font-family:inherit; font-size:0.82em; cursor:pointer; background:transparent; color:var(--theme-text-primary);">' + p.name + '</button>';
                                            }).join('') + '</div></div>';
                                    }).join('')}
                                </div>
                                <button type="button" is="emby-button" class="btnMySavedFilters raised btn-neutral" style="width:100%; margin-bottom:6px; background:transparent; border:1px solid var(--line-color); color:var(--theme-text-secondary); display:flex; align-items:center; justify-content:space-between;"><span><i class="md-icon" style="margin-right:5px;">bookmarks</i>My saved filters</span><i class="md-icon mi-expand-icon" style="transition:transform 0.2s; font-size:1.2em;">expand_more</i></button>
                                <div class="mi-saved-panel" style="display:none; border:1px solid var(--line-color); border-radius:6px; padding:10px 12px; margin-bottom:8px; background:rgba(128,128,128,0.06);">
                                    <div style="font-size:0.8em; color:var(--theme-text-secondary); margin-bottom:10px;">Select a saved filter to replace the current filters:</div>
                                    <div class="mi-saved-panel-content">${getMySavedFiltersPanelHtml()}</div>
                                    <div style="border-top:1px solid var(--line-color); margin:12px 0 10px;"></div>
                                    <div style="font-size:0.8em; color:var(--theme-text-secondary); margin-bottom:6px;">Save current filter as:</div>
                                    <div class="mi-saveas-bar" style="display:flex; gap:6px; align-items:flex-start;">
                                        <input type="text" class="txtSaveFilterName emby-input" placeholder="Filter name..." style="flex:1; padding:4px 8px; font-size:0.85em;" />
                                        <button type="button" is="emby-button" class="btnConfirmSaveFilter raised" style="background:var(--button-background); color:var(--button-foreground); flex-shrink:0;">Save</button>
                                    </div>
                                </div>
                            ` : ''}
                            </div>
                        </div>
                    </div>

                </div>

            <div class="tab-content tagname-tab" style="display:none;">
                    <div class="tag-tab-controls">
                        <div class="checkboxContainer checkboxContainer-withDescription">
                            <label>
                                <input is="emby-checkbox" type="checkbox" class="chkEnableTag" ${enableTag} />
                                <span>Apply Tag</span>
                            </label>
                            <div class="fieldDescription">Automatically tag matched items in Emby.</div>
                            <div class="nw-shared-warning fieldDescription" style="display:none; color:var(--hsc-warning-text); margin-top:6px;">${NEXT_WATCH_SHARED_WARNING}</div>
                        </div>
                        <div class="tag-settings" style="margin-left: 20px; padding-left: 15px; border-left: 2px solid var(--line-color); margin-top: 10px; display: ${tagConfig.EnableTag !== false ? 'block' : 'none'};">
                            <div class="inputContainer">
                                <input is="emby-input" type="text" class="txtTagName" label="Tag Name" value="${tagName}" placeholder="${labelName}" />
                                <div class="fieldDescription">Leave empty to use Display Name.</div>
                            </div>
                            <div class="extra-tag-list">${(tagConfig.ExtraTags || []).map(getExtraTagRowHtml).join('')}</div>
                            <button is="emby-button" type="button" class="raised btnAddExtraTag hsc-btn-dashed" style="width:100%;"><i class="md-icon" style="margin-right:5px;">add</i>Add another tag</button>
                            <div class="fieldDescription" style="margin-bottom:12px;">Extra tags go on the same titles. The home row, top-list and tag images use the Tag Name.</div>
                            <div class="mi-tag-target-section hsc-adv" data-adv="target" style="margin-top:4px;">
                                <div style="font-size:0.85em; opacity:0.6; margin-bottom:6px; display:flex; align-items:center; gap:8px;">
                                    <span>For TV shows, choose what level to tag:${ADV_BADGE}</span>
                                    <button type="button" is="emby-button" class="btnTagTargetHelp raised" style="background:transparent; border:1px solid rgba(128,128,128,0.35); color:var(--theme-text-secondary); font-size:0.82em; padding:0 10px; min-width:0;"><i class="md-icon" style="font-size:1em; margin-right:4px;">help_outline</i><span>How to use</span></button>
                                </div>
                                <div style="display:flex; flex-direction:row; align-items:center; gap:20px;">
                                    <label style="display:flex; align-items:center; gap:6px; cursor:pointer; white-space:nowrap;">
                                        <input type="checkbox" is="emby-checkbox" class="chkTagTargetSeries" ${_tagTargetSer ? 'checked' : ''} />
                                        <span>Series</span>
                                    </label>
                                    <label style="display:flex; align-items:center; gap:6px; cursor:pointer; white-space:nowrap;">
                                        <input type="checkbox" is="emby-checkbox" class="chkTagTargetSeason" ${_tagTargetSea ? 'checked' : ''} />
                                        <span>Season</span>
                                    </label>
                                    <label style="display:flex; align-items:center; gap:6px; cursor:pointer; white-space:nowrap;">
                                        <input type="checkbox" is="emby-checkbox" class="chkTagTargetEpisode" ${_tagTargetEp ? 'checked' : ''} />
                                        <span>Episode</span>
                                    </label>
                                </div>
                            </div>

                            <details class="tag-art-more hsc-adv" data-adv="tag-art"${tagConfig.TagPosterStyle || tagConfig.TagBackgroundStyle || tagConfig.TagBackgroundPath ? ' data-custom="1"' : ''}>
                            <summary><span><i class="md-icon" style="margin-right:5px;">image</i>Customize tag images${ADV_BADGE}<span class="tag-art-custom-chip">customized</span></span><i class="md-icon tag-art-chevron">expand_more</i></summary>
                            <div class="fieldDescription" style="margin-top:8px;">Images for the <strong>tag itself</strong>: the poster and background of the tag's own page in Emby, the page that opens when you click the tag on a movie or show. Not the posters of the movies or shows: those are never changed here.</div>
                            <div style="margin-top:15px;">
                                <p style="margin:0 0 8px 0; font-size:0.9em; font-weight:bold; opacity:0.7;">Tag Poster</p>
                                ${buildCollStylePickerHtml('poster', tagConfig.TagPosterStyle, 'tag', tagConfig.TagPosterOptions)}
                                <div class="fieldDescription tag-poster-note" style="display:${isKeepArtStyle(tagConfig.TagPosterStyle) ? 'none' : 'block'};">${tagConfig.TagPosterStyle ? 'Drawn from the posters of the first titles with this tag, with the tag name. It is redrawn when the titles change.' : "Emby's own collage of the tagged titles."}</div>
                            </div>

                            <div style="margin-top:15px;">
                                <p style="margin:0 0 8px 0; font-size:0.9em; font-weight:bold; opacity:0.7;">Tag Background</p>
                                ${buildCollStylePickerHtml('background', tagConfig.TagBackgroundStyle, 'tag', tagConfig.TagBackgroundOptions)}
                                <div class="tag-bg-upload" style="display:${tagConfig.TagBackgroundStyle ? 'none' : 'block'};">
                                <div class="bg-preview-container" style="margin-bottom:8px; display:${tagConfig.TagBackgroundPath ? 'block' : 'none'};">
                                    <span class="bg-filename" style="font-size:0.85em; opacity:0.7;">${tagConfig.TagBackgroundPath ? tagConfig.TagBackgroundPath.split(/[\\\\/]/).pop() : ''}</span>
                                    <button type="button" class="btnRemoveBg" style="margin-left:10px; font-size:0.8em; background:transparent; border:none; color:var(--hsc-danger-text); cursor:pointer; vertical-align:middle;">✕ Remove</button>
                                </div>
                                <img class="bg-preview-img" src="" alt="" style="max-width:240px; max-height:135px; border-radius:4px; display:none; margin-bottom:8px;" />
                                <input type="file" class="inputBgFile" accept="image/*" style="display:none;" />
                                <input type="hidden" class="hiddenBgPath" value="${tagConfig.TagBackgroundPath || ''}" />
                                <button type="button" is="emby-button" class="btnChooseBg raised hsc-btn-dashed" style="width:100%;">
                                    <i class="md-icon" style="margin-right:5px;">image</i>Choose Background Image
                                </button>
                                <div style="display:flex; align-items:center; gap:6px; margin-top:8px; opacity:0.45;">
                                    <div style="flex:1; height:1px; background:currentColor;"></div>
                                    <span style="font-size:0.75em;">or</span>
                                    <div style="flex:1; height:1px; background:currentColor;"></div>
                                </div>
                                <div style="display:flex; gap:6px; margin-top:6px;">
                                    <input class="txtBgUrl" is="emby-input" type="url" placeholder="https://example.com/background.jpg" style="flex:1;" />
                                    <button type="button" is="emby-button" class="btnLoadBgUrl raised btn-neutral">Load</button>
                                </div>
                                <div class="fieldDescription">Leave empty for no background on the tag's page (Emby's default).</div>
                                </div>
                            </div>

                            <div class="inputContainer tag-art-title-row" style="margin-top:15px; display:${isGenArtStyle(tagConfig.TagPosterStyle) || isGenArtStyle(tagConfig.TagBackgroundStyle) ? 'block' : 'none'};">
                                <input is="emby-input" type="text" class="txtTagArtTitle" label="Title on the art" value="${(tagConfig.TagArtTitle || '').replace(/"/g, '&quot;')}" placeholder="{name}" />
                                <div class="fieldDescription">Leave empty to use the tag name. <b>{name}</b> = tag name, <b>{week}</b> = this week's number.</div>
                            </div>

                            <div class="tag-art-preview-row" style="margin-top:10px; display:${isGenArtStyle(tagConfig.TagPosterStyle) || isGenArtStyle(tagConfig.TagBackgroundStyle) ? 'block' : 'none'};">
                                <button type="button" is="emby-button" class="btnTagArtPreview raised" style="background:transparent; border:1px solid rgba(128,128,128,0.35); color:var(--theme-text-secondary); font-size:0.82em; padding:0 10px; min-width:0;"><i class="md-icon" style="font-size:1em; margin-right:4px;">visibility</i><span>Preview art</span></button>
                            </div>
                            </details>
                        </div>
                    </div>
            </div>

                <div class="tab-content schedule-tab" style="display:none;">
                    <p style="margin:0 0 15px 0; font-size:0.9em; opacity:0.8;">Define when this tag should be active. If empty, it's always active.</p>
                    <div class="date-list-container">${intervals.map(i => getDateRowHtml(i)).join('')}</div>
                    <button is="emby-button" type="button" class="btnAddDate hsc-btn-dashed" style="width:100%; margin-top:10px;"><i class="md-icon" style="margin-right:5px;">event</i>Add Schedule Rule</button>
                    <button type="button" is="emby-button" class="btnSchedulePresets raised btn-neutral" style="width:100%; margin-top:6px; background:transparent; border:1px solid var(--line-color); color:var(--theme-text-secondary); display:flex; align-items:center; justify-content:space-between;"><span><i class="md-icon" style="margin-right:5px;">celebration</i>Holiday presets</span><i class="md-icon mi-expand-icon" style="transition:transform 0.2s; font-size:1.2em;">expand_more</i></button>
                    <div class="schedule-preset-panel" style="display:none; border:1px solid var(--line-color); border-radius:6px; padding:10px 12px; margin-top:6px; background:rgba(128,128,128,0.06);">
                        <div style="font-size:0.8em; color:var(--theme-text-secondary); margin-bottom:10px;">Select a holiday to add it as a rule (you can change the dates afterwards):</div>
                        <div style="display:flex; flex-wrap:wrap; gap:6px;">
                            ${SCHEDULE_PRESETS.map(function(p, pi) {
                                return '<button type="button" class="btnApplySchedulePreset" data-preset="' + pi + '"' +
                                    ' style="border:1px solid var(--hsc-hairline-strong); border-radius:9999px; padding:4px 12px; font-family:inherit; font-size:0.82em; cursor:pointer; background:transparent; color:var(--theme-text-primary);">' + p.name + '</button>';
                            }).join('')}
                        </div>
                    </div>
                    <div class="checkboxContainer checkboxContainer-withDescription hsc-adv" data-adv="check" style="margin-top:16px; ${intervals.length === 0 ? 'opacity:0.4;' : ''}">
                        <label>
                            <input is="emby-checkbox" type="checkbox" class="chkOverrideWhenActive" ${overrideChecked} ${intervals.length === 0 ? 'disabled' : ''} />
                            <span>Priority override when active${ADV_BADGE}</span>
                        </label>
                        <div class="fieldDescription">When this entry is in schedule, all other entries sharing the same tag or collection are suppressed — only this entry's items keep the tag and collection. If several priority entries are in schedule at once, the one highest in the list wins.</div>
                    </div>
                </div>

                <div class="tab-content collection-tab" style="display:none;">
                    <div class="collection-tab-controls">
                    <div class="checkboxContainer checkboxContainer-withDescription">
                        <label>
                            <input is="emby-checkbox" type="checkbox" class="chkEnableCollection" ${enableColl} />
                            <span>Create Collection</span>
                        </label>
                        <div class="fieldDescription">Automatically create and maintain an Emby Collection from these items.</div>
                        <div class="nw-shared-warning fieldDescription" style="display:none; color:var(--hsc-warning-text); margin-top:6px;">${NEXT_WATCH_SHARED_WARNING}</div>
                    </div>
                    
                    <div class="collection-settings" style="margin-left: 20px; padding-left: 15px; border-left: 2px solid var(--line-color); margin-top: 10px; display: ${tagConfig.EnableCollection ? 'block' : 'none'};">
                        <div class="inputContainer">
                            <input is="emby-input" type="text" class="txtCollectionName" label="Collection Name" value="${collName}" placeholder="${labelName}" />
                            <div class="fieldDescription">Leave empty to use Display Name.</div>
                        </div>

                        <div class="inputContainer">
                            <input is="emby-input" type="text" class="txtCollectionSortName" label="Sort Title" value="${collSortName}" />
                            <div class="fieldDescription">Optional. Used instead of the name when collections are sorted by name, e.g. "001" to show this collection first. Leave empty to sort by the collection name.</div>
                        </div>
                        ${getOutputTagHtml('coll', tagConfig.EnableCollectionTag, tagConfig.CollectionTag, tagConfig.CollectionExtraTags)}

                        <div class="mi-coll-target-section hsc-adv" data-adv="target" style="margin-top:10px;">
                            <div style="font-size:0.85em; opacity:0.6; margin-bottom:6px; display:flex; align-items:center; gap:8px;">
                                <span>For TV shows, choose what level to add to collection:${ADV_BADGE}</span>
                                <button type="button" is="emby-button" class="btnCollTargetHelp raised" style="background:transparent; border:1px solid rgba(128,128,128,0.35); color:var(--theme-text-secondary); font-size:0.82em; padding:0 10px; min-width:0;"><i class="md-icon" style="font-size:1em; margin-right:4px;">help_outline</i><span>How to use</span></button>
                            </div>
                            <div style="display:flex; flex-direction:row; align-items:center; gap:20px;">
                                <label style="display:flex; align-items:center; gap:6px; cursor:pointer; white-space:nowrap;">
                                    <input type="checkbox" is="emby-checkbox" class="chkCollTargetSeries" ${_collTargetSer ? 'checked' : ''} />
                                    <span>Series</span>
                                </label>
                                <label style="display:flex; align-items:center; gap:6px; cursor:pointer; white-space:nowrap;">
                                    <input type="checkbox" is="emby-checkbox" class="chkCollTargetSeason" ${_collTargetSea ? 'checked' : ''} />
                                    <span>Season</span>
                                </label>
                                <label style="display:flex; align-items:center; gap:6px; cursor:pointer; white-space:nowrap;">
                                    <input type="checkbox" is="emby-checkbox" class="chkCollTargetEpisode" ${_collTargetEp ? 'checked' : ''} />
                                    <span>Episode</span>
                                </label>
                            </div>
                        </div>

                        <div class="inputContainer" style="margin-top:15px;">
                            <textarea is="emby-textarea" class="txtCollectionDescription" rows="3"
                                label="Description"
                                placeholder="Optional description for this collection..."
                                style="width:100%; resize:vertical; box-sizing:border-box;">${collDescription}</textarea>
                        </div>

                        <div style="margin-top:15px;">
                            <p class="coll-poster-heading" style="margin:0 0 8px 0; font-size:0.9em; font-weight:bold; opacity:0.7;">Collection Poster</p>
                            ${buildCollStylePickerHtml('poster', tagConfig.CollectionPosterStyle, 'collection', tagConfig.CollectionPosterOptions, tagConfig.CollectionImported && !tagConfig.CollectionPosterReplaced)}
                            <div class="coll-poster-upload" style="display:${tagConfig.CollectionPosterStyle ? 'none' : 'block'};">
                            <div class="poster-preview-container" style="margin-bottom:8px; display:${collPosterPath ? 'block' : 'none'};">
                                <span class="poster-filename" style="font-size:0.85em; opacity:0.7;">${collPosterPath ? collPosterPath.split(/[\\\\/]/).pop() : ''}</span>
                                <button type="button" class="btnRemovePoster" style="margin-left:10px; font-size:0.8em; background:transparent; border:none; color:var(--hsc-danger-text); cursor:pointer; vertical-align:middle;">✕ Remove</button>
                            </div>
                            <img class="poster-preview-img" src="" alt="" style="max-width:120px; max-height:180px; border-radius:4px; display:none; margin-bottom:8px;" />
                            <input type="file" class="inputPosterFile" accept="image/*" style="display:none;" />
                            <input type="hidden" class="hiddenPosterPath" value="${collPosterPath}" />
                            <button type="button" is="emby-button" class="btnChoosePoster raised hsc-btn-dashed" style="width:100%;">
                                <i class="md-icon" style="margin-right:5px;">image</i>Choose Poster Image
                            </button>
                            <div style="display:flex; align-items:center; gap:6px; margin-top:8px; opacity:0.45;">
                                <div style="flex:1; height:1px; background:currentColor;"></div>
                                <span style="font-size:0.75em;">or</span>
                                <div style="flex:1; height:1px; background:currentColor;"></div>
                            </div>
                            <div style="display:flex; gap:6px; margin-top:6px;">
                                <input class="txtPosterUrl" is="emby-input" type="url" placeholder="https://example.com/poster.jpg" style="flex:1;" />
                                <button type="button" is="emby-button" class="btnLoadPosterUrl raised btn-neutral">Load</button>
                            </div>
                            </div>
                            <div class="fieldDescription coll-poster-gen-note" style="display:${isGenArtStyle(tagConfig.CollectionPosterStyle) ? 'block' : 'none'};">Drawn from the posters of the first titles in the collection, with the collection name. It is redrawn when the titles change.</div>
                        </div>

                        <div style="margin-top:15px;">
                            <p class="coll-bg-heading" style="margin:0 0 8px 0; font-size:0.9em; font-weight:bold; opacity:0.7;">Collection Background</p>
                            ${buildCollStylePickerHtml('background', tagConfig.CollectionBackgroundStyle, 'collection', tagConfig.CollectionBackgroundOptions, tagConfig.CollectionImported && !tagConfig.CollectionBackgroundReplaced)}
                            <div class="coll-bg-upload" style="display:${tagConfig.CollectionBackgroundStyle ? 'none' : 'block'};">
                            <div class="bg-preview-container" style="margin-bottom:8px; display:${tagConfig.CollectionBackgroundPath ? 'block' : 'none'};">
                                <span class="bg-filename" style="font-size:0.85em; opacity:0.7;">${tagConfig.CollectionBackgroundPath ? tagConfig.CollectionBackgroundPath.split(/[\\\\/]/).pop() : ''}</span>
                                <button type="button" class="btnRemoveBg" style="margin-left:10px; font-size:0.8em; background:transparent; border:none; color:var(--hsc-danger-text); cursor:pointer; vertical-align:middle;">✕ Remove</button>
                            </div>
                            <img class="bg-preview-img" src="" alt="" style="max-width:240px; max-height:135px; border-radius:4px; display:none; margin-bottom:8px;" />
                            <input type="file" class="inputBgFile" accept="image/*" style="display:none;" />
                            <input type="hidden" class="hiddenBgPath" value="${tagConfig.CollectionBackgroundPath || ''}" />
                            <button type="button" is="emby-button" class="btnChooseBg raised hsc-btn-dashed" style="width:100%;">
                                <i class="md-icon" style="margin-right:5px;">image</i>Choose Background Image
                            </button>
                            <div style="display:flex; align-items:center; gap:6px; margin-top:8px; opacity:0.45;">
                                <div style="flex:1; height:1px; background:currentColor;"></div>
                                <span style="font-size:0.75em;">or</span>
                                <div style="flex:1; height:1px; background:currentColor;"></div>
                            </div>
                            <div style="display:flex; gap:6px; margin-top:6px;">
                                <input class="txtBgUrl" is="emby-input" type="url" placeholder="https://example.com/background.jpg" style="flex:1;" />
                                <button type="button" is="emby-button" class="btnLoadBgUrl raised btn-neutral">Load</button>
                            </div>
                            <div class="fieldDescription">Leave empty to keep Emby's own background.</div>
                            </div>
                        </div>

                        <div style="margin-top:15px;">
                            <p style="margin:0 0 8px 0; font-size:0.9em; font-weight:bold; opacity:0.7;">Collection Thumb</p>
                            ${buildCollStylePickerHtml('thumb', tagConfig.CollectionThumbStyle, 'collection', tagConfig.CollectionThumbOptions, tagConfig.CollectionImported && !tagConfig.CollectionThumbReplaced)}
                            ${collImageUploadHtml('coll-thumb-upload', tagConfig.CollectionThumbStyle, tagConfig.CollectionThumbPath, 'Thumb', 'thumb.jpg', "Leave empty to keep Emby's own thumb. Wide 16:9 image, used where Emby shows items as thumbs.", 'max-width:240px; max-height:135px;')}
                        </div>

                        <div style="margin-top:15px;">
                            <p style="margin:0 0 8px 0; font-size:0.9em; font-weight:bold; opacity:0.7;">Collection Banner</p>
                            ${buildCollStylePickerHtml('banner', tagConfig.CollectionBannerStyle, 'collection', '', tagConfig.CollectionImported && !tagConfig.CollectionBannerReplaced)}
                            ${collImageUploadHtml('coll-banner-upload', tagConfig.CollectionBannerStyle, tagConfig.CollectionBannerPath, 'Banner', 'banner.jpg', "Leave empty to keep Emby's own banner. Very wide image (about 1000 × 185), used where Emby shows items as banners.", 'max-width:300px; max-height:56px;')}
                        </div>

                        <div class="inputContainer coll-art-title-row" style="margin-top:15px; display:${isGenArtStyle(tagConfig.CollectionPosterStyle) || isGenArtStyle(tagConfig.CollectionBackgroundStyle) || isGenArtStyle(tagConfig.CollectionThumbStyle) ? 'block' : 'none'};">
                            <input is="emby-input" type="text" class="txtCollArtTitle" label="Title on the art" value="${(tagConfig.CollectionArtTitle || '').replace(/"/g, '&quot;')}" placeholder="{name}" />
                            <div class="fieldDescription coll-art-title-note">Leave empty to use the collection name. <b>{name}</b> = collection name, <b>{week}</b> = this week's number, e.g. <i>{name} {week}</i> → "Top Movies for the Week 41".</div>
                        </div>

                        <div class="coll-art-preview-row" style="margin-top:10px; display:${isGenArtStyle(tagConfig.CollectionPosterStyle) || isGenArtStyle(tagConfig.CollectionBackgroundStyle) || isGenArtStyle(tagConfig.CollectionThumbStyle) ? 'block' : 'none'};">
                            <button type="button" is="emby-button" class="btnCollArtPreview raised" style="background:transparent; border:1px solid rgba(128,128,128,0.35); color:var(--theme-text-secondary); font-size:0.82em; padding:0 10px; min-width:0;"><i class="md-icon" style="font-size:1em; margin-right:4px;">visibility</i><span>Preview art</span></button>
                        </div>

                        <div class="ranked-only hsc-adv" data-adv="check">
                        <div class="checkboxContainer" style="margin-top:15px;">
                            <label>
                                <input is="emby-checkbox" type="checkbox" class="chkCollKeepOrder" ${tagConfig.CollectionKeepOrder ? 'checked' : ''} />
                                <span>Keep the list order — movies only (experimental)${ADV_BADGE}</span>
                            </label>
                        </div>
                        <div style="display:flex; align-items:flex-start; gap:8px; background:rgba(230,193,90,0.1); border:1px solid rgba(230,193,90,0.35); border-radius:6px; padding:12px; font-size:0.85em; line-height:1.5; margin-top:6px; margin-bottom:6px;">
                            <i class="md-icon" style="font-size:1.1em; color:var(--hsc-warning-text); flex-shrink:0; margin-top:1px;">warning</i>
                            <span><strong>Experimental:</strong> adds a hidden library "&lt;collection&gt; (ranked)" with .strm copies of the movies, merged with the real films as extra versions. Unticking removes them.</span>
                        </div>
                        </div>
                    </div>
                    </div>
                </div>

                <div class="tab-content playlist-tab" style="display:none;"
                    data-pl-userids="${playlistUserIdsEnc}"
                    data-pl-mappings="${playlistMappingsEnc}"
                    data-pl-loaded="0">
                    <div class="checkboxContainer checkboxContainer-withDescription">
                        <label>
                            <input is="emby-checkbox" type="checkbox" class="chkEnablePlaylist" ${enablePlaylist} />
                            <span>Create Playlist</span>
                        </label>
                        <div class="fieldDescription pl-enable-desc">Automatically create and maintain an individual Emby Playlist for each selected user.</div>
                    </div>
                    <div class="playlist-settings" style="margin-left: 20px; padding-left: 15px; border-left: 2px solid var(--line-color); margin-top: 10px; display: ${tagConfig.EnablePlaylist ? 'block' : 'none'};">
                        <div class="inputContainer pl-name-box">
                            <input is="emby-input" type="text" class="txtPlaylistName" label="Playlist Name" value="${playlistName}" placeholder="${labelName}" />
                            <div class="fieldDescription">Leave empty to use Display Name.</div>
                        </div>
                        ${getOutputTagHtml('pl', tagConfig.EnablePlaylistTag, tagConfig.PlaylistTag, tagConfig.PlaylistExtraTags)}
                        <div class="pl-users-box" style="margin-top:12px;">
                            <p style="margin:0 0 8px 0; font-size:0.9em; font-weight:bold; opacity:0.7;">Target Users</p>
                            <div class="playlist-user-list"><em style="opacity:0.5">Loading users...</em></div>
                        </div>
                        ${buildPlaylistLookHtml(tagConfig)}
                    </div>
                </div>

                <div class="tab-content advanced-tab" style="display:none;">
                    <div class="inputContainer">
                        <p style="margin:0 0 5px 0; font-size:0.9em; font-weight:bold; opacity:0.7;">Blacklist / Ignore (IMDB IDs)</p>
                        <textarea class="txtTagBlacklist" rows="2" placeholder="tt1234567&#10;tt9876543" style="width:100%;resize:none;overflow:hidden;padding:6px 8px;font-size:inherit;font-family:inherit;background:var(--plugin-input-bg,rgba(255,255,255,0.08));border:1px solid var(--plugin-input-border,rgba(255,255,255,0.2));border-radius:3px;color:inherit;line-height:1.4;min-height:44px;max-height:120px;overflow-y:auto;">${blacklist}</textarea>
                        <div class="fieldDescription">Items with these IDs will never be tagged or added to collection.</div>
                    </div>
                </div>

                <div class="tab-content homescreen-tab${topListOn ? ' tl-mode' : ''}${tagConfig.EnableTag === false && !tagConfig.EnableCollection && !tagConfig.EnablePlaylist && !topListOn ? ' hse-empty' : ''}" style="display:none;"
                    data-hse-libraryid="${homeSectionLibraryId}"
                    data-hse-userids="${homeSectionUserIdsEnc}"
                    data-hse-settings="${homeSectionSettingsEnc}"
                    data-hse-orig-name="${homeSectionOrigNameEnc}"
                    data-hse-tracked="${homeSectionTrackedEnc}"
                    data-hse-default-type="${hsDefaultSectionType}"
                    data-hse-loaded="0">
                    <div class="checkboxContainer checkboxContainer-withDescription">
                        <label>
                            <input is="emby-checkbox" class="chkEnableHomeSection" type="checkbox" ${enableHomeSection} />
                            <span>Add as home screen section</span>
                        </label>
                        <div class="fieldDescription">A home screen section will be managed for selected users each time sync runs.</div>
                        <div class="hse-disabled-hint" style="font-size:0.9em; color:var(--hsc-danger-text); margin-top:4px; display:${(enableHomeSection && tagConfig.EnableTag === false && !tagConfig.EnableCollection && !tagConfig.EnablePlaylist && !tagConfig.EnableTopList) ? 'block' : 'none'};">A plain row needs <strong>Apply Tag</strong>, <strong>Create Collection</strong> or <strong>Create Playlist</strong>, or tick <strong>Create as top-list</strong> below. Untick <em>Add as home screen section</em> if you do not want a row.</div>
                    </div>
                    <div class="hse-details" style="display:${enableHomeSection ? 'block' : 'none'}; margin-top:15px;">
                        ${buildTopListTabHtml(tagConfig)}
                        <div style="margin-bottom:15px;">
                            <p style="margin:0 0 8px 0; font-size:0.9em; font-weight:bold; opacity:0.7;">Target Users</p>
                            <div class="hse-user-list-inner"><em style="opacity:0.5">Loading users...</em></div>
                        </div>
                        <div>
                            <p style="margin:0 0 8px 0; font-size:0.9em; font-weight:bold; opacity:0.7;">Section Settings</p>
                            <div class="hse-fields-inner"><em style="opacity:0.5">Loading settings...</em></div>
                        </div>
                    </div>
                </div>

                <div style="display:flex; justify-content:flex-end; align-items:center; gap:8px; margin-top:20px; border-top:1px solid var(--line-color); padding-top:10px;">
                    <button is="emby-button" type="button" class="raised button-submit btnRunEntry hsc-btn-activity"><i class="md-icon" style="margin-right:5px;">play_arrow</i><span class="btnRunEntryLabel">Run Group</span></button>
                    <button is="emby-button" type="button" class="raised btnRemoveGroup hsc-btn-danger"><i class="md-icon" style="margin-right:5px;">delete</i>Remove Group</button>
                </div>
            </div>
        </div>`;

        if (afterRef) afterRef.insertAdjacentHTML('afterend', html);
        else if (prepend) container.insertAdjacentHTML('afterbegin', html);
        else container.insertAdjacentHTML('beforeend', html);

        var newRow = afterRef ? afterRef.nextElementSibling : (prepend ? container.firstElementChild : container.lastElementChild);
        setupRowEvents(newRow);
        initManualEditor(newRow);
        updateNextWatchWarning(newRow);
        updatePlaylistDescPlaceholder(newRow);
        refreshAdvanced(newRow);
        // Auto-size any pre-filled growing textareas
        newRow.querySelectorAll('textarea.txtMiValue, textarea.txtTagBlacklist').forEach(function (ta) {
            ta.style.height = 'auto';
            ta.style.height = Math.min(ta.scrollHeight, 120) + 'px';
            ta.style.overflowY = ta.scrollHeight > 120 ? 'auto' : 'hidden';
        });

        if (isNew) {
            setTimeout(() => { newRow.classList.remove('just-added'); }, 2000);
        }
    }

    // ── Create as top-list (top of the Home Screen tab: the source's home row as a ranked top-list) ──
    // The source is where the list is set: the server derives its TopLists entry on Save
    // (SourceTopLists.cs) and the next run makes the library, copies and rows. Its own fields
    // are content type, max items and the badges; users, title, position and look are the Home
    // Screen tab's, whose fields the top-list sets are hidden (tl-mode). Not for Your Next Watch.
    function buildTopListTabHtml(tc) {
        var on = !!tc.EnableTopList;
        // A list that is off starts as Auto when ticked (Movies was only the old default).
        var content = on && (tc.TopListContentType === 'Shows' || tc.TopListContentType === 'Movies') ? tc.TopListContentType : 'Auto';
        var enc = function (v) { return encodeURIComponent(v || ''); };
        return '<div class="toplist-tab hse-nw-hide" style="margin-bottom:18px;padding-bottom:14px;border-bottom:1px solid var(--line-color);"' +
            ' data-tl-badge="' + enc(tc.TopListBadgeStyle || 'neutral') + '"' +
            ' data-tl-badgeopts="' + enc(tc.TopListBadgeOptions) + '"' +
            ' data-tl-rp="' + enc(tc.TopListRealPoster) + '" data-tl-loaded="0">' +
            '<div class="checkboxContainer checkboxContainer-withDescription">' +
                '<label><input is="emby-checkbox" type="checkbox" class="chkEnableTopList"' + (on ? ' checked' : '') + ' /><span>Create as top-list</span></label>' +
                '<div class="fieldDescription">The row becomes a ranked top-list: the titles in list order with numbered posters, from a hidden library of its own. The users, title, position and look below are used for it.</div>' +
                '<div class="tl-req" style="display:none;font-size:0.9em;margin-top:6px;"></div>' +
                '<div class="tl-tagnote fieldDescription" style="display:none;margin-top:6px;"><i class="md-icon" style="font-size:1.15em;vertical-align:-3px;margin-right:4px;color:var(--hsc-info-text);">info</i>Apply Tag was switched off: the top-list does not need it. Switch it back on in the <a href="javascript:void(0)" class="tl-req-link help-link" data-goto="tag">Tag tab</a> if you want the tag on the titles in Emby too.</div>' +
                '<div class="tl-note fieldDescription" style="display:none;color:var(--hsc-warning-text);margin-top:6px;"></div>' +
            '</div>' +
            '<div class="tl-settings" style="margin-left:20px;padding-left:15px;border-left:2px solid var(--line-color);margin-top:10px;display:' + (on ? 'block' : 'none') + ';">' +
                '<div style="margin-bottom:12px;"><label class="selectLabel">Content</label>' +
                    '<select is="emby-select" class="selTopListContent" style="width:100%;">' +
                        '<option value="Auto"' + (content === 'Auto' ? ' selected' : '') + '>Auto (based on the source)</option>' +
                        '<option value="Movies"' + (content === 'Movies' ? ' selected' : '') + '>Movies</option>' +
                        '<option value="Shows"' + (content === 'Shows' ? ' selected' : '') + '>TV Shows</option>' +
                    '</select>' +
                    '<div class="fieldDescription">Only titles of this kind are ranked. Switching it later makes the list again (a new library).</div>' +
                '</div>' +
                '<div style="display:flex;align-items:center;gap:12px;margin-bottom:14px;flex-wrap:wrap;">' +
                    '<label style="font-size:0.9em;white-space:nowrap;margin:0;">Max items</label>' +
                    '<input is="emby-input" class="txtTopListMax" type="number" value="' + (Math.max(0, tc.TopListMaxItems || 0)) + '" min="0" style="width:90px;" />' +
                    '<span style="font-size:0.8em;opacity:0.5;">0 = all</span>' +
                '</div>' +
                '<div class="tl-badge-host"><em style="opacity:0.5">Loading badges...</em></div>' +
                '<div class="hsc-callout warning" style="margin-top:6px;"><i class="md-icon">warning</i>' +
                    '<span><strong>Library access is not checked per title.</strong> A top-list gets a library of its own, and every user it is shown to gets access to that library. Its titles can come from any library on the server, so those users can see and play them even from libraries they otherwise have no access to.</span>' +
                '</div>' +
            '</div>' +
            '</div>';
    }

    function hseUserIdsOf(row) {
        var hseTab = row.querySelector('.homescreen-tab');
        if (!hseTab) return [];
        if (hseTab.dataset.hseLoaded === '1')
            return Array.from(hseTab.querySelectorAll('.chkHseUser:checked')).map(function (c) { return c.value; });
        try { return JSON.parse(decodeURIComponent(hseTab.dataset.hseUserids || '%5B%5D')) || []; } catch (e) { return []; }
    }

    // 'Auto' (movies or TV shows by the source's titles, decided by each run), 'Movies' or 'Shows'.
    function topListContentOf(row) {
        var v = (row.querySelector('.selTopListContent') || {}).value;
        return v === 'Shows' || v === 'Movies' ? v : 'Auto';
    }

    // The kind the list is now: as set, or for Auto what the server made it (movies until then).
    function topListKindOf(row) {
        var c = topListContentOf(row);
        if (c !== 'Auto') return c;
        var tag = ((row.querySelector('.txtTagName') || {}).value || (row.querySelector('.txtEntryLabel') || {}).value || '').trim().toLowerCase();
        return _topListTypes[tag] === 'Shows' ? 'Shows' : 'Movies';
    }

    // What the top-list still needs: [tab to open ('' = none), text].
    function topListMissing(row) {
        var miss = [];
        if ((row.querySelector('.selSourceType') || {}).value === 'NextWatch')
            return [['', 'Not available for <strong>Your Next Watch</strong>: its row is each user\'s own playlist.']];
        // Users are not needed to tick it: Save asks for them (topListsWithoutUsers).
        return miss;
    }

    // A source card changed on this page, or added: Save's checks apply to it. Cards left alone are
    // saved as they are (a home row with nothing to show goes off, as before these checks).
    function rowChangedHere(row) {
        return row.dataset.touched === '1' || row.classList.contains('just-added');
    }

    // Sources with Create as top-list on but no user picked (Save is stopped for them).
    function topListsWithoutUsers(view) {
        return Array.from(view.querySelectorAll('#tagListContainer .tag-row')).filter(function (row) {
            return row.querySelector('.selSourceType') && rowChangedHere(row) && (row.querySelector('.selSourceType') || {}).value !== 'NextWatch'
                && !!(row.querySelector('.chkEnableTopList') || {}).checked
                && !!(row.querySelector('.chkEnableHomeSection') || {}).checked
                && hseUserIdsOf(row).length === 0;
        });
    }

    function updateTopListTab(row) {
        var tab = row.querySelector('.toplist-tab');
        if (!tab) return;
        var chk = tab.querySelector('.chkEnableTopList');
        var on = chk.checked;
        var miss = topListMissing(row);
        // Can't be switched on before it has what it needs; once on it stays switchable off.
        chk.disabled = !on && miss.length > 0;
        var label = chk.closest('.checkboxContainer');
        if (label) label.style.opacity = chk.disabled ? '0.75' : '';

        var req = tab.querySelector('.tl-req');
        if (miss.length) {
            req.style.display = 'block';
            req.style.color = on ? 'var(--hsc-warning-text)' : 'var(--hsc-danger-text)';
            req.innerHTML = (on ? 'Still needs: ' : 'Needs: ') + '<ul style="margin:4px 0 0;padding-left:20px;line-height:1.5;">' +
                miss.map(function (m) {
                    var link = m[0] ? ' <a href="javascript:void(0)" class="tl-req-link help-link" data-goto="' + m[0] + '">Tag tab</a>' : '';
                    return '<li>' + m[1] + link + '</li>';
                }).join('') + '</ul>';
        } else {
            req.style.display = 'none';
            req.innerHTML = '';
        }

        // What Save changes for a list the server already has.
        var note = tab.querySelector('.tl-note');
        var saved = row.dataset.tlSaved === '1';
        var tagNow = (((row.querySelector('.txtTagName') || {}).value || (row.querySelector('.txtEntryLabel') || {}).value || '') + '').trim().toLowerCase();
        var text = '';
        if (saved && !on) text = 'Saving with this off removes the top-list at the next run (its rows, library and copies), and the plain row comes back (when the home screen section stays on).';
        else if (saved && on && row.dataset.tlSavedTag && tagNow && tagNow !== row.dataset.tlSavedTag)
            text = 'The name has changed (the Tag Name, or the Display Name when that is empty): the top-list is made again under the new name at the next run, and the old one (its rows and library) is removed.';
        note.textContent = text;
        note.style.display = text ? 'block' : 'none';

        tab.querySelector('.tl-settings').style.display = on ? 'block' : 'none';
        var hseTab = row.querySelector('.homescreen-tab');
        if (hseTab) { hseTab.classList.toggle('tl-mode', on); updateHseImageTypeState(hseTab); }
    }

    function topListBadgeHtml(row, values) {
        var shows = topListKindOf(row) === 'Shows';
        return buildBadgePickerHtml(values.style, values.options, values.realPoster, shows ? 'shows' : 'movies');
    }

    function topListBadgeValues(tab) {
        var host = tab.querySelector('.tl-badge-host');
        if (tab.dataset.tlLoaded === '1')
            return { style: readBadgeStyle(host), options: readBadgeOptions(host), realPoster: readRealPoster(host) };
        var dec = function (v) { try { return decodeURIComponent(v || ''); } catch (e) { return ''; } };
        return { style: dec(tab.dataset.tlBadge) || 'neutral', options: dec(tab.dataset.tlBadgeopts), realPoster: dec(tab.dataset.tlRp) };
    }

    function initTopListTab(row) {
        var tab = row.querySelector('.toplist-tab');
        if (!tab || tab.dataset.tlLoaded === '1') return;
        var host = tab.querySelector('.tl-badge-host');
        host.innerHTML = topListBadgeHtml(row, topListBadgeValues(tab));
        tab.dataset.tlLoaded = '1';
        initBadgePicker(host, ((row.querySelector('.txtTagName') || {}).value || '').trim());
        updateTopListTab(row);
    }

    // Movies ↔ TV Shows: the badge previews are drawn for the kind (the chosen look is kept).
    function onTopListContentChanged(row) {
        var tab = row.querySelector('.toplist-tab');
        if (!tab || tab.dataset.tlLoaded !== '1') return;
        var host = tab.querySelector('.tl-badge-host');
        var values = topListBadgeValues(tab);
        host.innerHTML = topListBadgeHtml(row, values);
        initBadgePicker(host, ((row.querySelector('.txtTagName') || {}).value || '').trim());
    }

    // Switched on: Apply Tag goes off (the list is the source's titles, not the tagged ones; the
    // tag can be switched back on).
    function onTopListToggled(row) {
        var chk = row.querySelector('.chkEnableTopList');
        var tagNote = row.querySelector('.tl-tagnote');
        var tagChk = row.querySelector('.chkEnableTag');
        if (!chk.checked) {
            if (tagNote) tagNote.style.display = 'none';
            // Unticked again: Apply Tag comes back if ticking it switched the tag off.
            if (row.dataset.tlTagWasOn === '1' && tagChk && !tagChk.checked) {
                tagChk.checked = true;
                tagChk.dispatchEvent(new Event('change', { bubbles: true }));
            }
            delete row.dataset.tlTagWasOn;
            return;
        }
        if (tagChk && tagChk.checked) {
            tagChk.checked = false;
            tagChk.dispatchEvent(new Event('change', { bubbles: true }));
            row.dataset.tlTagWasOn = '1';   // after the event: a change by hand forgets it
            if (tagNote) tagNote.style.display = 'block';
        }
    }

    // The tab's fields as saved (TagConfig.EnableTopList etc.).
    function readTopListTab(row) {
        var tab = row.querySelector('.toplist-tab');
        if (!tab) return {};
        var b = topListBadgeValues(tab);
        return {
            EnableTopList: !!(tab.querySelector('.chkEnableTopList') || {}).checked,
            TopListContentType: topListContentOf(row),
            TopListMaxItems: Math.max(0, parseInt((tab.querySelector('.txtTopListMax') || {}).value, 10) || 0),
            TopListBadgeStyle: b.style || 'neutral',
            TopListBadgeOptions: b.options || '',
            TopListRealPoster: b.realPoster || ''
        };
    }

    function refreshTopListBadges() {
        document.querySelectorAll('.tag-row').forEach(function (row) {
            var container = row.querySelector('.badge-container');
            if (!container || !row.querySelector('.selSourceType')) return;
            var existing = container.querySelector('.tag-indicator.toplist');
            if (!!(row.querySelector('.chkEnableTopList') || {}).checked) {
                if (!existing) {
                    var span = document.createElement('span');
                    span.className = 'tag-indicator toplist';
                    span.innerHTML = '<i class="md-icon" style="font-size:1.1em;">format_list_numbered</i> Top-List';
                    container.appendChild(span);
                }
            } else if (existing) {
                existing.remove();
            }
        });
    }

    function setupRowEvents(row) {
        function updateBadges(row) {
            var container = row.querySelector('.badge-container');
            if (!container) return;

            var hasSchedule = row.querySelectorAll('.date-row').length > 0;
            var hasCollection = row.querySelector('.chkEnableCollection').checked;
            var hasHomeSection = row.querySelector('.chkEnableHomeSection').checked;
            var hasTag = row.querySelector('.chkEnableTag').checked;
            var hasPlaylist = !!(row.querySelector('.chkEnablePlaylist') || {}).checked;
            var overrideChk = row.querySelector('.chkOverrideWhenActive');
            if (overrideChk) {
                overrideChk.disabled = !hasSchedule;
                if (!hasSchedule) overrideChk.checked = false;
                var overrideContainer = overrideChk.closest('.checkboxContainer');
                if (overrideContainer) overrideContainer.style.opacity = hasSchedule ? '' : '0.4';
            }
            var hasOverride = hasSchedule && !!(overrideChk || {}).checked;

            var sourceBadge = row.querySelector('.source-badge');
            if (sourceBadge) sourceBadge.innerHTML = getSourceBadgeHtml(row.querySelector('.selSourceType').value);

            var html = '';
            if (hasSchedule) {
                var schedPriorityClass = hasOverride ? ' priority-active' : '';
                var schedActiveClass = isScheduleCurrentlyActive(readIntervalsFromRow(row)) ? ' schedule-active' : '';
                var schedText = hasOverride ? 'Schedule priority' : 'Schedule';
                html += `<span class="tag-indicator schedule${schedPriorityClass}${schedActiveClass}"><i class="md-icon" style="font-size:1.1em;">calendar_today</i> ${schedText}</span>`;
            }
            if (hasCollection) {
                html += `<span class="tag-indicator collection"><i class="md-icon" style="font-size:1.1em;">library_books</i> Collection</span>`;
            }
            if (hasCollection && row.dataset.collImported === '1') {
                html += `<span class="tag-indicator imported"><i class="md-icon" style="font-size:1.1em;">move_to_inbox</i> Imported</span>`;
            }
            if (hasHomeSection) {
                html += `<span class="tag-indicator homescreen"><i class="md-icon" style="font-size:1.1em;">home</i> Home Section</span>`;
            }
            if (hasTag) {
                html += `<span class="tag-indicator tag"><i class="md-icon" style="font-size:1.1em;">label</i> Tag</span>`;
            }
            if (hasPlaylist) {
                html += `<span class="tag-indicator playlist"><i class="md-icon" style="font-size:1.1em;">queue_music</i> Playlist</span>`;
            }
            if (!!(row.querySelector('.chkEnableTopList') || {}).checked) {
                html += `<span class="tag-indicator toplist"><i class="md-icon" style="font-size:1.1em;">format_list_numbered</i> Top-List</span>`;
            }
            try { html += lastRunChipHtml(JSON.parse(decodeURIComponent(row.dataset.lastRuns || '%5B%5D')), rowToFlatTags(row, true)); } catch (e) { }
            container.innerHTML = html;
        }

        row.querySelectorAll('.tag-tab').forEach(tab => {
            tab.addEventListener('click', function () {
                row.querySelectorAll('.tag-tab').forEach(t => t.classList.remove('active'));
                this.classList.add('active');
                var target = this.getAttribute('data-tab');
                row.dataset.activeTab = target;
                row.querySelector('.general-tab').style.display = target === 'general' ? 'block' : 'none';
                row.querySelector('.tagname-tab').style.display = target === 'tag' ? 'block' : 'none';
                row.querySelector('.schedule-tab').style.display = target === 'schedule' ? 'block' : 'none';
                row.querySelector('.collection-tab').style.display = target === 'collection' ? 'block' : 'none';
                row.querySelector('.advanced-tab').style.display = target === 'advanced' ? 'block' : 'none';
                row.querySelector('.homescreen-tab').style.display = target === 'homescreen' ? 'block' : 'none';
                row.querySelector('.playlist-tab').style.display = target === 'playlist' ? 'block' : 'none';
                if (target === 'homescreen') { initHomeSectionTab(row); initTopListTab(row); }
                if (target === 'playlist') initPlaylistTab(row);
                var activeTabEl = row.querySelector('.' + target + '-tab');
                if (activeTabEl) activeTabEl.querySelectorAll('textarea.txtMiValue, textarea.txtTagBlacklist').forEach(function(ta) {
                    ta.style.height = 'auto';
                    ta.style.height = Math.min(ta.scrollHeight, 120) + 'px';
                    ta.style.overflowY = ta.scrollHeight > 120 ? 'auto' : 'hidden';
                });
            });
        });

        // Create as top-list: what it needs, the Home Screen tab's top-list mode and the chip follow
        // every change in the card (users, tag name...).
        // Changed on this page: Save's checks (a home row with nothing to show, a top-list without
        // users) apply to it; a card left alone is saved as it is.
        row.addEventListener('change', function () { row.dataset.touched = '1'; });
        row.addEventListener('input', function () { row.dataset.touched = '1'; });
        row.addEventListener('change', function (e) {
            if (e.target.classList.contains('chkEnableTopList')) { onTopListToggled(row); updateHseSectionAvailability(row); }
            if (e.target.classList.contains('selTopListContent')) onTopListContentChanged(row);
            updateTopListTab(row);
            if (e.target.classList.contains('chkEnableTopList')) updateBadges(row);
        });
        row.addEventListener('input', function (e) {
            if (e.target.classList.contains('txtTagName') || e.target.classList.contains('txtEntryLabel')) updateTopListTab(row);
        });
        row.addEventListener('click', function (e) {
            var link = e.target.closest('.tl-req-link');
            if (!link || !row.contains(link)) return;
            e.preventDefault();
            var tab = row.querySelector('.tag-tab[data-tab="' + link.dataset.goto + '"]');
            if (tab) tab.click();
        });
        updateTopListTab(row);

        // A changed Max items hides the "9 of 10" chip until the next run.
        row.addEventListener('input', e => {
            if (/Limit$/.test(e.target.className.split(' ').filter(function (c) { return c.indexOf('txt') === 0; })[0] || '')) updateBadges(row);
        });

        row.addEventListener('change', e => {
            if (e.target.classList.contains('selSourceType')) {
                var type = e.target.value;
                // Your Next Watch has no top-list (its row is each user's own playlist; the box is hidden).
                var tlBox = row.querySelector('.chkEnableTopList');
                if (type === 'NextWatch' && tlBox && tlBox.checked) {
                    tlBox.checked = false;
                    tlBox.dispatchEvent(new Event('change', { bubbles: true }));
                }
                // Another kind of source is no longer the imported collection (saved with Save);
                // switching back before saving makes it imported again.
                if (row.dataset.importedSt) {
                    row.dataset.collImported = type === row.dataset.importedSt ? '1' : '0';
                    updateBadges(row);
                }
                row.querySelector('.source-external-container').style.display = type === 'External' ? 'block' : 'none';
                row.querySelector('.source-local-container').style.display = (type === 'LocalCollection' || type === 'LocalPlaylist') ? 'block' : 'none';
                row.querySelector('.source-mediainfo-container').style.display = (type && type !== '') ? 'block' : 'none';
                row.querySelector('.source-ai-container').style.display = type === 'AI' ? 'block' : 'none';
                row.querySelector('.source-manual-container').style.display = type === 'Manual' ? 'block' : 'none';

                var _hint = row.querySelector('.source-type-hint');
                if (_hint) {
                    var _hints = {
                        'External':         'Use an external list to tag, or create a collection, from the items that match your library.',
                        'LocalCollection':  'Every item in the selected collection(s) gets the configured tag or is added to a new collection. You can also use this to create a curated list of selected collections as a home screen section.',
                        'LocalPlaylist':    'Every item in the selected playlist(s) gets the configured tag or is added to a new collection.',
                        'Manual':           'Pick movies and shows by hand. Search to add them, use the arrows to set the order, and remove them with ✕.',
                        'MediaInfo':        'Filter your own library to select which movies or shows to tag or create a collection of. This is also known as a Smart Playlist.',
                        'AI':               'Use AI to create a list. Write your prompt and the AI will build a list based on it.',
                        'NextWatch':        NEXT_WATCH_HINT
                    };
                    _hint.textContent = _hints[type] || '';
                }

                var isMi = type === 'MediaInfo';
                var miLimitRow      = row.querySelector('.mi-limit-row');
                var miToggleRow     = row.querySelector('.mi-toggle-row');
                var miFilterBody    = row.querySelector('.mi-filter-body');
                var miHelpBtnRow    = row.querySelector('.mi-help-btn-row');
                var miPresetsSection = row.querySelector('.mi-presets-section');
                if (miPresetsSection) miPresetsSection.style.display = isMi ? 'block' : 'none';
                if (miLimitRow) miLimitRow.style.display = isMi || type === 'NextWatch' ? 'flex' : 'none';
                var miLimitHelp = row.querySelector('.btnMiLimitHelp');
                if (miLimitHelp) miLimitHelp.style.display = isMi ? '' : 'none';
                var nwSettings = row.querySelector('.nw-settings');
                if (nwSettings) nwSettings.style.display = type === 'NextWatch' ? 'block' : 'none';
                var miLimitInput = row.querySelector('.txtMediaInfoLimit');
                if (type === 'NextWatch' && miLimitInput && !(parseInt(miLimitInput.value, 10) > 0)) miLimitInput.value = '50';
                if (type === 'NextWatch') {
                    // Default for a Your Next Watch source (can be changed): its picks go to each
                    // user's own playlist, so Create Playlist starts on.
                    var _plChk = row.querySelector('.chkEnablePlaylist');
                    if (_plChk && !_plChk.checked) { _plChk.checked = true; _plChk.dispatchEvent(new Event('change', { bubbles: true })); }
                    // A tag or collection is shared by everyone (not each user's own picks), so
                    // Apply Tag and Create Collection start off. They can be turned back on.
                    ['.chkEnableTag', '.chkEnableCollection'].forEach(function (sel) {
                        var _c = row.querySelector(sel);
                        if (_c && _c.checked) { _c.checked = false; _c.dispatchEvent(new Event('change', { bubbles: true })); }
                    });
                    // A source made Your Next Watch here needs no move of older users.
                    row.dataset.nwMigrated = '1';
                }
                updateNextWatchWarning(row);
                updatePlaylistDescPlaceholder(row);
                var _hseTab = row.querySelector('.homescreen-tab');
                if (_hseTab && _hseTab.dataset.hseLoaded === '1') { updateHseItemsOnlyVisibility(_hseTab); updateHseImageTypeState(_hseTab); }
                if (miHelpBtnRow) miHelpBtnRow.style.display = isMi ? 'none' : 'flex';
                if (isMi) {
                    if (miToggleRow)  miToggleRow.style.display  = 'none';
                    if (miFilterBody) miFilterBody.style.display = 'block';
                } else if (type) {
                    // Always collapse filters when switching away from MediaInfo — user must open manually
                    if (miToggleRow)  miToggleRow.style.display  = 'block';
                    if (miFilterBody) miFilterBody.style.display = 'none';
                }
                if (type) {
                    var miList = row.querySelector('.mediainfo-filter-list');
                    if (miList && isMi && miList.querySelectorAll('.mediainfo-filter-group').length === 0) {
                        miList.insertAdjacentHTML('beforeend', getMediaInfoFilterGroupHtml({ Operator: 'AND', Criteria: [], GroupOperator: 'AND' }, 0, true));
                    }
                }

                if (type === 'LocalCollection' || type === 'LocalPlaylist') {
                    row.querySelector('.local-list-container').innerHTML = getLocalRowHtml(type, "", 0);
                    row.querySelector('.local-type-label').textContent = type === 'LocalPlaylist' ? "Select Playlists" : "Select Collections";
                }
                updateBadges(row);
            }
            
            if (e.target.classList.contains('selMiProperty')) {
                var miRule = e.target.closest('.mi-rule');
                miRule.querySelector('.mi-value-wrapper').innerHTML = getMiValueHtml(e.target.value, '', '');
                var existingHint = miRule.querySelector('.mi-rule-hint');
                if (existingHint) existingHint.outerHTML = getMiHintHtml(e.target.value);
            }
            if (e.target.classList.contains('selMiValue')) {
                var _miRule = e.target.closest('.mi-rule');
                if (_miRule) {
                    var _prop = (_miRule.querySelector('.selMiProperty') || {}).value;
                    if (_prop === 'MediaType') {
                        var _incParent = _miRule.querySelector('.mi-include-parent');
                        if (_incParent) _incParent.style.display = e.target.value === 'Episode' ? 'inline-flex' : 'none';
                    }
                }
            }
            if (e.target.classList.contains('selDateType')) {
                var dateRow = e.target.closest('.date-row');
                var type = e.target.value;
                dateRow.querySelector('.inputs-specific').style.display = type === 'SpecificDate' ? 'flex' : 'none';
                dateRow.querySelector('.inputs-annual').style.display = type === 'EveryYear' ? 'flex' : 'none';
                dateRow.querySelector('.inputs-weekly').style.display = type === 'Weekly' ? 'flex' : 'none';
                dateRow.querySelector('.inputs-time').style.display = type === 'TimeOfDay' ? 'flex' : 'none';
                dateRow.querySelector('.inputs-moving').style.display = isMovingType(type) ? 'flex' : 'none';
                // Easter, Advent and Friday the 13th are their own dates: only the days before/after apply.
                dateRow.querySelector('.mov-wd-month').style.display = type === 'MovingDate' ? 'flex' : 'none';
                dateRow.querySelector('.mov-hint').textContent = movingHint(type);
            }
            if (e.target.classList.contains('selStartMonth') || e.target.classList.contains('selEndMonth')) {
                var isStart = e.target.classList.contains('selStartMonth');
                var dateRow = e.target.closest('.date-row');
                var month = parseInt(e.target.value, 10);
                var maxDay = getMaxDays(month);
                var daySelect = dateRow.querySelector(isStart ? '.selStartDay' : '.selEndDay');
                var currentDay = Math.min(parseInt(daySelect.value, 10), maxDay);
                daySelect.innerHTML = getDayOptions(currentDay, maxDay);
            }
            setTimeout(checkFormState, 0);
        });

        var header = row.querySelector('.tag-header'), body = row.querySelector('.tag-body'), icon = row.querySelector('.expand-icon');
        header.addEventListener('click', e => {
            if (e.target.closest('.header-actions')) return;
            var isHidden = body.style.display === 'none';
            body.style.display = isHidden ? 'block' : 'none';
            icon.innerText = isHidden ? 'expand_less' : 'expand_more';
            if (isHidden) {
                body.querySelectorAll('textarea.txtMiValue, textarea.txtTagBlacklist').forEach(function(ta) {
                    ta.style.height = 'auto';
                    ta.style.height = Math.min(ta.scrollHeight, 120) + 'px';
                    ta.style.overflowY = ta.scrollHeight > 120 ? 'auto' : 'hidden';
                });
            }
        });

        var chk = row.querySelector('.chkTagActive'), lblStatus = row.querySelector('.lblActiveStatus');
        function updateRunGroupBtn() {
            var runBtn = row.querySelector('.btnRunEntry');
            if (!runBtn) return;
            var active = chk.checked;
            runBtn.disabled = !active;
            runBtn.style.opacity = active ? '1' : '0.4';
        }
        chk.addEventListener('change', function () {
            lblStatus.textContent = this.checked ? "Active" : "Disabled";
            lblStatus.style.color = this.checked ? "var(--hsc-primary-text)" : "var(--hsc-neutral)";
            if (this.checked) row.classList.remove('inactive');
            else row.classList.add('inactive');
            updateRunGroupBtn();
        });
        updateRunGroupBtn();

        row.querySelector('.chkEnableTag').addEventListener('change', function () {
            delete row.dataset.tlTagWasOn;   // the user's own choice wins over Create as top-list's
            // Switched back on: the note that it was switched off is no longer true.
            var tlTagNote = row.querySelector('.tl-tagnote');
            if (this.checked && tlTagNote) tlTagNote.style.display = 'none';
            row.querySelector('.tag-settings').style.display = this.checked ? 'block' : 'none';
            updateBadges(row);
            updateHseSectionAvailability(row);
            updateNextWatchWarning(row);
        });

        row.querySelector('.chkEnableCollection').addEventListener('change', function () {
            row.querySelector('.collection-settings').style.display = this.checked ? 'block' : 'none';
            updateBadges(row);
            updateHseSectionAvailability(row);
            updateNextWatchWarning(row);
        });

        row.querySelector('.chkEnablePlaylist').addEventListener('change', function () {
            row.querySelector('.playlist-settings').style.display = this.checked ? 'block' : 'none';
            updateBadges(row);
            updateHseSectionAvailability(row);
        });

        row.querySelectorAll('.chkOutTag').forEach(function (chk) {
            chk.addEventListener('change', function () {
                chk.closest('.out-tag-box').querySelector('.out-tag-field').style.display = chk.checked ? 'block' : 'none';
            });
        });
        row.querySelectorAll('.btnAddOutTag').forEach(function (btn) {
            btn.addEventListener('click', function () {
                var list = btn.closest('.out-tag-box').querySelector('.out-extra-tag-list');
                list.insertAdjacentHTML('beforeend', getOutExtraTagRowHtml(''));
                var input = list.lastElementChild.querySelector('.txtOutExtraTag');
                if (input) input.focus();
            });
        });

        row.querySelector('.chkOverrideWhenActive').addEventListener('change', function () {
            updateBadges(row);
        });

        row.querySelector('.chkEnableHomeSection').addEventListener('change', function () {
            var hseDetails = row.querySelector('.hse-details');
            if (hseDetails) hseDetails.style.display = this.checked ? 'block' : 'none';
            // The top-list is part of the home screen section: off with it.
            var tlChk = row.querySelector('.chkEnableTopList');
            if (!this.checked && tlChk && tlChk.checked) {
                tlChk.checked = false;
                tlChk.dispatchEvent(new Event('change', { bubbles: true }));
            }
            updateHseSectionAvailability(row);
            updateBadges(row);
        });

        var chkAiWatched = row.querySelector('.chkAiRecentlyWatched');
        if (chkAiWatched) {
            chkAiWatched.addEventListener('change', function () {
                var opts = row.querySelector('.ai-recently-watched-options');
                if (opts) opts.style.display = this.checked ? 'block' : 'none';
            });
        }

        var selAiProv = row.querySelector('.selAiProvider');
        if (selAiProv) {
            selAiProv.addEventListener('change', function () {
                var warn = row.querySelector('.ollama-experimental-warning');
                if (warn) warn.style.display = this.value === 'Ollama' ? 'flex' : 'none';
                updateAiProviderMissing(row);
            });
        }

        row.querySelector('.btnAddUrl').addEventListener('click', () => {
            row.querySelector('.url-list-container').insertAdjacentHTML('beforeend', getUrlRowHtml('', 0));
        });

        row.querySelector('.btnAddExtraTag').addEventListener('click', () => {
            var list = row.querySelector('.extra-tag-list');
            list.insertAdjacentHTML('beforeend', getExtraTagRowHtml(''));
            var input = list.lastElementChild.querySelector('.txtExtraTag');
            if (input) input.focus();
        });

        row.querySelector('.btnAddLocal').addEventListener('click', () => {
            var st = row.querySelector('.selSourceType').value;
            row.querySelector('.local-list-container').insertAdjacentHTML('beforeend', getLocalRowHtml(st, "", 0));
        });

        row.querySelector('.btnAddDate').addEventListener('click', () => {
            row.querySelector('.date-list-container').insertAdjacentHTML('beforeend', getDateRowHtml({ Type: 'SpecificDate' }));
            updateBadges(row);
        });

        row.querySelector('.btnTagTargetHelp').addEventListener('click', () => {
            (row.closest('#HomeScreenCompanionConfigPage') || activeView()).querySelector('#tagTargetHelpModalOverlay').classList.add('modal-visible');
        });

        row.querySelector('.btnCollTargetHelp').addEventListener('click', () => {
            (row.closest('#HomeScreenCompanionConfigPage') || activeView()).querySelector('#tagTargetHelpModalOverlay').classList.add('modal-visible');
        });

        row.addEventListener('click', e => {
            if (e.target.closest('.btnMiHelp')) {
                (row.closest('#HomeScreenCompanionConfigPage') || activeView()).querySelector('#miHelpModalOverlay').classList.add('modal-visible');
                return;
            }
            if (e.target.closest('.btnMiPreview')) {
                showSourcePreview(row, e.target.closest('.btnMiPreview'));
                return;
            }
            if (e.target.closest('.btnToggleAdditionalFilters')) {
                row.querySelector('.mi-toggle-row').style.display  = 'none';
                row.querySelector('.mi-filter-body').style.display = 'block';
                var miList = row.querySelector('.mediainfo-filter-list');
                if (miList && miList.querySelectorAll('.mediainfo-filter-group').length === 0) {
                    miList.insertAdjacentHTML('beforeend', getMediaInfoFilterGroupHtml({ Operator: 'AND', Criteria: [], GroupOperator: 'AND' }, 0, true));
                }
                return;
            }
            if (e.target.closest('.btnRemoveUrl')) {
                e.target.closest('.url-row').remove();
            }

            if (e.target.closest('.btnRemoveExtraTag')) {
                e.target.closest('.extra-tag-row').remove();
            }

            if (e.target.closest('.btnRemoveLocal')) {
                e.target.closest('.local-row').remove();
            }

            if (e.target.closest('.btnRemoveDate')) {
                e.target.closest('.date-row').remove();
                updateBadges(row);
            }

            if (e.target.closest('.btnSchedulePresets')) {
                var spBtn = e.target.closest('.btnSchedulePresets');
                var spPanel = row.querySelector('.schedule-preset-panel');
                var spOpen = spPanel.style.display === 'none';
                spPanel.style.display = spOpen ? '' : 'none';
                var spIcon = spBtn.querySelector('.mi-expand-icon');
                if (spIcon) spIcon.style.transform = spOpen ? 'rotate(180deg)' : '';
                return;
            }
            if (e.target.closest('.btnApplySchedulePreset')) {
                var sp = SCHEDULE_PRESETS[+e.target.closest('.btnApplySchedulePreset').dataset.preset];
                row.querySelector('.date-list-container').insertAdjacentHTML('beforeend', getDateRowHtml(Object.assign({}, sp.interval)));
                row.querySelector('.schedule-preset-panel').style.display = 'none';
                var spIcon2 = row.querySelector('.btnSchedulePresets .mi-expand-icon');
                if (spIcon2) spIcon2.style.transform = '';
                updateBadges(row);
                setTimeout(checkFormState, 0);
                return;
            }

            if (e.target.closest('.btnPremadeFilters')) {
                var btn = e.target.closest('.btnPremadeFilters');
                var panel = btn.closest('.source-mediainfo-container').querySelector('.mi-preset-panel');
                var open = panel.style.display === 'none';
                panel.style.display = open ? '' : 'none';
                var icon = btn.querySelector('.mi-expand-icon');
                if (icon) icon.style.transform = open ? 'rotate(180deg)' : '';
                return;
            }
            if (e.target.closest('.btnMySavedFilters')) {
                var btn = e.target.closest('.btnMySavedFilters');
                var savedPanel = btn.closest('.source-mediainfo-container').querySelector('.mi-saved-panel');
                var open = savedPanel.style.display === 'none';
                savedPanel.style.display = open ? '' : 'none';
                var icon = btn.querySelector('.mi-expand-icon');
                if (icon) icon.style.transform = open ? 'rotate(180deg)' : '';
                return;
            }
            if (e.target.closest('.btnConfirmSaveFilter')) {
                var miContainer = e.target.closest('.source-mediainfo-container');
                var nameInput = miContainer.querySelector('.txtSaveFilterName');
                var name = nameInput.value.trim();
                if (!name) { nameInput.focus(); return; }
                var filters = readMiFiltersFromContainer(miContainer);
                if (filters.length === 0) { nameInput.focus(); return; }
                savedFilters.push({ Name: name, Filters: filters });
                nameInput.value = '';
                refreshMySavedFiltersPanels();
                saveSavedFiltersNow();
                return;
            }
            if (e.target.closest('.btnApplyMySavedFilter')) {
                var idx = parseInt(e.target.closest('.btnApplyMySavedFilter').dataset.index, 10);
                var sf = savedFilters[idx];
                var savedList = e.target.closest('.source-mediainfo-container').querySelector('.mediainfo-filter-list');
                savedList.innerHTML = sf.Filters.map(function(f, i) { return getMediaInfoFilterGroupHtml(f, i, i === 0); }).join('');
                e.target.closest('.mi-saved-panel').style.display = 'none';
                setTimeout(checkFormState, 0);
                return;
            }
            if (e.target.closest('.btnDeleteMySavedFilter')) {
                var idx = parseInt(e.target.closest('.btnDeleteMySavedFilter').dataset.index, 10);
                savedFilters.splice(idx, 1);
                refreshMySavedFiltersPanels();
                saveSavedFiltersNow();
                return;
            }
            if (e.target.closest('.btnApplyMiPreset')) {
                var idxParts = e.target.closest('.btnApplyMiPreset').dataset.preset.split(',');
                var preset = MI_PRESETS[+idxParts[0]].presets[+idxParts[1]];
                var presetFilters = preset.build();
                var presetList = e.target.closest('.source-mediainfo-container').querySelector('.mediainfo-filter-list');
                presetList.innerHTML = presetFilters.map(function(f, i) { return getMediaInfoFilterGroupHtml(f, i, i === 0); }).join('');
                e.target.closest('.mi-preset-panel').style.display = 'none';
                setTimeout(checkFormState, 0);
                return;
            }
            if (e.target.closest('.btnAddMediaInfoFilter')) {
                var list = row.querySelector('.mediainfo-filter-list');
                var idx = list.querySelectorAll('.mediainfo-filter-group').length;
                list.insertAdjacentHTML('beforeend', getMediaInfoFilterGroupHtml({ Operator: 'AND', Criteria: [], GroupOperator: 'AND' }, idx, false));
            }

            if (e.target.closest('.btnClearAllFilters')) {
                var list = row.querySelector('.mediainfo-filter-list');
                if (list) { list.innerHTML = ''; }
            }

            if (e.target.closest('.btnRemoveFilterGroup')) {
                e.target.closest('.mediainfo-filter-group').remove();
                var miList = row.querySelector('.mediainfo-filter-list');
                var firstGroup = miList && miList.querySelector('.mediainfo-filter-group');
                if (firstGroup) { var conn = firstGroup.querySelector('.mi-group-connector'); if (conn) conn.remove(); }
            }

            if (e.target.closest('.btnGroupOpChoice')) {
                var btn = e.target.closest('.btnGroupOpChoice');
                var newOp = btn.dataset.value;
                var group = btn.closest('.mediainfo-filter-group');
                group.dataset.groupOp = newOp;
                group.querySelectorAll('.btnGroupOpChoice').forEach(function(b) {
                    var active = b.dataset.value === newOp;
                    b.style.background = active
                        ? 'var(--theme-background-level3)'
                        : 'transparent';
                    b.style.color = active ? 'var(--theme-text-primary)' : 'var(--theme-text-secondary)';
                });
                var desc = group.querySelector('.group-op-desc');
                if (desc) desc.textContent = newOp === 'AND' ? 'Both groups must match' : 'Either group is enough';
                setTimeout(checkFormState, 0);
            }

            if (e.target.closest('.btnGroupInnerOpChoice')) {
                var btn = e.target.closest('.btnGroupInnerOpChoice');
                var newOp = btn.dataset.value;
                var group = btn.closest('.mediainfo-filter-group');
                group.dataset.op = newOp;
                group.querySelectorAll('.btnGroupInnerOpChoice').forEach(function(b) {
                    var active = b.dataset.value === newOp;
                    b.style.background = active
                        ? 'var(--theme-background-level3)'
                        : 'transparent';
                    b.style.color = active ? 'var(--theme-text-primary)' : 'var(--theme-text-secondary)';
                });
                var desc = group.querySelector('.inner-op-desc');
                if (desc) desc.textContent = newOp === 'AND' ? 'All rules must match' : 'Any rule is enough';
                setTimeout(checkFormState, 0);
            }

            if (e.target.closest('.btnNotToggle')) {
                var btn = e.target.closest('.btnNotToggle');
                var active = btn.dataset.not === '1';
                active = !active;
                btn.dataset.not = active ? '1' : '0';
                btn.style.background = active ? 'rgba(229,115,115,0.15)' : 'transparent';
                btn.style.color = active ? 'var(--hsc-danger-text)' : 'var(--theme-text-secondary)';
                btn.style.border = active ? '1px solid rgba(229,115,115,0.5)' : '1px solid var(--hsc-hairline-strong)';
                setTimeout(checkFormState, 0);
            }

            if (e.target.closest('.mi-include-parent')) {
                setTimeout(checkFormState, 0);
            }

            if (e.target.closest('.btnAddMiRule')) {
                var rulesList = e.target.closest('.mediainfo-filter-group').querySelector('.mi-rules-list');
                rulesList.insertAdjacentHTML('beforeend', getMediaInfoRuleHtml(''));
            }

            if (e.target.closest('.btnRemoveMiRule')) {
                e.target.closest('.mi-rule').remove();
            }

            if (e.target.closest('.btnRemoveGroup')) {
                confirmDeleteGroup(row, function () {
                    var rowView = row.closest('#HomeScreenCompanionConfigPage') || activeView();
                    row.remove();
                    if (rowView) updateIssueCounter(rowView);
                    setTimeout(checkFormState, 0);
                });
            }

            if (e.target.closest('.btnRunEntry')) {
                var entryName = row.querySelector('.txtEntryLabel').value || row.querySelector('.txtTagName').value;
                if (!entryName) { window.Dashboard.alert('Entry has no name or tag.'); return; }
                var doRun = function () {
                    var view = row.closest('#HomeScreenCompanionConfigPage');
                    var btn = row.querySelector('.btnRunEntry');
                    var lbl = btn.querySelector('.btnRunEntryLabel');
                    var btnSaveEl = view ? view.querySelector('.btn-save') : null;
                    var dotEl = view ? view.querySelector('#dotStatus') : null;
                    var labelEl = view ? view.querySelector('#lastRunStatusLabel') : null;
                    // Enter running state
                    lbl.textContent = 'Running…';
                    btn.disabled = true;
                    if (btnSaveEl) { btnSaveEl.disabled = true; btnSaveEl.style.opacity = '0.5'; btnSaveEl.querySelector('span').textContent = 'Sync in progress...'; }
                    if (dotEl) { dotEl.className = 'status-dot running'; }
                    if (labelEl) labelEl.textContent = 'Running...';
                    if (view) setTimeout(function () { refreshStatus(view); }, 500); // live step (and 1 s polling) right away
                    fetch(window.ApiClient.getUrl('HomeScreenCompanion/RunEntry'), {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json', 'X-MediaBrowser-Token': window.ApiClient.accessToken() },
                        body: JSON.stringify({ EntryName: entryName })
                    }).then(function (r) { return r.json(); })
                    .then(function (result) {
                        lbl.textContent = 'Run Group';
                        btn.disabled = false;
                        if (view) { invalidateTagTabs(view); refreshStatus(view); }
                        window.Dashboard.alert(result.Queued ? result.Message : result.Success ? ('Done: ' + result.Message) : ('Failed: ' + result.Message));
                    }).catch(function () {
                        lbl.textContent = 'Run Group';
                        btn.disabled = false;
                        if (view) refreshStatus(view);
                        window.Dashboard.alert('Request failed.');
                    });
                };
                saveIfDirtyThenRun(row.closest('#HomeScreenCompanionConfigPage') || activeView(), doRun);
            }

            var btnTest = e.target.closest('.btnTestUrl');
            if (btnTest) {
                var uRow = btnTest.closest('.url-row');
                var url = uRow.querySelector('.txtTagUrl').value;
                if (!url) return;

                var limitVal = parseInt(uRow.querySelector('.txtUrlLimit').value, 10) || 0;

                btnTest.disabled = true;
                var urlModal = openSourceTestModal('Source test', url);
                window.ApiClient.getJSON(window.ApiClient.getUrl("HomeScreenCompanion/TestUrl", { Url: url, Limit: limitVal })).then(result => {
                    if (result.Success) urlModal.show(result);
                    else urlModal.fail(result.Message);
                }).catch(() => urlModal.fail('Request failed. Check server logs.'))
                .finally(() => btnTest.disabled = false);
            }

            var btnTestAi = e.target.closest('.btnTestAiSource');
            if (btnTestAi) {
                var aiContainer = btnTestAi.closest('.source-ai-container');
                var aiProvider = (aiContainer.querySelector('.selAiProvider') || {}).value || 'OpenAI';
                var aiPrompt = ((aiContainer.querySelector('.txtAiPrompt') || {}).value || '').trim();
                var aiIncludeWatched = !!(aiContainer.querySelector('.chkAiRecentlyWatched') || {}).checked;
                var aiWatchedUserId = aiIncludeWatched ? ((aiContainer.querySelector('.selAiWatchedUser') || {}).value || '') : '';
                var aiWatchedCount = parseInt(((aiContainer.querySelector('.txtAiWatchedCount') || {}).value || '20'), 10) || 20;
                var resultSpan = aiContainer.querySelector('.ai-test-result');

                if (!aiPrompt) { if (resultSpan) resultSpan.textContent = 'Please enter a prompt first.'; return; }

                btnTestAi.disabled = true;
                if (resultSpan) resultSpan.textContent = 'Testing...';
                var aiModal = openSourceTestModal('AI source test', aiProviderLabel(aiProvider) + ' · ' + aiPrompt);

                fetch(window.ApiClient.getUrl('HomeScreenCompanion/TestAiSource'), {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json', 'X-MediaBrowser-Token': window.ApiClient.accessToken() },
                    body: JSON.stringify({
                        Provider: aiProvider,
                        Prompt: aiPrompt,
                        IncludeRecentlyWatched: aiIncludeWatched,
                        RecentlyWatchedUserId: aiWatchedUserId,
                        RecentlyWatchedCount: aiWatchedCount
                    })
                }).then(function(r) { return r.json(); })
                .then(function(result) {
                    if (resultSpan) resultSpan.textContent = result.Success ? result.Message : ('Failed: ' + result.Message);
                    if (result.Success) aiModal.show(result);
                    else aiModal.fail('AI test failed: ' + result.Message);
                }).catch(function(err) {
                    if (resultSpan) resultSpan.textContent = 'Error: ' + err.message;
                    aiModal.fail('Error: ' + err.message);
                }).finally(function() { btnTestAi.disabled = false; });
            }
        });

        function updateTagTitle() {
            var lbl = row.querySelector('.txtEntryLabel').value;
            var tag = row.querySelector('.txtTagName').value;
            row.querySelector('.tag-title').textContent = lbl || tag || 'New';
            row.querySelector('.txtTagName').setAttribute('placeholder', lbl);
            row.querySelector('.txtCollectionName').setAttribute('placeholder', lbl);
            var hseCustomTitle = row.querySelector('[data-field="CustomName"]');
            if (hseCustomTitle) hseCustomTitle.setAttribute('placeholder', lbl);
            updateBadges(row);
        }
        row.querySelector('.txtEntryLabel').addEventListener('input', function() {
            updateTagTitle();
        });
        row.querySelector('.txtTagName').addEventListener('input', updateTagTitle);

        // Manual order: drag the handle (mouse, touch or pen) or use its arrow keys.
        makeRowSortable(row, row.querySelector('.drag-handle'), {
            list: function () { return row.parentElement; },
            rowSelector: '.tag-row',
            canDrag: function () { return !!row.parentElement && !row.parentElement.classList.contains('sort-hidden'); },
            onStart: function () {
                // Closed rows are easier to move past each other.
                row.parentElement.querySelectorAll('.tag-body').forEach(function (b) { b.style.display = 'none'; });
                row.parentElement.querySelectorAll('.expand-icon').forEach(function (i) { i.innerText = 'expand_more'; });
            },
            onMoved: function () { setTimeout(checkFormState, 0); }
        });

        row.querySelector('.btnDuplicateRow').addEventListener('click', () => {
            var config = markAsCopy(readRowAsConfig(row));
            renderTagGroup(config, row.closest('#tagListContainer'), true, undefined, true);
            applyFilters(row.closest('#HomeScreenCompanionConfigPage') || activeView());
            setTimeout(checkFormState, 0);
        });

        row.querySelector('.btnCopySourceRow').addEventListener('click', () => {
            copySourceToClipboard(row);
        });

        row.querySelectorAll('.coll-style-picker').forEach(function (picker) {
            var opts = Array.from(picker.querySelectorAll('.coll-style-opt'));
            opts.forEach(function (label) {
                label.addEventListener('click', function () {
                    opts.forEach(function (l) { l.style.borderColor = 'var(--line-color,rgba(255,255,255,0.12))'; });
                    label.style.borderColor = 'var(--hsc-primary)';
                });
            });
            picker.addEventListener('change', picker.dataset.scope === 'tag' ? syncTagArt : picker.dataset.scope === 'playlist' ? syncPlArt : syncCollArt);
            // Customise shows for a generated style only (not Custom / Emby's own).
            picker.addEventListener('change', function () {
                var on = picker.querySelector('input[type=radio]:checked');
                var customRow = picker.querySelector('.coll-art-custom-row');
                if (customRow) customRow.style.display = on && isGenArtStyle(on.value) ? 'flex' : 'none';
                var keepNote = picker.querySelector('.coll-art-keep-note');
                if (keepNote) keepNote.style.display = on && isKeepArtStyle(on.value) ? 'block' : 'none';
            });
            var custBtn = picker.querySelector('.btnArtCustomise');
            if (custBtn) custBtn.addEventListener('click', function (e) { e.preventDefault(); e.stopPropagation(); openArtCustomise(picker); });
        });
        setTimeout(fillCollStyleThumbs, 0);
        function syncCollArt() {
            var art = readCollectionArt(row);
            row.querySelector('.coll-poster-upload').style.display = art.CollectionPosterStyle ? 'none' : 'block';
            row.querySelector('.coll-poster-gen-note').style.display = isGenArtStyle(art.CollectionPosterStyle) ? 'block' : 'none';
            row.querySelector('.coll-bg-upload').style.display = art.CollectionBackgroundStyle ? 'none' : 'block';
            row.querySelector('.coll-thumb-upload').style.display = art.CollectionThumbStyle ? 'none' : 'block';
            row.querySelector('.coll-banner-upload').style.display = art.CollectionBannerStyle ? 'none' : 'block';
            var collGen = isGenArtStyle(art.CollectionPosterStyle) || isGenArtStyle(art.CollectionBackgroundStyle) || isGenArtStyle(art.CollectionThumbStyle);
            row.querySelector('.coll-art-preview-row').style.display = collGen ? 'block' : 'none';
            row.querySelector('.coll-art-title-row').style.display = collGen ? 'block' : 'none';
        }
        function syncPlArt() {
            var look = readPlaylistLook(row), box = row.querySelector('.pl-look');
            var gen = isGenArtStyle(look.PlaylistPosterStyle) || isGenArtStyle(look.PlaylistBackgroundStyle);
            box.querySelector('.pl-poster-upload').style.display = look.PlaylistPosterStyle ? 'none' : 'block';
            box.querySelector('.pl-poster-gen-note').style.display = isGenArtStyle(look.PlaylistPosterStyle) ? 'block' : 'none';
            box.querySelector('.pl-bg-upload').style.display = look.PlaylistBackgroundStyle ? 'none' : 'block';
            box.querySelector('.pl-art-preview-row').style.display = gen ? 'block' : 'none';
            box.querySelector('.pl-art-title-row').style.display = gen ? 'block' : 'none';
        }
        function syncTagArt() {
            var art = readTagArt(row);
            var tagArtMore = row.querySelector('.tag-art-more');
            if (tagArtMore) tagArtMore.dataset.custom = art.TagPosterStyle || art.TagBackgroundStyle || art.TagBackgroundPath ? '1' : '0';
            row.querySelector('.tag-poster-note').style.display = isKeepArtStyle(art.TagPosterStyle) ? 'none' : 'block';
            row.querySelector('.tag-poster-note').textContent = art.TagPosterStyle
                ? 'Drawn from the posters of the first titles with this tag, with the tag name. It is redrawn when the titles change.'
                : "Emby's own collage of the tagged titles.";
            row.querySelector('.tag-bg-upload').style.display = art.TagBackgroundStyle ? 'none' : 'block';
            var tagGen = isGenArtStyle(art.TagPosterStyle) || isGenArtStyle(art.TagBackgroundStyle);
            row.querySelector('.tag-art-preview-row').style.display = tagGen ? 'block' : 'none';
            row.querySelector('.tag-art-title-row').style.display = tagGen ? 'block' : 'none';
        }

        // Background upload (Custom): same endpoints as the poster upload. Wired once for the
        // collection background and once for the tag background (each its own block).
        function bgHeaders() {
            var headers = { 'Content-Type': 'application/json' };
            var token = window.ApiClient.accessToken();
            if (token) headers['X-Emby-Token'] = token;
            return headers;
        }
        function wireBgUpload(box) {
            if (!box) return;
            function bgUploaded(filePath, name, previewSrc) {
                box.querySelector('.hiddenBgPath').value = filePath;
                box.querySelector('.bg-filename').textContent = name;
                box.querySelector('.bg-preview-container').style.display = 'block';
                var img = box.querySelector('.bg-preview-img');
                img.src = previewSrc;
                img.style.display = 'block';
                box.querySelector('.hiddenBgPath').dispatchEvent(new Event('change', { bubbles: true }));
            }
            box.querySelector('.btnChooseBg').addEventListener('click', function () {
                box.querySelector('.inputBgFile').click();
            });
            box.querySelector('.inputBgFile').addEventListener('change', function () {
                var file = this.files[0];
                if (!file) return;
                var reader = new FileReader();
                reader.onload = function (e) {
                    var dataUrl = e.target.result;
                    fetch(window.ApiClient.getUrl('HomeScreenCompanion/UploadCollectionImage'), {
                        method: 'POST',
                        headers: bgHeaders(),
                        body: JSON.stringify({ FileName: file.name, Base64Data: dataUrl.split(',')[1], OldFilePath: box.querySelector('.hiddenBgPath').value })
                    }).then(function (r) { return r.json(); })
                    .then(function (result) {
                        if (result.Success) bgUploaded(result.FilePath, file.name, dataUrl);
                        else window.Dashboard.alert('Upload failed: ' + (result.Message || 'Unknown error'));
                    }).catch(function () { window.Dashboard.alert('Upload error. Check server logs.'); });
                };
                reader.readAsDataURL(file);
            });
            box.querySelector('.btnRemoveBg').addEventListener('click', function () {
                box.querySelector('.hiddenBgPath').value = '';
                box.querySelector('.bg-filename').textContent = '';
                box.querySelector('.bg-preview-container').style.display = 'none';
                box.querySelector('.bg-preview-img').style.display = 'none';
                box.querySelector('.inputBgFile').value = '';
                box.querySelector('.hiddenBgPath').dispatchEvent(new Event('change', { bubbles: true }));
            });
            box.querySelector('.btnLoadBgUrl').addEventListener('click', function () {
                var url = box.querySelector('.txtBgUrl').value.trim();
                if (!url) return;
                fetch(window.ApiClient.getUrl('HomeScreenCompanion/FetchCollectionImageFromUrl'), {
                    method: 'POST',
                    headers: bgHeaders(),
                    body: JSON.stringify({ Url: url, OldFilePath: box.querySelector('.hiddenBgPath').value })
                }).then(function (r) { return r.json(); })
                .then(function (result) {
                    if (result.Success) {
                        bgUploaded(result.FilePath, url.split('/').pop().split('?')[0], url);
                        box.querySelector('.txtBgUrl').value = '';
                    } else window.Dashboard.alert('Failed to load image: ' + (result.Message || 'Unknown error'));
                }).catch(function () { window.Dashboard.alert('Error fetching image. Check the URL and server logs.'); });
            });
        }
        wireBgUpload(row.querySelector('.coll-bg-upload'));
        wireBgUpload(row.querySelector('.coll-thumb-upload'));
        wireBgUpload(row.querySelector('.coll-banner-upload'));
        wireBgUpload(row.querySelector('.tag-bg-upload'));
        wireBgUpload(row.querySelector('.pl-bg-upload'));
        row.querySelector('.btnPlArtPreview').addEventListener('click', function () {
            showCollectionArtPreview(row, this, 'playlist');
        });
        row.querySelector('.btnTagArtPreview').addEventListener('click', function () {
            showCollectionArtPreview(row, this, true);
        });
        row.querySelector('.btnCollArtPreview').addEventListener('click', function () {
            showCollectionArtPreview(row, this);
        });

        // Poster upload (Custom): wired once for the collection poster and once for the playlist poster.
        function wirePosterUpload(box) {
            if (!box) return;
            box.querySelector('.btnChoosePoster').addEventListener('click', function () {
                box.querySelector('.inputPosterFile').click();
            });

            box.querySelector('.inputPosterFile').addEventListener('change', function () {
                var file = this.files[0];
                if (!file) return;
                var reader = new FileReader();
                reader.onload = function (e) {
                    var dataUrl = e.target.result;
                    var base64 = dataUrl.split(',')[1];
                    var img = box.querySelector('.poster-preview-img');
                    img.src = dataUrl;
                    img.style.display = 'block';

                    var headers = { 'Content-Type': 'application/json' };
                    var token = window.ApiClient.accessToken();
                    if (token) headers['X-Emby-Token'] = token;
                    fetch(window.ApiClient.getUrl('HomeScreenCompanion/UploadCollectionImage'), {
                        method: 'POST',
                        headers: headers,
                        body: JSON.stringify({ FileName: file.name, Base64Data: base64, OldFilePath: box.querySelector('.hiddenPosterPath').value })
                    }).then(function (r) { return r.json(); })
                    .then(function (result) {
                        if (result.Success) {
                            box.querySelector('.hiddenPosterPath').value = result.FilePath;
                            box.querySelector('.poster-filename').textContent = file.name;
                            box.querySelector('.poster-preview-container').style.display = 'block';
                            box.querySelector('.hiddenPosterPath').dispatchEvent(new Event('change', { bubbles: true }));
                        } else {
                            window.Dashboard.alert('Upload failed: ' + (result.Message || 'Unknown error'));
                            img.style.display = 'none';
                        }
                    }).catch(function () {
                        window.Dashboard.alert('Upload error. Check server logs.');
                        img.style.display = 'none';
                    });
                };
                reader.readAsDataURL(file);
            });

            box.querySelector('.btnRemovePoster').addEventListener('click', function () {
                box.querySelector('.hiddenPosterPath').value = '';
                box.querySelector('.poster-filename').textContent = '';
                box.querySelector('.poster-preview-container').style.display = 'none';
                box.querySelector('.poster-preview-img').style.display = 'none';
                box.querySelector('.inputPosterFile').value = '';
                box.querySelector('.hiddenPosterPath').dispatchEvent(new Event('change', { bubbles: true }));
            });

            box.querySelector('.btnLoadPosterUrl').addEventListener('click', function () {
                var url = box.querySelector('.txtPosterUrl').value.trim();
                if (!url) return;

                var headers = { 'Content-Type': 'application/json' };
                var token = window.ApiClient.accessToken();
                if (token) headers['X-Emby-Token'] = token;

                fetch(window.ApiClient.getUrl('HomeScreenCompanion/FetchCollectionImageFromUrl'), {
                    method: 'POST',
                    headers: headers,
                    body: JSON.stringify({ Url: url, OldFilePath: box.querySelector('.hiddenPosterPath').value })
                }).then(function (r) { return r.json(); })
                .then(function (result) {
                    if (result.Success) {
                        box.querySelector('.hiddenPosterPath').value = result.FilePath;
                        box.querySelector('.poster-filename').textContent = url.split('/').pop().split('?')[0];
                        box.querySelector('.poster-preview-container').style.display = 'block';
                        var img = box.querySelector('.poster-preview-img');
                        img.src = url;
                        img.style.display = 'block';
                        box.querySelector('.txtPosterUrl').value = '';
                        box.querySelector('.hiddenPosterPath').dispatchEvent(new Event('change', { bubbles: true }));
                    } else {
                        window.Dashboard.alert('Failed to load image: ' + (result.Message || 'Unknown error'));
                    }
                }).catch(function () {
                    window.Dashboard.alert('Error fetching image. Check the URL and server logs.');
                });
            });
        }
        wirePosterUpload(row.querySelector('.coll-poster-upload'));
        wirePosterUpload(row.querySelector('.pl-poster-upload'));

        setTimeout(function () { updateHseSectionAvailability(row); }, 0);
    }


    // Renders the merged execution log as colour-coded lines. Classification is based on the
    // leading symbol each server task writes (✔ ⚠ ✖ –) and on the [DEBUG] token, which is
    // stripped for display. A source badge is shown only when several tasks have output.
    function renderLogLines(container, entries, isRunning) {
        var stickToBottom = isRunning && (container.scrollHeight - container.scrollTop - container.clientHeight) < 40;
        var wasEmpty = container.childElementCount === 0;
        if (!entries.length) { container.textContent = '(no logs yet)'; return; }

        var srcCount = {};
        entries.forEach(function (e) { srcCount[e.src] = 1; });
        var showSrc = Object.keys(srcCount).length > 1;

        var frag = document.createDocumentFragment();
        entries.forEach(function (e) {
            var raw = e.text || '';
            var ts = '';
            var m = raw.match(/^\[(\d{2}:\d{2}:\d{2})\] ?/);
            if (m) { ts = m[1]; raw = raw.substring(m[0].length); }

            var cls = '';
            if (/^\[DEBUG\] ?/.test(raw)) { cls = 'log-debug'; raw = raw.replace(/^\[DEBUG\] ?/, ''); }
            else if (raw.trim() === '') cls = 'log-blank';
            else if (/^═+$/.test(raw.trim())) cls = 'log-rule';
            else if (raw.indexOf('✖') >= 0) cls = 'log-err';
            else if (raw.indexOf('⚠') >= 0) cls = 'log-warn';
            else if (raw.indexOf('✔') >= 0) cls = 'log-ok';
            else if (/^\s{1,3}– /.test(raw)) cls = 'log-skip';
            else if (isLogHeader(raw)) cls = 'log-head';

            var line = document.createElement('div');
            line.className = 'log-line' + (cls ? ' ' + cls : '');
            if (cls !== 'log-blank') {
                var tsEl = document.createElement('span');
                tsEl.className = 'log-ts';
                tsEl.textContent = ts;
                line.appendChild(tsEl);
                if (showSrc) {
                    var srcEl = document.createElement('span');
                    srcEl.className = 'log-src';
                    srcEl.textContent = e.src;
                    line.appendChild(srcEl);
                }
                line.appendChild(document.createTextNode(raw));
            } else {
                line.textContent = ' ';
            }
            frag.appendChild(line);
        });
        container.textContent = '';
        container.appendChild(frag);
        if (stickToBottom || wasEmpty) container.scrollTop = container.scrollHeight;
    }

    // Popup for the "Test" buttons on list/AI sources: shows every item the source returned and
    // whether it exists in the library. Opens in a loading state; call .show(result) or .fail(msg).
    // The source Preview reuses it and draws its own list into .body.
    function openSourceTestModal(title, subtitle, loadingText) {
        function esc(s) { return String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;'); }

        var overlay = document.createElement('div');
        overlay.className = 'modal-backdrop';
        overlay.innerHTML =
            '<div class="modal-content stm-content">' +
                '<div class="modal-header-bar stm-header">' +
                    '<i class="md-icon modal-header-icon">fact_check</i>' +
                    '<div style="min-width:0;">' +
                        '<h2 class="modal-header-title">' + esc(title) + '</h2>' +
                        (subtitle ? '<span class="stm-sub">' + esc(subtitle) + '</span>' : '') +
                    '</div>' +
                    '<button type="button" class="stm-close" title="Close"><i class="md-icon">close</i></button>' +
                '</div>' +
                '<div class="stm-body" style="display:flex;flex-direction:column;min-height:0;flex:1;">' +
                    '<div class="stm-empty"><i class="md-icon" style="font-size:2em;display:block;margin-bottom:8px;">hourglass_empty</i>' + esc(loadingText || 'Fetching source…') + '</div>' +
                '</div>' +
            '</div>';
        document.body.appendChild(overlay);
        requestAnimationFrame(function () { overlay.classList.add('modal-visible'); });

        function close() {
            overlay.classList.remove('modal-visible');
            document.removeEventListener('keydown', onKey);
            setTimeout(function () { overlay.remove(); }, 300);
        }
        function onKey(e) { if (e.key === 'Escape') close(); }
        document.addEventListener('keydown', onKey);
        overlay.addEventListener('click', function (e) {
            if (e.target === overlay || e.target.closest('.stm-close') || e.target.closest('.stm-done')) close();
        });

        var body = overlay.querySelector('.stm-body');

        function show(result) {
            var items = result.Items || [];
            var inCount = items.filter(function (i) { return i.InLibrary; }).length;
            var outCount = items.length - inCount;
            var pct = items.length ? Math.round(inCount * 100 / items.length) : 0;
            var filter = 'all';

            function rowHtml(item, idx) {
                var poster = item.InLibrary && item.LibraryItemId
                    ? '<div class="stm-poster" style="background-image:url(&quot;' + esc(window.ApiClient.getImageUrl(item.LibraryItemId, { type: 'Primary', maxHeight: 100 })) + '&quot;)"></div>'
                    : '<div class="stm-poster"><i class="md-icon">movie</i></div>';
                var year = item.Year || item.LibraryYear;
                var meta = [];
                if (year) meta.push(year);
                if (item.ItemType) meta.push(item.ItemType === 'Series' ? 'Series' : 'Movie');
                if (item.Imdb) meta.push('<a href="https://www.imdb.com/title/' + encodeURIComponent(item.Imdb) + '/" target="_blank" rel="noopener">' + esc(item.Imdb) + '</a>');
                else meta.push('no IMDb id');
                if (item.MatchedByTitle) meta.push('matched by title');
                if (item.InLibrary && item.LibraryName && item.LibraryName.toLowerCase() !== (item.Name || '').toLowerCase())
                    meta.push('as “' + esc(item.LibraryName) + '”');
                return '<div class="stm-row ' + (item.InLibrary ? 'found' : 'missing') + '" style="animation-delay:' + Math.min(idx * 15, 400) + 'ms">' +
                    '<div class="stm-rank">' + (idx + 1) + '</div>' +
                    poster +
                    '<div class="stm-info">' +
                        '<div class="stm-title" title="' + esc(item.Name) + '">' + esc(item.Name || '(untitled)') + '</div>' +
                        '<div class="stm-meta">' + meta.join(' · ') + '</div>' +
                    '</div>' +
                    '<div class="stm-status ' + (item.InLibrary ? 'in' : 'out') + '" title="' + (item.InLibrary ? 'In library' : 'Not in library') + '">' +
                        '<i class="md-icon">' + (item.InLibrary ? 'check' : 'close') + '</i>' +
                    '</div>' +
                '</div>';
            }

            function renderList() {
                var html = '';
                items.forEach(function (item, idx) {
                    if (filter === 'in' && !item.InLibrary) return;
                    if (filter === 'out' && item.InLibrary) return;
                    html += rowHtml(item, idx);
                });
                body.querySelector('.stm-list').innerHTML = html || '<div class="stm-empty">Nothing to show.</div>';
                body.querySelectorAll('.stm-chip').forEach(function (c) { c.classList.toggle('active', c.dataset.filter === filter); });
            }

            body.innerHTML =
                '<div class="stm-summary">' +
                    '<div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:8px;">' +
                        '<span><strong style="font-size:1.4em;">' + inCount + '</strong> <span style="opacity:0.7;">of ' + items.length + ' in your library</span></span>' +
                        '<span style="font-weight:bold;color:' + (pct >= 50 ? 'var(--hsc-primary-text)' : 'var(--hsc-warning-text)') + ';">' + pct + '%</span>' +
                    '</div>' +
                    '<div class="stm-bar"><div class="stm-bar-fill" style="width:0%"></div></div>' +
                    '<div class="stm-filters">' +
                        '<button type="button" class="stm-chip" data-filter="all"><i class="md-icon">list</i>All ' + items.length + '</button>' +
                        '<button type="button" class="stm-chip stm-chip-in" data-filter="in"><i class="md-icon">check_circle</i>In library ' + inCount + '</button>' +
                        '<button type="button" class="stm-chip stm-chip-out" data-filter="out"><i class="md-icon">cancel</i>Missing ' + outCount + '</button>' +
                    '</div>' +
                '</div>' +
                '<div class="stm-list"></div>' +
                '<div class="stm-footer"><span>Matched by IMDb id' + (items.some(function (i) { return i.MatchedByTitle; }) ? ', with title + year as fallback' : '') + '</span><button type="button" class="stm-done">Close</button></div>';

            body.querySelector('.stm-filters').addEventListener('click', function (e) {
                var chip = e.target.closest('.stm-chip');
                if (!chip) return;
                filter = chip.dataset.filter;
                renderList();
            });
            renderList();
            requestAnimationFrame(function () {
                requestAnimationFrame(function () { body.querySelector('.stm-bar-fill').style.width = pct + '%'; });
            });
        }

        function fail(message) {
            body.innerHTML =
                '<div class="stm-empty"><i class="md-icon" style="font-size:2.4em;display:block;margin-bottom:8px;color:var(--hsc-danger-text);">error_outline</i>' + esc(message) + '</div>' +
                '<div class="stm-footer"><button type="button" class="stm-done">Close</button></div>';
        }

        return { show: show, fail: fail, body: body };
    }

    // Picks which task's log to show (running task, else the most recently started one, unless the
    // user chose a tab), updates the tab bar and renders that single log.
    function renderLogModal(view) {
        var content = view.querySelector('#logContent');
        var tabs = view.querySelectorAll('#logTabs .log-tab[data-log]');
        if (!content) return;

        var keys = ['sync', 'hsc', 'tl'];
        function startedMs(k) { var s = _lastStatus[k]; var t = s && s.StartedUtc ? Date.parse(s.StartedUtc) : NaN; return isNaN(t) ? 0 : t; }
        function running(k) { return !!(_lastStatus[k] && _lastStatus[k].IsRunning); }
        function logs(k) { return (_lastStatus[k] && _lastStatus[k].Logs) || []; }

        var selected = _logTab;
        if (!selected) {
            selected = keys.filter(running)[0];
            if (!selected) {
                var best = 0;
                keys.forEach(function (k) { if (startedMs(k) > best) { best = startedMs(k); selected = k; } });
            }
            if (!selected) selected = 'sync';
        }

        tabs.forEach(function (tab) {
            var k = tab.getAttribute('data-log');
            tab.classList.toggle('active', k === selected);
            tab.classList.toggle('empty', logs(k).length === 0 && !running(k));
            var dot = tab.querySelector('.status-dot');
            if (dot) { dot.className = 'status-dot'; if (running(k)) dot.classList.add('running'); dot.style.visibility = (running(k) || logs(k).length) ? 'visible' : 'hidden'; }
            var timeEl = tab.querySelector('.log-tab-time');
            if (timeEl) {
                var ms = startedMs(k);
                timeEl.textContent = ms ? new Date(ms).toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' }) : '';
            }
        });

        var issuesBtn = view.querySelector('#btnLogIssuesOnly');
        if (issuesBtn) issuesBtn.classList.toggle('active', _logIssuesOnly);

        var entries = logs(selected).map(function (l) { return { src: selected, text: l }; });
        if (!entries.length) { content.textContent = '(no runs yet)'; return; }
        if (_logIssuesOnly) {
            entries = logIssueEntries(entries);
            if (!entries.length) { content.textContent = '(no warnings or errors in this run)'; return; }
        }
        renderLogLines(content, entries, running(selected));
    }

    // A group, phase or run header line of the log (timestamp already removed).
    function isLogHeader(raw) {
        return /^(»|Results$|Summary$|\[Cleanup\]$|\[\d+\/\d+\] |Home Screen (Companion|Sync) )/.test(raw);
    }

    // The log's ⚠ / ✖ lines, each run of them under the group (or phase) header it belongs to.
    // Detail lines (indented deeper) right after an issue line are kept with it.
    function logIssueEntries(entries) {
        var out = [], head = null, shownHead = null, inIssue = false;
        entries.forEach(function (e) {
            var raw = (e.text || '').replace(/^\[\d{2}:\d{2}:\d{2}\] ?/, '');
            if (/^\[DEBUG\]/.test(raw)) return;
            if (isLogHeader(raw)) { head = e; inIssue = false; return; }
            if (raw.indexOf('✖') < 0 && raw.indexOf('⚠') < 0) {
                if (inIssue && /^\s{6,}\S/.test(raw)) out.push(e);
                else inIssue = false;
                return;
            }
            inIssue = true;
            if (head && head !== shownHead) {
                if (out.length) out.push({ src: e.src, text: '' });
                out.push(head);
                shownHead = head;
            }
            out.push(e);
        });
        return out;
    }

    // A sync changes tags, so the tabs that list tags (Top-Lists, Cleanup) load them again the
    // next time they are opened. lastSyncRunKey identifies the last finished sync seen.
    var lastSyncRunKey = null;
    function invalidateTagTabs(view) {
        ['#tlContainer', '#tcManageContainer'].forEach(function (sel) {
            var el = view.querySelector(sel);
            if (el) el.dataset.loaded = '';
        });
    }

    // Run Group (Sources and Top Lists): with unsaved changes, ask "Save and run?", save, then run.
    // waitForTopLists: also wait until the top-list forms have been saved (that save rebuilds the
    // list and can take a while) — the run starts once nothing is left to save.
    function saveIfDirtyThenRun(view, doRun, waitForTopLists) {
        var _saveBtn = view ? view.querySelector('.btn-save') : null;
        var isDirty = _saveBtn && !_saveBtn.disabled;
        if (!isDirty) { doRun(); return; }
        if (!confirm('You have unsaved changes. Save and run?')) return;
        _saveBtn.click();
        if (!waitForTopLists) { setTimeout(doRun, 800); return; }
        var deadline = Date.now() + 600000;
        (function waitSaved() {
            var tlCont = view.querySelector('#tlContainer');
            var saving = _saveBtn.innerHTML.indexOf('hourglass_empty') !== -1;
            var pending = tlCont && tlCont.querySelector('.tag-body[data-dirty="1"]');
            if (!saving && !pending) { setTimeout(doRun, 800); return; }
            if (!saving && pending) return; // the save failed and said so — do not run
            if (Date.now() < deadline) setTimeout(waitSaved, 300);
        })();
    }

    // "Running · <step> · <elapsed>" for the Last run chip; the step comes from the Status endpoints.
    function runningStepText(st, isHomeScreen) {
        var step = st.Step || '';
        if (isHomeScreen && step.indexOf('Home Screen Sync') !== 0) step = 'Home Screen Sync · ' + step;
        if (step.length > 70) step = step.substring(0, 69) + '…';
        var text = 'Running · ' + step;
        var started = st.StartedUtc ? Date.parse(st.StartedUtc) : NaN;
        if (!isNaN(started)) {
            var s = Math.max(0, Math.floor((Date.now() - started) / 1000));
            text += ' · ' + (s >= 3600 ? Math.floor(s / 3600) + 'h ' + Math.floor((s % 3600) / 60) + 'm'
                : s >= 60 ? Math.floor(s / 60) + 'm ' + (s % 60) + 's' : s + 's');
        }
        // Runs waiting for this one go last: the label is cut to the width with an ellipsis
        // (the full text is in its tooltip).
        if (st.Waiting) text += ' · ' + st.Waiting;
        return text;
    }

    function refreshStatus(view) {
        var myId = ++statusRequestId;
        Promise.all([
            window.ApiClient.getJSON(window.ApiClient.getUrl("HomeScreenCompanion/Status")),
            window.ApiClient.getJSON(window.ApiClient.getUrl("HomeScreenCompanion/Hsc/Status")).catch(function () { return null; }),
            window.ApiClient.getJSON(window.ApiClient.getUrl("HomeScreenCompanion/TopList/Status")).catch(function () { return null; })
        ]).then(function (results) {
            if (myId !== statusRequestId) return;
            var result = results[0], hscResult = results[1], tlResult = results[2];
            var label = view.querySelector('#lastRunStatusLabel'), dot = view.querySelector('#dotStatus'), content = view.querySelector('#logContent');
            var btnSave = view.querySelector('.btn-save'), btnRun = view.querySelector('#btnRunSync');

            setGroupIssues(view, result.GroupIssues);
            if (!result.IsRunning) {
                var runKey = (result.StartedUtc || '') + '|' + (result.LastRunStatus || '');
                if (lastSyncRunKey !== null && runKey !== lastSyncRunKey) invalidateTagTabs(view);
                lastSyncRunKey = runKey;
            }

            var eitherRunning = result.IsRunning || (hscResult && hscResult.IsRunning);
            setStatusPollRate(view, eitherRunning || !!(tlResult && tlResult.IsRunning));
            if (eitherRunning) {
                if (btnSave) { btnSave.disabled = true; btnSave.style.opacity = "0.5"; btnSave.querySelector('span').textContent = "Sync in progress..."; }
                if (btnRun) btnRun.disabled = true;
            } else {
                if (btnRun) btnRun.disabled = false;
                if (btnSave) {
                    btnSave.querySelector('span').textContent = "Save Settings";
                    checkFormState();
                }
            }

            // While a run is busy: what it is doing now and for how long (sync / Run Group,
            // Home Screen Sync, top-list sync), e.g. "Running · Playlists — Top 10: 34/88 users · 1m 12s".
            var live = null;
            if (result.IsRunning && result.Step) live = result;
            else if (hscResult && hscResult.IsRunning && hscResult.Step) live = hscResult;
            else if (tlResult && tlResult.Step) live = tlResult;
            var liveText = live ? runningStepText(live, live === hscResult) : '';
            if (label) { label.textContent = liveText || result.LastRunStatus || "Never"; label.title = liveText || result.LastRunStatus || ""; }
            if (dot) {
                dot.className = "status-dot";
                var st = liveText || result.LastRunStatus || '';
                if (st.includes("Running")) dot.classList.add("running");
                else if (/failed|error/i.test(st)) dot.classList.add("failed");
                else if (/warning/i.test(st)) dot.classList.add("warn");
            }

            _lastStatus = { sync: result, hsc: hscResult, tl: tlResult };
            if (content) renderLogModal(view);
        }).catch(function () {
            if (myId !== statusRequestId) return;
        });
    }

    // A source row's place in the manual order: the order remembered by sortRows, else its place
    // in the saved config. A new row (no saved place) counts as first, where it was added.
    function manualRank(row) {
        if (row.dataset.manualOrder != null) return +row.dataset.manualOrder;
        var i = parseInt(row.dataset.index, 10);
        return isNaN(i) || i === 9999 ? -1 : i;
    }

    // The source rows in their manual order, whatever order the list shows (Name, Active, ...),
    // so saving never replaces the manual order with a display sort.
    function sourceRowsInManualOrder(view) {
        var container = view.querySelector('#tagListContainer');
        var rows = container ? Array.from(container.querySelectorAll('.tag-row')) : [];
        if (container && container.classList.contains('sort-hidden'))
            rows = rows.map(function (r, i) { return { r: r, i: i }; })
                .sort(function (a, b) { return (manualRank(a.r) - manualRank(b.r)) || (a.i - b.i); })
                .map(function (x) { return x.r; });
        return rows;
    }

    function sortRows(container, criteria) {
        var rows = Array.from(container.querySelectorAll('.tag-row'));
        // While the list is in manual order its DOM order is the user's order (moved and new rows
        // included, saved or not): remember it, so switching back to Manual restores it.
        if (!container.classList.contains('sort-hidden')) rows.forEach(function (r, i) { r.dataset.manualOrder = i; });

        rows.sort((a, b) => {
            if (criteria === 'Name') {
                var na = (a.querySelector('.txtEntryLabel').value || a.querySelector('.txtTagName').value).toLowerCase();
                var nb = (b.querySelector('.txtEntryLabel').value || b.querySelector('.txtTagName').value).toLowerCase();
                return natCmp(na, nb);
            }
            if (criteria === 'Active') {
                var aa = a.querySelector('.chkTagActive').checked ? 1 : 0;
                var bb = b.querySelector('.chkTagActive').checked ? 1 : 0;
                return bb - aa;
            }
            if (criteria === 'LatestEdited') {
                var da = new Date(a.dataset.lastModified || 0).getTime();
                var db = new Date(b.dataset.lastModified || 0).getTime();
                return db - da;
            }
            return manualRank(a) - manualRank(b);
        });

        rows.forEach(row => container.appendChild(row));

        if (criteria !== 'Manual') container.classList.add('sort-hidden');
        else container.classList.remove('sort-hidden');
    }

    // ---- Home Section Tab helpers ----
    // One user fetch per page load, shared by every user list. Each user carries its parental
    // rating limit (Policy.MaxParentalRating, null = no limit) and the item types it blocks when
    // unrated, plus the rating's display name from the server's parental-ratings list.
    var _hseUsersCache = null;
    var _hseUsersPromise = null;
    function getHseUsers() {
        if (_hseUsersCache) return Promise.resolve(_hseUsersCache);
        if (_hseUsersPromise) return _hseUsersPromise;
        var ratingsP = Promise.all([
            window.ApiClient.getJSON(window.ApiClient.getUrl('Localization/ParentalRatings')).catch(function () { return []; }),
            window.ApiClient.getJSON(window.ApiClient.getUrl('System/Configuration')).catch(function () { return {}; })
        ]);
        // Disabled users too: they stay selected (marked "Disabled") so a save keeps them; the sync
        // leaves them out until they are enabled again.
        _hseUsersPromise = Promise.all([window.ApiClient.getJSON(window.ApiClient.getUrl('Users')), ratingsP])
            .then(function(res) {
                var ratings = res[1][0] || [], country = ((res[1][1] || {}).MetadataCountryCode || '').toUpperCase();
                _hseUsersCache = (res[0] || []).map(function(u) {
                    var p = u.Policy || {};
                    var max = (p.MaxParentalRating === null || p.MaxParentalRating === undefined) ? null : Number(p.MaxParentalRating);
                    return { Id: u.Id, Name: u.Name, MaxParentalRating: max, IsDisabled: !!p.IsDisabled,
                        RatingName: max === null ? '' : parentalRatingName(max, ratings, country),
                        BlockUnratedItems: (p.BlockUnratedItems || []).slice() };
                }).sort(byName);
                return _hseUsersCache;
            })
            .catch(function (e) { _hseUsersPromise = null; throw e; });
        return _hseUsersPromise;
    }

    // Emby's parental-ratings list mixes countries: US names have no prefix, others are "GB-12",
    // "ZA-13"… Several names share one value. Pick the server's country (prefix stripped), else a
    // common US film rating, else the shortest name; a value with no name uses the next lower one.
    var US_COMMON_RATINGS = ['G', 'PG', 'PG-13', 'R', 'NC-17', 'TV-Y', 'TV-Y7', 'TV-Y7-FV', 'TV-14', 'TV-MA', 'X'];
    function parentalRatingName(value, ratings, country) {
        // "GB-12" → "GB"; US names such as PG-13, TV-14 and NC-17 have no country prefix.
        function prefixOf(n) { var m = /^([A-Z]{2})-/.exec(n); return m && ['PG', 'TV', 'NC'].indexOf(m[1]) === -1 ? m[1] : ''; }
        function best(list) {
            if (!list.length) return '';
            var local = country && country !== 'US' ? list.filter(function (r) { return prefixOf(r.Name) === country; }) : [];
            // Several local names on one value (ZA-7–9PG, ZA-10–12PG): the user may watch all of
            // them, so show the highest (the list is ordered from mild to strict).
            if (local.length) return local[local.length - 1].Name.slice(3);
            var plain = list.filter(function (r) { return !prefixOf(r.Name); });
            for (var i = 0; i < US_COMMON_RATINGS.length; i++) {
                for (var j = 0; j < plain.length; j++) if (plain[j].Name === US_COMMON_RATINGS[i]) return plain[j].Name;
            }
            var pool = (plain.length ? plain.map(function (r) { return r.Name; }) : list.map(function (r) { return prefixOf(r.Name) ? r.Name.slice(3) : r.Name; }));
            return pool.sort(function (a, b) { return a.length - b.length; })[0];
        }
        var exact = ratings.filter(function (r) { return r.Value === value; });
        if (exact.length) return best(exact);
        var lower = ratings.filter(function (r) { return r.Value < value; });
        if (lower.length) {
            var top = Math.max.apply(null, lower.map(function (r) { return r.Value; }));
            return best(lower.filter(function (r) { return r.Value === top; }));
        }
        return String(value);
    }

    // Strictness band for the chip colour: kids (up to PG / 7), teens (12–15), adults (16+).
    // Uses the age in the name when it has one (e.g. "16"), else Emby's rating value
    // (5 = PG, 6–8 = 12 to 15, 9+ = 18).
    function userRatingBand(u) {
        var ages = (u.RatingName || '').match(/\d{1,2}/g);
        if (ages && !/^TV-Y7/.test(u.RatingName)) {
            var a = Math.max.apply(null, ages.map(Number));
            return a <= 9 ? 'kid' : a <= 15 ? 'teen' : 'adult';
        }
        var v = u.MaxParentalRating;
        return v <= 5 ? 'kid' : v <= 8 ? 'teen' : 'adult';
    }

    function isUserRestricted(u) { return !!u && u.MaxParentalRating !== null && u.MaxParentalRating !== undefined; }

    // Small coloured chip after a user's name: their max parental rating (none when unlimited)
    // and a lock when they block unrated items. Inline styles: these lists also render in
    // dialogs appended to <body>. Solid fills with white text read in light and dark themes.
    var RATING_CHIP_COLORS = { kid: '#2E7D32', teen: '#C77700', adult: '#C62828' };
    function userRatingChipHtml(u) {
        function esc(s) { return String(s || '').replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); }
        var html = '';
        if (isUserRestricted(u)) {
            html += '<span class="hsc-rating-chip" title="Max rating: ' + esc(u.RatingName) + '" style="display:inline-block;margin-left:6px;padding:0 5px;' +
                'border-radius:3px;font-size:0.72em;font-weight:700;line-height:1.5;vertical-align:middle;white-space:nowrap;color:#fff;background:' +
                RATING_CHIP_COLORS[userRatingBand(u)] + ';">' + esc(u.RatingName) + '</span>';
        }
        if (u && u.BlockUnratedItems && u.BlockUnratedItems.length) {
            html += '<span class="hsc-rating-lock" title="Blocks unrated: ' + esc(u.BlockUnratedItems.join(', ')) + '" style="margin-left:4px;font-size:0.8em;vertical-align:middle;cursor:help;">&#128274;</span>';
        }
        if (u && u.IsDisabled) {
            html += '<span class="hsc-disabled-chip" title="Disabled in Emby: stays selected, but HSC leaves this user out until they are enabled again" style="display:inline-block;margin-left:6px;padding:0 5px;' +
                'border-radius:3px;font-size:0.72em;font-weight:700;line-height:1.4;vertical-align:middle;white-space:nowrap;border:1px solid var(--hsc-hairline);opacity:0.75;cursor:help;">Disabled</span>';
        }
        return html;
    }

    // Same information as plain text, for <option>s (a native select cannot show the chip).
    function userRatingSuffix(u) {
        var s = isUserRestricted(u) ? ' \u00B7 ' + u.RatingName : '';
        if (u && u.BlockUnratedItems && u.BlockUnratedItems.length) s += ' \uD83D\uDD12';
        if (u && u.IsDisabled) s += ' \u00B7 disabled';
        return s;
    }

    // Quick picks next to "Select all", shared by the dropdown and the plain user lists: only the
    // users without / with a parental rating limit.
    function userSelectLinksInnerHtml() {
        return '<span style="opacity:0.6;">Select only:</span>' +
            '<a href="#" class="hsc-user-select-unrestricted" style="color:var(--hsc-primary-text);" title="Select only users without a parental rating limit">Unrestricted</a>' +
            '<a href="#" class="hsc-user-select-restricted" style="color:var(--hsc-primary-text);" title="Select only users with a parental rating limit">Restricted</a>';
    }

    // Applies one of the quick picks to a list of user checkboxes (never the Select all box); one
    // change event afterwards so the list's own listeners (and checkFormState) run once.
    function applyUserQuickSelect(link, boxes) {
        var restricted = link.classList.contains('hsc-user-select-restricted');
        boxes = boxes.filter(function (chk) { return !chk.classList.contains('hsc-select-all'); });
        boxes.forEach(function (chk) {
            chk.checked = (chk.dataset.restricted === '1') === restricted;
        });
        if (boxes.length) boxes[boxes.length - 1].dispatchEvent(new Event('change', { bubbles: true }));
    }

    // "Select all" row for a list of user checkboxes. Its checkbox has no value, and the
    // hsc-select-all class keeps it out of every selector that reads the chosen users.
    function buildSelectAllRowHtml(rowClass) {
        return '<div class="' + (rowClass || 'checkboxContainer') + ' hsc-select-all-row" style="margin:2px 0 4px;padding-bottom:4px;border-bottom:1px solid var(--line-color,rgba(128,128,128,0.2));">' +
            '<label style="display:flex;align-items:center;gap:10px;cursor:pointer;width:100%;">' +
            '<input type="checkbox" is="emby-checkbox" class="hsc-select-all" />' +
            '<span style="font-weight:600;">Select all</span></label></div>';
    }

    // Wires the select-all box in container to the checkboxes matching itemSelector. Each box
    // it flips fires its own change event, so dirty checks and button states react as if the
    // user had clicked it. Disabled boxes (the Home Screen Sync source user) are left alone.
    // Returns a function that re-reads the state after boxes were changed from code.
    function wireSelectAll(container, itemSelector) {
        var all = container && container.querySelector('.hsc-select-all');
        if (!all) return function () {};
        function items() {
            return Array.from(container.querySelectorAll(itemSelector)).filter(function (c) { return !c.disabled; });
        }
        function sync() {
            var list = items();
            var n = list.filter(function (c) { return c.checked; }).length;
            all.checked = list.length > 0 && n === list.length;
            all.indeterminate = n > 0 && n < list.length;
        }
        all.addEventListener('change', function () {
            var want = all.checked;
            items().forEach(function (c) {
                if (c.checked === want) return;
                c.checked = want;
                c.dispatchEvent(new Event('change', { bubbles: true }));
            });
            sync();
        });
        container.addEventListener('change', function (e) {
            if (e.target !== all && e.target.matches && e.target.matches(itemSelector)) sync();
        });
        sync();
        return sync;
    }

    function buildUserMultiSelectHtml(users, selectedIds, checkboxClass) {
        function escAttr(s) { return String(s || '').replace(/&/g, '&amp;').replace(/"/g, '&quot;'); }
        function escHtml(s) { return String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); }
        if (!users || users.length === 0) return '<em style="opacity:0.5">No users found</em>';
        var sel = selectedIds || [];
        var rows = buildSelectAllRowHtml() + users.map(function(u) {
            var chk = sel.indexOf(u.Id) !== -1 ? ' checked' : '';
            return '<div class="checkboxContainer" style="margin:2px 0;">' +
                '<label><input type="checkbox" is="emby-checkbox" class="' + escAttr(checkboxClass) + '" value="' + escAttr(u.Id) + '" data-name="' + escAttr(u.Name) + '" data-restricted="' + (isUserRestricted(u) ? '1' : '0') + '"' + chk + '>' +
                '<span>' + escHtml(u.Name) + userRatingChipHtml(u) + '</span></label></div>';
        }).join('');
        var checkedNames = users.filter(function(u) { return sel.indexOf(u.Id) !== -1; }).map(function(u) { return u.Name; });
        var lbl = checkedNames.length === 0 ? 'No users selected'
            : checkedNames.length === users.length ? 'All users'
            : checkedNames.join(', ');
        var btnStyle = 'display:flex;align-items:center;width:100%;padding:6px 10px;' +
            'background:var(--plugin-input-bg,rgba(128,128,128,0.08));' +
            'border:1px solid var(--plugin-input-border,var(--line-color));' +
            'border-radius:6px;font-size:0.9em;color:inherit;cursor:pointer;' +
            'box-sizing:border-box;text-align:left;';
        return '<div class="filter-dropdown-wrapper hsc-user-dropdown" style="width:100%;">' +
            '<button type="button" class="hsc-user-dropdown-btn" style="' + btnStyle + '">' +
            '<span class="hsc-user-dropdown-label" style="flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">' + escHtml(lbl) + '</span>' +
            '<i class="md-icon hsc-user-dropdown-caret" style="font-size:1em;margin-left:6px;flex-shrink:0;">expand_more</i>' +
            '</button>' +
            '<div class="filter-dropdown-panel" style="min-width:220px;width:100%;box-sizing:border-box;">' +
            '<div style="display:flex;flex-wrap:wrap;gap:4px 12px;padding:2px 0 6px 0;margin-bottom:4px;border-bottom:1px solid var(--line-color);font-size:0.85em;">' +
            userSelectLinksInnerHtml() + '</div>' +
            rows + '</div>' +
            '</div>';
    }

    function wireUserMultiSelect(container) {
        var wrapper = container && container.querySelector('.hsc-user-dropdown');
        if (!wrapper) return;
        var btn    = wrapper.querySelector('.hsc-user-dropdown-btn');
        var panel  = wrapper.querySelector('.filter-dropdown-panel');
        var lbl    = wrapper.querySelector('.hsc-user-dropdown-label');
        var caret  = wrapper.querySelector('.hsc-user-dropdown-caret');
        var userBoxSelector = 'input[type="checkbox"]:not(.hsc-select-all)';
        function updateLabel() {
            var allBoxes     = panel.querySelectorAll(userBoxSelector);
            var checkedBoxes = panel.querySelectorAll(userBoxSelector + ':checked');
            if (checkedBoxes.length === 0) {
                lbl.textContent = 'No users selected';
            } else if (checkedBoxes.length === allBoxes.length) {
                lbl.textContent = 'All users';
            } else {
                lbl.textContent = Array.from(checkedBoxes).map(function(cb) {
                    return cb.dataset.name || '';
                }).join(', ');
            }
        }
        btn.addEventListener('click', function(e) {
            e.stopPropagation();
            var open = panel.classList.toggle('open');
            caret.textContent = open ? 'expand_less' : 'expand_more';
        });
        panel.querySelectorAll(userBoxSelector).forEach(function(chk) {
            chk.addEventListener('change', updateLabel);
        });
        wireSelectAll(panel, userBoxSelector);
        // Unrestricted / Restricted: one change event afterwards so form-state listeners run once.
        panel.querySelectorAll('.hsc-user-select-unrestricted, .hsc-user-select-restricted').forEach(function(link) {
            link.addEventListener('click', function(e) {
                e.preventDefault();
                e.stopPropagation();
                applyUserQuickSelect(link, Array.from(panel.querySelectorAll('input[type="checkbox"]')));
            });
        });
        document.addEventListener('click', function closeUserDrop(e) {
            if (!wrapper.isConnected) { document.removeEventListener('click', closeUserDrop); return; }
            if (!panel.contains(e.target) && e.target !== btn) {
                panel.classList.remove('open');
                caret.textContent = 'expand_more';
            }
        });
    }

    var _hseLibraryCachePromise = null;
    function preFetchLibraryData() {
        if (_hseLibraryCachePromise) return _hseLibraryCachePromise;
        var tok = window.ApiClient.accessToken ? window.ApiClient.accessToken() : '';
        _hseLibraryCachePromise = Promise.all([
            fetch(window.ApiClient.getUrl('HomeScreenCompanion/TopList/List'), { headers: { 'X-MediaBrowser-Token': tok } })
                .then(function(r) { return r.json(); }).catch(function() { return { FolderNames: [] }; }),
            fetch(window.ApiClient.getUrl('Library/VirtualFolders'), { headers: { 'X-MediaBrowser-Token': tok } })
                .then(function(r) { return r.json(); }).catch(function() { return []; })
        ]).then(function(results) {
            return {
                topListFolderNames: new Set((results[0].FolderNames || []).map(function(n) { return n.toLowerCase(); })),
                virtualFolders: (results[1] || []).slice().sort(byName)
            };
        }).catch(function() {
            _hseLibraryCachePromise = null; // tillåt omförsök vid fel
            return { topListFolderNames: new Set(), virtualFolders: [] };
        });
        return _hseLibraryCachePromise;
    }



    // Emby's per-section card size (ContentSection.CardSizeOffset): negative = smaller cards,
    // more per row. "0" (not "") is Default, so switching back really resets the section.
    var CARD_SIZE_OPTIONS = [['0', 'Default'], ['-1', 'Smaller'], ['-2', 'Smallest'], ['1', 'Larger']];
    function normCardSize(v) {
        var n = parseInt(v, 10);
        return isNaN(n) ? '0' : String(n);
    }
    function cardSizeOptionsHtml(selected) {
        var sel = normCardSize(selected);
        return CARD_SIZE_OPTIONS.map(function (o) {
            return '<option value="' + o[0] + '"' + (sel === o[0] ? ' selected' : '') + '>' + o[1] + '</option>';
        }).join('');
    }

    var PLAYLIST_SECTION_LABEL = "This source's playlist (each user's own)";

    // A home row's identity across users (same as HomeScreenCompanionTask.SectionKey): its custom
    // title, else its type and library.
    // A tag row outside its schedule has this before its title in Emby (HomeScreenCompanionTask.InactivePrefix).
    var HSE_INACTIVE_PREFIX = '[INACTIVE] ';
    function stripInactive(title) {
        title = title || '';
        return title.toUpperCase().indexOf(HSE_INACTIVE_PREFIX) === 0 ? title.substring(HSE_INACTIVE_PREFIX.length) : title;
    }

    function hseSectionKey(s) {
        var cn = stripInactive(s.CustomName).trim();
        return cn ? 'name:' + cn.toLowerCase() : 'type:' + (s.SectionType || '').toLowerCase() + '|' + (s.ParentId || '');
    }

    var HSC_POSITION_DESC = 'Where the row goes when it is made, and where every user\'s row moves to when you change this. ' +
        'A user who does not have the chosen row gets it next to the nearest row around it that they do have. ' +
        'A row moved by hand in Emby\'s home screen settings keeps its place until you change this setting.';

    // The row a saved "before:" / "after:" key points at, shown before the row list has loaded.
    function hsePositionKeyLabel(key) {
        if (key.indexOf('name:') === 0) return key.substring(5);
        return key.substring(5).split('|')[0];
    }

    // Position of a home row: Bottom, Top, or Before / After one of the rows. The hidden
    // .hsc-pos-value holds it ("", "top", "before:<key>", "after:<key>"); hiddenAttrs makes it a
    // form field (a source's data-field="_position"). labelStyle: inline label style (popups), else
    // the page's selectLabel.
    function positionFieldHtml(posVal, hiddenAttrs, labelStyle, desc) {
        posVal = posVal || '';
        var mode = posVal === 'top' ? 'top' : posVal.indexOf('before:') === 0 ? 'before' : posVal.indexOf('after:') === 0 ? 'after' : '';
        var key = (mode === 'before' || mode === 'after') ? posVal.substring(mode.length + 1) : '';
        var lbl = labelStyle ? ' style="' + labelStyle + '"' : ' class="selectLabel"';
        return '<div class="hsc-pos-field">' +
            '<input type="hidden" class="hsc-pos-value"' + (hiddenAttrs || '') + ' value="' + escapeHtml(posVal) + '" />' +
            '<label' + lbl + '>Position</label>' +
            '<select is="emby-select" class="hsc-pos-mode" style="width:100%;">' +
            [['', 'Bottom (default)'], ['top', 'Top'], ['before', 'Before a row'], ['after', 'After a row']].map(function (o) {
                return '<option value="' + o[0] + '"' + (mode === o[0] ? ' selected' : '') + '>' + o[1] + '</option>';
            }).join('') +
            '</select>' +
            '<div class="hsc-pos-row-wrap" style="margin-top:8px;' + (mode === 'before' || mode === 'after' ? '' : 'display:none;') + '">' +
            '<label' + lbl + '>Row</label>' +
            '<select is="emby-select" class="hsc-pos-row" style="width:100%;">' +
            '<option value=""' + (key ? '' : ' selected') + '>— Choose a row —</option>' +
            (key ? '<option value="' + escapeHtml(key) + '" selected>' + escapeHtml(hsePositionKeyLabel(key)) + '</option>' : '') +
            '</select></div>' +
            (desc ? '<div class="fieldDescription">' + desc + '</div>' : '') +
            '</div>';
    }

    // Keeps the hidden value in step with the two pickers (before their change reaches the page).
    function wirePositionField(root) {
        var f = root && root.querySelector('.hsc-pos-field');
        if (!f || f.dataset.wired) return;
        f.dataset.wired = '1';
        var mode = f.querySelector('.hsc-pos-mode'), row = f.querySelector('.hsc-pos-row');
        var wrap = f.querySelector('.hsc-pos-row-wrap'), val = f.querySelector('.hsc-pos-value');
        function update() {
            var m = mode.value, needsRow = m === 'before' || m === 'after';
            wrap.style.display = needsRow ? '' : 'none';
            val.value = needsRow ? (row.value ? m + ':' + row.value : '') : m;
        }
        mode.addEventListener('change', update);
        row.addEventListener('change', update);
    }

    function readPositionValue(root) {
        var v = root && root.querySelector('.hsc-pos-field .hsc-pos-value');
        return v ? v.value : '';
    }

    // Lists the rows on the user's home screen as choices for Before / After (the row being placed
    // left out: ownName, its title). The saved choice stays when the list cannot be loaded.
    function fillPositionRows(root, userId, ownName) {
        var sel = root && root.querySelector('.hsc-pos-field .hsc-pos-row');
        if (!sel || !userId) return;
        var headers = {};
        var token = window.ApiClient.accessToken && window.ApiClient.accessToken();
        if (token) headers['X-Emby-Token'] = token;
        fetch(window.ApiClient.getUrl('HomeScreenCompanion/Hsc/UserSections', { UserId: userId }), { headers: headers })
            .then(function (r) { return r.json(); })
            .then(function (data) {
                var own = typeof ownName === 'function' ? ownName() : ownName;
                own = (own || '').trim().toLowerCase();
                var current = sel.value;
                ((data && data.Sections) || []).forEach(function (s, i) {
                    var key = hseSectionKey(s);
                    if (own && key === 'name:' + own) return;
                    var label = stripInactive(s.CustomName || '') || s.Name || s.SectionType || ('Row ' + (i + 1));
                    var existing = Array.from(sel.options).find(function (o) { return o.value === key; });
                    if (existing) { existing.textContent = label; return; }
                    var o = document.createElement('option');
                    o.value = key; o.textContent = label;
                    sel.appendChild(o);
                });
                sel.value = current;
            })
            .catch(function () { /* the saved choice stays; Bottom and Top still work */ });
    }

    function buildHomeSectionFormHtml(savedSettings, defaultSectionType, defaultName, tagEnabled, collEnabled, libraryOptions, savedLibraryId, allLibraries, plEnabled) {
        var s = savedSettings || {};
        var html = '';

        var st = s.SectionType || defaultSectionType || (collEnabled ? 'boxset' : 'items');
        html += '<div class="hse-st-field hse-nw-hide hse-tl-hide" style="margin-bottom:12px;"><label class="selectLabel">Section Type</label>';
        html += '<select is="emby-select" class="selHseSectionType hse-field-str" data-field="SectionType" style="width:100%;">';
        var sectionTypeOptions = [];
        if (collEnabled) sectionTypeOptions.push(['boxset', 'Single Collection']);
        if (tagEnabled)  sectionTypeOptions.push(['items',  'Dynamic Media (tag)']);
        if (plEnabled)   sectionTypeOptions.push(['playlist', PLAYLIST_SECTION_LABEL]);
        sectionTypeOptions.forEach(function(o) {
            html += '<option value="' + o[0] + '"' + (st === o[0] ? ' selected' : '') + '>' + o[1] + '</option>';
        });
        html += '</select></div>';

        var displayModeVal = s.DisplayMode || '';
        html += '<div class="hsc-adv" data-adv-def="" style="margin-bottom:12px;"><label class="selectLabel">Show this section' + ADV_BADGE + '</label>';
        html += '<select is="emby-select" class="hse-field-str" data-field="DisplayMode" style="width:100%;">';
        html += '<option value=""'              + (displayModeVal === ''               ? ' selected' : '') + '>Always</option>';
        html += '<option value="tv"'            + (displayModeVal === 'tv'             ? ' selected' : '') + '>When TV Display Mode is on</option>';
        html += '<option value="mobile,desktop"'+ (displayModeVal === 'mobile,desktop' ? ' selected' : '') + '>When TV Display Mode is off</option>';
        html += '</select></div>';

        var savedItemTypes = [];
        try { savedItemTypes = JSON.parse(s.ItemTypes || '[]'); } catch {}
        var savedItemTypesStr = savedItemTypes.length > 0 ? savedItemTypes.join(',') : 'Movie,Series';
        html += '<div class="hse-items-only hse-nw-hide hse-tl-hide" style="margin-bottom:12px;"><label class="selectLabel">Media Type</label>';
        html += '<select is="emby-select" class="selHseItemTypes" style="width:100%;">';
        var itemTypeOptions = [
                ['Movie',        'Movies'],
                ['Series',       'Shows'],
                ['Movie,Series', 'Movies & Shows'],
                ['Episode',      'Episodes'],
                ['BoxSet',       'Collections'],
                ['MusicVideo',   'Music Videos'],
                ['Video',        'Videos'],
                ['Photo',        'Photos'],
                ['Program',      'Programs'],
                ['TvChannel',    'Live TV Channels'],
                ['MusicAlbum',   'Music Albums'],
                ['MusicArtist',  'Artists'],
                ['Audio',        'Songs'],
                ['AudioBook',    'Audiobooks'],
                ['Trailer',      'Trailers'],
                ['Game',         'Games'],
                ['Book',         'Books']
              ];
        itemTypeOptions.forEach(function(o) {
            html += '<option value="' + o[0] + '"' + (savedItemTypesStr === o[0] ? ' selected' : '') + '>' + o[1] + '</option>';
        });
        html += '</select>';
        html += '<div class="fieldDescription hse-playlist-only hse-tl-hide">Playlist rows show the whole playlist: <em>Media Type</em> and <em>Playstate</em> are not used (filter the source itself instead).</div>';
        html += '</div>';

        var customName = ((s.CustomName || '').trim() === (defaultName || '').trim() ? '' : (s.CustomName || '')).replace(/"/g, '&quot;');
        var customNamePlaceholder = (defaultName || '').replace(/"/g, '&quot;');
        html += '<div style="margin-bottom:12px;"><input is="emby-input" type="text" class="hse-field-str" data-field="CustomName" label="Custom Title" value="' + customName + '" placeholder="' + customNamePlaceholder + '"/></div>';

        // Where the row goes. The Row list is the first selected user's rows (filled in by
        // fillPositionRows); the saved choice is shown until that list has loaded.
        html += '<div style="margin-bottom:12px;">' + positionFieldHtml(s._position || '', ' data-field="_position"', '', HSC_POSITION_DESC) + '</div>';

        // Emby native uses "" for Cards (default); migrate old stored "cards" value
        var viewTypeVal = (s.ViewType === 'cards' ? '' : s.ViewType) || '';
        html += '<div class="hse-items-only hse-tl-hide" style="margin-bottom:12px;"><label class="selectLabel">View Type</label>';
        html += '<select is="emby-select" class="selHseViewType hse-field-str" data-field="ViewType" style="width:100%;">';
        [['','Cards (default)'],['spotlight','Spotlight']].forEach(function(o) {
            html += '<option value="' + o[0] + '"' + (viewTypeVal === o[0] ? ' selected' : '') + '>' + o[1] + '</option>';
        });
        html += '</select></div>';

        // Your Next Watch: Primary = show cards use the show's poster (Auto would show the episode image).
        var imgTypeVal = s.ImageType || (st === 'playlist' ? 'Primary' : '');
        var imgTypeDisabled = viewTypeVal === 'spotlight';
        html += '<div style="margin-bottom:12px;"><label class="selectLabel">Image Type</label>';
        html += '<select is="emby-select" class="selHseImageType hse-field-str" data-field="ImageType" style="width:100%;"' + (imgTypeDisabled ? ' disabled' : '') + '><option value=""' + (imgTypeVal === '' ? ' selected' : '') + '>Auto</option>';
        ['Primary','Thumb'].forEach(function(o) { html += '<option value="' + o + '"' + (imgTypeVal === o ? ' selected' : '') + '>' + o + '</option>'; });
        html += '</select>';
        html += '<div class="fieldDescription hse-playlist-only hse-tl-hide">Playlist rows: <em>Auto</em> and <em>Primary</em> show a show\'s poster on its card (the playlist holds its first episode); <em>Thumb</em> shows the show\'s landscape image.</div>';
        html += '</div>';

        html += '<div class="hsc-adv" data-adv-def="0" style="margin-bottom:12px;"><label class="selectLabel">Card Size' + ADV_BADGE + '</label>';
        html += '<select is="emby-select" class="selHseCardSize hse-field-str" data-field="CardSizeOffset" style="width:100%;">' + cardSizeOptionsHtml(s.CardSizeOffset) + '</select></div>';

        var sortByVal = s.SortBy || '';
        html += '<div class="hse-nw-hide hse-tl-hide" style="margin-bottom:12px;"><label class="selectLabel">Sort By</label>';
        html += '<select is="emby-select" class="hse-field-str" data-field="SortBy" style="width:100%;">';
        html += '<option value=""' + (sortByVal === '' ? ' selected' : '') + '>(Default)</option>';
        [
            ['CommunityRating,SortName',            'Rating'],
            ['DateCreated,SortName',                'Date Added'],
            ['SortName',                            'Name'],
            ['Runtime,SortName',                    'Runtime'],
            ['ProductionYear,PremiereDate,SortName','Release Date'],
            ['ProductionYear,SortName',             'Year'],
            ['Random',                              'Random']
        ].forEach(function(o) {
            html += '<option value="' + o[0] + '"' + (sortByVal === o[0] ? ' selected' : '') + '>' + o[1] + '</option>';
        });
        html += '</select>';
        html += '<div class="fieldDescription hse-playlist-only hse-tl-hide">Playlist rows: <em>(Default)</em> keeps the playlist order (Your Next Watch: best match first). <em>Random</em> shuffles the row every time the home screen loads, so it shows a different few of the list each time.</div>';
        html += '</div>';

        var sortOrderVal = s.SortOrder || '';
        html += '<div class="hse-nw-hide hse-tl-hide" style="margin-bottom:12px;"><label class="selectLabel">Sort Order</label>';
        html += '<select is="emby-select" class="hse-field-str" data-field="SortOrder" style="width:100%;">';
        html += '<option value=""' + (sortOrderVal === '' ? ' selected' : '') + '>(Default)</option>';
        [['Ascending','Ascending'],['Descending','Descending']].forEach(function(o) { html += '<option value="' + o[0] + '"' + (sortOrderVal === o[0] ? ' selected' : '') + '>' + o[1] + '</option>'; });
        html += '</select></div>';

        var dispModeVal = s.ScrollDirection || '';
        html += '<div class="hsc-adv" data-adv-def="" style="margin-bottom:12px;"><label class="selectLabel">Scroll Direction' + ADV_BADGE + '</label>';
        html += '<select is="emby-select" class="hse-field-str" data-field="ScrollDirection" style="width:100%;"><option value="">(Auto)</option>';
        [['Horizontal','Horizontal'],['Vertical','Vertical']].forEach(function(o) { html += '<option value="' + o[0] + '"' + (dispModeVal === o[0] ? ' selected' : '') + '>' + o[1] + '</option>'; });
        html += '</select></div>';

        var playstateVal = s._queryIsPlayed || '';
        html += '<div class="hse-items-only hse-nw-hide hse-tl-hide" style="margin-bottom:12px;"><label class="selectLabel">Playstate</label>';
        html += '<select is="emby-select" class="hse-field-str" data-field="_queryIsPlayed" style="width:100%;">';
        html += '<option value=""' + (playstateVal === '' ? ' selected' : '') + '>Any</option>';
        [['true','Played'],['false','Unplayed']].forEach(function(o) { html += '<option value="' + o[0] + '"' + (playstateVal === o[0] ? ' selected' : '') + '>' + o[1] + '</option>'; });
        html += '</select></div>';

        // Hidden library selector — used only for boxset type (preserves the collection library ID).
        var curLibId = savedLibraryId || 'auto';
        html += '<select class="selHseLibrary" style="display:none;">';
        html += '<option value="auto"' + (curLibId === 'auto' ? ' selected' : '') + '>auto</option>';
        (libraryOptions || []).forEach(function (lib) {
            html += '<option value="' + lib.id + '"' + (curLibId === lib.id ? ' selected' : '') + '>' + lib.name + '</option>';
        });
        html += '</select>';

        // Multi-library checkboxes for items type.
        // Excluded IDs are stored in HomeSectionSettings._queryExcludeViewIds → Query.ExcludeUserViewIds in Emby.
        // Checked = include (not excluded), unchecked = exclude.
        if (st !== 'boxset' && (allLibraries || []).length > 0) {
            var excludedIds = new Set();
            try {
                var raw = (s._queryExcludeViewIds || '').split(',').map(function(x) { return x.trim(); }).filter(function(x) { return x; });
                raw.forEach(function(id) { excludedIds.add(id); });
            } catch {}
            // Om inga explicita exkluderingar sparats (ny sektion eller legacy), exkludera alla top-list-bibliotek.
            // Bakåtkompatibelt: befintliga grupper utan _queryExcludeViewIds får top-list auto-avbockat.
            if (excludedIds.size === 0) {
                (allLibraries || []).forEach(function(lib) {
                    if (lib.isTopList) excludedIds.add(lib.id);
                });
            }
            // Hidden checkboxes — library exclusions managed automatically under the hood
            html += '<div style="display:none;">';
            (allLibraries || []).forEach(function (lib) {
                var isChecked = !excludedIds.has(lib.id);
                html += '<input type="checkbox" class="chkHseLibrary" value="' + lib.id + '"' + (isChecked ? ' checked' : '') + '/>';
            });
            html += '</div>';
        }

        return html;
    }

    // Your Next Watch: the row is each user's own playlist (Movies & Shows, best match first), so
    // Section Type, Media Type, Sort By, Sort Order and Playstate are set and hidden.
    function applyNextWatchHomeSection(tab) {
        var row = tab.closest('.tag-row');
        var stSel = tab.querySelector('.selHseSectionType');
        var isNw = !!row && (row.querySelector('.selSourceType') || {}).value === 'NextWatch'
            && !!stSel && Array.from(stSel.options).some(function (o) { return o.value === 'playlist'; });
        if (isNw) {
            stSel.value = 'playlist';
            var itSel = tab.querySelector('.selHseItemTypes');
            if (itSel) itSel.value = 'Movie,Series';
            ['SortBy', 'SortOrder', '_queryIsPlayed'].forEach(function (f) {
                var el = tab.querySelector('[data-field="' + f + '"]');
                if (el) el.value = '';
            });
        }
        return isNw;
    }

    function updateHseItemsOnlyVisibility(tab) {
        var isNw = applyNextWatchHomeSection(tab);
        var stSel = tab.querySelector('.selHseSectionType');
        // Section Type is only a choice when two or more of tag / collection / playlist are on.
        var stField = tab.querySelector('.hse-st-field');
        if (stField) stField.classList.toggle('hse-st-single', !stSel || stSel.options.length < 2);
        var isItems = !stSel || stSel.value !== 'boxset';
        tab.querySelectorAll('.hse-items-only').forEach(function(el) {
            el.style.display = isItems ? '' : 'none';
        });
        var isPlaylist = !!stSel && stSel.value === 'playlist';
        tab.querySelectorAll('.hse-playlist-only').forEach(function(el) {
            el.style.display = isPlaylist ? '' : 'none';
        });
        tab.querySelectorAll('.hse-nw-hide').forEach(function(el) {
            if (isNw) el.style.display = 'none';
            else if (!el.classList.contains('hse-items-only')) el.style.display = '';
        });
    }

    function updateHseImageTypeState(tab) {
        var stSel = tab.querySelector('.selHseSectionType');
        var vtSel = tab.querySelector('.selHseViewType');
        var imgSel = tab.querySelector('.selHseImageType');
        if (!imgSel) return;
        var isItems = !stSel || stSel.value !== 'boxset';
        var isCards = !vtSel || vtSel.value === '' || vtSel.value === 'cards';
        // A top-list row is always cards (its View Type is hidden).
        imgSel.disabled = isItems && !isCards && !tab.classList.contains('tl-mode');
    }

    function wireHomeSectionTypeChange(tab) {
        var stSel = tab.querySelector('.selHseSectionType');
        if (!stSel) return;
        updateHseItemsOnlyVisibility(tab);
        updateHseImageTypeState(tab);
        stSel.addEventListener('change', function() {
            updateHseItemsOnlyVisibility(tab);
            updateHseImageTypeState(tab);
        });
        var vtSel = tab.querySelector('.selHseViewType');
        if (vtSel) {
            vtSel.addEventListener('change', function() {
                updateHseImageTypeState(tab);
            });
        }
    }

    // Hämta live ContentSection från Emby och spegla värdena i formuläret.
    // Returns a Promise that resolves when sync is complete (or immediately if no tracked section).
    function syncHomeSectionFromEmby(tab) {
        var tracked = [];
        try { tracked = JSON.parse(decodeURIComponent(tab.dataset.hseTracked || '%5B%5D')); } catch {}
        var entry = tracked.find(function(t) { return t.SectionId && !t.SectionId.startsWith('hsc__'); });
        if (!entry) return Promise.resolve();

        var syncHeaders = {};
        var syncToken = window.ApiClient && window.ApiClient.accessToken && window.ApiClient.accessToken();
        if (syncToken) syncHeaders['X-Emby-Token'] = syncToken;
        var syncUrl = window.ApiClient
            ? window.ApiClient.getUrl('HomeScreenCompanion/Hsc/UserSections', { UserId: entry.UserId })
            : '/HomeScreenCompanion/Hsc/UserSections?UserId=' + encodeURIComponent(entry.UserId);

        return fetch(syncUrl, { headers: syncHeaders })
            .then(function(r) { return r.json(); })
            .then(function(data) {
                var sections = (data && data.Sections) || [];
                var section = sections.find(function(s) { return s.Id === entry.SectionId; });
                if (!section) return;

                // Fält som kan läsas tillbaka från Emby och ha ett formulärfält
                var fieldMap = {
                    SectionType:     section.SectionType || '',
                    CustomName:      stripInactive(section.CustomName),
                    DisplayMode:     section.DisplayMode || '',
                    ViewType:        section.ViewType || '',
                    ImageType:       section.ImageType || '',
                    CardSizeOffset:  normCardSize(section.CardSizeOffset),
                    SortBy:          section.SortBy || '',
                    SortOrder:       section.SortOrder || '',
                    ScrollDirection: (function() {
                        var sd = section.ScrollDirection;
                        if (sd === null || sd === undefined) return '';
                        if (typeof sd === 'number') return sd === 0 ? 'Horizontal' : sd === 1 ? 'Vertical' : '';
                        return String(sd);
                    })(),
                    '_queryIsPlayed': (section.Query != null && section.Query.IsPlayed === true) ? 'true'
                                    : (section.Query != null && (section.Query.IsUnplayed === true || section.Query.IsPlayed === false)) ? 'false'
                                    : ''
                };

                Object.keys(fieldMap).forEach(function(field) {
                    var el = tab.querySelector('[data-field="' + field + '"]');
                    if (!el) return;
                    var val = fieldMap[field];
                    if (el.tagName === 'SELECT') {
                        for (var i = 0; i < el.options.length; i++) {
                            if (el.options[i].value === val) { el.selectedIndex = i; break; }
                        }
                    } else {
                        el.value = val;
                    }
                });

                // Sync ItemTypes dropdown separately (not a data-field element)
                var itemTypesSel = tab.querySelector('.selHseItemTypes');
                if (itemTypesSel && section.ItemTypes && section.ItemTypes.length > 0) {
                    var itemTypesStr = section.ItemTypes.join(',');
                    for (var i = 0; i < itemTypesSel.options.length; i++) {
                        if (itemTypesSel.options[i].value === itemTypesStr) { itemTypesSel.selectedIndex = i; break; }
                    }
                }

                // Sync library checkboxes från Emby's ContentSection.ExcludedFolders (källan till sanning).
                // Om Emby returnerar en array (även tom) används den — annars behålls UI:ts nuvarande state.
                console.log('[HSC] syncHomeSectionFromEmby ExcludedFolders:', section.ExcludedFolders);
                if (Array.isArray(section.ExcludedFolders)) {
                    var embyExcluded = new Set((section.ExcludedFolders || []).map(function(id) { return String(id); }));
                    tab.querySelectorAll('.chkHseLibrary').forEach(function(chk) {
                        chk.checked = !embyExcluded.has(chk.value);
                    });
                }

                updateHseItemsOnlyVisibility(tab);
                updateHseImageTypeState(tab);
            })
            .catch(function() { return undefined; }); // ignorera nätverksfel tyst
    }



    function initPlaylistTab(row) {
        var tab = row.querySelector('.playlist-tab');
        if (!tab || tab.dataset.plLoaded === '1') return;
        tab.dataset.plLoaded = '1';
        var savedUserIds = [];
        try { savedUserIds = JSON.parse(decodeURIComponent(tab.dataset.plUserids || '%5B%5D')); } catch {}
        getHseUsers().then(function(users) {
            var listEl = tab.querySelector('.playlist-user-list');
            if (!listEl) return;
            listEl.innerHTML = buildUserMultiSelectHtml(users, savedUserIds, 'chkPlaylistUser');
            wireUserMultiSelect(listEl);
        });
    }

    function initHomeSectionTab(row) {
        var tab = row.querySelector('.homescreen-tab');
        if (!tab || tab.dataset.hseLoaded === '1') return;
        tab.dataset.hseLoaded = 'loading'; // prevent re-entry while loading

        var savedUserIds = [];
        var savedSettings = {};
        try { savedUserIds = JSON.parse(decodeURIComponent(tab.dataset.hseUserids || '%5B%5D')); } catch {}
        try { savedSettings = JSON.parse(decodeURIComponent(tab.dataset.hseSettings || '%7B%7D')); } catch {}
        var defaultSectionType = tab.dataset.hseDefaultType || 'items';
        var savedLibraryId = decodeURIComponent(tab.dataset.hseLibraryid || 'auto');

        Promise.all([getHseUsers(), preFetchLibraryData()])
            .then(function(results) {
                var users = results[0];
                var topListFolderNames = results[1].topListFolderNames;
                var virtualFolders = results[1].virtualFolders;

                // Build library options for the hidden boxset selector (all non-top-list folders)
                var libraryOptions = virtualFolders
                    .filter(function (f) {
                        return !(f.Locations || []).some(function (loc) {
                            var parts = loc.replace(/\\/g, '/').split('/');
                            var folderName = parts[parts.length - 1] || parts[parts.length - 2] || '';
                            return topListFolderNames.has(folderName.toLowerCase());
                        });
                    })
                    .map(function (f) { return { id: f.ItemId, name: f.Name }; });

                // All libraries with isTopList flag for the multi-checkbox section
                var allLibraries = virtualFolders.map(function (f) {
                    var isTopList = (f.Locations || []).some(function (loc) {
                        var parts = loc.replace(/\\/g, '/').split('/');
                        var folderName = parts[parts.length - 1] || parts[parts.length - 2] || '';
                        return topListFolderNames.has(folderName.toLowerCase());
                    });
                    return { id: f.ItemId, name: f.Name, isTopList: isTopList };
                });

                // User list
                tab.querySelector('.hse-user-list-inner').innerHTML = buildUserMultiSelectHtml(users, savedUserIds, 'chkHseUser');
                wireUserMultiSelect(tab.querySelector('.hse-user-list-inner'));

                // Structured section settings form
                var defaultTagName = row.querySelector('.txtEntryLabel').value || row.querySelector('.txtTagName').value || '';
                var tagEnabled  = !!(row.querySelector('.chkEnableTag') || {}).checked;
                var collEnabled = !!(row.querySelector('.chkEnableCollection') || {}).checked;

                // Check dirty state BEFORE rendering — so we know if user had unsaved changes already
                var _hseView = row.closest('#HomeScreenCompanionConfigPage') || activeView();
                var _wasAlreadyDirty = _hseView && originalConfigState &&
                    JSON.stringify(getUiConfig(_hseView, true)) !== originalConfigState;

                var plEnabled = !!(row.querySelector('.chkEnablePlaylist') || {}).checked;
                tab.querySelector('.hse-fields-inner').innerHTML = buildHomeSectionFormHtml(savedSettings, defaultSectionType, defaultTagName, tagEnabled, collEnabled, libraryOptions, savedLibraryId, allLibraries, plEnabled);
                wireHomeSectionTypeChange(tab);
                wirePositionField(tab);
                refreshAdvanced(tab);
                fillPositionRows(tab, savedUserIds[0] || ((users || [])[0] || {}).Id, function () {
                    var nameEl = tab.querySelector('[data-field="CustomName"]');
                    return (nameEl && (nameEl.value || nameEl.placeholder)) || '';
                });

                // Mark as fully loaded only after form is in DOM so getUiConfig reads form values
                tab.dataset.hseLoaded = '1';

                // Sync live values from Emby, then re-anchor baseline and check form state.
                // Re-anchoring happens AFTER sync so changes made by sync don't trigger a false dirty state.
                syncHomeSectionFromEmby(tab).then(function() {
                    refreshAdvanced(tab);
                    if (!_wasAlreadyDirty && _hseView && originalConfigState) {
                        try { originalConfigState = JSON.stringify(getUiConfig(_hseView, true)); } catch {}
                    }
                    setTimeout(checkFormState, 0);
                });
            })
            .catch(function(e) {
                tab.querySelector('.hse-fields-inner').innerHTML = '<em style="color:var(--hsc-danger-text)">Failed to load: ' + e.message + '</em>';
                tab.dataset.hseLoaded = '0'; // allow retry
            });
    }
    function updateHseSectionAvailability(row) {
        var tagEnabled  = !!(row.querySelector('.chkEnableTag') || {}).checked;
        var collEnabled = !!(row.querySelector('.chkEnableCollection') || {}).checked;
        var hseCbx = row.querySelector('.chkEnableHomeSection');
        if (!hseCbx) return;
        var plEnabled = !!(row.querySelector('.chkEnablePlaylist') || {}).checked;
        // A top-list row needs none of them: its titles come from the source's run.
        var tlEnabled = !!(row.querySelector('.chkEnableTopList') || {}).checked && (row.querySelector('.selSourceType') || {}).value !== 'NextWatch';

        // The checkbox stays usable (Create as top-list is inside it); a plain row with nothing to
        // show says so, and Save stops until it has something (hseAllowed).
        var hint = row.querySelector('.hse-disabled-hint');
        hseCbx.disabled = false;
        var hseEmpty = !tagEnabled && !collEnabled && !plEnabled && !tlEnabled;
        if (hint) hint.style.display = hseCbx.checked && hseEmpty ? 'block' : 'none';
        // Nothing to show: only Create as top-list stays, the row's other fields are hidden.
        var hseTabEl = row.querySelector('.homescreen-tab');
        if (hseTabEl) hseTabEl.classList.toggle('hse-empty', hseEmpty);

        var tab = row.querySelector('.homescreen-tab');
        if (tab && tab.dataset.hseLoaded === '1') {
            refreshHseSectionTypeOptions(tab, tagEnabled, collEnabled, plEnabled);
        }
    }

    // The home screen section has something to show: a tag, collection or playlist, or its top-list.
    function hseAllowed(row) {
        var nw = (row.querySelector('.selSourceType') || {}).value === 'NextWatch';
        return ['.chkEnableTag', '.chkEnableCollection', '.chkEnablePlaylist'].concat(nw ? [] : ['.chkEnableTopList'])
            .some(function (c) { return !!(row.querySelector(c) || {}).checked; });
    }

    function refreshHseSectionTypeOptions(tab, tagEnabled, collEnabled, plEnabled) {
        var stSel = tab.querySelector('.selHseSectionType');
        if (!stSel) return;
        var currentVal = stSel.value;
        stSel.innerHTML = '';
        if (collEnabled) { var o1 = document.createElement('option'); o1.value = 'boxset'; o1.textContent = 'Single Collection'; stSel.appendChild(o1); }
        if (tagEnabled)  { var o2 = document.createElement('option'); o2.value = 'items';  o2.textContent = 'Dynamic Media (tag)'; stSel.appendChild(o2); }
        if (plEnabled)   { var o3 = document.createElement('option'); o3.value = 'playlist'; o3.textContent = PLAYLIST_SECTION_LABEL; stSel.appendChild(o3); }
        var stillValid = Array.from(stSel.options).some(function(o) { return o.value === currentVal; });
        if (stillValid) stSel.value = currentVal;
        updateHseItemsOnlyVisibility(tab);
    }

    // ---- End Home Section Tab helpers ----

    // The flat TagConfig entries one source card saves (one per URL / local source). Used by
    // Save and by Copy, so a copied source is exactly what Save would write.
    function rowToFlatTags(row, forComparison) {
        var flatTags = [];
        (function () {
            if (!row.querySelector('.txtEntryLabel')) return;
            var entryLabel = row.querySelector('.txtEntryLabel').value;
            var name = row.querySelector('.txtTagName').value || entryLabel;
            var active = row.querySelector('.chkTagActive').checked;

            var blInput = row.querySelector('.txtTagBlacklist');
            var bl = blInput ? blInput.value.split(/[\n\r]+/).map(s => s.trim()).filter(s => s.length > 0) : [];

            var enableTagChk = row.querySelector('.chkEnableTag').checked;
            var enableColl = row.querySelector('.chkEnableCollection').checked;
            var overrideWhenActive = !!(row.querySelector('.chkOverrideWhenActive') || {}).checked;

            var collName = row.querySelector('.txtCollectionName').value;
            var collSortName = row.querySelector('.txtCollectionSortName') ? row.querySelector('.txtCollectionSortName').value.trim() : '';
            var collDescription = row.querySelector('.txtCollectionDescription') ? row.querySelector('.txtCollectionDescription').value : '';
            var collPoster = (row.querySelector('.collection-tab .hiddenPosterPath') || { value: '' }).value;

            var intervals = readIntervalsFromRow(row);

            var currentLastMod = row.dataset.lastModified || new Date().toISOString();
            
            var st = row.querySelector('.selSourceType').value;
            var miFilters = [];
            row.querySelectorAll('.mediainfo-filter-group').forEach(function (group, gi) {
                var operator = group.dataset.op || 'AND';
                var groupOp = gi === 0 ? 'AND' : (group.dataset.groupOp || 'AND');
                var criteria = [];
                group.querySelectorAll('.mi-rule').forEach(function (rule) {
                    var prop = (rule.querySelector('.selMiProperty') || {}).value || '';
                    var selVal = rule.querySelector('.selMiValue');
                    var txtVal = rule.querySelector('.txtMiValue');
                    var selOp  = rule.querySelector('.selMiOp');
                    var txtNum = rule.querySelector('.txtMiNum');
                    var selUser = rule.querySelector('.selMiUser');
                    var selTextOp = rule.querySelector('.selMiTextOp');
                    var val = selVal ? selVal.value : (txtVal ? txtVal.value.replace(/\r?\n/g, '\n').trim() : '');
                    if (prop === 'MediaType' && val === 'Episode') {
                        var incParentChk = rule.querySelector('.chkIncludeParentSeries');
                        if (incParentChk && incParentChk.checked) val = 'EpisodeIncludeSeries';
                    }
                    var op2 = selOp ? selOp.value : '';
                    var textMatchOp = selTextOp ? selTextOp.value : '';
                    var num = txtNum ? txtNum.value.trim() : '';
                    var userId = selUser ? selUser.value : '';
                    var finalOp = op2 || textMatchOp;
                    var finalVal = op2 ? num : val;
                    var notBtn = rule.querySelector('.btnNotToggle');
                    var isNot = notBtn && notBtn.dataset.not === '1';
                    var crit = buildCriterion(prop, finalOp, finalVal, userId);
                    if (crit) criteria.push(isNot ? '!' + crit : crit);
                });
                if (criteria.length > 0) miFilters.push({ Operator: operator, Criteria: criteria, GroupOperator: groupOp });
            });

            var plTab2 = row.querySelector('.playlist-tab');
            var plUserIds2 = plTab2 && plTab2.dataset.plLoaded === '1'
                ? Array.from(plTab2.querySelectorAll('.chkPlaylistUser:checked')).map(function(c) { return c.value; })
                : (function() { try { return JSON.parse(decodeURIComponent((plTab2 && plTab2.dataset.plUserids) || '%5B%5D')); } catch { return []; } })();

            var hseTab = row.querySelector('.homescreen-tab');
            var enableHse = hseTab ? !!(hseTab.querySelector('.chkEnableHomeSection') || {}).checked && hseAllowed(row) : false;
            var hseLibraryId = hseTab && hseTab.dataset.hseLoaded === '1'
                ? ((hseTab.querySelector('.selHseLibrary') || {}).value || 'auto')
                : decodeURIComponent((hseTab && hseTab.dataset.hseLibraryid) || 'auto');
            var hseUserIds = hseTab && hseTab.dataset.hseLoaded === '1'
                ? Array.from(hseTab.querySelectorAll('.chkHseUser:checked')).map(function(c) { return c.value; })
                : (function() { try { return JSON.parse(decodeURIComponent((hseTab && hseTab.dataset.hseUserids) || '%5B%5D')); } catch { return []; } })();
            var hseSettings = {};
            if (hseTab && hseTab.dataset.hseLoaded === '1') {
                hseTab.querySelectorAll('[data-field]').forEach(function(el) {
                    var f = el.dataset.field;
                    var v = el.type === 'checkbox' ? String(el.checked) : el.value;
                    hseSettings[f] = v;
                });
                var itemTypesVal = (hseTab.querySelector('.selHseItemTypes') || {}).value || 'Movie,Series';
                hseSettings['ItemTypes'] = JSON.stringify(itemTypesVal.split(','));
                // Compute excluded library IDs from unchecked boxes → stored in _queryExcludeViewIds → Query.ExcludeUserViewIds
                // Also stored in ExcludedFolders → ContentSection.ExcludedFolders (visas i Embys native-meny)
                var _excludedLibIds2 = Array.from(hseTab.querySelectorAll('.chkHseLibrary:not(:checked)')).map(function(c) { return c.value; });
                if (_excludedLibIds2.length > 0) {
                    hseSettings['_queryExcludeViewIds'] = _excludedLibIds2.join(',');
                    hseSettings['ExcludedFolders'] = _excludedLibIds2.join(',');
                } else {
                    delete hseSettings['_queryExcludeViewIds'];
                    delete hseSettings['ExcludedFolders'];
                }
            } else {
                try { hseSettings = JSON.parse(decodeURIComponent((hseTab && hseTab.dataset.hseSettings) || '%7B%7D')); } catch {}
            }
            clearDefaultCustomName(hseSettings, hseTab);
            var hseTracked = (function() { try { return JSON.parse(decodeURIComponent((hseTab && hseTab.dataset.hseTracked) || '%5B%5D')); } catch { return []; } })();
            var collTag = readOutputTag(row, 'coll'), plTag = readOutputTag(row, 'pl');

            var baseTag = {
                Name: entryLabel, Tag: name, ExtraTags: readExtraTags(row), Active: active, Blacklist: bl, ActiveIntervals: intervals,
                EnableTag: enableTagChk, EnableCollection: enableColl, CollectionName: collName, CollectionSortName: collSortName, CollectionDescription: collDescription, CollectionPosterPath: collPoster, OnlyCollection: false, OverrideWhenActive: overrideWhenActive, LastModified: currentLastMod,
                CollectionPosterStyle: readCollectionArt(row).CollectionPosterStyle,
                CollectionBackgroundStyle: readCollectionArt(row).CollectionBackgroundStyle,
                CollectionBackgroundPath: readCollectionArt(row).CollectionBackgroundPath,
                CollectionKeepOrder: readCollectionArt(row).CollectionKeepOrder, CollectionImported: row.dataset.collImported === '1',
            CollectionPosterReplaced: row.dataset.posterReplaced === '1', CollectionBackgroundReplaced: row.dataset.bgReplaced === '1',
                CollectionArtTitle: readCollectionArt(row).CollectionArtTitle,
                CollectionPosterOptions: readCollectionArt(row).CollectionPosterOptions,
                CollectionBackgroundOptions: readCollectionArt(row).CollectionBackgroundOptions,
                CollectionThumbStyle: readCollectionArt(row).CollectionThumbStyle, CollectionThumbOptions: readCollectionArt(row).CollectionThumbOptions,
                CollectionThumbPath: readCollectionArt(row).CollectionThumbPath,
                CollectionBannerStyle: readCollectionArt(row).CollectionBannerStyle, CollectionBannerPath: readCollectionArt(row).CollectionBannerPath,
                CollectionThumbReplaced: row.dataset.thumbReplaced === '1', CollectionBannerReplaced: row.dataset.bannerReplaced === '1',
                TagPosterStyle: readTagArt(row).TagPosterStyle, TagBackgroundStyle: readTagArt(row).TagBackgroundStyle,
                TagBackgroundPath: readTagArt(row).TagBackgroundPath, TagArtTitle: readTagArt(row).TagArtTitle,
                TagPosterOptions: readTagArt(row).TagPosterOptions, TagBackgroundOptions: readTagArt(row).TagBackgroundOptions,
                SourceType: st, MediaInfoFilters: miFilters, MediaInfoConditions: [],
                TagTargetEpisode:        !!(row.querySelector('.chkTagTargetEpisode')  || {}).checked,
                TagTargetSeason:         !!(row.querySelector('.chkTagTargetSeason')   || {}).checked,
                TagTargetSeries:         !!(row.querySelector('.chkTagTargetSeries')   || {}).checked,
                CollectionTargetEpisode: !!(row.querySelector('.chkCollTargetEpisode') || {}).checked,
                CollectionTargetSeason:  !!(row.querySelector('.chkCollTargetSeason')  || {}).checked,
                CollectionTargetSeries:  !!(row.querySelector('.chkCollTargetSeries')  || {}).checked,
                MediaInfoTargetEpisode: false, MediaInfoTargetSeason: false, MediaInfoTargetSeries: false,
                MediaInfoTargetType: '', MediaInfoSeasonMode: false,
                EnableHomeSection: enableHse, HomeSectionLibraryId: hseLibraryId,
                HomeSectionUserIds: hseUserIds,
                HomeSectionSettings: JSON.stringify(hseSettings),
                HomeSectionTracked: hseTracked,
                EnablePlaylist:   !!(row.querySelector('.chkEnablePlaylist') || {}).checked,
                EnableCollectionTag: collTag.enabled, CollectionTag: collTag.tag, CollectionExtraTags: collTag.extras,
                EnablePlaylistTag: plTag.enabled, PlaylistTag: plTag.tag, PlaylistExtraTags: plTag.extras,
                PlaylistName:     (row.querySelector('.txtPlaylistName') || { value: '' }).value,
                PlaylistDescription: readPlaylistLook(row).PlaylistDescription, PlaylistPosterPath: readPlaylistLook(row).PlaylistPosterPath,
                PlaylistPosterStyle: readPlaylistLook(row).PlaylistPosterStyle, PlaylistBackgroundStyle: readPlaylistLook(row).PlaylistBackgroundStyle,
                PlaylistBackgroundPath: readPlaylistLook(row).PlaylistBackgroundPath, PlaylistArtTitle: readPlaylistLook(row).PlaylistArtTitle,
                PlaylistPosterOptions: readPlaylistLook(row).PlaylistPosterOptions, PlaylistBackgroundOptions: readPlaylistLook(row).PlaylistBackgroundOptions,
                PlaylistUserIds:  plUserIds2,
                PlaylistMappings: (function() { try { return JSON.parse(decodeURIComponent((plTab2 && plTab2.dataset.plMappings) || '%5B%5D')); } catch { return []; } })(),
                NextWatchUsersMigrated: row.dataset.nwMigrated === '1',
                NextWatchSeedDays: readNwSeedDays(row), NextWatchUseTaste: readNwUseTaste(row)
            };
            Object.assign(baseTag, readTopListTab(row));

            if (st === 'External') {
                var pushedExternal = false;
                row.querySelectorAll('.url-row').forEach(uRow => {
                    var urlVal = uRow.querySelector('.txtTagUrl').value.trim();
                    var limitVal = parseInt(uRow.querySelector('.txtUrlLimit').value, 10) || 0;
                    if (urlVal) { flatTags.push(Object.assign({}, baseTag, { Url: urlVal, Limit: limitVal, LocalSourceId: "" })); pushedExternal = true; }
                });
                // No URL yet: the source is still saved (the sync skips it and changes nothing).
                if (!pushedExternal) {
                    var firstUrlLimit = parseInt((row.querySelector('.url-row .txtUrlLimit') || {}).value, 10) || 0;
                    flatTags.push(Object.assign({}, baseTag, { Url: "", Limit: firstUrlLimit, LocalSourceId: "" }));
                }
            } else if (st === 'LocalCollection' || st === 'LocalPlaylist') {
                var pushedLocal = false;
                row.querySelectorAll('.local-row').forEach(lRow => {
                    var localVal = lRow.querySelector('.selLocalSource').value;
                    var limitVal = parseInt(lRow.querySelector('.txtLocalLimit').value, 10) || 0;
                    if (localVal) { flatTags.push(Object.assign({}, baseTag, { Url: "", Limit: limitVal, LocalSourceId: localVal })); pushedLocal = true; }
                });
                // Nothing picked yet: the source is still saved (the sync skips it and changes nothing).
                if (!pushedLocal) {
                    var firstLocalLimit = parseInt((row.querySelector('.local-row .txtLocalLimit') || {}).value, 10) || 0;
                    flatTags.push(Object.assign({}, baseTag, { Url: "", Limit: firstLocalLimit, LocalSourceId: "" }));
                }
            } else if (st === 'Manual') {
                flatTags.push(Object.assign({}, baseTag, { Url: "", Limit: 0, LocalSourceId: "", ManualItemIds: readManualIds(row) }));
            } else if (st === 'AI') {
                var aiLimitVal = parseInt((row.querySelector('.txtAiLimit') || {}).value, 10) || 0;
                flatTags.push(Object.assign({}, baseTag, {
                    Url: "", Limit: aiLimitVal, LocalSourceId: "",
                    AiProvider: (row.querySelector('.selAiProvider') || {}).value || 'OpenAI',
                    AiPrompt: (row.querySelector('.txtAiPrompt') || {}).value || '',
                    AiIncludeRecentlyWatched: !!(row.querySelector('.chkAiRecentlyWatched') || {}).checked,
                    AiRecentlyWatchedUserId: (row.querySelector('.selAiWatchedUser') || {}).value || '',
                    // Compared as typed (so 0 or an empty box is a change); saved as 20 when empty, as the sync uses.
                    AiRecentlyWatchedCount: forComparison
                        ? ((row.querySelector('.txtAiWatchedCount') || {}).value || '')
                        : (parseInt(((row.querySelector('.txtAiWatchedCount') || {}).value || '20'), 10) || 20),
                    AiRefreshIntervalDays: parseInt(((row.querySelector('.txtAiRefreshInterval') || {}).value || '0'), 10) || 0
                }));
            } else if (st === 'NextWatch') {
                var nwLimitVal = parseInt((row.querySelector('.txtMediaInfoLimit') || {}).value, 10) || 50;
                flatTags.push(Object.assign({}, baseTag, { Url: "", Limit: nwLimitVal, LocalSourceId: "" }));
            } else {
                var miLimitVal = parseInt((row.querySelector('.txtMediaInfoLimit') || {}).value, 10) || 0;
                flatTags.push(Object.assign({}, baseTag, { Url: "", Limit: miLimitVal, LocalSourceId: "" }));
            }
        })();
        return flatTags;
    }

    function getUiConfig(view, forComparison) {
        var flatTags = [];
        var sourceRows = sourceRowsInManualOrder(view);
        var otherRows = Array.from(view.querySelectorAll('.tag-row')).filter(function (r) { return sourceRows.indexOf(r) < 0; });
        sourceRows.concat(otherRows).forEach(row => { flatTags.push.apply(flatTags, rowToFlatTags(row, forComparison)); });

        var hscEnabled      = view.querySelector('#chkHscEnabled');
        var cwbEnabled      = view.querySelector('#chkCwbEnabled');
        var cwbMode         = view.querySelector('#selCwbMode');
        var cwbAllUsers     = view.querySelector('#chkCwbAllUsers');
        // Compare what is in the field, so clearing one counts as a change; an empty field is
        // saved as the default the AI call needs.
        function fieldOr(id, def) {
            var v = (view.querySelector(id) || {}).value || '';
            return forComparison ? v : (v.trim() ? v : def);
        }

        return {
            TraktClientId: view.querySelector('#txtTraktClientId').value,
            MdblistApiKey: view.querySelector('#txtMdblistApiKey').value,
            TmdbApiKey: view.querySelector('#txtTmdbApiKey').value,
            OpenAiApiKey: (view.querySelector('#txtOpenAiApiKey') || {}).value || '',
            OpenAiModel: fieldOr('#txtOpenAiModel', 'gpt-4o-mini'),
            GeminiApiKey: (view.querySelector('#txtGeminiApiKey') || {}).value || '',
            GeminiModel: fieldOr('#txtGeminiModel', 'gemini-2.5-flash-lite'),
            ClaudeApiKey: (view.querySelector('#txtClaudeApiKey') || {}).value || '',
            ClaudeModel: fieldOr('#txtClaudeModel', 'claude-haiku-4-5-20251001'),
            OllamaBaseUrl: fieldOr('#txtOllamaBaseUrl', 'http://localhost:11434'),
            OllamaModel: (view.querySelector('#txtOllamaModel') || {}).value || '',
            AiConnections: readAiConnections(view),
            AiSystemPrompt: (view.querySelector('#txtAiSystemPrompt') || {}).value || '',
            ExtendedConsoleOutput: view.querySelector('#chkExtendedConsoleOutput').checked,
            LogMissingItems: view.querySelector('#chkLogMissingItems').checked,
            DryRunMode: view.querySelector('#chkDryRunMode').checked,
            PreserveTagsOnEmptyResult: view.querySelector('#chkPreserveTagsOnEmptyResult').checked,
            IgnoreMarkedAsWatched: view.querySelector('#chkIgnoreMarkedAsWatched').checked,
            TopListMirrorCollections: view.querySelector('#chkTopListMirrorCollections').checked,
            HideTopListLibraries: view.querySelector('#chkHideTopListLibraries').checked,
            ShowCopyPasteButtons: view.querySelector('#chkShowCopyPasteButtons').checked,
            ShowImportCollectionButton: view.querySelector('#chkShowImportCollectionButton').checked,
            RankedCollectionsEnabled: view.querySelector('#chkRankedCollectionsEnabled').checked,
            SettingsLevel: settingsLevel,
            Tags: flatTags,
            SavedFilters: savedFilters,
            HomeSyncEnabled: hscEnabled ? hscEnabled.checked : (lastHscConfig.HomeSyncEnabled || false),
            HomeSyncProfiles: hscEnabled ? readHscProfiles(view) : (lastHscConfig.HomeSyncProfiles || []),
            // Replaced by HomeSyncProfiles (see effectiveHscProfiles): cleared on save.
            HomeSyncLibraryOrder: false, HomeSyncSourceUserId: '', HomeSyncTargetUserIds: [],
            ContinueWatchingBumpEnabled: cwbEnabled ? cwbEnabled.checked : (lastHscConfig.ContinueWatchingBumpEnabled || false),
            ContinueWatchingBumpMode: cwbMode ? (cwbMode.value || 'AllEpisodes') : (lastHscConfig.ContinueWatchingBumpMode || 'AllEpisodes'),
            ContinueWatchingBumpAllUsers: cwbAllUsers ? cwbAllUsers.checked : (lastHscConfig.ContinueWatchingBumpAllUsers || false),
            ContinueWatchingBumpUserIds: cwbEnabled
                ? Array.from(view.querySelectorAll('.cwb-user-chk:checked')).map(function(c) { return c.value; })
                : (lastHscConfig.ContinueWatchingBumpUserIds || [])
        };
    }

    // ---- AI: OpenAI-compatible connections and model lists ----
    // Settings holds any number of OpenAI-compatible connections (Groq, OpenRouter, LM Studio …).
    // A source uses one with AiProvider "Compat:<id>", so renaming it keeps the sources on it.
    var AI_COMPAT = 'Compat:';
    var _aiConnections = [];   // the connections as they are in Settings now, for the provider menus
    var AI_BUILTIN_PROVIDERS = [['Claude', 'Anthropic Claude'], ['Gemini', 'Google Gemini'], ['Ollama', 'Ollama (Local)'], ['OpenAI', 'OpenAI (ChatGPT)']];

    function newAiConnId() {
        return Date.now().toString(36) + Math.random().toString(36).slice(2, 10);
    }
    function aiConnName(c) { return (c && (c.Name || '').trim()) || 'OpenAI-compatible'; }
    function aiProviderMissing(value) {
        if ((value || '').indexOf(AI_COMPAT) !== 0) return false;
        var id = value.substring(AI_COMPAT.length);
        return !_aiConnections.some(function (c) { return c.Id === id; });
    }
    function aiProviderLabel(value) {
        var b = AI_BUILTIN_PROVIDERS.filter(function (p) { return p[0] === value; })[0];
        if (b) return b[1];
        var id = (value || '').indexOf(AI_COMPAT) === 0 ? value.substring(AI_COMPAT.length) : null;
        var c = id && _aiConnections.filter(function (x) { return x.Id === id; })[0];
        return c ? aiConnName(c) : (id ? 'Missing connection' : (value || ''));
    }
    // The AI Provider options of a source: the built-in providers, then each connection by name.
    // A connection that was removed stays selected as "Missing connection", so a save keeps it
    // pointing there (and the source shows it needs a provider) instead of moving it to Claude.
    function aiProviderOptionsHtml(value) {
        var opts = AI_BUILTIN_PROVIDERS.map(function (p) {
            return '<option value="' + p[0] + '"' + (p[0] === value ? ' selected' : '') + '>' + p[1] + '</option>';
        });
        _aiConnections.slice().sort(function (a, b) { return aiConnName(a).localeCompare(aiConnName(b)); }).forEach(function (c) {
            var v = AI_COMPAT + c.Id;
            opts.push('<option value="' + escapeHtml(v) + '"' + (v === value ? ' selected' : '') + '>' + escapeHtml(aiConnName(c)) + '</option>');
        });
        if (aiProviderMissing(value))
            opts.push('<option value="' + escapeHtml(value) + '" selected>Missing connection</option>');
        return opts.join('');
    }
    function updateAiProviderMissing(row) {
        var sel = row.querySelector('.selAiProvider'), hint = row.querySelector('.ai-provider-missing');
        if (sel && hint) hint.style.display = aiProviderMissing(sel.value) ? 'block' : 'none';
    }
    function refreshAiProviderSelects(view) {
        _aiConnections = readAiConnections(view);
        view.querySelectorAll('.tag-row .selAiProvider').forEach(function (sel) {
            var v = sel.value;
            sel.innerHTML = aiProviderOptionsHtml(v);
            sel.value = v;
            updateAiProviderMissing(sel.closest('.tag-row'));
        });
    }

    function aiConnCardHtml(c) {
        var id = escapeHtml(c.Id);
        return '<div class="hsc-ai-conn" data-id="' + id + '">' +
            '<div class="inputContainer"><input is="emby-input" type="text" class="txtAiConnName" label="Name" placeholder="Groq" value="' + escapeHtml(c.Name || '') + '" /></div>' +
            '<div class="inputContainer"><input is="emby-input" type="text" class="txtAiConnBaseUrl" label="Base URL" placeholder="https://api.groq.com/openai/v1" value="' + escapeHtml(c.BaseUrl || '') + '" /></div>' +
            '<div class="inputContainer"><input is="emby-input" type="text" class="api-key-field txtAiConnApiKey" autocomplete="off" spellcheck="false" data-lpignore="true" data-1p-ignore id="txtAiConnKey_' + id + '" label="API Key" value="' + escapeHtml(c.ApiKey || '') + '" />' +
                '<label style="display:inline-flex; align-items:center; gap:5px; font-size:0.85em; opacity:0.75; margin-top:4px; cursor:pointer;">' +
                '<input type="checkbox" class="chkShowApiKey" data-target="txtAiConnKey_' + id + '" style="margin:0;" /> Show</label></div>' +
            '<div class="inputContainer hsc-model-field" data-provider="Compat">' +
                '<label class="selectLabel">Model</label>' +
                '<div class="hsc-model-row"><div class="hsc-model-sel"><select is="emby-select" class="selAiModel"></select></div>' +
                '<button type="button" is="emby-button" class="raised hsc-btn-ghost btnAiModelsReload" title="Reload models"><i class="md-icon">refresh</i></button></div>' +
                '<div class="hsc-model-custom"><input is="emby-input" type="text" class="txtAiConnModel" label="Model name" value="' + escapeHtml(c.Model || '') + '" /></div>' +
                '<div class="hsc-model-status"></div>' +
            '</div>' +
            '<div class="hsc-ai-conn-foot"><button type="button" is="emby-button" class="raised hsc-btn-danger btnRemoveAiConn"><i class="md-icon" style="font-size:1em; margin-right:4px;">delete</i><span>Remove</span></button></div>' +
            '</div>';
    }
    function readAiConnections(view) {
        return Array.from(view.querySelectorAll('#aiConnList .hsc-ai-conn')).map(function (card) {
            function val(cls) { return ((card.querySelector(cls) || {}).value || '').trim(); }
            return { Id: card.dataset.id, Name: val('.txtAiConnName'), BaseUrl: val('.txtAiConnBaseUrl'), ApiKey: val('.txtAiConnApiKey'), Model: val('.txtAiConnModel') };
        });
    }
    function renderAiConnections(view, list) {
        var box = view.querySelector('#aiConnList');
        if (!box) return;
        box.innerHTML = (list || []).filter(function (c) { return c && c.Id; }).map(aiConnCardHtml).join('');
        _aiConnections = readAiConnections(view);
        box.querySelectorAll('.hsc-model-field').forEach(function (f) { syncModelField(f); });
    }

    // ---- Model dropdowns ----
    // Each Model field is a list read from the provider (HomeScreenCompanion/Ai/Models, the key
    // stays on the server) plus "Custom…", which shows the text field. The text field holds the
    // value that is saved; a saved model that is not in the list stays selected.
    var AI_MODEL_CUSTOM = '__hsc_custom__';
    function modelFieldInput(field) { return field.querySelector('.hsc-model-custom input'); }
    function modelFieldRequest(field) {
        var view = field.closest('#HomeScreenCompanionConfigPage') || document;
        var card = field.closest('.hsc-ai-conn');
        function byId(id) { return id ? ((view.querySelector('#' + id) || {}).value || '').trim() : ''; }
        if (card) return {
            Provider: 'Compat', ConnectionId: card.dataset.id,
            BaseUrl: ((card.querySelector('.txtAiConnBaseUrl') || {}).value || '').trim(),
            ApiKey: ((card.querySelector('.txtAiConnApiKey') || {}).value || '').trim()
        };
        return { Provider: field.dataset.provider, ConnectionId: '', BaseUrl: byId(field.dataset.url), ApiKey: byId(field.dataset.key) };
    }
    function modelFieldReady(req) {
        return req.Provider === 'Compat' || req.Provider === 'Ollama' ? !!req.BaseUrl : !!req.ApiKey;
    }
    function syncModelField(field) {
        var sel = field.querySelector('.selAiModel'), input = modelFieldInput(field);
        if (!sel || !input) return;
        var models = field._models || [], v = (input.value || '').trim();
        var custom = field.classList.contains('is-custom');
        var html = [];
        if (!v && !custom) html.push('<option value=""></option>');
        if (v && !custom && models.indexOf(v) < 0) html.push('<option value="' + escapeHtml(v) + '">' + escapeHtml(v) + '</option>');
        models.forEach(function (m) { html.push('<option value="' + escapeHtml(m) + '">' + escapeHtml(m) + '</option>'); });
        html.push('<option value="' + AI_MODEL_CUSTOM + '">Custom…</option>');
        sel.innerHTML = html.join('');
        sel.value = custom ? AI_MODEL_CUSTOM : v;
    }
    function loadModelField(field, force) {
        var req = modelFieldRequest(field), status = field.querySelector('.hsc-model-status');
        if (!modelFieldReady(req)) { field._models = []; field._sig = ''; if (status) status.textContent = ''; syncModelField(field); return; }
        var sig = JSON.stringify(req);
        if (!force && field._sig === sig) return;
        field._sig = sig;
        field.classList.add('is-loading');
        fetch(window.ApiClient.getUrl('HomeScreenCompanion/Ai/Models'), {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'X-MediaBrowser-Token': window.ApiClient.accessToken() },
            body: JSON.stringify(req)
        }).then(function (r) { return r.json(); }).then(function (res) {
            if (field._sig !== sig) return;
            field._models = res.Success ? (res.Models || []) : [];
            if (status) status.textContent = res.Success ? (field._models.length ? '' : (res.Message || 'No models')) : (res.Message || 'Failed');
            syncModelField(field);
        }).catch(function (err) {
            if (field._sig !== sig) return;
            field._models = [];
            if (status) status.textContent = err && err.message ? err.message : 'Failed';
            syncModelField(field);
        }).finally(function () { field.classList.remove('is-loading'); });
    }
    function syncModelFields(view) {
        view.querySelectorAll('.hsc-model-field').forEach(function (f) {
            f.classList.remove('is-custom');
            f._sig = '';
            syncModelField(f);
            if (f.offsetParent) loadModelField(f);
        });
    }

    // ---- What is unsaved, in words ----
    // Compares two getUiConfig(view, true) results (saved, current) and returns
    // { settings: [label], sources: [{ name, what }] } for the unsaved-changes popup.
    var _settingLabels = {
        OpenAiModel: 'OpenAI model', GeminiModel: 'Gemini model', ClaudeModel: 'Claude model',
        OllamaBaseUrl: 'Ollama URL', OllamaModel: 'Ollama model', AiConnections: 'OpenAI-compatible connections', AiSystemPrompt: 'AI system prompt',
        ExtendedConsoleOutput: 'Extended log', LogMissingItems: 'Log missing movies and shows',
        DryRunMode: 'Dry Run Mode', PreserveTagsOnEmptyResult: 'Preserve tags and collections on empty result',
        IgnoreMarkedAsWatched: 'Ignore titles only marked as watched',
        TopListMirrorCollections: 'Show collections on top-list items', HideTopListLibraries: 'Hide HSC libraries',
        ShowCopyPasteButtons: 'Copy & paste sources', ShowImportCollectionButton: 'Import collection',
        RankedCollectionsEnabled: 'Ranked collections', SavedFilters: 'Saved filters',
        HomeSyncEnabled: 'Home Screen sync: on/off', HomeSyncProfiles: 'Home Screen sync: profiles',
        ContinueWatchingBumpEnabled: 'Continue Watching bump: on/off', ContinueWatchingBumpMode: 'Continue Watching bump: mode',
        ContinueWatchingBumpAllUsers: 'Continue Watching bump: all users', ContinueWatchingBumpUserIds: 'Continue Watching bump: users'
    };
    // A source card's field → the card tab it lives on.
    function sourceFieldArea(k) {
        if (k === 'Name') return 'name';
        if (k === 'Tag') return 'tag name';
        if (k === 'Active') return 'on/off';
        if (k === 'Blacklist') return 'blacklist';
        if (k === 'ActiveIntervals') return 'schedule';
        if (k === 'EnableTag' || /^Tag(Target|Poster|Background|ArtTitle)/.test(k)) return 'tag settings';
        if (/^(Enable)?Collection|^OverrideWhenActive$/.test(k)) return 'collection';
        if (/^(Enable)?Playlist/.test(k)) return 'playlist';
        if (/^(Enable)?HomeSection/.test(k)) return 'home screen';
        return 'source';
    }
    function describeUnsavedChanges(saved, current) {
        var same = function (a, b) { return JSON.stringify(a) === JSON.stringify(b); };
        var out = { settings: [], sources: [] };
        Object.keys(_settingLabels).forEach(function (k) {
            if (!same(saved[k], current[k])) out.settings.push(_settingLabels[k]);
        });
        // A card is one or more flat entries in a row (one per URL / local source).
        function groups(tags) {
            var gs = [];
            (tags || []).forEach(function (t) {
                var last = gs[gs.length - 1];
                if (last && last.name === t.Name && last.tag === t.Tag) last.items.push(t);
                else gs.push({ name: t.Name, tag: t.Tag, lm: t.LastModified, items: [t] });
            });
            return gs;
        }
        var before = groups(saved.Tags), after = groups(current.Tags);
        var match = new Array(after.length).fill(-1), used = new Set();
        // By name first, then (a renamed card) by the saved card's LastModified, which stays put.
        after.forEach(function (g, i) {
            var j = before.findIndex(function (b, x) { return !used.has(x) && b.name === g.name && b.tag === g.tag; });
            if (j >= 0) { match[i] = j; used.add(j); }
        });
        after.forEach(function (g, i) {
            if (match[i] >= 0) return;
            var j = before.findIndex(function (b, x) { return !used.has(x) && b.lm && b.lm === g.lm; });
            if (j >= 0) { match[i] = j; used.add(j); }
        });
        function label(g) { return g.name || g.tag || '(unnamed source)'; }
        after.forEach(function (g, i) {
            if (match[i] < 0) { out.sources.push({ name: label(g), what: 'new source' }); return; }
            var b = before[match[i]];
            var areas = [];
            function add(a) { if (areas.indexOf(a) < 0) areas.push(a); }
            if (b.items.length !== g.items.length) add('source');
            var keys = new Set();
            b.items.concat(g.items).forEach(function (t) { Object.keys(t).forEach(function (k) { keys.add(k); }); });
            keys.delete('LastModified');
            keys.forEach(function (k) {
                for (var n = 0; n < Math.max(b.items.length, g.items.length); n++) {
                    var x = b.items[n] || b.items[0], y = g.items[n] || g.items[0];
                    if (!same(x[k], y[k])) { add(sourceFieldArea(k)); break; }
                }
            });
            if (!areas.length) return;
            var name = label(g);
            if (label(b) !== name) name = label(b) + ' → ' + name;
            out.sources.push({ name: name, what: areas.join(', ') });
        });
        before.forEach(function (b, x) {
            if (!used.has(x)) out.sources.push({ name: label(b), what: 'deleted', deleted: true });
        });
        // Same cards, other order.
        var kept = match.filter(function (j) { return j >= 0; });
        if (kept.some(function (j, n) { return n > 0 && j < kept[n - 1]; })) out.settings.push('Order of sources');
        return out;
    }

    function checkFormState() {
        var view = activeView();
        if (!view || !originalConfigState) return;
        
        var isDirty = false;
        try {
            var current = JSON.stringify(getUiConfig(view, true));
            isDirty = current !== originalConfigState;
        } catch (e) {
            isDirty = true;
        }

        var btnApplyManage = view.querySelector('#btnApplyManage');
        if (btnApplyManage && !btnApplyManage.disabled) isDirty = true;

        var tcContainer = view.querySelector('#tcManageContainer');
        if (tcContainer && tcContainer._tcHasPending) isDirty = true;

        var tlContainer = view.querySelector('#tlContainer');
        if (tlContainer && tlContainer.querySelector('.tag-body[data-dirty="1"]')) isDirty = true;

        var btnSave = view.querySelector('.btn-save');
        if (btnSave) {
            var isSyncRunning = (btnSave.querySelector('span').textContent || "").includes("progress");
            if (isSyncRunning) {
                btnSave.disabled = true;
                btnSave.style.opacity = "0.5";
            } else {
                btnSave.disabled = !isDirty;
                btnSave.style.opacity = isDirty ? "1" : "0.5";
            }
        }
    }

    function updateDryRunWarning() {
        var view = activeView();
        if (!view || !originalConfigState) return;
        // Settings > Features: the buttons follow the saved settings, like this warning.
        try {
            var saved = JSON.parse(originalConfigState);
            view.classList.toggle('hsc-no-copypaste', !saved.ShowCopyPasteButtons);
            view.classList.toggle('hsc-no-import', !saved.ShowImportCollectionButton);
            view.classList.toggle('hsc-no-ranked', !saved.RankedCollectionsEnabled);
        } catch (e) { }
        // A power-user setting put back to its default in Basic goes once it is saved.
        refreshAdvanced(view);
        var warn = view.querySelector('.dry-run-warning');
        if (warn) {
            try {
                var savedConfig = JSON.parse(originalConfigState);
                warn.style.display = savedConfig.DryRunMode ? 'flex' : 'none';
            } catch (e) {
                warn.style.display = 'none';
            }
        }
    }

    function applyFilters(view) {
        var container = view.querySelector('#tagListContainer');
        var rows = container.querySelectorAll('.tag-row');

        var searchTerm = (view.querySelector('#txtSearchTags').value || "").toLowerCase();

        var fTag        = view.querySelector('#chkFilterTag')?.checked;
        var fColl       = view.querySelector('#chkFilterCollection')?.checked;
        var fSched      = view.querySelector('#chkFilterSchedule')?.checked;
        var fHome       = view.querySelector('#chkFilterHomeScreen')?.checked;
        var fSrcExt     = view.querySelector('#chkFilterSrcExternal')?.checked;
        var fSrcMI      = view.querySelector('#chkFilterSrcMediaInfo')?.checked;
        var fSrcColl    = view.querySelector('#chkFilterSrcCollection')?.checked;
        var fSrcPlay    = view.querySelector('#chkFilterSrcPlaylist')?.checked;
        var fSrcAI      = view.querySelector('#chkFilterSrcAI')?.checked;
        var fActive     = view.querySelector('#chkFilterActive')?.checked;
        var fInactive   = view.querySelector('#chkFilterInactive')?.checked;
        var fIssues     = view.querySelector('#chkFilterIssues')?.checked;

        var anyFeature  = fTag || fColl || fSched || fHome;
        var anySrc      = fSrcExt || fSrcMI || fSrcColl || fSrcPlay || fSrcAI;
        var anyStatus   = fActive || fInactive;

        // Update button appearance
        var btn = view.querySelector('#btnFilterDropdown');
        var lbl = view.querySelector('#filterDropdownLabel');
        if (btn && lbl) {
            var activeCount = [fTag, fColl, fSched, fHome, fSrcExt, fSrcMI, fSrcColl, fSrcPlay, fSrcAI, fActive, fInactive, fIssues].filter(Boolean).length;
            lbl.textContent = activeCount > 0 ? 'Filter (' + activeCount + ')' : 'Filter';
            btn.classList.toggle('active', activeCount > 0);
        }

        rows.forEach(row => {
            var tagName  = (row.querySelector('.txtTagName').value || "").toLowerCase();
            var entryLbl = (row.querySelector('.txtEntryLabel').value || "").toLowerCase();
            var matchesSearch = !searchTerm || tagName.includes(searchTerm) || entryLbl.includes(searchTerm);

            var matchesFeature = true;
            if (anyFeature) {
                var hasTag   = row.querySelector('.chkEnableTag')?.checked;
                var hasColl  = row.querySelector('.chkEnableCollection')?.checked;
                var hasSched = row.querySelectorAll('.date-row').length > 0;
                var hasHome  = row.querySelector('.chkEnableHomeSection')?.checked;
                matchesFeature = (fTag && hasTag) || (fColl && hasColl) || (fSched && hasSched) || (fHome && hasHome);
            }

            var matchesSrc = true;
            if (anySrc) {
                var src = row.querySelector('.selSourceType')?.value || '';
                matchesSrc = (fSrcExt && src === 'External') || (fSrcMI && src === 'MediaInfo') ||
                             (fSrcColl && src === 'LocalCollection') || (fSrcPlay && src === 'LocalPlaylist') ||
                             (fSrcAI && src === 'AI');
            }

            var matchesStatus = true;
            if (anyStatus) {
                var isActive = row.querySelector('.chkTagActive')?.checked;
                matchesStatus = (fActive && isActive) || (fInactive && !isActive);
            }

            var matchesIssues = !fIssues || !!_groupIssues[decodeURIComponent(row.dataset.groupKey || '')];

            row.style.display = (matchesSearch && matchesFeature && matchesSrc && matchesStatus && matchesIssues) ? 'block' : 'none';
        });
        // Recount here too: rows are rendered, added and reloaded after the issues arrive.
        updateIssueCounter(view);
        var counter = view.querySelector('#btnIssueCounter');
        if (counter) counter.classList.toggle('active', !!fIssues);
    }

    function checkForUpdates(view) {
        window.ApiClient.getJSON(window.ApiClient.getUrl("HomeScreenCompanion/Version")).then(function (result) {
            var currentVer = result.Version || '';
            var channel = (result.Channel || '').replace(/[^A-Za-z0-9 .-]/g, '');
            var footerVer = view.querySelector('#footerVersionText');
            if (footerVer && currentVer) {
                var releaseUrl = 'https://github.com/soderlund91/HomeScreenCompanion/releases/tag/v' + currentVer;
                footerVer.innerHTML = '<a href="' + releaseUrl + '" target="_blank" style="color:inherit;text-decoration:none;">v' + currentVer
                    + (channel ? ' ' + channel : '') + '</a>';
            }
            if (!currentVer) return;

            fetch('https://api.github.com/repos/soderlund91/HomeScreenCompanion/releases/latest')
                .then(function (r) { return r.json(); })
                .then(function (release) {
                    var latestTag = (release.tag_name || '').replace(/^v/i, '');
                    if (!latestTag) return;
                    var a = latestTag.split('.').map(Number);
                    var b = currentVer.split('.').map(Number);
                    var isNewer = false;
                    for (var i = 0; i < Math.max(a.length, b.length); i++) {
                        if ((a[i] || 0) > (b[i] || 0)) { isNewer = true; break; }
                        if ((a[i] || 0) < (b[i] || 0)) break;
                    }
                    if (isNewer) {
                        var footerUpdate = view.querySelector('#footerUpdateInfo');
                        if (footerUpdate) {
                            footerUpdate.innerHTML = '<a href="' + release.html_url + '"'
                                + ' target="_blank" class="footer-update-link">Update available: v' + latestTag + '</a>';
                            var footerUpdateSep = view.querySelector('#footerUpdateSep');
                            if (footerUpdateSep) footerUpdateSep.style.display = '';
                        }
                    }
                })
                .catch(function () {});
        }).catch(function () {});
    }

    window._testUpdateBanner = function (mode) {
        var fakeTag = '99.99.99';
        var fakeUrl = 'https://github.com/soderlund91/HomeScreenCompanion/releases';
        var footerUpdate = document.getElementById('footerUpdateInfo');
        if (!footerUpdate) { console.warn('_testUpdateBanner: #footerUpdateInfo not found'); return; }
        var footerUpdateSep = document.getElementById('footerUpdateSep');
        if (mode === 'banner' || mode === 'discreet') {
            footerUpdate.innerHTML = '<a href="' + fakeUrl + '"'
                + ' target="_blank" class="footer-update-link">Update available: v' + fakeTag + '</a>';
            if (footerUpdateSep) footerUpdateSep.style.display = '';
        } else if (mode === 'reset') {
            footerUpdate.innerHTML = '';
            if (footerUpdateSep) footerUpdateSep.style.display = 'none';
        }
    };

    // "9 of 10" chip: the last run put fewer titles in than the Limit. Each source (URL / local
    // source) of the card must have a run recorded with its current Limit; sources without a Limit
    // are left out. Changing a Limit hides the chip until the next run.
    function lastRunKey(t) { return (t.Url || '') + '\x1F' + (t.LocalSourceId || ''); }
    function lastRunChipHtml(runs, sources) {
        var found = 0, limit = 0;
        for (var i = 0; i < sources.length; i++) {
            var lim = sources[i].Limit || 0;
            if (lim <= 0) continue;
            var key = lastRunKey(sources[i]);
            var r = (runs || []).filter(function (x) { return x.k === key; })[0];
            if (!r || r.l !== lim) return '';
            found += r.c; limit += lim;
        }
        if (!limit || found >= limit) return '';
        return `<span class="tag-indicator short" title="Last run found ${found} of ${limit}"><i class="md-icon" style="font-size:1.1em;">warning</i> ${found} of ${limit}</span>`;
    }

    // Warning sign before a group's name: its last run (full sync or Run Group) had warnings
    // (orange) or failed (red). The card's body lists the messages. Keyed by the saved Name + Tag,
    // like the server.
    function groupIssueKey(name, tag) { return ((name || '').trim() + '\x1F' + (tag || '').trim()).toLowerCase(); }
    function groupIssueFlagHtml(issue) {
        if (!issue) return '';
        var isError = issue.Level === 'error';
        return `<i class="md-icon${isError ? ' error' : ''}" style="font-size:1.3em;" title="${isError ? 'Error last run' : 'Warning last run'}">warning</i>`;
    }
    function groupIssueBoxHtml(issue) {
        if (!issue) return '';
        var isError = issue.Level === 'error';
        var when = issue.RunUtc ? new Date(issue.RunUtc) : null;
        var whenText = when && !isNaN(when) ? ' · ' + when.toLocaleString([], { dateStyle: 'short', timeStyle: 'short' }) : '';
        return `<div class="${isError ? 'error' : ''}"><div class="hsc-run-issue-head"><i class="md-icon" style="font-size:1.2em;">warning</i>${isError ? 'Error' : 'Warning'} last run${whenText}</div>` +
            '<ul>' + (issue.Messages || []).map(function (m) { return '<li>' + escapeHtml(m) + '</li>'; }).join('') + '</ul></div>';
    }
    function applyGroupIssue(row) {
        var issue = _groupIssues[decodeURIComponent(row.dataset.groupKey || '')];
        var flag = row.querySelector('.tag-info .hsc-run-issue'), box = row.querySelector('.tag-body .hsc-run-issue-box');
        if (flag) flag.innerHTML = groupIssueFlagHtml(issue);
        if (box) box.innerHTML = groupIssueBoxHtml(issue);
    }
    function setGroupIssues(view, list) {
        var sig = JSON.stringify(list || []);
        if (sig === _groupIssuesSig) return;
        _groupIssuesSig = sig;
        _groupIssues = {};
        (list || []).forEach(function (i) { _groupIssues[groupIssueKey(i.Name, i.Tag)] = i; });
        view.querySelectorAll('.tag-row').forEach(applyGroupIssue);
        updateIssueCounter(view);
        if (view.querySelector('#chkFilterIssues')?.checked) applyFilters(view);
    }
    // "⚠ 7 groups need attention" next to the filters (red when one of them failed); hidden when
    // no group on the page has an issue.
    function updateIssueCounter(view) {
        var btn = view.querySelector('#btnIssueCounter');
        if (!btn) return;
        var n = 0, anyError = false;
        view.querySelectorAll('#tagListContainer .tag-row').forEach(function (row) {
            var issue = _groupIssues[decodeURIComponent(row.dataset.groupKey || '')];
            if (!issue) return;
            n++;
            if (issue.Level === 'error') anyError = true;
        });
        btn.style.display = n ? '' : 'none';
        btn.classList.toggle('error', anyError);
        btn.innerHTML = '<i class="md-icon" style="font-size:1.1em;">warning</i>' + n + (n === 1 ? ' group needs' : ' groups need') + ' attention';
    }

    function groupConfigTags(tags) {
        var grouped = {};
        (tags || []).forEach(t => {
            var key = t.Name ? t.Name + '\x1F' + t.Tag : t.Tag;
            if (!grouped[key]) {
                grouped[key] = {
                    Tag: t.Tag, ExtraTags: t.ExtraTags || [], Name: t.Name || '', Urls: [], LocalSources: [], Active: t.Active !== false, Blacklist: t.Blacklist, ActiveIntervals: t.ActiveIntervals,
                    EnableTag: t.EnableTag !== false, EnableCollection: t.EnableCollection, CollectionName: t.CollectionName, CollectionSortName: t.CollectionSortName || '', CollectionDescription: t.CollectionDescription || '', CollectionPosterPath: t.CollectionPosterPath || '',
                    CollectionPosterStyle: t.CollectionPosterStyle || '', CollectionBackgroundStyle: t.CollectionBackgroundStyle || '',
                    CollectionBackgroundPath: t.CollectionBackgroundPath || '', CollectionKeepOrder: !!t.CollectionKeepOrder, CollectionImported: !!t.CollectionImported,
                    CollectionPosterReplaced: !!t.CollectionPosterReplaced, CollectionBackgroundReplaced: !!t.CollectionBackgroundReplaced,
                    CollectionArtTitle: t.CollectionArtTitle || '',
                    CollectionPosterOptions: t.CollectionPosterOptions || '', CollectionBackgroundOptions: t.CollectionBackgroundOptions || '',
                    CollectionThumbStyle: t.CollectionThumbStyle || '', CollectionThumbOptions: t.CollectionThumbOptions || '', CollectionThumbPath: t.CollectionThumbPath || '',
                    CollectionBannerStyle: t.CollectionBannerStyle || '', CollectionBannerPath: t.CollectionBannerPath || '',
                    CollectionThumbReplaced: !!t.CollectionThumbReplaced, CollectionBannerReplaced: !!t.CollectionBannerReplaced,
                    TagPosterStyle: t.TagPosterStyle || '', TagBackgroundStyle: t.TagBackgroundStyle || '',
                    TagBackgroundPath: t.TagBackgroundPath || '', TagArtTitle: t.TagArtTitle || '',
                    TagPosterOptions: t.TagPosterOptions || '', TagBackgroundOptions: t.TagBackgroundOptions || '',
                    OnlyCollection: t.OnlyCollection, OverrideWhenActive: t.OverrideWhenActive || false, LastModified: t.LastModified,
                    SourceType: t.SourceType || "External", MediaInfoConditions: t.MediaInfoConditions || [], MediaInfoFilters: t.MediaInfoFilters || [],
                    Limit: t.Limit || 0,
                    EnableHomeSection: t.EnableHomeSection || false, HomeSectionLibraryId: t.HomeSectionLibraryId || 'auto',
                    HomeSectionUserIds: t.HomeSectionUserIds || [], HomeSectionSettings: t.HomeSectionSettings || '{}',
                    HomeSectionTracked: t.HomeSectionTracked || [],
                    AiProvider: t.AiProvider || 'OpenAI',
                    AiPrompt: t.AiPrompt || '',
                    AiIncludeRecentlyWatched: t.AiIncludeRecentlyWatched || false,
                    AiRecentlyWatchedUserId: t.AiRecentlyWatchedUserId || '',
                    AiRecentlyWatchedCount: t.AiRecentlyWatchedCount || 20,
                    AiRefreshIntervalDays: t.AiRefreshIntervalDays || 0,
                    TagTargetEpisode: t.TagTargetEpisode || false,
                    TagTargetSeason:  t.TagTargetSeason  || false,
                    TagTargetSeries:  t.TagTargetSeries  || false,
                    CollectionTargetEpisode: t.CollectionTargetEpisode || false,
                    CollectionTargetSeason:  t.CollectionTargetSeason  || false,
                    CollectionTargetSeries:  t.CollectionTargetSeries  || false,
                    EnablePlaylist:   t.EnablePlaylist   || false,
                    EnableCollectionTag: !!t.EnableCollectionTag, CollectionTag: t.CollectionTag || '', CollectionExtraTags: t.CollectionExtraTags || [],
                    EnablePlaylistTag: !!t.EnablePlaylistTag, PlaylistTag: t.PlaylistTag || '', PlaylistExtraTags: t.PlaylistExtraTags || [],
                    PlaylistName:     t.PlaylistName     || '',
                    PlaylistDescription: t.PlaylistDescription || '', PlaylistPosterPath: t.PlaylistPosterPath || '',
                    PlaylistPosterStyle: t.PlaylistPosterStyle || '', PlaylistBackgroundStyle: t.PlaylistBackgroundStyle || '',
                    PlaylistBackgroundPath: t.PlaylistBackgroundPath || '', PlaylistArtTitle: t.PlaylistArtTitle || '',
                    PlaylistPosterOptions: t.PlaylistPosterOptions || '', PlaylistBackgroundOptions: t.PlaylistBackgroundOptions || '',
                    PlaylistUserIds:  t.PlaylistUserIds  || [],
                    PlaylistMappings: t.PlaylistMappings || [],
                    NextWatchUsersMigrated: !!t.NextWatchUsersMigrated,
                    NextWatchSeedDays: t.NextWatchSeedDays > 0 ? t.NextWatchSeedDays : 90, NextWatchUseTaste: t.NextWatchUseTaste !== false,
                    EnableTopList: !!t.EnableTopList, TopListContentType: (t.TopListContentType === 'Shows' || t.TopListContentType === 'Movies') ? t.TopListContentType : 'Auto',
                    TopListMaxItems: t.TopListMaxItems || 0, TopListBadgeStyle: t.TopListBadgeStyle || 'neutral',
                    TopListBadgeOptions: t.TopListBadgeOptions || '', TopListRealPoster: t.TopListRealPoster || '',
                    LastRuns: [],
                };
            }
            grouped[key].LastRuns.push({ k: lastRunKey(t), c: t.LastRunCount || 0, l: t.LastRunLimit || 0, lim: t.Limit || 0 });
            // A source saved without a URL / local source yet comes back as one empty row (with its limit).
            if (t.SourceType === 'External') grouped[key].Urls.push({ url: t.Url || '', limit: t.Limit });
            if (t.SourceType === 'LocalCollection' || t.SourceType === 'LocalPlaylist') grouped[key].LocalSources.push({ id: t.LocalSourceId || '', limit: t.Limit });
            if (t.SourceType === 'MediaInfo') grouped[key].Limit = t.Limit;
            if (t.SourceType === 'AI') grouped[key].Limit = t.Limit;
            if (t.SourceType === 'NextWatch') {
                grouped[key].Limit = t.Limit;
                // Older Your Next Watch configs kept their users on the Home Screen tab: the Playlist
                // tab's users now decide who gets a list, so an empty list takes those users — once
                // (NextWatchUsersMigrated; the server does the same on its next run), so a list the
                // user empties later stays empty.
                if (!grouped[key].NextWatchUsersMigrated) {
                    if (grouped[key].EnablePlaylist && !grouped[key].PlaylistUserIds.length && (grouped[key].HomeSectionUserIds || []).length)
                        grouped[key].PlaylistUserIds = grouped[key].HomeSectionUserIds.slice();
                    grouped[key].NextWatchUsersMigrated = true;
                }
            }
            if (t.SourceType === 'Manual') grouped[key].ManualItemIds = t.ManualItemIds || [];
        });
        return grouped;
    }


    // Unrestricted / Restricted quick picks above a plain (always open) per-user checkbox list — same
    // look as the dropdown version in buildUserMultiSelectHtml.
    function userListSelectLinksHtml() {
        return '<div class="hsc-user-list-links" style="display:flex;flex-wrap:wrap;gap:4px 12px;padding:2px 0 6px 0;margin-bottom:4px;font-size:0.85em;">' +
            userSelectLinksInnerHtml() + '</div>';
    }

    // Wires those links: sets every enabled checkbox in listEl, then fires one change event so
    // the list's own listeners (and checkFormState) run once.
    function wireUserListSelectLinks(linksEl, listEl) {
        if (!linksEl || !listEl) return;
        linksEl.querySelectorAll('.hsc-user-select-unrestricted, .hsc-user-select-restricted').forEach(function (link) {
            link.addEventListener('click', function (e) {
                e.preventDefault();
                applyUserQuickSelect(link, Array.from(listEl.querySelectorAll('input[type="checkbox"]')).filter(function (c) { return !c.disabled; }));
            });
        });
    }

    // Copy & Sync profiles as the page edits them: HomeSyncProfiles, or the old single source
    // user (HomeSyncSourceUserId …) as one profile. Saving writes profiles and clears the old fields.
    function effectiveHscProfiles(config) {
        if ((config.HomeSyncProfiles || []).length) {
            return config.HomeSyncProfiles.map(function (p) {
                return {
                    Active: p.Active !== false, SourceUserId: p.SourceUserId || '', TargetUserIds: (p.TargetUserIds || []).slice().sort(), LibraryOrder: !!p.LibraryOrder,
                    Mode: p.Mode === 'selected' ? 'selected' : 'all', KeepOwnRows: p.KeepOwnRows !== false, AddToSources: !!p.AddToSources, SelectedRows: (p.SelectedRows || []).slice().sort()
                };
            });
        }
        if (!config.HomeSyncSourceUserId) return [];
        return [{
            Active: true, SourceUserId: config.HomeSyncSourceUserId, TargetUserIds: (config.HomeSyncTargetUserIds || []).slice().sort(), LibraryOrder: !!config.HomeSyncLibraryOrder,
            Mode: 'all', KeepOwnRows: true, AddToSources: false, SelectedRows: []
        }];
    }

    // The profiles in the Copy & Sync tab, in page order.
    function readHscProfiles(view) {
        return Array.from(view.querySelectorAll('#hscContainer .hsc-profile')).map(function (el) {
            return {
                Active: !!(el.querySelector('.chkHscProfileActive') || {}).checked,
                SourceUserId: (el.querySelector('.selHscProfileSource') || {}).value || '',
                TargetUserIds: Array.from(el.querySelectorAll('.hsc-target-chk:checked')).map(function (c) { return c.value; }).sort(),  // sorted: the dirty check compares with effectiveHscProfiles
                LibraryOrder: !!(el.querySelector('.chkHscProfileLibraryOrder') || {}).checked,
                Mode: (el.querySelector('.selHscProfileMode') || {}).value === 'selected' ? 'selected' : 'all',
                KeepOwnRows: !!(el.querySelector('.chkHscProfileKeepOwn') || {}).checked,
                AddToSources: !!(el.querySelector('.chkHscProfileAddToSources') || {}).checked,
                SelectedRows: hscSelectedRows(el)
            };
        });
    }

    // The rows a profile copies with Copy selected (sorted, like effectiveHscProfiles): the ticked
    // boxes once the source user's rows are listed, else what was saved.
    function hscSelectedRows(el) {
        var list = el.querySelector('.hsc-profile-row-list');
        if (!list || !list.dataset.source) return (el._hscSavedRows || []).slice().sort();
        return Array.from(list.querySelectorAll('.hsc-row-chk:checked')).map(function (c) { return c.value; }).sort();
    }

    // Copy selected: the source user's home rows to tick (GET Hsc/SourceRows). Rows ticked before
    // that are not on that home screen now stay listed (and ticked), so a save keeps them.
    function loadHscProfileRows(el, users) {
        var list = el.querySelector('.hsc-profile-row-list');
        var source = (el.querySelector('.selHscProfileSource') || {}).value || '';
        if (!list || list.dataset.loading === source) return;
        var keep = hscSelectedRows(el);
        el._hscSavedRows = keep;
        delete list.dataset.source;
        if (!source) {
            list.innerHTML = '<p class="textMuted" style="font-size:0.85em;">Select a source user first.</p>';
            return;
        }
        list.dataset.loading = source;
        list.innerHTML = '<p class="textMuted" style="font-size:0.85em;">Loading rows…</p>';
        var sourceUser = (users || []).filter(function (u) { return u.Id === source; })[0];
        var sourceName = sourceUser ? sourceUser.Name : 'the source user';
        window.ApiClient.getJSON(window.ApiClient.getUrl('HomeScreenCompanion/Hsc/SourceRows', { UserId: source }))
            .then(function (rows) { return rows || []; }, function () { return null; })
            .then(function (rows) {
                if (list.dataset.loading !== source) return;   // the source user was changed meanwhile
                delete list.dataset.loading;
                if (rows === null) {
                    list.innerHTML = '<p class="textMuted" style="font-size:0.85em;">Could not load the rows of ' + escapeHtml(sourceName) + '.</p>';
                    return;
                }
                var kinds = { source: 'Source', playlist: 'Playlist', toplist: 'Top-list' };
                function rowHtml(key, title, kind, missing) {
                    var chip = kinds[kind] ? '<span style="margin-left:8px;font-size:0.75em;padding:1px 8px;border-radius:10px;border:1px solid var(--line-color);color:var(--theme-text-secondary);">' + kinds[kind] + '</span>' : '';
                    var note = missing ? '<span style="margin-left:auto;font-size:0.8em;color:var(--theme-text-secondary);">Not on ' + escapeHtml(sourceName) + '\'s home screen</span>' : '';
                    return '<div class="hsc-user-row"' + (missing ? ' style="opacity:0.6;"' : '') + '><label style="display:flex;align-items:center;gap:10px;cursor:pointer;width:100%;">' +
                        '<input is="emby-checkbox" type="checkbox" class="hsc-row-chk" value="' + escapeHtml(key) + '"' + (keep.indexOf(key) >= 0 ? ' checked' : '') + ' />' +
                        '<span>' + escapeHtml(title) + chip + '</span>' + note + '</label></div>';
                }
                var keys = rows.map(function (r) { return r.Key; });
                var missing = keep.filter(function (k) { return keys.indexOf(k) < 0; });
                list.dataset.source = source;
                list.innerHTML = rows.length || missing.length
                    ? buildSelectAllRowHtml('hsc-user-row') +
                      rows.map(function (r) { return rowHtml(r.Key, r.Title, r.Kind, false); }).join('') +
                      missing.map(function (k) { return rowHtml(k, hscRowKeyLabel(k), '', true); }).join('')
                    : '<p class="textMuted" style="font-size:0.85em;">' + escapeHtml(sourceName) + ' has no home screen rows.</p>';
                list._syncAll = wireSelectAll(list, '.hsc-row-chk');
                var container = el.closest('#hscContainer');
                if (container) refreshHscProfiles(container);
                setTimeout(checkFormState, 0);
            });
    }

    // A saved row key the source user does not have now, as a readable name.
    function hscRowKeyLabel(key) {
        var k = key.replace(/^(src|tl|row):/, '');
        if (key.indexOf('src:') === 0) {
            // "src:<name>/<tag>", each URI-escaped (HomeSectionSyncTask.SourceKey).
            var parts = k.split('/').map(function (x) { try { return decodeURIComponent(x); } catch (e) { return x; } });
            return parts[0] || parts[1] || k;
        }
        if (k.indexOf('name:') === 0) return k.substring(5);
        if (k.indexOf('type:') === 0) return k.substring(5).split('|')[0] + ' row';
        return k;
    }

    // One profile: its header names the source user and who it copies to; the body holds its settings.
    function hscProfileHtml(p, users, open) {
        var sourceOptions = users.map(function (u) {
            return '<option value="' + u.Id + '"' + (p.SourceUserId === u.Id ? ' selected' : '') + '>' + escapeHtml(u.Name + userRatingSuffix(u)) + '</option>';
        }).join('');
        var targetRows = users.map(function (u) {
            var checked = (p.TargetUserIds || []).indexOf(u.Id) >= 0 ? ' checked' : '';
            return '<div class="hsc-user-row"><label style="display:flex;align-items:center;gap:10px;cursor:pointer;width:100%;">' +
                '<input is="emby-checkbox" type="checkbox" class="hsc-target-chk" value="' + u.Id + '" data-restricted="' + (isUserRestricted(u) ? '1' : '0') + '"' + checked + ' />' +
                '<span>' + escapeHtml(u.Name) + userRatingChipHtml(u) + '</span>' +
                '<span class="hsc-target-note" style="margin-left:auto;font-size:0.8em;color:var(--theme-text-secondary);"></span>' +
                '</label></div>';
        }).join('');
        return [
            '<div class="advanced-section settings-panel hsc-profile">',
            '<div class="advanced-header settings-panel-toggle">',
            '<i class="md-icon hsc-panel-icon">person</i>',
            '<div style="min-width:0; flex:1;">',
            '<div class="hsc-title hsc-profile-source"></div>',
            '<div class="hsc-profile-targets" style="font-size:0.85em; color:var(--theme-text-secondary); white-space:nowrap; overflow:hidden; text-overflow:ellipsis;"></div>',
            '</div>',
            '<span class="hsc-profile-off" style="display:none; margin-left:8px; font-size:0.75em; padding:1px 8px; border-radius:10px; border:1px solid var(--line-color); color:var(--theme-text-secondary);">Off</span>',
            '<button type="button" is="emby-button" class="raised btnRemoveHscProfile btn-row-remove" title="Remove profile" style="margin:0 0 0 8px;"><i class="md-icon">delete</i></button>',
            '<i class="md-icon settings-panel-chevron" style="margin-left:4px; transition:transform 0.2s;' + (open ? ' transform:rotate(180deg);' : '') + '">expand_more</i>',
            '</div>',
            '<div class="advanced-body settings-panel-body" style="display:' + (open ? 'block' : 'none') + '; padding-top:4px;">',
            '<div class="checkboxContainer checkboxContainer-withDescription">',
            '<label><input is="emby-checkbox" type="checkbox" class="chkHscProfileActive"' + (p.Active !== false ? ' checked' : '') + ' /><span>Sync this profile</span></label>',
            '<div class="fieldDescription">Untick to pause this profile: its users keep the home screen they have, and stay on the sources and top-lists Copy & Sync added them to.</div>',
            '</div>',
            '<div class="inputContainer" style="margin-top:16px;">',
            '<select is="emby-select" class="selHscProfileSource" label="Source User">',
            '<option value="">— Select source user —</option>',
            sourceOptions,
            '</select>',
            '<div class="fieldDescription">Home screen sections are copied FROM this user to the users selected below.</div>',
            '</div>',
            '<div class="checkboxContainer checkboxContainer-withDescription" style="margin-top:8px;">',
            '<label><input is="emby-checkbox" type="checkbox" class="chkHscProfileLibraryOrder"' + (p.LibraryOrder ? ' checked' : '') + ' /><span>Also copy library order</span></label>',
            '<div class="fieldDescription">Also copies the order of media libraries in the navigation sidebar.</div>',
            '</div>',
            '<div class="inputContainer" style="margin-top:16px;">',
            '<select is="emby-select" class="selHscProfileMode" label="What to copy">',
            '<option value="all"' + (p.Mode !== 'selected' ? ' selected' : '') + '>Everything (replaces the home screen)</option>',
            '<option value="selected"' + (p.Mode === 'selected' ? ' selected' : '') + '>Selected rows only</option>',
            '</select>',
            '</div>',
            '<div class="hsc-mode-all"' + (p.Mode === 'selected' ? ' style="display:none;"' : '') + '>',
            '<div class="checkboxContainer checkboxContainer-withDescription">',
            '<label><input is="emby-checkbox" type="checkbox" class="chkHscProfileKeepOwn"' + (p.KeepOwnRows !== false ? ' checked' : '') + ' /><span>Keep each user\'s own rows</span></label>',
            '</div>',
            '<div class="hsc-callout" style="margin-top:4px;"><i class="md-icon">info</i><span>',
            'Each user\'s home screen is replaced by the source user\'s.<br>',
            '<strong>On:</strong> rows from sources and top-lists the user is selected on, that the source user doesn\'t have, stay where they are.<br>',
            '<strong>Off:</strong> an exact copy. Everything else is removed. With "Also add users to the sources and top-lists" on, the user is also taken off those sources and top-lists; without it, rows from their own sources come back with the next full sync.',
            '</span></div>',
            '</div>',
            '<div class="hsc-mode-selected"' + (p.Mode === 'selected' ? '' : ' style="display:none;"') + '>',
            '<div class="hsc-callout" style="margin-bottom:8px;"><i class="md-icon">info</i><span>',
            'Only the ticked rows are copied: updated where a user already has them, else added at the same place as on the source user\'s home screen. Each user\'s other rows stay.<br>',
            'Unticking a row later removes it again.',
            '</span></div>',
            '<div class="hsc-user-list hsc-profile-row-list"></div>',
            '</div>',
            '<div class="checkboxContainer checkboxContainer-withDescription hsc-adv" data-adv="check" style="margin-top:16px;">',
            '<label><input is="emby-checkbox" type="checkbox" class="chkHscProfileAddToSources"' + (p.AddToSources ? ' checked' : '') + ' /><span>Also add users to the sources and top-lists of copied rows' + ADV_BADGE + '</span></label>',
            '<div class="fieldDescription">Each user is added to the source or top-list of every row they get, so the full sync keeps that row updated for them and makes their own playlist rows (such as Your Next Watch). They show up in its user list. Copy & Sync takes them off again when the row is no longer copied or the user leaves the profile, and the row stays as a plain copy (a personal playlist row goes). Users you added yourself stay. Off: the rows are only copied, and a top-list row only goes to the list\'s own users, as before 4.4.3.</div>',
            '</div>',
            '<div class="hsc-eyebrow" style="margin:16px 0 8px;">Copy to</div>',
            (targetRows ? userListSelectLinksHtml() : ''),
            '<div class="hsc-user-list hsc-profile-target-list">',
            targetRows ? buildSelectAllRowHtml('hsc-user-row') + targetRows : '<p class="textMuted" style="font-size:0.85em;">No users found.</p>',
            '</div>',
            '</div>',
            '</div>'
        ].join('');
    }

    function renderHscTab(container, config, users) {
        var profiles = config.HomeSyncProfiles || [];
        var syncDisplay = config.HomeSyncEnabled ? '' : 'none';
        container._hscUsers = users;
        container.innerHTML = [
            '<div class="hsc-card">',
            '<h3 class="hsc-section-title">Configuration</h3>',
            '<div class="checkboxContainer checkboxContainer-withDescription">',
            '<label><input is="emby-checkbox" type="checkbox" id="chkHscEnabled"' + (config.HomeSyncEnabled ? ' checked' : '') + ' /><span>Enable Home Screen sync</span></label>',
            '<div class="fieldDescription">When enabled, each profile copies its source user\'s home screen (everything, or the rows you select) to the users it lists. When disabled, nothing changes, even if the task is triggered manually: users keep their home screens and stay on the sources and top-lists Copy & Sync added them to. Delete a profile to take its users off those again.</div>',
            '</div>',
            '</div>',
            '<div id="hscProfilesWrap" class="hsc-card" style="display:' + syncDisplay + '">',
            '<h3 class="hsc-section-title">Profiles</h3>',
            '<p class="textMuted" style="font-size:0.88em; margin-bottom:12px;">Each profile copies one user\'s home screen to other users, e.g. one for the adults and one for the children. Several profiles can use the same source user, e.g. to copy different rows to different users. A user can get their home screen from one profile only.</p>',
            '<p class="hsc-profile-empty textMuted" style="font-size:0.85em; display:' + (profiles.length ? 'none' : 'block') + ';">No profiles yet.</p>',
            '<div class="hsc-profile-list">',
            profiles.map(function (p) { return hscProfileHtml(p, users, false); }).join(''),
            '</div>',
            '<button type="button" is="emby-button" class="raised btnAddHscProfile hsc-btn-dashed" style="width:100%;"><i class="md-icon" style="margin-right:5px;">add</i>Add profile</button>',
            '</div>'
        ].join('');
        container.dataset.loaded = '1';
        refreshAdvanced(container);
    }

    // Select-all and the Unrestricted / Restricted links of one profile's target list.
    function wireHscProfile(el) {
        var list = el.querySelector('.hsc-profile-target-list');
        if (!list) return;
        list._syncAll = wireSelectAll(list, '.hsc-target-chk');
        wireUserListSelectLinks(list.previousElementSibling, list);
    }

    // Locks what would make two profiles write the same user: a source user is never a target,
    // and a user is the target of one profile only (the first that has them). Several profiles
    // may share a source user (e.g. different rows to different users). Then the headers.
    function refreshHscProfiles(container) {
        var users = container._hscUsers || [];
        function nameOf(id) { var u = users.filter(function (x) { return x.Id === id; })[0]; return u ? u.Name : id; }
        var els = Array.from(container.querySelectorAll('.hsc-profile'));
        var sources = els.map(function (el) { return (el.querySelector('.selHscProfileSource') || {}).value || ''; });
        var owner = {};
        els.forEach(function (el, i) {
            el.querySelectorAll('.hsc-target-chk').forEach(function (chk) {
                if (chk.checked && sources.indexOf(chk.value) < 0 && !(chk.value in owner)) owner[chk.value] = i;
            });
        });
        els.forEach(function (el, i) {
            el.querySelectorAll('.hsc-target-chk').forEach(function (chk) {
                var why = '';
                if (chk.value === sources[i]) why = 'Source user of this profile';
                else if (sources.indexOf(chk.value) >= 0) why = 'Source user of another profile';
                else if ((chk.value in owner) && owner[chk.value] !== i) why = 'Copied from ' + (sources[owner[chk.value]] ? nameOf(sources[owner[chk.value]]) : 'another profile');
                var row = chk.closest('.hsc-user-row');
                if (why) chk.checked = false;
                chk.disabled = !!why;
                row.title = why;
                row.style.opacity = why ? '0.45' : '';
                var note = row.querySelector('.hsc-target-note');
                if (note) note.textContent = why;
            });
            var sel = el.querySelector('.selHscProfileSource');
            if (sel) Array.from(sel.options).forEach(function (o) {
                if (!o.value) return;
                o.disabled = o.value !== sources[i] && (o.value in owner);
            });
            var list = el.querySelector('.hsc-profile-target-list');
            if (list && list._syncAll) list._syncAll();

            var targets = Array.from(el.querySelectorAll('.hsc-target-chk:checked')).map(function (c) { return nameOf(c.value); });
            var selectedMode = (el.querySelector('.selHscProfileMode') || {}).value === 'selected';
            el.querySelector('.hsc-mode-all').style.display = selectedMode ? 'none' : '';
            el.querySelector('.hsc-mode-selected').style.display = selectedMode ? '' : 'none';
            var rowList = el.querySelector('.hsc-profile-row-list');
            if (selectedMode && rowList && (rowList.dataset.source || '') !== sources[i] && rowList.dataset.loading !== sources[i]) loadHscProfileRows(el, users);
            var nSelected = hscSelectedRows(el).length;
            var what = selectedMode ? nSelected + ' selected row' + (nSelected === 1 ? '' : 's')
                : (el.querySelector('.chkHscProfileKeepOwn') || {}).checked ? '' : 'exact copy';
            el.querySelector('.hsc-profile-source').textContent = sources[i] ? nameOf(sources[i]) : 'No source user';
            el.querySelector('.hsc-profile-targets').textContent = (targets.length ? '→ ' + targets.join(', ') : 'Copies to nobody yet') + (what ? '  ·  ' + what : '');
            el.querySelector('.hsc-profile-off').style.display = (el.querySelector('.chkHscProfileActive') || {}).checked ? 'none' : '';
        });
        var empty = container.querySelector('.hsc-profile-empty');
        if (empty) empty.style.display = els.length ? 'none' : 'block';
    }

    // A profile set to an exact copy (Everything, own rows not kept) that also changes the lists.
    function hscProfileMirrors(el) {
        return (el.querySelector('.selHscProfileMode') || {}).value !== 'selected'
            && !(el.querySelector('.chkHscProfileKeepOwn') || {}).checked
            && !!(el.querySelector('.chkHscProfileAddToSources') || {}).checked;
    }

    function confirmHscMirror(el) {
        var n = el.querySelectorAll('.hsc-target-chk:checked').length;
        return confirm('Exact copy with "Also add users to the sources and top-lists" on takes '
            + (n === 1 ? 'this user' : 'these ' + n + ' users')
            + ' off every source and top-list the source user isn\'t on, also those you selected them on yourself. '
            + 'Turning it off again later does not put them back. Continue?');
    }

    function loadHscUsers(view) {
        var container = view.querySelector('#hscContainer');
        if (!container) return;

        getHseUsers()
            .then(function (users) {
                renderHscTab(container, lastHscConfig, users || []);
                var savedProfiles = lastHscConfig.HomeSyncProfiles || [];
                container.querySelectorAll('.hsc-profile').forEach(function (el, i) {
                    el._hscSavedRows = ((savedProfiles[i] || {}).SelectedRows || []).slice();
                    wireHscProfile(el);
                });
                refreshHscProfiles(container);

                container.querySelector('#chkHscEnabled').addEventListener('change', function () {
                    container.querySelector('#hscProfilesWrap').style.display = this.checked ? '' : 'none';
                    setTimeout(checkFormState, 0);
                });

                container.querySelector('.btnAddHscProfile').addEventListener('click', function () {
                    var list = container.querySelector('.hsc-profile-list');
                    list.insertAdjacentHTML('beforeend', hscProfileHtml({ Active: true, SourceUserId: '', TargetUserIds: [], LibraryOrder: false, Mode: 'all', KeepOwnRows: true, AddToSources: false, SelectedRows: [] }, container._hscUsers || [], true));
                    wireHscProfile(list.lastElementChild);
                    refreshAdvanced(list.lastElementChild);
                    refreshHscProfiles(container);
                    setTimeout(checkFormState, 0);
                });

                // On the container, which outlives a reload (a backup restore loads the tab
                // again): the earlier handlers go first, so each runs once.
                if (container._hscHandlers) {
                    container.removeEventListener('click', container._hscHandlers.click);
                    container.removeEventListener('change', container._hscHandlers.change);
                    container.removeEventListener('input', container._hscHandlers.input);
                }
                container._hscHandlers = {
                    // Before the page's own panel toggle (on the view): removing must not fold the panel.
                    click: function (e) {
                        var del = e.target.closest('.btnRemoveHscProfile');
                        if (!del) return;
                        e.stopPropagation();
                        var gone = del.closest('.hsc-profile');
                        var src = (gone.querySelector('.hsc-profile-source') || {}).textContent || 'this user';
                        var n = gone.querySelectorAll('.hsc-target-chk:checked').length;
                        if (!confirm('Delete the profile that copies from ' + src + ' to ' + n + ' user' + (n === 1 ? '' : 's') + '? '
                            + 'After saving, they are taken off the sources and top-lists Copy & Sync added them to. Their home screens stay as they are, except a personal playlist row.')) return;
                        gone.remove();
                        refreshHscProfiles(container);
                        setTimeout(checkFormState, 0);
                    },
                    change: function (e) {
                        var profile = e.target.closest('.hsc-profile');
                        // An exact copy that changes the lists cannot be undone: asked first.
                        if (profile && e.target.matches('.chkHscProfileKeepOwn, .chkHscProfileAddToSources, .selHscProfileMode')
                            && hscProfileMirrors(profile) && !confirmHscMirror(profile)) {
                            if (e.target.matches('.selHscProfileMode')) e.target.value = 'selected';
                            else e.target.checked = !e.target.checked;
                        }
                        if (profile) refreshHscProfiles(container);
                        setTimeout(checkFormState, 0);
                    },
                    input: function () { setTimeout(checkFormState, 0); }
                };
                container.addEventListener('click', container._hscHandlers.click);
                container.addEventListener('change', container._hscHandlers.change);
                container.addEventListener('input', container._hscHandlers.input);
            })
            .catch(function () {
                container.innerHTML = '<p class="textMuted" style="padding:20px;">Failed to load users. Check server connection.</p>';
            });
    }


    // ── Home Screen Tweaks ───────────────────────────────────────────────────
    // Expandable panels in the same style as the Settings tab (.settings-panel —
    // toggled by the delegated click handler, so no per-panel wiring is needed).

    function renderHstTab(container, config, users) {
        var cwbUserIds   = config.ContinueWatchingBumpUserIds || [];
        var cwbUserRows  = users.map(function (u) {
            var checked = cwbUserIds.indexOf(u.Id) >= 0 ? ' checked' : '';
            return '<div class="hsc-user-row"><label style="display:flex;align-items:center;gap:10px;cursor:pointer;width:100%;">' +
                '<input is="emby-checkbox" type="checkbox" class="cwb-user-chk" value="' + u.Id + '" data-restricted="' + (isUserRestricted(u) ? '1' : '0') + '"' + checked + ' />' +
                '<span>' + escapeHtml(u.Name) + userRatingChipHtml(u) + '</span>' +
                '</label></div>';
        }).join('');
        var cwbEnabled   = config.ContinueWatchingBumpEnabled ? ' checked' : '';
        var cwbAllUsers  = config.ContinueWatchingBumpAllUsers ? ' checked' : '';
        var cwbMode      = config.ContinueWatchingBumpMode || 'AllEpisodes';
        var cwbDisplay   = config.ContinueWatchingBumpEnabled ? '' : 'none';
        var cwbUsersDisp = config.ContinueWatchingBumpAllUsers ? 'none' : '';

        container.innerHTML = [
            '<div class="advanced-section settings-panel" style="margin-bottom: 15px;">',
            '<div class="advanced-header settings-panel-toggle">',
            '<span class="hsc-title">Continue Watching</span>',
            '<i class="md-icon settings-panel-chevron" style="margin-left: auto; transition: transform 0.2s;">expand_more</i>',
            '</div>',
            '<div class="advanced-body settings-panel-body" style="display:none; padding-top: 4px;">',

            '<div style="display:flex; align-items:flex-start; gap:8px; margin:0 0 12px; padding:8px 12px; border:1px solid rgba(230,193,90,0.5); border-radius:6px; background:rgba(230,193,90,0.08); font-size:0.85em; line-height:1.45;">',
            '<i class="md-icon" style="color:var(--hsc-warning-text); font-size:1.2em;">warning</i>',
            '<div><strong style="color:var(--hsc-warning-text);">Experimental.</strong> Safe to use — it never changes watched status, only the "last played" date of one episode. It has been tested, but needs more real-world use before it can be considered fully stable.</div>',
            '</div>',

            '<p style="font-size:0.9em; line-height:1.5; margin:0 0 24px;">',
            'Emby sorts Continue Watching by when you last watched a series, so a show you finished a year ago stays far down the row even when a new season arrives. ',
            'This moves such a series to the front as soon as a new episode is added.',
            '</p>',

            '<div class="checkboxContainer checkboxContainer-withDescription">',
            '<label><input is="emby-checkbox" type="checkbox" id="chkCwbEnabled"' + cwbEnabled + ' /><span>Bump series with new episodes to the front of Continue Watching</span></label>',
            '<div class="fieldDescription">Applies only to series you had watched up to the end. It bumps once per new episode: if the new episode stays unwatched, later episodes do not bump it again. Runs about 2 minutes after a library scan, plus the scheduled task <em>Continue Watching Bump</em> as a catch-up.</div>',
            '</div>',
            '<div id="cwbConfig" style="display:' + cwbDisplay + '">',
            '<div class="inputContainer" style="margin-top:16px;">',
            '<select is="emby-select" id="selCwbMode" label="Bump when">',
            '<option value="AllEpisodes"' + (cwbMode === 'AllEpisodes' ? ' selected' : '') + '>Any new episode arrives</option>',
            '<option value="NewSeasonsOnly"' + (cwbMode === 'NewSeasonsOnly' ? ' selected' : '') + '>A new season arrives</option>',
            '</select>',
            '<div class="fieldDescription">"A new season" only bumps when the next episode to watch starts a new season (e.g. S02E01 after finishing season 1).</div>',
            '</div>',
            '<div class="checkboxContainer checkboxContainer-withDescription" style="margin-top:8px;">',
            '<label><input is="emby-checkbox" type="checkbox" id="chkCwbAllUsers"' + cwbAllUsers + ' /><span>All users</span></label>',
            '<div class="fieldDescription">Applies to every user, including users added later.</div>',
            '</div>',
            '<div id="cwbUserListWrap" style="display:' + cwbUsersDisp + '">',
            '<p class="textMuted" style="font-size:0.88em;margin:12px 0;">Bump Continue Watching for these users:</p>',
            (cwbUserRows ? userListSelectLinksHtml() : ''),
            '<div class="hsc-user-list" id="cwbUserList">',
            cwbUserRows ? buildSelectAllRowHtml('hsc-user-row') + cwbUserRows : '<p class="textMuted" style="font-size:0.85em;">No users found.</p>',
            '</div>',
            '</div>',
            '</div>',

            '<div style="margin-top:16px; padding:10px 14px; border:1px solid rgba(128,128,128,0.2); border-radius:6px;">',
            '<div class="hsc-eyebrow" style="margin-bottom:6px;">Good to know</div>',
            '<ul style="font-size:0.85em; line-height:1.5; margin:0; padding-left:18px; opacity:0.85;">',
            '<li>The <strong>"last played" date</strong> of your last watched episode is set to now. Watched status and play count are unchanged, but anything showing or sorting by last played (e.g. watch history, tag rules using <em>Last played</em>) will see the new date.</li>',
            '<li><strong>Replacing an unwatched episode</strong> (e.g. a quality upgrade) makes it look newly added and can bump the series again.</li>',
            '<li>No bump if you <strong>skipped episodes</strong> — every episode up to your last watched one must be played. Specials (season 0) are ignored.</li>',
            '<li>A series you have <strong>hidden</strong> from Continue Watching stays hidden.</li>',
            '</ul>',
            '</div>',

            '</div>',
            '</div>'
        ].join('');

        container.dataset.loaded = '1';
    }

    function loadHstTab(view) {
        var container = view.querySelector('#hstContainer');
        if (!container) return;

        getHseUsers()
            .then(function (users) {
                renderHstTab(container, lastHscConfig, users || []);
                wireSelectAll(container.querySelector('#cwbUserList'), '.cwb-user-chk');

                var cwbEnableChk = container.querySelector('#chkCwbEnabled');
                if (cwbEnableChk) {
                    cwbEnableChk.addEventListener('change', function () {
                        var cwbConfig = container.querySelector('#cwbConfig');
                        if (cwbConfig) cwbConfig.style.display = this.checked ? '' : 'none';
                    });
                }
                var cwbAllUsersChk = container.querySelector('#chkCwbAllUsers');
                if (cwbAllUsersChk) {
                    cwbAllUsersChk.addEventListener('change', function () {
                        var wrap = container.querySelector('#cwbUserListWrap');
                        if (wrap) wrap.style.display = this.checked ? 'none' : '';
                    });
                }

                var cwbList = container.querySelector('#cwbUserList');
                wireUserListSelectLinks(cwbList && cwbList.previousElementSibling, cwbList);

                container.querySelectorAll('input, select').forEach(function (el) {
                    el.addEventListener('change', function () { setTimeout(checkFormState, 0); });
                    el.addEventListener('input',  function () { setTimeout(checkFormState, 0); });
                });
            })
            .catch(function () {
                container.innerHTML = '<p class="textMuted" style="padding:20px;">Failed to load users. Check server connection.</p>';
            });
    }

    function getManDragAfterElement(container, y) {
        var els = [...container.querySelectorAll('.man-section-row:not(.man-dragging)')];
        return els.reduce(function (closest, child) {
            var box = child.getBoundingClientRect();
            var offset = y - box.top - box.height / 2;
            if (offset < 0 && offset > closest.offset) return { offset: offset, element: child };
            return closest;
        }, { offset: Number.NEGATIVE_INFINITY }).element;
    }

    function loadTagManageTab(view) {
        var container = view.querySelector('#tcManageContainer');
        if (!container) return;

        container.innerHTML = '<div style="padding:20px;color:var(--theme-text-secondary);display:flex;align-items:center;gap:10px;">Loading <span class="tc-dot-loader"><span></span><span></span><span></span></span></div>';

        var token = window.ApiClient.accessToken();
        var pendingTagDeletes = {};       // tagName.toLowerCase() -> { name, itemCount }
        var pendingCollDeletes = {};      // collectionId -> { id, name, itemCount }
        var pendingPlDeletes = {};        // playlistId -> { id, name, itemCount, owner, managed }

        Promise.all([
            fetch(window.ApiClient.getUrl('HomeScreenCompanion/Manage/Tags'), { headers: { 'X-MediaBrowser-Token': token } }).then(function (r) { return r.json(); }),
            fetch(window.ApiClient.getUrl('HomeScreenCompanion/Manage/Collections'), { headers: { 'X-MediaBrowser-Token': token } }).then(function (r) { return r.json(); }),
            window.ApiClient.getPluginConfiguration(pluginId).catch(function () { return { Tags: [] }; }),
            fetch(window.ApiClient.getUrl('HomeScreenCompanion/Manage/Playlists'), { headers: { 'X-MediaBrowser-Token': token } })
                .then(function (r) { return r.ok ? r.json() : { Playlists: [] }; }).catch(function () { return { Playlists: [] }; })
        ]).then(function (results) {
            var tagsData = results[0];
            var collectionsData = results[1];
            var pluginConfig = results[2];
            var playlists = (results[3] && results[3].Playlists) || [];
            var playlistById = {};
            playlists.forEach(function (p) { playlistById[p.Id] = p; });

            // Build maps: which tags/collections are managed by a plugin group
            // Each logical group may appear multiple times in cfg.Tags (one entry per URL/source).
            // Deduplicate by Tag value so we get one entry per logical group.
            var managedTagMap = {};   // tagName.toLowerCase() -> [{ displayName, groupIndex, groupActive }, ...]
            var managedCollMap = {};  // collectionName.toLowerCase() -> [{ displayName, groupIndex, groupActive }, ...]
            var seenGroupByTag = {};  // tKey -> Set of first-seen indices (to deduplicate multi-URL groups)
            (pluginConfig.Tags || []).forEach(function (t, idx) {
                if (!t.Tag) return;
                var tName = t.Tag.trim();
                var tKey = tName.toLowerCase();
                // Skip if we already recorded an entry for this exact tag from a previous URL-row of the same group
                if (seenGroupByTag[tKey]) return;
                seenGroupByTag[tKey] = true;
                var groupLabel = (t.Name && t.Name.trim() && t.Name.trim().toLowerCase() !== tKey) ? t.Name.trim() : tName;
                var entry = { displayName: groupLabel, groupIndex: idx, groupActive: !!t.Active };
                if (!managedTagMap[tKey]) managedTagMap[tKey] = [];
                managedTagMap[tKey].push(entry);
                if (t.EnableCollection) {
                    var cName = (t.CollectionName && t.CollectionName.trim()) ? t.CollectionName.trim() : tName;
                    var cKey = cName.toLowerCase();
                    if (!managedCollMap[cKey]) managedCollMap[cKey] = [];
                    managedCollMap[cKey].push(entry);
                }
            });

            function escAttr(s) { return String(s || '').replace(/&/g, '&amp;').replace(/"/g, '&quot;'); }
            function escHtml(s) { return String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); }


            function renderSection(title, items, isTagSection, headerExtra) {
                var sectionId = isTagSection ? 'tcTagSection' : 'tcCollSection';
                var rows = items.length === 0
                    ? '<div style="color:var(--theme-text-secondary);padding:8px 0;">No items found.</div>'
                    : items.map(function (item) {
                        var id = item.Id || '';
                        var name = item.Name || '';
                        var count = item.ItemCount != null ? item.ItemCount : 0;
                        var managed = isTagSection ? managedTagMap[name.toLowerCase()] : managedCollMap[name.toLowerCase()];
                        var badge = managed && managed.length > 0
                            ? '<span class="hsc-managed-pill">Managed by HSC Plugin</span>'
                            : '';
                        var typesVal = isTagSection ? (item.ItemTypes || []).map(function (t) { return t.toLowerCase(); }).join(',') : '';
                        return '<tr class="tc-manage-row" data-rowname="' + escAttr(name.toLowerCase()) + '" data-managed="' + (managed && managed.length > 0 ? '1' : '0') + '" data-count="' + count + '" data-types="' + escAttr(typesVal) + '">' +
                            '<td style="padding:9px 4px;border-bottom:1px solid var(--line-color);width:100%;max-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">' +
                            (id
                                ? '<a class="tc-item-name tc-nav-link" href="javascript:void(0)" data-navid="' + escAttr(id) + '" style="color:inherit;text-decoration:none;cursor:pointer;" onmouseover="this.style.textDecoration=\'underline\'" onmouseout="this.style.textDecoration=\'none\'">' + escHtml(name) + '</a>'
                                : '<span class="tc-item-name">' + escHtml(name) + '</span>') +
                            badge +
                            '</td>' +
                            '<td style="padding:9px 4px 9px 16px;border-bottom:1px solid var(--line-color);white-space:nowrap;color:var(--theme-text-secondary);font-size:0.88em;">' + count + ' items</td>' +
                            '<td style="padding:9px 4px 9px 8px;border-bottom:1px solid var(--line-color);white-space:nowrap;">' +
                            '<button type="button" class="btnTcMark" data-id="' + escAttr(id) + '" data-name="' + escAttr(name) + '" data-count="' + count + '" data-type="' + (isTagSection ? 'tag' : 'coll') + '">Remove</button>' +
                            '</td>' +
                            '</tr>';
                    }).join('');

                return '<div id="' + sectionId + '" style="flex:1 1 300px;min-width:0;">' +
                    '<div style="display:flex;align-items:center;gap:30px;margin-bottom:12px;">' + 
                    '<h3 class="hsc-title" style="margin:0;">' + escHtml(title) + '</h3>' +
                    (headerExtra || '') +
                    '<button type="button" class="btnTcRefresh" style="background:transparent;color:var(--theme-text-secondary);border:1px solid var(--hsc-hairline-strong);margin-left:auto;"><i class="md-icon" style="font-size:1em;vertical-align:middle;">refresh</i></button>' +
                    '</div>' +
                    '<table class="tc-manage-list" style="width:100%;border-collapse:collapse;"><tbody>' + rows + '</tbody></table>' +
                    '</div>';
            }

            // Playlists: every user's, with the owner on a line under the name (several users often
            // have a playlist with the same name, e.g. one per user from an HSC source).
            function renderPlaylistSection(headerExtra) {
                var cell = 'padding:9px 4px;border-bottom:1px solid var(--line-color);';
                var rows = playlists.length === 0
                    ? '<div style="color:var(--theme-text-secondary);padding:8px 0;">No items found.</div>'
                    : playlists.map(function (p) {
                        var id = p.Id || '';
                        var name = p.Name || '';
                        var count = p.ItemCount != null ? p.ItemCount : 0;
                        var owner = p.OwnerName || '';
                        var shared = p.SharedWith || [];
                        var ownerTip = (owner ? 'Owner: ' + owner : 'No user owns this playlist (e.g. its owner was removed, or Emby found its playlist file again).') +
                            (p.IsPublic ? '\nPublic: every user sees it' : '') +
                            (shared.length ? '\n' + (owner ? 'Also shared with: ' : 'Shared with: ') + shared.join(', ') : '');
                        var ownerHtml = '<span title="' + escAttr(ownerTip) + '" style="display:inline-flex;align-items:center;gap:4px;max-width:100%;color:var(--theme-text-secondary);">' +
                            (owner
                                ? '<i class="md-icon" style="font-size:1.1em;">person</i><span style="overflow:hidden;text-overflow:ellipsis;">' + escHtml(owner) + '</span>'
                                : '<span style="display:inline-flex;align-items:center;gap:4px;color:var(--hsc-warning-text);"><i class="md-icon" style="font-size:1.1em;">person_off</i>No owner</span>') +
                            (shared.length ? '<span style="opacity:0.8;">+' + shared.length + '</span>' : '') +
                            (p.IsPublic ? '<span style="display:inline-flex;align-items:center;gap:2px;margin-left:6px;"><i class="md-icon" style="font-size:1.1em;">public</i>Public</span>' : '') +
                            '</span>';
                        var badge = p.ManagedBy ? '<span class="hsc-managed-pill">Managed by HSC Plugin</span>' : '';
                        return '<tr class="tc-manage-row" data-rowname="' + escAttr(name.toLowerCase()) + '" data-owner="' + escAttr(owner.toLowerCase()) + '" data-ownerid="' + escAttr(p.OwnerId || '') + '" data-managed="' + (p.ManagedBy ? '1' : '0') + '" data-count="' + count + '">' +
                            '<td style="' + cell + 'width:100%;max-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">' +
                            (id
                                ? '<a class="tc-item-name tc-nav-link" href="javascript:void(0)" data-navid="' + escAttr(id) + '" style="color:inherit;text-decoration:none;cursor:pointer;" onmouseover="this.style.textDecoration=\'underline\'" onmouseout="this.style.textDecoration=\'none\'">' + escHtml(name) + '</a>'
                                : '<span class="tc-item-name">' + escHtml(name) + '</span>') +
                            badge +
                            '<div style="font-size:0.85em;margin-top:2px;overflow:hidden;">' + ownerHtml + '</div>' +
                            '</td>' +
                            '<td style="' + cell + 'padding-left:16px;white-space:nowrap;color:var(--theme-text-secondary);font-size:0.88em;">' + count + ' items</td>' +
                            '<td style="' + cell + 'padding-left:8px;white-space:nowrap;">' +
                            '<button type="button" class="btnTcMark" data-id="' + escAttr(id) + '" data-name="' + escAttr(name) + '" data-count="' + count + '" data-type="pl">Remove</button>' +
                            '</td>' +
                            '</tr>';
                    }).join('');

                return '<div id="tcPlSection" style="flex:1 1 300px;min-width:0;">' +
                    '<div style="display:flex;align-items:center;gap:30px;margin-bottom:12px;">' +
                    '<h3 class="hsc-title" style="margin:0;">Playlists</h3>' +
                    (headerExtra || '') +
                    '<button type="button" class="btnTcRefresh" style="background:transparent;color:var(--theme-text-secondary);border:1px solid var(--hsc-hairline-strong);margin-left:auto;"><i class="md-icon" style="font-size:1em;vertical-align:middle;">refresh</i></button>' +
                    '</div>' +
                    '<table class="tc-manage-list" style="width:100%;border-collapse:collapse;"><tbody>' + rows + '</tbody></table>' +
                    '</div>';
            }

            // The owners present, for the Playlists filter ("" = No owner).
            var plOwners = [];
            var plOwnerSeen = {};
            playlists.forEach(function (p) {
                var key = p.OwnerId || '';
                if (plOwnerSeen[key]) return;
                plOwnerSeen[key] = true;
                plOwners.push({ id: key, name: p.OwnerName || '' });
            });
            plOwners.sort(function (a, b) { return (a.id === '') - (b.id === '') || natCmp(a.name.toLowerCase(), b.name.toLowerCase()); });
            var ownerFilterDropdownHtml = plOwners.length > 1
                ? '<div class="filter-dropdown-wrapper" id="tcOwnerFilterWrap">' +
                  '<div class="filter-dropdown-btn" id="tcOwnerFilterBtn">' +
                  '<i class="md-icon" style="font-size:1.1em;">filter_list</i>' +
                  '<span id="tcOwnerFilterLabel">Filter users</span>' +
                  '<i class="md-icon" style="font-size:0.9em;opacity:0.6;" id="tcOwnerFilterCaret">expand_more</i>' +
                  '</div>' +
                  '<div class="filter-dropdown-panel" id="tcOwnerFilterDropdown">' +
                  '<div class="filter-dropdown-label">Owner</div>' +
                  plOwners.map(function (o) {
                      return '<label class="filter-chk-row"><input type="checkbox" class="cbOwnerFilter" data-owner="' + escAttr(o.id) + '"> <span>' + (o.id ? escHtml(o.name) : '<em>No owner</em>') + '</span></label>';
                  }).join('') +
                  '</div></div>'
                : '';

            var searchInputStyle = 'background:rgba(128,128,128,0.08);border:1px solid var(--line-color);border-radius:6px;padding:5px 10px;font-size:0.9em;color:inherit;width:400px;max-width:100%;';

            var tcTypeGroups = [
                { label: 'Movies',       types: ['movie'] },
                { label: 'Series',       types: ['series'] },
                { label: 'Episodes',     types: ['episode'] },
                { label: 'Seasons',      types: ['season'] },
                { label: 'Music',        types: ['audio', 'musicvideo', 'musicalbum', 'musicartist'] },
                { label: 'Books',        types: ['book'] },
                { label: 'Games',        types: ['game'] },
                { label: 'Trailers',     types: ['trailer'] },
                { label: 'Theme songs',  types: ['themesong'] },
                { label: 'Theme videos', types: ['themevideo', 'video'] },
                { label: 'Extras',       types: ['behindthescenes', 'deletedscene', 'interview', 'scene', 'clip', 'featurette', 'short'] },
                { label: 'People',       types: ['person'] },
                { label: 'Collections',  types: ['boxset'] },
                { label: 'Photos',       types: ['photo', 'photoalbum'] },
                { label: 'Playlists',    types: ['playlist'] },
                { label: 'Recordings',   types: ['recording'] },
                { label: 'Studios',      types: ['studio'] }
            ];

            var tcExtraTypes = ['themesong', 'themevideo', 'trailer', 'behindthescenes', 'deletedscene', 'interview', 'scene', 'clip', 'featurette', 'short'];

            var presentGroups = tcTypeGroups.filter(function (g) {
                return (tagsData.Tags || []).some(function (tag) {
                    return (tag.ItemTypes || []).some(function (t) {
                        return g.types.indexOf(t.toLowerCase()) !== -1;
                    });
                });
            });

            var typeFilterDropdownHtml = presentGroups.length > 0
                ? '<div class="filter-dropdown-wrapper" id="tcTypeFilterWrap">' +
                  '<div class="filter-dropdown-btn" id="tcTypeFilterBtn">' +
                  '<i class="md-icon" style="font-size:1.1em;">filter_list</i>' +
                  '<span id="tcTypeFilterLabel">Filter tags</span>' +
                  '<i class="md-icon" style="font-size:0.9em;opacity:0.6;" id="tcTypeFilterCaret">expand_more</i>' +
                  '</div>' +
                  '<div class="filter-dropdown-panel" id="tcTypeFilterDropdown">' +
                  '<div class="filter-dropdown-label">Media type</div>' +
                  presentGroups.map(function (g) {
                      return '<label class="filter-chk-row"><input type="checkbox" class="cbTypeFilter" data-group="' + escAttr(g.label) + '"> <span>' + escHtml(g.label) + '</span></label>';
                  }).join('') +
                  '</div></div>'
                : '';

            var extrasCheckboxHtml =
                '<label style="display:flex;align-items:center;gap:6px;cursor:pointer;font-size:0.9em;white-space:nowrap;opacity:0.8;">' +
                '<input type="checkbox" id="cbIncludeExtras" style="cursor:pointer;margin:0;">' +
                '<span>Include extras</span>' +
                '</label>';

            // One list at a time, picked with the segmented control at the top (remembered in this browser).
            var tcSections = [
                { key: 'tags', label: 'Tags',        icon: 'label',              id: 'tcTagSection' },
                { key: 'coll', label: 'Collections', icon: 'video_library',      id: 'tcCollSection' },
                { key: 'pl',   label: 'Playlists',   icon: 'playlist_play',      id: 'tcPlSection' }
            ];
            var shownSection = 'tags';
            try {
                var savedShown = localStorage.getItem('HomeScreenCompanion_CleanupList');
                if (tcSections.some(function (s) { return s.key === savedShown; })) shownSection = savedShown;
            } catch (e) {}

            var tcSegBase = 'flex:1;cursor:pointer;border:none;border-radius:6px;padding:8px 12px;font-family:inherit;font-size:0.9em;font-weight:500;display:flex;align-items:center;justify-content:center;gap:6px;color:inherit;';
            var tcSegOn  = tcSegBase + 'background:var(--plugin-popup-bg2, var(--theme-background-level3));';
            var tcSegOff = tcSegBase + 'background:transparent;opacity:0.7;';
            var listPickerHtml =
                '<div id="tcListPicker" style="display:flex;border:1px solid var(--plugin-popup-border,rgba(255,255,255,0.12));border-radius:8px;overflow:hidden;margin-bottom:16px;padding:2px;gap:2px;background:var(--hsc-tint-1);max-width:560px;">' +
                tcSections.map(function (s) {
                    return '<button type="button" class="btnTcList" data-key="' + s.key + '" style="' + (s.key === shownSection ? tcSegOn : tcSegOff) + '"><i class="md-icon" style="font-size:1.1em;">' + s.icon + '</i>' + s.label + '</button>';
                }).join('') +
                '</div>';

            container.innerHTML =
                listPickerHtml +
                '<div style="display:flex;align-items:center;gap:10px;margin-bottom:16px;flex-wrap:wrap;">' +
                '<input type="text" id="tcSearch" placeholder="Search…" style="' + searchInputStyle + '" />' +
                '<select is="emby-select" id="tcSort" style="color:inherit;background:rgba(128,128,128,0.08);border:1px solid var(--line-color);padding:5px;border-radius:6px;font-size:0.9em;cursor:pointer;">' +
                '<option value="name-asc">Name A–Z</option>' +
                '<option value="name-desc">Name Z–A</option>' +
                '<option value="count-desc">Most items</option>' +
                '<option value="count-asc">Fewest items</option>' +
                '<option value="managed">Managed first</option>' +
                '</select>' +
                '<label style="display:flex;align-items:center;gap:6px;cursor:pointer;font-size:0.9em;white-space:nowrap;opacity:0.8;">' +
                '<input type="checkbox" id="cbTcOnlyManaged" style="cursor:pointer;margin:0;">' +
                '<span>Only managed by HSC</span>' +
                '</label>' +
                '</div>' +
                '<div id="tcSectionsWrap" style="display:flex;flex-wrap:wrap;gap:40px;align-items:flex-start;">' +
                renderSection('Tags', tagsData.Tags || [], true, typeFilterDropdownHtml + extrasCheckboxHtml) +
                renderSection('Collections', collectionsData.Collections || [], false) +
                renderPlaylistSection(ownerFilterDropdownHtml) +
                '</div>';

            container.dataset.loaded = '1';

            function applyShownSection() {
                tcSections.forEach(function (s) {
                    var el = container.querySelector('#' + s.id);
                    if (el) el.style.display = s.key === shownSection ? '' : 'none';
                });
                container.querySelectorAll('.btnTcList').forEach(function (btn) {
                    btn.style.cssText = btn.dataset.key === shownSection ? tcSegOn : tcSegOff;
                });
            }
            applyShownSection();

            container.querySelectorAll('.btnTcList').forEach(function (btn) {
                btn.addEventListener('click', function () {
                    shownSection = this.dataset.key;
                    try { localStorage.setItem('HomeScreenCompanion_CleanupList', shownSection); } catch (e) {}
                    applyShownSection();
                });
            });

            function getSelectedTypeGroups() {
                return Array.from(container.querySelectorAll('.cbTypeFilter:checked')).map(function (cb) { return cb.dataset.group; });
            }

            function getSelectedOwners() {
                return Array.from(container.querySelectorAll('.cbOwnerFilter:checked')).map(function (cb) { return cb.dataset.owner || ''; });
            }

            function rowMatchesTypeFilter(row, selectedGroups) {
                if (selectedGroups.length === 0) return true;
                var rowTypes = (row.dataset.types || '').split(',').filter(Boolean);
                return selectedGroups.some(function (groupLabel) {
                    var group = tcTypeGroups.find(function (g) { return g.label === groupLabel; });
                    if (!group) return false;
                    return rowTypes.some(function (t) { return group.types.indexOf(t) !== -1; });
                });
            }

            function updateTypeFilterBtn() {
                var btn = container.querySelector('#tcTypeFilterBtn');
                var lbl = container.querySelector('#tcTypeFilterLabel');
                if (!btn || !lbl) return;
                var selected = getSelectedTypeGroups();
                lbl.textContent = selected.length === 0 ? 'Filter tags' : selected.length + ' type' + (selected.length > 1 ? 's' : '');
                if (selected.length > 0) btn.classList.add('active');
                else btn.classList.remove('active');
            }

            function applySearchSort() {
                var query = (container.querySelector('#tcSearch').value || '').toLowerCase();
                var sort = container.querySelector('#tcSort').value;
                var selectedGroups = getSelectedTypeGroups();
                var includeExtras = !!(container.querySelector('#cbIncludeExtras') || {}).checked;
                var onlyManaged = !!(container.querySelector('#cbTcOnlyManaged') || {}).checked;
                var selectedOwners = getSelectedOwners();

                ['tcTagSection', 'tcCollSection', 'tcPlSection'].forEach(function (sectionId) {
                    var section = container.querySelector('#' + sectionId);
                    if (!section) return;
                    var rows = Array.from(section.querySelectorAll('.tc-manage-row'));
                    var isTagSection = sectionId === 'tcTagSection';
                    var isPlSection = sectionId === 'tcPlSection';

                    rows.forEach(function (row) {
                        var rowName = row.dataset.rowname || '';
                        // A playlist is also found by its owner's name.
                        var nameMatch = !query || rowName.indexOf(query) !== -1 || (isPlSection && (row.dataset.owner || '').indexOf(query) !== -1);
                        var ownerOk = !isPlSection || selectedOwners.length === 0 || selectedOwners.indexOf(row.dataset.ownerid || '') !== -1;
                        var typeMatch = !isTagSection || rowMatchesTypeFilter(row, selectedGroups);
                        var extrasOk = !isTagSection || includeExtras || (function () {
                            var types = (row.dataset.types || '').split(',').filter(Boolean);
                            return types.length === 0 || !types.every(function (t) { return tcExtraTypes.indexOf(t) !== -1; });
                        })();
                        var managedOk = !onlyManaged || row.dataset.managed === '1';
                        row.style.display = (nameMatch && typeMatch && extrasOk && managedOk && ownerOk) ? '' : 'none';
                    });

                    var list = section.querySelector('.tc-manage-list');
                    if (!list) return;
                    var visibleRows = rows.filter(function (r) { return r.style.display !== 'none'; });
                    visibleRows.sort(function (a, b) {
                        var nameA = a.dataset.rowname || '';
                        var nameB = b.dataset.rowname || '';
                        var countA = parseInt(a.dataset.count || '0', 10);
                        var countB = parseInt(b.dataset.count || '0', 10);
                        var managedA = a.dataset.managed === '1';
                        var managedB = b.dataset.managed === '1';
                        // Same-named playlists stay in owner order.
                        var ownerCmp = natCmp(a.dataset.owner || '', b.dataset.owner || '');
                        if (sort === 'name-asc') return natCmp(nameA, nameB) || ownerCmp;
                        if (sort === 'name-desc') return natCmp(nameB, nameA) || ownerCmp;
                        if (sort === 'count-desc') return countB - countA;
                        if (sort === 'count-asc') return countA - countB;
                        if (sort === 'managed') return (managedB ? 1 : 0) - (managedA ? 1 : 0) || natCmp(nameA, nameB);
                        return 0;
                    });
                    visibleRows.forEach(function (r) { list.appendChild(r); });
                });
            }

            container.querySelector('#tcSearch').addEventListener('input', applySearchSort);
            container.querySelector('#tcSort').addEventListener('change', applySearchSort);
            var cbIncludeExtras = container.querySelector('#cbIncludeExtras');
            if (cbIncludeExtras) cbIncludeExtras.addEventListener('change', applySearchSort);
            // "Only managed by HSC" is remembered in this browser.
            var cbOnlyManaged = container.querySelector('#cbTcOnlyManaged');
            try { cbOnlyManaged.checked = localStorage.getItem('HomeScreenCompanion_CleanupOnlyManaged') === '1'; } catch (e) {}
            cbOnlyManaged.addEventListener('change', function () {
                try { localStorage.setItem('HomeScreenCompanion_CleanupOnlyManaged', this.checked ? '1' : '0'); } catch (e) {}
                applySearchSort();
            });
            if (cbOnlyManaged.checked) applySearchSort();

            var typeFilterBtn = container.querySelector('#tcTypeFilterBtn');
            var typeFilterDropdown = container.querySelector('#tcTypeFilterDropdown');
            var typeFilterCaret = container.querySelector('#tcTypeFilterCaret');
            if (typeFilterBtn && typeFilterDropdown) {
                typeFilterBtn.addEventListener('click', function (e) {
                    e.stopPropagation();
                    var open = typeFilterDropdown.classList.toggle('open');
                    if (typeFilterCaret) typeFilterCaret.textContent = open ? 'expand_less' : 'expand_more';
                });
                typeFilterDropdown.addEventListener('change', function (e) {
                    if (e.target.classList.contains('cbTypeFilter')) {
                        updateTypeFilterBtn();
                        applySearchSort();
                    }
                });
                document.addEventListener('click', function closeTypeFilter(e) {
                    if (!typeFilterDropdown.contains(e.target) && !typeFilterBtn.contains(e.target)) {
                        typeFilterDropdown.classList.remove('open');
                        if (typeFilterCaret) typeFilterCaret.textContent = 'expand_more';
                    }
                    if (!container.isConnected) document.removeEventListener('click', closeTypeFilter);
                });
            }

            var ownerFilterBtn = container.querySelector('#tcOwnerFilterBtn');
            var ownerFilterDropdown = container.querySelector('#tcOwnerFilterDropdown');
            var ownerFilterCaret = container.querySelector('#tcOwnerFilterCaret');
            if (ownerFilterBtn && ownerFilterDropdown) {
                ownerFilterBtn.addEventListener('click', function (e) {
                    e.stopPropagation();
                    var open = ownerFilterDropdown.classList.toggle('open');
                    if (ownerFilterCaret) ownerFilterCaret.textContent = open ? 'expand_less' : 'expand_more';
                });
                ownerFilterDropdown.addEventListener('change', function (e) {
                    if (!e.target.classList.contains('cbOwnerFilter')) return;
                    var n = getSelectedOwners().length;
                    var lbl = container.querySelector('#tcOwnerFilterLabel');
                    if (lbl) lbl.textContent = n === 0 ? 'Filter users' : n + ' user' + (n > 1 ? 's' : '');
                    ownerFilterBtn.classList.toggle('active', n > 0);
                    applySearchSort();
                });
                document.addEventListener('click', function closeOwnerFilter(e) {
                    if (!ownerFilterDropdown.contains(e.target) && !ownerFilterBtn.contains(e.target)) {
                        ownerFilterDropdown.classList.remove('open');
                        if (ownerFilterCaret) ownerFilterCaret.textContent = 'expand_more';
                    }
                    if (!container.isConnected) document.removeEventListener('click', closeOwnerFilter);
                });
            }

            applySearchSort();

            // ── Hover tooltip: visar vilka objekttyper en tagg finns på ──
            var tcTooltip = document.createElement('div');
            tcTooltip.style.cssText = 'position:fixed;z-index:9999;pointer-events:none;display:none;' +
                'background:var(--plugin-popup-bg,#2a2a2a);color:var(--plugin-popup-color,#eee);' +
                'border:1px solid var(--plugin-popup-border,#444);border-radius:6px;' +
                'padding:8px 12px;font-size:0.82em;line-height:1.6;max-width:210px;' +
                'box-shadow:var(--hsc-shadow-float);';
            document.body.appendChild(tcTooltip);

            // Ta bort tooltip när containern lämnar DOM
            var tcTooltipObserver = new MutationObserver(function () {
                if (!container.isConnected) { tcTooltip.remove(); tcTooltipObserver.disconnect(); }
            });
            if (container.parentNode) tcTooltipObserver.observe(container.parentNode, { childList: true });

            function getTypeLabels(typesStr) {
                var rawTypes = (typesStr || '').split(',').filter(Boolean);
                if (!rawTypes.length) return null;
                var seen = {};
                var labels = [];
                tcTypeGroups.forEach(function (g) {
                    if (!seen[g.label] && rawTypes.some(function (t) { return g.types.indexOf(t) !== -1; })) {
                        seen[g.label] = true;
                        labels.push(g.label);
                    }
                });
                return labels.length ? labels : null;
            }

            var tcTagSection = container.querySelector('#tcTagSection');
            if (tcTagSection) {
                tcTagSection.addEventListener('mouseover', function (e) {
                    var nameEl = e.target.closest('.tc-item-name');
                    if (!nameEl) { tcTooltip.style.display = 'none'; return; }
                    var row = nameEl.closest('.tc-manage-row');
                    if (!row) return;
                    var labels = getTypeLabels(row.dataset.types);
                    if (!labels) { tcTooltip.style.display = 'none'; return; }
                    tcTooltip.innerHTML =
                        '<div class="hsc-eyebrow" style="margin-bottom:5px;color:inherit;opacity:0.7;">Found in</div>' +
                        labels.map(function (l) {
                            return '<div style="display:flex;align-items:center;gap:6px;">' +
                                '<i class="md-icon" style="font-size:0.95em;opacity:0.7;">label</i>' +
                                escHtml(l) + '</div>';
                        }).join('');
                    tcTooltip.style.display = 'block';
                });
                tcTagSection.addEventListener('mousemove', function (e) {
                    var nameEl = e.target.closest('.tc-item-name');
                    if (!nameEl) { tcTooltip.style.display = 'none'; return; }
                    tcTooltip.style.left = (e.clientX + 16) + 'px';
                    tcTooltip.style.top = (e.clientY + 12) + 'px';
                    var rect = tcTooltip.getBoundingClientRect();
                    if (rect.right > window.innerWidth - 8) tcTooltip.style.left = (e.clientX - rect.width - 16) + 'px';
                    if (rect.bottom > window.innerHeight - 8) tcTooltip.style.top = (e.clientY - rect.height - 12) + 'px';
                });
                tcTagSection.addEventListener('mouseout', function (e) {
                    var nameEl = e.target.closest('.tc-item-name');
                    if (!nameEl) return;
                    if (e.relatedTarget && nameEl.contains(e.relatedTarget)) return;
                    tcTooltip.style.display = 'none';
                });
            }

            function updateSaveButton() {
                var hasPending = Object.keys(pendingTagDeletes).length > 0 || Object.keys(pendingCollDeletes).length > 0 || Object.keys(pendingPlDeletes).length > 0;
                container._tcHasPending = hasPending;
                checkFormState();
            }

            if (container._tcClickHandler) container.removeEventListener('click', container._tcClickHandler);
            container._tcClickHandler = function (e) {
                var navLink = e.target.closest('.tc-nav-link');
                if (navLink) {
                    var navId = navLink.dataset.navid;
                    var baseUrl = window.location.href.split('#')[0];
                    var serverId = (window.ApiClient && window.ApiClient.serverId) ? window.ApiClient.serverId() : '';
                    var url = baseUrl + '#!/item?id=' + encodeURIComponent(navId) +
                              (serverId ? '&serverId=' + encodeURIComponent(serverId) : '');
                    window.open(url, '_blank');
                    return;
                }

                var btn = e.target.closest('button');
                if (!btn) return;

                // Mark for deletion
                if (btn.classList.contains('btnTcMark')) {
                    var type = btn.dataset.type;
                    var id = btn.dataset.id;
                    var name = btn.dataset.name;
                    var count = parseInt(btn.dataset.count || '0', 10);
                    if (type === 'tag') pendingTagDeletes[id.toLowerCase()] = { name: name, itemCount: count };
                    else if (type === 'pl') pendingPlDeletes[id] = { id: id, name: name, itemCount: count, playlist: playlistById[id] || {} };
                    else pendingCollDeletes[id] = { id: id, name: name, itemCount: count };
                    var row = btn.closest('.tc-manage-row');
                    if (row) {
                        row.style.opacity = '0.45';
                        var nameEl = row.querySelector('.tc-item-name');
                        if (nameEl) nameEl.style.textDecoration = 'line-through';
                        btn.textContent = 'Undo';
                        btn.classList.remove('btnTcMark');
                        btn.classList.add('btnTcUndo');
                    }
                    updateSaveButton();
                    return;
                }

                // Undo pending deletion
                if (btn.classList.contains('btnTcUndo')) {
                    var type = btn.dataset.type;
                    var id = btn.dataset.id;
                    if (type === 'tag') delete pendingTagDeletes[id.toLowerCase()];
                    else if (type === 'pl') delete pendingPlDeletes[id];
                    else delete pendingCollDeletes[id];
                    var row = btn.closest('.tc-manage-row');
                    if (row) {
                        row.style.opacity = '1';
                        var nameEl = row.querySelector('.tc-item-name');
                        if (nameEl) nameEl.style.textDecoration = '';
                        btn.textContent = 'Remove';
                        btn.classList.remove('btnTcUndo');
                        btn.classList.add('btnTcMark');
                    }
                    updateSaveButton();
                    return;
                }

                // Refresh
                if (btn.classList.contains('btnTcRefresh')) {
                    container.dataset.loaded = '';
                    loadTagManageTab(view);
                    return;
                }

            };
            container.addEventListener('click', container._tcClickHandler);

            container._tcShowModal = function () { showSummaryModal(); };

            function showSummaryModal() {
                var undoBtnStyle = 'cursor:pointer;border:none;background:transparent;color:var(--hsc-danger-text);border-radius:6px;padding:2px 6px;font-size:1.1em;line-height:1;margin-right:8px;flex-shrink:0;';

                function buildRows(items, isTag) {
                    return items.map(function (item) {
                        var name = item.name;
                        var key = isTag ? item.name.toLowerCase() : item.id;
                        var managed = isTag ? managedTagMap[name.toLowerCase()] : managedCollMap[name.toLowerCase()];
                        var warning = '';
                        var activeManaged = managed ? managed.filter(function (m) { return m.groupActive; }) : [];
                        if (activeManaged.length > 0) {
                            var what = isTag ? 'recreate this tag' : 'recreate this collection';
                            var warningText = activeManaged.length === 1
                                ? 'Group <strong>' + escHtml(activeManaged[0].displayName) + '</strong> is active and may ' + what + ' on next sync.'
                                : activeManaged.length + ' active groups may ' + what + ' on next sync.';
                            var checkboxes = activeManaged.map(function (m) {
                                return '<label style="display:flex;align-items:center;gap:6px;margin-top:5px;cursor:pointer;">' +
                                    '<input type="checkbox" class="cbInactivateGroup" data-group-indices="' + escAttr(JSON.stringify([m.groupIndex])) + '"> ' +
                                    'Deactivate <strong>' + escHtml(m.displayName) + '</strong>' +
                                    '</label>';
                            }).join('');
                            warning =
                                '<div style="color:var(--hsc-warning-text);margin-top:6px;font-size:0.88em;">' +
                                '<i class="md-icon" style="font-size:1em;vertical-align:middle;margin-right:4px;">warning</i>' +
                                warningText + checkboxes + '</div>';
                        }
                        return '<div style="padding:10px 0;border-bottom:1px solid var(--line-color);display:flex;align-items:flex-start;">' +
                            '<button type="button" class="btnModalUndo" style="' + undoBtnStyle + '" data-key="' + escAttr(key) + '" data-type="' + (isTag ? 'tag' : 'coll') + '" title="Keep this one">✕</button>' +
                            '<div style="flex:1;">' +
                            '<span style="font-weight:500;">' + escHtml(name) + '</span>' +
                            '<span style="color:var(--theme-text-secondary);font-size:0.88em;margin-left:8px;">(' + item.itemCount + ' items)</span>' +
                            warning +
                            '</div>' +
                            '</div>';
                    }).join('');
                }

                function buildPlRows(items) {
                    return items.map(function (item) {
                        var p = item.playlist || {};
                        var warning = '';
                        if (p.ManagedBy && p.ManagedActive) {
                            warning =
                                '<div style="color:var(--hsc-warning-text);margin-top:6px;font-size:0.88em;">' +
                                '<i class="md-icon" style="font-size:1em;vertical-align:middle;margin-right:4px;">warning</i>' +
                                'Group <strong>' + escHtml(p.ManagedBy) + '</strong> is active and may recreate this playlist on next sync.' +
                                '<label style="display:flex;align-items:center;gap:6px;margin-top:5px;cursor:pointer;">' +
                                '<input type="checkbox" class="cbInactivateGroup" data-group-indices="' + escAttr(JSON.stringify([p.ManagedIndex])) + '"> ' +
                                'Deactivate <strong>' + escHtml(p.ManagedBy) + '</strong>' +
                                '</label></div>';
                        }
                        return '<div style="padding:10px 0;border-bottom:1px solid var(--line-color);display:flex;align-items:flex-start;">' +
                            '<button type="button" class="btnModalUndo" style="' + undoBtnStyle + '" data-key="' + escAttr(item.id) + '" data-type="pl" title="Keep this one">✕</button>' +
                            '<div style="flex:1;">' +
                            '<span style="font-weight:500;">' + escHtml(item.name) + '</span>' +
                            '<span style="color:var(--theme-text-secondary);font-size:0.88em;margin-left:8px;">(' + item.itemCount + ' items)</span>' +
                            '<div style="color:var(--theme-text-secondary);font-size:0.88em;margin-top:2px;">' +
                            (p.OwnerName ? 'Owner: ' + escHtml(p.OwnerName) : 'No owner') +
                            (p.IsPublic ? ' · public (every user sees it)' : '') +
                            ((p.SharedWith || []).length ? (p.OwnerName ? ' · also shared with ' : ' · shared with ') + escHtml(p.SharedWith.join(', ')) : '') +
                            '</div>' +
                            warning +
                            '</div>' +
                            '</div>';
                    }).join('');
                }

                function buildContent() {
                    var tagList = Object.values(pendingTagDeletes);
                    var collList = Object.values(pendingCollDeletes);
                    var plList = Object.values(pendingPlDeletes);
                    var tagSection = tagList.length > 0
                        ? '<div style="margin-bottom:20px;"><h4 style="margin:0 0 8px;font-weight:600;">Tags to remove (' + tagList.length + ')</h4>' + buildRows(tagList, true) + '</div>'
                        : '';
                    var collSection = collList.length > 0
                        ? '<div style="margin-bottom:20px;"><h4 style="margin:0 0 8px;font-weight:600;">Collections to remove (' + collList.length + ')</h4>' + buildRows(collList, false) + '</div>'
                        : '';
                    var plSection = plList.length > 0
                        ? '<div style="margin-bottom:20px;"><h4 style="margin:0 0 8px;font-weight:600;">Playlists to remove (' + plList.length + ')</h4>' + buildPlRows(plList) + '</div>'
                        : '';
                    return tagSection + collSection + plSection;
                }

                var modal = document.createElement('div');
                modal.style.cssText = 'position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.6);z-index:9999;display:flex;align-items:center;justify-content:center;';

                function renderModal() {
                    var tagList = Object.values(pendingTagDeletes);
                    var collList = Object.values(pendingCollDeletes);
                    var plList = Object.values(pendingPlDeletes);
                    if (tagList.length === 0 && collList.length === 0 && plList.length === 0) { modal.remove(); updateSaveButton(); return; }
                    modal.innerHTML =
                        '<div style="background:var(--plugin-popup-bg,#2a2a2a);color:var(--plugin-popup-color,#e8e8e8);border:1px solid var(--plugin-popup-border,rgba(255,255,255,0.12));border-radius:12px;box-shadow:var(--hsc-shadow-modal);overscroll-behavior:contain;padding:28px;max-width:600px;width:90%;max-height:80vh;overflow-y:auto;">' +
                        '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:20px;">' +
                        '<h3 style="margin:0;font-size:1.05em;font-weight:600;">Summary — Pending changes</h3>' +
                        '<button type="button" id="tcModalClose" class="hsc-x" aria-label="Close" title="Close" style="background:transparent;border:none;color:inherit;cursor:pointer;padding:2px;opacity:0.6;line-height:1;border-radius:8px;"><i class="md-icon">close</i></button>' +
                        '</div>' +
                        '<div id="tcModalBody">' + buildContent() + '</div>' +
                        '<div style="display:flex;justify-content:flex-end;gap:12px;margin-top:20px;border-top:1px solid var(--line-color);padding-top:16px;">' +
                        '<button type="button" id="tcModalCancel" class="hsc-mbtn hsc-btn-secondary">Cancel</button>' +
                        '<button type="button" id="tcModalConfirm" class="hsc-mbtn hsc-btn-primary"><i class="md-icon" style="font-size:1em;vertical-align:middle;margin-right:5px;">check</i>Confirm &amp; Save</button>' +
                        '</div></div>';

                    modal.querySelector('#tcModalCancel').addEventListener('click', function () { modal.remove(); });
                    modal.querySelector('#tcModalClose').addEventListener('click', function () { modal.querySelector('#tcModalCancel').click(); });

                    modal.addEventListener('click', function (e) {
                        var undoBtn = e.target.closest('.btnModalUndo');
                        if (!undoBtn) return;
                        var type = undoBtn.dataset.type;
                        var key = undoBtn.dataset.key;
                        if (type === 'tag') delete pendingTagDeletes[key];
                        else if (type === 'pl') delete pendingPlDeletes[key];
                        else delete pendingCollDeletes[key];

                        // Restore the row in the main list directly (no .click() to avoid re-triggering handler)
                        var keyLower = key.toLowerCase();
                        var mainBtn = Array.from(container.querySelectorAll('.btnTcUndo')).find(function (b) { return b.dataset.id.toLowerCase() === keyLower; });
                        if (mainBtn) {
                            var row = mainBtn.closest('.tc-manage-row');
                            if (row) {
                                row.style.opacity = '1';
                                var nameEl = row.querySelector('.tc-item-name');
                                if (nameEl) nameEl.style.textDecoration = '';
                            }
                            mainBtn.textContent = 'Remove';
                            mainBtn.classList.remove('btnTcUndo');
                            mainBtn.classList.add('btnTcMark');
                        }

                        updateSaveButton();
                        renderModal();
                    });

                    modal.querySelector('#tcModalConfirm').addEventListener('click', function () {
                        var confirmBtn = modal.querySelector('#tcModalConfirm');
                        confirmBtn.disabled = true;
                        confirmBtn.innerHTML = 'Saving <span class="tc-dot-loader"><span></span><span></span><span></span></span>';

                        var tagList = Object.values(pendingTagDeletes);
                        var collList = Object.values(pendingCollDeletes);
                        var plList = Object.values(pendingPlDeletes);

                        var groupsToInactivate = new Set();
                        modal.querySelectorAll('.cbInactivateGroup:checked').forEach(function (cb) {
                            var indices = cb.dataset.groupIndices ? JSON.parse(cb.dataset.groupIndices) : [];
                            indices.forEach(function (idx) { groupsToInactivate.add(idx); });
                        });

                        var tok = window.ApiClient.accessToken();

                        // All tags in one batch request — avoids race conditions when items carry multiple managed tags
                        var tagBatchPromise = tagList.length === 0 ? Promise.resolve() :
                            fetch(window.ApiClient.getUrl('HomeScreenCompanion/Manage/DeleteTags'), {
                                method: 'POST',
                                headers: { 'Content-Type': 'application/json', 'X-MediaBrowser-Token': tok },
                                body: JSON.stringify({ TagNames: tagList.map(function (t) { return t.name; }) })
                            }).then(function (r) { return r.json(); });

                        // Collections are independent — run sequentially after tags
                        var collOps = [];
                        collList.forEach(function (c) {
                            collOps.push(function () {
                                return fetch(window.ApiClient.getUrl('HomeScreenCompanion/Manage/DeleteCollection'), {
                                    method: 'POST',
                                    headers: { 'Content-Type': 'application/json', 'X-MediaBrowser-Token': tok },
                                    body: JSON.stringify({ CollectionId: c.id })
                                }).then(function (r) { return r.json(); });
                            });
                        });
                        plList.forEach(function (p) {
                            collOps.push(function () {
                                return fetch(window.ApiClient.getUrl('HomeScreenCompanion/Manage/DeletePlaylist'), {
                                    method: 'POST',
                                    headers: { 'Content-Type': 'application/json', 'X-MediaBrowser-Token': tok },
                                    body: JSON.stringify({ PlaylistId: p.id })
                                }).then(function (r) { return r.json(); });
                            });
                        });

                        tagBatchPromise.then(function () {
                            return collOps.reduce(function (chain, op) { return chain.then(op); }, Promise.resolve());
                        }).then(function () {
                            if (groupsToInactivate.size === 0) return Promise.resolve();
                            return window.ApiClient.getPluginConfiguration(pluginId).then(function (cfg) {
                                var stampBefore = hscServerStamp(cfg);
                                // Collect the Tag values for the seed indices, then inactivate ALL rows sharing that Tag
                                var tagsToInactivate = new Set();
                                groupsToInactivate.forEach(function (idx) {
                                    if (cfg.Tags && cfg.Tags[idx] && cfg.Tags[idx].Tag)
                                        tagsToInactivate.add(cfg.Tags[idx].Tag.trim().toLowerCase());
                                });
                                (cfg.Tags || []).forEach(function (t) {
                                    if (t.Tag && tagsToInactivate.has(t.Tag.trim().toLowerCase()))
                                        t.Active = false;
                                });
                                return window.ApiClient.updatePluginConfiguration(pluginId, cfg)
                                    .then(function (r) { return advanceServerStamp(stampBefore).then(function () { return r; }); });
                            });
                        }).then(function () {
                            modal.remove();
                            // Update SOURCES tab rows for deactivated groups immediately.
                            // groupsToInactivate holds seed indices; find all rows sharing the same Tag value.
                            var inactivatedTagKeys = new Set();
                            groupsToInactivate.forEach(function (idx) {
                                var seedRow = view.querySelector('#tagListContainer .tag-row[data-index="' + idx + '"]');
                                if (seedRow && seedRow.dataset.tag) inactivatedTagKeys.add(seedRow.dataset.tag.toLowerCase());
                            });
                            view.querySelectorAll('#tagListContainer .tag-row').forEach(function (sourceRow) {
                                var rowTag = (sourceRow.dataset.tag || '').toLowerCase();
                                if (!inactivatedTagKeys.has(rowTag)) return;
                                var chk = sourceRow.querySelector('.chkTagActive');
                                var lbl = sourceRow.querySelector('.lblActiveStatus');
                                if (chk) chk.checked = false;
                                if (lbl) { lbl.textContent = 'Disabled'; lbl.style.color = 'var(--hsc-neutral)'; }
                                sourceRow.classList.add('inactive');
                                var runBtn = sourceRow.querySelector('.btnRunEntry');
                                if (runBtn) { runBtn.disabled = true; runBtn.style.opacity = '0.4'; }
                            });
                            pendingTagDeletes = {};
                            pendingCollDeletes = {};
                            pendingPlDeletes = {};
                            container._tcHasPending = false;
                            checkFormState();
                            container.dataset.loaded = '';
                            loadTagManageTab(view);
                        }).catch(function (err) {
                            modal.remove();
                            alert('Error saving: ' + err);
                        });
                    });
                }

                renderModal();
                document.body.appendChild(modal);
            }

        }).catch(function (err) {
            container.innerHTML = '<div style="color:var(--hsc-danger-text);padding:20px;">Failed to load: ' + err + '</div>';
        });
    }

    function executeTopListCreationSteps(tagName, displayName, selectedUserIds, displayMode, customName, imageType, maxItems, prepareResult, ui, onSuccess) {
        var saveBtn    = ui.saveBtn;
        var errEl      = ui.errEl;
        var modal      = ui.modal;
        var badgeStyle = ui.badgeStyle || 'neutral';
        var isShows    = ui.contentType === 'Shows';
        var badgeOptions = ui.badgeOptions;   // undefined: keep what the list has
        var realPoster = ui.realPoster;       // undefined: keep what the list has
        var tok = window.ApiClient.accessToken ? window.ApiClient.accessToken() : '';
        var snapshotId = null;
        var pendingLibraryId = null;

        saveBtn.innerHTML = 'Creating library <span class="tc-dot-loader"><span></span><span></span><span></span></span>';

        // Step 0: Snapshot all user policies server-side BEFORE library creation.
        // POST Library/VirtualFolders causes Emby to set EnableAllFolders=true for all users.
        // The server stores the exact per-user state and restores it in step 4b.
        // The chain is returned so silent callers (backup restore) can await it.
        return fetch(window.ApiClient.getUrl('HomeScreenCompanion/TopList/SnapshotPolicies'), {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'X-Emby-Token': tok },
            body: JSON.stringify({})
        })
        .then(function(r) { return r.json(); })
        .then(function(result) {
            if (result && result.SnapshotId) {
                snapshotId = result.SnapshotId;
                console.log('[HSC] Policy snapshot taken:', result.UserCount, 'users, id:', snapshotId);
            }
        }).catch(function() {})
        .then(function() {

        // Steps 2+3: Check if the virtual library already exists before creating it.
        // POSTing to Library/VirtualFolders — even for an already-existing library — causes
        // Emby to update all users' policies and fire "User Policy Updated" notifications.
        // By skipping the POST when the library is already present we avoid spurious notifications.
        return fetch(window.ApiClient.getUrl('Library/VirtualFolders'), {
            headers: { 'X-MediaBrowser-Token': tok }
        })
        .then(function (r) { return r.json(); })
        .then(function (existingFolders) {
            // Normalize path separators and strip trailing slashes before comparing,
            // since the C# backend uses backslashes on Windows while Emby's API may
            // return forward slashes (or vice versa).
            function normLibPath(p) { return (p || '').replace(/\\/g, '/').replace(/\/+$/, '').toLowerCase(); }
            var targetPath = normLibPath(prepareResult.FolderPath);
            // Record all library IDs before creation so we can find the new one by diff
            // if path matching fails (Emby may not register the library synchronously).
            var preCreationIds = new Set((existingFolders || []).map(function (f) { return f.ItemId; }).filter(Boolean));
            var alreadyExists = (existingFolders || []).some(function (f) {
                return (f.Locations || []).some(function (loc) {
                    return normLibPath(loc) === targetPath;
                });
            });
            if (alreadyExists) {
                return { folders: existingFolders, preCreationIds: new Set() };
            }
            // Library doesn't exist yet — create it, then re-fetch the updated list.
            // A show list's library only holds copies the plugin writes complete (nfo + images
            // linked from the originals), so it needs no online metadata, and it stays out of
            // search where every show would otherwise appear twice.
            var libraryOptions = isShows
                ? {
                    EnableInternetProviders: false,
                    ExcludeFromSearch: true,
                    TypeOptions: ['Series', 'Season', 'Episode'].map(function (t) {
                        return { Type: t, MetadataFetchers: ['Nfo'], MetadataFetcherOrder: ['Nfo'], ImageFetchers: [], ImageFetcherOrder: [] };
                    })
                }
                : {
                    EnableInternetProviders: true,
                    TypeOptions: [{
                        Type: 'Movie',
                        MetadataFetchers: ['Nfo', 'TheMovieDb', 'TheTVDB'],
                        MetadataFetcherOrder: ['Nfo', 'TheMovieDb', 'TheTVDB'],
                        ImageFetchers: ['TheMovieDb', 'TheTVDB'],
                        ImageFetcherOrder: ['TheMovieDb', 'TheTVDB']
                    }]
                };
            return fetch(window.ApiClient.getUrl('Library/VirtualFolders'), {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', 'X-Emby-Token': tok },
                body: JSON.stringify({
                    Name: customName,
                    CollectionType: isShows ? 'tvshows' : 'movies',
                    RefreshLibrary: false,
                    Paths: [prepareResult.FolderPath],
                    LibraryOptions: libraryOptions
                })
            }).catch(function () {})
            .then(function () {
                // Emby may register the new library asynchronously — re-fetch a few times until
                // it shows up, otherwise the library id stays 'auto' and nothing gets scanned.
                function fetchUntilRegistered(attempt) {
                    return fetch(window.ApiClient.getUrl('Library/VirtualFolders'), {
                        headers: { 'X-MediaBrowser-Token': tok }
                    }).then(function (r) { return r.json(); })
                    .then(function (folders) {
                        var found = (folders || []).some(function (f) {
                            return f.ItemId && (f.Locations || []).some(function (loc) {
                                return normLibPath(loc) === targetPath;
                            });
                        });
                        if (found || attempt >= 10) return folders;
                        return new Promise(function (res) { setTimeout(res, 1000); })
                            .then(function () { return fetchUntilRegistered(attempt + 1); });
                    });
                }
                return fetchUntilRegistered(1);
            })
            .then(function (newFolders) {
                return { folders: newFolders, preCreationIds: preCreationIds };
            });
        })
        .then(function (result) {
            saveBtn.innerHTML = 'Saving settings <span class="tc-dot-loader"><span></span><span></span><span></span></span>';

            var folders = result.folders;
            var preCreationIds = result.preCreationIds;

            // Find the library by its folder path to get its ItemId
            function normLibPath2(p) { return (p || '').replace(/\\/g, '/').replace(/\/+$/, '').toLowerCase(); }
            var match = (folders || []).find(function (f) {
                return (f.Locations || []).some(function (loc) {
                    return normLibPath2(loc) === normLibPath2(prepareResult.FolderPath);
                });
            });
            var newLibId = match ? match.ItemId : null;

            // Fallback: if path matching failed (timing issue), find the new library by
            // comparing current IDs against the pre-creation snapshot.
            if (!newLibId && preCreationIds.size > 0) {
                var diffMatch = (folders || []).find(function (f) {
                    return f.ItemId && !preCreationIds.has(f.ItemId);
                });
                if (diffMatch) newLibId = diffMatch.ItemId;
            }

            // Same view list Emby's section editor offers under "Libraries" (hidden libraries and
            // channels included), for every selected user — anything missing stays ticked.
            var viewsPromise = Promise.all(selectedUserIds.map(function (userId) {
                return fetch(window.ApiClient.getUrl('Users/' + userId + '/Views', { IncludeHidden: true, AllowDynamicChildren: false }), { headers: { 'X-MediaBrowser-Token': tok } })
                    .then(function (r) { return r.json(); })
                    .catch(function () { return { Items: [] }; });
            }));

            return viewsPromise.then(function (viewsResults) {
                var allViewIds = new Set();
                (folders || []).forEach(function (f) { if (f.ItemId && f.ItemId !== newLibId) allViewIds.add(f.ItemId); });
                viewsResults.forEach(function (viewsResult) {
                    ((viewsResult && viewsResult.Items) || []).forEach(function (v) {
                        if (!v.Id || (newLibId && (v.Id === newLibId || v.Guid === newLibId))) return;
                        allViewIds.add(v.Id);
                    });
                });
                var excludedViewIds = Array.from(allViewIds).join(',');
                return { prepareResult: prepareResult, libraryItemId: newLibId, excludedViewIds: excludedViewIds };
            });
        })
        .then(function (ctx) {
            // Step 4: Save top-list home section config
            return window.ApiClient.getPluginConfiguration(pluginId).then(function (config) {
                var topLists = config.TopLists || [];
                var existing = topLists.find(function (t) {
                    return (t.TagName || '').toLowerCase() === tagName.toLowerCase();
                });
                var storedSettings = {};
                try { storedSettings = JSON.parse((existing && existing.HomeSectionSettings) || '{}'); } catch (e) {}
                var cardSizeOffset = ui.cardSizeOffset != null ? normCardSize(ui.cardSizeOffset) : normCardSize(storedSettings.CardSizeOffset);
                if (badgeOptions == null) badgeOptions = storedSettings.BadgeOptions || '';
                if (realPoster == null) realPoster = storedSettings.RealPosterBadge || '';
                // Callers without a Position field keep the list's saved one.
                var position = ui.position != null ? ui.position : (storedSettings._position || '');
                var hseSettings = JSON.stringify({
                    SectionType: 'items',
                    DisplayMode: displayMode,
                    CustomName: customName,
                    MaxItems: String(maxItems),
                    ViewType: '',
                    ImageType: imageType,
                    CardSizeOffset: cardSizeOffset,
                    BadgeStyle: badgeStyle, BadgeOptions: badgeOptions, RealPosterBadge: realPoster,
                    SortBy: 'SortName',
                    SortOrder: 'Ascending',
                    ScrollDirection: '',
                    ItemTypes: JSON.stringify([isShows ? 'Series' : 'Movie']),
                    _queryIsPlayed: '',
                    _queryExcludeViewIds: ctx.excludedViewIds,
                    ExcludedFolders: ctx.excludedViewIds,
                    _position: position
                });
                if (existing) {
                    existing.HomeSectionUserIds = selectedUserIds;
                    existing.HomeSectionLibraryId = ctx.libraryItemId || 'auto';
                    existing.HomeSectionSettings = hseSettings;
                    existing.HomeSectionTracked = existing.HomeSectionTracked || [];
                    existing.MaxItems = maxItems;
                    existing.ContentType = isShows ? 'Shows' : 'Movies';
                } else {
                    topLists.push({
                        TagName: tagName,
                        MaxItems: maxItems,
                        HomeSectionUserIds: selectedUserIds,
                        HomeSectionLibraryId: ctx.libraryItemId || 'auto',
                        HomeSectionSettings: hseSettings,
                        HomeSectionTracked: [],
                        ContentType: isShows ? 'Shows' : 'Movies'
                    });
                }
                config.TopLists = topLists;
                return window.ApiClient.updatePluginConfiguration(pluginId, config)
                    .then(function () { return { prepareResult: ctx.prepareResult, libraryItemId: ctx.libraryItemId }; });
            });
        })
        .then(function (ctx2) {
            // Step 4b: Store the library ID for the final access-restore step.
            // RestoreAndGrantAccess runs AFTER the library scan (last step) to ensure Emby
            // has fully registered the new library before we write it to EnabledFolders.
            if (ctx2.libraryItemId) pendingLibraryId = ctx2.libraryItemId;
            return ctx2;
        })
        .then(function (ctx2) {
            // Step 5: Create home sections immediately
            saveBtn.innerHTML = 'Creating home sections <span class="tc-dot-loader"><span></span><span></span><span></span></span>';
            var tok5 = window.ApiClient.accessToken ? window.ApiClient.accessToken() : '';
            return fetch(window.ApiClient.getUrl('HomeScreenCompanion/TopList/SyncHomeSections'), {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', 'X-Emby-Token': tok5 },
                body: JSON.stringify({ TagName: tagName })
            })
            .then(function (r) { return r.json(); })
            .then(function (syncResult) {
                if (!syncResult.Success) throw new Error(syncResult.Message || 'Failed to create home sections.');
                return ctx2;
            });
        })
        .then(function (ctx2) {
            // Step 6: Scan the new library so Emby indexes and identifies the .strm files.
            // The library is created with RefreshLibrary=false and the files were written before
            // it existed, so without this scan the library (and the home section) stays empty.
            saveBtn.innerHTML = 'Scanning library <span class="tc-dot-loader"><span></span><span></span><span></span></span>';
            var tok6 = window.ApiClient.accessToken ? window.ApiClient.accessToken() : '';
            var scanOwnLibrary = ctx2.libraryItemId
                ? fetch(window.ApiClient.getUrl('Items/' + ctx2.libraryItemId + '/Refresh') + '?Recursive=true&MetadataRefreshMode=Default&ImageRefreshMode=Default', {
                    method: 'POST',
                    headers: { 'X-MediaBrowser-Token': tok6 }
                }).then(function (r) { return r.ok; }).catch(function () { return false; })
                : Promise.resolve(false);
            return scanOwnLibrary.then(function (ok) {
                if (ok) return;
                // Fallback: full library scan (also picks up the new library's files).
                console.warn('[HSC] Top-list library refresh failed — falling back to a full library scan.');
                return fetch(window.ApiClient.getUrl('Library/Refresh'), {
                    method: 'POST',
                    headers: { 'X-MediaBrowser-Token': tok6 }
                }).catch(function () {});
            })
            .then(function () {
                // Step 6b: Wait until the scan has indexed + identified the files, so the
                // final section sync runs against a populated library.
                var expected = ctx2.prepareResult.FilesCreated || 0;
                if (!expected) return;
                var deadline = Date.now() + 120000;
                function poll() {
                    return fetch(window.ApiClient.getUrl('HomeScreenCompanion/TopList/IndexStatus', { FolderPath: ctx2.prepareResult.FolderPath }), {
                        headers: { 'X-MediaBrowser-Token': tok6 }
                    })
                    .then(function (r) { return r.json(); })
                    .catch(function () { return null; })
                    .then(function (st) {
                        if (!st || !st.Success) return;
                        saveBtn.innerHTML = 'Identifying ' + (isShows ? 'shows' : 'movies') + ' (' + Math.min(st.Identified, expected) + '/' + expected + ') <span class="tc-dot-loader"><span></span><span></span><span></span></span>';
                        if (st.Indexed >= expected && st.Identified >= st.Indexed) return;
                        if (Date.now() > deadline) {
                            console.warn('[HSC] Top-list scan still running after timeout — ' + st.Indexed + '/' + expected + ' indexed, ' + st.Identified + ' identified.');
                            return;
                        }
                        return new Promise(function (res) { setTimeout(res, 2000); }).then(poll);
                    });
                }
                return poll();
            })
            .then(function () { return ctx2.prepareResult; });
        })
        .then(function (prepareResult) {
            // NOTE: Linking each .strm as an alternate version of its original movie AND probing it
            // for a real RunTimeTicks (so resume works instead of marking the film fully-watched) is
            // handled automatically server-side by the ItemAdded/ItemUpdated hook in ServerEntryPoint,
            // which fires as Emby indexes each .strm. No UI step is needed here — hence the
            // "finishing touches will continue in the background" note on the success screen.
            // Step 7: Sync all top-list home sections so exclusion lists are up to date
            var tok7 = window.ApiClient.accessToken ? window.ApiClient.accessToken() : '';
            return fetch(window.ApiClient.getUrl('HomeScreenCompanion/TopList/SyncAllSections'), {
                method: 'POST',
                headers: { 'X-MediaBrowser-Token': tok7 }
            }).catch(function () {}).then(function () { return prepareResult; });
        })
        .then(function (prepareResult) {
            // Step 8: Grant library access to all restricted users via Emby's standard
            // user-policy API (GET /Users + POST /Users/{Id}/Policy).
            // This avoids server-side reflection entirely and works with any Emby version.
            // Users with EnableAllFolders=true already have access — skip them.
            // Users with EnableAllFolders=false get the new library added to EnabledFolders
            // without touching any of their other existing settings.
            if (!pendingLibraryId) return prepareResult;
            var tok8 = window.ApiClient.accessToken ? window.ApiClient.accessToken() : '';
            var libIdLower = pendingLibraryId.toLowerCase();
            return fetch(window.ApiClient.getUrl('Users'), {
                headers: { 'X-MediaBrowser-Token': tok8 }
            })
            .then(function (r) { return r.json(); })
            .then(function (users) {
                // GET /Users may return Policy=null for users other than the caller in
                // some Emby versions. Fetch each user individually to guarantee full Policy data.
                return Promise.all((users || []).map(function (u) {
                    if (u && u.Policy) return Promise.resolve(u);
                    return fetch(window.ApiClient.getUrl('Users/' + u.Id), {
                        headers: { 'X-MediaBrowser-Token': tok8 }
                    }).then(function (r) { return r.json(); }).catch(function () { return u; });
                }));
            })
            .then(function (users) {
                var updates = (users || [])
                    .filter(function (u) {
                        return u && u.Policy && !u.Policy.EnableAllFolders &&
                            selectedUserIds.some(function (id) { return id.toLowerCase() === (u.Id || '').toLowerCase(); }) &&
                            !(u.Policy.EnabledFolders || []).some(function (f) {
                                return (f || '').toLowerCase() === libIdLower;
                            });
                    })
                    .map(function (u) {
                        var pol = JSON.parse(JSON.stringify(u.Policy));
                        pol.EnabledFolders = (u.Policy.EnabledFolders || []).concat([pendingLibraryId]);
                        return fetch(window.ApiClient.getUrl('Users/' + u.Id + '/Policy'), {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json', 'X-MediaBrowser-Token': tok8 },
                            body: JSON.stringify(pol)
                        }).catch(function () {});
                    });
                return Promise.all(updates);
            })
            .catch(function () {})
            .then(function () { return prepareResult; });
        })
        .then(function (prepareResult) {
            _topListTagNames.add(tagName.toLowerCase());
            refreshTopListBadges();
            if (ui.silent && typeof ui.closeHandler === 'function') { ui.closeHandler(); return; }
            var innerBox = ui.innerBox || modal.querySelector('div');
            innerBox.innerHTML =
                '<div style="text-align:center;padding:10px 0 20px;">' +
                '<i class="md-icon" style="font-size:2.5em;color:var(--hsc-primary-text);display:block;margin-bottom:12px;">check_circle</i>' +
                '<p style="margin:0 0 6px;font-size:1.05em;font-weight:500;">Top-list created!</p>' +
                '<p style="margin:0;opacity:0.65;font-size:0.9em;">' + prepareResult.FilesCreated + (isShows ? ' show' : ' movie') + (prepareResult.FilesCreated !== 1 ? 's' : '') + ' included.</p>' +
                '<p style="margin:8px 0 0;opacity:0.5;font-size:0.82em;font-style:italic;">Finishing touches will continue in the background.</p>' +
                '</div>' +
                '<div style="display:flex;justify-content:center;padding-top:16px;border-top:1px solid var(--line-color);margin-top:20px;">' +
                '<button type="button" class="btnTlmDone hsc-mbtn hsc-btn-primary">Close</button>' +
                '</div>';
            innerBox.querySelector('.btnTlmDone').addEventListener('click', function () {
                if (typeof ui.closeHandler === 'function') { ui.closeHandler(); }
                else { modal.remove(); if (typeof onSuccess === 'function') onSuccess(); }
            });
        })
        .catch(function (err) {
            saveBtn.disabled = false;
            saveBtn.innerHTML = '<i class="md-icon" style="font-size:1em;vertical-align:middle;margin-right:6px;">check</i>Save and apply';
            errEl.textContent = err.message || String(err);
            if (ui.silent) throw err;
        });
        }); // end step 0 wrapper
    }

    // ── Top-list badge: Badge Style ──────────────────────────────────────────────────
    // The six colour presets, the Top 10 tile and No number (the original picker). Customise is for
    // the Top 10 tile only: its options live in BadgeOptions (JSON, '' = the standard tile). A colour
    // preset can tick Top 3 medal colours (BadgeOptions {"medals":true}).
    var BADGE_PRESETS = [
        { val: 'neutral',    label: 'Neutral',    bg: 'rgba(0,0,0,0.82)' },
        { val: 'slate-grey', label: 'Slate grey', bg: 'rgba(65,65,75,0.88)' },
        { val: 'emby-green', label: 'Emby green', bg: 'rgba(82,181,75,0.78)' },
        { val: 'ocean-blue', label: 'Ocean blue', bg: 'rgba(46,134,193,0.82)' },
        { val: 'soft-red',   label: 'Soft red',   bg: 'rgba(201,69,69,0.82)' },
        { val: 'violet',     label: 'Violet',     bg: 'rgba(123,82,181,0.82)' }
    ];
    var _badgeStyles = [
        { val: 'neutral',    label: 'Neutral',    bg: 'rgba(0,0,0,0.82)',        textColor: '#fff' },
        { val: 'slate-grey', label: 'Slate grey', bg: 'rgba(65,65,75,0.88)',     textColor: '#fff' },
        { val: 'emby-green', label: 'Emby green', bg: 'rgba(82,181,75,0.78)',    textColor: '#fff' },
        { val: 'ocean-blue', label: 'Ocean blue', bg: 'rgba(46,134,193,0.82)',   textColor: '#fff' },
        { val: 'soft-red',   label: 'Soft red',   bg: 'rgba(201,69,69,0.82)',    textColor: '#fff' },
        { val: 'violet',     label: 'Violet',     bg: 'rgba(123,82,181,0.82)',   textColor: '#fff' },
        { val: 'top10',      label: 'Top 10 tile', bg: '#141414',                textColor: '#141414', tile: true },
        { val: 'none',       label: 'No number',  bg: 'transparent',             textColor: '#fff', noNumber: true }
    ];
    // The colour presets' one option (BadgeOptions {"medals":true}): ranks 1-3 in gold / silver / bronze.
    var BADGE_MEDALS_JSON = '{"medals":true}';
    function isBadgePreset(val) { return !!val && val !== 'top10' && val !== 'none'; }

    var _badgePickerSeq = 0;
    // contentKind 'shows' / 'movies': the list's titles (the previews draw Emby's count / ✓ chip for them).
    function buildBadgePickerHtml(selectedVal, optionsJson, realPoster, contentKind) {
        var sel = selectedVal || 'neutral';
        var esc = function (v) { return String(v || '').replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;'); };
        var cardBase = 'cursor:pointer;display:flex;flex-direction:column;align-items:center;gap:8px;padding:10px 12px;border-radius:6px;border:2px solid transparent;transition:border-color 0.15s;';
        var cardActive = cardBase + 'border-color:var(--hsc-primary);';
        var cardInactive = cardBase + 'border-color:var(--line-color,rgba(255,255,255,0.12));';
        var tileSel = sel === 'top10';
        // Same Customise button as the art pickers, under the tiles; shown for the Top 10 tile only.
        var custBtn = '<button type="button" class="btnBadgeCustomise hsc-mbtn hsc-btn-secondary"><i class="md-icon" style="font-size:1.1em;">settings</i><span>Customise</span></button>';
        // Each picker gets its own radio group: with one shared name, opening a second top-list
        // card (or the create popup) unticked the first card's choice, so its Save lit up.
        var radioName = 'tlBadgeStyle_' + (++_badgePickerSeq);
        // Options saved for a number badge (4.2.0.32-.36) are not used: the badge draws in its preset colour.
        var optKind = tileSel ? 'top10' : 'badge';
        var medalsOn = isBadgePreset(sel) && /"medals"\s*:\s*true/.test(optionsJson || '');
        var medalDot = function (c) { return '<span style="display:inline-block;width:12px;height:12px;border-radius:50%;background:' + c + ';"></span>'; };
        return '<div class="tl-badge-picker" data-content="' + (contentKind === 'shows' ? 'shows' : 'movies') + '" style="margin-bottom:16px;">' +
            '<span class=\"hsc-label\" style=\"display:block;margin-bottom:8px;color:inherit;opacity:0.75;\">Badge Style</span>' +
            '<input type="hidden" class="tl-badge-preset" value="neutral">' +
            '<input type="hidden" class="tl-badge-opts" data-optkind="' + optKind + '" value="' + esc(optionsJson || '') + '">' +
            '<div style="display:flex;gap:8px;flex-wrap:wrap;">' +
            _badgeStyles.map(function (s) {
                var active = s.val === sel;
                return '<label class="tl-badge-opt" data-kind="' + s.val + '" style="' + (active ? cardActive : cardInactive) + '">' +
                    '<input type="radio" name="' + radioName + '" value="' + s.val + '" style="position:absolute;opacity:0;pointer-events:none;"' + (active ? ' checked' : '') + '>' +
                    (s.tile
                        ? '<div style="width:82px;height:46px;border-radius:4px;background:' + s.bg + ';display:flex;align-items:flex-end;justify-content:center;gap:2px;overflow:hidden;"><span style="font-size:44px;line-height:40px;font-weight:900;color:' + s.textColor + ';-webkit-text-stroke:1.5px #9696a0;font-family:Impact,sans-serif;">7</span><span style="width:24px;height:36px;margin-bottom:5px;border-radius:2px;background:linear-gradient(160deg,#6b7a8f,#2c3440);"></span></div>'
                        : '<div style="width:46px;height:46px;border-radius:50%;background:' + s.bg + ';display:flex;align-items:center;justify-content:center;font-size:22px;font-weight:700;color:' + s.textColor + ';font-family:sans-serif;">' + (s.noNumber ? '' : '7') + '</div>') +
                    '<span style="font-size:0.78em;opacity:0.8;white-space:nowrap;">' + s.label + '</span>' +
                    '</label>';
            }).join('') +
            '</div>' +
            // One slot under the tiles: Top 3 medal colours for a colour preset, Customise for the Top 10 tile.
            '<div class="tl-badge-extra" style="display:flex;align-items:center;min-height:36px;margin-top:8px;">' +
            '<div class="tl-badge-medal-row checkboxContainer" style="margin:0;' + (isBadgePreset(sel) ? '' : 'display:none;') + '"><label>' +
                '<input is="emby-checkbox" type="checkbox" class="tl-badge-medals"' + (medalsOn ? ' checked' : '') + ' />' +
                '<span style="display:inline-flex;align-items:center;gap:6px;">Top 3 medal colours ' +
                    '<span style="display:inline-flex;gap:3px;">' + medalDot('#d4af37') + medalDot('#b8bcc6') + medalDot('#b87333') + '</span></span></label></div>' +
            '<div class="tl-badge-custom-row" style="display:' + (tileSel ? 'flex' : 'none') + ';align-items:center;gap:8px;">' + custBtn +
                '<span class="tl-badge-custom-note fieldDescription" style="margin:0;' + (tileSel && optionsJson ? '' : 'display:none;') + '">Customised</span>' +
            '</div>' +
            '</div>' +
            // The tile's big numeral has room for 1-10 (BadgeRenderer.RenderRanked).
            '<div class="tl-badge-top10-note fieldDescription" style="margin:2px 0 6px;' + (tileSel ? '' : 'display:none;') + '">' +
                '<i class="md-icon" style="font-size:1.15em;vertical-align:-3px;margin-right:4px;color:var(--hsc-info-text);">info</i>' +
                'The Top 10 tile has room for 10 titles. From number 11 on, titles get a round number badge instead: set Max items to 10 to keep the whole row as tiles.</div>' +
            buildRealPosterHtml(realPoster, contentKind) +
            '</div>';
    }

    // "Real posters" block under Badge Style: the TOP 10 square on the title's own poster
    // everywhere in Emby (drawn by the server's image enhancer; image files stay untouched).
    // Stored in HomeSectionSettings.RealPosterBadge: '' = off, else 'label|size' (top right) or
    // 'label|size|{"pos":"tl"}' (label top10 | top10rank, size s | m | l, pos tl | tr | bl | bc | br).
    // A value saved by 4.2.0.35/.36 with a whole Customise look there still loads: only its pos counts.
    function parseRealPoster(v) {
        var str = String(v || ''), i1 = str.indexOf('|'), i2 = i1 < 0 ? -1 : str.indexOf('|', i1 + 1);
        var p = [i1 < 0 ? str : str.slice(0, i1), i1 < 0 ? '' : str.slice(i1 + 1, i2 < 0 ? undefined : i2)];
        var opts = i2 < 0 ? '' : str.slice(i2 + 1), m = /"pos"\s*:\s*"(tl|tr|bl|bc|br)"/i.exec(opts);
        return { on: !!v, label: p[0] === 'top10rank' ? 'top10rank' : 'top10', size: (p[1] === 's' || p[1] === 'l') ? p[1] : 'm', pos: m ? m[1].toLowerCase() : 'tr' };
    }
    // The badge popup's Position control: a mini poster with a dot per spot, the spot's name beside it.
    var POS_SPOTS = [['tl', 'Top left'], ['tr', 'Top right'], ['bl', 'Bottom left'], ['bc', 'Bottom centre'], ['br', 'Bottom right']];
    // emby: mark Emby's chip in the top-right corner, with the top-right spot beside it.
    function posPickerHtml(spots, sel, emby) {
        var on = spots.some(function (p) { return p[0] === sel; }) ? sel : spots[0][0];
        var name = spots.filter(function (p) { return p[0] === on; })[0][1];
        return '<div class="artc-posrow"><div class="artc-posposter' + (emby ? ' artc-emby' : '') + '" data-field="pos">' + (emby ? '<span class="artc-pos-emby" title="Emby">\u2713</span>' : '') + spots.map(function (p) {
            return '<button type="button" class="artc-posdot artc-pos-' + p[0] + (on === p[0] ? ' artc-on' : '') + '" data-value="' + p[0] + '" title="' + p[1] + '" aria-label="' + p[1] + '"><span></span></button>';
        }).join('') + '</div><span class="artc-posname">' + escapeHtml(name) + '</span></div>';
    }
    function buildRealPosterHtml(value, contentKind) {
        var rp = parseRealPoster(value);
        function seg(key, opts, cur) {
            return '<div class="artc-seg" data-rp="' + key + '">' + opts.map(function (o) {
                return '<button type="button" class="artc-segbtn' + (o[0] === cur ? ' artc-on' : '') + '" data-value="' + o[0] + '">' + o[1] + '</button>';
            }).join('') + '</div>';
        }
        var lbl = 'font-size:0.82em;opacity:0.75;min-width:56px;';
        return '<div class="tl-rp" data-kind="' + (contentKind === 'shows' ? 'shows' : 'movies') + '" style="margin-top:14px;">' +
            '<span class=\"hsc-label\" style=\"display:block;margin-bottom:4px;color:inherit;opacity:0.75;\">Real posters</span>' +
            '<div class="checkboxContainer" style="margin-bottom:2px;"><label>' +
                '<input is="emby-checkbox" type="checkbox" class="tl-rp-on"' + (rp.on ? ' checked' : '') + ' />' +
                '<span>Badge the real posters too</span></label></div>' +
            '<div class="fieldDescription" style="margin:0 0 8px;">Puts a TOP 10 square on each listed title\'s own poster everywhere on the server (libraries, search, the item page, other rows and every app), not only in this top list. The image files are not changed.</div>' +
            '<div class="tl-rp-opts" style="' + (rp.on ? '' : 'display:none;') + 'margin-left:4px;">' +
                '<div style="display:flex;align-items:flex-start;gap:16px;flex-wrap:wrap;">' +
                '<div>' +
                '<div style="display:flex;align-items:center;gap:10px;margin-bottom:8px;"><span style="' + lbl + '">Label</span>' + seg('label', [['top10', 'TOP 10'], ['top10rank', 'TOP 10 #3']], rp.label) + '</div>' +
                '<div style="display:flex;align-items:center;gap:10px;margin-bottom:8px;"><span style="' + lbl + '">Size</span>' + seg('size', [['s', 'S'], ['m', 'M'], ['l', 'L']], rp.size) + '</div>' +
                '<div class="tl-rp-pos" style="display:flex;align-items:center;gap:10px;"><span style="' + lbl + '">Position</span>' + posPickerHtml(POS_SPOTS, rp.pos, true) + '</div>' +
                '</div>' +
                // The look the real posters get (rank 7 on a stand-in poster), drawn by the server.
                '<img class="tl-rp-icon" alt="" style="width:60px;height:90px;object-fit:contain;display:block;border-radius:3px;" />' +
                '</div>' +
            '</div>' +
            '</div>';
    }
    function readRealPoster(container) {
        var on = container.querySelector('.tl-rp-on');
        if (!on || !on.checked) return '';
        var l = container.querySelector('.tl-rp [data-rp="label"] .artc-on');
        var z = container.querySelector('.tl-rp [data-rp="size"] .artc-on');
        var d = container.querySelector('.tl-rp .artc-posdot.artc-on'), pos = d ? d.getAttribute('data-value') : 'tr';
        return (l ? l.getAttribute('data-value') : 'top10') + '|' + (z ? z.getAttribute('data-value') : 'm') + (pos !== 'tr' ? '|{"pos":"' + pos + '"}' : '');
    }
    // The real-poster icon: the server's drawing of the square with the block's Label / Size / Position.
    function refreshRealPosterBlock(block) {
        var chk = block.querySelector('.tl-rp-on'), icon = block.querySelector('.tl-rp-icon');
        if (!icon || !chk.checked) return;
        var token = readRealPoster(block);
        if (icon.dataset.iconFor === token) return;
        icon.dataset.iconFor = token;
        realPosterIconImage(token, block.dataset.kind).then(function (src) { if (icon.dataset.iconFor === token) icon.src = src; }).catch(function () { });
    }
    function realPosterIconImage(rpVal, kind) {
        var key = 'real|' + (kind || '') + '|' + rpVal;
        if (!_badgeIconCache[key])
            _badgeIconCache[key] = fetch(window.ApiClient.getUrl('HomeScreenCompanion/BadgeCustomPreview'), {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', 'X-MediaBrowser-Token': window.ApiClient.accessToken() },
                body: JSON.stringify({ BadgeStyle: 'neutral', Options: '', Variant: 'real', RealPoster: rpVal, Kind: kind || '' })
            }).then(function (r) { return r.json(); }).then(function (res) {
                if (!res.Success) throw new Error(res.Message || 'Preview failed');
                return res.Image;
            }).catch(function (e) { delete _badgeIconCache[key]; throw e; });
        return _badgeIconCache[key];
    }
    function initRealPoster(container) {
        container.querySelectorAll('.tl-rp').forEach(function (block) {
            if (block.dataset.rpInit) return;
            block.dataset.rpInit = '1';
            var chk = block.querySelector('.tl-rp-on');
            var optsEl = block.querySelector('.tl-rp-opts');
            chk.addEventListener('change', function () { optsEl.style.display = chk.checked ? '' : 'none'; refreshRealPosterBlock(block); });
            refreshRealPosterBlock(block);
            block.querySelectorAll('.artc-seg').forEach(function (segEl) {
                segEl.querySelectorAll('.artc-segbtn').forEach(function (btn) {
                    btn.addEventListener('click', function (e) {
                        e.preventDefault();
                        if (btn.classList.contains('artc-on')) return;
                        segEl.querySelectorAll('.artc-segbtn').forEach(function (b) { b.classList.remove('artc-on'); });
                        btn.classList.add('artc-on');
                        // The forms' dirty checks listen for change on their inputs.
                        chk.dispatchEvent(new Event('change', { bubbles: true }));
                    });
                });
            });
            block.querySelectorAll('.artc-posdot').forEach(function (dot) {
                dot.addEventListener('click', function (e) {
                    e.preventDefault();
                    if (dot.classList.contains('artc-on')) return;
                    block.querySelectorAll('.artc-posdot').forEach(function (x) { x.classList.remove('artc-on'); });
                    dot.classList.add('artc-on');
                    var nm = block.querySelector('.tl-rp-pos .artc-posname');
                    if (nm) nm.textContent = dot.getAttribute('title');
                    chk.dispatchEvent(new Event('change', { bubbles: true }));
                });
            });
        });
    }

    // The Top 10 tile icon: the original hand-made icon; once customised, the server's drawing of
    // the look (rank 7 on the stand-in poster) in the same box. Cached per look for the page.
    var _badgeIconCache = {};
    var _badgeTileCache = {};   // the badge popup's shape tiles, by look (kept for the page)
    function badgeIconImage(style, options, variant, kind) {
        var key = style + '|' + variant + '|' + (kind || '') + '|' + options;
        if (!_badgeIconCache[key])
            _badgeIconCache[key] = fetch(window.ApiClient.getUrl('HomeScreenCompanion/BadgeCustomPreview'), {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', 'X-MediaBrowser-Token': window.ApiClient.accessToken() },
                body: JSON.stringify({ BadgeStyle: style, Options: options, Variant: variant, Kind: kind || '' })
            }).then(function (r) { return r.json(); }).then(function (res) {
                if (!res.Success) throw new Error(res.Message || 'Preview failed');
                return res.Image;
            }).catch(function (e) { delete _badgeIconCache[key]; throw e; });
        return _badgeIconCache[key];
    }
    function refreshBadgeTileIcons(picker, container) {
        var optsIn = picker.querySelector('.tl-badge-opts');
        var label = picker.querySelector('.tl-badge-opt[data-kind="top10"]');
        if (!optsIn || !label) return;
        var radio = label.querySelector('input[type="radio"]'), art = radio && radio.nextElementSibling;
        if (!art || art.classList.contains('tl-badge-custom-icon')) return;
        var options = optsIn.value && optsIn.dataset.optkind === 'top10' ? optsIn.value : '';
        var imgSel = container && container.querySelector && container.querySelector('.tlm-image-type, .mtlImageType');
        var variant = imgSel && imgSel.value === 'Thumb' ? 'thumb' : 'primary';
        var kind = picker.dataset.content || '';
        var token = variant + '|' + kind + '|' + options;
        label.dataset.iconFor = token;
        var old = label.querySelector('.tl-badge-custom-icon');
        if (art.dataset.origDisplay == null) art.dataset.origDisplay = art.style.display;
        if (!options) { if (old) old.remove(); art.style.display = art.dataset.origDisplay; return; }
        badgeIconImage('top10', options, variant, kind).then(function (src) {
            if (label.dataset.iconFor !== token) return;   // changed again meanwhile
            var img = label.querySelector('.tl-badge-custom-icon');
            if (!img) {
                img = document.createElement('img');
                img.className = 'tl-badge-custom-icon';
                img.alt = '';
                img.style.cssText = 'width:82px;height:46px;object-fit:contain;display:block;';
                art.parentNode.insertBefore(img, art.nextSibling);
            }
            img.src = src;
            art.style.display = 'none';
        }).catch(function () { /* keep the original icon */ });
    }

    function initBadgePicker(container, tagName) {
        if (container && container.dataset) container.dataset.tlTag = tagName || '';
        initRealPoster(container);
        var opts = Array.from(container.querySelectorAll('.tl-badge-opt'));
        opts.forEach(function (label) {
            label.addEventListener('click', function () {
                opts.forEach(function (l) { l.style.borderColor = 'var(--line-color,rgba(255,255,255,0.12))'; });
                label.style.borderColor = 'var(--hsc-primary)';
            });
        });
        // One Customise button: the Top 10 tile's popup; hidden for the presets and No number.
        container.querySelectorAll('.tl-badge-picker').forEach(function (picker) {
            var btn = picker.querySelector('.btnBadgeCustomise');
            if (!btn) return;
            function sync() {
                var checked = picker.querySelector('input[name^="tlBadgeStyle"]:checked');
                var tile = !!checked && checked.value === 'top10';
                var custRow = picker.querySelector('.tl-badge-custom-row');
                if (custRow) custRow.style.display = tile ? 'flex' : 'none';
                var top10Note = picker.querySelector('.tl-badge-top10-note');
                if (top10Note) top10Note.style.display = tile ? '' : 'none';
                var medalRow = picker.querySelector('.tl-badge-medal-row');
                if (medalRow) medalRow.style.display = checked && isBadgePreset(checked.value) ? '' : 'none';
                var note = picker.querySelector('.tl-badge-custom-note'), optsIn = picker.querySelector('.tl-badge-opts');
                if (note && optsIn) note.style.display = tile && optsIn.value && optsIn.dataset.optkind === 'top10' ? '' : 'none';
            }
            picker.addEventListener('change', sync);
            sync();
            refreshBadgeTileIcons(picker, container);
            btn.addEventListener('click', function (e) {
                e.preventDefault(); e.stopPropagation();
                var checked = picker.querySelector('input[name^="tlBadgeStyle"]:checked');
                if (!checked || checked.value !== 'top10') return;
                openBadgeCustomise(picker, 'top10', container, null);
            });
        });
    }

    function readBadgeStyle(container) {
        var checked = container.querySelector('input[name^="tlBadgeStyle"]:checked');
        return checked ? checked.value : 'neutral';
    }

    // Options for the Top 10 tile (the ones it was customised with), or a colour preset's Top 3 medal colours.
    function readBadgeOptions(container) {
        var checked = container.querySelector('input[name^="tlBadgeStyle"]:checked');
        if (checked && isBadgePreset(checked.value)) {
            var medals = container.querySelector('.tl-badge-medals');
            return medals && medals.checked ? BADGE_MEDALS_JSON : '';
        }
        if (!checked || checked.value !== 'top10') return '';
        var input = container.querySelector('.tl-badge-opts');
        if (!input || input.dataset.optkind !== 'top10') return '';
        return input.value || '';
    }

    // ── Badge Customise popup ────────────────────────────────────────────────────────
    // The same popup as the art Customise (same overlay, OK / Cancel / Reset); the live preview
    // is one card drawn by the server (rank 7 on a stand-in poster). Fields that do not apply to the chosen type
    // stay visible with a short note.
    var NUM_COLOURS = [['#ffffff', 'White'], ['#ececf0', 'Light grey'], ['#141414', 'Page dark'], ['#f5c518', 'Gold'], ['#e50914', 'Red'], ['#52b54b', 'Emby green'], ['#7fd4ff', 'Ice blue'], ['#000000', 'Black']];
    var OUT_COLOURS = [['#9696a0', 'Grey'], ['#000000', 'Black'], ['#ffffff', 'White'], ['#f5c518', 'Gold'], ['#e50914', 'Red'], ['#52b54b', 'Emby green']];
    var BADGE_FIELDS = ['font', 'num', 'numcolour', 'outcolour', 'outline', 'size', 'shape', 'colour', 'medal', 'shadow', 'pos', 'tilepos', 'tilebg', 'tilebgcolour', 'special', 'move', 'moveeq', 'weeks', 'plays', 'logo', 'logolabel', 'logosize',
                        'logopos', 'movepos', 'weekspos', 'playspos', 'embygap'];
    // The Top 10 tile's extras and the spots they can sit in (ntl = top left of the number area).
    var LAY_EXTRAS = [['logo', 'TOP 10 logo', 'TOP 10'], ['move', 'Rank movement', '\u25B22'], ['weeks', 'Weeks', 'wks'], ['plays', 'Plays', '\u25B6 plays']];
    var LAY_SPOTS = [['ntl', 'Number area'], ['tl', 'Top left'], ['tr', 'Top right'], ['bl', 'Bottom left'], ['bc', 'Bottom centre'], ['br', 'Bottom right']];
    function badgeDefaults(kind) {
        var tile = kind === 'top10';
        return { font: tile ? 'anton' : 'lemonmilk', num: tile ? 'outline' : 'filled', numcolour: tile ? '#141414' : '#ffffff', outcolour: tile ? '#9696a0' : '#000000',
                 outline: 'normal', size: 'm', shape: 'circle', colour: '', medal: '', shadow: false, pos: 'tl', tilepos: 'beside', tilebg: 'flat', tilebgcolour: '#16223a',
                 special: '', move: false, moveeq: false, weeks: false, plays: false, logo: false, logolabel: 'top10', logosize: 'm',
                 logopos: 'tr', movepos: 'ntl', weekspos: 'bl', playspos: 'bl', embygap: false };
    }
    // The number colour each style starts with (switching style moves an untouched colour along).
    function badgeNumDefault(kind, num) {
        if (kind === 'top10') return num === 'outline' ? '#141414' : num === 'gradient' ? '#52b54b' : '#ececf0';
        return num === 'gradient' ? '#52b54b' : '#ffffff';
    }
    // Font tiles for both Customise popups, grouped (General / Horror / …); each tile is the
    // server's sample of the font (its name drawn in the font). The list sits in the popup's
    // normal flow — no inner scroll box. defId is the font drawn when none is chosen: its tile is
    // marked "(default)" (and added if the list lacks it), so the default is always visible.
    function fontTilesHtml(fonts, selected, defId) {
        var groups = {}, order = [];
        if (defId && !fonts.some(function (f) { return f.Id === defId; }))
            fonts = [{ Id: defId, Name: defId.charAt(0).toUpperCase() + defId.slice(1), Group: 'General' }].concat(fonts);
        fonts.forEach(function (f) {
            var g = f.Group || 'General';
            if (!groups[g]) { groups[g] = []; order.push(g); }
            groups[g].push(f);
        });
        order.forEach(function (g) { groups[g].sort(byName); });
        return '<div class="artc-fonts">' + order.map(function (g) {
            return '<div class="artc-fontgroup">' + escapeHtml(g) + '</div>' + groups[g].map(function (f) {
                var isDef = f.Id === defId, nm = f.Name + (isDef ? ' (default)' : '');
                return '<button type="button" class="artc-font' + (selected === f.Id ? ' artc-on' : '') + '" data-value="' + f.Id + '" title="' + escapeHtml(nm) + '" aria-label="' + escapeHtml(nm) + '"' + (isDef ? ' style="flex-direction:column;"' : '') + '>'
                    + (f.Sample ? '<img src="' + f.Sample + '" alt="' + escapeHtml(f.Name) + '" />' : '<span class="artc-fontname">' + escapeHtml(f.Name) + '</span>')
                    + (isDef ? '<span class="artc-fontdef" style="font-size:0.72em; line-height:1.2; margin-top:2px;">(default)</span>' : '') + '</button>';
            }).join('');
        }).join('') + '</div>';
    }
    var _badgeInfoPromise = null;
    function getBadgeCustomiseInfo() {
        if (!_badgeInfoPromise)
            _badgeInfoPromise = window.ApiClient.getJSON(window.ApiClient.getUrl('HomeScreenCompanion/BadgeCustomiseInfo'))
                .catch(function (e) { _badgeInfoPromise = null; throw e; });
        return _badgeInfoPromise;
    }

    function openBadgeCustomise(picker, kind, container, onCancel) {
        var view = picker.closest('#HomeScreenCompanionConfigPage') || activeView();
        var overlay = view ? bodyOverlay(view, 'artCustomiseModalOverlay') : document.querySelector('#artCustomiseModalOverlay');
        if (!overlay) return;
        var tile = kind === 'top10';
        var optsInput = picker.querySelector('.tl-badge-opts');
        var presetInput = picker.querySelector('.tl-badge-preset');
        var body = overlay.querySelector('.artc-body');
        overlay.querySelector('.artc-title').textContent = 'Customise ' + (tile ? 'Top 10 tile' : 'number badge');
        overlay.querySelector('.artc-subtitle').textContent = 'Top-list badge — ' + (tile ? 'Top 10 tile' : 'Number badge');
        body.innerHTML = '<div style="padding:20px 0; text-align:center; opacity:0.8;">Loading…</div>';
        overlay.style.zIndex = '20000';
        overlay._artcCancel = onCancel || null;
        overlay._artcOk = function () { overlay.classList.remove('modal-visible'); };   // until loaded: never the last popup's OK
        overlay._artcReset = function () { };
        overlay.classList.add('modal-visible');

        getBadgeCustomiseInfo().then(function (info) {
            var def = badgeDefaults(kind);
            // Show lists keep their extras clear of Emby's unplayed count by default (their cards always get one).
            var contentKind = picker.dataset.content === 'shows' ? 'shows' : 'movies';
            def.embygap = true;
            var laySel = null;   // the extra the Layout picker moves
            var stored = {};
            try { stored = optsInput.value ? JSON.parse(optsInput.value) : {}; } catch (e) { stored = {}; }
            if (optsInput.dataset.optkind && optsInput.dataset.optkind !== kind) stored = {};
            var state = Object.assign({}, def);
            BADGE_FIELDS.forEach(function (k) { if (stored[k] !== undefined && stored[k] !== null) state[k] = stored[k]; });
            // The badge colour: a preset id (kept in BadgeStyle) or a hex in the options.
            var preset = presetInput.value || 'neutral';
            state.colour = stored.colour || preset;
            if (stored.numcolour == null) state.numcolour = badgeNumDefault(kind, state.num);
            // Big numeral's own look for the number fields it was saved without.
            var BIG_KEYS = ['font', 'num', 'numcolour', 'outcolour'];
            var BIG_LOOK = { font: 'anton', num: 'outline', numcolour: '#000000', outcolour: '#ffffff' };
            if (!tile && state.shape === 'bigoutline') BIG_KEYS.forEach(function (k) { if (stored[k] == null) state[k] = BIG_LOOK[k]; });
            // The font drawn when none is chosen (Big numeral has its own); a saved font the server no longer has draws as that.
            function defFont() { return !tile && state.shape === 'bigoutline' ? BIG_LOOK.font : def.font; }
            if (!info.Fonts.some(function (f) { return f.Id === state.font; })) state.font = defFont();
            // One preview card, the image the list's Image Type uses (Thumb = landscape, else Primary).
            var imgSel = container && container.querySelector && container.querySelector('.tlm-image-type, .mtlImageType');
            var imageType = imgSel ? imgSel.value : '';
            var variant = imageType === 'Thumb' ? 'thumb' : 'primary';
            var previewBox = variant === 'thumb' ? 'artc-bg' : tile ? 'artc-card43' : 'artc-poster';
            var SHAPES = [['circle', 'Circle'], ['roundsq', 'Rounded square'], ['pill', 'Pill'], ['ribbon', 'Ribbon'], ['banner', 'Flag'], ['diag', 'Diagonal'], ['netflix', 'TOP 10 square'], ['numonly', 'Number only'], ['bigoutline', 'Big numeral'], ['hidden', 'None (extras only)']];
            var tileCache = _badgeTileCache, lastTile = {};
            var tileSeq = 0;
            render();

            // The stored options of a popup state (st; default = the popup's own): only changed fields.
            function toOptions(st) {
                st = st || state;
                var out = {}, any = false;
                BADGE_FIELDS.forEach(function (k) {
                    if (k === 'colour') { if (st.colour && st.colour[0] === '#') { out.colour = st.colour; any = true; } return; }
                    // Big numeral draws its own look (Anton, outline, white edge) for unset fields, so
                    // its number fields are always stored: the preview shows what is selected.
                    var big = !tile && st.shape === 'bigoutline' && BIG_KEYS.indexOf(k) >= 0;
                    if (!big && k === 'numcolour' && st.numcolour === badgeNumDefault(kind, st.num)) return;
                    if (k === 'moveeq' && !st.move) return;   // only with the movement chip on
                    if ((k === 'logolabel' || k === 'logosize') && !st.logo) return;   // only with the TOP 10 logo on
                    if (/^(logo|move|weeks|plays)pos$/.test(k) && !st[k.slice(0, -3)]) return;   // a spot only for an extra that is on
                    if (k === 'embygap' && !layOn(st).length) return;
                    if (!big && st[k] === def[k]) return;
                    out[k] = st[k]; any = true;
                });
                return any ? JSON.stringify(out) : '';
            }
            function previewStyle(st) { st = st || state; return tile ? 'top10' : (st.colour && st.colour[0] !== '#' ? st.colour : 'neutral'); }
            // The state after picking a shape: switching to / from Big numeral moves untouched number fields to that shape's look.
            function stateWithShape(v) {
                var st = Object.assign({}, state);
                if (!tile && (v === 'bigoutline') !== (st.shape === 'bigoutline')) {
                    var toBig = v === 'bigoutline';
                    var from = toBig ? normalLook() : BIG_LOOK, to = toBig ? BIG_LOOK : normalLook();
                    BIG_KEYS.forEach(function (k) { if (st[k] === from[k]) st[k] = to[k]; });
                    if (!toBig && st.numcolour === BIG_LOOK.numcolour) st.numcolour = badgeNumDefault(kind, st.num);
                }
                st.shape = v;
                return st;
            }
            // Shape tiles: each is the preview's drawing of stateWithShape(shape). Keyed by the look sent,
            // so a click on a shape (same looks) is answered from the cache.
            function tileLook(shape) { var st = stateWithShape(shape); return { style: previewStyle(st), options: toOptions(st) }; }
            function tileKey(shape) { var l = tileLook(shape); return variant + '|' + contentKind + '|' + l.style + '|' + l.options; }
            function refreshTiles() {
                if (tile) return;
                var wanted = SHAPES.map(function (o) { return { shape: o[0], key: tileKey(o[0]), look: tileLook(o[0]) }; });
                // A hex colour typed in changes the looks without a redraw of the popup: re-key the tiles.
                wanted.forEach(function (w) { var img = body.querySelector('.artc-shape[data-value="' + w.shape + '"] img'); if (img) img.dataset.key = w.key; });
                function show() {
                    wanted.forEach(function (w) {
                        var img = body.querySelector('.artc-shape[data-value="' + w.shape + '"] img');
                        var src = tileCache[w.key];
                        if (src) lastTile[w.shape] = src;
                        if (img && src && img.dataset.key === w.key && img.getAttribute('src') !== src) img.src = src;
                    });
                }
                var missing = wanted.filter(function (w) { return !tileCache[w.key]; });
                if (!missing.length) { show(); return; }
                // One request per preview style (a hex colour vs a preset changes it per tile).
                var byStyle = {};
                missing.forEach(function (w) { (byStyle[w.look.style] = byStyle[w.look.style] || []).push(w); });
                var mine = ++tileSeq, t0 = performance.now();
                Promise.all(Object.keys(byStyle).map(function (st) {
                    var list = byStyle[st];
                    return fetch(window.ApiClient.getUrl('HomeScreenCompanion/BadgeShapeTiles'), {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json', 'X-MediaBrowser-Token': window.ApiClient.accessToken() },
                        body: JSON.stringify({ BadgeStyle: st, Variant: variant, Kind: contentKind, Options: list.map(function (w) { return w.look.options; }) })
                    }).then(function (r) { return r.json(); }).then(function (res) {
                        if (res.Success) list.forEach(function (w, n) { if (res.Images[n]) tileCache[w.key] = res.Images[n]; });
                    });
                })).then(function () {
                    window._hscTileMs = Math.round(performance.now() - t0);
                    if (mine === tileSeq) show();
                }).catch(function () { });
            }

            // ── Layout: the extras that are on, each in a spot; one extra per spot (weeks and plays
            // share the bottom left, stacked, until one is moved — the tile's original look).
            function layOn(st) { st = st || state; return LAY_EXTRAS.filter(function (e) { return !!st[e[0]]; }).map(function (e) { return e[0]; }); }
            function layAt(spot) { return layOn().filter(function (e) { return state[e + 'pos'] === spot; }); }
            function layFree() { var f = LAY_SPOTS.filter(function (p) { return !layAt(p[0]).length; })[0]; return f ? f[0] : 'ntl'; }
            // Puts extra x in spot; what was there moves to x's old spot (or a free one when that is still taken).
            function layPlace(x, spot) {
                var old = state[x + 'pos'];
                if (old === spot) return;
                var there = layAt(spot).filter(function (e) { return e !== x; });
                state[x + 'pos'] = spot;
                there.forEach(function (e, i) { state[e + 'pos'] = i === 0 && !layAt(old).length ? old : layFree(); });
            }
            // An extra switched on goes to its spot, or a free one when another extra has it.
            function layTake(x) {
                var there = layAt(state[x + 'pos']).filter(function (e) { return e !== x; });
                var lines = x === 'weeks' || x === 'plays';
                if (there.length && !(lines && there.length === 1 && (there[0] === 'weeks' || there[0] === 'plays'))) state[x + 'pos'] = layFree();
            }
            function layoutHtml() {
                var on = layOn();
                if (on.indexOf(laySel) < 0) laySel = on[0] || null;
                var land = variant === 'thumb', W = 240, H = land ? 135 : 180;
                // The poster card's place on the tile (the renderer's: Primary 4:3 crop, Thumb 16:9).
                var c = land ? { x: W * 0.49, y: H * 0.076, w: W * 0.317, h: H * 0.847 } : { x: W * 0.503, y: H * 0.014, w: W * 0.486, h: H * 0.972 };
                var right = W - c.x - c.w, bottom = H - c.y - c.h;
                var embyR = right + 3, trRight = embyR + 40;   // Emby's chip in the card corner, the top-right spot beside it
                var at = { ntl: 'left:4px;top:4px;', tl: 'left:' + (c.x + 3) + 'px;top:' + (c.y + 3) + 'px;', tr: 'right:' + trRight + 'px;top:' + (c.y + 3) + 'px;',
                           bl: 'left:' + (c.x + 3) + 'px;bottom:' + (bottom + 3) + 'px;', bc: 'left:' + (c.x + c.w / 2) + 'px;bottom:' + (bottom + 3) + 'px;transform:translateX(-50%);',
                           br: 'right:' + (right + 3) + 'px;bottom:' + (bottom + 3) + 'px;' };
                var name = {}; LAY_EXTRAS.forEach(function (e) { name[e[0]] = e[2]; });
                var picker = '<div class="artc-seg" data-lay="1" style="margin-bottom:8px;">' + LAY_EXTRAS.filter(function (e) { return on.indexOf(e[0]) >= 0; }).map(function (e) {
                    return '<button type="button" class="artc-segbtn artc-layx' + (laySel === e[0] ? ' artc-on' : '') + '" data-value="' + e[0] + '">' + e[1] + '</button>';
                }).join('') + '</div>';
                var spots = LAY_SPOTS.map(function (p) {
                    var here = layAt(p[0]);
                    return '<button type="button" class="artc-layspot' + (here.length ? ' artc-full' : '') + (here.indexOf(laySel) >= 0 ? ' artc-on' : '') + '" data-value="' + p[0] + '" title="' + p[1] + '" aria-label="' + p[1] + '" style="' + at[p[0]] + '">' +
                        here.map(function (e) { return '<span>' + name[e] + '</span>'; }).join('') + '</button>';
                }).join('');
                var where = LAY_SPOTS.filter(function (p) { return laySel && p[0] === state[laySel + 'pos']; })[0];
                return picker + '<div class="artc-posrow"><div class="artc-lay" style="height:' + H + 'px;">' +
                    '<span class="artc-lay-num">7</span>' +
                    '<div class="artc-lay-card" style="left:' + c.x + 'px;top:' + c.y + 'px;width:' + c.w + 'px;height:' + c.h + 'px;"></div>' +
                    '<span class="artc-lay-emby" style="right:' + embyR + 'px;top:' + (c.y + 5) + 'px;">Emby</span>' + spots + '</div>' +
                    '<span class="artc-posname">' + (where ? escapeHtml(where[1]) : '') + '</span></div>';
            }

            function seg(f, opts) {
                return '<div class="artc-seg" data-field="' + f + '">' + opts.map(function (o) {
                    return '<button type="button" class="artc-segbtn' + (String(state[f]) === String(o[0]) ? ' artc-on' : '') + '" data-value="' + o[0] + '">' + o[1] + '</button>';
                }).join('') + '</div>';
            }
            // Longer option lists: an even grid of equal-width buttons (wraps cleanly).
            function segGrid(f, opts) {
                return '<div class="artc-seg artc-grid" data-field="' + f + '">' + opts.map(function (o) {
                    return '<button type="button" class="artc-segbtn' + (String(state[f]) === String(o[0]) ? ' artc-on' : '') + '" data-value="' + o[0] + '">' + o[1] + '</button>';
                }).join('') + '</div>';
            }
            function colours(f, list) {
                return '<div class="artc-colours" data-field="' + f + '">' + list.map(function (c) {
                    return '<button type="button" class="artc-swatch' + (state[f] === c[0] ? ' artc-on' : '') + '" data-value="' + c[0] + '" title="' + c[1] + '" style="background:' + (c[2] || c[0]) + ';"></button>';
                }).join('') + '<input type="text" class="artc-hex" data-field="' + f + '" maxlength="7" value="' + escapeHtml(state[f] && state[f][0] === '#' ? state[f] : '') + '" placeholder="#rrggbb" spellcheck="false" /></div>';
            }
            // Only fields that change the picture for this type (and shape) are shown.
            function shown(f) {
                var noNumber = !tile && state.shape === 'hidden';
                // The TOP 10 square draws its own white TOP / 10 / #rank (the real posters' square): no number look.
                var square = !tile && state.shape === 'netflix';
                var backed = ['circle', 'roundsq', 'pill', 'ribbon', 'banner', 'diag', 'netflix'].indexOf(state.shape) >= 0;
                switch (f) {
                    case 'special': return !noNumber;
                    case 'font': case 'num': case 'medal': return !noNumber && !square;
                    case 'numcolour': return !noNumber && !square && state.num !== 'metallic';
                    case 'outcolour': case 'outline': return !noNumber && !square && (state.num === 'outline' || (!tile && state.shape === 'bigoutline' && state.num === 'filled'));
                    case 'size': return !tile && !noNumber && state.shape !== 'bigoutline';
                    case 'shape': return !tile;
                    case 'colour': return !tile && backed;
                    case 'shadow': return tile || (!noNumber && state.shape !== 'numonly');
                    case 'pos': return !tile && (!noNumber || state.move || state.logo);
                    case 'tilepos': case 'tilebg': return tile;
                    case 'tilebgcolour': return tile && state.tilebg === 'solid';
                    case 'moveeq': return !!state.move;
                    case 'logolabel': case 'logosize': return !!state.logo;
                    case 'layout': case 'embygap': return tile && layOn().length > 0;
                    default: return true;
                }
            }
            function specialOpts() {
                var o = [['', 'None']];
                if (tile || state.shape !== 'bigoutline') o.push(['bigger', 'Bigger']);
                o.push(['crown', 'Crown']);
                if (tile || (state.shape !== 'numonly' && state.shape !== 'bigoutline')) o.push(['glow', 'Glow']);
                return o;
            }
            function section(title, inner) { return inner ? '<div class="artc-section">' + title + '</div>' + inner : ''; }
            function field(label, html, f, note) {
                if (!shown(f)) return '';
                return '<div class="artc-field" data-f="' + f + '"><div class="artc-label">' + label + '</div>' + html +
                    '<div class="artc-hint"' + (note ? '' : ' style="display:none;"') + '>' + escapeHtml(note || '') + '</div></div>';
            }
            function render() {
                var fonts = fontTilesHtml(info.Fonts, state.font, defFont());
                // Each tile is the preview's own drawing (rank 7, every current setting) with only the
                // shape changed: from the cache when known, else the tile's last picture until the new one comes.
                var shapes = '<div class="artc-shapes' + (variant === 'thumb' ? ' artc-shapes-land' : '') + '" data-field="shape">' + SHAPES.map(function (o) {
                    var key = tileKey(o[0]), img = tileCache[key] || lastTile[o[0]];
                    return '<button type="button" class="artc-shape' + (state.shape === o[0] ? ' artc-on' : '') + '" data-value="' + o[0] + '" title="' + escapeHtml(o[1]) + '">' +
                        '<img data-key="' + escapeHtml(key) + '"' + (img ? ' src="' + img + '"' : '') + ' alt="" />' + '<span class="artc-shape-name">' + escapeHtml(o[1]) + '</span></button>';
                }).join('') + '</div>';
                // Mini poster with a dot at each place the number can go.
                var POS = POS_SPOTS;
                // Flag and diagonal sit on an edge: no bottom-centre spot.
                if (state.shape === 'banner' || state.shape === 'diag') POS = POS.filter(function (p) { return p[0] !== 'bc'; });
                // bc draws as bottom left there
                var posPicker = posPickerHtml(POS, POS.some(function (p) { return p[0] === state.pos; }) ? state.pos : 'bl');
                var badgeCols = BADGE_PRESETS.map(function (p) { return [p.val, p.label, p.bg]; });
                var shapeNote = ['ribbon', 'diag', 'banner'].indexOf(state.shape) >= 0 ? 'This shape uses the left or right edge (top or bottom for flag/diagonal).' : state.shape === 'netflix' ? 'Red unless you pick a hex colour.' : '';
                body.innerHTML =
                    '<div class="artc-wrap">' +
                      '<div class="artc-previewcol">' +
                        '<div class="artc-preview ' + previewBox + '"><img class="artc-img" alt="" /><div class="artc-busy">Drawing…</div></div>' +
                        '<div class="fieldDescription" style="margin-top:6px;">' + ('Drawn with a stand-in poster at rank 7 (' + (variant === 'thumb' ? 'Thumb, landscape' : 'Primary') + ', from the list\'s Image Type), so medal colours and the #1 special do not show here. Movement, weeks and plays show sample values.') + '</div>' +
                      '</div>' +
                      '<div class="artc-fieldscol">' +
                        section('Number',
                            field('Font', fonts, 'font') +
                            field('Style', seg('num', [['filled', 'Filled'], ['outline', 'Outline'], ['gradient', 'Gradient'], ['metallic', 'Metallic']]), 'num') +
                            field('Number colour', colours('numcolour', NUM_COLOURS), 'numcolour') +
                            field('Outline colour', colours('outcolour', OUT_COLOURS), 'outcolour') +
                            field('Outline', seg('outline', [['thin', 'Thin'], ['normal', 'Normal'], ['thick', 'Thick']]), 'outline') +
                            field('Size', seg('size', [['s', 'S'], ['m', 'M'], ['l', 'L'], ['xl', 'XL']]), 'size')) +
                        section('Shape', field('Shape', shapes, 'shape', shapeNote)) +
                        section('Colours',
                            field('Badge colour', colours('colour', state.shape === 'netflix' ? [[state.colour && state.colour[0] !== '#' ? state.colour : (presetInput.value || 'neutral'), 'TOP 10 red (default)', '#e50914']] : badgeCols), 'colour', state.shape === 'netflix' ? 'The TOP 10 square is red unless you type a hex colour.' : '') +
                            field('Medal colours for 1–3', seg('medal', [['', 'Off'], ['flat', 'Flat'], ['metal', 'Metallic']]), 'medal') +
                            field('Shadow', seg('shadow', [[false, 'Off'], [true, 'On']]), 'shadow')) +
                        section('Position',
                            field('Number badge', posPicker, 'pos') +
                            field('Top 10 tile', seg('tilepos', [['beside', 'Beside the poster'], ['behind', 'Tucked behind']]), 'tilepos')) +
                        section('Tile background',
                            field('Background', segGrid('tilebg', [['flat', 'Flat #141414'], ['solid', 'Solid colour'], ['blur', 'Blurred poster']]), 'tilebg') +
                            field('Solid colour', colours('tilebgcolour', [['#16223a', 'Navy'], ['#1e1e22', 'Charcoal'], ['#2a0a0a', 'Dark red'], ['#0f2a16', 'Dark green']]), 'tilebgcolour')) +
                        section('#1 special', field('Rank 1', seg('special', specialOpts()), 'special')) +
                        section('Extras',
                            field('Rank movement ▲▼ / NEW', seg('move', [[false, 'Off'], [true, 'On']]), 'move', 'Lists with a Popular rule compare with where each title stood one week ago; other lists compare with the previous run.') +
                            field('', '<label style="display:inline-flex; align-items:center; gap:6px; cursor:pointer; font-size:0.9em;"><input type="checkbox" class="artc-chk" data-field="moveeq"' + (state.moveeq ? ' checked' : '') + ' style="margin:0;" /><span>Show = for unchanged</span></label>', 'moveeq', 'Off: only ▲ / ▼ / NEW; a title whose rank did not change gets no chip.') +
                            field('Weeks in the list', seg('weeks', [[false, 'Off'], [true, 'On']]), 'weeks', '"N wks in list" at the bottom of the poster.') +
                            field('Plays', seg('plays', [[false, 'Off'], [true, 'On']]), 'plays', 'All users\' plays of the title (all time).') +
                            field('TOP 10 logo', seg('logo', [[false, 'Off'], [true, 'On']]), 'logo', tile ? 'Red TOP 10 logo in the corner of the poster card.' : 'Red TOP 10 corner logo; with shape "None" it is the only mark.') +
                            field('Logo label', seg('logolabel', [['top10', 'TOP 10'], ['top10rank', 'TOP 10 #3']]), 'logolabel', 'TOP 10 #3: the title\'s rank on a second line under the logo.') +
                            field('Logo size', seg('logosize', [['s', 'S'], ['m', 'M'], ['l', 'L']]), 'logosize')) +
                        section('Layout',
                            field('', layoutHtml(), 'layout')) +
                      '</div>' +
                    '</div>';
                wire();
                refreshPreview();
            }
            function set(f, v) {
                if (f === 'num') { if (state.numcolour === badgeNumDefault(kind, state.num)) state.numcolour = badgeNumDefault(kind, v); }
                if (f === 'shape') state = stateWithShape(v); else state[f] = v;
                if (v === true && LAY_EXTRAS.some(function (e) { return e[0] === f; })) { layTake(f); laySel = f; }
                render();
            }
            function normalLook() { return { font: def.font, num: def.num, numcolour: badgeNumDefault(kind, def.num), outcolour: def.outcolour }; }
            function wire() {
                body.querySelectorAll('.artc-seg').forEach(function (g) {
                    if (!g.dataset.field) return;   // the Layout picker's extras: wired below
                    g.querySelectorAll('.artc-segbtn').forEach(function (b) {
                        b.addEventListener('click', function () {
                            var v = b.dataset.value, f = g.dataset.field;
                            set(f, v === 'true' ? true : v === 'false' ? false : v);
                        });
                    });
                });
                body.querySelectorAll('.artc-chk').forEach(function (b) { b.addEventListener('change', function () { set(b.dataset.field, b.checked); }); });
                body.querySelectorAll('.artc-font').forEach(function (b) { b.addEventListener('click', function () { set('font', b.dataset.value); }); });
                body.querySelectorAll('.artc-shape').forEach(function (b) { b.addEventListener('click', function () { set('shape', b.dataset.value); }); });
                body.querySelectorAll('.artc-posdot').forEach(function (b) { b.addEventListener('click', function () { set('pos', b.dataset.value); }); });
                body.querySelectorAll('.artc-layx').forEach(function (b) { b.addEventListener('click', function () { laySel = b.dataset.value; render(); }); });
                body.querySelectorAll('.artc-layspot').forEach(function (b) {
                    b.addEventListener('click', function () {
                        var here = layAt(b.dataset.value);
                        if (!laySel) { if (here.length) { laySel = here[0]; render(); } return; }
                        layPlace(laySel, b.dataset.value);
                        render();
                    });
                });
                body.querySelectorAll('.artc-colours').forEach(function (g) {
                    var f = g.dataset.field;
                    g.querySelectorAll('.artc-swatch').forEach(function (b) { b.addEventListener('click', function () { set(f, b.dataset.value); }); });
                    var hex = g.querySelector('.artc-hex');
                    hex.addEventListener('input', function () {
                        var v = hex.value.trim().toLowerCase();
                        if (v && v[0] !== '#') v = '#' + v;
                        if (/^#[0-9a-f]{6}$/.test(v)) {
                            state[f] = v;
                            g.querySelectorAll('.artc-swatch').forEach(function (s) { s.classList.toggle('artc-on', s.dataset.value === v); });
                            refreshPreview();
                        }
                    });
                });
            }
            var timer = null, seq = 0;
            function refreshPreview() {
                clearTimeout(timer);
                var busy = body.querySelector('.artc-busy');
                if (busy) { busy.style.display = 'block'; busy.textContent = 'Drawing…'; }
                timer = setTimeout(function () {
                    var mine = ++seq;
                    fetch(window.ApiClient.getUrl('HomeScreenCompanion/BadgeCustomPreview'), {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json', 'X-MediaBrowser-Token': window.ApiClient.accessToken() },
                        body: JSON.stringify({ BadgeStyle: previewStyle(), Options: toOptions(), Variant: variant, Kind: contentKind })
                    }).then(function (r) { return r.json(); }).then(function (res) {
                        if (mine !== seq) return;   // a newer drawing is on its way
                        var img = body.querySelector('.artc-img'), b = body.querySelector('.artc-busy');
                        if (res.Success && img) { img.src = res.Image; if (b) b.style.display = 'none'; }
                        else if (b) b.textContent = res.Message || 'Preview failed.';
                    }).catch(function (e) {
                        if (mine !== seq) return;
                        var b = body.querySelector('.artc-busy'); if (b) b.textContent = 'Preview failed: ' + e.message;
                    });
                    refreshTiles();
                }, 300);
            }

            overlay._artcReset = function () { state = Object.assign({}, def); state.colour = 'neutral'; render(); };
            overlay._artcOk = function () {
                var val = toOptions();
                var newPreset = !tile && state.colour && state.colour[0] !== '#' ? state.colour : presetInput.value;
                var changed = optsInput.value !== val || presetInput.value !== newPreset || optsInput.dataset.optkind !== kind;
                optsInput.value = val;
                optsInput.dataset.optkind = kind;
                overlay._artcCancel = null;
                presetInput.value = newPreset || 'neutral';
                var note = picker.querySelector('.tl-badge-custom-note');
                if (note) note.style.display = val ? '' : 'none';
                refreshBadgeTileIcons(picker, container);
                if (changed) {
                    optsInput.dispatchEvent(new Event('change', { bubbles: true }));
                    setTimeout(checkFormState, 0);
                }
                overlay.classList.remove('modal-visible');
            };
        }).catch(function (e) {
            body.innerHTML = '<div style="padding:12px 0;">Could not load the options: ' + escapeHtml(e && e.message || String(e)) + '</div>';
            overlay._artcOk = function () { overlay.classList.remove('modal-visible'); };
            overlay._artcReset = function () { };
        });

        if (!overlay._artcWired) {
            overlay._artcWired = true;
            overlay.querySelector('.btnArtcCancel').addEventListener('click', function () {
                var undo = overlay._artcCancel; overlay._artcCancel = null;
                overlay.classList.remove('modal-visible');
                if (undo) undo();
            });
            overlay.querySelector('.btnArtcOk').addEventListener('click', function () { if (overlay._artcOk) overlay._artcOk(); });
            overlay.querySelector('.btnArtcReset').addEventListener('click', function () { if (overlay._artcReset) overlay._artcReset(); });
        }
    }

    // contentType: 'Movies' (default) or 'Shows'.
    // prefill: a pasted top-list (Paste top-list) – same fields as existingData plus notices, but the
    // dialog stays a Create dialog.
    function showTopListModal(tagName, displayName, onSuccess, existingData, contentType, prefill) {
        var isShows = contentType === 'Shows';
        var preset = existingData || prefill || null;
        function escAttr(s) { return String(s || '').replace(/&/g, '&amp;').replace(/"/g, '&quot;'); }
        function escHtml(s) { return String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); }

        var inputStyle = 'background:var(--plugin-input-bg);border:1px solid var(--plugin-input-border);border-radius:6px;padding:6px 10px;font-size:0.9em;color:var(--plugin-popup-color);width:100%;box-sizing:border-box;';
        var labelStyle = 'font-size:0.88em;font-weight:500;opacity:0.75;display:block;margin-bottom:5px;';
        var fieldStyle = 'margin-bottom:16px;';

        var modal = document.createElement('div');
        modal.style.cssText = 'position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.6);z-index:9999;display:flex;align-items:center;justify-content:center;';

        function renderBox(content) {
            modal.innerHTML =
                '<div style="background:var(--plugin-popup-bg,#2a2a2a);color:var(--plugin-popup-color,#e8e8e8);' +
                'border:1px solid var(--plugin-popup-border,rgba(255,255,255,0.12));border-radius:12px;box-shadow:var(--hsc-shadow-modal);overscroll-behavior:contain;' +
                'padding:28px;max-width:860px;width:95%;max-height:90vh;overflow-y:auto;">' +
                content + '</div>';
        }

        renderBox('<div style="padding:10px 0;display:flex;align-items:center;gap:10px;">Loading <span class="tc-dot-loader"><span></span><span></span><span></span></span></div>');
        document.body.appendChild(modal);

        getHseUsers().then(function (users) {
            var usersHtml = buildUserMultiSelectHtml(
                users,
                preset ? (preset.userIds || []) : [],
                'chkTlmUser'
            );


            var html =
                '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:22px;">' +
                '<h3 style="margin:0;font-size:1.05em;font-weight:600;">' + (existingData ? 'Edit' : 'Create') + (isShows ? ' TV Show' : '') + ' Top-List: ' + escHtml(displayName) + '</h3>' +
                '<button type="button" class="btnTlmClose hsc-x" aria-label="Close" title="Close" style="background:transparent;border:none;color:inherit;cursor:pointer;padding:2px;opacity:0.6;line-height:1;border-radius:8px;"><i class="md-icon">close</i></button>' +
                '</div>' +
                (prefill ? sourceNoticesHtml(prefill.notices) : '') +

                '<div style="' + fieldStyle + '">' +
                '<span style="' + labelStyle + '">Target Users</span>' +
                '<div class="tlm-user-list">' + usersHtml + '</div>' +
                '</div>' +

                '<div class="hsc-adv" data-adv-def="" style="' + fieldStyle + '">' +
                '<label style="' + labelStyle + '">Show this section' + ADV_BADGE + '</label>' +
                '<select is="emby-select" class="tlm-display-mode" style="width:100%;">' +
                '<option value="">Always</option>' +
                '<option value="tv">When TV Display Mode is on</option>' +
                '<option value="mobile,desktop">When TV Display Mode is off</option>' +
                '</select></div>' +

                '<div style="' + fieldStyle + '">' +
                '<label style="' + labelStyle + '">Custom Title</label>' +
                '<input type="text" class="tlm-custom-name" style="' + inputStyle + '" placeholder="' + escAttr(displayName) + '" />' +
                '</div>' +

                '<div style="' + fieldStyle + '">' + positionFieldHtml(preset ? preset.position : '', '', labelStyle, HSC_POSITION_DESC) + '</div>' +

                '<div style="' + fieldStyle + '">' +
                '<label style="' + labelStyle + '">Image Type</label>' +
                '<select is="emby-select" class="tlm-image-type" style="width:100%;">' +
                '<option value="">Auto</option>' +
                '<option value="Primary">Primary</option>' +
                '<option value="Thumb">Thumb</option>' +
                '</select></div>' +

                '<div class="hsc-adv" data-adv-def="0" style="' + fieldStyle + '">' +
                '<label style="' + labelStyle + '">Card Size' + ADV_BADGE + '</label>' +
                '<select is="emby-select" class="tlm-card-size" style="width:100%;">' + cardSizeOptionsHtml(preset ? preset.cardSizeOffset : '0') + '</select></div>' +

                buildBadgePickerHtml(preset ? preset.badgeStyle : 'neutral', preset ? preset.badgeOptions : '', preset ? preset.realPoster : '', isShows ? 'shows' : 'movies') +

                '<div style="' + fieldStyle + '">' +
                '<label style="' + labelStyle + '">Max items <span style="font-weight:400;text-transform:none;letter-spacing:0;opacity:0.7;">(0 = all)</span></label>' +
                '<input type="number" class="tlm-max-items" min="0" step="1" style="' + inputStyle + '" placeholder="0" />' +
                '</div>' +

                '<div class="tlm-error" style="color:var(--hsc-danger-text);font-size:0.85em;min-height:1.2em;margin-bottom:4px;"></div>' +
                '<div style="border-top:1px solid var(--line-color);padding-top:16px;display:flex;gap:10px;align-items:center;justify-content:flex-end;">' +
                '<button type="button" class="btnTlmCancel hsc-mbtn hsc-btn-secondary">Cancel</button>' +
                '<button type="button" class="btnTlmSave hsc-mbtn hsc-btn-primary">' +
                '<i class="md-icon" style="font-size:1em;vertical-align:middle;margin-right:6px;">check</i>Save and apply</button>' +
                '</div>';

            renderBox(html);
            initBadgePicker(modal, tagName);
            wireUserMultiSelect(modal);
            wirePositionField(modal);
            fillPositionRows(modal, ((preset && preset.userIds) || [])[0] || ((users || [])[0] || {}).Id, function () {
                var el = modal.querySelector('.tlm-custom-name');
                return (el && (el.value || el.placeholder)) || '';
            });

            if (preset) {
                if (preset.customName) modal.querySelector('.tlm-custom-name').value = preset.customName;
                if (preset.displayMode) modal.querySelector('.tlm-display-mode').value = preset.displayMode;
                if (preset.imageType) modal.querySelector('.tlm-image-type').value = preset.imageType;
                if (preset.maxItems && preset.maxItems !== '0') modal.querySelector('.tlm-max-items').value = preset.maxItems;
            }
            refreshAdvanced(modal);

            modal.querySelector('.btnTlmClose').addEventListener('click', function () { modal.remove(); });
            modal.querySelector('.btnTlmCancel').addEventListener('click', function () { modal.remove(); });
            modal.addEventListener('click', function (e) { if (e.target === modal) modal.remove(); });

            modal.querySelector('.btnTlmSave').addEventListener('click', function () {
                var saveBtn = this;
                var errEl = modal.querySelector('.tlm-error');
                errEl.textContent = '';

                var selectedUserIds = Array.from(modal.querySelectorAll('.chkTlmUser:checked')).map(function (c) { return c.value; });
                if (selectedUserIds.length === 0) {
                    errEl.textContent = 'Please select at least one target user.';
                    return;
                }

                var customName    = modal.querySelector('.tlm-custom-name').value.trim() || displayName;
                var displayMode   = modal.querySelector('.tlm-display-mode').value;
                var imageType     = modal.querySelector('.tlm-image-type').value;
                var cardSize      = normCardSize(modal.querySelector('.tlm-card-size').value);
                var badgeStyle    = readBadgeStyle(modal);
                var badgeOptions  = readBadgeOptions(modal);
                var realPoster    = readRealPoster(modal);
                var maxItems      = Math.max(0, parseInt(modal.querySelector('.tlm-max-items').value, 10) || 0);
                var tok = window.ApiClient.accessToken ? window.ApiClient.accessToken() : '';

                saveBtn.disabled = true;
                saveBtn.innerHTML = 'Preparing files <span class="tc-dot-loader"><span></span><span></span><span></span></span>';

                // Step 1: Create folder + write .strm files
                fetch(window.ApiClient.getUrl('HomeScreenCompanion/TopList/PrepareFolder'), {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json', 'X-Emby-Token': tok },
                    body: JSON.stringify({ TagName: tagName, MaxItems: maxItems, BadgeStyle: badgeStyle, BadgeOptions: badgeOptions, RealPosterBadge: realPoster, ContentType: isShows ? 'Shows' : 'Movies' })
                })
                .then(function (r) { return r.json(); })
                .then(function (prepareResult) {
                    if (!prepareResult.Success) throw new Error(prepareResult.Message || 'Failed to create folder.');
                    if (isShows && !prepareResult.FilesCreated) throw new Error('No shows with episode files carry this tag.');
                    executeTopListCreationSteps(
                        tagName, displayName, selectedUserIds, displayMode, customName, imageType, maxItems,
                        prepareResult, { saveBtn: saveBtn, errEl: errEl, modal: modal, badgeStyle: badgeStyle, badgeOptions: badgeOptions, realPoster: realPoster, cardSizeOffset: cardSize, position: readPositionValue(modal), contentType: isShows ? 'Shows' : 'Movies' }, onSuccess
                    );
                })
                .catch(function (err) {
                    saveBtn.disabled = false;
                    saveBtn.innerHTML = '<i class="md-icon" style="font-size:1em;vertical-align:middle;margin-right:6px;">check</i>Save and apply';
                    errEl.textContent = err.message || String(err);
                });
            });
        }).catch(function (err) {
            renderBox('<div style="color:var(--hsc-danger-text);padding:10px 0;">Failed to load: ' + (err.message || err) + '</div>' +
                '<div style="margin-top:16px;text-align:right;"><button type="button" class="btnTlmClose hsc-mbtn hsc-btn-secondary">Close</button></div>');
            modal.querySelector('.btnTlmClose').addEventListener('click', function () { modal.remove(); });
        });
    }

    // What a manual top-list can rank: movies or, for a show top-list, series (top-list copies left out).
    function loadTopListCandidates(isShows, tok) {
        return fetch(window.ApiClient.getUrl(isShows ? 'HomeScreenCompanion/TopList/AllSeries' : 'HomeScreenCompanion/TopList/AllMovies'), {
            headers: { 'X-MediaBrowser-Token': tok }
        }).then(function (r) { return r.json(); });
    }

    // existingData = { listName, customName, displayMode, imageType, userIds: [], movies: [{ItemId,ImdbId,Name,Year}] }
    // contentType: 'Movies' (default) or 'Shows' — for shows, "movies" below are series.
    // options.prefill: a pasted top-list (Paste top-list) – presets like existingData, still a Create dialog.
    function showManualTopListModal(onSuccess, existingData, contentType, options) {
        function escAttr(s) { return String(s || '').replace(/&/g, '&amp;').replace(/"/g, '&quot;'); }
        function escHtml(s) { return String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); }

        var isShows = contentType === 'Shows' || !!(existingData && existingData.contentType === 'Shows');
        var itemWord = isShows ? 'show' : 'movie';
        var isEdit = !!existingData;
        var preset = existingData || (options && options.prefill) || null;
        var inputStyle = 'background:var(--plugin-input-bg);border:1px solid var(--plugin-input-border);border-radius:6px;padding:6px 10px;font-size:0.9em;color:var(--plugin-popup-color);width:100%;box-sizing:border-box;';
        var labelStyle = 'font-size:0.88em;font-weight:500;opacity:0.75;display:block;margin-bottom:5px;';
        var fieldStyle = 'margin-bottom:14px;';
        var colHeaderStyle = 'font-size:0.75em;font-weight:600;text-transform:uppercase;letter-spacing:0.6px;opacity:0.7;padding-bottom:8px;margin-bottom:12px;border-bottom:1px solid var(--plugin-popup-border, var(--hsc-hairline));';

        var modal = document.createElement('div');
        modal.style.cssText = 'position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.6);z-index:9999;display:flex;align-items:center;justify-content:center;';
        modal.innerHTML =
            '<div style="background:var(--plugin-popup-bg,#2a2a2a);color:var(--plugin-popup-color,#e8e8e8);' +
            'border:1px solid var(--plugin-popup-border,rgba(255,255,255,0.12));border-radius:12px;box-shadow:var(--hsc-shadow-modal);overscroll-behavior:contain;' +
            'padding:28px;max-width:860px;width:95%;max-height:90vh;overflow-y:auto;">' +
            '<div style="padding:10px 0;display:flex;align-items:center;gap:10px;">Loading <span class="tc-dot-loader"><span></span><span></span><span></span></span></div>' +
            '</div>';
        document.body.appendChild(modal);

        function onEsc(e) { if (e.key === 'Escape') { modal.remove(); document.removeEventListener('keydown', onEsc); } }
        document.addEventListener('keydown', onEsc);
        modal.addEventListener('click', function (e) { if (e.target === modal) { modal.remove(); document.removeEventListener('keydown', onEsc); } });

        var tok = window.ApiClient.accessToken ? window.ApiClient.accessToken() : '';

        var loadCandidates = loadTopListCandidates(isShows, tok);

        Promise.all([
            loadCandidates,
            getHseUsers()
        ])
        .then(function (results) {
            var allMovies = (results[0].Movies || []);
            var users     = results[1];

            var presetUserIds = (preset && preset.userIds) || [];

            var usersHtml = buildUserMultiSelectHtml(users, presetUserIds, 'chkMtlUser');


            var presetName       = (preset && preset.listName)    || '';
            var presetCustomName = (preset && preset.customName)  || '';
            var presetDisplay    = (preset && preset.displayMode) || '';
            var presetImageType  = (preset && preset.imageType)   || '';
            var presetCardSize   = (preset && preset.cardSizeOffset) || '0';
            var presetBadgeStyle = (preset && preset.badgeStyle)  || 'neutral';
            var presetBadgeOptions = (preset && preset.badgeOptions) || '';
            var presetRealPoster = (preset && preset.realPoster) || '';

            var displayOptions = [
                { val: '',               label: 'Always' },
                { val: 'tv',             label: 'When TV Display Mode is on' },
                { val: 'mobile,desktop', label: 'When TV Display Mode is off' }
            ].map(function (o) {
                return '<option value="' + escAttr(o.val) + '"' + (o.val === presetDisplay ? ' selected' : '') + '>' + escHtml(o.label) + '</option>';
            }).join('');

            var imageOptions = [
                { val: '',        label: 'Auto' },
                { val: 'Primary', label: 'Primary' },
                { val: 'Thumb',   label: 'Thumb' }
            ].map(function (o) {
                return '<option value="' + escAttr(o.val) + '"' + (o.val === presetImageType ? ' selected' : '') + '>' + escHtml(o.label) + '</option>';
            }).join('');

            var titleText    = (isEdit ? 'Edit Manual ' : 'Create Manual ') + (isShows ? 'TV Show ' : '') + 'Top-List';
            var createBtnLabel = isEdit ? 'Save changes' : 'Create top-list';

            var innerBox = modal.querySelector('div');
            innerBox.innerHTML =
                // ── Header ──────────────────────────────────────────────────
                '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:20px;">' +
                '<h3 style="margin:0;font-size:1.05em;font-weight:600;">' + escHtml(titleText) + '</h3>' +
                '<button type="button" class="btnMtlClose hsc-x" aria-label="Close" title="Close" style="background:transparent;border:none;color:inherit;cursor:pointer;padding:2px;opacity:0.6;line-height:1;border-radius:8px;"><i class="md-icon">close</i></button>' +
                '</div>' +
                (options && options.prefill ? sourceNoticesHtml(options.prefill.notices) : '') +

                // ── Two columns ──────────────────────────────────────────────
                '<div style="display:flex;gap:0;align-items:stretch;">' +

                // LEFT column: home section settings
                '<div style="flex:1;min-width:0;padding-right:20px;border-right:1px solid var(--line-color);">' +
                '<div style="' + colHeaderStyle + '"><i class="md-icon" style="font-size:0.9em;vertical-align:middle;margin-right:5px;">home</i>Home Section Settings</div>' +

                '<div style="' + fieldStyle + '">' +
                '<label style="' + labelStyle + '">List Name</label>' +
                '<input type="text" class="mtlListName" style="' + inputStyle + '" placeholder="e.g. My Favorites"' + (isEdit ? ' readonly style="' + inputStyle + 'opacity:0.6;cursor:not-allowed;"' : '') + ' value="' + escAttr(presetName) + '" /></div>' +

                '<div style="' + fieldStyle + '">' +
                '<label style="' + labelStyle + '">Custom Title <span style="font-weight:400;text-transform:none;letter-spacing:0;opacity:0.7;">(shown on home screen)</span></label>' +
                '<input type="text" class="mtlCustomName" style="' + inputStyle + '" placeholder="Defaults to list name" value="' + escAttr(presetCustomName) + '" /></div>' +

                '<div style="' + fieldStyle + '"><span style="' + labelStyle + '">Target Users</span>' +
                '<div>' + usersHtml + '</div></div>' +

                '<div style="' + fieldStyle + '">' + positionFieldHtml(preset ? preset.position : '', '', labelStyle, HSC_POSITION_DESC) + '</div>' +

                '<div class="hsc-adv" data-adv-def="" style="' + fieldStyle + '">' +
                '<label style="' + labelStyle + '">Show this section' + ADV_BADGE + '</label>' +
                '<select is="emby-select" class="mtlDisplayMode" style="width:100%;">' + displayOptions + '</select></div>' +

                '<div style="' + fieldStyle + '">' +
                '<label style="' + labelStyle + '">Image Type</label>' +
                '<select is="emby-select" class="mtlImageType" style="width:100%;">' + imageOptions + '</select></div>' +
                '<div class="hsc-adv" data-adv-def="0" style="' + fieldStyle + '">' +
                '<label style="' + labelStyle + '">Card Size' + ADV_BADGE + '</label>' +
                '<select is="emby-select" class="mtlCardSize" style="width:100%;">' + cardSizeOptionsHtml(presetCardSize) + '</select></div>' +

                '</div>' +

                // RIGHT column: movie list
                '<div style="flex:1;min-width:0;padding-left:20px;">' +
                '<div style="' + colHeaderStyle + '"><i class="md-icon" style="font-size:0.9em;vertical-align:middle;margin-right:5px;">format_list_numbered</i>' + (isShows ? 'Show List' : 'Movie List') + '</div>' +

                '<div style="' + fieldStyle + '">' +
                '<label style="' + labelStyle + '">' + (isShows ? 'Add Show' : 'Add Movie') + '</label>' +
                '<div style="position:relative;">' +
                '<input type="text" class="mtlMovieSearch" autocomplete="off" style="' + inputStyle + '" placeholder="Type to search…" />' +
                '<div class="mtlSearchResults" style="display:none;position:absolute;top:100%;left:0;right:0;z-index:200;background:var(--plugin-popup-bg,#2a2a2a);border:1px solid var(--line-color);border-radius:6px;max-height:200px;overflow-y:auto;margin-top:2px;box-shadow:var(--hsc-shadow-float);"></div>' +
                '</div></div>' +

                '<div class="mtlSelectedList" style="max-height:500px;overflow-y:auto;border:1px solid var(--line-color);border-radius:6px;padding:4px 8px;min-height:60px;"></div>' +
                '</div>' +

                '</div>' +

                '<div style="border-top:1px solid var(--line-color);padding-top:14px;margin-top:4px;">' +
                buildBadgePickerHtml(presetBadgeStyle, presetBadgeOptions, presetRealPoster, isShows ? 'shows' : 'movies') +
                '</div>' +

                '<div class="mtl-error" style="color:var(--hsc-danger-text);font-size:0.85em;min-height:1.2em;margin-top:12px;margin-bottom:4px;"></div>' +
                '<div style="border-top:1px solid var(--line-color);padding-top:16px;margin-top:8px;display:flex;gap:10px;align-items:center;justify-content:flex-end;">' +
                '<button type="button" class="btnMtlCancel hsc-mbtn hsc-btn-secondary">Cancel</button>' +
                '<button type="button" class="btnMtlCreate hsc-mbtn hsc-btn-primary">' +
                '<i class="md-icon" style="font-size:1em;">playlist_add</i>' + escHtml(createBtnLabel) + '</button>' +
                '</div>';

            initBadgePicker(modal);
            wireUserMultiSelect(modal);
            wirePositionField(modal);
            refreshAdvanced(modal);
            fillPositionRows(modal, presetUserIds[0] || ((users || [])[0] || {}).Id, function () {
                return (modal.querySelector('.mtlCustomName') || {}).value || (modal.querySelector('.mtlListName') || {}).value || '';
            });

            // In edit mode, set list name field readonly again (the value= attr trick above may not
            // work if the readonly attr comes after a style attr — set it via JS to be safe)
            if (isEdit) {
                var nameInput = modal.querySelector('.mtlListName');
                nameInput.readOnly = true;
                nameInput.style.opacity = '0.6';
                nameInput.style.cursor  = 'not-allowed';
            }

            var selectedMovies = (preset && preset.movies) ? preset.movies.slice() : [];

            function renderSelectedList() {
                var listEl = modal.querySelector('.mtlSelectedList');
                if (selectedMovies.length === 0) {
                    listEl.innerHTML = '<div style="padding:8px 4px;opacity:0.5;font-size:0.9em;">No ' + itemWord + 's added yet.</div>';
                    return;
                }
                listEl.innerHTML = selectedMovies.map(function (m, idx) {
                    var label = escHtml(m.Name) + (m.Year ? ' (' + escHtml(String(m.Year)) + ')' : '');
                    var upDis  = idx === 0 ? ' disabled' : '';
                    var dnDis  = idx === selectedMovies.length - 1 ? ' disabled' : '';
                    return '<div style="display:flex;align-items:center;gap:5px;padding:5px 2px;border-bottom:1px solid rgba(128,128,128,0.15);">' +
                        '<span style="min-width:22px;font-size:0.8em;opacity:0.55;font-weight:600;text-align:right;">' + (idx + 1) + '.</span>' +
                        '<span style="flex:1;font-size:0.88em;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="' + escAttr(m.Name) + '">' + label + '</span>' +
                        '<button type="button" class="btnMtlUp" data-idx="' + idx + '"' + upDis + ' style="cursor:pointer;border:none;background:transparent;color:inherit;padding:2px 4px;opacity:0.7;font-size:0.9em;line-height:1;" title="Move up">▲</button>' +
                        '<button type="button" class="btnMtlDown" data-idx="' + idx + '"' + dnDis + ' style="cursor:pointer;border:none;background:transparent;color:inherit;padding:2px 4px;opacity:0.7;font-size:0.9em;line-height:1;" title="Move down">▼</button>' +
                        '<button type="button" class="btnMtlRemove" data-idx="' + idx + '" style="cursor:pointer;border:none;background:transparent;color:var(--hsc-danger-text);padding:2px 4px;font-size:0.9em;line-height:1;" title="Remove">✕</button>' +
                        '</div>';
                }).join('');
            }

            renderSelectedList();

            modal.querySelector('.btnMtlClose').addEventListener('click', function () { modal.remove(); document.removeEventListener('keydown', onEsc); });
            modal.querySelector('.btnMtlCancel').addEventListener('click', function () { modal.remove(); document.removeEventListener('keydown', onEsc); });

            var searchInput  = modal.querySelector('.mtlMovieSearch');
            var resultsBox   = modal.querySelector('.mtlSearchResults');

            function showSearchResults(q) {
                q = (q || '').trim().toLowerCase();
                if (q.length < 1) { resultsBox.style.display = 'none'; resultsBox.innerHTML = ''; return; }
                var hits = allMovies.filter(function (m) {
                    return m.Name.toLowerCase().indexOf(q) !== -1 ||
                           (m.Year && String(m.Year).indexOf(q) !== -1);
                }).slice(0, 20);
                if (hits.length === 0) { resultsBox.style.display = 'none'; return; }
                var alreadyIds = new Set(selectedMovies.map(function (m) { return m.ItemId; }));
                resultsBox.innerHTML = hits.map(function (m) {
                    var added = alreadyIds.has(m.ItemId);
                    var label = escHtml(m.Name) + (m.Year ? ' (' + m.Year + ')' : '');
                    return '<div class="mtlSearchResult" data-itemid="' + escAttr(m.ItemId) + '"' +
                        ' data-imdbid="' + escAttr(m.ImdbId) + '"' +
                        ' data-name="' + escAttr(m.Name) + '"' +
                        ' data-year="' + escAttr(String(m.Year || '')) + '"' +
                        ' style="padding:7px 12px;cursor:pointer;font-size:0.9em;border-bottom:1px solid rgba(128,128,128,0.12);' +
                        (added ? 'opacity:0.42;pointer-events:none;' : '') + '">' +
                        label + (added ? ' <span style="font-size:0.8em;">(already added)</span>' : '') + '</div>';
                }).join('');
                resultsBox.style.display = 'block';
            }

            searchInput.addEventListener('input', function () { showSearchResults(this.value); });
            searchInput.addEventListener('focus',  function () { showSearchResults(this.value); });

            resultsBox.addEventListener('mousedown', function (e) {
                // mousedown fires before blur so we can intercept before the field loses focus
                var row = e.target.closest('.mtlSearchResult');
                if (!row || !row.dataset.itemid) return;
                e.preventDefault(); // keep focus on searchInput
                if (selectedMovies.some(function (m) { return m.ItemId === row.dataset.itemid; })) return;
                selectedMovies.push({
                    ItemId: row.dataset.itemid,
                    ImdbId: row.dataset.imdbid || '',
                    Name:   row.dataset.name   || '',
                    Year:   row.dataset.year   ? parseInt(row.dataset.year, 10) : null
                });
                searchInput.value = '';
                resultsBox.style.display = 'none';
                renderSelectedList();
            });

            searchInput.addEventListener('blur', function () {
                setTimeout(function () { resultsBox.style.display = 'none'; }, 150);
            });

            modal.querySelector('.mtlSelectedList').addEventListener('click', function (e) {
                var btn = e.target.closest('button');
                if (!btn) return;
                var idx = parseInt(btn.dataset.idx, 10);
                if (isNaN(idx)) return;
                var tmp;
                if (btn.classList.contains('btnMtlUp') && idx > 0) {
                    tmp = selectedMovies[idx - 1];
                    selectedMovies[idx - 1] = selectedMovies[idx];
                    selectedMovies[idx] = tmp;
                } else if (btn.classList.contains('btnMtlDown') && idx < selectedMovies.length - 1) {
                    tmp = selectedMovies[idx + 1];
                    selectedMovies[idx + 1] = selectedMovies[idx];
                    selectedMovies[idx] = tmp;
                } else if (btn.classList.contains('btnMtlRemove')) {
                    selectedMovies.splice(idx, 1);
                }
                renderSelectedList();
            });

            modal.querySelector('.btnMtlCreate').addEventListener('click', function () {
                var createBtn  = this;
                var errEl      = modal.querySelector('.mtl-error');
                errEl.textContent = '';

                var listName        = (modal.querySelector('.mtlListName').value || '').trim();
                var customNameVal   = (modal.querySelector('.mtlCustomName').value || '').trim() || listName;
                var displayMode     = modal.querySelector('.mtlDisplayMode').value;
                var imageType       = modal.querySelector('.mtlImageType').value;
                var cardSize        = normCardSize(modal.querySelector('.mtlCardSize').value);
                var badgeStyle      = readBadgeStyle(modal);
                var badgeOptions    = readBadgeOptions(modal);
                var realPoster    = readRealPoster(modal);
                var selectedUserIds = Array.from(modal.querySelectorAll('.chkMtlUser:checked')).map(function (c) { return c.value; });

                if (!listName)                    { errEl.textContent = 'Please enter a name for the list.'; return; }
                if (selectedUserIds.length === 0) { errEl.textContent = 'Please select at least one target user.'; return; }
                if (selectedMovies.length === 0)  { errEl.textContent = 'Please add at least one ' + itemWord + '.'; return; }

                createBtn.disabled = true;
                createBtn.innerHTML = 'Preparing files <span class="tc-dot-loader"><span></span><span></span><span></span></span>';

                var tok2 = window.ApiClient.accessToken ? window.ApiClient.accessToken() : '';
                fetch(window.ApiClient.getUrl('HomeScreenCompanion/TopList/PrepareManualFolder'), {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json', 'X-Emby-Token': tok2 },
                    body: JSON.stringify({
                        ListName: listName,
                        BadgeStyle: badgeStyle, BadgeOptions: badgeOptions, RealPosterBadge: realPoster,
                        ContentType: isShows ? 'Shows' : 'Movies',
                        Items: selectedMovies.map(function (m) { return { ImdbId: m.ImdbId, ItemId: m.ItemId }; })
                    })
                })
                .then(function (r) { return r.json(); })
                .then(function (prepareResult) {
                    if (!prepareResult.Success) throw new Error(prepareResult.Message || 'Failed to create folder.');
                    if (isShows && !prepareResult.FilesCreated) throw new Error('None of the selected shows has episode files.');
                    executeTopListCreationSteps(
                        listName, customNameVal, selectedUserIds, displayMode, customNameVal, imageType, 0,
                        prepareResult, { saveBtn: createBtn, errEl: errEl, modal: modal, badgeStyle: badgeStyle, badgeOptions: badgeOptions, realPoster: realPoster, cardSizeOffset: cardSize, contentType: isShows ? 'Shows' : 'Movies' }, function () {
                            document.removeEventListener('keydown', onEsc);
                            if (typeof onSuccess === 'function') onSuccess();
                        }
                    );
                })
                .catch(function (err) {
                    createBtn.disabled = false;
                    createBtn.innerHTML = '<i class="md-icon" style="font-size:1em;">playlist_add</i>' + escHtml(createBtnLabel);
                    errEl.textContent = err.message || String(err);
                });
            });
        })
        .catch(function (err) {
            var innerBox = modal.querySelector('div');
            innerBox.innerHTML =
                '<div style="color:var(--hsc-danger-text);padding:10px 0;">Failed to load: ' + (err.message || String(err)).replace(/</g, '&lt;') + '</div>' +
                '<div style="margin-top:16px;text-align:right;">' +
                '<button type="button" class="btnMtlClose hsc-mbtn hsc-btn-secondary">Close</button>' +
                '</div>';
            modal.querySelector('.btnMtlClose').addEventListener('click', function () { modal.remove(); document.removeEventListener('keydown', onEsc); });
        });
    }

    // Shows the Tag & Collection tab with the source card open on one of its tabs.
    function openSourceRowTab(view, row, tabName) {
        if (view._hscShowPageTab) view._hscShowPageTab('TagCollection');
        var body = row.querySelector('.tag-body'), icon = row.querySelector('.expand-icon');
        if (body && body.style.display === 'none') {
            body.style.display = 'block';
            if (icon) icon.innerText = 'expand_less';
        }
        var tab = row.querySelector('.tag-tab[data-tab="' + tabName + '"]');
        if (tab) tab.click();
        setTimeout(function () { try { row.scrollIntoView({ behavior: 'smooth', block: 'start' }); } catch (e) { row.scrollIntoView(); } }, 50);
    }

    // A source top-list (Top-Lists tab): its source's Home Screen tab (Create as top-list); switchOff unticks it there
    // (removed on Save, like every source change).
    function openSourceTopListTab(view, tagName, switchOff) {
        var want = (tagName || '').trim().toLowerCase();
        var rows = Array.from(view.querySelectorAll('#tagListContainer .tag-row')).filter(function (r) {
            var tag = ((r.querySelector('.txtTagName') || {}).value || (r.querySelector('.txtEntryLabel') || {}).value || '').trim().toLowerCase();
            return r.querySelector('.selSourceType') && tag === want;
        });
        var row = rows.filter(function (r) { return (r.querySelector('.chkEnableTopList') || {}).checked; })[0] || rows[0];
        if (!row) { window.Dashboard.alert('No source with the tag "' + tagName + '" was found on this page.'); return; }
        openSourceRowTab(view, row, 'homescreen');
        if (switchOff) {
            var chk = row.querySelector('.chkEnableTopList');
            if (chk && chk.checked) {
                chk.checked = false;
                chk.dispatchEvent(new Event('change', { bubbles: true }));
            }
            // Nothing else to show (Apply Tag went off with the top-list): no plain row either.
            var hseChk = row.querySelector('.chkEnableHomeSection');
            if (hseChk && hseChk.checked && !hseAllowed(row)) {
                hseChk.checked = false;
                hseChk.dispatchEvent(new Event('change', { bubbles: true }));
            }
            showToast('Top-list switched off in its source — press Save to remove it');
        }
    }

    function renderSourceTopListSummary(body, d) {
        function escAttr(s) { return String(s || '').replace(/&/g, '&amp;').replace(/"/g, '&quot;'); }
        function escHtml(s) { return String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); }
        var displayModeLabel = ({ '': 'Always', 'tv': 'TV mode only', 'mobile,desktop': 'Non-TV only' })[d.displayMode || ''] || 'Always';
        var line = function (k, v) { return '<div style="display:flex;gap:10px;padding:4px 0;font-size:0.9em;"><span style="min-width:120px;opacity:0.7;">' + k + '</span><span>' + v + '</span></div>'; };
        getHseUsers().catch(function () { return []; }).then(function (users) {
            var names = (d.userIds || []).map(function (id) {
                var u = (users || []).find(function (x) { return (x.Id || '').toLowerCase() === (id || '').toLowerCase(); });
                return u ? u.Name : null;
            }).filter(Boolean);
            body.innerHTML =
                '<div style="display:flex;justify-content:flex-end;margin-bottom:4px;">' +
                '<button type="button" is="emby-button" class="btnCopyTopList raised hsc-btn-ghost hsc-btn-sm" title="Copy this top-list to the clipboard, to paste it on another server"><i class="md-icon" style="font-size:1em;margin-right:4px;">share</i><span>Copy</span></button>' +
                '</div>' +
                '<div class="hsc-callout" style="margin-bottom:12px;"><i class="md-icon">info</i><span>Set in the <strong>Home Screen</strong> tab of the source <strong>' + escHtml(d.sourceName || d.tagName) + '</strong> (<em>Create as top-list</em>), with its users, title and position.</span></div>' +
                (d.noLibrary ? '<div class="hsc-callout warning" style="margin-bottom:12px;"><i class="md-icon">schedule</i><span>Not made yet: the next run of the source (or a full sync) makes its library and rows.</span></div>' : '') +
                line('Content', d.contentType === 'Shows' ? 'TV Shows' : 'Movies') +
                line('Max items', d.maxItems && d.maxItems !== '0' ? escHtml(d.maxItems) : 'All') +
                line('Users', names.length ? escHtml(names.join(', ')) : '<em style="opacity:0.6;">none</em>') +
                line('Show', displayModeLabel) +
                '<div style="display:flex;justify-content:flex-end;align-items:center;gap:8px;margin-top:16px;border-top:1px solid var(--line-color);padding-top:10px;flex-wrap:wrap;">' +
                '<button type="button" is="emby-button" class="raised btnTlOpenSource hsc-btn-secondary" data-name="' + escAttr(d.tagName) + '"><i class="md-icon" style="margin-right:5px;">open_in_new</i>Open source</button>' +
                '<button type="button" is="emby-button" class="raised button-submit btnRunEntry btnTlRun hsc-btn-activity" data-name="' + escAttr(d.tagName) + '"><i class="md-icon" style="margin-right:5px;">play_arrow</i><span class="btnRunEntryLabel">Run Group</span></button>' +
                '<button type="button" is="emby-button" class="raised btnTlSourceOff hsc-btn-danger" data-name="' + escAttr(d.tagName) + '" title="Switches Top-List off in the source; Save removes the list"><i class="md-icon" style="margin-right:5px;">delete</i>Delete top-list</button>' +
                '</div>';
        });
    }

    function loadInlineEditForm(row, body, onSuccess) {
        var editJson;
        try { editJson = JSON.parse(row.dataset.editjson || '{}'); } catch (e) { editJson = {}; }
        if (editJson.fromSource) { renderSourceTopListSummary(body, editJson); return; }

        var tagName     = editJson.tagName     || '';
        var isManual    = !!editJson.isManual;
        var displayName = editJson.displayName || editJson.customName || tagName;
        var isShows     = editJson.contentType === 'Shows';
        var contentType = isShows ? 'Shows' : 'Movies';
        var itemWord    = isShows ? 'show' : 'movie';

        function escAttr(s) { return String(s || '').replace(/&/g, '&amp;').replace(/"/g, '&quot;'); }
        function escHtml(s) { return String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); }

        var inputStyle = 'background:var(--plugin-input-bg);border:1px solid var(--plugin-input-border);border-radius:6px;padding:6px 10px;font-size:0.9em;color:var(--plugin-popup-color);width:100%;box-sizing:border-box;';
        var labelStyle = 'font-size:0.88em;font-weight:500;opacity:0.75;display:block;margin-bottom:5px;';
        var fieldStyle = 'margin-bottom:14px;';

        // Run Group: same button as on a source card (look, size, place next to Remove).
        var runHtml = '<button type="button" is="emby-button" class="raised button-submit btnRunEntry btnTlRun hsc-btn-activity" data-name="' + escAttr(tagName) + '"><i class="md-icon" style="margin-right:5px;">play_arrow</i><span class="btnRunEntryLabel">Run Group</span></button>';
        var deleteHtml = '<button type="button" is="emby-button" class="raised btnTlDelete hsc-btn-danger" data-name="' + escAttr(tagName) + '"><i class="md-icon" style="margin-right:5px;">delete</i>Delete top-list</button>';
        // Copy (same look and place as on a source card): the saved list, to paste on another server.
        var copyBarHtml = '<div style="display:flex;justify-content:flex-end;margin-bottom:4px;">' +
            '<button type="button" is="emby-button" class="btnCopyTopList raised hsc-btn-ghost hsc-btn-sm" title="Copy this top-list to the clipboard, to paste it on another server"><i class="md-icon" style="font-size:1em;margin-right:4px;">share</i><span>Copy</span></button>' +
            '</div>';

        body.innerHTML = '<div style="padding:8px 0;opacity:0.6;font-size:0.9em;">Loading… <span class="tc-dot-loader"><span></span><span></span><span></span></span></div>';

        var tok = window.ApiClient.accessToken ? window.ApiClient.accessToken() : '';

        if (isManual) {
            Promise.all([
                loadTopListCandidates(isShows, tok),
                getHseUsers(),
                fetch(window.ApiClient.getUrl('HomeScreenCompanion/TopList/ManualItems') + '?ListName=' + encodeURIComponent(tagName), {
                    headers: { 'X-MediaBrowser-Token': tok }
                }).then(function (r) { return r.json(); })
            ]).then(function (res) {
                var allMovies = res[0].Movies || [];
                var users     = res[1];
                var data      = res[2];

                if (!data.Success) {
                    body.innerHTML = '<div style="color:var(--hsc-danger-text);padding:8px 0;">Failed to load: ' + escHtml(data.Message || 'Unknown error') + '</div>';
                    return;
                }

                var presetUserIds    = data.UserIds      || editJson.userIds      || [];
                var presetDisplay    = data.DisplayMode  || editJson.displayMode  || '';
                var presetImageType  = data.ImageType    || editJson.imageType    || '';
                var presetCardSize   = data.CardSizeOffset != null ? String(data.CardSizeOffset) : (editJson.cardSizeOffset || '0');
                var presetBadgeStyle = data.BadgeStyle   || editJson.badgeStyle   || 'neutral';
                var presetBadgeOptions = data.BadgeOptions != null ? data.BadgeOptions : (editJson.badgeOptions || '');
                var presetRealPoster = data.RealPosterBadge != null ? data.RealPosterBadge : (editJson.realPoster || '');
                var presetCustomName = data.CustomName   || editJson.customName   || '';
                var presetMovies     = data.Movies       || [];

                var colHeaderStyle = 'font-size:0.75em;font-weight:600;text-transform:uppercase;letter-spacing:0.6px;opacity:0.7;padding-bottom:8px;margin-bottom:12px;border-bottom:1px solid var(--plugin-popup-border, var(--hsc-hairline));';

                var usersHtml = buildUserMultiSelectHtml(users, presetUserIds, 'chkMtlUser');

                var displayOptions = [
                    { val: '',               label: 'Always' },
                    { val: 'tv',             label: 'When TV Display Mode is on' },
                    { val: 'mobile,desktop', label: 'When TV Display Mode is off' }
                ].map(function (o) {
                    return '<option value="' + escAttr(o.val) + '"' + (o.val === presetDisplay ? ' selected' : '') + '>' + escHtml(o.label) + '</option>';
                }).join('');

                var imageOptions = [
                    { val: '',        label: 'Auto' },
                    { val: 'Primary', label: 'Primary' },
                    { val: 'Thumb',   label: 'Thumb' }
                ].map(function (o) {
                    return '<option value="' + escAttr(o.val) + '"' + (o.val === presetImageType ? ' selected' : '') + '>' + escHtml(o.label) + '</option>';
                }).join('');

                var wrapper = document.createElement('div');
                wrapper.innerHTML =
                    copyBarHtml +
                    '<div style="display:flex;gap:0;align-items:stretch;">' +

                    '<div style="flex:1;min-width:0;padding-right:20px;border-right:1px solid var(--line-color);">' +
                    '<div style="' + colHeaderStyle + '"><i class="md-icon" style="font-size:0.9em;vertical-align:middle;margin-right:5px;">home</i>Home Section Settings</div>' +

                    '<div style="' + fieldStyle + '">' +
                    '<label style="' + labelStyle + '">Custom Title <span style="font-weight:400;text-transform:none;letter-spacing:0;opacity:0.7;">(shown on home screen)</span></label>' +
                    '<input type="text" class="mtlCustomName" style="' + inputStyle + '" placeholder="Defaults to list name" value="' + escAttr(presetCustomName) + '" /></div>' +

                    '<div style="' + fieldStyle + '"><span style="' + labelStyle + '">Target Users</span>' +
                    '<div>' + usersHtml + '</div></div>' +

                    '<div class="hsc-adv" data-adv-def="" style="' + fieldStyle + '">' +
                    '<label style="' + labelStyle + '">Show this section' + ADV_BADGE + '</label>' +
                    '<select is="emby-select" class="mtlDisplayMode" style="width:100%;">' + displayOptions + '</select></div>' +

                    '<div style="' + fieldStyle + '">' +
                    '<label style="' + labelStyle + '">Image Type</label>' +
                    '<select is="emby-select" class="mtlImageType" style="width:100%;">' + imageOptions + '</select></div>' +
                '<div class="hsc-adv" data-adv-def="0" style="' + fieldStyle + '">' +
                '<label style="' + labelStyle + '">Card Size' + ADV_BADGE + '</label>' +
                '<select is="emby-select" class="mtlCardSize" style="width:100%;">' + cardSizeOptionsHtml(presetCardSize) + '</select></div>' +

                    '</div>' +

                    '<div style="flex:1;min-width:0;padding-left:20px;">' +
                    '<div style="' + colHeaderStyle + '"><i class="md-icon" style="font-size:0.9em;vertical-align:middle;margin-right:5px;">format_list_numbered</i>' + (isShows ? 'Show List' : 'Movie List') + '</div>' +

                    '<div style="' + fieldStyle + '">' +
                    '<label style="' + labelStyle + '">' + (isShows ? 'Add Show' : 'Add Movie') + '</label>' +
                    '<div style="position:relative;">' +
                    '<input type="text" class="mtlMovieSearch" autocomplete="off" style="' + inputStyle + '" placeholder="Type to search…" />' +
                    '<div class="mtlSearchResults" style="display:none;position:absolute;top:100%;left:0;right:0;z-index:200;background:var(--plugin-popup-bg,#2a2a2a);border:1px solid var(--line-color);border-radius:6px;max-height:200px;overflow-y:auto;margin-top:2px;box-shadow:var(--hsc-shadow-float);"></div>' +
                    '</div></div>' +

                    '<div class="mtlSelectedList" style="max-height:280px;overflow-y:auto;border:1px solid var(--line-color);border-radius:6px;padding:4px 8px;min-height:60px;"></div>' +
                    '</div>' +

                    '</div>' +

                    '<div style="border-top:1px solid var(--line-color);padding-top:14px;margin-top:4px;">' +
                    buildBadgePickerHtml(presetBadgeStyle, presetBadgeOptions, presetRealPoster, isShows ? 'shows' : 'movies') +
                    '</div>' +

                    '<div class="mtl-error" style="color:var(--hsc-danger-text);font-size:0.85em;min-height:1.2em;margin-top:12px;margin-bottom:4px;"></div>' +
                    '<div style="border-top:1px solid var(--line-color);padding-top:16px;margin-top:8px;display:flex;gap:10px;align-items:center;justify-content:flex-end;">' +
                    runHtml + deleteHtml +
                    '</div>';

                body.innerHTML = '';
                body.appendChild(wrapper);
                initBadgePicker(body, tagName);
                wireUserMultiSelect(wrapper);
                refreshAdvanced(wrapper);

                var selectedMovies = presetMovies.slice();
                var originalManualState;

                function renderSelectedList() {
                    var listEl = wrapper.querySelector('.mtlSelectedList');
                    if (selectedMovies.length === 0) {
                        listEl.innerHTML = '<div style="padding:8px 4px;opacity:0.5;font-size:0.9em;">No ' + itemWord + 's added yet.</div>';
                        updateManualDirty();
                        return;
                    }
                    listEl.innerHTML = selectedMovies.map(function (m, idx) {
                        var label = escHtml(m.Name) + (m.Year ? ' (' + escHtml(String(m.Year)) + ')' : '');
                        var upDis = idx === 0 ? ' disabled' : '';
                        var dnDis = idx === selectedMovies.length - 1 ? ' disabled' : '';
                        return '<div style="display:flex;align-items:center;gap:5px;padding:5px 2px;border-bottom:1px solid rgba(128,128,128,0.15);">' +
                            '<span style="min-width:22px;font-size:0.8em;opacity:0.55;font-weight:600;text-align:right;">' + (idx + 1) + '.</span>' +
                            '<span style="flex:1;font-size:0.88em;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="' + escAttr(m.Name) + '">' + label + '</span>' +
                            '<button type="button" class="btnMtlUp" data-idx="' + idx + '"' + upDis + ' style="cursor:pointer;border:none;background:transparent;color:inherit;padding:2px 4px;opacity:0.7;font-size:0.9em;line-height:1;" title="Move up">▲</button>' +
                            '<button type="button" class="btnMtlDown" data-idx="' + idx + '"' + dnDis + ' style="cursor:pointer;border:none;background:transparent;color:inherit;padding:2px 4px;opacity:0.7;font-size:0.9em;line-height:1;" title="Move down">▼</button>' +
                            '<button type="button" class="btnMtlRemove" data-idx="' + idx + '" style="cursor:pointer;border:none;background:transparent;color:var(--hsc-danger-text);padding:2px 4px;font-size:0.9em;line-height:1;" title="Remove">✕</button>' +
                            '</div>';
                    }).join('');
                    updateManualDirty();
                }

                renderSelectedList();
                originalManualState = getManualFormState();
                // After a save the saved values become the new baseline, so changing a field
                // back to its previous value counts as a change again.
                body.tlResetBaseline = function () { originalManualState = getManualFormState(); };

                var searchInput = wrapper.querySelector('.mtlMovieSearch');
                var resultsBox  = wrapper.querySelector('.mtlSearchResults');

                function showSearchResults(q) {
                    q = (q || '').trim().toLowerCase();
                    if (q.length < 1) { resultsBox.style.display = 'none'; resultsBox.innerHTML = ''; return; }
                    var alreadyIds = new Set(selectedMovies.map(function (m) { return m.ItemId; }));
                    var hits = allMovies.filter(function (m) {
                        return m.Name.toLowerCase().indexOf(q) !== -1 || (m.Year && String(m.Year).indexOf(q) !== -1);
                    }).slice(0, 20);
                    if (hits.length === 0) { resultsBox.style.display = 'none'; return; }
                    resultsBox.innerHTML = hits.map(function (m) {
                        var added = alreadyIds.has(m.ItemId);
                        var lbl = escHtml(m.Name) + (m.Year ? ' (' + m.Year + ')' : '');
                        return '<div class="mtlSearchResult" data-itemid="' + escAttr(m.ItemId) + '" data-imdbid="' + escAttr(m.ImdbId) + '" data-name="' + escAttr(m.Name) + '" data-year="' + escAttr(String(m.Year || '')) + '" style="padding:7px 12px;cursor:pointer;font-size:0.9em;border-bottom:1px solid rgba(128,128,128,0.12);' + (added ? 'opacity:0.42;pointer-events:none;' : '') + '">' + lbl + (added ? ' <span style="font-size:0.8em;">(already added)</span>' : '') + '</div>';
                    }).join('');
                    resultsBox.style.display = 'block';
                }

                searchInput.addEventListener('input', function () { showSearchResults(this.value); });
                searchInput.addEventListener('focus', function () { showSearchResults(this.value); });
                searchInput.addEventListener('blur',  function () { setTimeout(function () { resultsBox.style.display = 'none'; }, 150); });

                resultsBox.addEventListener('mousedown', function (e) {
                    var resultRow = e.target.closest('.mtlSearchResult');
                    if (!resultRow || !resultRow.dataset.itemid) return;
                    e.preventDefault();
                    if (selectedMovies.some(function (m) { return m.ItemId === resultRow.dataset.itemid; })) return;
                    selectedMovies.push({
                        ItemId: resultRow.dataset.itemid,
                        ImdbId: resultRow.dataset.imdbid || '',
                        Name:   resultRow.dataset.name   || '',
                        Year:   resultRow.dataset.year   ? parseInt(resultRow.dataset.year, 10) : null
                    });
                    searchInput.value = '';
                    resultsBox.style.display = 'none';
                    renderSelectedList();
                });

                wrapper.querySelector('.mtlSelectedList').addEventListener('click', function (e) {
                    var btn = e.target.closest('button');
                    if (!btn) return;
                    var idx = parseInt(btn.dataset.idx, 10);
                    if (isNaN(idx)) return;
                    var tmp;
                    if (btn.classList.contains('btnMtlUp') && idx > 0) {
                        tmp = selectedMovies[idx - 1]; selectedMovies[idx - 1] = selectedMovies[idx]; selectedMovies[idx] = tmp;
                    } else if (btn.classList.contains('btnMtlDown') && idx < selectedMovies.length - 1) {
                        tmp = selectedMovies[idx + 1]; selectedMovies[idx + 1] = selectedMovies[idx]; selectedMovies[idx] = tmp;
                    } else if (btn.classList.contains('btnMtlRemove')) {
                        selectedMovies.splice(idx, 1);
                    }
                    renderSelectedList();
                });

                function getManualFormState() {
                    return JSON.stringify({
                        customName: (wrapper.querySelector('.mtlCustomName').value || '').trim(),
                        displayMode: wrapper.querySelector('.mtlDisplayMode').value,
                        imageType: wrapper.querySelector('.mtlImageType').value,
                        cardSize: wrapper.querySelector('.mtlCardSize').value,
                        badgeStyle: readBadgeStyle(body),
                        badgeOptions: readBadgeOptions(body),
                        realPoster: readRealPoster(body),
                        userIds: Array.from(wrapper.querySelectorAll('.chkMtlUser:checked')).map(function (c) { return c.value; }).sort(),
                        movies: selectedMovies.map(function (m) { return m.ItemId; })
                    });
                }
                function updateManualDirty() {
                    if (!originalManualState) return;
                    body.dataset.dirty = getManualFormState() !== originalManualState ? '1' : '0';
                    checkFormState();
                }
                wrapper.querySelectorAll('input, select').forEach(function (el) {
                    el.addEventListener('change', updateManualDirty);
                });
                wrapper.querySelectorAll('input[type="text"], input[type="number"]').forEach(function (el) {
                    el.addEventListener('input', updateManualDirty);
                });

                body.tlSaveForm = function () {
                    return new Promise(function (resolve, reject) {
                        var customNameVal   = (wrapper.querySelector('.mtlCustomName').value || '').trim() || tagName;
                        var displayMode     = wrapper.querySelector('.mtlDisplayMode').value;
                        var imageType       = wrapper.querySelector('.mtlImageType').value;
                        var cardSize        = normCardSize(wrapper.querySelector('.mtlCardSize').value);
                        var badgeStyle      = readBadgeStyle(body);
                        var badgeOptions    = readBadgeOptions(body);
                        var realPoster    = readRealPoster(body);
                        var userIds         = Array.from(wrapper.querySelectorAll('.chkMtlUser:checked')).map(function (c) { return c.value; });

                        if (userIds.length === 0)      { reject(new Error('Please select at least one target user.')); return; }
                        if (selectedMovies.length === 0) { reject(new Error('Please add at least one ' + itemWord + '.')); return; }

                        var tok2 = window.ApiClient.accessToken ? window.ApiClient.accessToken() : '';
                        fetch(window.ApiClient.getUrl('HomeScreenCompanion/TopList/PrepareManualFolder'), {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json', 'X-Emby-Token': tok2 },
                            body: JSON.stringify({
                                ListName: tagName, BadgeStyle: badgeStyle, BadgeOptions: badgeOptions, RealPosterBadge: realPoster, ContentType: contentType,
                                Items: selectedMovies.map(function (m) { return { ImdbId: m.ImdbId, ItemId: m.ItemId }; })
                            })
                        })
                        .then(function (r) { return r.json(); })
                        .then(function (prepareResult) {
                            if (!prepareResult.Success) throw new Error(prepareResult.Message || 'Failed to prepare folder.');
                            var fakeBtn = { disabled: false, innerHTML: '' };
                            var errEl = wrapper.querySelector('.mtl-error');
                            executeTopListCreationSteps(
                                tagName, customNameVal, userIds, displayMode, customNameVal, imageType, 0,
                                prepareResult, { saveBtn: fakeBtn, errEl: errEl, modal: body, innerBox: wrapper, badgeStyle: badgeStyle, badgeOptions: badgeOptions, realPoster: realPoster, cardSizeOffset: cardSize, closeHandler: resolve, silent: true, contentType: contentType },
                                resolve
                            );
                        })
                        .catch(reject);
                    });
                };

            }).catch(function (err) {
                body.innerHTML = '<div style="color:var(--hsc-danger-text);padding:8px 0;">Failed to load: ' + escHtml(err.message || String(err)) + '</div>';
            });

        } else {
            getHseUsers().then(function (users) {
                var presetUserIds    = editJson.userIds     || [];
                var presetDisplay    = editJson.displayMode || '';
                var presetImageType  = editJson.imageType   || '';
                var presetCardSize   = editJson.cardSizeOffset || '0';
                var presetBadgeStyle = editJson.badgeStyle  || 'neutral';
                var presetBadgeOptions = editJson.badgeOptions || '';
                var presetRealPoster = editJson.realPoster || '';
                var presetCustomName = editJson.customName  || '';
                var presetMaxItems   = editJson.maxItems    || '0';

                var usersHtml = buildUserMultiSelectHtml(users, presetUserIds, 'chkTlmUser');

                var wrapper = document.createElement('div');
                wrapper.innerHTML =
                    copyBarHtml +
                    '<div style="' + fieldStyle + '">' +
                    '<span style="' + labelStyle + '">Target Users</span>' +
                    '<div>' + usersHtml + '</div>' +
                    '</div>' +

                    '<div class="hsc-adv" data-adv-def="" style="' + fieldStyle + '">' +
                    '<label style="' + labelStyle + '">Show this section' + ADV_BADGE + '</label>' +
                    '<select is="emby-select" class="tlm-display-mode" style="width:100%;">' +
                    '<option value="">Always</option>' +
                    '<option value="tv">When TV Display Mode is on</option>' +
                    '<option value="mobile,desktop">When TV Display Mode is off</option>' +
                    '</select></div>' +

                    '<div style="' + fieldStyle + '">' +
                    '<label style="' + labelStyle + '">Custom Title</label>' +
                    '<input type="text" class="tlm-custom-name" style="' + inputStyle + '" placeholder="' + escAttr(displayName) + '" />' +
                    '</div>' +

                    '<div style="' + fieldStyle + '">' +
                    '<label style="' + labelStyle + '">Image Type</label>' +
                    '<select is="emby-select" class="tlm-image-type" style="width:100%;">' +
                    '<option value="">Auto</option>' +
                    '<option value="Primary">Primary</option>' +
                    '<option value="Thumb">Thumb</option>' +
                    '</select></div>' +

                    '<div class="hsc-adv" data-adv-def="0" style="' + fieldStyle + '">' +
                    '<label style="' + labelStyle + '">Card Size' + ADV_BADGE + '</label>' +
                    '<select is="emby-select" class="tlm-card-size" style="width:100%;">' + cardSizeOptionsHtml(presetCardSize) + '</select></div>' +

                    buildBadgePickerHtml(presetBadgeStyle, presetBadgeOptions, presetRealPoster, isShows ? 'shows' : 'movies') +

                    '<div style="' + fieldStyle + '">' +
                    '<label style="' + labelStyle + '">Max items <span style="font-weight:400;text-transform:none;letter-spacing:0;opacity:0.7;">(0 = all)</span></label>' +
                    '<input type="number" class="tlm-max-items" min="0" step="1" style="' + inputStyle + '" placeholder="0" />' +
                    '</div>' +

                    '<div class="tlm-error" style="color:var(--hsc-danger-text);font-size:0.85em;min-height:1.2em;margin-bottom:4px;"></div>' +
                    '<div style="border-top:1px solid var(--line-color);padding-top:16px;display:flex;gap:10px;align-items:center;justify-content:flex-end;">' +
                    runHtml + deleteHtml +
                    '</div>';

                body.innerHTML = '';
                body.appendChild(wrapper);
                initBadgePicker(body, tagName);
                wireUserMultiSelect(wrapper);

                if (presetCustomName) wrapper.querySelector('.tlm-custom-name').value = presetCustomName;
                if (presetDisplay)    wrapper.querySelector('.tlm-display-mode').value = presetDisplay;
                if (presetImageType)  wrapper.querySelector('.tlm-image-type').value = presetImageType;
                if (presetMaxItems && presetMaxItems !== '0') wrapper.querySelector('.tlm-max-items').value = presetMaxItems;
                refreshAdvanced(wrapper);

                function getRegularFormState() {
                    return JSON.stringify({
                        customName: (wrapper.querySelector('.tlm-custom-name').value || '').trim(),
                        displayMode: wrapper.querySelector('.tlm-display-mode').value,
                        imageType: wrapper.querySelector('.tlm-image-type').value,
                        cardSize: wrapper.querySelector('.tlm-card-size').value,
                        badgeStyle: readBadgeStyle(body),
                        badgeOptions: readBadgeOptions(body),
                        realPoster: readRealPoster(body),
                        maxItems: wrapper.querySelector('.tlm-max-items').value,
                        userIds: Array.from(wrapper.querySelectorAll('.chkTlmUser:checked')).map(function (c) { return c.value; }).sort()
                    });
                }
                var originalRegularState = getRegularFormState();
                body.tlResetBaseline = function () { originalRegularState = getRegularFormState(); };
                function updateRegularDirty() {
                    body.dataset.dirty = getRegularFormState() !== originalRegularState ? '1' : '0';
                    checkFormState();
                }
                wrapper.querySelectorAll('input, select').forEach(function (el) {
                    el.addEventListener('change', updateRegularDirty);
                });
                wrapper.querySelectorAll('input[type="text"], input[type="number"]').forEach(function (el) {
                    el.addEventListener('input', updateRegularDirty);
                });

                body.tlSaveForm = function () {
                    return new Promise(function (resolve, reject) {
                        var userIds       = Array.from(wrapper.querySelectorAll('.chkTlmUser:checked')).map(function (c) { return c.value; });
                        if (userIds.length === 0) { reject(new Error('Please select at least one target user.')); return; }

                        var customNameVal = wrapper.querySelector('.tlm-custom-name').value.trim() || displayName;
                        var displayMode   = wrapper.querySelector('.tlm-display-mode').value;
                        var imageType     = wrapper.querySelector('.tlm-image-type').value;
                        var cardSize      = normCardSize(wrapper.querySelector('.tlm-card-size').value);
                        var badgeStyle    = readBadgeStyle(body);
                        var badgeOptions  = readBadgeOptions(body);
                        var realPoster    = readRealPoster(body);
                        var maxItems      = Math.max(0, parseInt(wrapper.querySelector('.tlm-max-items').value, 10) || 0);
                        var tok2 = window.ApiClient.accessToken ? window.ApiClient.accessToken() : '';

                        fetch(window.ApiClient.getUrl('HomeScreenCompanion/TopList/PrepareFolder'), {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json', 'X-Emby-Token': tok2 },
                            body: JSON.stringify({ TagName: tagName, MaxItems: maxItems, BadgeStyle: badgeStyle, BadgeOptions: badgeOptions, RealPosterBadge: realPoster, ContentType: contentType })
                        })
                        .then(function (r) { return r.json(); })
                        .then(function (prepareResult) {
                            if (!prepareResult.Success) throw new Error(prepareResult.Message || 'Failed to prepare folder.');
                            var fakeBtn = { disabled: false, innerHTML: '' };
                            var errEl = wrapper.querySelector('.tlm-error');
                            executeTopListCreationSteps(
                                tagName, customNameVal, userIds, displayMode, customNameVal, imageType, maxItems,
                                prepareResult, { saveBtn: fakeBtn, errEl: errEl, modal: body, innerBox: wrapper, badgeStyle: badgeStyle, badgeOptions: badgeOptions, realPoster: realPoster, cardSizeOffset: cardSize, closeHandler: resolve, silent: true, contentType: contentType },
                                resolve
                            );
                        })
                        .catch(reject);
                    });
                };

            }).catch(function (err) {
                body.innerHTML = '<div style="color:var(--hsc-danger-text);padding:8px 0;">Failed to load users: ' + escHtml(err.message || String(err)) + '</div>';
            });
        }
    }

    // existingTypes: sanitized list name (lower case) → 'Movies' | 'Shows'
    function showCreateTopListChooser(tagsData, existingTopLists, onSuccess, existingTypes) {
        existingTypes = existingTypes || {};
        function escAttr(s) { return String(s || '').replace(/&/g, '&amp;').replace(/"/g, '&quot;'); }
        function escHtml(s) { return String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); }
        function sanitizeName(name) {
            var safe = (name || 'unknown').replace(/[\\/:*?"<>|\x00-\x1f]/g, '_').replace(/^\.+|\.+$/g, '').trim();
            return safe.length === 0 ? 'unknown' : safe;
        }

        var modal = document.createElement('div');
        modal.style.cssText = 'position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.6);z-index:9999;display:flex;align-items:center;justify-content:center;';
        document.body.appendChild(modal);

        function onEsc(e) { if (e.key === 'Escape') { modal.remove(); document.removeEventListener('keydown', onEsc); } }
        document.addEventListener('keydown', onEsc);
        modal.addEventListener('click', function (e) { if (e.target === modal) { modal.remove(); document.removeEventListener('keydown', onEsc); } });

        var innerStyle = 'background:var(--plugin-popup-bg,#2a2a2a);color:var(--plugin-popup-color,#e8e8e8);' +
            'border:1px solid var(--plugin-popup-border,rgba(255,255,255,0.12));border-radius:12px;box-shadow:var(--hsc-shadow-modal);overscroll-behavior:contain;' +
            'padding:28px;max-width:480px;width:90%;max-height:85vh;overflow-y:auto;';

        var contentType = 'Movies';

        function renderStep1() {
            var isShows = contentType === 'Shows';
            var segBase = 'flex:1;cursor:pointer;border:none;border-radius:6px;padding:8px 12px;font-family:inherit;font-size:0.9em;font-weight:500;display:flex;align-items:center;justify-content:center;gap:6px;';
            var segOn  = segBase + 'background:var(--plugin-popup-bg2, var(--theme-background-level3));color:inherit;';
            var segOff = segBase + 'background:transparent;color:inherit;opacity:0.7;';
            modal.innerHTML =
                '<div style="' + innerStyle + '">' +
                '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:24px;">' +
                '<h3 style="margin:0;font-size:1.05em;font-weight:600;">Create Top-List</h3>' +
                '<button type="button" class="btnChooserClose hsc-x" aria-label="Close" title="Close" style="background:transparent;border:none;color:inherit;cursor:pointer;padding:2px;opacity:0.6;line-height:1;border-radius:8px;"><i class="md-icon">close</i></button>' +
                '</div>' +
                '<div style="display:flex;border:1px solid var(--plugin-popup-border,rgba(255,255,255,0.12));border-radius:8px;overflow:hidden;margin-bottom:20px;padding:2px;gap:2px;background:var(--hsc-tint-1);">' +
                '<button type="button" class="btnChooseType" data-type="Movies" style="' + (isShows ? segOff : segOn) + '"><i class="md-icon" style="font-size:1.1em;">movie</i>Movies</button>' +
                '<button type="button" class="btnChooseType" data-type="Shows" style="' + (isShows ? segOn : segOff) + '"><i class="md-icon" style="font-size:1.1em;">tv</i>TV Shows</button>' +
                '</div>' +
                '<p style="margin:0 0 20px;font-size:0.9em;color:var(--theme-text-secondary);">How do you want to create this top-list?</p>' +
                '<div style="display:flex;gap:16px;">' +
                '<button type="button" class="btnChooseManual" style="flex:1;cursor:pointer;background:var(--plugin-input-bg,rgba(255,255,255,0.05));border:1px solid var(--plugin-input-border,rgba(255,255,255,0.12));border-radius:8px;padding:20px 16px;text-align:left;color:inherit;">' +
                '<div style="font-size:1.4em;margin-bottom:10px;color:var(--hsc-primary-text);"><i class="md-icon">format_list_numbered</i></div>' +
                '<div style="font-weight:600;font-size:0.95em;margin-bottom:6px;">Manual</div>' +
                '<div style="font-size:0.82em;color:var(--theme-text-secondary);line-height:1.5;">Pick ' + (isShows ? 'shows' : 'movies') + ' manually and build a custom list</div>' +
                '</button>' +
                '<button type="button" class="btnChooseByTag" style="flex:1;cursor:pointer;background:var(--plugin-input-bg,rgba(255,255,255,0.05));border:1px solid var(--plugin-input-border,rgba(255,255,255,0.12));border-radius:8px;padding:20px 16px;text-align:left;color:inherit;">' +
                '<div style="font-size:1.4em;margin-bottom:10px;color:var(--hsc-primary-text);"><i class="md-icon">label</i></div>' +
                '<div style="font-weight:600;font-size:0.95em;margin-bottom:6px;">From a source</div>' +
                '<div style="font-size:0.82em;color:var(--theme-text-secondary);line-height:1.5;">Rank a source\'s titles: set in the source\'s Home Screen tab</div>' +
                '</button>' +
                '</div>' +
                // A top-list is a library of its own, and every user it is shown to gets access to it.
                '<div class="hsc-callout warning" style="margin-top:20px;">' +
                '<i class="md-icon">warning</i>' +
                '<span><strong>Library access is not checked per title.</strong> A top-list gets a library of its own, and every user you show it to gets access to that library. Its ' + (isShows ? 'shows' : 'movies') + ' can come from any library on the server, so those users can see and play them even from libraries they otherwise have no access to.</span>' +
                '</div>' +
                '</div>';

            modal.querySelector('.btnChooserClose').addEventListener('click', function () { modal.remove(); document.removeEventListener('keydown', onEsc); });

            modal.querySelectorAll('.btnChooseType').forEach(function (btn) {
                btn.addEventListener('click', function () { contentType = this.dataset.type; renderStep1(); });
            });

            var btnManualCard = modal.querySelector('.btnChooseManual');
            var btnByTagCard  = modal.querySelector('.btnChooseByTag');

            [btnManualCard, btnByTagCard].forEach(function (btn) {
                btn.addEventListener('mouseover', function () { this.style.borderColor = 'var(--hsc-primary)'; });
                btn.addEventListener('mouseout',  function () { this.style.borderColor = 'var(--plugin-input-border,rgba(255,255,255,0.12))'; });
            });

            btnManualCard.addEventListener('click', function () {
                modal.remove();
                document.removeEventListener('keydown', onEsc);
                showManualTopListModal(onSuccess, null, contentType);
            });

            btnByTagCard.addEventListener('click', function () { renderSourceStep(); });

        }

        // From a source: pick the source; its Home Screen tab opens (the list is set there and made on Save).
        function renderSourceStep() {
            var view = activeView();
            var rows = view ? Array.from(view.querySelectorAll('#tagListContainer .tag-row')).filter(function (r) { return r.querySelector('.selSourceType'); }) : [];
            var listHtml = rows.length === 0
                ? '<p style="padding:12px 4px;color:var(--theme-text-secondary);font-size:0.88em;font-style:italic;">No sources yet. Add one in the Tag &amp; Collection tab.</p>'
                : rows.map(function (r, i) {
                    var label = ((r.querySelector('.txtEntryLabel') || {}).value || '').trim();
                    var tag = ((r.querySelector('.txtTagName') || {}).value || '').trim() || label;
                    var on = !!(r.querySelector('.chkEnableTopList') || {}).checked;
                    return '<button type="button" class="btnSelectSource" data-idx="' + i + '" ' +
                        'style="width:100%;cursor:pointer;background:transparent;border:none;border-bottom:1px solid var(--line-color);' +
                        'padding:10px 4px;display:flex;align-items:center;color:inherit;text-align:left;">' +
                        '<span style="flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">' + escHtml(label || tag || 'New source') +
                        (on ? '<span class="tag-indicator toplist" style="margin-left:6px;font-size:0.72em;">top-list</span>' : '') + '</span>' +
                        (tag && tag !== label ? '<span style="margin-left:12px;white-space:nowrap;font-size:0.85em;color:var(--theme-text-secondary);">' + escHtml(tag) + '</span>' : '') +
                        '</button>';
                }).join('');
            modal.innerHTML =
                '<div style="' + innerStyle + 'max-width:520px;">' +
                '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;">' +
                '<div style="display:flex;align-items:center;gap:10px;">' +
                '<button type="button" class="btnChooserBack" style="background:transparent;border:none;color:var(--theme-text-secondary);cursor:pointer;padding:2px;line-height:1;opacity:0.7;"><i class="md-icon">arrow_back</i></button>' +
                '<h3 style="margin:0;font-size:1.05em;font-weight:600;">Select Source</h3>' +
                '</div>' +
                '<button type="button" class="btnChooserClose hsc-x" aria-label="Close" title="Close" style="background:transparent;border:none;color:inherit;cursor:pointer;padding:2px;opacity:0.6;line-height:1;border-radius:8px;"><i class="md-icon">close</i></button>' +
                '</div>' +
                '<p style="margin:0 0 12px;font-size:0.88em;color:var(--theme-text-secondary);line-height:1.5;">The source opens on its <strong>Home Screen</strong> tab. Tick <em>Add as home screen section</em> and <em>Create as top-list</em> there and press Save: the next run makes the list.</p>' +
                '<div style="max-height:50vh;overflow-y:auto;">' + listHtml + '</div>' +
                '</div>';
            modal.querySelector('.btnChooserClose').addEventListener('click', function () { modal.remove(); document.removeEventListener('keydown', onEsc); });
            modal.querySelector('.btnChooserBack').addEventListener('click', function () { renderStep1(); });
            modal.querySelectorAll('.btnSelectSource').forEach(function (btn) {
                btn.addEventListener('mouseover', function () { this.style.background = 'rgba(82,181,75,0.08)'; });
                btn.addEventListener('mouseout',  function () { this.style.background = 'transparent'; });
                btn.addEventListener('click', function () {
                    var r = rows[parseInt(this.dataset.idx, 10)];
                    modal.remove();
                    document.removeEventListener('keydown', onEsc);
                    if (r) openSourceRowTab(view, r, 'homescreen');
                });
            });
        }

        function renderStep2() {
            var isShows = contentType === 'Shows';
            // Show lists need a tag on shows (or their seasons/episodes).
            var tags = (tagsData.Tags || []).filter(function (t) { return !isShows || (t.SeriesCount || 0) > 0; });
            var inputStyle = 'background:var(--plugin-input-bg);border:1px solid var(--plugin-input-border);border-radius:6px;padding:6px 10px;font-size:0.9em;color:var(--plugin-popup-color);width:100%;box-sizing:border-box;';

            var tagRowsHtml = tags.length === 0
                ? '<p style="padding:12px 4px;color:var(--theme-text-secondary);font-size:0.88em;font-style:italic;">' + (isShows ? 'No tags on TV shows found.' : 'No tags found.') + '</p>'
                : tags.map(function (tag) {
                    var name  = tag.Name || '';
                    var count = isShows ? (tag.SeriesCount || 0)
                        : (tag.MovieCount != null ? tag.MovieCount : (tag.ItemCount != null ? tag.ItemCount : 0));
                    var countText = count + (isShows ? (count === 1 ? ' show' : ' shows') : (count === 1 ? ' movie' : ' movies'));
                    var hasTopList = existingTopLists.has(sanitizeName(name).toLowerCase());
                    var badge = hasTopList
                        ? '<span class="tag-indicator toplist" style="margin-left:6px;font-size:0.72em;">top-list</span>'
                        : '';
                    return '<button type="button" class="btnSelectTag" data-name="' + escAttr(name) + '" ' +
                        'style="width:100%;cursor:pointer;background:transparent;border:none;border-bottom:1px solid var(--line-color);' +
                        'padding:10px 4px;display:flex;align-items:center;color:inherit;text-align:left;">' +
                        '<span style="flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">' + escHtml(name) + badge + '</span>' +
                        '<span style="margin-left:12px;white-space:nowrap;font-size:0.85em;color:var(--theme-text-secondary);">' + countText + '</span>' +
                        '</button>';
                }).join('');

            modal.innerHTML =
                '<div style="' + innerStyle + 'max-width:520px;">' +
                '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;">' +
                '<div style="display:flex;align-items:center;gap:10px;">' +
                '<button type="button" class="btnChooserBack" style="background:transparent;border:none;color:var(--theme-text-secondary);cursor:pointer;padding:2px;line-height:1;opacity:0.7;"><i class="md-icon">arrow_back</i></button>' +
                '<h3 style="margin:0;font-size:1.05em;font-weight:600;">Select Tag</h3>' +
                '</div>' +
                '<button type="button" class="btnChooserClose hsc-x" aria-label="Close" title="Close" style="background:transparent;border:none;color:inherit;cursor:pointer;padding:2px;opacity:0.6;line-height:1;border-radius:8px;"><i class="md-icon">close</i></button>' +
                '</div>' +
                '<div style="margin-bottom:14px;">' +
                '<input type="text" id="tlChooserSearch" placeholder="Search tags…" style="' + inputStyle + '" />' +
                '</div>' +
                '<div id="tlChooserTagList" style="max-height:50vh;overflow-y:auto;">' +
                tagRowsHtml +
                '</div>' +
                '</div>';

            modal.querySelector('.btnChooserClose').addEventListener('click', function () { modal.remove(); document.removeEventListener('keydown', onEsc); });
            modal.querySelector('.btnChooserBack').addEventListener('click', function () { renderStep1(); });

            modal.querySelector('#tlChooserSearch').addEventListener('input', function () {
                var q = this.value.toLowerCase();
                modal.querySelectorAll('.btnSelectTag').forEach(function (btn) {
                    btn.style.display = (!q || (btn.dataset.name || '').toLowerCase().indexOf(q) !== -1) ? '' : 'none';
                });
            });

            modal.querySelectorAll('.btnSelectTag').forEach(function (btn) {
                btn.addEventListener('mouseover', function () { this.style.background = 'rgba(82,181,75,0.08)'; });
                btn.addEventListener('mouseout',  function () { this.style.background = 'transparent'; });
                btn.addEventListener('click', function () {
                    var tagName = this.dataset.name;
                    // One top-list per tag: its folder and settings are keyed by the tag name.
                    var otherType = existingTypes[sanitizeName(tagName).toLowerCase()];
                    if (otherType && otherType !== contentType) {
                        alert('This tag already has a ' + (otherType === 'Shows' ? 'TV show' : 'movie') + ' top-list. A tag can have one top-list — delete that one first to create a ' + (contentType === 'Shows' ? 'TV show' : 'movie') + ' list.');
                        return;
                    }
                    modal.remove();
                    document.removeEventListener('keydown', onEsc);
                    showTopListModal(tagName, tagName, onSuccess, null, contentType);
                });
            });
        }

        renderStep1();
    }

    // ── Backup & Restore ─────────────────────────────────────────────────────────────
    // Sections mirror the server's BackupFile. Only configuration is exported; everything the
    // sync tasks generate (tags, collections, playlists, top-list files, images) is rebuilt on run.
    var _backupSections = [
        { key: 'Settings',     label: 'General settings',          desc: 'AI models, system prompt, logging, dry run, preserve-on-empty.' },
        { key: 'ApiKeys',      label: 'API keys',                  desc: 'Trakt, MDBList, TMDB, OpenAI, Gemini, Claude, OpenAI-compatible. Stored in plain text in the file.' },
        { key: 'Tags',         label: 'Tag & collection groups',   desc: 'All source groups incl. schedules, blacklists, filters, collection settings, home sections and playlists.' },
        { key: 'SavedFilters', label: 'Saved media-info filters',  desc: 'Your saved filter presets.' },
        { key: 'TopLists',     label: 'Top lists',                 desc: 'Top-list settings and the movie and show lists of manual top-lists.' },
        { key: 'HomeSync',     label: 'Home screen sync',          desc: 'Copy & Sync profiles (source and target users, library order) and Continue Watching bump.' }
    ];

    function buildBackupModalShell() {
        var modal = document.createElement('div');
        modal.style.cssText = 'position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.6);z-index:9999;display:flex;align-items:center;justify-content:center;';
        modal.renderBox = function (content) {
            modal.innerHTML =
                '<div style="background:var(--plugin-popup-bg,#2a2a2a);color:var(--plugin-popup-color,#e8e8e8);' +
                'border:1px solid var(--plugin-popup-border,rgba(255,255,255,0.12));border-radius:12px;box-shadow:var(--hsc-shadow-modal);overscroll-behavior:contain;' +
                'padding:28px;max-width:560px;width:90%;max-height:85vh;overflow-y:auto;position:relative;">' +
                '<button type="button" class="btnShellClose" aria-label="Close" title="Close" style="position:absolute;top:22px;right:18px;background:transparent;border:none;color:inherit;cursor:pointer;padding:2px;opacity:0.6;line-height:1;"><i class="md-icon">close</i></button>' +
                content + '</div>';
            // Header X: same as the popup's Cancel/Close (modal.close).
            modal.querySelector('.btnShellClose').addEventListener('click', function () { modal.close(); });
        };
        function onEsc(e) { if (e.key === 'Escape') modal.close(); }
        modal.close = function () { modal.remove(); document.removeEventListener('keydown', onEsc); };
        document.addEventListener('keydown', onEsc);
        modal.addEventListener('click', function (e) { if (e.target === modal && !modal.dataset.busy) modal.close(); });
        document.body.appendChild(modal);
        return modal;
    }

    // available: null = every section selectable; otherwise a Set of section keys present in the file
    function buildBackupSectionsHtml(available, chkClass) {
        return _backupSections.map(function (s) {
            var present = !available || available.has(s.key);
            var rowStyle = 'display:flex;align-items:flex-start;gap:10px;padding:8px 0;border-bottom:1px solid var(--line-color,rgba(255,255,255,0.08));' + (present ? '' : 'opacity:0.45;');
            return '<label style="' + rowStyle + 'cursor:' + (present ? 'pointer' : 'default') + ';">' +
                '<input type="checkbox" class="' + chkClass + '" data-section="' + s.key + '"' + (present ? ' checked' : ' disabled') + ' style="margin-top:3px;" />' +
                '<span style="flex:1;">' +
                '<span style="display:block;font-weight:600;font-size:0.95em;">' + s.label + (present ? '' : ' <span style="font-weight:400;opacity:0.7;">(not in file)</span>') + '</span>' +
                '<span style="display:block;font-size:0.82em;opacity:0.65;margin-top:2px;">' + s.desc + '</span>' +
                '</span></label>';
        }).join('');
    }

    function readBackupSectionFlags(modal, chkClass) {
        var flags = {};
        modal.querySelectorAll('.' + chkClass).forEach(function (c) { flags[c.dataset.section] = !c.disabled && c.checked; });
        return flags;
    }

    var _backupTitleStyle = 'margin:0 0 6px;font-size:1.05em;font-weight:600;letter-spacing:-0.1px;';
    var _backupHintStyle = 'font-size:0.88em;opacity:0.7;margin:0 0 14px;line-height:1.45;';

    // ── Copy / Paste source (between servers) ──
    // A copy gets its own name, tag, collection and playlist ("… (copy)"), so it never writes
    // into (or removes from) the original's. Playlist links belong to the original's playlists.
    function markAsCopy(config) {
        config.Name = (config.Name || config.Tag || 'Source') + ' (copy)';
        if (config.Tag) config.Tag = config.Tag + ' (copy)';
        if (config.CollectionName) config.CollectionName = config.CollectionName + ' (copy)';
        if (config.PlaylistName) config.PlaylistName = config.PlaylistName + ' (copy)';
        config.PlaylistMappings = [];
        config.CollectionImported = false;   // a copy makes its own collection
        config.CollectionPosterReplaced = false; config.CollectionBackgroundReplaced = false;
        config.CollectionThumbReplaced = false; config.CollectionBannerReplaced = false;
        return config;
    }

    function postSourceJson(path, body) {
        var tok = window.ApiClient.accessToken ? window.ApiClient.accessToken() : '';
        return fetch(window.ApiClient.getUrl(path), {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'X-Emby-Token': tok },
            body: JSON.stringify(body)
        }).then(function (r) { if (!r.ok) throw new Error('Server returned ' + r.status); return r.json(); });
    }

    // navigator.clipboard only exists on https (and localhost); over plain http on the LAN a
    // selected textarea + execCommand('copy') still works.
    function writeClipboard(text) {
        function fallback() {
            var ta = document.createElement('textarea');
            ta.value = text;
            ta.setAttribute('readonly', '');
            ta.style.cssText = 'position:fixed;top:0;left:0;width:1px;height:1px;opacity:0;';
            document.body.appendChild(ta);
            ta.select();
            var ok = false;
            try { ok = document.execCommand('copy'); } catch (e) { ok = false; }
            document.body.removeChild(ta);
            return ok ? Promise.resolve() : Promise.reject(new Error('The browser blocked the copy.'));
        }
        if (navigator.clipboard && window.isSecureContext) return navigator.clipboard.writeText(text).catch(fallback);
        return fallback();
    }

    function sourceNoticesHtml(notices) {
        function escHtml(s) { return String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); }
        if (!notices || notices.length === 0) return '';
        return '<span class="hsc-eyebrow" style="display:block;margin-bottom:4px;color:var(--hsc-warning-text);">Notices</span>' +
            '<ul class="source-notices" style="margin:0 0 14px;padding-left:20px;font-size:0.9em;line-height:1.5;opacity:0.85;">' +
            notices.map(function (n) { return '<li>' + escHtml(n) + '</li>'; }).join('') + '</ul>';
    }

    // Box with a check mark, the notices and a Close button (same look as "Backup restored").
    function showSourceResult(modal, title, hint, notices) {
        modal = modal || buildBackupModalShell();
        modal.renderBox(
            '<div style="text-align:center;padding:4px 0 14px;">' +
            '<i class="md-icon" style="font-size:2.5em;color:var(--hsc-primary-text);display:block;margin-bottom:8px;">check_circle</i>' +
            '<p style="margin:0;font-size:1.05em;font-weight:500;">' + title + '</p>' +
            '</div>' + sourceNoticesHtml(notices) +
            (hint ? '<p style="' + _backupHintStyle + '">' + hint + '</p>' : '') +
            '<div style="display:flex;gap:10px;justify-content:flex-end;align-items:center;padding-top:14px;border-top:1px solid var(--line-color);">' +
            '<button type="button" class="btnSourceDone hsc-mbtn hsc-btn-primary">Close</button></div>'
        );
        modal.querySelector('.btnSourceDone').addEventListener('click', modal.close);
    }

    // Copy: the server exports the item with names for the server-specific ids (and uploaded
    // images inline); the page adds the kind's header and puts the text on the clipboard.
    var _copyKinds = {
        source: {
            header: 'hsc-source', versionKey: 'SourceVersion', label: 'Source',
            pasteHint: 'Paste it with <strong>Paste source</strong> (next to + Add New Source) on the other server.'
        },
        toplist: {
            header: 'hsc-toplist', versionKey: 'TopListVersion', label: 'Top-list',
            pasteHint: 'Paste it with <strong>Paste top-list</strong> (next to + Create New in the Top Lists tab) on the other server.',
            empty: function (file) { return !file.ListName; }
        }
    };

    function copyExportToClipboard(kind, path, body, btn, extraNotices) {
        if (btn) btn.disabled = true;
        return postSourceJson(path, body)
            .then(function (file) {
                var notices = (extraNotices || []).concat(file.Notices || []);
                if (kind.empty && kind.empty(file)) throw new Error(notices[0] || 'Nothing to copy.');
                var out = {};
                out[kind.header] = file[kind.versionKey] || 1;
                Object.keys(file).forEach(function (k) { if (k !== kind.versionKey && k !== 'Notices') out[k] = file[k]; });
                var text = JSON.stringify(out);
                return writeClipboard(text).then(function () {
                    showToast(kind.label + ' copied');
                    if (notices.length > 0) showSourceResult(null, kind.label + ' copied', kind.pasteHint, notices);
                }, function () { showCopyText(kind, text, notices); });
            })
            .catch(function (err) { window.Dashboard.alert('Copy failed: ' + (err.message || err)); })
            .finally(function () { if (btn) btn.disabled = false; });
    }

    // A source: the card as Save would write it (saved or not).
    function copySourceToClipboard(row) {
        var tags = rowToFlatTags(row, true);
        if (tags.length === 0) return;
        copyExportToClipboard(_copyKinds.source, 'HomeScreenCompanion/Source/Export', { Tags: tags }, row.querySelector('.btnCopySourceRow'));
    }

    // A top-list: the saved list (its settings live on the server, not in the card).
    function copyTopListToClipboard(row, btn) {
        var editJson = {};
        try { editJson = JSON.parse(row.dataset.editjson || '{}'); } catch (e) {}
        var body = row.querySelector('.tag-body');
        var extra = body && body.dataset.dirty === '1'
            ? ['This top-list has unsaved changes – the copy is the saved list. Click Save first to copy them too.'] : [];
        copyExportToClipboard(_copyKinds.toplist, 'HomeScreenCompanion/TopList/Export', { TagName: editJson.tagName || '' }, btn, extra);
    }

    // When the browser refuses the clipboard: show the text, selected, with its own Copy button.
    function showCopyText(kind, text, notices) {
        var modal = buildBackupModalShell();
        modal.renderBox(
            '<h3 style="' + _backupTitleStyle + '">Copy ' + kind.label + '</h3>' +
            '<p style="' + _backupHintStyle + '">The browser did not allow copying automatically. Click <strong>Copy</strong>, or press Ctrl+C (Cmd+C) – the text is selected.</p>' +
            '<textarea class="txtCopySource" readonly rows="8" style="width:100%;box-sizing:border-box;font-family:monospace;font-size:0.82em;background:transparent;color:inherit;border:1px solid var(--line-color,rgba(255,255,255,0.2));border-radius:6px;padding:8px;margin-bottom:12px;"></textarea>' +
            sourceNoticesHtml(notices) +
            '<div class="backup-error" style="color:var(--hsc-danger-text);font-size:0.85em;min-height:1.2em;margin-bottom:6px;"></div>' +
            '<div style="display:flex;gap:10px;justify-content:flex-end;align-items:center;">' +
            '<button type="button" class="btnCopySourceClose hsc-mbtn hsc-btn-secondary">Close</button>' +
            '<button type="button" class="btnCopySourceAgain hsc-mbtn hsc-btn-primary"><i class="md-icon" style="font-size:1em;vertical-align:middle;margin-right:6px;">share</i>Copy</button>' +
            '</div>'
        );
        var ta = modal.querySelector('.txtCopySource');
        ta.value = text;
        ta.focus();
        ta.select();
        modal.querySelector('.btnCopySourceClose').addEventListener('click', modal.close);
        modal.querySelector('.btnCopySourceAgain').addEventListener('click', function () {
            writeClipboard(text).then(function () { showToast(kind.label + ' copied'); modal.close(); }, function () {
                ta.focus(); ta.select();
                modal.querySelector('.backup-error').textContent = 'Still blocked – press Ctrl+C (Cmd+C) to copy the selected text.';
            });
        });
    }

    function sourceNameTaken(view, config) {
        var names = [config.Name, config.Tag].filter(function (n) { return n; }).map(function (n) { return n.trim().toLowerCase(); });
        return Array.from(view.querySelectorAll('#tagListContainer .tag-row')).some(function (r) {
            var label = ((r.querySelector('.txtEntryLabel') || {}).value || '').trim().toLowerCase();
            var tag = ((r.querySelector('.txtTagName') || {}).value || '').trim().toLowerCase();
            return names.some(function (n) { return n === label || n === tag; });
        });
    }

    // Paste shared by sources and top-lists. Called only from the Paste button's click: where the
    // browser allows it (https or localhost) the clipboard is read first, and if it holds a copy of
    // this kind it is imported straight away. Otherwise (no permission, plain http, other text) the
    // paste box opens; it checks the kind's header, sends the text to opts.path and hands the
    // server's mapped result to opts.onResult(result, modal) – modal is null for a direct paste.
    function parsePasteText(kind, opts, text) {
        var parsed = null;
        try { parsed = JSON.parse(text); } catch (e) { parsed = null; }
        if (!parsed || typeof parsed !== 'object') return { parsed: null, ok: false };
        return { parsed: parsed, ok: !!parsed[kind.header] && opts.isValid(parsed) };
    }

    function importPasteText(opts, text) {
        var body = {};
        body[opts.bodyKey] = text;
        return postSourceJson(opts.path, body).then(function (result) {
            if (!result || !result.Success) throw new Error((result && result.Message) || 'Unknown error');
            return result;
        });
    }

    function readClipboardForPaste() {
        try {
            if (!(window.isSecureContext && navigator.clipboard && navigator.clipboard.readText)) return Promise.resolve('');
            return Promise.resolve(navigator.clipboard.readText()).then(function (t) { return typeof t === 'string' ? t.trim() : ''; }, function () { return ''; });
        } catch (e) { return Promise.resolve(''); }
    }

    function showPasteModal(kind, opts) {
        readClipboardForPaste().then(function (clip) {
            var check = clip ? parsePasteText(kind, opts, clip) : { parsed: null, ok: false };
            if (check.ok) {
                importPasteText(opts, clip)
                    .then(function (result) { opts.onResult(result, null); })
                    .catch(function (err) { openPasteModal(kind, opts, { text: clip, error: 'Paste failed: ' + (err.message || err) }); });
                return;
            }
            var other = null;
            if (check.parsed) {
                Object.keys(_copyKinds).forEach(function (k) {
                    if (_copyKinds[k] !== kind && check.parsed[_copyKinds[k].header]) other = _copyKinds[k];
                });
            }
            openPasteModal(kind, opts, other ? {
                clipHint: 'The clipboard holds a copied ' + other.label.toLowerCase() + ' – use <strong>Paste ' + other.label.toLowerCase() + '</strong> for that.'
            } : null);
        });
    }

    function openPasteModal(kind, opts, state) {
        state = state || {};
        var modal = buildBackupModalShell();
        var applyHtml = '<i class="md-icon" style="font-size:1em;vertical-align:middle;margin-right:6px;">content_paste</i>' + opts.applyLabel;
        modal.renderBox(
            '<h3 style="' + _backupTitleStyle + '">Paste ' + kind.label + '</h3>' +
            '<p style="' + _backupHintStyle + '">' + opts.hint + '</p>' +
            (state.clipHint ? '<p class="paste-clip-hint" style="' + _backupHintStyle + 'color:var(--hsc-warning-text);">' + state.clipHint + '</p>' : '') +
            '<textarea class="txtPasteSource" rows="8" placeholder=\'{"' + kind.header + '":1, …}\' style="width:100%;box-sizing:border-box;font-family:monospace;font-size:0.82em;background:transparent;color:inherit;border:1px solid var(--line-color,rgba(255,255,255,0.2));border-radius:6px;padding:8px;margin-bottom:8px;"></textarea>' +
            '<div class="backup-error" style="color:var(--hsc-danger-text);font-size:0.85em;min-height:1.2em;margin-bottom:6px;"></div>' +
            '<div style="display:flex;gap:10px;justify-content:flex-end;align-items:center;">' +
            '<button type="button" class="btnPasteCancel hsc-mbtn hsc-btn-secondary">Cancel</button>' +
            '<button type="button" class="btnPasteApply hsc-mbtn hsc-btn-primary">' + applyHtml + '</button>' +
            '</div>'
        );
        var ta = modal.querySelector('.txtPasteSource');
        if (state.text) ta.value = state.text;
        if (state.error) modal.querySelector('.backup-error').textContent = state.error;
        ta.focus();
        modal.querySelector('.btnPasteCancel').addEventListener('click', modal.close);
        modal.querySelector('.btnPasteApply').addEventListener('click', function () {
            var btn = this;
            var errEl = modal.querySelector('.backup-error');
            var text = ta.value.trim();
            if (!parsePasteText(kind, opts, text).ok) {
                errEl.textContent = text ? opts.notMsg : 'Paste a copied ' + kind.label.toLowerCase() + ' first.';
                return;
            }
            errEl.textContent = '';
            btn.disabled = true;
            btn.innerHTML = 'Adding <span class="tc-dot-loader"><span></span><span></span><span></span></span>';
            modal.dataset.busy = '1';
            importPasteText(opts, text)
                .then(function (result) {
                    opts.onResult(result, modal);
                    delete modal.dataset.busy;
                })
                .catch(function (err) {
                    delete modal.dataset.busy;
                    btn.disabled = false;
                    btn.innerHTML = applyHtml;
                    errEl.textContent = 'Paste failed: ' + (err.message || err);
                });
        });
    }

    // Paste source: the server maps users, libraries, Manual List titles and images to this
    // server and returns the source; it becomes a new unsaved card at the top to check and save.
    function showPasteSourceModal(view) {
        showPasteModal(_copyKinds.source, {
            hint: 'Paste a source copied with <strong>Copy</strong> on a source card – from this server or another one. It is added as a new card at the top; check it and click <strong>Save</strong>. Users and libraries are matched by name, Manual List titles by IMDb/TMDb/TVDb id (then name and year); anything this server does not have is listed afterwards.',
            applyLabel: 'Add source',
            notMsg: 'This is not a copied source. Use Copy on a source card and paste that text here.',
            isValid: function (p) { return Array.isArray(p.Tags); },
            path: 'HomeScreenCompanion/Source/Import',
            bodyKey: 'SourceJson',
            onResult: function (result, modal) { addImportedSource(view, result, modal); }
        });
    }

    // Adds an imported source (Paste source, Quick Start starters) as a new unsaved card at the
    // top. prepare(config) may change it before the card is drawn. Returns the new card.
    function addImportedSource(view, result, modal, prepare) {
        var groups = groupConfigTags(result.Tags || []);
        var keys = Object.keys(groups);
        if (keys.length === 0) throw new Error('The pasted source is empty.');
        var config = groups[keys[0]];
        config.HomeSectionTracked = [];
        config.PlaylistMappings = [];
        config.LastModified = new Date().toISOString();
        if (prepare) prepare(config);
        if (sourceNameTaken(view, config)) markAsCopy(config);
        var container = view.querySelector('#tagListContainer');
        renderTagGroup(config, container, true, undefined, true);
        applyFilters(view);
        setTimeout(checkFormState, 0);
        showSourceResult(modal, 'Source added: ' + String(config.Name || config.Tag).replace(/&/g, '&amp;').replace(/</g, '&lt;'),
            'It is not saved yet – check the new card at the top and click <strong>Save</strong>.', result.Notices || []);
        return container.firstElementChild;
    }

    // Quick Start starter sources: built-in copied sources (no API key needed), added through the
    // same import as Paste source – so they work with the Copy & paste buttons switched off. Each
    // gets a home screen row (tag based) for all users; the users are filled in after the import.
    function starterHomeSettings(name, extra) {
        var s = { SectionType: 'items', ItemTypes: JSON.stringify(['Movie']), CustomName: name };
        Object.keys(extra || {}).forEach(function (k) { s[k] = extra[k]; });
        return JSON.stringify(s);
    }
    var QUICK_STARTERS = {
        popular: {
            Name: 'Popular on this server', Tag: 'Popular on this server', SourceType: 'MediaInfo', Limit: 20,
            MediaInfoFilters: [{ Operator: 'AND', GroupOperator: 'AND', Criteria: ['MediaType:Movie', 'Popular:30:>=:1'] }],
            HomeSectionSettings: starterHomeSettings('Popular on this server')
        },
        recent: {
            Name: 'Recently added movies', Tag: 'Recently added movies', SourceType: 'MediaInfo', Limit: 0,
            MediaInfoFilters: [{ Operator: 'AND', GroupOperator: 'AND', Criteria: ['MediaType:Movie', 'DateAdded:<=:30'] }],
            HomeSectionSettings: starterHomeSettings('Recently added movies', { SortBy: 'DateCreated,SortName', SortOrder: 'Descending' })
        },
        trending: {
            Name: 'Most watched this week', Tag: 'Most watched this week', SourceType: 'External', Limit: 0,
            Url: 'https://mdblist.com/lists/linaspurinis/top-watched-movies-of-the-week',
            HomeSectionSettings: starterHomeSettings('Most watched this week')
        }
    };

    function addStarterSource(view, key) {
        var starter = QUICK_STARTERS[key];
        if (!starter) return Promise.reject(new Error('Unknown starter source.'));
        var tag = { Active: true, EnableTag: true, EnableHomeSection: true, HomeSectionLibraryId: 'auto', HomeSectionUserIds: [] };
        Object.keys(starter).forEach(function (k) { tag[k] = starter[k]; });
        var text = JSON.stringify({ 'hsc-source': 1, Tags: [tag] });
        return Promise.all([importPasteText({ path: 'HomeScreenCompanion/Source/Import', bodyKey: 'SourceJson' }, text), getHseUsers()])
            .then(function (res) {
                var userIds = (res[1] || []).map(function (u) { return u.Id; });
                return addImportedSource(view, res[0], null, function (config) { config.HomeSectionUserIds = userIds; });
            });
    }

    // Import collection: existing Emby collections (made by another plugin or by hand) become Manual
    // List sources that take the collection over – the same collection item (HSC finds it by
    // name), its titles, description and sort title, and its images kept (Keep current image).
    // The cards are added unsaved at the top, like Paste source; nothing changes until Save and
    // the next sync. HSC never deletes an imported collection (TagConfig.CollectionImported). Collections a source already makes (saved, or a card on this page) and
    // names shared by two collections cannot be ticked.
    function showImportCollectionsModal(view) {
        function esc(s) { return String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;'); }
        var modal = buildBackupModalShell();
        modal.renderBox('<h3 style="' + _backupTitleStyle + '">Import collection</h3>' +
            '<p style="' + _backupHintStyle + '">Loading collections <span class="tc-dot-loader"><span></span><span></span><span></span></span></p>');

        // Collections made by a card on this page (saved or not): name → card name.
        var onPage = {};
        view.querySelectorAll('#tagListContainer .tag-row').forEach(function (r) {
            if (!(r.querySelector('.chkEnableCollection') || {}).checked) return;
            var label = ((r.querySelector('.txtEntryLabel') || {}).value || '').trim();
            var tag = ((r.querySelector('.txtTagName') || {}).value || '').trim() || label;
            var cn = ((r.querySelector('.txtCollectionName') || {}).value || '').trim() || tag;
            if (cn) onPage[cn.toLowerCase()] = label || tag;
        });

        window.ApiClient.getJSON(window.ApiClient.getUrl('HomeScreenCompanion/Collections/ImportList')).then(function (res) {
            var list = ((res && res.Collections) || []).slice().sort(byName);
            list.forEach(function (c) {
                c.reason = c.ManagedBy ? 'Made by the source “' + c.ManagedBy + '”'
                    : onPage[(c.Name || '').trim().toLowerCase()] ? 'Made by the card “' + onPage[(c.Name || '').trim().toLowerCase()] + '” on this page'
                    : c.DuplicateName ? 'Another collection has the same name – rename one in Emby first'
                    : '';
            });
            var rowsHtml = list.map(function (c, i) {
                var off = !!c.reason;
                return '<label class="imp-coll-row" data-name="' + esc((c.Name || '').toLowerCase()) + '" data-count="' + (c.ItemCount || 0) + '" style="display:flex;align-items:center;gap:10px;padding:7px 4px;border-bottom:1px solid var(--line-color,rgba(255,255,255,0.08));cursor:' + (off ? 'default' : 'pointer') + ';' + (off ? 'opacity:0.45;' : '') + '">' +
                    '<input type="checkbox" class="chkImpColl" data-idx="' + i + '"' + (off ? ' disabled' : '') + ' />' +
                    '<span style="flex:1;min-width:0;">' +
                        '<span style="display:block;font-size:0.93em;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">' + esc(c.Name) + '</span>' +
                        (off ? '<span style="display:block;font-size:0.78em;opacity:0.8;">' + esc(c.reason) + '</span>' : '') +
                    '</span>' +
                    '<span style="font-size:0.78em;opacity:0.6;white-space:nowrap;">' + c.ItemCount + ' item' + (c.ItemCount === 1 ? '' : 's') + '</span>' +
                    '</label>';
            }).join('');
            modal.renderBox(
                '<h3 style="' + _backupTitleStyle + '">Import collection</h3>' +
                '<p style="' + _backupHintStyle + '">Tick the collections HSC should take over. Each becomes a <strong>Manual List</strong> source that keeps the <strong>same collection</strong> (links, home rows and favourites keep working) with its name, sort title, description and current titles. Its poster and background are set to <strong>Keep current image</strong>. The new cards are added at the top, not saved yet – check them and click <strong>Save</strong>; HSC manages them from the next sync. Nothing is deleted – not now, and not later if you delete or switch off the source: an imported collection stays in Emby.</p>' +
                (list.length > 8 ? '<div style="display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin-bottom:8px;">' +
                    '<input type="search" class="txtImpCollSearch emby-input" placeholder="Search collections…" autocomplete="off" style="flex:1 1 200px;min-width:0;width:auto;box-sizing:border-box;padding:8px;font-size:0.9em;" />' +
                    '<div style="flex:0 0 auto;"><select is="emby-select" class="selImpCollSort" title="Sort"><option value="name">Name</option><option value="most">Most items</option><option value="fewest">Fewest items</option></select></div>' +
                    '<div style="flex:0 0 auto;"><select is="emby-select" class="selImpCollCount" title="Items"><option value="">All</option><option value="0-0">Empty (0)</option><option value="1-2">1–2 items</option><option value="3-10">3–10 items</option><option value="11-50">11–50 items</option><option value="51-">51+ items</option></select></div>' +
                    '</div>' : '') +
                '<div class="imp-coll-list" style="max-height:45vh;overflow-y:auto;border:1px solid var(--line-color,rgba(255,255,255,0.12));border-radius:6px;padding:0 8px;margin-bottom:10px;">' +
                    (rowsHtml || '<div style="padding:10px 4px;opacity:0.6;font-size:0.9em;">There are no collections in the library.</div>') +
                    '<div class="imp-coll-none" style="display:none;padding:10px 4px;opacity:0.6;font-size:0.9em;">No collection matches.</div>' +
                '</div>' +
                '<div class="imp-coll-warn" style="display:none;border:1px solid rgba(224,160,48,0.5);background:rgba(224,160,48,0.08);border-radius:6px;padding:10px 12px;margin-bottom:10px;font-size:0.88em;line-height:1.45;"></div>' +
                '<div class="backup-error" style="color:var(--hsc-danger-text);font-size:0.85em;min-height:1.2em;margin-bottom:6px;"></div>' +
                '<div style="display:flex;gap:10px;justify-content:flex-end;align-items:center;">' +
                '<span class="imp-coll-count" style="margin-right:auto;font-size:0.85em;opacity:0.7;"></span>' +
                '<button type="button" class="btnImpCancel hsc-mbtn hsc-btn-secondary">Cancel</button>' +
                '<button type="button" class="btnImpApply hsc-mbtn hsc-btn-primary"><i class="md-icon" style="font-size:1em;vertical-align:middle;margin-right:6px;">library_add</i>Import</button>' +
                '</div>'
            );
            var applyBtn = modal.querySelector('.btnImpApply');
            function picked() {
                return Array.from(modal.querySelectorAll('.chkImpColl:checked')).map(function (c) { return list[parseInt(c.dataset.idx, 10)]; });
            }
            function refresh() {
                var sel = picked();
                modal.querySelector('.imp-coll-count').textContent = sel.length ? sel.length + ' selected' : '';
                applyBtn.disabled = sel.length === 0;
                applyBtn.style.opacity = sel.length ? '' : '0.5';
                var warn = modal.querySelector('.imp-coll-warn');
                // Generic: HSC cannot know whether another plugin or tool also updates a collection.
                var html = sel.length ? 'If another plugin or tool also updates ' + (sel.length === 1 ? 'this collection' : 'these collections') + ', switch that off there first, otherwise the two keep changing ' + (sel.length === 1 ? 'it' : 'them') + '.' : '';
                warn.innerHTML = html;
                warn.style.display = html ? 'block' : 'none';
            }
            refresh();
            modal.querySelectorAll('.chkImpColl').forEach(function (c) { c.addEventListener('change', refresh); });
            var search = modal.querySelector('.txtImpCollSearch');
            if (search) {
                var selSort = modal.querySelector('.selImpCollSort'), selCount = modal.querySelector('.selImpCollCount');
                var applyView = function () {
                    var q = search.value.trim().toLowerCase(), shown = 0;
                    var band = selCount.value.split('-'), lo = band[0] === '' ? 0 : +band[0], hi = band[1] === '' || band[1] == null ? Infinity : +band[1];
                    modal.querySelectorAll('.imp-coll-row').forEach(function (r) {
                        var n = +r.dataset.count;
                        var on = (!q || r.dataset.name.indexOf(q) >= 0) && n >= lo && n <= hi;
                        r.style.display = on ? 'flex' : 'none';
                        if (on) shown++;
                    });
                    modal.querySelector('.imp-coll-none').style.display = shown ? 'none' : 'block';
                };
                search.addEventListener('input', applyView);
                selCount.addEventListener('change', applyView);
                selSort.addEventListener('change', function () {
                    var box = modal.querySelector('.imp-coll-list'), none = box.querySelector('.imp-coll-none');
                    var rows = Array.from(box.querySelectorAll('.imp-coll-row')), dir = selSort.value === 'most' ? -1 : 1;
                    rows.sort(function (a, b) {
                        var ia = +a.querySelector('.chkImpColl').dataset.idx, ib = +b.querySelector('.chkImpColl').dataset.idx;
                        return selSort.value === 'name' ? ia - ib : (dir * (a.dataset.count - b.dataset.count) || ia - ib);
                    });
                    rows.forEach(function (r) { box.insertBefore(r, none); });
                });
                search.addEventListener('keydown', function (e) { if (e.key === 'Enter') e.preventDefault(); });
                search.focus();
            }
            modal.querySelector('.btnImpCancel').addEventListener('click', modal.close);
            applyBtn.addEventListener('click', function () {
                var sel = picked();
                if (!sel.length) return;
                var errEl = modal.querySelector('.backup-error');
                errEl.textContent = '';
                applyBtn.disabled = true;
                applyBtn.innerHTML = 'Importing <span class="tc-dot-loader"><span></span><span></span><span></span></span>';
                modal.dataset.busy = '1';
                postSourceJson('HomeScreenCompanion/Collections/Import', { Ids: sel.map(function (c) { return c.Id; }) })
                    .then(function (result) {
                        if (!result || !result.Success) throw new Error((result && result.Message) || 'Unknown error');
                        var groups = groupConfigTags(result.Tags || []);
                        var configs = Object.keys(groups).map(function (k) { return groups[k]; });
                        // Prepended one by one: last first, so the cards keep the list's order.
                        configs.slice().reverse().forEach(function (config) {
                            config.HomeSectionTracked = [];
                            config.PlaylistMappings = [];
                            config.LastModified = new Date().toISOString();
                            // A source with this name already exists (e.g. one that only tags): give the
                            // card its own tag key so the two stay separate. The collection name is untouched.
                            if (sourceNameTaken(view, config)) config.Tag = config.Tag + ' (collection)';
                            renderTagGroup(config, view.querySelector('#tagListContainer'), true, undefined, true);
                        });
                        applyFilters(view);
                        setTimeout(checkFormState, 0);
                        delete modal.dataset.busy;
                        var names = configs.map(function (c) { return esc(c.CollectionName || c.Name); }).join(', ');
                        showSourceResult(modal,
                            configs.length ? (configs.length === 1 ? 'Collection imported: ' : configs.length + ' collections imported: ') + names : 'Nothing was imported',
                            configs.length ? 'Not saved yet – check the new card' + (configs.length === 1 ? '' : 's') + ' at the top and click <strong>Save</strong>. HSC manages ' + (configs.length === 1 ? 'it' : 'them') + ' from the next sync.' : '',
                            result.Notices || []);
                    })
                    .catch(function (err) {
                        delete modal.dataset.busy;
                        applyBtn.disabled = false;
                        applyBtn.innerHTML = '<i class="md-icon" style="font-size:1em;vertical-align:middle;margin-right:6px;">library_add</i>Import';
                        errEl.textContent = 'Import failed: ' + (err.message || err);
                    });
            });
        }).catch(function (err) {
            modal.renderBox('<h3 style="' + _backupTitleStyle + '">Import collection</h3>' +
                '<p style="color:var(--hsc-danger-text);font-size:0.9em;">The collections could not be loaded: ' + esc(err && (err.message || err.statusText) || err) + '</p>' +
                '<div style="display:flex;justify-content:flex-end;"><button type="button" class="btnImpCancel hsc-mbtn hsc-btn-secondary">Close</button></div>');
            modal.querySelector('.btnImpCancel').addEventListener('click', modal.close);
        });
    }

    // Paste top-list: the server maps users and ranked titles to this server; the create dialog
    // for that kind of list then opens pre-filled (with the notices on top) – check it and click
    // Create, which runs the usual steps (library or show tags, home rows, access).
    function showPasteTopListModal(onSuccess) {
        showPasteModal(_copyKinds.toplist, {
            hint: 'Paste a top-list copied with <strong>Copy</strong> on a top-list – from this server or another one. The create dialog for that kind of list opens filled in; check it and click <strong>Create</strong>. Users are matched by name, titles by IMDb/TMDb/TVDb id (then name and year); anything this server does not have is listed in the dialog.',
            applyLabel: 'Open in create dialog',
            notMsg: 'This is not a copied top-list. Use Copy on a top-list and paste that text here.',
            isValid: function (p) { return typeof p.ListName === 'string'; },
            path: 'HomeScreenCompanion/TopList/Import',
            bodyKey: 'TopListJson',
            onResult: function (result, modal) {
                if (modal) modal.close();
                openPastedTopList(result, onSuccess);
            }
        });
    }

    function openPastedTopList(r, onSuccess) {
        var isShows = r.ContentType === 'Shows';
        var prefill = {
            listName:       r.ListName || '',
            userIds:        r.UserIds || [],
            customName:     r.CustomName || '',
            displayMode:    r.DisplayMode || '',
            imageType:      r.ImageType || '',
            cardSizeOffset: normCardSize(r.CardSizeOffset),
            badgeStyle:     r.BadgeStyle || 'neutral',
            badgeOptions:   r.BadgeOptions || '',
            realPoster:     r.RealPosterBadge || '',
            maxItems:       String(r.MaxItems || '0'),
            movies:         r.Items || [],
            notices:        r.Notices || []
        };
        if (r.IsManual) showManualTopListModal(onSuccess, null, isShows ? 'Shows' : '', { prefill: prefill });
        else showTopListModal(r.SourceTag, r.SourceTag, onSuccess, null, isShows ? 'Shows' : 'Movies', prefill);
    }

    function showBackupModal() {
        var modal = buildBackupModalShell();
        modal.renderBox(
            '<h3 style="' + _backupTitleStyle + '">Download Backup</h3>' +
            '<p style="' + _backupHintStyle + '">Choose what to include. Only configuration is saved – tags, collections, playlists, top-list files and images are recreated by the plugin on the next sync run.</p>' +
            '<div style="margin-bottom:16px;">' + buildBackupSectionsHtml(null, 'chkBackupSection') + '</div>' +
            '<div class="backup-error" style="color:var(--hsc-danger-text);font-size:0.85em;min-height:1.2em;margin-bottom:6px;"></div>' +
            '<div style="display:flex;gap:10px;justify-content:flex-end;align-items:center;">' +
            '<button type="button" class="btnBackupCancel hsc-mbtn hsc-btn-secondary">Cancel</button>' +
            '<button type="button" class="btnBackupDownload hsc-mbtn hsc-btn-primary"><i class="md-icon" style="font-size:1em;vertical-align:middle;margin-right:6px;">download</i>Download</button>' +
            '</div>'
        );
        modal.querySelector('.btnBackupCancel').addEventListener('click', modal.close);
        modal.querySelector('.btnBackupDownload').addEventListener('click', function () {
            var btn = this;
            var errEl = modal.querySelector('.backup-error');
            var flags = readBackupSectionFlags(modal, 'chkBackupSection');
            if (!Object.keys(flags).some(function (k) { return flags[k]; })) { errEl.textContent = 'Select at least one section.'; return; }
            errEl.textContent = '';
            btn.disabled = true;
            btn.innerHTML = 'Preparing <span class="tc-dot-loader"><span></span><span></span><span></span></span>';
            modal.dataset.busy = '1';
            var tok = window.ApiClient.accessToken ? window.ApiClient.accessToken() : '';
            fetch(window.ApiClient.getUrl('HomeScreenCompanion/Backup/Export'), {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', 'X-Emby-Token': tok },
                body: JSON.stringify(flags)
            })
            .then(function (r) { if (!r.ok) throw new Error('Server returned ' + r.status); return r.json(); })
            .then(function (backup) {
                var json = JSON.stringify(backup, null, 2);
                var blob = new Blob([json], { type: 'application/json' });
                var url = URL.createObjectURL(blob);
                var a = document.createElement('a');
                a.href = url; a.download = 'HSC_Backup_' + new Date().toISOString().split('T')[0] + '.json';
                document.body.appendChild(a); a.click(); document.body.removeChild(a); URL.revokeObjectURL(url);
                modal.close();
            })
            .catch(function (err) {
                delete modal.dataset.busy;
                btn.disabled = false;
                btn.innerHTML = '<i class="md-icon" style="font-size:1em;vertical-align:middle;margin-right:6px;">download</i>Download';
                errEl.textContent = 'Backup failed: ' + (err.message || err);
            });
        });
    }

    // Download logs: one zip for troubleshooting (see LogBundle.cs on the server). API keys are always
    // masked there; user names only when "Anonymise user names" is ticked (the default). "Who is who"
    // lists the number each user gets, here only (never in the zip).
    function showDownloadLogsModal() {
        var modal = buildBackupModalShell();
        // Opened from the log window (.modal-backdrop, z-index 99999): on top of it.
        modal.style.zIndex = '100000';
        modal.renderBox(
            '<h3 style="' + _backupTitleStyle + '">Download logs</h3>' +
            '<p style="' + _backupHintStyle + '">One zip file with everything needed to look into a problem: the plugin\'s own log of the last 7 days (all detail lines, also when Extended log is off), the latest run of each task, the plugin settings and a snapshot of the server right now – libraries, users and their library access, every user\'s home rows and the top-lists.</p>' +
            '<label style="display:flex;align-items:flex-start;gap:10px;padding:8px 0;cursor:pointer;">' +
            '<input type="checkbox" class="chkLogsAnonymise" checked style="margin-top:3px;" />' +
            '<span style="flex:1;">' +
            '<span style="display:block;font-weight:600;font-size:0.95em;">Anonymise user names</span>' +
            '<span style="display:block;font-size:0.82em;opacity:0.65;margin-top:2px;">Each user\'s name and id becomes User 1, User 2 … – the same one in every file, so a user can still be followed. Names you typed into sources, tags or libraries yourself may still contain them.</span>' +
            '</span></label>' +
            '<details class="help-more logs-who" style="margin:0 0 4px 23px;">' +
            '<summary><i class="md-icon">expand_more</i><span class="help-more-closed">Show who is who</span><span class="help-more-open">Hide who is who</span></summary>' +
            '<div style="font-size:0.82em;opacity:0.65;margin:6px 0 8px;">Only shown here, never put in the zip – so you can tell the one you send it to which user you mean.</div>' +
            '<div class="logs-who-list" style="display:grid;grid-template-columns:repeat(auto-fill,minmax(170px,1fr));gap:3px 16px;font-size:0.85em;max-height:180px;overflow-y:auto;">Loading…</div>' +
            '</details>' +
            '<div class="hsc-callout" style="margin:12px 0 16px;">' +
            '<i class="md-icon">lock</i>' +
            '<span><strong>API keys are always hidden.</strong> Keys and tokens (Trakt, MDBList, TMDb, OpenAI, Gemini, Claude, OpenAI-compatible, and keys in list URLs) are replaced by *** in every file, whether or not names are anonymised. Titles, source and library names stay as they are.</span>' +
            '</div>' +
            '<div class="logs-error" style="color:var(--hsc-danger-text);font-size:0.85em;min-height:1.2em;margin-bottom:6px;"></div>' +
            '<div style="display:flex;gap:10px;justify-content:flex-end;align-items:center;">' +
            '<button type="button" class="btnLogsCancel hsc-mbtn hsc-btn-secondary">Cancel</button>' +
            '<button type="button" class="btnLogsDownload hsc-mbtn hsc-btn-primary"><i class="md-icon" style="font-size:1em;vertical-align:middle;margin-right:6px;">download</i>Download</button>' +
            '</div>'
        );
        // "Who is who": the number each user gets in an anonymised zip (the server's order).
        var chkAnon = modal.querySelector('.chkLogsAnonymise');
        var whoBox = modal.querySelector('.logs-who');
        chkAnon.addEventListener('change', function () { whoBox.style.display = chkAnon.checked ? '' : 'none'; });
        var tokWho = window.ApiClient.accessToken ? window.ApiClient.accessToken() : '';
        fetch(window.ApiClient.getUrl('HomeScreenCompanion/Logs/UserNumbers'), { headers: { 'X-Emby-Token': tokWho } })
        .then(function (r) { if (!r.ok) throw new Error('Server returned ' + r.status); return r.json(); })
        .then(function (res) {
            var users = (res && res.Users) || [];
            modal.querySelector('.logs-who-list').innerHTML = users.length === 0 ? 'No users.' : users.map(function (u) {
                return '<span style="white-space:nowrap;overflow:hidden;text-overflow:ellipsis;" title="User ' + u.Number + ' = ' + escapeHtml(u.Name) + '">' +
                    '<span style="opacity:0.6;margin-right:6px;">User ' + u.Number + '</span>' + escapeHtml(u.Name) + '</span>';
            }).join('');
        })
        .catch(function () { modal.querySelector('.logs-who-list').textContent = 'The list could not be loaded.'; });
        modal.querySelector('.btnLogsCancel').addEventListener('click', modal.close);
        modal.querySelector('.btnLogsDownload').addEventListener('click', function () {
            var btn = this;
            var errEl = modal.querySelector('.logs-error');
            var anonymise = modal.querySelector('.chkLogsAnonymise').checked;
            errEl.textContent = '';
            btn.disabled = true;
            btn.innerHTML = 'Preparing <span class="tc-dot-loader"><span></span><span></span><span></span></span>';
            modal.dataset.busy = '1';
            var tok = window.ApiClient.accessToken ? window.ApiClient.accessToken() : '';
            fetch(window.ApiClient.getUrl('HomeScreenCompanion/Logs/Download', { Anonymize: anonymise }), { headers: { 'X-Emby-Token': tok } })
            .then(function (r) { if (!r.ok) throw new Error('Server returned ' + r.status); return r.json(); })
            .then(function (res) {
                if (!res.Success) throw new Error(res.Message || 'The logs could not be put together.');
                var bin = atob(res.Data);
                var bytes = new Uint8Array(bin.length);
                for (var i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
                var url = URL.createObjectURL(new Blob([bytes], { type: 'application/zip' }));
                var a = document.createElement('a');
                a.href = url; a.download = res.FileName || 'hsc-logs.zip';
                document.body.appendChild(a); a.click(); document.body.removeChild(a); URL.revokeObjectURL(url);
                modal.close();
            })
            .catch(function (err) {
                delete modal.dataset.busy;
                btn.disabled = false;
                btn.innerHTML = '<i class="md-icon" style="font-size:1em;vertical-align:middle;margin-right:6px;">download</i>Download';
                errEl.textContent = 'Download failed: ' + (err.message || err);
            });
        });
    }

    // Reads which sections a backup file contains. Current files list them; legacy files were a
    // raw config dump, so infer from the keys that are present.
    function detectBackupSections(parsed) {
        var info = { legacy: false, sections: new Set(), createdUtc: '', pluginVersion: '' };
        if (parsed && typeof parsed.BackupVersion === 'number') {
            (parsed.Sections || []).forEach(function (s) { info.sections.add(s); });
            info.createdUtc = parsed.CreatedUtc || '';
            info.pluginVersion = parsed.PluginVersion || '';
            return info;
        }
        // A legacy backup is a saved plugin configuration: only the sections whose fields are in the
        // file are offered (any other JSON would restore defaults and wipe the API keys). A copied
        // source or top-list is not a backup.
        info.legacy = true;
        if (parsed['hsc-source'] !== undefined || parsed['hsc-toplist'] !== undefined) return info;
        var has = function (keys) { return keys.some(function (k) { return Object.prototype.hasOwnProperty.call(parsed, k); }); };
        if (has(['OpenAiModel', 'GeminiModel', 'ClaudeModel', 'OllamaBaseUrl', 'OllamaModel', 'AiSystemPrompt', 'ExtendedConsoleOutput',
            'LogMissingItems', 'DryRunMode', 'PreserveTagsOnEmptyResult', 'TopListMirrorCollections', 'ShowCopyPasteButtons',
            'ShowImportCollectionButton', 'RankedCollectionsEnabled'])) info.sections.add('Settings');
        if (has(['TraktClientId', 'MdblistApiKey', 'TmdbApiKey', 'OpenAiApiKey', 'GeminiApiKey', 'ClaudeApiKey'])) info.sections.add('ApiKeys');
        if (Array.isArray(parsed.Tags)) info.sections.add('Tags');
        if (Array.isArray(parsed.SavedFilters)) info.sections.add('SavedFilters');
        return info;
    }

    // onRestored(result) is called after a successful import so the page can reload its state.
    function showRestoreModal(rawText, onRestored) {
        function escHtml(s) { return String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); }

        var parsed;
        try { parsed = JSON.parse(rawText); } catch (err) {
            window.Dashboard.alert('Failed to parse configuration file. The file may be corrupt or not a valid backup file. Error: ' + err.message);
            return;
        }
        if (!parsed || typeof parsed !== 'object') {
            window.Dashboard.alert('The selected file is not a Home Screen Companion backup.');
            return;
        }
        var info = detectBackupSections(parsed);
        if (info.sections.size === 0) {
            window.Dashboard.alert('The selected file is not a Home Screen Companion backup.');
            return;
        }

        var fileInfo = info.legacy
            ? 'Legacy backup (created by an older plugin version)'
            : 'Created ' + (info.createdUtc ? new Date(info.createdUtc).toLocaleString() : 'unknown') + (info.pluginVersion ? ' · plugin v' + escHtml(info.pluginVersion) : '');

        var modal = buildBackupModalShell();
        modal.renderBox(
            '<h3 style="' + _backupTitleStyle + '">Restore Backup</h3>' +
            '<p style="' + _backupHintStyle + 'margin-bottom:6px;">' + fileInfo + '</p>' +
            '<p style="' + _backupHintStyle + '">Select what to restore. Each selected section <strong>replaces</strong> the current configuration on the server immediately. Sections you leave unchecked are not touched.</p>' +
            '<div style="margin-bottom:16px;">' + buildBackupSectionsHtml(info.sections, 'chkRestoreSection') + '</div>' +
            '<div class="backup-error" style="color:var(--hsc-danger-text);font-size:0.85em;min-height:1.2em;margin-bottom:6px;"></div>' +
            '<div style="display:flex;gap:10px;justify-content:flex-end;align-items:center;">' +
            '<button type="button" class="btnRestoreCancel hsc-mbtn hsc-btn-secondary">Cancel</button>' +
            '<button type="button" class="btnRestoreApply hsc-mbtn hsc-btn-primary"><i class="md-icon" style="font-size:1em;vertical-align:middle;margin-right:6px;">upload</i>Restore</button>' +
            '</div>'
        );
        modal.querySelector('.btnRestoreCancel').addEventListener('click', modal.close);
        modal.querySelector('.btnRestoreApply').addEventListener('click', function () {
            var btn = this;
            var errEl = modal.querySelector('.backup-error');
            var flags = readBackupSectionFlags(modal, 'chkRestoreSection');
            if (!Object.keys(flags).some(function (k) { return flags[k]; })) { errEl.textContent = 'Select at least one section.'; return; }
            errEl.textContent = '';
            btn.disabled = true;
            btn.innerHTML = 'Restoring <span class="tc-dot-loader"><span></span><span></span><span></span></span>';
            modal.dataset.busy = '1';
            var body = Object.assign({ BackupJson: rawText }, flags);
            var tok = window.ApiClient.accessToken ? window.ApiClient.accessToken() : '';
            fetch(window.ApiClient.getUrl('HomeScreenCompanion/Backup/Import'), {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', 'X-Emby-Token': tok },
                body: JSON.stringify(body)
            })
            .then(function (r) { if (!r.ok) throw new Error('Server returned ' + r.status); return r.json(); })
            .then(function (result) {
                if (!result || !result.Success) throw new Error((result && result.Message) || 'Unknown error');
                delete modal.dataset.busy;
                if (typeof onRestored === 'function') onRestored(result);
                renderRestoreResult(modal, result);
            })
            .catch(function (err) {
                delete modal.dataset.busy;
                btn.disabled = false;
                btn.innerHTML = '<i class="md-icon" style="font-size:1em;vertical-align:middle;margin-right:6px;">upload</i>Restore';
                errEl.textContent = 'Restore failed: ' + (err.message || err);
            });
        });
    }

    function renderRestoreResult(modal, result) {
        function escHtml(s) { return String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); }
        var pending = result.TopListsNeedingLibrary || [];
        var listStyle = 'margin:0 0 14px;padding-left:20px;font-size:0.9em;line-height:1.5;';

        var html =
            '<div style="text-align:center;padding:4px 0 14px;">' +
            '<i class="md-icon" style="font-size:2.5em;color:var(--hsc-primary-text);display:block;margin-bottom:8px;">check_circle</i>' +
            '<p style="margin:0;font-size:1.05em;font-weight:500;">Backup restored</p>' +
            '</div>' +
            '<span class="hsc-eyebrow" style="display:block;margin-bottom:4px;color:inherit;opacity:0.7;">Applied</span>' +
            '<ul style="' + listStyle + '">' + (result.Applied || []).map(function (a) { return '<li>' + escHtml(a) + '</li>'; }).join('') + '</ul>';

        if ((result.Warnings || []).length > 0) {
            html += '<span class="hsc-eyebrow" style="display:block;margin-bottom:4px;color:var(--hsc-warning-text);">Warnings</span>' +
                '<ul style="' + listStyle + 'opacity:0.85;">' + result.Warnings.map(function (w) { return '<li>' + escHtml(w) + '</li>'; }).join('') + '</ul>';
        }

        if (pending.length > 0) {
            html += '<div style="border:1px solid rgba(224,160,48,0.5);background:rgba(224,160,48,0.08);border-radius:6px;padding:12px 14px;margin-bottom:14px;font-size:0.9em;line-height:1.5;">' +
                '<strong>' + pending.length + ' top-list' + (pending.length !== 1 ? 's' : '') + ' need' + (pending.length === 1 ? 's' : '') + ' an Emby library:</strong> ' +
                pending.map(function (p) { return escHtml(p.CustomName || p.TagName); }).join(', ') + '.<br/>' +
                'Their settings and files are restored, but no library exists for them on this server yet. Create them now (this creates the libraries, home sections and access rights exactly like <em>+ Create New</em>), or later by opening each list in the Top Lists tab and clicking Save.' +
                '<div class="restore-tl-progress" style="margin-top:8px;font-size:0.88em;opacity:0.8;"></div>' +
                '</div>';
        }

        html += '<p style="' + _backupHintStyle + '">Run a sync afterwards to rebuild tags, collections, playlists and home sections from the restored configuration.</p>' +
            '<div style="display:flex;gap:10px;justify-content:flex-end;align-items:center;padding-top:14px;border-top:1px solid var(--line-color);">' +
            (pending.length > 0 ? '<button type="button" class="btnRestoreCreateLibs hsc-mbtn hsc-btn-primary"><i class="md-icon" style="font-size:1em;vertical-align:middle;margin-right:6px;">library_add</i>Create libraries now</button>' : '') +
            '<button type="button" class="btnRestoreDone hsc-mbtn ' + (pending.length > 0 ? 'hsc-btn-secondary' : 'hsc-btn-primary') + '">Close</button>' +
            '</div>';

        modal.renderBox(html);
        modal.querySelector('.btnRestoreDone').addEventListener('click', modal.close);

        var createBtn = modal.querySelector('.btnRestoreCreateLibs');
        if (createBtn) {
            createBtn.addEventListener('click', function () {
                createBtn.disabled = true;
                modal.querySelector('.btnRestoreDone').disabled = true;
                modal.dataset.busy = '1';
                var progressEl = modal.querySelector('.restore-tl-progress');
                var failures = [];
                // Dummy UI targets: executeTopListCreationSteps writes its status into these.
                var dummyBtn = document.createElement('button');
                var dummyErr = document.createElement('div');
                var dummyModal = document.createElement('div');

                pending.reduce(function (p, tl, idx) {
                    return p.then(function () {
                        progressEl.textContent = 'Creating ' + (idx + 1) + ' of ' + pending.length + ': ' + (tl.CustomName || tl.TagName) + '…';
                        return new Promise(function (resolve, reject) {
                            executeTopListCreationSteps(
                                tl.TagName, tl.CustomName || tl.TagName, tl.UserIds || [], tl.DisplayMode || '', tl.CustomName || tl.TagName,
                                tl.ImageType || '', tl.MaxItems || 0, { FolderPath: tl.FolderPath, FilesCreated: 0 },
                                { saveBtn: dummyBtn, errEl: dummyErr, modal: dummyModal, badgeStyle: tl.BadgeStyle || 'neutral', badgeOptions: tl.BadgeOptions || '', cardSizeOffset: tl.CardSizeOffset, silent: true, closeHandler: resolve }
                            ).catch(reject);
                        }).catch(function (err) {
                            failures.push((tl.CustomName || tl.TagName) + ': ' + (err && err.message ? err.message : err));
                        });
                    });
                }, Promise.resolve()).then(function () {
                    delete modal.dataset.busy;
                    modal.querySelector('.btnRestoreDone').disabled = false;
                    if (failures.length === 0) {
                        progressEl.style.color = 'var(--hsc-primary)';
                        progressEl.textContent = 'All ' + pending.length + ' librar' + (pending.length === 1 ? 'y' : 'ies') + ' created. Finishing touches continue in the background.';
                        createBtn.style.display = 'none';
                    } else {
                        progressEl.style.color = 'var(--hsc-danger-text)';
                        progressEl.innerHTML = 'Some libraries could not be created:<br/>' + failures.map(escHtml).join('<br/>');
                        createBtn.disabled = false;
                    }
                    var tlContainer = document.querySelector('#tlContainer');
                    if (tlContainer) tlContainer.dataset.loaded = '';
                });
            });
        }
    }

    function loadTopListsTab(view) {
        var container = view.querySelector('#tlContainer');
        if (!container) return;

        container.innerHTML = '<div style="padding:20px;color:var(--theme-text-secondary);display:flex;align-items:center;gap:10px;">Loading <span class="tc-dot-loader"><span></span><span></span><span></span></span></div>';

        var token = window.ApiClient.accessToken();

        Promise.all([
            fetch(window.ApiClient.getUrl('HomeScreenCompanion/Manage/Tags'), { headers: { 'X-MediaBrowser-Token': token } }).then(function (r) { return r.json(); }),
            fetch(window.ApiClient.getUrl('HomeScreenCompanion/Manage/Collections'), { headers: { 'X-MediaBrowser-Token': token } }).then(function (r) { return r.json(); }),
            window.ApiClient.getPluginConfiguration(pluginId).catch(function () { return { Tags: [] }; }),
            fetch(window.ApiClient.getUrl('HomeScreenCompanion/TopList/List'), { headers: { 'X-MediaBrowser-Token': token } }).then(function (r) { return r.json(); }).catch(function () { return { FolderNames: [] }; })
        ]).then(function (results) {
            var tagsData = results[0];
            var collectionsData = results[1];
            var pluginConfig = results[2];
            var existingTopLists = new Set((results[3].FolderNames || []).map(function (n) { return n.toLowerCase(); }));

            function sanitizeTlName(name) {
                var safe = (name || 'unknown').replace(/[\\/:*?"<>|\x00-\x1f]/g, '_').replace(/^\.+|\.+$/g, '').trim();
                return safe.length === 0 ? 'unknown' : safe;
            }

            var topListCustomNameMap = {};
            var topListLibraryIdMap = {};
            (pluginConfig.TopLists || []).forEach(function (tl) {
                if (!tl.TagName) return;
                var key = sanitizeTlName(tl.TagName).toLowerCase();
                var settings = {};
                try { settings = JSON.parse(tl.HomeSectionSettings || '{}'); } catch (e) {}
                if (settings.CustomName) topListCustomNameMap[key] = settings.CustomName;
                if (tl.HomeSectionLibraryId && tl.HomeSectionLibraryId !== 'auto')
                    topListLibraryIdMap[key] = tl.HomeSectionLibraryId;
            });

            var managedTagMap = {};
            var managedCollMap = {};
            var seenGroupByTag = {};
            (pluginConfig.Tags || []).forEach(function (t, idx) {
                if (!t.Tag) return;
                var tName = t.Tag.trim();
                var tKey = tName.toLowerCase();
                if (seenGroupByTag[tKey]) return;
                seenGroupByTag[tKey] = true;
                var groupLabel = (t.Name && t.Name.trim() && t.Name.trim().toLowerCase() !== tKey) ? t.Name.trim() : tName;
                var entry = { displayName: groupLabel, groupIndex: idx, groupActive: !!t.Active };
                if (!managedTagMap[tKey]) managedTagMap[tKey] = [];
                managedTagMap[tKey].push(entry);
                if (t.EnableCollection) {
                    var cName = (t.CollectionName && t.CollectionName.trim()) ? t.CollectionName.trim() : tName;
                    var cKey = cName.toLowerCase();
                    if (!managedCollMap[cKey]) managedCollMap[cKey] = [];
                    managedCollMap[cKey].push(entry);
                }
            });

            function escAttr(s) { return String(s || '').replace(/&/g, '&amp;').replace(/"/g, '&quot;'); }
            function escHtml(s) { return String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); }


            var searchInputStyle = 'background:var(--plugin-input-bg);border:1px solid var(--plugin-input-border);border-radius:6px;padding:5px 10px;font-size:0.9em;color:var(--plugin-popup-color);width:400px;max-width:100%;';

            var realTagNamesLower = new Set((tagsData.Tags || []).map(function (t) { return (t.Name || '').toLowerCase(); }));

            // The source that sets each source top-list (Create as top-list in its Home Screen tab).
            var sourceNameByTag = {};
            (pluginConfig.Tags || []).forEach(function (t) {
                var k = (t.Tag || '').trim().toLowerCase();
                if (t.EnableTopList && k && !sourceNameByTag[k]) sourceNameByTag[k] = (t.Name || '').trim() || (t.Tag || '').trim();
            });
            // Every list, also one whose folder is gone (the sync then asks to open it and Save; the
            // card says so).
            var allExistingTopLists = (pluginConfig.TopLists || []).filter(function (tl) {
                return !!tl.TagName;
            }).map(function (tl) {
                var key = sanitizeTlName(tl.TagName || '').toLowerCase();
                var settings = {};
                try { settings = JSON.parse(tl.HomeSectionSettings || '{}'); } catch (e) {}
                return {
                    tagName:     tl.TagName || '',
                    displayName: settings.CustomName || tl.TagName || '',
                    fromSource:  !!tl.FromSource,
                    sourceName:  sourceNameByTag[(tl.TagName || '').trim().toLowerCase()] || '',
                    noLibrary:   !tl.HomeSectionLibraryId || tl.HomeSectionLibraryId === 'auto',
                    isManual:    !tl.FromSource && !realTagNamesLower.has((tl.TagName || '').toLowerCase()),
                    contentType: tl.ContentType === 'Shows' ? 'Shows' : 'Movies',
                    count:       (results[3].MovieCounts || {})[key] || 0,
                    userIds:     tl.HomeSectionUserIds || [],
                    customName:  settings.CustomName  || '',
                    displayMode: settings.DisplayMode || '',
                    imageType:   settings.ImageType   || '',
                    cardSizeOffset: normCardSize(settings.CardSizeOffset),
                    badgeStyle:  settings.BadgeStyle  || 'neutral',
                    badgeOptions: settings.BadgeOptions || '',
                    realPoster:  settings.RealPosterBadge || '',
                    maxItems:    settings.MaxItems    || 0,
                    position:    settings._position   || ''
                };
            });

            function renderTopListRows(items) {
                if (items.length === 0) {
                    return '<div id="tlRowsList"><p style="color:var(--theme-text-secondary);font-size:0.9em;font-style:italic;padding:20px 0;">No top-lists created yet. Click <strong>+ Create New</strong> to get started. <a href="javascript:void(0)" class="help-link hsc-open-quickstart">Quick Start</a></p></div>';
                }
                var labelStyle = 'font-size:0.88em;font-weight:500;opacity:0.75;display:block;margin-bottom:2px;';
                var rows = items.map(function (item) {
                    var isManual = item.isManual;
                    var typeBadge = isManual
                        ? '<span class="tag-indicator toplist" style="flex-shrink:0;"><i class="md-icon" style="font-size:1.1em;">format_list_numbered</i> Manual</span>'
                        : '<span class="tag-indicator tag" style="flex-shrink:0;"><i class="md-icon" style="font-size:1.1em;">label</i> ' + escHtml(item.tagName) + '</span>';
                    var isShows = item.contentType === 'Shows';
                    var contentBadge = isShows
                        ? '<span class="tag-indicator ct-shows" style="flex-shrink:0;"><i class="md-icon" style="font-size:1.1em;">tv</i> TV Shows</span>'
                        : '<span class="tag-indicator ct-movies" style="flex-shrink:0;"><i class="md-icon" style="font-size:1.1em;">movie</i> Movies</span>';
                    var displayModeLabel = ({'': 'Always', 'tv': 'TV mode only', 'mobile,desktop': 'Non-TV only'})[item.displayMode] || 'Always';
                    var imageTypeLabel = item.imageType || 'Auto';
                    var maxItemsLabel  = item.maxItems ? String(item.maxItems) : '0 (all)';
                    if (item.fromSource)
                        typeBadge = '<span class="tag-indicator tag" style="flex-shrink:0;" title="Set in the source\'s Home Screen tab"><i class="md-icon" style="font-size:1.1em;">label</i> ' + escHtml(item.sourceName || item.tagName) + '</span>';
                    var editJson = escAttr(JSON.stringify({
                        tagName:     item.tagName,
                        displayName: item.displayName,
                        fromSource:  item.fromSource,
                        sourceName:  item.sourceName,
                        noLibrary:   item.noLibrary,
                        isManual:    item.isManual,
                        contentType: item.contentType,
                        userIds:     item.userIds,
                        customName:  item.customName,
                        displayMode: item.displayMode,
                        imageType:   item.imageType,
                        cardSizeOffset: item.cardSizeOffset,
                        badgeStyle:  item.badgeStyle,
                        badgeOptions: item.badgeOptions || '',
                        realPoster:  item.realPoster || '',
                        maxItems:    String(item.maxItems || '0'),
                        position:    item.position || ''
                    }));
                    return '<div class="tag-row" data-tlname="' + escAttr(item.tagName.toLowerCase()) + '" data-ismanual="' + (isManual ? '1' : '0') + '" data-isshows="' + (isShows ? '1' : '0') + '" data-count="' + item.count + '" data-editjson="' + editJson + '">' +
                        '<div class="tl-row-header tag-header">' +
                        // Same order as a source row: name, then its chips (type, tag / Manual, number of items)
                        '<div style="display:flex;align-items:center;flex-wrap:wrap;row-gap:4px;min-width:0;">' +
                        '<span class="tag-title" style="margin-left:0;">' + escHtml(item.displayName) + '</span>' +
                        contentBadge + typeBadge +
                        '<span class="tag-indicator" style="flex-shrink:0;font-variant-numeric:tabular-nums;">' + item.count + (isShows ? (item.count === 1 ? ' show' : ' shows') : (item.count === 1 ? ' movie' : ' movies')) + '</span>' +
                        (!item.fromSource && item.count === 0 ? '<span class="hsc-run-issue"><i class="md-icon" style="font-size:1.3em;" title="This list has no titles: its folder is empty or gone. Open it, add titles and click Save to make it again, or delete it.">warning</i></span>' : '') +
                        '</div>' +
                        '<i class="md-icon expand-icon" style="flex-shrink:0;margin-left:12px;">expand_more</i>' +
                        '</div>' +
                        '<div class="tag-body" style="display:none;">' +
                        '</div>' +
                        '</div>';
                }).join('');
                return '<div id="tlRowsList">' + rows + '</div>';
            }

            container.innerHTML =
                '<div style="max-width:900px;">' +
                '<div class="sectionTitleContainer flex align-items-center" style="margin-bottom:1em;margin-top:2em;">' +
                '<h2 class="sectionTitle" style="margin-bottom:0;margin-right:auto;">Top Lists</h2>' +
                '<button type="button" id="btnPasteTopList" is="emby-button" class="raised btn-neutral mb025" style="margin-right:8px;" title="Add a top-list copied with Copy on a top-list (this or another server)">' +
                '<i class="md-icon" style="margin-right:5px;">content_paste</i><span>Paste top-list</span>' +
                '</button>' +
                '<button type="button" id="btnCreateNewTopList" is="emby-button" class="raised button-submit mb025 hsc-btn-secondary">' +
                '<span>+ Create New</span>' +
                '</button>' +
                '</div>' +
                '<div style="margin-bottom:16px;font-size:0.9em;color:var(--theme-text-secondary);line-height:1.5;">' +
                'All top-lists. A list from a source is set in that source\'s <strong>Home Screen</strong> tab (<em>Create as top-list</em>, Tag &amp; Collection); a manual list, with movies or TV shows you pick yourself, is made and edited here. Each list gets its own hidden library with numbered posters.' +
                '</div>' +
                '<div style="display:flex;align-items:center;gap:10px;margin-bottom:16px;flex-wrap:wrap;">' +
                '<input type="text" id="tlSearch" placeholder="Search…" style="' + searchInputStyle + '" />' +
                '<select is="emby-select" id="tlFilter" style="color:var(--plugin-popup-color);background:var(--plugin-input-bg);border:1px solid var(--plugin-input-border);padding:5px;border-radius:6px;font-size:0.9em;cursor:pointer;">' +
                '<option value="all">All</option>' +
                '<option value="manual">Manual</option>' +
                '<option value="bytag">From a source</option>' +
                '<option value="movies">Movies</option>' +
                '<option value="shows">TV shows</option>' +
                '</select>' +
                '<select is="emby-select" id="tlSort" style="color:var(--plugin-popup-color);background:var(--plugin-input-bg);border:1px solid var(--plugin-input-border);padding:5px;border-radius:6px;font-size:0.9em;cursor:pointer;">' +
                '<option value="name-asc">Name A–Z</option>' +
                '<option value="name-desc">Name Z–A</option>' +
                '<option value="count-desc">Most items</option>' +
                '<option value="count-asc">Fewest items</option>' +
                '</select>' +
                '<button type="button" class="btnTlRefresh" style="border:1px solid var(--hsc-hairline-strong);background:transparent;color:var(--theme-text-secondary);"><i class="md-icon" style="font-size:1em;vertical-align:middle;">refresh</i></button>' +
                '</div>' +
                renderTopListRows(allExistingTopLists) +
                '</div>';

            container.dataset.loaded = '1';

            var btnPasteTl = container.querySelector('#btnPasteTopList');
            if (btnPasteTl) {
                btnPasteTl.addEventListener('click', function () {
                    showPasteTopListModal(function () {
                        container.dataset.loaded = '';
                        loadTopListsTab(view);
                    });
                });
            }

            var btnCreateNew = container.querySelector('#btnCreateNewTopList');
            if (btnCreateNew) {
                btnCreateNew.addEventListener('click', function () {
                    var existingTypes = {};
                    (pluginConfig.TopLists || []).forEach(function (tl) {
                        if (tl.TagName) existingTypes[sanitizeTlName(tl.TagName).toLowerCase()] = tl.ContentType === 'Shows' ? 'Shows' : 'Movies';
                    });
                    showCreateTopListChooser(tagsData, existingTopLists, function () {
                        container.dataset.loaded = '';
                        loadTopListsTab(view);
                    }, existingTypes);
                });
            }

            function applySearchSort() {
                var query  = (container.querySelector('#tlSearch').value || '').toLowerCase();
                var filter = container.querySelector('#tlFilter').value;
                var sort   = container.querySelector('#tlSort').value;

                var rowsList = container.querySelector('#tlRowsList');
                if (!rowsList) return;
                var rows = Array.from(rowsList.querySelectorAll('.tag-row'));

                rows.forEach(function (row) {
                    var tlName   = row.dataset.tlname || '';
                    var isManual = row.dataset.ismanual === '1';
                    var isShowList = row.dataset.isshows === '1';
                    var matchSearch = !query || tlName.indexOf(query) !== -1;
                    var matchFilter = true;
                    if (filter === 'manual') matchFilter = isManual;
                    if (filter === 'bytag')  matchFilter = !isManual;
                    if (filter === 'movies') matchFilter = !isShowList;
                    if (filter === 'shows')  matchFilter = isShowList;
                    row.style.display = (matchSearch && matchFilter) ? '' : 'none';
                });

                var visibleRows = rows.filter(function (r) { return r.style.display !== 'none'; });
                visibleRows.sort(function (a, b) {
                    var nameA  = a.dataset.tlname || '';
                    var nameB  = b.dataset.tlname || '';
                    var countA = parseInt(a.dataset.count || '0', 10);
                    var countB = parseInt(b.dataset.count || '0', 10);
                    if (sort === 'name-asc')   return natCmp(nameA, nameB);
                    if (sort === 'name-desc')  return natCmp(nameB, nameA);
                    if (sort === 'count-desc') return countB - countA;
                    if (sort === 'count-asc')  return countA - countB;
                    return 0;
                });
                visibleRows.forEach(function (r) { rowsList.appendChild(r); });
            }

            container.querySelector('#tlSearch').addEventListener('input', applySearchSort);
            container.querySelector('#tlFilter').addEventListener('change', applySearchSort);
            container.querySelector('#tlSort').addEventListener('change', applySearchSort);

            applySearchSort();


            if (container._tlClickHandler) container.removeEventListener('click', container._tlClickHandler);
            container._tlClickHandler = function (e) {
                var rowHeader = e.target.closest('.tl-row-header');
                if (rowHeader && !e.target.closest('button')) {
                    var row = rowHeader.closest('.tag-row');
                    var body = row && row.querySelector('.tag-body');
                    var icon = rowHeader.querySelector('.expand-icon');
                    if (body) {
                        var expanded = body.style.display !== 'none';
                        body.style.display = expanded ? 'none' : 'block';
                        if (icon) icon.textContent = expanded ? 'expand_more' : 'expand_less';
                        if (!expanded && !body.dataset.formLoaded) {
                            body.dataset.formLoaded = '1';
                            loadInlineEditForm(row, body, function () { container.dataset.loaded = ''; loadTopListsTab(view); });
                        }
                    }
                    return;
                }

                var btn = e.target.closest('button');
                if (!btn) return;

                if (btn.classList.contains('btnCopyTopList')) {
                    copyTopListToClipboard(btn.closest('.tag-row'), btn);
                    return;
                }

                if (btn.classList.contains('btnTlOpenSource') || btn.classList.contains('btnTlSourceOff')) {
                    openSourceTopListTab(view, btn.dataset.name, btn.classList.contains('btnTlSourceOff'));
                    return;
                }

                if (btn.classList.contains('btnTlRun')) {
                    var runName = btn.dataset.name;
                    var runBtn = btn;
                    var doTlRun = function () {
                        var lbl = runBtn.querySelector('.btnRunEntryLabel');
                        var btnSaveEl = view.querySelector('.btn-save');
                        var dotEl = view.querySelector('#dotStatus');
                        var labelEl = view.querySelector('#lastRunStatusLabel');
                        lbl.textContent = 'Running…';
                        runBtn.disabled = true;
                        if (btnSaveEl) { btnSaveEl.disabled = true; btnSaveEl.style.opacity = '0.5'; btnSaveEl.querySelector('span').textContent = 'Sync in progress...'; }
                        if (dotEl) { dotEl.className = 'status-dot running'; }
                        if (labelEl) labelEl.textContent = 'Running...';
                        setTimeout(function () { refreshStatus(view); }, 500); // live step (and 1 s polling) right away
                        fetch(window.ApiClient.getUrl('HomeScreenCompanion/TopList/RunOne'), {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json', 'X-MediaBrowser-Token': window.ApiClient.accessToken() },
                            body: JSON.stringify({ TagName: runName })
                        }).then(function (r) { return r.json(); })
                        .then(function (result) {
                            lbl.textContent = 'Run Group';
                            runBtn.disabled = false;
                            invalidateTagTabs(view); refreshStatus(view);
                            window.Dashboard.alert(result.Queued ? result.Message : result.Success ? ('Done: ' + result.Message) : ('Failed: ' + result.Message));
                        }).catch(function () {
                            lbl.textContent = 'Run Group';
                            runBtn.disabled = false;
                            refreshStatus(view);
                            window.Dashboard.alert('Request failed.');
                        });
                    };
                    saveIfDirtyThenRun(view, doTlRun, true);
                    return;
                }

                if (btn.classList.contains('btnTlDelete')) {
                    var deleteName = btn.dataset.name;
                    confirmDeleteTopList(deleteName, function () {
                    var deleteBtn = btn;
                    deleteBtn.disabled = true;
                    deleteBtn.textContent = 'Deleting…';
                    var deleteToken = window.ApiClient.accessToken();
                        fetch(window.ApiClient.getUrl('HomeScreenCompanion/TopList/Delete'), {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json', 'X-MediaBrowser-Token': deleteToken },
                            body: JSON.stringify({ TagName: deleteName })
                        })
                        .then(function (r) { return r.json(); })
                        .then(function (delResult) {
                            if (!delResult.Success) throw new Error(delResult.Message || 'Delete failed');
                            var folderPath = delResult.FolderPath;
                            if (!folderPath) return; // nothing left to remove
                            return fetch(window.ApiClient.getUrl('Library/VirtualFolders'), {
                                headers: { 'X-MediaBrowser-Token': deleteToken }
                            })
                            .then(function (r) { return r.json(); })
                            .then(function (folders) {
                                function normDlp(p) { return (p || '').replace(/\\/g, '/').replace(/\/+$/, '').toLowerCase(); }
                                var match = (folders || []).find(function (f) {
                                    return (f.Locations || []).some(function (loc) {
                                        return normDlp(loc) === normDlp(folderPath);
                                    });
                                });
                                if (match && match.ItemId) {
                                    return fetch(window.ApiClient.getUrl('Library/VirtualFolders') + '?Id=' + encodeURIComponent(match.ItemId) + '&RefreshLibrary=false', {
                                        method: 'DELETE',
                                        headers: { 'X-MediaBrowser-Token': deleteToken }
                                    });
                                }
                            });
                        })
                        .then(function () {
                            _topListTagNames.delete(deleteName.toLowerCase());
                            refreshTopListBadges();
                            container.dataset.loaded = '';
                            loadTopListsTab(view);
                        })
                        .catch(function (err) {
                            deleteBtn.disabled = false;
                            deleteBtn.textContent = 'Delete top-list';
                            alert('Error deleting top-list: ' + (err.message || err));
                        });
                    });
                    return;
                }

                if (btn.classList.contains('btnTlRefresh')) {
                    container.dataset.loaded = '';
                    loadTopListsTab(view);
                    return;
                }
            };
            container.addEventListener('click', container._tlClickHandler);

        }).catch(function (err) {
            container.innerHTML = '<div style="color:var(--hsc-danger-text);padding:20px;">Failed to load: ' + err + '</div>';
        });
    }

    function loadHscManageTab(view) {
        var container = view.querySelector('#hscManageContainer');
        if (!container) return;

        getHseUsers()
            .then(function (users) {
                var userOptions = (users || []).map(function (u) {
                    return '<option value="' + u.Id + '">' + escapeHtml(u.Name + userRatingSuffix(u)) + '</option>';
                }).join('');

                container.innerHTML = [
                    '<div class="hsc-card">',
                    '<h3 class="hsc-section-title">Manage Home Screen</h3>',
                    '<p class="textMuted" style="font-size:0.88em;margin-bottom:16px;">Select a user to view and manage their home screen sections. Drag rows to reorder, then click Save or Apply (changes will take effect immediately).</p>',
                    '<div style="display:flex;align-items:flex-end;gap:10px;margin-bottom:20px;">',
                    '<div style="flex-grow:1;">',
                    '<select is="emby-select" id="selManageUser" label="User">',
                    '<option value="">— Select user —</option>',
                    userOptions,
                    '</select>',
                    '</div>',
                    '<button type="button" is="emby-button" class="raised btn-neutral" id="btnRefreshManageSections" style="min-width:90px;margin-bottom:2px;" disabled>',
                    '<i class="md-icon" style="margin-right:4px;">refresh</i>Refresh',
                    '</button>',
                    '</div>',
                    '<div id="manSectionList"></div>',
                    '<div style="margin-top:16px;display:flex;justify-content:flex-end;">',
                    '<button type="button" is="emby-button" class="raised button-submit hsc-btn-secondary" id="btnApplyManage" disabled>',
                    '<i class="md-icon" style="margin-right:4px;">check</i><span>Apply changes</span>',
                    '</button>',
                    '</div>',
                    '</div>',
                ].join('');

                var selUser = container.querySelector('#selManageUser');
                var btnRefresh = container.querySelector('#btnRefreshManageSections');
                var listEl = container.querySelector('#manSectionList');

                selUser.addEventListener('change', function () {
                    btnRefresh.disabled = !this.value;
                    if (this.value) fetchManageSections(view, this.value);
                });

                btnRefresh.addEventListener('click', function () {
                    var uid = selUser.value;
                    if (uid) fetchManageSections(view, uid);
                });

                container.querySelector('#btnApplyManage').addEventListener('click', function () {
                    applyManageSections(view);
                });

                var manRafId = null;
                listEl.addEventListener('dragover', function (e) {
                    e.preventDefault();
                    if (manRafId) return;
                    manRafId = requestAnimationFrame(function () {
                        var draggingRow = listEl.querySelector('.man-section-row.man-dragging');
                        if (!draggingRow) { manRafId = null; return; }
                        var afterEl = getManDragAfterElement(listEl, e.clientY);
                        var ph = listEl.querySelector('.sort-placeholder');
                        if (!ph) { ph = document.createElement('div'); ph.className = 'sort-placeholder'; }
                        if (afterEl == null) { if (ph.nextElementSibling !== null) listEl.appendChild(ph); }
                        else { if (ph.nextElementSibling !== afterEl) listEl.insertBefore(ph, afterEl); }
                        manRafId = null;
                    });
                });

                listEl.addEventListener('drop', function (e) {
                    e.preventDefault();
                    var draggingRow = listEl.querySelector('.man-section-row.man-dragging');
                    var ph = listEl.querySelector('.sort-placeholder');
                    if (draggingRow && ph) {
                        listEl.insertBefore(draggingRow, ph);
                        ph.remove();
                    } else if (ph) {
                        ph.remove();
                    }
                });

                container.dataset.loaded = '1';
            })
            .catch(function () {
                container.innerHTML = '<p class="textMuted" style="padding:20px;">Failed to load users. Check server connection.</p>';
            });
    }

    function fetchManageSections(view, userId) {
        var container = view.querySelector('#hscManageContainer');
        if (!container) return;
        var listEl = container.querySelector('#manSectionList');
        if (!listEl) return;

        listEl.innerHTML = '<p class="textMuted" style="padding:10px 0;">Loading sections...</p>';
        container.querySelector('#btnApplyManage').disabled = true;

        var headers = {};
        var token = window.ApiClient.accessToken();
        if (token) headers['X-Emby-Token'] = token;

        fetch(window.ApiClient.getUrl('HomeScreenCompanion/Hsc/UserSections', { userId: userId }), { headers: headers })
            .then(function (r) { return r.json(); })
            .then(function (result) {
                currentManageSections = result.Sections || [];
                renderManageSections(view);
            })
            .catch(function () {
                listEl.innerHTML = '<p class="textMuted" style="padding:10px 0;color:var(--hsc-danger-text);">Failed to load sections.</p>';
            });
    }

    function renderManageSections(view) {
        var container = view.querySelector('#hscManageContainer');
        if (!container) return;
        var listEl = container.querySelector('#manSectionList');
        if (!listEl) return;

        if (currentManageSections.length === 0) {
            listEl.innerHTML = '<p class="textMuted" style="padding:10px 0;">No sections found for this user.</p>';
            return;
        }

        listEl.innerHTML = currentManageSections.map(function (s, i) {
            var name = s.CustomName || s.Name || s.SectionType || ('Section ' + (i + 1));
            return [
                '<div class="man-section-row" draggable="false" data-section-index="' + i + '">',
                '<span class="drag-handle"><i class="md-icon">drag_indicator</i></span>',
                '<span style="flex-grow:1;">' + name + '</span>',
                '<button type="button" is="emby-button" class="man-btn-delete raised" data-section-index="' + i + '" title="Remove section">',
                '<i class="md-icon">delete</i>',
                '</button>',
            ].join('') + '</div>';
        }).join('');

        listEl.querySelectorAll('.man-section-row').forEach(function (row) {
            var handle = row.querySelector('.drag-handle');

            handle.addEventListener('mousedown', function () { row.setAttribute('draggable', 'true'); });
            handle.addEventListener('mouseup',   function () { row.setAttribute('draggable', 'false'); });

            handle.addEventListener('touchstart', function (e) {
                e.preventDefault();
                row.classList.add('man-dragging');

                function onTouchMove(ev) {
                    ev.preventDefault();
                    var touch = ev.touches[0];
                    var afterEl = getManDragAfterElement(listEl, touch.clientY);
                    var ph = listEl.querySelector('.sort-placeholder');
                    if (!ph) { ph = document.createElement('div'); ph.className = 'sort-placeholder'; }
                    if (afterEl == null) { if (ph.nextElementSibling !== null) listEl.appendChild(ph); }
                    else { if (ph.nextElementSibling !== afterEl) listEl.insertBefore(ph, afterEl); }
                }

                function onTouchEnd() {
                    document.removeEventListener('touchmove', onTouchMove);
                    document.removeEventListener('touchend', onTouchEnd);
                    document.removeEventListener('touchcancel', onTouchCancel);
                    row.classList.remove('man-dragging');
                    var ph = listEl.querySelector('.sort-placeholder');
                    if (ph) { listEl.insertBefore(row, ph); ph.remove(); }
                    var domRows = [...listEl.querySelectorAll('.man-section-row')];
                    if (domRows.length > 0) {
                        var snapshot = currentManageSections.slice();
                        currentManageSections = domRows.map(function (r) {
                            return snapshot[parseInt(r.dataset.sectionIndex)];
                        });
                        domRows.forEach(function (r, pos) {
                            r.dataset.sectionIndex = pos;
                            var delBtn = r.querySelector('.man-btn-delete');
                            if (delBtn) delBtn.dataset.sectionIndex = pos;
                        });
                        var btnApply = container.querySelector('#btnApplyManage');
                        if (btnApply) { btnApply.disabled = false; checkFormState(); }
                    }
                }

                function onTouchCancel() {
                    document.removeEventListener('touchmove', onTouchMove);
                    document.removeEventListener('touchend', onTouchEnd);
                    document.removeEventListener('touchcancel', onTouchCancel);
                    row.classList.remove('man-dragging');
                    var ph = listEl.querySelector('.sort-placeholder');
                    if (ph) ph.remove();
                }

                document.addEventListener('touchmove', onTouchMove, { passive: false });
                document.addEventListener('touchend', onTouchEnd);
                document.addEventListener('touchcancel', onTouchCancel);
            }, { passive: false });

            row.addEventListener('dragstart', function (e) {
                row.classList.add('man-dragging');
                e.dataTransfer.effectAllowed = 'move';
                e.dataTransfer.setData('text/plain', '');
                setTimeout(function () { row.style.display = 'none'; }, 0);
            });

            row.addEventListener('dragend', function () {
                row.style.display = '';
                row.classList.remove('man-dragging');
                row.setAttribute('draggable', 'false');
                var ph = listEl.querySelector('.sort-placeholder');
                if (ph) ph.remove();

                var domRows = [...listEl.querySelectorAll('.man-section-row')];
                if (domRows.length > 0) {
                    var snapshot = currentManageSections.slice();
                    currentManageSections = domRows.map(function (r) {
                        return snapshot[parseInt(r.dataset.sectionIndex)];
                    });
                    domRows.forEach(function (r, pos) {
                        r.dataset.sectionIndex = pos;
                        var delBtn = r.querySelector('.man-btn-delete');
                        if (delBtn) delBtn.dataset.sectionIndex = pos;
                    });
                    var btnApply = container.querySelector('#btnApplyManage');
                    if (btnApply) { btnApply.disabled = false; checkFormState(); }
                }
            });

            row.querySelector('.man-btn-delete').addEventListener('click', function () {
                currentManageSections.splice(parseInt(this.dataset.sectionIndex), 1);
                renderManageSections(view);
                var btnApply = container.querySelector('#btnApplyManage');
                if (btnApply) { btnApply.disabled = false; checkFormState(); }
            });
        });
    }

    function applyManageSections(view) {
        var container = view.querySelector('#hscManageContainer');
        if (!container) return;
        var selUser = container.querySelector('#selManageUser');
        var btnApply = container.querySelector('#btnApplyManage');
        if (!selUser || !selUser.value) return;

        btnApply.disabled = true;
        var btnSpan = btnApply.querySelector('span');
        var origText = btnSpan ? btnSpan.textContent : '';
        if (btnSpan) btnSpan.textContent = 'Applying\u2026';

        var headers = { 'Content-Type': 'application/json' };
        var token = window.ApiClient.accessToken();
        if (token) headers['X-Emby-Token'] = token;

        fetch(window.ApiClient.getUrl('HomeScreenCompanion/Hsc/UserSections'), {
            method: 'POST',
            headers: headers,
            body: JSON.stringify({ UserId: selUser.value, Sections: currentManageSections })
        })
            .then(function (r) { return r.json(); })
            .then(function (result) {
                if (btnSpan) btnSpan.textContent = origText;
                if (result.Success) {
                    window.Dashboard.alert('Home screen layout saved successfully!');
                } else {
                    window.Dashboard.alert('Failed to save: ' + (result.Message || 'Unknown error'));
                    btnApply.disabled = false;
                }
            })
            .catch(function () {
                if (btnSpan) btnSpan.textContent = origText;
                window.Dashboard.alert('Error applying changes. Check server logs.');
                btnApply.disabled = false;
            });
    }


    return function (view) {
        view.addEventListener('viewshow', () => {
            _activeView = view;
            // Emby fires viewhide/viewshow when the app is paused/resumed (browser tab switch, window
            // loses focus). Don't rebuild the form then, or unsaved edits and expanded rows are lost.
            if (view._hscPaused && view.dataset.hscInit) {
                view._hscPaused = false;
                refreshStatus(view);
                startStatusPolling(view);
                return;
            }
            view._hscPaused = false;

            if (!document.getElementById('homeScreenCompanionCustomCss')) {
                document.body.insertAdjacentHTML('beforeend', customCss);
            }
            applyPluginTheme();
            // Re-check the theme whenever a popup opens: popups open from clicks, so a capture
            // listener runs just before any open handler on the HSC page.
            if (!window._hscThemeClickHook) {
                window._hscThemeClickHook = true;
                document.addEventListener('click', function () {
                    var v = activeView();
                    if (v && v.isConnected && v.offsetParent !== null) applyPluginTheme();
                }, true);
            }

            var form = view.querySelector('.HomeScreenCompanionForm');
            var isFirstVisit = !view.dataset.hscInit;
            if (isFirstVisit) view.dataset.hscInit = '1';

            originalConfigState = null;
            // Fresh users (and their rating limits) each time the page is shown.
            _hseUsersCache = null; _hseUsersPromise = null;
            view._hscLoading = true;

            var changeHandler = function(e) {
                if (view._hscLoading && e && e.target) rememberLoadEdit(view, e.target);
                setTimeout(checkFormState, 0);
            };
            if (_formAc) _formAc.abort();
            _formAc = new AbortController();
            var _signal = _formAc.signal;
            form.addEventListener('input', changeHandler, { signal: _signal });
            form.addEventListener('change', changeHandler, { signal: _signal });
            form.addEventListener('input', function (e) {
                var ta = e.target.closest('textarea.txtMiValue, textarea.txtTagBlacklist');
                if (!ta) return;
                ta.style.height = 'auto';
                ta.style.height = Math.min(ta.scrollHeight, 120) + 'px';
                ta.style.overflowY = ta.scrollHeight > 120 ? 'auto' : 'hidden';
            }, { signal: _signal });
            window.addEventListener('beforeunload', beforeUnloadHandler);
            document.addEventListener('click', closeSpeedDial);
            var settingsTab = view.querySelector('#tabSettings');
            settingsTab.addEventListener('input',  changeHandler, { signal: _signal });
            settingsTab.addEventListener('change', changeHandler, { signal: _signal });

            var spTa = view.querySelector('#txtAiSystemPrompt');
            var resetBtn = view.querySelector('#btnResetAiSystemPrompt');
            if (spTa && resetBtn) {
                spTa.addEventListener('input', function () { updateSystemPromptResetBtn(view); }, { signal: _signal });
                resetBtn.addEventListener('click', function () {
                    spTa.value = DEFAULT_AI_SYSTEM_PROMPT;
                    updateSystemPromptResetBtn(view);
                    changeHandler();
                }, { signal: _signal });
            }

            form.addEventListener('click', (e) => {
                var dayBtn = e.target.closest('.day-toggle');
                if (dayBtn) dayBtn.classList.toggle('active');
                if (e.target.closest('.btnRemoveUrl, .btnAddUrl, .btnRemoveExtraTag, .btnAddExtraTag, .btnAddOutTag, .btnRemoveLocal, .btnAddLocal, .btnRemoveDate, .btnAddDate, .btnRemoveFilterGroup, .btnAddMediaInfoFilter, .btnClearAllFilters, .btnGroupOpChoice, .btnGroupInnerOpChoice, .btnAddMiRule, .btnRemoveMiRule, .btnRemoveGroup, .day-toggle, .btnRemovePoster, .btnApplyMiPreset, .btnApplySchedulePreset')) {
                    changeHandler();
                }
            }, { signal: _signal });
            
            if (isFirstVisit) {
                var logOverlay = view.querySelector('#logModalOverlay');
                var helpOverlay = view.querySelector('#helpModalOverlay');
                var bugOverlay = view.querySelector('#bugReportModalOverlay');

                view.querySelector('#btnOpenLogs').addEventListener('click', e => { e.preventDefault(); _logTab = null; renderLogModal(view); logOverlay.classList.add('modal-visible'); });
                view.querySelector('#btnCloseLogs').addEventListener('click', () => { _logTab = null; logOverlay.classList.remove('modal-visible'); });
                view.querySelector('#btnDownloadLogs').addEventListener('click', () => showDownloadLogsModal());
                logOverlay.addEventListener('click', e => { if (e.target === logOverlay) { _logTab = null; logOverlay.classList.remove('modal-visible'); } });
                view.querySelectorAll('#logTabs .log-tab').forEach(function (tab) {
                    if (tab.id === 'btnLogIssuesOnly') { tab.addEventListener('click', function () { _logIssuesOnly = !_logIssuesOnly; renderLogModal(view); }); return; }
                    tab.addEventListener('click', function () { _logTab = tab.getAttribute('data-log'); renderLogModal(view); });
                });

                view.querySelector('#btnOpenHelp').addEventListener('click', () => helpOverlay.classList.add('modal-visible'));
                view.querySelector('#btnCloseHelp').addEventListener('click', () => helpOverlay.classList.remove('modal-visible'));
                helpOverlay.addEventListener('click', e => { if (e.target === helpOverlay) helpOverlay.classList.remove('modal-visible'); });

                // Back to top: shows once the guide is scrolled down, lifts above "Close Guide" at the very end.
                var helpScroll = helpOverlay.querySelector('.modal-body-scroll');
                var helpToTop = view.querySelector('#btnHelpBackToTop');
                // Contents: the section in view is marked (the last one whose heading has passed the top).
                var helpNavTargets = [];
                helpOverlay.querySelectorAll('nav .help-nav-link').forEach(function (link) {
                    var m = /#(help-sec-[\w-]+)/.exec(link.getAttribute('onclick') || '');
                    var sec = m && helpScroll.querySelector('#' + m[1]);
                    if (sec) helpNavTargets.push({ link: link, sec: sec });
                });
                function markHelpSection() {
                    var top = helpScroll.getBoundingClientRect().top, current = null;
                    var atEnd = helpScroll.scrollHeight - helpScroll.scrollTop - helpScroll.clientHeight < 4;
                    helpNavTargets.forEach(function (t) { if (t.sec.getBoundingClientRect().top - top <= 80) current = t; });
                    if (atEnd && helpNavTargets.length) current = helpNavTargets[helpNavTargets.length - 1];
                    helpNavTargets.forEach(function (t) { t.link.classList.toggle('active', t === current); });
                }
                helpScroll.addEventListener('scroll', () => {
                    helpToTop.classList.toggle('visible', helpScroll.scrollTop > 300);
                    helpToTop.classList.toggle('raised', helpScroll.scrollHeight - helpScroll.scrollTop - helpScroll.clientHeight < 90);
                    markHelpSection();
                });
                helpToTop.addEventListener('click', () => helpScroll.scrollTo({ top: 0, behavior: 'smooth' }));

                // Search: marks every match in the guide; Enter / the arrows step through them.
                var helpSearch = view.querySelector('#txtHelpSearch');
                var helpSearchCount = view.querySelector('#helpSearchCount');
                var helpSearchPrev = view.querySelector('#btnHelpSearchPrev');
                var helpSearchNext = view.querySelector('#btnHelpSearchNext');
                var helpHits = [], helpHitIdx = -1, helpSearchTimer = null, helpLastQuery = '';
                function clearHelpHits() {
                    helpScroll.querySelectorAll('mark.help-hit').forEach(function (m) {
                        var parent = m.parentNode;
                        parent.replaceChild(document.createTextNode(m.textContent), m);
                        parent.normalize();
                    });
                    helpHits = []; helpHitIdx = -1;
                }
                function showHelpHit(i) {
                    if (!helpHits.length) return;
                    if (helpHitIdx >= 0) helpHits[helpHitIdx].classList.remove('current');
                    helpHitIdx = (i + helpHits.length) % helpHits.length;
                    var hit = helpHits[helpHitIdx];
                    hit.classList.add('current');
                    for (var d = hit.parentElement.closest('details'); d; d = d.parentElement.closest('details')) d.open = true;
                    hit.scrollIntoView({ block: 'center', behavior: 'smooth' });
                    helpSearchCount.textContent = (helpHitIdx + 1) + ' of ' + helpHits.length;
                }
                function runHelpSearch() {
                    clearHelpHits();
                    var q = helpSearch.value.trim().toLowerCase();
                    helpLastQuery = q;
                    helpSearchCount.textContent = '';
                    if (q.length >= 2) {
                        var walker = document.createTreeWalker(helpScroll, NodeFilter.SHOW_TEXT, {
                            acceptNode: function (n) {
                                if (n.parentElement.closest('.help-search, nav')) return NodeFilter.FILTER_REJECT;
                                return n.nodeValue.toLowerCase().indexOf(q) >= 0 ? NodeFilter.FILTER_ACCEPT : NodeFilter.FILTER_REJECT;
                            }
                        });
                        var nodes = [];
                        while (walker.nextNode()) nodes.push(walker.currentNode);
                        nodes.forEach(function (node) {
                            var text = node.nodeValue, lower = text.toLowerCase(), pos = 0, at;
                            var frag = document.createDocumentFragment();
                            while ((at = lower.indexOf(q, pos)) >= 0) {
                                if (at > pos) frag.appendChild(document.createTextNode(text.slice(pos, at)));
                                var mark = document.createElement('mark');
                                mark.className = 'help-hit';
                                mark.textContent = text.slice(at, at + q.length);
                                frag.appendChild(mark);
                                helpHits.push(mark);
                                pos = at + q.length;
                            }
                            if (pos < text.length) frag.appendChild(document.createTextNode(text.slice(pos)));
                            node.parentNode.replaceChild(frag, node);
                        });
                        if (helpHits.length) showHelpHit(0); else helpSearchCount.textContent = 'No matches';
                    }
                    helpSearchPrev.disabled = helpSearchNext.disabled = helpHits.length < 2;
                }
                helpSearch.addEventListener('input', () => { clearTimeout(helpSearchTimer); helpSearchTimer = setTimeout(runHelpSearch, 250); });
                helpSearch.addEventListener('keydown', e => {
                    if (e.key === 'Enter') { e.preventDefault(); clearTimeout(helpSearchTimer); if (helpSearch.value.trim().toLowerCase() !== helpLastQuery) runHelpSearch(); else showHelpHit(helpHitIdx + (e.shiftKey ? -1 : 1)); }
                    else if (e.key === 'Escape' && helpSearch.value) { e.preventDefault(); e.stopPropagation(); helpSearch.value = ''; runHelpSearch(); }
                });
                helpSearchPrev.addEventListener('click', () => showHelpHit(helpHitIdx - 1));
                helpSearchNext.addEventListener('click', () => showHelpHit(helpHitIdx + 1));

                // Quick Start: opens only from its button and the "Quick Start" links (never by itself).
                var quickStartOverlay = view.querySelector('#quickStartModalOverlay');
                view.querySelector('#btnOpenQuickStart').addEventListener('click', () => quickStartOverlay.classList.add('modal-visible'));
                view.querySelector('#btnCloseQuickStart').addEventListener('click', () => quickStartOverlay.classList.remove('modal-visible'));
                quickStartOverlay.addEventListener('click', e => { if (e.target === quickStartOverlay) quickStartOverlay.classList.remove('modal-visible'); });
                view.addEventListener('click', e => {
                    if (!e.target.closest('.hsc-open-quickstart')) return;
                    e.preventDefault();
                    helpOverlay.classList.remove('modal-visible');
                    quickStartOverlay.classList.add('modal-visible');
                });
                quickStartOverlay.querySelectorAll('.btnAddStarter').forEach(function (btn) {
                    btn.addEventListener('click', function () {
                        btn.disabled = true;
                        addStarterSource(view, btn.getAttribute('data-starter'))
                            .then(function (row) {
                                quickStartOverlay.classList.remove('modal-visible');
                                showPageTab('TagCollection');
                                if (row) row.scrollIntoView({ behavior: 'smooth', block: 'start' });
                            })
                            .catch(function (err) { window.Dashboard.alert('Could not add the source: ' + (err.message || err)); })
                            .finally(function () { btn.disabled = false; });
                    });
                });

                view.querySelectorAll('#hscLevelSwitch button').forEach(function (b) {
                    b.addEventListener('click', function () {
                        var level = b.getAttribute('data-level');
                        if (level !== settingsLevel) setSettingsLevel(view, level, true);
                    });
                });

                view.querySelector('#btnOpenBugReport').addEventListener('click', () => bugOverlay.classList.add('modal-visible'));
                view.querySelector('#btnCloseBugReport').addEventListener('click', () => bugOverlay.classList.remove('modal-visible'));
                bugOverlay.addEventListener('click', e => { if (e.target === bugOverlay) bugOverlay.classList.remove('modal-visible'); });

                var miHelpOverlay = view.querySelector('#miHelpModalOverlay');
                view.querySelector('#btnCloseMiHelp').addEventListener('click', () => miHelpOverlay.classList.remove('modal-visible'));
                miHelpOverlay.addEventListener('click', e => { if (e.target === miHelpOverlay) miHelpOverlay.classList.remove('modal-visible'); });

                var miPreviewOverlay = view.querySelector('#miPreviewModalOverlay');
                view.querySelector('#btnCloseMiPreview').addEventListener('click', () => miPreviewOverlay.classList.remove('modal-visible'));
                miPreviewOverlay.addEventListener('click', e => { if (e.target === miPreviewOverlay) miPreviewOverlay.classList.remove('modal-visible'); });

                var tagTargetHelpOverlay = view.querySelector('#tagTargetHelpModalOverlay');
                view.querySelector('#btnCloseTagTargetHelp').addEventListener('click', () => tagTargetHelpOverlay.classList.remove('modal-visible'));
                tagTargetHelpOverlay.addEventListener('click', e => { if (e.target === tagTargetHelpOverlay) tagTargetHelpOverlay.classList.remove('modal-visible'); });

                // Header X: clicks the popup's own Close/Cancel button (by id, or by class for Customise),
                // so it does exactly what that button does.
                view.querySelectorAll('.modal-header-close').forEach(function (x) {
                    x.addEventListener('click', function () {
                        var t = x.getAttribute('data-close');
                        var box = x.closest('.modal-content');
                        var b = view.querySelector('#' + t) || box.querySelector('#' + t) || box.querySelector('.' + t);
                        if (b) b.click();
                    });
                });

                var headerAction = view.querySelector('.sectionTitleContainer');
                if (headerAction && !view.querySelector('#cbSortTags')) {
                    var savedSort = localStorage.getItem('HomeScreenCompanion_SortBy') || 'Manual';
                    headerAction.style.display = "flex";
                    headerAction.style.alignItems = "center";
                    headerAction.style.justifyContent = "space-between";
                    headerAction.style.width = "100%";
                    headerAction.style.marginBottom = "10px";

                    var controlRowHtml = `
                    <div class="control-row" style="flex-direction: row !important; align-items: center !important; flex-wrap: nowrap !important; justify-content: flex-start !important; padding: 10px 15px !important; gap: 0 !important;">

                        <span class="control-label" style="margin-right: 10px; flex-shrink: 0;">Sort:</span>

                        <select is="emby-select" id="cbSortTags" style="color:inherit; background:rgba(128,128,128,0.08); border:1px solid var(--line-color); padding:5px; border-radius:6px; font-size:0.9em; cursor:pointer; width: 80px; margin-right: 0px;">
                            <option value="Manual" ${savedSort === 'Manual' ? 'selected' : ''}>Manual</option>
                            <option value="Name" ${savedSort === 'Name' ? 'selected' : ''}>Name</option>
                            <option value="Active" ${savedSort === 'Active' ? 'selected' : ''}>Status</option>
                            <option value="LatestEdited" ${savedSort === 'LatestEdited' ? 'selected' : ''}>Latest</option>
                        </select>

                        <div style="width: 1px; height: 25px; background: var(--line-color); margin-right: 10px;"></div>

                        <span class="control-label" style="margin-right: 10px; flex-shrink: 0;"> | Filters:</span>

                        <div class="search-input-wrapper" style="width: 200px !important; margin-right: 10px; flex-shrink: 0;">
                            <i class="md-icon search-icon">search</i>
                            <input type="text" id="txtSearchTags" placeholder="Search..." autocomplete="off" style="padding-left: 28px !important; background: rgba(128,128,128,0.06) !important; border: 1px solid var(--line-color) !important; width: 100% !important;" />
                            <i class="md-icon" id="btnClearSearch">close</i>
                        </div>

                        <div class="filter-dropdown-wrapper">
                            <div class="filter-dropdown-btn" id="btnFilterDropdown">
                                <i class="md-icon" style="font-size:1.1em;">filter_list</i>
                                <span id="filterDropdownLabel">Filter</span>
                                <i class="md-icon" style="font-size:0.9em; opacity:0.6;" id="filterDropdownCaret">expand_more</i>
                            </div>
                            <div class="filter-dropdown-panel" id="filterDropdownPanel">
                                <div class="filter-dropdown-label">Features</div>
                                <label class="filter-chk-row"><input type="checkbox" id="chkFilterTag" /><span>Tag</span></label>
                                <label class="filter-chk-row"><input type="checkbox" id="chkFilterCollection" /><span>Collection</span></label>
                                <label class="filter-chk-row"><input type="checkbox" id="chkFilterSchedule" /><span>Schedule</span></label>
                                <label class="filter-chk-row"><input type="checkbox" id="chkFilterHomeScreen" /><span>Home Screen Section</span></label>
                                <div class="filter-dropdown-divider"></div>
                                <div class="filter-dropdown-label">Sources</div>
                                <label class="filter-chk-row"><input type="checkbox" id="chkFilterSrcAI" /><span>AI created lists</span></label>
                                <label class="filter-chk-row"><input type="checkbox" id="chkFilterSrcExternal" /><span>External</span></label>
                                <label class="filter-chk-row"><input type="checkbox" id="chkFilterSrcCollection" /><span>Local Collection</span></label>
                                <label class="filter-chk-row"><input type="checkbox" id="chkFilterSrcMediaInfo" /><span>Local Media Information</span></label>
                                <label class="filter-chk-row"><input type="checkbox" id="chkFilterSrcPlaylist" /><span>Local Playlist</span></label>
                                <div class="filter-dropdown-divider"></div>
                                <div class="filter-dropdown-label">Status</div>
                                <label class="filter-chk-row"><input type="checkbox" id="chkFilterActive" /><span style="display:flex;align-items:center;gap:6px;"><span style="width:8px;height:8px;border-radius:50%;background:var(--hsc-primary);flex-shrink:0;"></span>Active</span></label>
                                <label class="filter-chk-row"><input type="checkbox" id="chkFilterInactive" /><span style="display:flex;align-items:center;gap:6px;"><span style="width:8px;height:8px;border-radius:50%;background:rgba(128,128,128,0.5);flex-shrink:0;"></span>Inactive</span></label>
                                <div class="filter-dropdown-divider"></div>
                                <div class="filter-dropdown-label">Last run</div>
                                <label class="filter-chk-row"><input type="checkbox" id="chkFilterIssues" /><span style="display:flex;align-items:center;gap:6px;"><i class="md-icon" style="font-size:1.1em;color:var(--hsc-warning-text);">warning</i>Warnings &amp; errors</span></label>
                            </div>
                        </div>

                        <button type="button" id="btnIssueCounter" class="hsc-issue-counter" style="display:none;" title="Show only these groups"></button>

                    </div>`;

                    headerAction.insertAdjacentHTML('afterend', controlRowHtml);

                    const txtSearch = view.querySelector('#txtSearchTags');
                    const btnClear = view.querySelector('#btnClearSearch');

                    txtSearch.addEventListener('input', () => {
                        btnClear.style.display = txtSearch.value ? 'block' : 'none';
                        applyFilters(view);
                    });

                    btnClear.addEventListener('click', () => {
                        txtSearch.value = '';
                        btnClear.style.display = 'none';
                        txtSearch.focus();
                        applyFilters(view);
                    });

                    view.querySelector('#cbSortTags').addEventListener('change', function () {
                        localStorage.setItem('HomeScreenCompanion_SortBy', this.value);
                        sortRows(view.querySelector('#tagListContainer'), this.value);
                    });

                    ['#chkFilterTag','#chkFilterCollection','#chkFilterSchedule','#chkFilterHomeScreen',
                     '#chkFilterSrcExternal','#chkFilterSrcMediaInfo','#chkFilterSrcCollection','#chkFilterSrcPlaylist',
                     '#chkFilterSrcAI','#chkFilterActive','#chkFilterInactive','#chkFilterIssues'
                    ].forEach(id => view.querySelector(id).addEventListener('change', () => applyFilters(view)));

                    // "⚠ 7 groups need attention": toggles the Last run filter.
                    view.querySelector('#btnIssueCounter').addEventListener('click', function () {
                        var chk = view.querySelector('#chkFilterIssues');
                        chk.checked = !chk.checked;
                        applyFilters(view);
                    });
                    updateIssueCounter(view);

                    var dropBtn   = view.querySelector('#btnFilterDropdown');
                    var dropPanel = view.querySelector('#filterDropdownPanel');
                    var dropCaret = view.querySelector('#filterDropdownCaret');

                    dropBtn.addEventListener('click', e => {
                        e.stopPropagation();
                        var open = dropPanel.classList.toggle('open');
                        dropCaret.textContent = open ? 'expand_less' : 'expand_more';
                    });

                    document.addEventListener('click', function closeFilterDrop(e) {
                        if (!dropPanel.contains(e.target) && e.target !== dropBtn) {
                            dropPanel.classList.remove('open');
                            dropCaret.textContent = 'expand_more';
                        }
                    });
                }

                view.querySelector('#btnAddTag').addEventListener('click', () => {
                    renderTagGroup({ Tag: '', Urls: [{ url: '', limit: 0 }], Active: true }, view.querySelector('#tagListContainer'), true, undefined, true);
                    applyFilters(view);
                });

                view.querySelector('#btnPasteSource').addEventListener('click', () => {
                    showPasteSourceModal(view);
                });

                view.querySelector('#btnImportCollection').addEventListener('click', () => {
                    showImportCollectionsModal(view);
                });

                view.querySelector('#btnBackupConfig').addEventListener('click', function () {
                    showBackupModal();
                });

                var fileInput = view.querySelector('#fileRestoreConfig');
                view.querySelector('#btnRestoreConfigTrigger').addEventListener('click', function () {
                    if (hasDirtyState() && !confirm('You have unsaved changes. They will be discarded when a backup is restored. Continue?')) return;
                    fileInput.click();
                });
                fileInput.addEventListener('change', function (e) {
                    var file = e.target.files[0]; if (!file) return;
                    var reader = new FileReader();
                    reader.onload = function (ev) {
                        fileInput.value = '';
                        showRestoreModal(ev.target.result, function () {
                            // Server config changed underneath us — reload every tab from scratch.
                            ['#hscContainer', '#hscManageContainer', '#tlContainer', '#tcManageContainer'].forEach(function (sel) {
                                var el = view.querySelector(sel);
                                if (el) el.dataset.loaded = '';
                            });
                            loadConfig().then(function () {
                                refreshMySavedFiltersPanels();
                                var activeTab = view.querySelector('.page-tab-btn.active');
                                var target = activeTab ? activeTab.getAttribute('data-page-tab') : '';
                                if (target === 'HomeCompanion') { loadHscUsers(view); loadHscManageTab(view); }
                                else if (target === 'TopLists') loadTopListsTab(view);
                                else if (target === 'Cleanup') loadTagManageTab(view);
                            });
                        });
                    };
                    reader.readAsText(file);
                });

            }

            if (!view.querySelector('.dry-run-warning')) {
                view.insertAdjacentHTML('afterbegin', '<div class="dry-run-warning"><i class="md-icon" style="font-size:1.4em;color:var(--hsc-warning-text);"></i>DRY RUN MODE IS ACTIVE - NO CHANGES WILL BE SAVED</div>');
            }

            var btnSave = view.querySelector('.btn-save');
            if (btnSave) { btnSave.disabled = true; btnSave.style.opacity = "0.5"; }

            checkForUpdates(view);
            refreshStatus(view);
            startStatusPolling(view);
            getHseUsers().then(function(users) { _miUsers = users; });

            // A failed lookup must not stop the settings from loading: carry on with an empty list.
            Promise.all([
                window.ApiClient.getJSON(window.ApiClient.getUrl("Users/" + window.ApiClient.getCurrentUserId() + "/Items", { IncludeItemTypes: "BoxSet", Recursive: true })).catch(function () { return { Items: [] }; }),
                window.ApiClient.getJSON(window.ApiClient.getUrl("Items", { IncludeItemTypes: "Playlist", Recursive: true })).catch(function () { return { Items: [] }; }),
                window.ApiClient.getJSON(window.ApiClient.getUrl("Items/Filters2", { UserId: window.ApiClient.getCurrentUserId(), Recursive: true })).catch(function () { return { Tags: [] }; })
            ]).then(responses => {
                cachedCollections = (responses[0].Items || []).slice().sort(byName);
                cachedPlaylists = (responses[1].Items || []).slice().sort(byName);
                cachedTags = ((responses[2] && responses[2].Tags) || []).slice().sort(natCmp);

                // Only the copy of the page being shown picks up what was left behind on leaving.
                loadConfig({ leaveState: readLeaveState() || { ui: null } });
            });
        });

        view.addEventListener('viewhide', (e) => {
            stopStatusPolling();
            // Shown from document.body (see bodyOverlay), so they do not leave with the page.
            Object.keys(view._bodyOverlays || {}).forEach(function (id) { view._bodyOverlays[id].classList.remove('modal-visible'); });
            // App pause hides the current view without a new one; real navigation always has newViewInfo.
            view._hscPaused = !!(e.detail && e.detail.newViewInfo === null);
        });

        // ---- Unsaved changes survive leaving the page ----
        // Emby either re-shows this copy (Back) or builds a brand-new copy (sidebar/hash), and
        // either way loadConfig() re-renders from the server. So on leaving, this copy writes its
        // unsaved settings (if any) to sessionStorage; the copy that is shown next reads them back.
        // The page itself always opens on its first tab, at the top. Each copy writes only on its
        // own viewhide, and only while it is the copy on screen, so a hidden old copy can never
        // overwrite a newer state.
        function leaveStateKey() {
            var sid = '', uid = '';
            try { sid = window.ApiClient.serverId ? window.ApiClient.serverId() : ''; } catch (e) { }
            try { uid = window.ApiClient.getCurrentUserId(); } catch (e) { }
            return 'hsc-draft-' + sid + '-' + uid;
        }
        function readLeaveState() {
            try { var s = sessionStorage.getItem(leaveStateKey()); return s ? JSON.parse(s) : null; } catch (e) { return null; }
        }
        function writeLeaveState(obj) {
            try {
                if (obj) sessionStorage.setItem(leaveStateKey(), JSON.stringify(obj));
                else sessionStorage.removeItem(leaveStateKey());
            } catch (e) { }
        }
        // API keys, client ids, tokens and passwords never go into sessionStorage: they are left
        // out of the stored draft (and the server stamp), so a restored draft keeps the server's
        // values for them. (An unsaved edit of a key itself is therefore not restored.)
        function stripSecrets(v) {
            if (Array.isArray(v)) return v.map(stripSecrets);
            if (!v || typeof v !== 'object') return v;
            var out = {};
            Object.keys(v).forEach(function (k) { if (!/(apikey|clientid|secret|token|password)$/i.test(k)) out[k] = stripSecrets(v[k]); });
            return out;
        }
        // What the server sent, minus the parts the sync tasks rewrite on their own (Save re-reads
        // those from the server anyway), so a background sync does not count as "changed".
        function serverStamp(config) { return hscServerStamp(config); }
        // Save: is the server still as this page loaded it? If not, nothing is saved and the user
        // chooses between reloading (losing their edits here) and going on editing.
        function serverUnchangedSinceLoad() {
            return window.ApiClient.getPluginConfiguration(pluginId).then(function (c) {
                if (!view._hscServerStamp || serverStamp(c) === view._hscServerStamp) return true;
                showSaveConflictDialog(function () { writeLeaveState(null); hideDraftBar(); loadConfig(); });
                return false;
            }, function () { return true; });
        }
        function rowKey(row) {
            var lbl = row.querySelector('.txtEntryLabel'), tn = row.querySelector('.txtTagName');
            var name = lbl ? lbl.value : '';
            var tag = (tn && tn.value) || name;
            return name ? name + '\x1F' + tag : tag;
        }
        function captureUi() {
            var rows = [];
            view.querySelectorAll('#tagListContainer .tag-row').forEach(function (row, i) {
                var body = row.querySelector('.tag-body');
                if (!body || body.style.display === 'none') return;
                rows.push({ key: rowKey(row), index: i, expanded: true, tab: row.dataset.activeTab || 'general' });
            });
            return { rows: rows };
        }
        function applyUiRows(ui) {
            if (!ui || !ui.rows || !ui.rows.length) return;
            var rows = Array.from(view.querySelectorAll('#tagListContainer .tag-row'));
            var keys = rows.map(rowKey);
            var used = new Set();
            ui.rows.forEach(function (r) {
                // Rows are matched by name; a new, still unnamed source by its place in the list.
                var i = (!r.key && keys[r.index] === '' && !used.has(r.index)) ? r.index
                    : keys.findIndex(function (k, j) { return k === r.key && !used.has(j); });
                if (i < 0 || !r.expanded) return;
                used.add(i);
                var row = rows[i];
                var body = row.querySelector('.tag-body'), icon = row.querySelector('.expand-icon');
                if (body && body.style.display === 'none') {
                    body.style.display = 'block';
                    if (icon) icon.innerText = 'expand_less';
                }
                // Clicking the tab runs its lazy set-up (Home Screen, Playlist) like a user click.
                var tab = row.querySelector('.tag-tab[data-tab="' + (r.tab || 'general') + '"]');
                if (tab) tab.click();
            });
        }
        function showDraftBar(serverChanged) {
            var bar = view.querySelector('.hsc-draft-bar');
            if (!bar) {
                bar = document.createElement('div');
                bar.className = 'hsc-draft-bar';
                bar.style.cssText = 'max-width:900px;box-sizing:border-box;display:flex;align-items:center;gap:12px;flex-wrap:wrap;padding:10px 14px;margin-bottom:16px;border-radius:8px;background:rgba(82,181,75,0.08);border:1px solid rgba(82,181,75,0.45);';
                bar.innerHTML = '<i class="md-icon" style="font-size:1.3em;">restore</i><span class="hsc-draft-text" style="flex:1;min-width:200px;"></span>' +
                    '<button type="button" is="emby-button" class="raised button-cancel hsc-draft-show" style="margin:0;"><span>Show changes</span></button>' +
                    '<button type="button" is="emby-button" class="raised button-cancel hsc-draft-discard" style="margin:0;"><span>Discard</span></button>';
                // Above every page tab, since restored settings can live on any of them.
                var firstTab = view.querySelector('.page-tab-content');
                firstTab.parentNode.insertBefore(bar, firstTab);
                bar.querySelector('.hsc-draft-show').addEventListener('click', showDraftChanges);
                bar.querySelector('.hsc-draft-discard').addEventListener('click', discardDraft);
            }
            bar.style.background = serverChanged ? 'rgba(230,193,90,0.08)' : 'rgba(82,181,75,0.08)';
            bar.style.borderColor = serverChanged ? 'rgba(230,193,90,0.45)' : 'rgba(82,181,75,0.45)';
            bar.querySelector('.hsc-draft-text').textContent = serverChanged
                ? 'Unsaved changes restored. Settings changed on the server while you were away — Save will not overwrite them; it offers to reload the current settings instead.'
                : 'Unsaved changes restored.';
            bar.style.display = 'flex';
        }
        function hideDraftBar() {
            var bar = view.querySelector('.hsc-draft-bar');
            if (bar) bar.style.display = 'none';
        }
        function discardDraft() {
            writeLeaveState(null);
            hideDraftBar();
            loadConfig();
        }
        // Show changes: what is unsaved right now compared with the saved settings.
        function showDraftChanges() {
            var ch = null;
            try { ch = describeUnsavedChanges(JSON.parse(originalConfigState), getUiConfig(view, true)); } catch (e) { }
            var item = function (main, sub, deleted) {
                return '<li style="padding:6px 0;border-bottom:1px solid var(--line-color,rgba(255,255,255,0.08));">' +
                    '<span style="font-weight:600;' + (deleted ? 'text-decoration:line-through;opacity:0.8;' : '') + '">' + escapeHtml(main) + '</span>' +
                    (sub ? ' <span style="opacity:0.65;font-size:0.9em;">— ' + escapeHtml(sub) + '</span>' : '') + '</li>';
            };
            var list = '';
            if (ch && ch.sources.length) {
                list += '<div style="font-size:0.8em;text-transform:uppercase;letter-spacing:0.5px;opacity:0.6;margin:4px 0 2px;">Sources</div>' +
                    '<ul style="list-style:none;margin:0 0 12px;padding:0;">' +
                    ch.sources.map(function (s) { return item(s.name, s.what, s.deleted); }).join('') + '</ul>';
            }
            if (ch && ch.settings.length) {
                list += '<div style="font-size:0.8em;text-transform:uppercase;letter-spacing:0.5px;opacity:0.6;margin:4px 0 2px;">Settings</div>' +
                    '<ul style="list-style:none;margin:0 0 12px;padding:0;">' +
                    ch.settings.map(function (s) { return item(s); }).join('') + '</ul>';
            }
            if (!list) list = '<p style="' + _backupHintStyle + '">Nothing differs from the saved settings any more.</p>';
            var modal = buildBackupModalShell();
            modal.renderBox(
                '<h3 style="' + _backupTitleStyle + '">Unsaved changes</h3>' +
                '<p style="' + _backupHintStyle + '">These changes are not saved yet. <strong>Save and apply</strong> keeps them; <strong>Discard</strong> puts everything back as last saved.</p>' +
                list +
                '<div style="display:flex;gap:10px;justify-content:flex-end;align-items:center;flex-wrap:wrap;margin-top:6px;">' +
                '<button type="button" class="btnDraftClose hsc-mbtn hsc-btn-secondary">Close</button>' +
                '<button type="button" class="btnDraftDiscard hsc-mbtn hsc-btn-danger"><i class="md-icon" style="font-size:1em;vertical-align:middle;margin-right:6px;">undo</i>Discard</button>' +
                '<button type="button" class="btnDraftSave hsc-mbtn hsc-btn-primary"><i class="md-icon" style="font-size:1em;vertical-align:middle;margin-right:6px;">save</i>Save and apply</button>' +
                '</div>'
            );
            modal.querySelector('.btnDraftClose').addEventListener('click', modal.close);
            modal.querySelector('.btnDraftDiscard').addEventListener('click', function () { modal.close(); discardDraft(); });
            // Same as the page's Save Settings button (disabled with it, e.g. while a run is going).
            var pageSave = view.querySelector('.btn-save');
            var popupSave = modal.querySelector('.btnDraftSave');
            if (!pageSave || pageSave.disabled) { popupSave.disabled = true; popupSave.style.opacity = '0.5'; }
            popupSave.addEventListener('click', function () { modal.close(); pageSave.click(); });
        }

        view.addEventListener('viewhide', function () {
            // App pause: the page stays as it is (viewshow returns early), so its listeners stay too.
            if (view._hscPaused) return;
            document.removeEventListener('click', closeSpeedDial);
            window.removeEventListener('beforeunload', beforeUnloadHandler);
            // Another copy of the page is already on screen, or this one never finished loading
            // (then whatever was stored is still there, untouched).
            if (_activeView !== view || view._hscLoading || !originalConfigState) return;
            var current = null, dirty = false;
            try {
                current = getUiConfig(view, true);
                dirty = JSON.stringify(current) !== originalConfigState;
            } catch (e) { }
            writeLeaveState(dirty ? { draft: stripSecrets(current), serverStamp: view._hscServerStamp || '', serverUsers: view._hscLoadedUsers || null } : null);
        });

        // Save submits this copy's own form. (A form="…" attribute would submit the first form with
        // that id in the document, which can be an old hidden copy of this page.)
        view.querySelector('.btn-save').addEventListener('click', e => {
            e.preventDefault();
            var f = view.querySelector('.HomeScreenCompanionForm');
            if (f.requestSubmit) f.requestSubmit();
            else f.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }));
        });

        view.querySelector('.HomeScreenCompanionForm').addEventListener('submit', e => {
            e.preventDefault();

            // A home row with nothing to show (no tag, collection, playlist or top-list) is not saved.
            var _hseEmpty = Array.from(view.querySelectorAll('#tagListContainer .tag-row')).filter(function (r) {
                return r.querySelector('.selSourceType') && rowChangedHere(r) && !!(r.querySelector('.chkEnableHomeSection') || {}).checked && !hseAllowed(r);
            });
            if (_hseEmpty.length > 0) {
                var _emptyNames = _hseEmpty.map(function (r) {
                    return ((r.querySelector('.txtEntryLabel') || {}).value || (r.querySelector('.txtTagName') || {}).value || 'New source').trim();
                });
                openSourceRowTab(view, _hseEmpty[0], 'homescreen');
                window.Dashboard.alert('Not saved: the home screen section of ' + _emptyNames.join(', ') + ' has nothing to show. Tick Apply Tag, Create Collection, Create Playlist or Create as top-list, or untick Add as home screen section.');
                return;
            }

            // A top-list needs its users before it can be saved (it can be ticked without them).
            var _tlNoUsers = topListsWithoutUsers(view);
            if (_tlNoUsers.length > 0) {
                var _names = _tlNoUsers.map(function (r) {
                    return ((r.querySelector('.txtEntryLabel') || {}).value || (r.querySelector('.txtTagName') || {}).value || 'New source').trim();
                });
                openSourceRowTab(view, _tlNoUsers[0], 'homescreen');
                window.Dashboard.alert('Not saved: pick at least one user for the top-list under Target Users in the Home Screen tab of ' + _names.join(', ') + '. They get the top-list row.');
                return;
            }

            // First: were the settings changed elsewhere since this page loaded? Then the save
            // below is replayed (synchronously, flagged) once the server says it is unchanged.
            if (!view._hscStampChecked) {
                var formEl = e.currentTarget || view.querySelector('.HomeScreenCompanionForm');
                serverUnchangedSinceLoad().then(function (ok) {
                    if (!ok) return;
                    view._hscStampChecked = true;
                    try { formEl.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true })); }
                    finally { view._hscStampChecked = false; }
                });
                return;
            }

            // Flush dirty inline top-list forms before the global config save.
            // Must happen first because the global save re-fetches TopLists from the server.
            var _tlCont = view.querySelector('#tlContainer');
            var _dirtyBodies = _tlCont ? Array.from(_tlCont.querySelectorAll('.tag-body[data-dirty="1"]')) : [];
            if (_dirtyBodies.length > 0) {
                var _btn = view.querySelector('.btn-save');
                var _origHtml = _btn ? _btn.innerHTML : '';
                if (_btn) { _btn.disabled = true; _btn.innerHTML = '<i class="md-icon" style="margin-right:5px;">hourglass_empty</i><span>Saving…</span>'; }
                _dirtyBodies.reduce(function (p, b) {
                    return p.then(function () { return typeof b.tlSaveForm === 'function' ? b.tlSaveForm() : Promise.resolve(); });
                }, Promise.resolve()).then(function () {
                    _dirtyBodies.forEach(function (b) {
                        delete b.dataset.dirty;
                        if (typeof b.tlResetBaseline === 'function') b.tlResetBaseline();
                    });
                    if (_btn) { _btn.innerHTML = _origHtml; _btn.disabled = false; }
                    doSave();
                }).catch(function (err) {
                    if (_btn) { _btn.innerHTML = _origHtml; _btn.disabled = false; }
                    alert('Failed to save top-list changes: ' + (err.message || err));
                });
                return;
            }
            doSave();
            function doSave() {

            // If CLEANUP tab is active and has pending deletions, show its modal instead of normal config save
            var cleanupTab = view.querySelector('#tabCleanup');
            var tcContainer = view.querySelector('#tcManageContainer');
            if (cleanupTab && cleanupTab.style.display !== 'none' && tcContainer && tcContainer._tcHasPending && tcContainer._tcShowModal) {
                tcContainer._tcShowModal();
                return;
            }

            var btnApplyManage = view.querySelector('#btnApplyManage');
            if (btnApplyManage && !btnApplyManage.disabled) applyManageSections(view);

            var configObj = getUiConfig(view, false);
            
            // LastModified moves only on the sources the user changed: each card as it is now is
            // compared with the card as loaded — both grouped and read the same way (comparing a
            // flat save entry with a grouped card always differed, so one Save touched every source).
            var originalConf = JSON.parse(originalConfigState);
            var originalTags = groupConfigTags(originalConf.Tags);
            var currentTags = groupConfigTags(getUiConfig(view, true).Tags);
            var sameCard = function (a, b) {
                return !!a && !!b && JSON.stringify(Object.assign({}, a, { LastModified: '' })) === JSON.stringify(Object.assign({}, b, { LastModified: '' }));
            };

            configObj.Tags.forEach(tag => {
                var key = tag.Name ? tag.Name + '\x1F' + tag.Tag : tag.Tag;
                var originalTag = originalTags[key];
                if (originalTag && sameCard(currentTags[key], originalTag)) tag.LastModified = originalTag.LastModified;
                else tag.LastModified = new Date().toISOString();
            });

            // Pre-fetch current config to preserve HomeSectionTracked set by the sync task
            // (UI dataset may be stale if sync ran after page load)
            window.ApiClient.getPluginConfiguration(pluginId).catch(function() { return { Tags: [] }; }).then(function(currentConfig) {
                var currentGrouped = groupConfigTags(currentConfig.Tags);
                configObj.Tags.forEach(function(t) {
                    var key = t.Name ? t.Name + '\x1F' + t.Tag : t.Tag;
                    var existing = currentGrouped[key];
                    // Users put on or taken off on the server since the page loaded (Copy & Sync,
                    // another tab) are kept, as are the user changes made here.
                    var loadedUsers = (view._hscLoadedUsers || {})[key];
                    if (existing && loadedUsers) {
                        t.HomeSectionUserIds = mergeUserIds(t.HomeSectionUserIds, loadedUsers.Home, existing.HomeSectionUserIds);
                        t.PlaylistUserIds = mergeUserIds(t.PlaylistUserIds, loadedUsers.Playlist, existing.PlaylistUserIds);
                    }
                    if (existing && existing.HomeSectionTracked && existing.HomeSectionTracked.length > 0
                        && (!t.HomeSectionTracked || t.HomeSectionTracked.length === 0)) {
                        t.HomeSectionTracked = existing.HomeSectionTracked;
                    }
                    // Set by a run since the page loaded: keep it (only the sync sets these).
                    if (existing && existing.CollectionImported && t.CollectionImported) {
                        t.CollectionPosterReplaced = !!(t.CollectionPosterReplaced || existing.CollectionPosterReplaced);
                        t.CollectionBackgroundReplaced = !!(t.CollectionBackgroundReplaced || existing.CollectionBackgroundReplaced);
                        t.CollectionThumbReplaced = !!(t.CollectionThumbReplaced || existing.CollectionThumbReplaced);
                        t.CollectionBannerReplaced = !!(t.CollectionBannerReplaced || existing.CollectionBannerReplaced);
                    }
                    if (existing && existing.PlaylistMappings && existing.PlaylistMappings.length > 0
                        && (!t.PlaylistMappings || t.PlaylistMappings.length === 0)) {
                        t.PlaylistMappings = existing.PlaylistMappings;
                    }
                    // The AI's last-run date is written by the runs, never by this page: keep it, so
                    // "Refresh every … days" is not restarted by every Save. Only an AI source whose
                    // question changed asks again on the next sync.
                    var rawAi = t.SourceType === 'AI' ? (currentConfig.Tags || []).find(function (x) {
                        return x.SourceType === 'AI' && (x.Name ? x.Name + '\x1F' + x.Tag : x.Tag) === key;
                    }) : null;
                    if (rawAi && rawAi.AiLastRunDate && !t.AiLastRunDate
                        && (rawAi.AiProvider || 'OpenAI') === (t.AiProvider || 'OpenAI') && (rawAi.AiPrompt || '') === (t.AiPrompt || '')
                        && (rawAi.Limit || 0) === (t.Limit || 0) && !!rawAi.AiIncludeRecentlyWatched === !!t.AiIncludeRecentlyWatched
                        && (rawAi.AiRecentlyWatchedUserId || '') === (t.AiRecentlyWatchedUserId || '')
                        && (rawAi.AiRecentlyWatchedCount || 20) === (t.AiRecentlyWatchedCount || 20)) {
                        t.AiLastRunDate = rawAi.AiLastRunDate;
                    }
                });
                configObj.TopLists = currentConfig.TopLists || [];

                // Ensure all items-type home sections exclude top-list libraries automatically
                return preFetchLibraryData().then(function(libData) {
                    var topListIds = new Set(
                        libData.virtualFolders
                            .filter(function(f) {
                                return (f.Locations || []).some(function(loc) {
                                    var parts = loc.replace(/\\/g, '/').split('/');
                                    var fn = (parts[parts.length - 1] || parts[parts.length - 2] || '').toLowerCase();
                                    return libData.topListFolderNames.has(fn);
                                });
                            })
                            .map(function(f) { return f.ItemId; })
                    );
                    if (topListIds.size > 0) {
                        configObj.Tags.forEach(function(tag) {
                            if (!tag.EnableHomeSection) return;
                            var settings = {};
                            try { settings = JSON.parse(tag.HomeSectionSettings || '{}'); } catch {}
                            if ((settings.SectionType || 'items') !== 'items') return;
                            var excluded = new Set(
                                (settings._queryExcludeViewIds || '').split(',')
                                    .map(function(id) { return id.trim(); }).filter(Boolean)
                            );
                            var changed = false;
                            topListIds.forEach(function(id) {
                                if (!excluded.has(id)) { excluded.add(id); changed = true; }
                            });
                            if (changed) {
                                var excStr = Array.from(excluded).join(',');
                                settings._queryExcludeViewIds = excStr;
                                settings.ExcludedFolders = excStr;
                                tag.HomeSectionSettings = JSON.stringify(settings);
                            }
                        });
                    }
                    return window.ApiClient.updatePluginConfiguration(pluginId, configObj);
                });
            }).then(r => {
                window.Dashboard.processPluginConfigurationUpdateResult(r);
                _topListTagNames = new Set((configObj.TopLists || []).map(function (tl) { return (tl.TagName || '').toLowerCase(); }).filter(Boolean));
                noteTopListTypes(configObj.TopLists);

                var newGrouped = groupConfigTags(configObj.Tags);
                view.querySelectorAll('.tag-row').forEach(row => {
                    if (!row.querySelector('.txtEntryLabel')) return;
                    var name = row.querySelector('.txtEntryLabel').value;
                    var tagName = row.querySelector('.txtTagName').value || name;
                    var key = name ? name + '\x1F' + tagName : tagName;
                    var tc = newGrouped[key];
                    if (tc) {
                        row.dataset.lastModified = tc.LastModified;
                        var hseTab = row.querySelector('.homescreen-tab');
                        if (hseTab) {
                            hseTab.dataset.hseTracked = encodeURIComponent(JSON.stringify(tc.HomeSectionTracked || []));
                            hseTab.dataset.hseSettings = encodeURIComponent(tc.HomeSectionSettings || '{}');
                            hseTab.dataset.hseUserids = encodeURIComponent(JSON.stringify(tc.HomeSectionUserIds || []));
                        }
                        // What the server now has for Create as top-list (its notes compare with it).
                        row.dataset.tlSaved = tc.EnableTopList ? '1' : '0';
                        row.dataset.tlSavedTag = (tc.Tag || '').trim().toLowerCase();
                        updateTopListTab(row);
                    }
                });

                originalConfigState = JSON.stringify(getUiConfig(view, true));
                checkFormState();
                updateDryRunWarning();
                writeLeaveState(null);
                hideDraftBar();
                // The server now holds what was just saved: that is the new "as fetched" state.
                window.ApiClient.getPluginConfiguration(pluginId).then(function (c) { view._hscServerStamp = serverStamp(c); view._hscLoadedUsers = sourceUserLists(c); }).catch(function () { });

                // Apply home section settings immediately for existing tracked sections (fire-and-forget)
                configObj.Tags.forEach(function(tc) {
                    if (!tc.EnableHomeSection) return;
                    var hasTracked = (tc.HomeSectionTracked || []).some(function(t) {
                        return t.SectionId && !t.SectionId.startsWith('hsc__');
                    });
                    if (!hasTracked) return;
                    var applyUrl = window.ApiClient.getUrl('HomeScreenCompanion/Hsc/ApplyTagHomeSections');
                    var tok = window.ApiClient.accessToken ? window.ApiClient.accessToken() : '';
                    fetch(applyUrl, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json', 'X-Emby-Token': tok },
                        body: JSON.stringify({ TagName: tc.Name || tc.Tag })
                    }).then(function(r) { return r.json(); }).then(function(res) {
                        console.log('[HSC] ApplyTagHomeSections', tc.Name || tc.Tag, res);
                    }).catch(function(e) { console.warn('[HSC] ApplyTagHomeSections failed', tc.Name || tc.Tag, e); });
                });
                // Top-lists of sources: a changed title, position or look reaches their rows now
                // (fire-and-forget; a list without a library yet gets its rows from the next run).
                var tlDone = {};
                configObj.Tags.forEach(function (tc) {
                    var tag = (tc.Tag || '').trim();
                    if (!tc.EnableTopList || !tag || tlDone[tag.toLowerCase()]) return;
                    tlDone[tag.toLowerCase()] = 1;
                    var tok = window.ApiClient.accessToken ? window.ApiClient.accessToken() : '';
                    fetch(window.ApiClient.getUrl('HomeScreenCompanion/TopList/SyncHomeSections'), {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json', 'X-Emby-Token': tok },
                        body: JSON.stringify({ TagName: tag })
                    }).then(function (r) { return r.json(); }).then(function (res) {
                        console.log('[HSC] Top-list rows', tag, res);
                    }).catch(function (e) { console.warn('[HSC] Top-list rows failed', tag, e); });
                });
            });
            } // end doSave
        });

        var speedDial = view.querySelector('#runSpeedDial');
        var syncMenu = view.querySelector('#runSyncMenu');
        view.querySelector('#btnRunSync').addEventListener('click', function (e) {
            e.stopPropagation();
            var isOpen = speedDial.classList.toggle('open');
            syncMenu.classList.toggle('open', isOpen);
        });

        // Other: opens the less used runs above it, inside the open menu.
        var otherBtn = view.querySelector('#btnDialOther'), otherGroup = view.querySelector('#runDialOther');
        function setOtherOpen(open) {
            otherGroup.classList.toggle('open', open);
            otherBtn.setAttribute('aria-expanded', open ? 'true' : 'false');
        }
        otherBtn.addEventListener('click', function (e) {
            e.stopPropagation(); // a click outside the menu closes it
            setOtherOpen(!otherGroup.classList.contains('open'));
        });

        function closeSpeedDial() {
            speedDial.classList.remove('open');
            syncMenu.classList.remove('open');
            setOtherOpen(false);
        }
        // Starts one of HSC's tasks through the plugin, so the tasks hidden from Emby's Scheduled
        // Tasks (which Emby's own task list may leave out) can be run here too.
        function startHscTask(key) {
            var headers = { 'Content-Type': 'application/json' };
            var token = window.ApiClient.accessToken && window.ApiClient.accessToken();
            if (token) headers['X-Emby-Token'] = token;
            return fetch(window.ApiClient.getUrl('HomeScreenCompanion/RunTask'), {
                method: 'POST', headers: headers, body: JSON.stringify({ Key: key })
            }).then(function (r) { return r.json(); });
        }

        function runTask(key, label) {
            startHscTask(key).then(function (res) {
                window.Dashboard.alert(res && res.Success ? (res.Message || label + ' started!') : 'Could not start ' + label + (res && res.Message ? ': ' + res.Message : ''));
            }).catch(function (e) { window.Dashboard.alert('Could not start ' + label + ': ' + e.message); });
            closeSpeedDial();
        }

        view.querySelector('#btnDialTagsCollections').addEventListener('click', function () {
            runTask('HomeScreenCompanionSyncTask', 'Tag sync');
        });

        view.querySelector('#btnDialHomeScreen').addEventListener('click', function () {
            runTask('HomeSectionSyncTask', 'Home screen sync');
        });

        view.querySelector('#btnDialTopListRows').addEventListener('click', function () {
            runTask('TopListSyncTask', 'Top-list row sync');
        });

        view.querySelector('#btnDialMergeVersions').addEventListener('click', function () {
            runTask('MergeTopListVersionsTask', 'Merge top-list versions');
        });

        view.querySelector('#btnDialContinueWatching').addEventListener('click', function () {
            runTask('ContinueWatchingBumpTask', 'Continue Watching bump');
        });

        view.querySelector('#btnDialFullSync').addEventListener('click', function () {
            Promise.all([startHscTask('HomeScreenCompanionSyncTask'), startHscTask('HomeSectionSyncTask')])
                .then(function (res) {
                    var failed = res.filter(function (r) { return !r || !r.Success; });
                    window.Dashboard.alert(failed.length ? 'Could not start the full sync: ' + failed.map(function (r) { return (r && r.Message) || 'unknown error'; }).join('; ') : 'Full sync started!');
                })
                .catch(function (e) { window.Dashboard.alert('Could not start the full sync: ' + e.message); });
            closeSpeedDial();
        });

        function beforeUnloadHandler(e) {
            if (hasDirtyState()) {
                e.preventDefault();
                e.returnValue = '';
            }
        }
        // Added on every viewshow and removed on every viewhide (see the leave-state handler).

        // Fetches the settings and shows them. opts.leaveState (only from viewshow) is what this page
        // left in sessionStorage: unsaved settings to put back on top. Coming back to the page
        // always starts on the first tab, at the top, with every row closed; every other caller
        // keeps the rows that are open right now.
        function loadConfig(opts) {
            opts = opts || {};
            var leave = opts.leaveState || null;
            var ui = leave ? null : captureUi();
            _hseLibraryCachePromise = null; // återställ cache så nya bibliotek (t.ex. ny top-list) hämtas
            preFetchLibraryData();          // starta hämtning direkt vid sidladdning
            view._hscLoading = true;
            return window.ApiClient.getPluginConfiguration(pluginId).then(config => {
                var fetchedStamp = serverStamp(config);
                renderConfig(view, config);
                return new Promise(function (resolve) { requestAnimationFrame(resolve); }).then(function () {
                    try {
                        originalConfigState = JSON.stringify(getUiConfig(view, true));
                    } catch (err) {
                        originalConfigState = null;
                    }
                    view._hscServerStamp = fetchedStamp;
                    view._hscLoadedUsers = sourceUserLists(config);
                    var restored = false, serverChanged = false;
                    if (leave && leave.draft) {
                        // The draft holds no keys: a connection still on the server gets its key back.
                        if (Array.isArray(leave.draft.AiConnections)) leave.draft.AiConnections.forEach(function (c) {
                            var s = (config.AiConnections || []).filter(function (x) { return x.Id === c.Id; })[0];
                            if (c.ApiKey === undefined) c.ApiKey = s ? (s.ApiKey || '') : '';
                        });
                        renderConfig(view, Object.assign({}, config, leave.draft));
                        restored = true;
                        serverChanged = !!leave.serverStamp && leave.serverStamp !== fetchedStamp;
                        // Keep the stamp the draft was based on, so the warning stays until Save/Discard.
                        if (leave.serverStamp) view._hscServerStamp = leave.serverStamp;
                        if (leave.serverUsers) view._hscLoadedUsers = leave.serverUsers;
                    }
                    view._hscLoading = false;
                    replayLoadEdits(view);
                    checkFormState();
                    updateDryRunWarning();
                    if (restored && JSON.stringify(getUiConfig(view, true)) !== originalConfigState) showDraftBar(serverChanged);
                    else { hideDraftBar(); view._hscServerStamp = fetchedStamp; }
                    if (leave) writeLeaveState(null); // picked up; written again on the next viewhide
                    if (leave) {
                        // Back on the page (also via Back, which re-shows this same copy as it was).
                        showPageTab('TagCollection');
                        var st = view.querySelector('.hsc-sub-tab-btn.active');
                        if (st && st.getAttribute('data-hsc-tab') !== 'manage') showSubTab('manage');
                        pageScroller(view).scrollTop = 0;
                    }
                    else if (ui) applyUiRows(ui);
                });
            }).catch(function () { view._hscLoading = false; });
        }

        // Clears and rebuilds every settings field and source row from a config object (the
        // server's, or the server's with an unsaved draft on top).
        function renderConfig(view, config) {
                lastHscConfig = {
                    HomeSyncEnabled:       config.HomeSyncEnabled       || false,
                    HomeSyncProfiles:      effectiveHscProfiles(config),
                    ContinueWatchingBumpEnabled:  config.ContinueWatchingBumpEnabled  || false,
                    ContinueWatchingBumpMode:     config.ContinueWatchingBumpMode     || 'AllEpisodes',
                    ContinueWatchingBumpAllUsers: config.ContinueWatchingBumpAllUsers || false,
                    ContinueWatchingBumpUserIds:  config.ContinueWatchingBumpUserIds  || []
                };
                savedFilters = config.SavedFilters || [];
                setSettingsLevel(view, config.SettingsLevel || initialSettingsLevel(config), false);

                var container = view.querySelector('#tagListContainer'); container.innerHTML = '';
                view.querySelector('#txtTraktClientId').value = config.TraktClientId || '';
                view.querySelector('#txtMdblistApiKey').value = config.MdblistApiKey || '';
                view.querySelector('#txtTmdbApiKey').value = config.TmdbApiKey || '';
                var oaElInit = view.querySelector('#txtOpenAiApiKey'); if (oaElInit) oaElInit.value = config.OpenAiApiKey || '';
                var oamElInit = view.querySelector('#txtOpenAiModel'); if (oamElInit) oamElInit.value = config.OpenAiModel || 'gpt-4o-mini';
                var gmElInit = view.querySelector('#txtGeminiApiKey'); if (gmElInit) gmElInit.value = config.GeminiApiKey || '';
                var gmmElInit = view.querySelector('#txtGeminiModel'); if (gmmElInit) gmmElInit.value = config.GeminiModel || 'gemini-2.5-flash-lite';
                var clElInit = view.querySelector('#txtClaudeApiKey'); if (clElInit) clElInit.value = config.ClaudeApiKey || '';
                var clmElInit = view.querySelector('#txtClaudeModel'); if (clmElInit) clmElInit.value = config.ClaudeModel || 'claude-haiku-4-5-20251001';
                var olElInit = view.querySelector('#txtOllamaBaseUrl'); if (olElInit) olElInit.value = config.OllamaBaseUrl || 'http://localhost:11434';
                var omElInit = view.querySelector('#txtOllamaModel'); if (omElInit) omElInit.value = config.OllamaModel || '';
                renderAiConnections(view, config.AiConnections || []);
                syncModelFields(view);
                var spElInit = view.querySelector('#txtAiSystemPrompt'); if (spElInit) spElInit.value = config.AiSystemPrompt || ''; updateSystemPromptResetBtn(view);
                view.querySelector('#chkExtendedConsoleOutput').checked = config.ExtendedConsoleOutput || false;
                view.querySelector('#chkLogMissingItems').checked = config.LogMissingItems || false;
                view.querySelector('#chkDryRunMode').checked = config.DryRunMode || false;
                view.querySelector('#chkPreserveTagsOnEmptyResult').checked = config.PreserveTagsOnEmptyResult || false;
                view.querySelector('#chkIgnoreMarkedAsWatched').checked = !!config.IgnoreMarkedAsWatched;
                view.querySelector('#chkTopListMirrorCollections').checked = config.TopListMirrorCollections || false;
                view.querySelector('#chkHideTopListLibraries').checked = config.HideTopListLibraries !== false;
                view.querySelector('#chkShowCopyPasteButtons').checked = config.ShowCopyPasteButtons || false;
                view.querySelector('#chkShowImportCollectionButton').checked = config.ShowImportCollectionButton || false;
                view.querySelector('#chkRankedCollectionsEnabled').checked = config.RankedCollectionsEnabled || false;
                if (view.querySelector('#txtSearchTags')) {
                    view.querySelector('#txtSearchTags').value = '';
                    view.querySelector('#btnClearSearch').style.display = 'none';
                }
                ['#chkFilterTag','#chkFilterCollection','#chkFilterSchedule','#chkFilterHomeScreen',
                 '#chkFilterSrcExternal','#chkFilterSrcMediaInfo','#chkFilterSrcCollection','#chkFilterSrcPlaylist',
                 '#chkFilterSrcAI','#chkFilterActive','#chkFilterInactive','#chkFilterIssues'
                ].forEach(id => { var el = view.querySelector(id); if (el) el.checked = false; });
                var lbl = view.querySelector('#filterDropdownLabel'); if (lbl) lbl.textContent = 'Filter';
                var btn = view.querySelector('#btnFilterDropdown'); if (btn) btn.classList.remove('active');

                _topListTagNames = new Set((config.TopLists || []).map(function (tl) { return (tl.TagName || '').toLowerCase(); }).filter(Boolean));
                noteTopListTypes(config.TopLists);

                var grouped = groupConfigTags(config.Tags);

                var keys = Object.keys(grouped);
                keys.forEach((k, i) => renderTagGroup(grouped[k], container, false, i));

                if (keys.length === 0) renderTagGroup({ Tag: '', Urls: [{ url: '', limit: 0 }], Active: true }, container, false, 0);
                var srcEmptyHint = view.querySelector('#srcEmptyHint');
                if (srcEmptyHint) srcEmptyHint.style.display = keys.length === 0 ? '' : 'none';

                var savedSort = localStorage.getItem('HomeScreenCompanion_SortBy') || 'Manual';
                sortRows(container, savedSort);
                applyFilters(view);
        }

        function hasDirtyState() {
            var btnSave = view.querySelector('.btn-save');
            if (btnSave && !btnSave.disabled) return true;
            var tcContainer = view.querySelector('#tcManageContainer');
            if (tcContainer && tcContainer._tcHasPending) return true;
            return false;
        }

        // Shows a top page tab (and runs its lazy loader), without the unsaved-changes question.
        view._hscShowPageTab = function (target) { showPageTab(target); };
        function showPageTab(target) {
                var tabBtn = view.querySelector('.page-tab-btn[data-page-tab="' + target + '"]');
                var tabEl = view.querySelector('#tab' + target);
                if (!tabBtn || !tabEl) return;
                view.querySelectorAll('.page-tab-btn').forEach(function (b) { b.classList.remove('active'); });
                tabBtn.classList.add('active');
                view.querySelectorAll('.page-tab-content').forEach(function (c) { c.style.display = 'none'; });
                tabEl.style.display = '';

                if (target === 'HomeCompanion') {
                    var hscContainer = view.querySelector('#hscContainer');
                    if (hscContainer && !hscContainer.dataset.loaded) {
                        loadHscUsers(view);
                    }
                    var manageContainer = view.querySelector('#hscManageContainer');
                    if (manageContainer && !manageContainer.dataset.loaded) {
                        loadHscManageTab(view);
                    }
                } else if (target === 'Cleanup') {
                    var container = view.querySelector('#tcManageContainer');
                    if (container && !container.dataset.loaded) loadTagManageTab(view);
                } else if (target === 'TopLists') {
                    var tlContainer = view.querySelector('#tlContainer');
                    if (tlContainer && !tlContainer.dataset.loaded) loadTopListsTab(view);
                }
        }

        view.querySelectorAll('.page-tab-btn').forEach(function (btn) {
            btn.addEventListener('click', function () {
                var target = this.getAttribute('data-page-tab');
                var wasDirty = hasDirtyState();
                if (wasDirty && !confirm('You have unsaved changes. Leave this tab and discard changes?')) return;
                if (wasDirty) { writeLeaveState(null); hideDraftBar(); loadConfig(); }
                showPageTab(target);
            });
        });

        // AI: OpenAI-compatible connections (Add / Remove) and the Model dropdowns.
        view.addEventListener('click', function (e) {
            if (e.target.closest('#btnAddAiConn')) {
                var box = view.querySelector('#aiConnList');
                box.insertAdjacentHTML('beforeend', aiConnCardHtml({ Id: newAiConnId() }));
                syncModelField(box.lastElementChild.querySelector('.hsc-model-field'));
                refreshAiProviderSelects(view);
                var nm = box.lastElementChild.querySelector('.txtAiConnName'); if (nm) nm.focus();
                setTimeout(checkFormState, 0);
                return;
            }
            var rm = e.target.closest('.btnRemoveAiConn');
            if (rm) {
                rm.closest('.hsc-ai-conn').remove();
                refreshAiProviderSelects(view);
                setTimeout(checkFormState, 0);
                return;
            }
            var rl = e.target.closest('.btnAiModelsReload');
            if (rl) { loadModelField(rl.closest('.hsc-model-field'), true); return; }
            // Opening a provider's panel loads its models (once per key / URL).
            var hd = e.target.closest('.settings-panel-toggle');
            if (hd) setTimeout(function () {
                var panel = hd.closest('.settings-panel');
                if (panel) panel.querySelectorAll('.hsc-model-field').forEach(function (f) { if (f.offsetParent) loadModelField(f); });
            }, 0);
        });
        view.addEventListener('change', function (e) {
            var sel = e.target.closest('.selAiModel');
            if (sel) {
                var field = sel.closest('.hsc-model-field'), input = modelFieldInput(field);
                if (sel.value === AI_MODEL_CUSTOM) {
                    field.classList.add('is-custom');
                    syncModelField(field);
                    if (input) input.focus();
                } else if (input) {
                    field.classList.remove('is-custom');
                    input.value = sel.value;
                    syncModelField(field);
                    input.dispatchEvent(new Event('change', { bubbles: true }));
                }
                return;
            }
            // A key or URL filled in or changed: read that provider's models again.
            var t = e.target;
            if (t.matches && t.matches('.txtAiConnBaseUrl, .txtAiConnApiKey')) {
                loadModelField(t.closest('.hsc-ai-conn').querySelector('.hsc-model-field'), true);
                return;
            }
            if (t.id) view.querySelectorAll('.hsc-model-field[data-key="' + t.id + '"], .hsc-model-field[data-url="' + t.id + '"]').forEach(function (f) { loadModelField(f, true); });
        });
        view.addEventListener('input', function (e) {
            if (e.target.closest && e.target.closest('.txtAiConnName')) refreshAiProviderSelects(view);
        });

        view.addEventListener('change', function (e) {
            var cb = e.target.closest('.chkShowApiKey');
            if (!cb) return;
            var input = view.querySelector('#' + cb.dataset.target);
            if (input) input.classList.toggle('api-key-shown', cb.checked);
        });

        view.addEventListener('click', function (e) {
            var header = e.target.closest('.settings-panel-toggle');
            if (header) {
                var panel = header.closest('.settings-panel');
                if (panel) {
                    var body = panel.querySelector('.settings-panel-body');
                    var chevron = header.querySelector('.settings-panel-chevron');
                    if (body) {
                        var isOpen = body.style.display !== 'none';
                        body.style.display = isOpen ? 'none' : 'block';
                        if (chevron) chevron.style.transform = isOpen ? '' : 'rotate(180deg)';
                    }
                }
                return;
            }
        });

        view.addEventListener('click', function (e) {
            var btn = e.target.closest('.hsc-sub-tab-btn');
            if (btn) showSubTab(btn.getAttribute('data-hsc-tab'));
        });

        // Shows a Home Screen sub-tab (and runs its lazy loader).
        function showSubTab(target) {
            var btn = view.querySelector('.hsc-sub-tab-btn[data-hsc-tab="' + target + '"]');
            if (!btn) return;
            view.querySelectorAll('.hsc-sub-tab-btn').forEach(function (b) { b.classList.remove('active'); });
            btn.classList.add('active');
            view.querySelectorAll('.hsc-sub-tab-content').forEach(function (c) { c.style.display = 'none'; });
            if (target === 'copy') {
                view.querySelector('#hscSubTabCopy').style.display = '';
                var hscContainer = view.querySelector('#hscContainer');
                if (hscContainer && !hscContainer.dataset.loaded) loadHscUsers(view);
            } else if (target === 'tweaks') {
                view.querySelector('#hscSubTabTweaks').style.display = '';
                var hstContainer = view.querySelector('#hstContainer');
                if (hstContainer && !hstContainer.dataset.loaded) loadHstTab(view);
            } else if (target === 'manage') {
                view.querySelector('#hscSubTabManage').style.display = '';
                var manageContainer = view.querySelector('#hscManageContainer');
                if (manageContainer && !manageContainer.dataset.loaded) loadHscManageTab(view);
            }
        }


        startStatusPolling(view);
    };
});