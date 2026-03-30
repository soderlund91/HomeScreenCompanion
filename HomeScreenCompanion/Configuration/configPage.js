define(['emby-input', 'emby-button', 'emby-select', 'emby-checkbox'], function () {
    'use strict';

    var pluginId = "7c10708f-43e4-4d69-923c-77d01802315b";
    var statusInterval = null;
    var originalConfigState = null;
    var statusRequestId = 0;

    function applyPluginTheme() {
        var candidates = ['.skinHeader', '.mainDrawer', '.contentScrollSlider', 'body'];
        var bg = null;
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
        } else {
            root.style.setProperty('--plugin-popup-bg',     '#f2f2f2');
            root.style.setProperty('--plugin-popup-bg2',    '#e0e0e0');
            root.style.setProperty('--plugin-popup-color',  '#1a1a1a');
            root.style.setProperty('--plugin-popup-muted',  '#555555');
            root.style.setProperty('--plugin-popup-border', 'rgba(0,0,0,0.15)');
            root.style.setProperty('--plugin-popup-hover',  'rgba(0,0,0,0.08)');
            root.style.setProperty('--plugin-popup-badge',  'rgba(0,0,0,0.1)');
        }
    }

    var cachedCollections = [];
    var cachedPlaylists = [];
    var cachedTags = [];
    var lastHscConfig = {};
    var currentManageSections = [];
    var savedFilters = [];

    var customCss = `
    <style id="homeScreenCompanionCustomCss">
        .day-toggle {
            background: rgba(128,128,128,0.08);
            color: var(--theme-text-secondary);
            border: 1px solid var(--line-color);
            border-radius: 4px;
            padding: 8px 12px;
            cursor: pointer;
            font-size: 0.9em;
            transition: all 0.2s;
            text-transform: uppercase;
            font-weight: bold;
            flex-grow: 1;
            text-align: center;
        }
        .day-toggle:hover {
            background: var(--theme-background-level2);
            color: var(--theme-text-primary);
            border-color: var(--theme-primary-color);
        }
        .day-toggle.active {
            background: #52B54B;
            color: #fff;
            border-color: #52B54B;
            box-shadow: 0 2px 5px rgba(0,0,0,0.3);
        }

        .date-row-container {
            background: rgba(128,128,128,0.06);
            border: 1px solid var(--line-color);
            border-radius: 6px;
            padding: 15px;
            margin-bottom: 10px;
        }

        .selectLabel {
            font-size: 0.9em;
            color: var(--theme-text-secondary);
            margin-bottom: 5px;
            font-weight: 500;
            display: block;
        }

        .tag-indicator {
            margin-left: 10px;
            font-size: 0.75em;
            padding: 2px 8px;
            border-radius: 4px;
            display: flex;
            align-items: center;
            gap: 4px;
            font-weight: 500;
            position: relative;
        }

        .tag-indicator.schedule {
            color: #00a4dc;
            background: rgba(0,164,220,0.15);
            border: 1px solid rgba(0,164,220,0.35);
        }

        .tag-indicator.collection {
            color: var(--theme-text-primary);
            background: rgba(128,128,128,0.12);
            border: 1px solid rgba(128,128,128,0.3);
        }

        .tag-indicator.homescreen {
            color: #4CAF50;
            background: rgba(76,175,80,0.15);
            border: 1px solid rgba(76,175,80,0.35);
        }

        .tag-indicator.schedule::after {
            content: '';
            position: absolute;
            top: -3px;
            right: -3px;
            width: 6px;
            height: 6px;
            border-radius: 50%;
            background: #f59e0b;
            box-shadow: 0 0 0 1.5px var(--theme-background, #101010);
        }

        .tag-indicator.schedule.schedule-active::after {
            background: #52B54B;
        }

        .tag-indicator.tag {
            color: #FFC107;
            background: rgba(255,193,7,0.15);
            border: 1px solid rgba(255,193,7,0.35);
        }

        .tag-indicator.source {
            color: #78909c;
            background: rgba(120,144,156,0.15);
            border: 1px solid rgba(120,144,156,0.35);
            padding: 2px 5px;
            margin-left: 0;
            margin-right: 8px;
        }

        .badge-container {
            display: flex;
            align-items: center;
        }

        .sort-hidden .drag-handle {
            display: none !important;
        }

        .dry-run-warning {
            background-color: #E67E22;
            color: #000000;
            padding: 15px;
            border-radius: 5px;
            margin-bottom: 20px;
            text-align: center;
            font-weight: bold;
            font-size: 1.1em;
            box-shadow: 0 4px 8px rgba(0,0,0,0.3);
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
            margin-right: 15px;
            color: var(--theme-text-secondary);
            display: flex;
            align-items: center;
        }

        .drag-handle:active {
            cursor: grabbing;
        }

        .tag-row {
            position: relative;
            background: var(--theme-background-level2);
            margin-bottom: 15px;
            border-radius: 6px;
            border: 1px solid var(--line-color);
            border-left: 5px solid #52B54B;
            transition: all 0.2s ease;
            box-shadow: 0 2px 6px rgba(0,0,0,0.12);
            overflow: hidden;
        }

        .tag-row.inactive {
            border-left-color: rgba(128,128,128,0.5);
        }

        .tag-row.dragging {
            opacity: 0.4 !important;
            border: 2px dashed #999 !important;
            background: var(--theme-background-level1) !important;
        }

        .sort-placeholder {
            height: 40px;
            background-color: transparent;
            margin-bottom: 15px;
            border-radius: 6px;
            border: 2px dashed var(--line-color);
            transition: height 0.2s;
        }

        .tag-row.just-moved {
            animation: moveHighlight 2s ease-out forwards;
        }

        .tag-row.just-added {
            animation: addHighlight 2s ease-out forwards;
        }

        @keyframes moveHighlight {
            0% {
                border-top: 1px solid #00a4dc;
                border-right: 1px solid #00a4dc;
                border-bottom: 1px solid #00a4dc;
                box-shadow: 0 0 15px rgba(0,164,220,0.5);
            }
            100% {
                border-top: 1px solid var(--line-color);
                border-right: 1px solid var(--line-color);
                border-bottom: 1px solid var(--line-color);
                box-shadow: 0 2px 6px rgba(0,0,0,0.12);
            }
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
            0% {
                border-top: 1px solid #52B54B;
                border-right: 1px solid #52B54B;
                border-bottom: 1px solid #52B54B;
                box-shadow: 0 0 15px rgba(82,181,75,0.5);
            }
            100% {
                border-top: 1px solid var(--line-color);
                border-right: 1px solid var(--line-color);
                border-bottom: 1px solid var(--line-color);
                box-shadow: 0 2px 6px rgba(0,0,0,0.12);
            }
        }

        .control-row {
            background: rgba(128,128,128,0.06);
            padding: 12px;
            border-radius: 6px;
            margin-bottom: 20px;
            border: 1px solid var(--line-color);
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
            font-size: 0.85em;
            opacity: 0.5;
            text-transform: uppercase;
            font-weight: bold;
            letter-spacing: 0.5px;
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
            font-size: 1.2em;
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
            color: #cc3333;
        }

        #txtSearchTags {
            width: 100%;
            background: rgba(128,128,128,0.06) !important;
            border: 1px solid var(--line-color) !important;
            border-radius: 4px !important;
            padding: 6px 30px 6px 35px !important;
            color: inherit;
            font-size: 0.95em;
        }

        #txtSearchTags:focus {
            border-color: var(--theme-primary-color) !important;
            background: rgba(128,128,128,0.1) !important;
        }

        .filter-dropdown-wrapper {
            position: relative;
            flex-shrink: 0;
        }

        .filter-dropdown-btn {
            display: flex;
            align-items: center;
            gap: 5px;
            padding: 5px 10px;
            background: rgba(128,128,128,0.08);
            border: 1px solid var(--line-color);
            border-radius: 4px;
            font-size: 0.9em;
            cursor: pointer;
            color: inherit;
            white-space: nowrap;
            user-select: none;
        }

        .filter-dropdown-btn:hover {
            background: rgba(128,128,128,0.15);
        }

        .filter-dropdown-btn.active {
            border-color: #52B54B;
            color: #52B54B;
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
            border-radius: 6px;
            box-shadow: 0 6px 20px rgba(0,0,0,0.4);
            padding: 10px 14px;
            min-width: 200px;
        }

        .filter-dropdown-panel.open {
            display: block;
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
            opacity: 0.5;
            text-transform: uppercase;
            letter-spacing: 0.05em;
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
            color: #cc3333;
            display: flex;
            align-items: center;
            justify-content: center;
            box-shadow: none;
            margin-top: 12px;
        }

        .btn-neutral {
            background: var(--theme-background-level2) !important;
            border: 1px solid rgba(128,128,128,0.4) !important;
            color: var(--theme-text-primary) !important;
        }
    </style>`;

    function getUrlRowHtml(value, limit) {
        var val = value || '';
        var lim = limit !== undefined ? limit : 0;
        return `
            <div class="url-row" style="display:flex; align-items:center; gap:10px; margin-bottom:10px;">
                <div style="flex-grow:1;">
                    <input is="emby-input" class="txtTagUrl" type="text" label="Trakt/MDBList URL or ID" value="${val}" />
                </div>
                <div style="width:110px;">
                    <input is="emby-input" class="txtUrlLimit" type="number" label="Max (0=All)" value="${lim}" min="0" />
                </div>
                <button type="button" is="emby-button" class="raised button-submit btnTestUrl" style="min-width:60px; height:36px; padding:0 10px; font-size:0.8rem; margin-top:12px;" title="Test Source"><span>Test</span></button>
                <button type="button" is="emby-button" class="raised btnRemoveUrl btn-row-remove" title="Remove URL"><i class="md-icon">remove_circle_outline</i></button>
            </div>`;
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

    function getDateRowHtml(interval) {
        var type = interval.Type || 'SpecificDate';
        var sDate = interval.Start ? new Date(interval.Start).toISOString().split('T')[0] : '';
        var eDate = interval.End ? new Date(interval.End).toISOString().split('T')[0] : '';
        var sMonth = interval.Start ? new Date(interval.Start).getMonth() + 1 : 12;
        var sDay = interval.Start ? new Date(interval.Start).getDate() : 1;
        var eMonth = interval.End ? new Date(interval.End).getMonth() + 1 : 12;
        var eDay = interval.End ? new Date(interval.End).getDate() : 28;
        var sMaxDay = getMaxDays(sMonth);
        var eMaxDay = getMaxDays(eMonth);
        sDay = Math.min(sDay, sMaxDay);
        eDay = Math.min(eDay, eMaxDay);
        var dayOfWeek = interval.DayOfWeek || '';

        return `
            <div class="date-row date-row-container" style="display: flex; flex-wrap: wrap; align-items: flex-start; gap: 15px;">
                
                <div style="width:160px;">
                    <label class="selectLabel">Rule Type</label>
                    <select is="emby-select" class="selDateType" style="width:100%;">
                        <option value="SpecificDate" ${type === 'SpecificDate' ? 'selected' : ''}>Specific Date</option>
                        <option value="EveryYear" ${type === 'EveryYear' ? 'selected' : ''}>Recurring</option>
                        <option value="Weekly" ${type === 'Weekly' ? 'selected' : ''}>Week Days</option>
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

                <div class="inputs-weekly" style="display: ${type === 'Weekly' ? 'flex' : 'none'}; flex-grow: 1; align-items: center; gap: 5px; flex-wrap: wrap;">
                    <div style="width:100%;">
                        <label class="selectLabel">Active On Days</label>
                        <div class="week-btn-container" style="display:flex; gap:5px; margin-top:2px;">
                            ${getWeekButtons(dayOfWeek)}
                        </div>
                    </div>
                </div>

                <button type="button" is="emby-button" class="btnRemoveDate" style="background:transparent; color:#cc3333; min-width:40px; margin-top: 25px;" title="Remove Rule"><i class="md-icon">delete</i></button>
            </div>`;
    }

    function getDragAfterElement(container, y) {
        const draggableElements = [...container.querySelectorAll('.tag-row:not(.dragging)')];

        return draggableElements.reduce((closest, child) => {
            const box = child.getBoundingClientRect();
            const offset = y - box.top - box.height / 2;
            if (offset < 0 && offset > closest.offset) {
                return { offset: offset, element: child };
            } else {
                return closest;
            }
        }, { offset: Number.NEGATIVE_INFINITY }).element;
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
        var collName = row.querySelector('.txtCollectionName').value;
        var collDesc = row.querySelector('.txtCollectionDescription') ? row.querySelector('.txtCollectionDescription').value : '';
        var collPoster = row.querySelector('.hiddenPosterPath') ? row.querySelector('.hiddenPosterPath').value : '';
        var st = row.querySelector('.selSourceType').value;

        var intervals = [];
        row.querySelectorAll('.date-row').forEach(function(dr) {
            var type = dr.querySelector('.selDateType').value;
            var s = null, e = null, days = '';
            if (type === 'SpecificDate') {
                s = dr.querySelector('.txtFullStartDate').value;
                e = dr.querySelector('.txtFullEndDate').value;
            } else if (type === 'EveryYear') {
                var sM = dr.querySelector('.selStartMonth').value, sD = dr.querySelector('.selStartDay').value;
                var eM = dr.querySelector('.selEndMonth').value, eD = dr.querySelector('.selEndDay').value;
                s = '2000-' + sM.padStart(2, '0') + '-' + sD.padStart(2, '0');
                e = '2000-' + eM.padStart(2, '0') + '-' + eD.padStart(2, '0');
            } else if (type === 'Weekly') {
                days = Array.from(dr.querySelectorAll('.day-toggle.active')).map(function(b) { return b.dataset.day; }).join(',');
            }
            intervals.push({ Type: type, Start: s || null, End: e || null, DayOfWeek: days });
        });

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
        var enableHse = hseTab ? !!(hseTab.querySelector('.chkEnableHomeSection') || {}).checked : false;
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
                // If field is empty, fall back to placeholder (e.g. CustomName uses display name as placeholder)
                if (v === '' && el.placeholder) v = el.placeholder;
                hseSettings[f] = v;
            });
            var itemTypesVal = (hseTab.querySelector('.selHseItemTypes') || {}).value || 'Movie,Series';
            hseSettings['ItemTypes'] = JSON.stringify(itemTypesVal.split(','));
        } else {
            try { hseSettings = JSON.parse(decodeURIComponent((hseTab && hseTab.dataset.hseSettings) || '%7B%7D')); } catch(ex) {}
        }

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

        return {
            Name: entryLabel, Tag: tagName, Active: active, Blacklist: bl, ActiveIntervals: intervals,
            EnableTag: enableTag, EnableCollection: enableColl, CollectionName: collName,
            CollectionDescription: collDesc, CollectionPosterPath: collPoster,
            OverrideWhenActive: overrideWhenActive, SourceType: st,
            Urls: urls, LocalSources: localSources, Limit: miLimit,
            MediaInfoFilters: miFilters, MediaInfoConditions: [],
            EnableHomeSection: enableHse, HomeSectionLibraryId: hseLibraryId,
            HomeSectionUserIds: hseUserIds, HomeSectionSettings: JSON.stringify(hseSettings),
            HomeSectionTracked: [], LastModified: new Date().toISOString(),
            AiProvider: aiProvider, AiPrompt: aiPrompt,
            AiIncludeRecentlyWatched: aiIncludeRecentlyWatched,
            AiRecentlyWatchedUserId: aiRecentlyWatchedUserId,
            AiRecentlyWatchedCount: aiRecentlyWatchedCount,
            TagTargetEpisode:        !!(row.querySelector('.chkTagTargetEpisode')  || {}).checked,
            TagTargetSeason:         !!(row.querySelector('.chkTagTargetSeason')   || {}).checked,
            TagTargetSeries:         !!(row.querySelector('.chkTagTargetSeries')   || {}).checked,
            CollectionTargetEpisode: !!(row.querySelector('.chkCollTargetEpisode') || {}).checked,
            CollectionTargetSeason:  !!(row.querySelector('.chkCollTargetSeason')  || {}).checked,
            CollectionTargetSeries:  !!(row.querySelector('.chkCollTargetSeries')  || {}).checked,
            MediaInfoTargetEpisode: false, MediaInfoTargetSeason: false, MediaInfoTargetSeries: false,
            MediaInfoTargetType: '', MediaInfoSeasonMode: false
        };
    }

    var _formAc = null;

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
        var parts = crit.split(':');
        if (parts.length === 1) {
            var mapped = MI_CRITERION_MAP[crit];
            return mapped ? { prop: mapped.prop, op: '', val: mapped.val, userId: '', not: not } : { prop: '', op: '', val: crit, userId: '', not: not };
        }
        if (parts.length === 2) return { prop: parts[0], op: '', val: parts[1], userId: '', not: not };
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
    var MI_NUMERIC_PROPS = ['CommunityRating', 'Year', 'Runtime', 'DateAdded', 'DateModified', 'FileSize', 'LastPlayed', 'PlayCount', 'BitRate', 'SampleRate', 'BitsPerSample', 'TrackNumber', 'DiscNumber', 'WatchedByCount'];
    var MI_UNIT_LABELS = { DateAdded: 'days ago', DateModified: 'days ago', LastPlayed: 'days ago', FileSize: 'MB', PlayCount: 'plays', BitRate: 'kbps', SampleRate: 'Hz', BitsPerSample: 'bits', WatchedByCount: 'users' };
    var MI_USER_PROPS = ['IsPlayed', 'LastPlayed', 'PlayCount'];
    var MI_TEXT_MATCH_PROPS = ['Tag', 'Title', 'EpisodeTitle', 'Overview', 'Studio', 'Genre', 'Actor', 'Director', 'Writer', 'ContentRating', 'AudioLanguage', 'Artist', 'Album'];
    var MI_TEXT_MATCH_DEFAULT = {
        Title: 'contains', EpisodeTitle: 'contains', Overview: 'contains', Studio: 'contains', Genre: 'contains', Tag: 'contains',
        Actor: 'exact', Director: 'exact', Writer: 'exact', Artist: 'contains', Album: 'contains',
        ContentRating: 'exact', AudioLanguage: 'exact'
    };
    var _miUsers = null;
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
        Artist: 'e.g. Radiohead, Pink Floyd', Album: 'e.g. OK Computer, Dark Side of the Moon'
    };

    function propertyOptionsHtml(selected) {
        var groups = [
            { label: 'Video', props: [['Resolution','Resolution'], ['VideoCodec','Video Codec'], ['HDR','HDR']] },
            { label: 'Audio', props: [['AudioFormat','Audio Format'], ['AudioChannels','Audio Channels'], ['AudioLanguage','Audio Language']] },
            { label: 'Content', props: [['MediaType','Media Type'], ['Tag','Tag'], ['Title','Title'], ['EpisodeTitle','Title (Episode)'], ['Overview','Overview'], ['Studio','Studio'], ['Genre','Genre'], ['Actor','Actor / Cast'], ['Director','Director'], ['Writer','Writer'], ['ContentRating','Content Rating'], ['ImdbId','IMDB ID'], ['Collection','In Collection'], ['Playlist','In Playlist']] },
            { label: 'Music', props: [['Artist','Artist'], ['Album','Album'], ['BitRate','Bit Rate (kbps)'], ['SampleRate','Sample Rate (Hz)'], ['BitsPerSample','Bit Depth'], ['TrackNumber','Track Number'], ['DiscNumber','Disc Number']] },
            { label: 'Metrics', props: [['CommunityRating','Community Rating'], ['Year','Year'], ['Runtime','Runtime (minutes)'], ['DateAdded','Date Added'], ['DateModified','Date Modified'], ['FileSize','File Size (MB)']] },
            { label: 'Activity', props: [['IsPlayed','Watched / Unwatched'], ['LastPlayed','Last Played'], ['PlayCount','Play Count'], ['WatchedByCount','Watched by (user count)']] }
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
                return '<option value="' + u.Id + '"' + (u.Id === savedUserId ? ' selected' : '') + '>' + u.Name + '</option>';
            }).join('');
            userHtml = '<select class="selMiUser" is="emby-select" style="flex:0 0 auto;min-width:110px;">' + uOpts + '</select>';
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
            return tagTextOpHtml + '<input class="txtMiValue" is="emby-input" type="text" placeholder="e.g. 4K" value="' + (savedVal || '').replace(/"/g, '&quot;') + '" style="flex:1;" />';
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
            var defaultOp = (prop === 'PlayCount') ? '>=' : '<=';
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
            var numStep = (prop === 'PlayCount') ? '1' : '0.01';
            return userHtml +
                '<select class="selMiOp" is="emby-select" style="flex:0 0 64px;">' + opOpts + '</select>' +
                infoTooltip +
                '<input class="txtMiNum" is="emby-input" type="number" step="' + numStep + '" value="' + (savedVal || '') + '" style="flex:1;" />' +
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
            return textOpHtml + '<textarea class="txtMiValue" placeholder="' + ph + '" rows="1" style="flex:1;resize:none;overflow:hidden;padding:6px 8px;font-size:inherit;font-family:inherit;background:var(--input-background,rgba(255,255,255,0.08));border:1px solid var(--input-border,rgba(255,255,255,0.2));border-radius:3px;color:inherit;line-height:1.4;max-height:120px;overflow-y:auto;">' + migratedVal.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;') + '</textarea>';
        }
        var ph = MI_TEXT_PLACEHOLDERS[prop] || '';
        var migratedVal = migrateCommaSeparated(savedVal || '');
        return '<textarea class="txtMiValue" placeholder="' + ph + '" rows="1" style="flex:1;resize:none;overflow:hidden;padding:6px 8px;font-size:inherit;font-family:inherit;background:var(--input-background,rgba(255,255,255,0.08));border:1px solid var(--input-border,rgba(255,255,255,0.2));border-radius:3px;color:inherit;line-height:1.4;max-height:120px;overflow-y:auto;">' + migratedVal.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;') + '</textarea>';
    }

    function getMiHintHtml(prop) {
        if (MI_TEXT_MATCH_PROPS.indexOf(prop) < 0 && prop !== 'ImdbId') return '<div class="mi-rule-hint"></div>';
        var example = MI_TEXT_PLACEHOLDERS[prop] || 'Value1\nValue2';
        return '<div class="mi-rule-hint" style="font-size:0.75em; opacity:0.5; margin-top:2px; padding-right:32px; text-align:right;">One value per line — ' + example.replace(/,\s*/g, ' / ') + '</div>';
    }

    function getMediaInfoRuleHtml(criterion) {
        var parsed = parseCriterion(criterion || '');
        var prop = parsed.prop || 'Resolution';
        var notActive = parsed.not;
        var notBg = notActive ? 'rgba(200,50,50,0.75)' : 'transparent';
        var notColor = notActive ? '#fff' : '';
        var notBorder = notActive ? '1px solid rgba(200,50,50,0.6)' : '1px solid rgba(128,128,128,0.4)';
        return '<div class="mi-rule" style="margin-bottom:6px;">' +
            '<div style="display:flex; gap:6px; align-items:center;">' +
            '<button type="button" class="btnNotToggle" data-not="' + (notActive ? '1' : '0') + '"' +
            ' style="border:' + notBorder + '; border-radius:10px; padding:3px 10px; font-size:0.78em; font-weight:bold; cursor:pointer; letter-spacing:0.5px; flex-shrink:0;' +
            ' background:' + notBg + '; color:' + notColor + ';" title="Negate this rule">NOT</button>' +
            '<select class="selMiProperty" is="emby-select" style="flex:0 0 155px;">' + propertyOptionsHtml(prop) + '</select>' +
            '<div class="mi-value-wrapper" style="flex:1; display:flex; gap:6px; align-items:center;">' + getMiValueHtml(prop, parsed.op, parsed.val, parsed.userId || '') + '</div>' +
            '<button type="button" class="btnRemoveMiRule" style="background:transparent; border:none; color:#cc3333; cursor:pointer; padding:2px 8px; font-size:1em; flex-shrink:0;" title="Remove rule">✕</button>' +
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
                    '<span style="font-size:0.7em; text-transform:uppercase; letter-spacing:1px; opacity:0.45;">Connect groups with</span>' +
                    '<div style="display:flex; border-radius:14px; overflow:hidden; border:1px solid rgba(128,128,128,0.4);">' +
                        '<button type="button" class="btnGroupOpChoice" data-value="AND"' +
                        ' style="border:none; padding:4px 16px; font-size:0.82em; font-weight:bold; cursor:pointer; letter-spacing:0.5px;' +
                        ' background:' + (groupOp === 'AND' ? 'rgba(0,164,220,0.75)' : 'transparent') + ';' +
                        ' color:' + (groupOp === 'AND' ? '#fff' : 'inherit') + ';"' +
                        ' title="Both filter groups must match">AND</button>' +
                        '<div style="width:1px; background:rgba(128,128,128,0.4);"></div>' +
                        '<button type="button" class="btnGroupOpChoice" data-value="OR"' +
                        ' style="border:none; padding:4px 16px; font-size:0.82em; font-weight:bold; cursor:pointer; letter-spacing:0.5px;' +
                        ' background:' + (groupOp === 'OR' ? 'rgba(220,120,0,0.75)' : 'transparent') + ';' +
                        ' color:' + (groupOp === 'OR' ? '#fff' : 'inherit') + ';"' +
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
                    '<span style="font-size:0.8em; font-weight:bold; text-transform:uppercase; letter-spacing:0.5px; opacity:0.6;">Match rules:</span>' +
                    '<div style="display:flex; flex-direction:column; gap:3px;">' +
                        '<div style="display:flex; border-radius:14px; overflow:hidden; border:1px solid rgba(128,128,128,0.4);">' +
                            '<button type="button" class="btnGroupInnerOpChoice" data-value="AND"' +
                            ' style="border:none; padding:4px 16px; font-size:0.82em; font-weight:bold; cursor:pointer; letter-spacing:0.5px;' +
                            ' background:' + (op === 'AND' ? 'rgba(0,164,220,0.75)' : 'transparent') + ';' +
                            ' color:' + (op === 'AND' ? '#fff' : 'inherit') + ';"' +
                            ' title="All rules in this group must match">ALL</button>' +
                            '<div style="width:1px; background:rgba(128,128,128,0.4);"></div>' +
                            '<button type="button" class="btnGroupInnerOpChoice" data-value="OR"' +
                            ' style="border:none; padding:4px 16px; font-size:0.82em; font-weight:bold; cursor:pointer; letter-spacing:0.5px;' +
                            ' background:' + (op === 'OR' ? 'rgba(220,120,0,0.75)' : 'transparent') + ';' +
                            ' color:' + (op === 'OR' ? '#fff' : 'inherit') + ';"' +
                            ' title="Any rule in this group is enough">ANY</button>' +
                        '</div>' +
                        '<span class="inner-op-desc" style="font-size:0.7em; opacity:0.55;">' +
                            (op === 'AND' ? 'All rules must match' : 'Any rule is enough') +
                        '</span>' +
                    '</div>' +
                '</div>' +
                '<button type="button" class="btnRemoveFilterGroup" style="background:transparent; border:none; color:#cc3333; cursor:pointer; padding:2px 8px; font-size:0.85em; flex-shrink:0;">✕ Remove</button>' +
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
                    ' style="border:1.5px solid #000; border-radius:14px 0 0 14px; padding:4px 10px; font-size:0.82em; cursor:pointer; background:transparent; color:var(--theme-text-primary);">' +
                    escapeHtml(sf.Name) + '</button>' +
                    '<button type="button" class="btnDeleteMySavedFilter" data-index="' + i + '"' +
                    ' style="border:1.5px solid #000; border-left:none; border-radius:0 14px 14px 0; background:transparent; color:#cc3333; cursor:pointer; padding:4px 8px; font-size:0.82em; line-height:1;" title="Delete">✕</button>' +
                    '</div>';
            }).join('') +
            '</div>';
    }

    function refreshMySavedFiltersPanels() {
        var v = document.querySelector('#HomeScreenCompanionConfigPage');
        if (!v) return;
        var html = getMySavedFiltersPanelHtml();
        v.querySelectorAll('.mi-saved-panel-content').forEach(function (el) {
            el.innerHTML = html;
        });
    }

    function saveSavedFiltersNow() {
        window.ApiClient.getPluginConfiguration(pluginId)
            .then(function (currentConfig) {
                currentConfig.SavedFilters = savedFilters;
                return window.ApiClient.updatePluginConfiguration(pluginId, currentConfig);
            })
            .then(function () {
                if (originalConfigState) {
                    try {
                        var state = JSON.parse(originalConfigState);
                        state.SavedFilters = savedFilters;
                        originalConfigState = JSON.stringify(state);
                    } catch (e) {}
                }
                var view = document.querySelector('#HomeScreenCompanionConfigPage');
                if (view) checkFormState();
            });
    }

    function isScheduleCurrentlyActive(intervals) {
        if (!intervals || intervals.length === 0) return false;
        var now = new Date();
        var dowNames = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
        var todayName = dowNames[now.getDay()];
        return intervals.some(function(iv) {
            if (iv.Type === 'Weekly') {
                var days = (iv.DayOfWeek || '').split(',').map(function(d) { return d.trim(); });
                return days.indexOf(todayName) >= 0;
            }
            if (!iv.Start || !iv.End) return false;
            var s = new Date(iv.Start);
            var e = new Date(iv.End);
            if (iv.Type === 'EveryYear') {
                var nowMD = now.getMonth() * 100 + now.getDate();
                var sMD  = s.getMonth() * 100 + s.getDate();
                var eMD  = e.getMonth() * 100 + e.getDate();
                return sMD <= nowMD && nowMD <= eMD;
            }
            // SpecificDate
            e.setHours(23, 59, 59, 999);
            return s <= now && now <= e;
        });
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
            } else if (type === 'Weekly') {
                days = Array.from(dr.querySelectorAll('.day-toggle.active')).map(function(b) { return b.dataset.day; }).join(',');
            }
            intervals.push({ Type: type, Start: s, End: e, DayOfWeek: days });
        });
        return intervals;
    }

    function getSourceBadgeHtml(st) {
        var map = {
            'External':        { icon: 'language',       title: 'External List' },
            'LocalCollection': { icon: 'folder_special', title: 'Local Collection' },
            'LocalPlaylist':   { icon: 'playlist_play',  title: 'Local Playlist' },
            'MediaInfo':       { icon: 'tune',           title: 'Smart Playlist' },
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
        var blacklist = migrateCommaSeparated((tagConfig.Blacklist || []).join('\n'));
        var intervals = tagConfig.ActiveIntervals || [];
        var idx = typeof index !== 'undefined' ? index : 9999;

        var lastMod = tagConfig.LastModified || new Date().toISOString();

        var enableTag = tagConfig.EnableTag !== false ? 'checked' : '';
        var enableColl = tagConfig.EnableCollection ? 'checked' : '';
        var overrideChecked = tagConfig.OverrideWhenActive ? 'checked' : '';

        var collName = tagConfig.CollectionName || '';
        var collDescription = tagConfig.CollectionDescription || '';
        var collPosterPath = tagConfig.CollectionPosterPath || '';

        var sourceType = tagConfig.SourceType || "";
        var localSources = tagConfig.LocalSources || [];
        if (localSources.length === 0) localSources = [{ id: "", limit: 0 }];

        var mediaInfoLimit = tagConfig.Limit || 0;
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
        var disableHomeSection = (tagConfig.EnableTag === false && !tagConfig.EnableCollection) ? 'disabled' : '';
        var homeSectionLibraryId = encodeURIComponent(tagConfig.HomeSectionLibraryId || 'auto');
        var homeSectionUserIdsEnc = encodeURIComponent(JSON.stringify(tagConfig.HomeSectionUserIds || []));
        var homeSectionSettingsEnc = encodeURIComponent(tagConfig.HomeSectionSettings || '{}');
        var homeSectionTrackedEnc = encodeURIComponent(JSON.stringify(tagConfig.HomeSectionTracked || []));
        var hsDefaultSectionType = tagConfig.EnableCollection ? 'boxset' : (tagConfig.EnableTag ? 'items' : 'boxset');

        var mediaFilters = (tagConfig.MediaInfoFilters && tagConfig.MediaInfoFilters.length > 0)
            ? tagConfig.MediaInfoFilters
            : ((tagConfig.MediaInfoConditions && tagConfig.MediaInfoConditions.length > 0)
                ? [{ Operator: 'AND', Criteria: tagConfig.MediaInfoConditions }]
                : []);
        var filterGroupsHtml = mediaFilters.map((f, i) => getMediaInfoFilterGroupHtml(f, i, i === 0)).join('');

        var activeText = tagConfig.Active !== false ? "Active" : "Disabled";
        var activeColor = tagConfig.Active !== false ? "#52B54B" : "var(--theme-text-secondary)";

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
        if (tagConfig.EnableHomeSection) {
            indicatorsHtml += `<span class="tag-indicator homescreen"><i class="md-icon" style="font-size:1.1em;">home</i> Home Section</span>`;
        }
        if (tagConfig.EnableTag) {
            indicatorsHtml += `<span class="tag-indicator tag"><i class="md-icon" style="font-size:1.1em;">label</i> Tag</span>`;
        }

        var initialStyle = isNew ? 'display:block;' : 'display:none;';
        var initialIcon = isNew ? 'expand_less' : 'expand_more';

        var inactiveClass = tagConfig.Active === false ? "inactive" : "";
        var newClass = isNew ? "just-added" : "";

        var html = `
        <div class="tag-row ${inactiveClass} ${newClass}" data-index="${idx}" data-tag="${tagName.toLowerCase()}" data-last-modified="${lastMod}" data-dirty="false">
            <div class="tag-header" style="display:flex; align-items:center; justify-content:space-between; padding:10px; cursor:pointer;">
                <div style="display:flex; align-items:center;">
                    <div class="header-actions" style="margin-right:15px; display:flex; align-items:center;" onclick="event.stopPropagation()">
                        <div class="drag-handle">
                            <i class="md-icon">reorder</i>
                        </div>
                        <span class="lblActiveStatus" style="margin-right:8px; font-size:0.9em; font-weight:bold; color:${activeColor}; min-width:60px; text-align:right;">${activeText}</span>
                        <label class="checkboxContainer" style="margin:0;">
                            <input type="checkbox" is="emby-checkbox" class="chkTagActive" ${isChecked} />
                            <span></span>
                        </label>
                    </div>
                    <div class="tag-info" style="display:flex; align-items:center;">
                        <span class="source-badge">${sourceBadgeHtml}</span>
                        <span class="tag-title" style="font-weight:bold; font-size:1.1em;">${labelName || tagName || 'New'}</span>
                        <span class="badge-container" style="display:flex; align-items:center;">${indicatorsHtml}</span>
                    </div>
                </div>
                <i class="md-icon expand-icon">${initialIcon}</i>
            </div>
            <div class="tag-body" style="${initialStyle} padding:15px; border-top:1px solid rgba(255,255,255,0.1);">
                <div style="display:flex; justify-content:flex-end; margin-bottom:4px;">
                    <button type="button" is="emby-button" class="btnDuplicateRow raised" style="background:transparent; color:var(--theme-text-secondary); font-size:0.82em; padding:0 10px; min-width:0; box-shadow:none;" title="Duplicate this source"><i class="md-icon" style="font-size:1em; margin-right:4px;">content_copy</i><span>Duplicate</span></button>
                </div>
                <div class="tag-tabs" style="display: flex; gap: 20px; margin-bottom: 15px; border-bottom: 1px solid rgba(255,255,255,0.1);">
                    <div class="tag-tab active" data-tab="general" style="padding: 8px 0; cursor: pointer; font-weight: bold; border-bottom: 2px solid #52B54B;">Source</div>
                    <div class="tag-tab" data-tab="tag" style="padding: 8px 0; cursor: pointer; opacity: 0.6; font-weight: bold; border-bottom: 2px solid transparent;">Tag</div>
                    <div class="tag-tab" data-tab="collection" style="padding: 8px 0; cursor: pointer; opacity: 0.6; font-weight: bold; border-bottom: 2px solid transparent;">Collection</div>
                    <div class="tag-tab" data-tab="schedule" style="padding: 8px 0; cursor: pointer; opacity: 0.6; font-weight: bold; border-bottom: 2px solid transparent;">Schedule</div>
                    <div class="tag-tab" data-tab="advanced" style="padding: 8px 0; cursor: pointer; opacity: 0.6; font-weight: bold; border-bottom: 2px solid transparent;">Blacklist</div>
                    <div class="tag-tab" data-tab="homescreen" style="padding: 8px 0; cursor: pointer; opacity: 0.6; font-weight: bold; border-bottom: 2px solid transparent;">Home Screen</div>
                </div>
                
                <div class="tab-content general-tab">
                    <div class="inputContainer" style="flex-grow:1;"><input is="emby-input" class="txtEntryLabel" type="text" label="Display Name" value="${labelName}" /></div>
                    
                    <div style="margin-bottom: 15px;">
                        <label class="selectLabel">Source Type</label>
                        <select is="emby-select" class="selSourceType" style="width:100%;">
                            <option value="" ${!sourceType ? 'selected' : ''}>-- Select source type --</option>
                            <option value="External" ${sourceType === 'External' ? 'selected' : ''}>External List (Trakt/MDBList)</option>
                            <option value="LocalCollection" ${sourceType === 'LocalCollection' ? 'selected' : ''}>Local Collection</option>
                            <option value="LocalPlaylist" ${sourceType === 'LocalPlaylist' ? 'selected' : ''}>Local Playlist</option>
                            <option value="MediaInfo" ${sourceType === 'MediaInfo' ? 'selected' : ''}>Local Media Information (Smart Playlist)</option>
                            <option value="AI" ${sourceType === 'AI' ? 'selected' : ''}>AI created lists</option>
                        </select>
                    </div>

                    <div class="source-external-container" style="display: ${sourceType === 'External' ? 'block' : 'none'};">
                        <div style="display:flex; align-items:baseline; gap:10px; margin:10px 0 10px 0;">
                            <p style="margin:0; font-size:0.9em; font-weight:bold; opacity:0.7;">Source URLs</p>
                            <span style="font-size:0.75em; opacity:0.5;">— Find lists: <a href="https://trakt.tv/discover" target="_blank" style="color:inherit; text-decoration:underline;">Trakt</a> &middot; <a href="https://mdblist.com/toplists/" target="_blank" style="color:inherit; text-decoration:underline;">MDBList</a></span>
                        </div>
                        <div class="url-list-container">${urls.map(u => getUrlRowHtml(u.url, u.limit)).join('')}</div>
                        <div style="margin-top:10px;"><button is="emby-button" type="button" class="raised btnAddUrl" style="width:100%; background:transparent; border:2px dashed rgba(128,128,128,0.4); color:var(--theme-text-secondary);"><i class="md-icon" style="margin-right:5px;">add</i>Add another URL</button></div>
                    </div>

                    <div class="source-local-container" style="display: ${(sourceType === 'LocalCollection' || sourceType === 'LocalPlaylist') ? 'block' : 'none'};">
                        <p style="margin:10px 0 10px 0; font-size:0.9em; font-weight:bold; opacity:0.7;" class="local-type-label">${sourceType === 'LocalPlaylist' ? 'Select Playlists' : 'Select Collections'}</p>
                        <div class="local-list-container">${localSources.map(ls => getLocalRowHtml(sourceType, ls.id, ls.limit)).join('')}</div>
                        <div style="margin-top:10px;"><button is="emby-button" type="button" class="raised btnAddLocal" style="width:100%; background:transparent; border:2px dashed rgba(128,128,128,0.4); color:var(--theme-text-secondary);"><i class="md-icon" style="margin-right:5px;">add</i>Add another</button></div>
                    </div>

                    <div class="source-ai-container" style="display: ${sourceType === 'AI' ? 'block' : 'none'};">
                        <div style="margin-bottom: 15px;">
                            <label class="selectLabel">AI Provider</label>
                            <select is="emby-select" class="selAiProvider" style="width:100%;">
                                <option value="OpenAI" ${(tagConfig.AiProvider || 'OpenAI') === 'OpenAI' ? 'selected' : ''}>OpenAI (ChatGPT)</option>
                                <option value="Gemini" ${(tagConfig.AiProvider || 'OpenAI') === 'Gemini' ? 'selected' : ''}>Google Gemini</option>
                            </select>
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
                                    ${(_miUsers || []).map(function(u) { return '<option value="' + u.Id + '"' + (u.Id === (tagConfig.AiRecentlyWatchedUserId || '') ? ' selected' : '') + '>' + u.Name + '</option>'; }).join('')}
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

                        <div style="margin-top:0;">
                            <button type="button" is="emby-button" class="raised btnTestAiSource btn-neutral" style="background:transparent; border:1px solid rgba(128,128,128,0.4); color:var(--theme-text-secondary);">
                                <i class="md-icon" style="margin-right:5px;">science</i>Test AI Source
                            </button>
                            <span class="ai-test-result" style="margin-left:10px; font-size:0.85em; opacity:0.7;"></span>
                        </div>
                    </div>

                    <div class="source-mediainfo-container" style="display: ${sourceType && sourceType !== '' ? 'block' : 'none'};">
                        <div class="mi-limit-row" style="display:${sourceType === 'MediaInfo' ? 'flex' : 'none'}; align-items:center; gap:12px; margin-bottom:14px; flex-wrap:wrap;">
                            <label style="font-size:0.9em; white-space:nowrap; margin:0;">Max items</label>
                            <input is="emby-input" class="txtMediaInfoLimit" type="number" value="${mediaInfoLimit}" min="0" style="width:90px;" />
                            <button type="button" is="emby-button" class="btnMiHelp raised" style="margin-left:auto; background:transparent; border:1px solid rgba(128,128,128,0.35); color:var(--theme-text-secondary); font-size:0.82em; padding:0 10px; min-width:0;"><i class="md-icon" style="font-size:1em; margin-right:4px;">help_outline</i><span>How to (filter guide)</span></button>
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
                                <button type="button" is="emby-button" class="btnAddMediaInfoFilter raised" style="flex:1; background:transparent; border:2px dashed rgba(128,128,128,0.4); color:var(--theme-text-secondary);"><i class="md-icon" style="margin-right:5px;">add</i>Add Filter Group</button>
                                <button type="button" is="emby-button" class="btnClearAllFilters raised" style="background:transparent; border:2px dashed rgba(204,51,51,0.4); color:#cc3333; padding:0 14px; min-width:0;" title="Clear all filters"><i class="md-icon" style="font-size:1em;">delete_sweep</i></button>
                            </div>
                            <div class="mi-presets-section" style="margin-top:30px; border-top:1px solid var(--line-color); padding-top:12px; display:${sourceType === 'MediaInfo' ? 'block' : 'none'};">
                            ${true ? `
                                <button type="button" is="emby-button" class="btnPremadeFilters raised btn-neutral" style="width:100%; margin-bottom:6px; background:transparent; border:1px solid var(--line-color); color:var(--theme-text-secondary); display:flex; align-items:center; justify-content:space-between;"><span><i class="md-icon" style="margin-right:5px;">auto_awesome</i>Premade filters</span><i class="md-icon mi-expand-icon" style="transition:transform 0.2s; font-size:1.2em;">expand_more</i></button>
                                <div class="mi-preset-panel" style="display:none; border:1px solid var(--line-color); border-radius:6px; padding:10px 12px; margin-bottom:8px; background:rgba(128,128,128,0.06);">
                                    <div style="font-size:0.8em; color:var(--theme-text-secondary); margin-bottom:10px;">Select a preset to replace the current filters:</div>
                                    ${MI_PRESETS.map(function(cat, ci) {
                                        return '<div style="margin-bottom:10px;">' +
                                            '<div style="font-size:0.72em; text-transform:uppercase; letter-spacing:1px; color:var(--theme-text-secondary); margin-bottom:5px;">' + cat.label + '</div>' +
                                            '<div style="display:flex; flex-wrap:wrap; gap:6px;">' +
                                            cat.presets.map(function(p, pi) {
                                                return '<button type="button" class="btnApplyMiPreset" data-preset="' + ci + ',' + pi + '"' +
                                                    ' style="border:1.5px solid #000; border-radius:14px; padding:4px 12px; font-size:0.82em; cursor:pointer; background:transparent; color:var(--theme-text-primary);">' + p.name + '</button>';
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
                    <div class="checkboxContainer checkboxContainer-withDescription">
                        <label>
                            <input is="emby-checkbox" type="checkbox" class="chkEnableTag" ${enableTag} />
                            <span>Apply Tag</span>
                        </label>
                        <div class="fieldDescription">Automatically tag matched items in Emby.</div>
                    </div>
                    <div class="tag-settings" style="margin-left: 20px; padding-left: 15px; border-left: 2px solid var(--line-color); margin-top: 10px; display: ${tagConfig.EnableTag !== false ? 'block' : 'none'};">
                        <div class="inputContainer" style="flex-grow:1;"><input is="emby-input" class="txtTagName" type="text" label="Tag Name" value="${tagName}" placeholder="${labelName}" /></div>
                        <div class="mi-tag-target-section" style="margin-top:14px;">
                            <div style="font-size:0.85em; opacity:0.6; margin-bottom:6px; display:flex; align-items:center; gap:8px;">
                                <span>For TV shows, choose what level to tag:</span>
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
                    </div>
            </div>

                <div class="tab-content schedule-tab" style="display:none;">
                    <p style="margin:0 0 15px 0; font-size:0.9em; opacity:0.8;">Define when this tag should be active. If empty, it's always active.</p>
                    <div class="date-list-container">${intervals.map(i => getDateRowHtml(i)).join('')}</div>
                    <button is="emby-button" type="button" class="btnAddDate" style="width:100%; background:transparent; border:2px dashed rgba(128,128,128,0.4); color:var(--theme-text-secondary); margin-top:10px;"><i class="md-icon" style="margin-right:5px;">event</i>Add Schedule Rule</button>
                    <div class="checkboxContainer checkboxContainer-withDescription" style="margin-top:16px; ${intervals.length === 0 ? 'opacity:0.4;' : ''}">
                        <label>
                            <input is="emby-checkbox" type="checkbox" class="chkOverrideWhenActive" ${overrideChecked} ${intervals.length === 0 ? 'disabled' : ''} />
                            <span>Priority override when active</span>
                        </label>
                        <div class="fieldDescription">When this entry is in schedule, all other entries sharing the same tag or collection are suppressed — only this entry's items keep the tag and collection.</div>
                    </div>
                </div>

                <div class="tab-content collection-tab" style="display:none;">
                    <div class="checkboxContainer checkboxContainer-withDescription">
                        <label>
                            <input is="emby-checkbox" type="checkbox" class="chkEnableCollection" ${enableColl} />
                            <span>Create Collection</span>
                        </label>
                        <div class="fieldDescription">Automatically create and maintain an Emby Collection from these items.</div>
                    </div>
                    
                    <div class="collection-settings" style="margin-left: 20px; padding-left: 15px; border-left: 2px solid var(--line-color); margin-top: 10px; display: ${tagConfig.EnableCollection ? 'block' : 'none'};">
                        <div class="inputContainer">
                            <input is="emby-input" type="text" class="txtCollectionName" label="Collection Name" value="${collName}" placeholder="${labelName}" />
                            <div class="fieldDescription">Leave empty to use Display Name.</div>
                        </div>

                        <div class="mi-coll-target-section" style="margin-top:10px;">
                            <div style="font-size:0.85em; opacity:0.6; margin-bottom:6px; display:flex; align-items:center; gap:8px;">
                                <span>For TV shows, choose what level to add to collection:</span>
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
                            <p style="margin:0 0 8px 0; font-size:0.9em; font-weight:bold; opacity:0.7;">Collection Poster</p>
                            <div class="poster-preview-container" style="margin-bottom:8px; display:${collPosterPath ? 'block' : 'none'};">
                                <span class="poster-filename" style="font-size:0.85em; opacity:0.7;">${collPosterPath ? collPosterPath.split(/[\\\\/]/).pop() : ''}</span>
                                <button type="button" class="btnRemovePoster" style="margin-left:10px; font-size:0.8em; background:transparent; border:none; color:#e55; cursor:pointer; vertical-align:middle;">✕ Remove</button>
                            </div>
                            <img class="poster-preview-img" src="" alt="" style="max-width:120px; max-height:180px; border-radius:4px; display:none; margin-bottom:8px;" />
                            <input type="file" class="inputPosterFile" accept="image/*" style="display:none;" />
                            <input type="hidden" class="hiddenPosterPath" value="${collPosterPath}" />
                            <button type="button" is="emby-button" class="btnChoosePoster raised" style="width:100%; background:transparent; border:2px dashed rgba(128,128,128,0.4); color:var(--theme-text-secondary);">
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
                    </div>
                </div>

                <div class="tab-content advanced-tab" style="display:none;">
                    <div class="inputContainer">
                        <p style="margin:0 0 5px 0; font-size:0.9em; font-weight:bold; opacity:0.7;">Blacklist / Ignore (IMDB IDs)</p>
                        <textarea class="txtTagBlacklist" rows="1" placeholder="tt1234567&#10;tt9876543" style="width:100%;resize:none;overflow:hidden;padding:6px 8px;font-size:inherit;font-family:inherit;background:var(--input-background,rgba(255,255,255,0.08));border:1px solid var(--input-border,rgba(255,255,255,0.2));border-radius:3px;color:inherit;line-height:1.4;max-height:120px;overflow-y:auto;">${blacklist}</textarea>
                        <div class="fieldDescription">Items with these IDs will never be tagged or added to collection.</div>
                    </div>
                </div>

                <div class="tab-content homescreen-tab" style="display:none;"
                    data-hse-libraryid="${homeSectionLibraryId}"
                    data-hse-userids="${homeSectionUserIdsEnc}"
                    data-hse-settings="${homeSectionSettingsEnc}"
                    data-hse-tracked="${homeSectionTrackedEnc}"
                    data-hse-default-type="${hsDefaultSectionType}"
                    data-hse-loaded="0">
                    <div class="checkboxContainer checkboxContainer-withDescription">
                        <label>
                            <input is="emby-checkbox" class="chkEnableHomeSection" type="checkbox" ${enableHomeSection} ${disableHomeSection}/>
                            <span>Add as home screen section</span>
                        </label>
                        <div class="fieldDescription">A home screen section will be managed for selected users each time sync runs.</div>
                        <div class="hse-disabled-hint" style="font-size:0.9em; color:#e07070; margin-top:4px; display:${(tagConfig.EnableTag === false && !tagConfig.EnableCollection) ? 'block' : 'none'};">Requires <strong>Apply Tag</strong> or <strong>Create Collection</strong> to be enabled.</div>
                    </div>
                    <div class="hse-details" style="display:${enableHomeSection ? 'block' : 'none'}; margin-top:15px;">
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
                    <button is="emby-button" type="button" class="raised button-submit btnRunEntry" style="background:#0099d5 !important; color:#fff !important;"><i class="md-icon" style="margin-right:5px;">play_arrow</i><span class="btnRunEntryLabel">Run Group</span></button>
                    <button is="emby-button" type="button" class="raised btnRemoveGroup" style="background:#cc3333 !important; color:#fff;"><i class="md-icon" style="margin-right:5px;">delete</i>Remove Group</button>
                </div>
            </div>
        </div>`;

        if (afterRef) afterRef.insertAdjacentHTML('afterend', html);
        else if (prepend) container.insertAdjacentHTML('afterbegin', html);
        else container.insertAdjacentHTML('beforeend', html);

        var newRow = afterRef ? afterRef.nextElementSibling : (prepend ? container.firstElementChild : container.lastElementChild);
        setupRowEvents(newRow);
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

    function setupRowEvents(row) {
        function updateBadges(row) {
            var container = row.querySelector('.badge-container');
            if (!container) return;

            var hasSchedule = row.querySelectorAll('.date-row').length > 0;
            var hasCollection = row.querySelector('.chkEnableCollection').checked;
            var hasHomeSection = row.querySelector('.chkEnableHomeSection').checked;
            var hasTag = row.querySelector('.chkEnableTag').checked;
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
            if (hasHomeSection) {
                html += `<span class="tag-indicator homescreen"><i class="md-icon" style="font-size:1.1em;">home</i> Home Section</span>`;
            }
            if (hasTag) {
                html += `<span class="tag-indicator tag"><i class="md-icon" style="font-size:1.1em;">label</i> Tag</span>`;
            }
            container.innerHTML = html;
        }

        row.querySelectorAll('.tag-tab').forEach(tab => {
            tab.addEventListener('click', function () {
                row.querySelectorAll('.tag-tab').forEach(t => { t.style.opacity = "0.6"; t.style.borderBottomColor = "transparent"; });
                this.style.opacity = "1"; this.style.borderBottomColor = "#52B54B";
                var target = this.getAttribute('data-tab');
                row.querySelector('.general-tab').style.display = target === 'general' ? 'block' : 'none';
                row.querySelector('.tagname-tab').style.display = target === 'tag' ? 'block' : 'none';
                row.querySelector('.schedule-tab').style.display = target === 'schedule' ? 'block' : 'none';
                row.querySelector('.collection-tab').style.display = target === 'collection' ? 'block' : 'none';
                row.querySelector('.advanced-tab').style.display = target === 'advanced' ? 'block' : 'none';
                row.querySelector('.homescreen-tab').style.display = target === 'homescreen' ? 'block' : 'none';
                if (target === 'homescreen') initHomeSectionTab(row);
            });
        });

        row.addEventListener('change', e => {
            if (e.target.classList.contains('selSourceType')) {
                var type = e.target.value;
                row.querySelector('.source-external-container').style.display = type === 'External' ? 'block' : 'none';
                row.querySelector('.source-local-container').style.display = (type === 'LocalCollection' || type === 'LocalPlaylist') ? 'block' : 'none';
                row.querySelector('.source-mediainfo-container').style.display = type ? 'block' : 'none';
                row.querySelector('.source-ai-container').style.display = type === 'AI' ? 'block' : 'none';

                var isMi = type === 'MediaInfo';
                var miLimitRow      = row.querySelector('.mi-limit-row');
                var miToggleRow     = row.querySelector('.mi-toggle-row');
                var miFilterBody    = row.querySelector('.mi-filter-body');
                var miHelpBtnRow    = row.querySelector('.mi-help-btn-row');
                var miPresetsSection = row.querySelector('.mi-presets-section');
                if (miPresetsSection) miPresetsSection.style.display = isMi ? 'block' : 'none';
                if (miLimitRow) miLimitRow.style.display = isMi ? 'flex' : 'none';
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
            lblStatus.style.color = this.checked ? "#52B54B" : "var(--theme-text-secondary)";
            if (this.checked) row.classList.remove('inactive');
            else row.classList.add('inactive');
            updateRunGroupBtn();
        });
        updateRunGroupBtn();

        row.querySelector('.chkEnableTag').addEventListener('change', function () {
            row.querySelector('.tag-settings').style.display = this.checked ? 'block' : 'none';
            updateBadges(row);
            updateHseSectionAvailability(row);
        });

        row.querySelector('.chkEnableCollection').addEventListener('change', function () {
            row.querySelector('.collection-settings').style.display = this.checked ? 'block' : 'none';
            updateBadges(row);
            updateHseSectionAvailability(row);
        });

        row.querySelector('.chkOverrideWhenActive').addEventListener('change', function () {
            updateBadges(row);
        });

        row.querySelector('.chkEnableHomeSection').addEventListener('change', function () {
            var hseDetails = row.querySelector('.hse-details');
            if (hseDetails) hseDetails.style.display = this.checked ? 'block' : 'none';
            updateBadges(row);
        });

        var chkAiWatched = row.querySelector('.chkAiRecentlyWatched');
        if (chkAiWatched) {
            chkAiWatched.addEventListener('change', function () {
                var opts = row.querySelector('.ai-recently-watched-options');
                if (opts) opts.style.display = this.checked ? 'block' : 'none';
            });
        }

        row.querySelector('.btnAddUrl').addEventListener('click', () => {
            row.querySelector('.url-list-container').insertAdjacentHTML('beforeend', getUrlRowHtml('', 0));
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
            document.getElementById('tagTargetHelpModalOverlay').classList.add('modal-visible');
        });

        row.querySelector('.btnCollTargetHelp').addEventListener('click', () => {
            document.getElementById('tagTargetHelpModalOverlay').classList.add('modal-visible');
        });

        row.addEventListener('click', e => {
            if (e.target.closest('.btnMiHelp')) {
                document.getElementById('miHelpModalOverlay').classList.add('modal-visible');
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

            if (e.target.closest('.btnRemoveLocal')) {
                e.target.closest('.local-row').remove();
            }

            if (e.target.closest('.btnRemoveDate')) {
                e.target.closest('.date-row').remove();
                updateBadges(row);
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
                        ? (newOp === 'AND' ? 'rgba(0,164,220,0.75)' : 'rgba(220,120,0,0.75)')
                        : 'transparent';
                    b.style.color = active ? '#fff' : '';
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
                        ? (newOp === 'AND' ? 'rgba(0,164,220,0.75)' : 'rgba(220,120,0,0.75)')
                        : 'transparent';
                    b.style.color = active ? '#fff' : '';
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
                btn.style.background = active ? 'rgba(200,50,50,0.75)' : 'transparent';
                btn.style.color = active ? '#fff' : '';
                btn.style.border = active ? '1px solid rgba(200,50,50,0.6)' : '1px solid rgba(128,128,128,0.4)';
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
                if (confirm("Delete this tag group?")) {
                    row.remove();
                }
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
                    fetch(window.ApiClient.getUrl('HomeScreenCompanion/RunEntry'), {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json', 'X-MediaBrowser-Token': window.ApiClient.accessToken() },
                        body: JSON.stringify({ EntryName: entryName })
                    }).then(function (r) { return r.json(); })
                    .then(function (result) {
                        lbl.textContent = 'Run Group';
                        btn.disabled = false;
                        if (view) refreshStatus(view);
                        window.Dashboard.alert(result.Success ? ('Done: ' + result.Message) : ('Failed: ' + result.Message));
                    }).catch(function () {
                        lbl.textContent = 'Run Group';
                        btn.disabled = false;
                        if (view) refreshStatus(view);
                        window.Dashboard.alert('Request failed.');
                    });
                };
                var _saveBtn = row.closest('#HomeScreenCompanionConfigPage') ? row.closest('#HomeScreenCompanionConfigPage').querySelector('.btn-save') : document.querySelector('.btn-save');
                var isDirty = _saveBtn && !_saveBtn.disabled;
                if (isDirty) {
                    if (confirm('You have unsaved changes. Save and run?')) {
                        _saveBtn.click();
                        setTimeout(doRun, 800);
                    }
                } else {
                    doRun();
                }
            }

            var btnTest = e.target.closest('.btnTestUrl');
            if (btnTest) {
                var uRow = btnTest.closest('.url-row');
                var url = uRow.querySelector('.txtTagUrl').value;
                if (!url) return;

                btnTest.disabled = true;
                window.ApiClient.getJSON(window.ApiClient.getUrl("HomeScreenCompanion/TestUrl", { Url: url, Limit: 1000 })).then(result => {
                    window.Dashboard.alert(result.Message);
                }).finally(() => btnTest.disabled = false);
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
                    if (result.Success && result.Preview && result.Preview.length > 0) {
                        window.Dashboard.alert('AI returned ' + result.Count + ' items:\n\n' + result.Preview.join('\n'));
                    } else if (!result.Success) {
                        window.Dashboard.alert('AI test failed:\n' + result.Message);
                    }
                }).catch(function(err) {
                    if (resultSpan) resultSpan.textContent = 'Error: ' + err.message;
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
        row.querySelector('.txtEntryLabel').addEventListener('input', updateTagTitle);
        row.querySelector('.txtTagName').addEventListener('input', updateTagTitle);

        var handle = row.querySelector('.drag-handle');

        handle.addEventListener('mousedown', () => {
            if (localStorage.getItem('HomeScreenCompanion_SortBy') === 'Manual') {
                row.setAttribute('draggable', 'true');
            }
        });

        handle.addEventListener('mouseup', () => {
            row.setAttribute('draggable', 'false');
        });

        handle.addEventListener('touchstart', (e) => {
            if (localStorage.getItem('HomeScreenCompanion_SortBy') !== 'Manual') return;
            e.preventDefault();
            var tagContainer = row.closest('#tagListContainer') || row.parentElement;
            document.querySelectorAll('.tag-body').forEach(b => b.style.display = 'none');
            document.querySelectorAll('.expand-icon').forEach(i => i.innerText = 'expand_more');
            row.classList.add('dragging');

            const onTouchMove = (ev) => {
                ev.preventDefault();
                const touch = ev.touches[0];
                const afterEl = getDragAfterElement(tagContainer, touch.clientY);
                let ph = tagContainer.querySelector('.sort-placeholder');
                if (!ph) { ph = document.createElement('div'); ph.className = 'sort-placeholder'; }
                if (afterEl == null) { if (ph.nextElementSibling !== null) tagContainer.appendChild(ph); }
                else { if (ph.nextElementSibling !== afterEl) tagContainer.insertBefore(ph, afterEl); }
            };

            const onTouchEnd = () => {
                document.removeEventListener('touchmove', onTouchMove);
                document.removeEventListener('touchend', onTouchEnd);
                document.removeEventListener('touchcancel', onTouchCancel);
                row.classList.remove('dragging');
                const ph = tagContainer.querySelector('.sort-placeholder');
                if (ph) { tagContainer.insertBefore(row, ph); ph.remove(); }
                row.classList.add('just-moved');
                setTimeout(() => { row.classList.remove('just-moved'); }, 2000);
                setTimeout(checkFormState, 0);
            };

            const onTouchCancel = () => {
                document.removeEventListener('touchmove', onTouchMove);
                document.removeEventListener('touchend', onTouchEnd);
                document.removeEventListener('touchcancel', onTouchCancel);
                row.classList.remove('dragging');
                const ph = tagContainer.querySelector('.sort-placeholder');
                if (ph) ph.remove();
            };

            document.addEventListener('touchmove', onTouchMove, { passive: false });
            document.addEventListener('touchend', onTouchEnd);
            document.addEventListener('touchcancel', onTouchCancel);
        }, { passive: false });

        row.querySelector('.btnDuplicateRow').addEventListener('click', () => {
            var config = readRowAsConfig(row);
            config.Name = (config.Name || config.Tag || 'Source') + ' (copy)';
            renderTagGroup(config, row.closest('#tagListContainer'), true, undefined, true);
            applyFilters(view);
            setTimeout(checkFormState, 0);
        });

        row.addEventListener('dragstart', (e) => {
            if (localStorage.getItem('HomeScreenCompanion_SortBy') !== 'Manual') { e.preventDefault(); return; }

            document.querySelectorAll('.tag-body').forEach(b => b.style.display = 'none');
            document.querySelectorAll('.expand-icon').forEach(i => i.innerText = 'expand_more');

            row.classList.add('dragging');
            e.dataTransfer.effectAllowed = 'move';
            e.dataTransfer.setData('text/plain', '');

            setTimeout(() => {
                row.style.display = 'none';
            }, 0);
        });

        row.addEventListener('dragend', () => {
            row.style.display = '';

            row.classList.remove('dragging');
            row.setAttribute('draggable', 'false');

            var existingPlaceholder = document.querySelector('.sort-placeholder');
            if (existingPlaceholder) existingPlaceholder.remove();

            row.classList.add('just-moved');
            setTimeout(() => { row.classList.remove('just-moved'); }, 2000);

            setTimeout(checkFormState, 0);
        });

        row.querySelector('.btnChoosePoster').addEventListener('click', function () {
            row.querySelector('.inputPosterFile').click();
        });

        row.querySelector('.inputPosterFile').addEventListener('change', function () {
            var file = this.files[0];
            if (!file) return;
            var reader = new FileReader();
            reader.onload = function (e) {
                var dataUrl = e.target.result;
                var base64 = dataUrl.split(',')[1];
                var img = row.querySelector('.poster-preview-img');
                img.src = dataUrl;
                img.style.display = 'block';

                var headers = { 'Content-Type': 'application/json' };
                var token = window.ApiClient.accessToken();
                if (token) headers['X-Emby-Token'] = token;
                fetch(window.ApiClient.getUrl('HomeScreenCompanion/UploadCollectionImage'), {
                    method: 'POST',
                    headers: headers,
                    body: JSON.stringify({ FileName: file.name, Base64Data: base64, OldFilePath: row.querySelector('.hiddenPosterPath').value })
                }).then(function (r) { return r.json(); })
                .then(function (result) {
                    if (result.Success) {
                        row.querySelector('.hiddenPosterPath').value = result.FilePath;
                        row.querySelector('.poster-filename').textContent = file.name;
                        row.querySelector('.poster-preview-container').style.display = 'block';
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

        row.querySelector('.btnRemovePoster').addEventListener('click', function () {
            row.querySelector('.hiddenPosterPath').value = '';
            row.querySelector('.poster-filename').textContent = '';
            row.querySelector('.poster-preview-container').style.display = 'none';
            row.querySelector('.poster-preview-img').style.display = 'none';
            row.querySelector('.inputPosterFile').value = '';
        });

        row.querySelector('.btnLoadPosterUrl').addEventListener('click', function () {
            var url = row.querySelector('.txtPosterUrl').value.trim();
            if (!url) return;

            var headers = { 'Content-Type': 'application/json' };
            var token = window.ApiClient.accessToken();
            if (token) headers['X-Emby-Token'] = token;

            fetch(window.ApiClient.getUrl('HomeScreenCompanion/FetchCollectionImageFromUrl'), {
                method: 'POST',
                headers: headers,
                body: JSON.stringify({ Url: url, OldFilePath: row.querySelector('.hiddenPosterPath').value })
            }).then(function (r) { return r.json(); })
            .then(function (result) {
                if (result.Success) {
                    row.querySelector('.hiddenPosterPath').value = result.FilePath;
                    row.querySelector('.poster-filename').textContent = url.split('/').pop().split('?')[0];
                    row.querySelector('.poster-preview-container').style.display = 'block';
                    var img = row.querySelector('.poster-preview-img');
                    img.src = url;
                    img.style.display = 'block';
                    row.querySelector('.txtPosterUrl').value = '';
                } else {
                    window.Dashboard.alert('Failed to load image: ' + (result.Message || 'Unknown error'));
                }
            }).catch(function () {
                window.Dashboard.alert('Error fetching image. Check the URL and server logs.');
            });
        });

        setTimeout(function () { updateHseSectionAvailability(row); }, 0);
    }


    function refreshStatus(view) {
        var myId = ++statusRequestId;
        Promise.all([
            window.ApiClient.getJSON(window.ApiClient.getUrl("HomeScreenCompanion/Status")),
            window.ApiClient.getJSON(window.ApiClient.getUrl("HomeScreenCompanion/Hsc/Status")).catch(function () { return null; })
        ]).then(function (results) {
            if (myId !== statusRequestId) return;
            var result = results[0], hscResult = results[1];
            var label = view.querySelector('#lastRunStatusLabel'), dot = view.querySelector('#dotStatus'), content = view.querySelector('#logContent');
            var btnSave = view.querySelector('.btn-save'), btnRun = view.querySelector('#btnRunSync');

            var eitherRunning = result.IsRunning || (hscResult && hscResult.IsRunning);
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

            if (label) label.textContent = result.LastRunStatus || "Never";
            if (dot) {
                dot.className = "status-dot";
                if (result.LastRunStatus.includes("Running")) dot.classList.add("running");
                else if (result.LastRunStatus.includes("Failed")) dot.classList.add("failed");
            }

            if (content) {
                function getLogTime(entry) {
                    var m = entry.match(/^\[(\d{2}:\d{2}:\d{2})\]/);
                    return m ? m[1] : '00:00:00';
                }
                var allLogs = [];
                (result.Logs || []).forEach(function (l) {
                    allLogs.push({ t: getLogTime(l), text: l.replace(/^(\[\d{2}:\d{2}:\d{2}\]) /, '$1 [HSC] ') });
                });
                (hscResult && hscResult.Logs || []).forEach(function (l) {
                    allLogs.push({ t: getLogTime(l), text: l.replace(/^(\[\d{2}:\d{2}:\d{2}\]) /, '$1 [Home Screen] ') });
                });
                allLogs.sort(function (a, b) { return a.t.localeCompare(b.t); });
                content.textContent = allLogs.map(function (x) { return x.text; }).join('\n') || '(no logs yet)';
            }
        }).catch(function () {
            if (myId !== statusRequestId) return;
        });
    }

    function sortRows(container, criteria) {
        var rows = Array.from(container.querySelectorAll('.tag-row'));

        rows.sort((a, b) => {
            if (criteria === 'Name') {
                var na = (a.querySelector('.txtEntryLabel').value || a.querySelector('.txtTagName').value).toLowerCase();
                var nb = (b.querySelector('.txtEntryLabel').value || b.querySelector('.txtTagName').value).toLowerCase();
                return na.localeCompare(nb);
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
            return parseInt(a.dataset.index) - parseInt(b.dataset.index);
        });

        rows.forEach(row => container.appendChild(row));

        if (criteria !== 'Manual') container.classList.add('sort-hidden');
        else container.classList.remove('sort-hidden');
    }

    // ---- Home Section Tab helpers ----
    var _hseUsersCache = null;
    function getHseUsers() {
        if (_hseUsersCache) return Promise.resolve(_hseUsersCache);
        return window.ApiClient.getJSON(window.ApiClient.getUrl('Users', { IsDisabled: false }))
            .then(function(users) {
                _hseUsersCache = (users || []).map(function(u) { return { Id: u.Id, Name: u.Name }; });
                return _hseUsersCache;
            });
    }



    function buildHomeSectionFormHtml(savedSettings, defaultSectionType, defaultName, tagEnabled, collEnabled) {
        var s = savedSettings || {};
        var html = '';

        var st = s.SectionType || defaultSectionType || (collEnabled ? 'boxset' : 'items');
        html += '<div style="margin-bottom:12px;"><label class="selectLabel">Section Type</label>';
        html += '<select is="emby-select" class="selHseSectionType hse-field-str" data-field="SectionType" style="width:100%;">';
        var sectionTypeOptions = [];
        if (collEnabled) sectionTypeOptions.push(['boxset', 'Single Collection']);
        if (tagEnabled)  sectionTypeOptions.push(['items',  'Dynamic Media (tag)']);
        sectionTypeOptions.forEach(function(o) {
            html += '<option value="' + o[0] + '"' + (st === o[0] ? ' selected' : '') + '>' + o[1] + '</option>';
        });
        html += '</select></div>';

        var savedItemTypes = [];
        try { savedItemTypes = JSON.parse(s.ItemTypes || '[]'); } catch {}
        var savedItemTypesStr = savedItemTypes.length > 0 ? savedItemTypes.join(',') : 'Movie,Series';
        html += '<div class="hse-items-only" style="margin-bottom:12px;"><label class="selectLabel">Media Type</label>';
        html += '<select is="emby-select" class="selHseItemTypes" style="width:100%;">';
        [
            ['Movie',        'Movies'],
            ['Series',       'Shows'],
            ['Movie,Series', 'Movies & Shows'],
            ['Episode',      'Episodes'],
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
        ].forEach(function(o) {
            html += '<option value="' + o[0] + '"' + (savedItemTypesStr === o[0] ? ' selected' : '') + '>' + o[1] + '</option>';
        });
        html += '</select></div>';

        var customName = (s.CustomName || '').replace(/"/g, '&quot;');
        var customNamePlaceholder = (defaultName || '').replace(/"/g, '&quot;');
        html += '<div style="margin-bottom:12px;"><input is="emby-input" type="text" class="hse-field-str" data-field="CustomName" label="Custom Title" value="' + customName + '" placeholder="' + customNamePlaceholder + '"/></div>';

        var viewTypeVal = s.ViewType || 'cards';
        html += '<div class="hse-items-only" style="margin-bottom:12px;"><label class="selectLabel">View Type</label>';
        html += '<select is="emby-select" class="selHseViewType hse-field-str" data-field="ViewType" style="width:100%;">';
        [['cards','Cards (default)'],['spotlight','Spotlight'],['list','List'],['buttons','Buttons']].forEach(function(o) {
            html += '<option value="' + o[0] + '"' + (viewTypeVal === o[0] ? ' selected' : '') + '>' + o[1] + '</option>';
        });
        html += '</select></div>';

        var imgTypeVal = s.ImageType || '';
        var imgTypeDisabled = viewTypeVal !== 'cards';
        html += '<div style="margin-bottom:12px;"><label class="selectLabel">Image Type</label>';
        html += '<select is="emby-select" class="selHseImageType hse-field-str" data-field="ImageType" style="width:100%;"' + (imgTypeDisabled ? ' disabled' : '') + '><option value=""' + (imgTypeVal === '' ? ' selected' : '') + '>Auto</option>';
        ['Primary','Thumb'].forEach(function(o) { html += '<option value="' + o + '"' + (imgTypeVal === o ? ' selected' : '') + '>' + o + '</option>'; });
        html += '</select></div>';

        var sortByVal = s.SortBy || '';
        html += '<div style="margin-bottom:12px;"><label class="selectLabel">Sort By</label>';
        html += '<select is="emby-select" class="hse-field-str" data-field="SortBy" style="width:100%;">';
        html += '<option value=""' + (sortByVal === '' ? ' selected' : '') + '>(Default)</option>';
        [['CommunityRating','Rating'],['DateCreated','Date Added'],['SortName','Name'],
         ['Runtime','Runtime'],['PremiereDate','Release Date'],['ProductionYear','Year'],['Random','Random']].forEach(function(o) {
            html += '<option value="' + o[0] + '"' + (sortByVal === o[0] ? ' selected' : '') + '>' + o[1] + '</option>';
        });
        html += '</select></div>';

        var sortOrderVal = s.SortOrder || '';
        html += '<div style="margin-bottom:12px;"><label class="selectLabel">Sort Order</label>';
        html += '<select is="emby-select" class="hse-field-str" data-field="SortOrder" style="width:100%;">';
        html += '<option value=""' + (sortOrderVal === '' ? ' selected' : '') + '>(Default)</option>';
        [['Ascending','Ascending'],['Descending','Descending']].forEach(function(o) { html += '<option value="' + o[0] + '"' + (sortOrderVal === o[0] ? ' selected' : '') + '>' + o[1] + '</option>'; });
        html += '</select></div>';

        var dispModeVal = s.ScrollDirection || '';
        html += '<div style="margin-bottom:12px;"><label class="selectLabel">Scroll Direction</label>';
        html += '<select is="emby-select" class="hse-field-str" data-field="ScrollDirection" style="width:100%;"><option value="">(Auto)</option>';
        [['Horizontal','Horizontal'],['Vertical','Vertical']].forEach(function(o) { html += '<option value="' + o[0] + '"' + (dispModeVal === o[0] ? ' selected' : '') + '>' + o[1] + '</option>'; });
        html += '</select></div>';

        var playstateVal = s._queryIsPlayed || '';
        html += '<div class="hse-items-only" style="margin-bottom:12px;"><label class="selectLabel">Playstate</label>';
        html += '<select is="emby-select" class="hse-field-str" data-field="_queryIsPlayed" style="width:100%;">';
        html += '<option value=""' + (playstateVal === '' ? ' selected' : '') + '>Any</option>';
        [['true','Played'],['false','Unplayed']].forEach(function(o) { html += '<option value="' + o[0] + '"' + (playstateVal === o[0] ? ' selected' : '') + '>' + o[1] + '</option>'; });
        html += '</select></div>';

        return html;
    }

    function updateHseItemsOnlyVisibility(tab) {
        var stSel = tab.querySelector('.selHseSectionType');
        var isItems = !stSel || stSel.value !== 'boxset';
        tab.querySelectorAll('.hse-items-only').forEach(function(el) {
            el.style.display = isItems ? '' : 'none';
        });
    }

    function updateHseImageTypeState(tab) {
        var stSel = tab.querySelector('.selHseSectionType');
        var vtSel = tab.querySelector('.selHseViewType');
        var imgSel = tab.querySelector('.selHseImageType');
        if (!imgSel) return;
        var isItems = !stSel || stSel.value !== 'boxset';
        var isCards = !vtSel || vtSel.value === 'cards';
        imgSel.disabled = isItems && !isCards;
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

    // Hämta live ContentSection från Emby och spegla värdena i formuläret
    function syncHomeSectionFromEmby(tab) {
        var tracked = [];
        try { tracked = JSON.parse(decodeURIComponent(tab.dataset.hseTracked || '%5B%5D')); } catch {}
        var entry = tracked.find(function(t) { return t.SectionId && !t.SectionId.startsWith('hsc__'); });
        if (!entry) return;

        var syncHeaders = {};
        var syncToken = window.ApiClient && window.ApiClient.accessToken && window.ApiClient.accessToken();
        if (syncToken) syncHeaders['X-Emby-Token'] = syncToken;
        var syncUrl = window.ApiClient
            ? window.ApiClient.getUrl('HomeScreenCompanion/Hsc/UserSections', { UserId: entry.UserId })
            : '/HomeScreenCompanion/Hsc/UserSections?UserId=' + encodeURIComponent(entry.UserId);

        fetch(syncUrl, { headers: syncHeaders })
            .then(function(r) { return r.json(); })
            .then(function(data) {
                var sections = (data && data.Sections) || [];
                var section = sections.find(function(s) { return s.Id === entry.SectionId; });
                if (!section) return;

                // Fält som kan läsas tillbaka från Emby och ha ett formulärfält
                var fieldMap = {
                    SectionType:     section.SectionType || '',
                    CustomName:      section.CustomName || '',
                    ViewType:        section.ViewType || 'cards',
                    ImageType:       section.ImageType || '',
                    SortBy:          section.SortBy || '',
                    SortOrder:       section.SortOrder || '',
                    ScrollDirection: (function() {
                        var sd = section.ScrollDirection;
                        if (sd === null || sd === undefined) return '';
                        if (typeof sd === 'number') return sd === 0 ? 'Horizontal' : sd === 1 ? 'Vertical' : '';
                        return String(sd);
                    })()
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

                updateHseItemsOnlyVisibility(tab);
                updateHseImageTypeState(tab);
            })
            .catch(function() {}); // ignorera nätverksfel tyst
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

        getHseUsers()
            .then(function(users) {

                // User list
                var userHtml = '';
                users.forEach(function(u) {
                    var chk = savedUserIds.indexOf(u.Id) !== -1 ? 'checked' : '';
                    userHtml += '<div style="margin:4px 0;"><label style="display:flex;align-items:center;gap:8px;cursor:pointer;"><input type="checkbox" is="emby-checkbox" class="chkHseUser" value="' + u.Id + '" ' + chk + '/><span>' + u.Name + '</span></label></div>';
                });
                tab.querySelector('.hse-user-list-inner').innerHTML = userHtml || '<em style="opacity:0.5">No users found</em>';

                // Structured section settings form
                var defaultTagName = row.querySelector('.txtEntryLabel').value || row.querySelector('.txtTagName').value || '';
                var tagEnabled  = !!(row.querySelector('.chkEnableTag') || {}).checked;
                var collEnabled = !!(row.querySelector('.chkEnableCollection') || {}).checked;
                tab.querySelector('.hse-fields-inner').innerHTML = buildHomeSectionFormHtml(savedSettings, defaultSectionType, defaultTagName, tagEnabled, collEnabled);
                wireHomeSectionTypeChange(tab);

                // Mark as fully loaded only after form is in DOM so getUiConfig reads form values
                tab.dataset.hseLoaded = '1';

                // Spegla live-värden från Emby ovanpå de sparade inställningarna
                syncHomeSectionFromEmby(tab);

                setTimeout(checkFormState, 0);
            })
            .catch(function(e) {
                tab.querySelector('.hse-fields-inner').innerHTML = '<em style="color:#cc4444">Failed to load: ' + e.message + '</em>';
                tab.dataset.hseLoaded = '0'; // allow retry
            });
    }
    function updateHseSectionAvailability(row) {
        var tagEnabled  = !!(row.querySelector('.chkEnableTag') || {}).checked;
        var collEnabled = !!(row.querySelector('.chkEnableCollection') || {}).checked;
        var hseCbx = row.querySelector('.chkEnableHomeSection');
        if (!hseCbx) return;

        var hint = row.querySelector('.hse-disabled-hint');
        if (!tagEnabled && !collEnabled) {
            hseCbx.disabled = true;
            hseCbx.checked = false;
            if (hint) hint.style.display = 'block';
            var hseDetails = row.querySelector('.hse-details');
            if (hseDetails) hseDetails.style.display = 'none';
            updateBadges(row);
        } else {
            hseCbx.disabled = false;
            if (hint) hint.style.display = 'none';
        }

        var tab = row.querySelector('.homescreen-tab');
        if (tab && tab.dataset.hseLoaded === '1') {
            refreshHseSectionTypeOptions(tab, tagEnabled, collEnabled);
        }
    }

    function refreshHseSectionTypeOptions(tab, tagEnabled, collEnabled) {
        var stSel = tab.querySelector('.selHseSectionType');
        if (!stSel) return;
        var currentVal = stSel.value;
        stSel.innerHTML = '';
        if (collEnabled) { var o1 = document.createElement('option'); o1.value = 'boxset'; o1.textContent = 'Single Collection'; stSel.appendChild(o1); }
        if (tagEnabled)  { var o2 = document.createElement('option'); o2.value = 'items';  o2.textContent = 'Dynamic Media (tag)'; stSel.appendChild(o2); }
        var stillValid = Array.from(stSel.options).some(function(o) { return o.value === currentVal; });
        if (stillValid) stSel.value = currentVal;
        updateHseItemsOnlyVisibility(tab);
    }

    // ---- End Home Section Tab helpers ----

    function getUiConfig(view, forComparison) {
        var flatTags = [];
        view.querySelectorAll('.tag-row').forEach(row => {
            var entryLabel = row.querySelector('.txtEntryLabel').value;
            var name = row.querySelector('.txtTagName').value || entryLabel;
            var active = row.querySelector('.chkTagActive').checked;

            var blInput = row.querySelector('.txtTagBlacklist');
            var bl = blInput ? blInput.value.split(/[\n\r]+/).map(s => s.trim()).filter(s => s.length > 0) : [];

            var enableTagChk = row.querySelector('.chkEnableTag').checked;
            var enableColl = row.querySelector('.chkEnableCollection').checked;
            var overrideWhenActive = !!(row.querySelector('.chkOverrideWhenActive') || {}).checked;

            var collName = row.querySelector('.txtCollectionName').value;
            var collDescription = row.querySelector('.txtCollectionDescription') ? row.querySelector('.txtCollectionDescription').value : '';
            var collPoster = row.querySelector('.hiddenPosterPath') ? row.querySelector('.hiddenPosterPath').value : '';

            var intervals = [];
            row.querySelectorAll('.date-row').forEach(dr => {
                var type = dr.querySelector('.selDateType').value;
                var s = null, e = null, days = "";

                if (type === 'SpecificDate') {
                    s = dr.querySelector('.txtFullStartDate').value;
                    e = dr.querySelector('.txtFullEndDate').value;
                } else if (type === 'EveryYear') {
                    var sM = dr.querySelector('.selStartMonth').value, sD = dr.querySelector('.selStartDay').value;
                    var eM = dr.querySelector('.selEndMonth').value, eD = dr.querySelector('.selEndDay').value;
                    s = `2000-${sM.padStart(2, '0')}-${sD.padStart(2, '0')}`;
                    e = `2000-${eM.padStart(2, '0')}-${eD.padStart(2, '0')}`;
                } else if (type === 'Weekly') {
                    var activeBtns = Array.from(dr.querySelectorAll('.day-toggle.active')).map(b => b.dataset.day);
                    days = activeBtns.join(',');
                }

                intervals.push({ Type: type, Start: s || null, End: e || null, DayOfWeek: days });
            });

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

            var hseTab = row.querySelector('.homescreen-tab');
            var enableHse = hseTab ? !!(hseTab.querySelector('.chkEnableHomeSection') || {}).checked : false;
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
            } else {
                try { hseSettings = JSON.parse(decodeURIComponent((hseTab && hseTab.dataset.hseSettings) || '%7B%7D')); } catch {}
            }
            var hseTracked = (function() { try { return JSON.parse(decodeURIComponent((hseTab && hseTab.dataset.hseTracked) || '%5B%5D')); } catch { return []; } })();

            var baseTag = {
                Name: entryLabel, Tag: name, Active: active, Blacklist: bl, ActiveIntervals: intervals,
                EnableTag: enableTagChk, EnableCollection: enableColl, CollectionName: collName, CollectionDescription: collDescription, CollectionPosterPath: collPoster, OnlyCollection: false, OverrideWhenActive: overrideWhenActive, LastModified: currentLastMod,
                SourceType: st, MediaInfoFilters: miFilters, MediaInfoConditions: [],
                TagTargetEpisode:        !!(row.querySelector('.chkTagTargetEpisode')  || {}).checked,
                TagTargetSeason:         !!(row.querySelector('.chkTagTargetSeason')   || {}).checked,
                TagTargetSeries:         !!(row.querySelector('.chkTagTargetSeries')   || {}).checked,
                CollectionTargetEpisode: !!(row.querySelector('.chkCollTargetEpisode') || {}).checked,
                CollectionTargetSeason:  !!(row.querySelector('.chkCollTargetSeason')  || {}).checked,
                CollectionTargetSeries:  !!(row.querySelector('.chkCollTargetSeries')  || {}).checked,
                MediaInfoTargetEpisode: false, MediaInfoTargetSeason: false, MediaInfoTargetSeries: false,
                MediaInfoTargetType: '', MediaInfoSeasonMode: false,
                EnableHomeSection: enableHse, HomeSectionLibraryId: hseLibraryId, HomeSectionUserIds: hseUserIds,
                HomeSectionSettings: JSON.stringify(hseSettings), HomeSectionTracked: hseTracked
            };

            if (st === 'External') {
                var pushedExternal = false;
                row.querySelectorAll('.url-row').forEach(uRow => {
                    var urlVal = uRow.querySelector('.txtTagUrl').value.trim();
                    var limitVal = parseInt(uRow.querySelector('.txtUrlLimit').value, 10) || 0;
                    if (urlVal) { flatTags.push(Object.assign({}, baseTag, { Url: urlVal, Limit: limitVal, LocalSourceId: "" })); pushedExternal = true; }
                });
                if (forComparison && !pushedExternal) {
                    flatTags.push(Object.assign({}, baseTag, { Url: "", Limit: 0, LocalSourceId: "" }));
                }
            } else if (st === 'LocalCollection' || st === 'LocalPlaylist') {
                var pushedLocal = false;
                row.querySelectorAll('.local-row').forEach(lRow => {
                    var localVal = lRow.querySelector('.selLocalSource').value;
                    var limitVal = parseInt(lRow.querySelector('.txtLocalLimit').value, 10) || 0;
                    if (localVal) { flatTags.push(Object.assign({}, baseTag, { Url: "", Limit: limitVal, LocalSourceId: localVal })); pushedLocal = true; }
                });
                if (forComparison && !pushedLocal) {
                    flatTags.push(Object.assign({}, baseTag, { Url: "", Limit: 0, LocalSourceId: "" }));
                }
            } else if (st === 'AI') {
                var aiLimitVal = parseInt((row.querySelector('.txtAiLimit') || {}).value, 10) || 0;
                flatTags.push(Object.assign({}, baseTag, {
                    Url: "", Limit: aiLimitVal, LocalSourceId: "",
                    AiProvider: (row.querySelector('.selAiProvider') || {}).value || 'OpenAI',
                    AiPrompt: (row.querySelector('.txtAiPrompt') || {}).value || '',
                    AiIncludeRecentlyWatched: !!(row.querySelector('.chkAiRecentlyWatched') || {}).checked,
                    AiRecentlyWatchedUserId: (row.querySelector('.selAiWatchedUser') || {}).value || '',
                    AiRecentlyWatchedCount: parseInt(((row.querySelector('.txtAiWatchedCount') || {}).value || '20'), 10) || 20
                }));
            } else {
                var miLimitVal = parseInt((row.querySelector('.txtMediaInfoLimit') || {}).value, 10) || 0;
                flatTags.push(Object.assign({}, baseTag, { Url: "", Limit: miLimitVal, LocalSourceId: "" }));
            }
        });

        var hscEnabled      = view.querySelector('#chkHscEnabled');
        var hscSource       = view.querySelector('#selHscSourceUser');
        var hscLibraryOrder = view.querySelector('#chkHscLibraryOrder');

        return {
            TraktClientId: view.querySelector('#txtTraktClientId').value,
            MdblistApiKey: view.querySelector('#txtMdblistApiKey').value,
            OpenAiApiKey: (view.querySelector('#txtOpenAiApiKey') || {}).value || '',
            GeminiApiKey: (view.querySelector('#txtGeminiApiKey') || {}).value || '',
            ExtendedConsoleOutput: view.querySelector('#chkExtendedConsoleOutput').checked,
            DryRunMode: view.querySelector('#chkDryRunMode').checked,
            Tags: flatTags,
            SavedFilters: savedFilters,
            HomeSyncEnabled: hscEnabled ? hscEnabled.checked : (lastHscConfig.HomeSyncEnabled || false),
            HomeSyncLibraryOrder: hscLibraryOrder ? hscLibraryOrder.checked : (lastHscConfig.HomeSyncLibraryOrder || false),
            HomeSyncSourceUserId: hscSource ? (hscSource.value || '') : (lastHscConfig.HomeSyncSourceUserId || ''),
            HomeSyncTargetUserIds: hscEnabled
                ? Array.from(view.querySelectorAll('.hsc-target-chk:checked')).map(function(c) { return c.value; })
                : (lastHscConfig.HomeSyncTargetUserIds || [])
        };
    }

    function checkFormState() {
        var view = document.querySelector('#HomeScreenCompanionConfigPage');
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
        var view = document.querySelector('#HomeScreenCompanionConfigPage');
        if (!view || !originalConfigState) return;
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

        var anyFeature  = fTag || fColl || fSched || fHome;
        var anySrc      = fSrcExt || fSrcMI || fSrcColl || fSrcPlay || fSrcAI;
        var anyStatus   = fActive || fInactive;

        // Update button appearance
        var btn = view.querySelector('#btnFilterDropdown');
        var lbl = view.querySelector('#filterDropdownLabel');
        if (btn && lbl) {
            var activeCount = [fTag, fColl, fSched, fHome, fSrcExt, fSrcMI, fSrcColl, fSrcPlay, fSrcAI, fActive, fInactive].filter(Boolean).length;
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

            row.style.display = (matchesSearch && matchesFeature && matchesSrc && matchesStatus) ? 'block' : 'none';
        });
    }

    function checkForUpdates(view) {
        if (view.querySelector('#autoTagVersionBadge')) return;

        window.ApiClient.getJSON(window.ApiClient.getUrl("HomeScreenCompanion/Version")).then(function (result) {
            var currentVer = result.Version || '';
            if (!currentVer) return;

            fetch('https://api.github.com/repos/soderlund91/HomeScreenCompanion/releases/latest')
                .then(function (r) { return r.json(); })
                .then(function (release) {
                    var latestTag = (release.tag_name || '').replace(/^v/i, '');
                    if (!latestTag) return;
                    var a = latestTag.split('.').map(Number);
                    var b = currentVer.split('.').map(Number);
                    var isNewer = false;
                    for (var i = 0; i < 3; i++) {
                        if ((a[i] || 0) > (b[i] || 0)) { isNewer = true; break; }
                        if ((a[i] || 0) < (b[i] || 0)) break;
                    }
                    if (isNewer) {
                        var container = view.querySelector('#versionBadgeArea');
                        if (!container) return;
                        var badge = document.createElement('div');
                        badge.id = 'autoTagVersionBadge';
                        badge.style.cssText = 'font-size:1.2em; padding: 0 0 10px 0;';
                        badge.innerHTML = '<a href="' + release.html_url + '" target="_blank" rel="noopener"'
                            + ' style="color:#E67E22; font-weight:bold; text-decoration:none;">'
                            + 'New update: v' + latestTag + ' available</a>';
                        container.appendChild(badge);
                    }
                })
                .catch(function () {});
        }).catch(function () {});
    }

    function groupConfigTags(tags) {
        var grouped = {};
        (tags || []).forEach(t => {
            var key = t.Name ? t.Name + '\x1F' + t.Tag : t.Tag;
            if (!grouped[key]) {
                grouped[key] = {
                    Tag: t.Tag, Name: t.Name || '', Urls: [], LocalSources: [], Active: t.Active !== false, Blacklist: t.Blacklist, ActiveIntervals: t.ActiveIntervals,
                    EnableTag: t.EnableTag !== false, EnableCollection: t.EnableCollection, CollectionName: t.CollectionName, CollectionDescription: t.CollectionDescription || '', CollectionPosterPath: t.CollectionPosterPath || '', OnlyCollection: t.OnlyCollection, OverrideWhenActive: t.OverrideWhenActive || false, LastModified: t.LastModified,
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
                    TagTargetEpisode: t.TagTargetEpisode || false,
                    TagTargetSeason:  t.TagTargetSeason  || false,
                    TagTargetSeries:  t.TagTargetSeries  || false,
                    CollectionTargetEpisode: t.CollectionTargetEpisode || false,
                    CollectionTargetSeason:  t.CollectionTargetSeason  || false,
                    CollectionTargetSeries:  t.CollectionTargetSeries  || false
                };
            }
            if (t.SourceType === 'External' && t.Url) grouped[key].Urls.push({ url: t.Url, limit: t.Limit });
            if ((t.SourceType === 'LocalCollection' || t.SourceType === 'LocalPlaylist') && t.LocalSourceId) grouped[key].LocalSources.push({ id: t.LocalSourceId, limit: t.Limit });
            if (t.SourceType === 'MediaInfo') grouped[key].Limit = t.Limit;
            if (t.SourceType === 'AI') grouped[key].Limit = t.Limit;
        });
        return grouped;
    }


    function renderHscTab(container, config, users) {
        var sourceOptions = users.map(function (u) {
            return '<option value="' + u.Id + '"' + (config.HomeSyncSourceUserId === u.Id ? ' selected' : '') + '>' + u.Name + '</option>';
        }).join('');

        var targetRows = users.map(function (u) {
            var checked = (config.HomeSyncTargetUserIds || []).indexOf(u.Id) >= 0 ? ' checked' : '';
            return '<div class="hsc-user-row"><label style="display:flex;align-items:center;gap:10px;cursor:pointer;width:100%;">' +
                '<input is="emby-checkbox" type="checkbox" class="hsc-target-chk" value="' + u.Id + '"' + checked + ' />' +
                '<span>' + u.Name + '</span>' +
                '</label></div>';
        }).join('');

        var enabled      = config.HomeSyncEnabled ? ' checked' : '';
        var libOrderChk  = config.HomeSyncLibraryOrder ? ' checked' : '';

        var syncDisplay = config.HomeSyncEnabled ? '' : 'none';

        container.innerHTML = [
            '<div class="hsc-card">',
            '<h3 class="hsc-section-title">Configuration</h3>',
            '<div class="checkboxContainer checkboxContainer-withDescription">',
            '<label><input is="emby-checkbox" type="checkbox" id="chkHscEnabled"' + enabled + ' /><span>Enable Home Screen sync</span></label>',
            '<div class="fieldDescription">When enabled, the plugin syncs all Home Sections from the target user and applies it to those selected. When disabled, the task always skips — even if triggered manually.</div>',
            '</div>',
            '<div id="hscSyncConfig" style="display:' + syncDisplay + '">',
            '<div class="checkboxContainer checkboxContainer-withDescription" style="margin-top:8px;">',
            '<label><input is="emby-checkbox" type="checkbox" id="chkHscLibraryOrder"' + libOrderChk + ' /><span>Also copy library order</span></label>',
            '<div class="fieldDescription">Also syncs the order of media libraries in the navigation sidebar.</div>',
            '</div>',
            '<div class="inputContainer" style="margin-top:16px;">',
            '<select is="emby-select" id="selHscSourceUser" label="Source User">',
            '<option value="">— Select source user —</option>',
            sourceOptions,
            '</select>',
            '<div class="fieldDescription">Home screen sections will be copied FROM this user to all users selected below.</div>',
            '</div>',
            '</div>',
            '</div>',

            '<div class="hsc-card" id="hscSyncToCard" style="display:' + syncDisplay + '">',
            '<h3 class="hsc-section-title">Sync to</h3>',
            '<p class="textMuted" style="font-size:0.88em;margin-bottom:12px;">These users will receive the source user\'s home screen layout on each sync.</p>',
            '<div class="hsc-user-list" id="hscTargetList">',
            targetRows || '<p class="textMuted" style="font-size:0.85em;">No users found.</p>',
            '</div>',
            '</div>',

        ].join('');

        container.dataset.loaded = '1';
    }

    function enforceHscSourceTargetConflict(container) {
        var sourceId = (container.querySelector('#selHscSourceUser') || {}).value || '';
        container.querySelectorAll('.hsc-target-chk').forEach(function (chk) {
            var isConflict = sourceId && chk.value === sourceId;
            if (isConflict) {
                chk.checked = false;
                chk.disabled = true;
                chk.closest('.hsc-user-row').title = 'Cannot sync a user to themselves';
                chk.closest('.hsc-user-row').style.opacity = '0.45';
            } else {
                chk.disabled = false;
                chk.closest('.hsc-user-row').title = '';
                chk.closest('.hsc-user-row').style.opacity = '';
            }
        });
    }

    function loadHscUsers(view) {
        var container = view.querySelector('#hscContainer');
        if (!container) return;

        window.ApiClient.getJSON(window.ApiClient.getUrl('Users', { IsDisabled: false }))
            .then(function (users) {
                renderHscTab(container, lastHscConfig, users || []);

                enforceHscSourceTargetConflict(container);

                var enableChk = container.querySelector('#chkHscEnabled');
                if (enableChk) {
                    enableChk.addEventListener('change', function () {
                        var show = this.checked;
                        var syncConfig = container.querySelector('#hscSyncConfig');
                        var syncToCard = container.querySelector('#hscSyncToCard');
                        if (syncConfig) syncConfig.style.display = show ? '' : 'none';
                        if (syncToCard) syncToCard.style.display = show ? '' : 'none';
                        setTimeout(checkFormState, 0);
                    });
                }

                var sourceSelect = container.querySelector('#selHscSourceUser');
                if (sourceSelect) {
                    sourceSelect.addEventListener('change', function () {
                        enforceHscSourceTargetConflict(container);
                        setTimeout(checkFormState, 0);
                    });
                }

                container.querySelectorAll('.hsc-target-chk').forEach(function (chk) {
                    chk.addEventListener('change', function () {
                        enforceHscSourceTargetConflict(container);
                        setTimeout(checkFormState, 0);
                    });
                });

                container.querySelectorAll('input:not(.hsc-target-chk), select:not(#selHscSourceUser)').forEach(function (el) {
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

        Promise.all([
            fetch(window.ApiClient.getUrl('HomeScreenCompanion/Manage/Tags'), { headers: { 'X-MediaBrowser-Token': token } }).then(function (r) { return r.json(); }),
            fetch(window.ApiClient.getUrl('HomeScreenCompanion/Manage/Collections'), { headers: { 'X-MediaBrowser-Token': token } }).then(function (r) { return r.json(); }),
            window.ApiClient.getPluginConfiguration(pluginId).catch(function () { return { Tags: [] }; })
        ]).then(function (results) {
            var tagsData = results[0];
            var collectionsData = results[1];
            var pluginConfig = results[2];

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

            var btnStyle = 'cursor:pointer;border:none;border-radius:3px;padding:4px 12px;font-size:0.82em;font-weight:500;';

            function renderSection(title, items, isTagSection) {
                var sectionId = isTagSection ? 'tcTagSection' : 'tcCollSection';
                var rows = items.length === 0
                    ? '<div style="color:var(--theme-text-secondary);padding:8px 0;">No items found.</div>'
                    : items.map(function (item) {
                        var id = isTagSection ? (item.Name || '') : (item.Id || '');
                        var name = item.Name || '';
                        var count = item.ItemCount != null ? item.ItemCount : 0;
                        var managed = isTagSection ? managedTagMap[name.toLowerCase()] : managedCollMap[name.toLowerCase()];
                        var badge = managed && managed.length > 0
                            ? '<span style="font-size:0.75em;background:#52B54B22;color:#52B54B;border:1px solid #52B54B55;border-radius:4px;padding:1px 6px;margin-left:8px;white-space:nowrap;">Managed by HSC Plugin</span>'
                            : '';
                        return '<tr class="tc-manage-row" data-rowname="' + escAttr(name.toLowerCase()) + '" data-managed="' + (managed && managed.length > 0 ? '1' : '0') + '" data-count="' + count + '">' +
                            '<td style="padding:9px 4px;border-bottom:1px solid var(--line-color);width:100%;max-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">' +
                            '<span class="tc-item-name">' + escHtml(name) + '</span>' +
                            badge +
                            '</td>' +
                            '<td style="padding:9px 4px 9px 16px;border-bottom:1px solid var(--line-color);white-space:nowrap;color:var(--theme-text-secondary);font-size:0.88em;">' + count + ' items</td>' +
                            '<td style="padding:9px 4px 9px 8px;border-bottom:1px solid var(--line-color);white-space:nowrap;">' +
                            '<button type="button" class="btnTcMark" style="' + btnStyle + 'background:#cc3333;color:#fff;" data-id="' + escAttr(id) + '" data-name="' + escAttr(name) + '" data-count="' + count + '" data-type="' + (isTagSection ? 'tag' : 'coll') + '">Remove</button>' +
                            '</td>' +
                            '</tr>';
                    }).join('');

                return '<div id="' + sectionId + '" style="flex:1 1 300px;min-width:0;">' +
                    '<div style="display:flex;align-items:center;margin-bottom:12px;">' +
                    '<h3 style="margin:0;font-size:1em;text-transform:uppercase;letter-spacing:1px;color:#52B54B;">' + escHtml(title) + '</h3>' +
                    '<button type="button" class="btnTcRefresh" style="' + btnStyle + 'background:transparent;color:var(--theme-text-secondary);border:1px solid var(--line-color);margin-left:auto;"><i class="md-icon" style="font-size:1em;vertical-align:middle;">refresh</i></button>' +
                    '</div>' +
                    '<table class="tc-manage-list" style="width:100%;border-collapse:collapse;"><tbody>' + rows + '</tbody></table>' +
                    '</div>';
            }

            var searchInputStyle = 'background:rgba(128,128,128,0.08);border:1px solid var(--line-color);border-radius:4px;padding:5px 10px;font-size:0.9em;color:inherit;width:400px;max-width:100%;';

            container.innerHTML =
                '<div style="display:flex;align-items:center;gap:10px;margin-bottom:16px;flex-wrap:wrap;">' +
                '<input type="text" id="tcSearch" placeholder="Search…" style="' + searchInputStyle + '" />' +
                '<select is="emby-select" id="tcSort" style="color:inherit;background:rgba(128,128,128,0.08);border:1px solid var(--line-color);padding:5px;border-radius:4px;font-size:0.9em;cursor:pointer;">' +
                '<option value="name-asc">Name A–Z</option>' +
                '<option value="name-desc">Name Z–A</option>' +
                '<option value="count-desc">Most items</option>' +
                '<option value="count-asc">Fewest items</option>' +
                '<option value="managed">Managed first</option>' +
                '</select>' +
                '</div>' +
                '<div id="tcSectionsWrap" style="display:flex;gap:40px;align-items:flex-start;">' +
                renderSection('Tags', tagsData.Tags || [], true) +
                renderSection('Collections', collectionsData.Collections || [], false) +
                '</div>';

            container.dataset.loaded = '1';

            function applySearchSort() {
                var query = (container.querySelector('#tcSearch').value || '').toLowerCase();
                var sort = container.querySelector('#tcSort').value;

                ['tcTagSection', 'tcCollSection'].forEach(function (sectionId) {
                    var section = container.querySelector('#' + sectionId);
                    if (!section) return;
                    var rows = Array.from(section.querySelectorAll('.tc-manage-row'));

                    rows.forEach(function (row) {
                        var rowName = row.dataset.rowname || '';
                        row.style.display = (!query || rowName.indexOf(query) !== -1) ? '' : 'none';
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
                        if (sort === 'name-asc') return nameA.localeCompare(nameB);
                        if (sort === 'name-desc') return nameB.localeCompare(nameA);
                        if (sort === 'count-desc') return countB - countA;
                        if (sort === 'count-asc') return countA - countB;
                        if (sort === 'managed') return (managedB ? 1 : 0) - (managedA ? 1 : 0) || nameA.localeCompare(nameB);
                        return 0;
                    });
                    visibleRows.forEach(function (r) { list.appendChild(r); });
                });
            }

            container.querySelector('#tcSearch').addEventListener('input', applySearchSort);
            container.querySelector('#tcSort').addEventListener('change', applySearchSort);
            applySearchSort();

            function updateSaveButton() {
                var hasPending = Object.keys(pendingTagDeletes).length > 0 || Object.keys(pendingCollDeletes).length > 0;
                container._tcHasPending = hasPending;
                checkFormState();
            }

            if (container._tcClickHandler) container.removeEventListener('click', container._tcClickHandler);
            container._tcClickHandler = function (e) {
                var btn = e.target.closest('button');
                if (!btn) return;

                // Mark for deletion
                if (btn.classList.contains('btnTcMark')) {
                    var type = btn.dataset.type;
                    var id = btn.dataset.id;
                    var name = btn.dataset.name;
                    var count = parseInt(btn.dataset.count || '0', 10);
                    if (type === 'tag') pendingTagDeletes[id.toLowerCase()] = { name: name, itemCount: count };
                    else pendingCollDeletes[id] = { id: id, name: name, itemCount: count };
                    var row = btn.closest('.tc-manage-row');
                    if (row) {
                        row.style.opacity = '0.45';
                        var nameEl = row.querySelector('.tc-item-name');
                        if (nameEl) nameEl.style.textDecoration = 'line-through';
                        btn.textContent = 'Undo';
                        btn.classList.remove('btnTcMark');
                        btn.classList.add('btnTcUndo');
                        btn.style.background = '#555';
                    }
                    updateSaveButton();
                    return;
                }

                // Undo pending deletion
                if (btn.classList.contains('btnTcUndo')) {
                    var type = btn.dataset.type;
                    var id = btn.dataset.id;
                    if (type === 'tag') delete pendingTagDeletes[id.toLowerCase()];
                    else delete pendingCollDeletes[id];
                    var row = btn.closest('.tc-manage-row');
                    if (row) {
                        row.style.opacity = '1';
                        var nameEl = row.querySelector('.tc-item-name');
                        if (nameEl) nameEl.style.textDecoration = '';
                        btn.textContent = 'Remove';
                        btn.classList.remove('btnTcUndo');
                        btn.classList.add('btnTcMark');
                        btn.style.background = '#cc3333';
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
                var undoBtnStyle = 'cursor:pointer;border:none;background:transparent;color:#cc2222;border-radius:3px;padding:2px 6px;font-size:1.1em;line-height:1;margin-right:8px;flex-shrink:0;';

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
                                '<div style="color:#f0a000;margin-top:6px;font-size:0.88em;">' +
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

                function buildContent() {
                    var tagList = Object.values(pendingTagDeletes);
                    var collList = Object.values(pendingCollDeletes);
                    var tagSection = tagList.length > 0
                        ? '<div style="margin-bottom:20px;"><h4 style="margin:0 0 8px;color:#52B54B;">Tags to remove (' + tagList.length + ')</h4>' + buildRows(tagList, true) + '</div>'
                        : '';
                    var collSection = collList.length > 0
                        ? '<div style="margin-bottom:20px;"><h4 style="margin:0 0 8px;color:#52B54B;">Collections to remove (' + collList.length + ')</h4>' + buildRows(collList, false) + '</div>'
                        : '';
                    return tagSection + collSection;
                }

                var modal = document.createElement('div');
                modal.style.cssText = 'position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.75);z-index:9999;display:flex;align-items:center;justify-content:center;';

                function renderModal() {
                    var tagList = Object.values(pendingTagDeletes);
                    var collList = Object.values(pendingCollDeletes);
                    if (tagList.length === 0 && collList.length === 0) { modal.remove(); updateSaveButton(); return; }
                    modal.innerHTML =
                        '<div style="background:var(--plugin-popup-bg,#2a2a2a);color:var(--plugin-popup-color,#e8e8e8);border:1px solid var(--plugin-popup-border,rgba(255,255,255,0.12));border-radius:8px;padding:28px;max-width:600px;width:90%;max-height:80vh;overflow-y:auto;">' +
                        '<h3 style="margin:0 0 20px;font-size:1.1em;color:#52B54B;">Summary — Pending changes</h3>' +
                        '<div id="tcModalBody">' + buildContent() + '</div>' +
                        '<div style="display:flex;justify-content:flex-end;gap:12px;margin-top:20px;border-top:1px solid var(--line-color);padding-top:16px;">' +
                        '<button type="button" id="tcModalCancel" style="cursor:pointer;border:1px solid var(--line-color);background:transparent;color:var(--theme-text-primary);border-radius:3px;padding:8px 18px;font-size:0.9em;">Cancel</button>' +
                        '<button type="button" id="tcModalConfirm" style="cursor:pointer;border:none;background:#52B54B;color:#fff;border-radius:3px;padding:8px 18px;font-size:0.9em;font-weight:500;"><i class="md-icon" style="font-size:1em;vertical-align:middle;margin-right:5px;">check</i>Confirm &amp; Save</button>' +
                        '</div></div>';

                    modal.querySelector('#tcModalCancel').addEventListener('click', function () { modal.remove(); });

                    modal.addEventListener('click', function (e) {
                        var undoBtn = e.target.closest('.btnModalUndo');
                        if (!undoBtn) return;
                        var type = undoBtn.dataset.type;
                        var key = undoBtn.dataset.key;
                        if (type === 'tag') delete pendingTagDeletes[key];
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
                            mainBtn.style.background = '#cc3333';
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

                        tagBatchPromise.then(function () {
                            return collOps.reduce(function (chain, op) { return chain.then(op); }, Promise.resolve());
                        }).then(function () {
                            if (groupsToInactivate.size === 0) return Promise.resolve();
                            return window.ApiClient.getPluginConfiguration(pluginId).then(function (cfg) {
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
                                return window.ApiClient.updatePluginConfiguration(pluginId, cfg);
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
                                if (lbl) { lbl.textContent = 'Disabled'; lbl.style.color = 'var(--theme-text-secondary)'; }
                                sourceRow.classList.add('inactive');
                                var runBtn = sourceRow.querySelector('.btnRunEntry');
                                if (runBtn) { runBtn.disabled = true; runBtn.style.opacity = '0.4'; }
                            });
                            pendingTagDeletes = {};
                            pendingCollDeletes = {};
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
            container.innerHTML = '<div style="color:#cc3333;padding:20px;">Failed to load: ' + err + '</div>';
        });
    }

    function loadHscManageTab(view) {
        var container = view.querySelector('#hscManageContainer');
        if (!container) return;

        window.ApiClient.getJSON(window.ApiClient.getUrl('Users', { IsDisabled: false }))
            .then(function (users) {
                var userOptions = (users || []).map(function (u) {
                    return '<option value="' + u.Id + '">' + u.Name + '</option>';
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
                    '<button type="button" is="emby-button" class="raised btn-neutral" id="btnRefreshManageSections" style="height:36px;min-width:90px;margin-bottom:2px;" disabled>',
                    '<i class="md-icon" style="margin-right:4px;">refresh</i>Refresh',
                    '</button>',
                    '</div>',
                    '<div id="manSectionList"></div>',
                    '<div style="margin-top:16px;display:flex;justify-content:flex-end;">',
                    '<button type="button" is="emby-button" class="raised button-submit" id="btnApplyManage" disabled>',
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
                listEl.innerHTML = '<p class="textMuted" style="padding:10px 0;color:#cc3333;">Failed to load sections.</p>';
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
            if (!document.getElementById('homeScreenCompanionCustomCss')) {
                document.body.insertAdjacentHTML('beforeend', customCss);
            }
            applyPluginTheme();

            var form = view.querySelector('.HomeScreenCompanionForm');
            var isFirstVisit = !view.dataset.hscInit;
            if (isFirstVisit) view.dataset.hscInit = '1';

            originalConfigState = null;

            var changeHandler = function() {
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
            });
            var settingsTab = view.querySelector('#tabSettings');
            settingsTab.addEventListener('input',  changeHandler, { signal: _signal });
            settingsTab.addEventListener('change', changeHandler, { signal: _signal });
            form.addEventListener('click', (e) => {
                var dayBtn = e.target.closest('.day-toggle');
                if (dayBtn) dayBtn.classList.toggle('active');
                if (e.target.closest('.btnRemoveUrl, .btnAddUrl, .btnRemoveLocal, .btnAddLocal, .btnRemoveDate, .btnAddDate, .btnRemoveFilterGroup, .btnAddMediaInfoFilter, .btnClearAllFilters, .btnGroupOpChoice, .btnGroupInnerOpChoice, .btnAddMiRule, .btnRemoveMiRule, .btnRemoveGroup, .day-toggle, .btnRemovePoster, .btnApplyMiPreset')) {
                    changeHandler();
                }
            }, { signal: _signal });
            
            var container = view.querySelector('#tagListContainer');

            if (isFirstVisit) {
                if (container) {
                    let rafId = null;
                    container.addEventListener('dragover', (e) => {
                        if (localStorage.getItem('HomeScreenCompanion_SortBy') !== 'Manual') return;
                        e.preventDefault();
                        if (rafId) return;
                        rafId = requestAnimationFrame(() => {
                            const draggingRow = document.querySelector('.tag-row.dragging');
                            if (!draggingRow) { rafId = null; return; }
                            const afterElement = getDragAfterElement(container, e.clientY);
                            var placeholder = document.querySelector('.sort-placeholder');
                            if (!placeholder) {
                                placeholder = document.createElement('div');
                                placeholder.className = 'sort-placeholder';
                            }
                            if (afterElement == null) {
                                if (placeholder.nextElementSibling !== null) container.appendChild(placeholder);
                            } else {
                                if (placeholder.nextElementSibling !== afterElement) container.insertBefore(placeholder, afterElement);
                            }
                            rafId = null;
                        });
                    });
                    container.addEventListener('drop', (e) => {
                        if (localStorage.getItem('HomeScreenCompanion_SortBy') !== 'Manual') return;
                        e.preventDefault();
                        const draggingRow = document.querySelector('.tag-row.dragging');
                        const placeholder = document.querySelector('.sort-placeholder');
                        if (draggingRow && placeholder) {
                            container.insertBefore(draggingRow, placeholder);
                            placeholder.remove();
                            changeHandler();
                        }
                    });
                }



                var logOverlay = view.querySelector('#logModalOverlay');
                var helpOverlay = view.querySelector('#helpModalOverlay');

                view.querySelector('#btnOpenLogs').addEventListener('click', e => { e.preventDefault(); logOverlay.classList.add('modal-visible'); });
                view.querySelector('#btnCloseLogs').addEventListener('click', () => logOverlay.classList.remove('modal-visible'));
                logOverlay.addEventListener('click', e => { if (e.target === logOverlay) logOverlay.classList.remove('modal-visible'); });

                view.querySelector('#btnOpenHelp').addEventListener('click', () => helpOverlay.classList.add('modal-visible'));
                view.querySelector('#btnCloseHelp').addEventListener('click', () => helpOverlay.classList.remove('modal-visible'));
                helpOverlay.addEventListener('click', e => { if (e.target === helpOverlay) helpOverlay.classList.remove('modal-visible'); });

                var miHelpOverlay = view.querySelector('#miHelpModalOverlay');
                view.querySelector('#btnCloseMiHelp').addEventListener('click', () => miHelpOverlay.classList.remove('modal-visible'));
                miHelpOverlay.addEventListener('click', e => { if (e.target === miHelpOverlay) miHelpOverlay.classList.remove('modal-visible'); });

                var tagTargetHelpOverlay = view.querySelector('#tagTargetHelpModalOverlay');
                view.querySelector('#btnCloseTagTargetHelp').addEventListener('click', () => tagTargetHelpOverlay.classList.remove('modal-visible'));
                tagTargetHelpOverlay.addEventListener('click', e => { if (e.target === tagTargetHelpOverlay) tagTargetHelpOverlay.classList.remove('modal-visible'); });

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

                        <span class="control-label" style="opacity:0.7; margin-right: 10px; flex-shrink: 0;">Sort:</span>

                        <select is="emby-select" id="cbSortTags" style="color:inherit; background:rgba(128,128,128,0.08); border:1px solid var(--line-color); padding:5px; border-radius:4px; font-size:0.9em; cursor:pointer; width: 80px; margin-right: 0px;">
                            <option value="Manual" ${savedSort === 'Manual' ? 'selected' : ''}>Manual</option>
                            <option value="Name" ${savedSort === 'Name' ? 'selected' : ''}>Name</option>
                            <option value="Active" ${savedSort === 'Active' ? 'selected' : ''}>Status</option>
                            <option value="LatestEdited" ${savedSort === 'LatestEdited' ? 'selected' : ''}>Latest</option>
                        </select>

                        <div style="width: 1px; height: 25px; background: var(--line-color); margin-right: 10px;"></div>

                        <span class="control-label" style="opacity:0.7; margin-right: 10px; flex-shrink: 0;"> | Filters:</span>

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
                                <label class="filter-chk-row"><input type="checkbox" id="chkFilterSrcExternal" /><span>External</span></label>
                                <label class="filter-chk-row"><input type="checkbox" id="chkFilterSrcMediaInfo" /><span>Local Media Information</span></label>
                                <label class="filter-chk-row"><input type="checkbox" id="chkFilterSrcCollection" /><span>Local Collection</span></label>
                                <label class="filter-chk-row"><input type="checkbox" id="chkFilterSrcPlaylist" /><span>Local Playlist</span></label>
                                <label class="filter-chk-row"><input type="checkbox" id="chkFilterSrcAI" /><span>AI created lists</span></label>
                                <div class="filter-dropdown-divider"></div>
                                <div class="filter-dropdown-label">Status</div>
                                <label class="filter-chk-row"><input type="checkbox" id="chkFilterActive" /><span style="display:flex;align-items:center;gap:6px;"><span style="width:8px;height:8px;border-radius:50%;background:#52B54B;flex-shrink:0;"></span>Active</span></label>
                                <label class="filter-chk-row"><input type="checkbox" id="chkFilterInactive" /><span style="display:flex;align-items:center;gap:6px;"><span style="width:8px;height:8px;border-radius:50%;background:rgba(128,128,128,0.5);flex-shrink:0;"></span>Inactive</span></label>
                            </div>
                        </div>

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
                     '#chkFilterSrcAI','#chkFilterActive','#chkFilterInactive'
                    ].forEach(id => view.querySelector(id).addEventListener('change', () => applyFilters(view)));

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

                view.querySelector('#btnBackupConfig').addEventListener('click', () => {
                    window.ApiClient.getPluginConfiguration(pluginId).then(config => {
                        var json = JSON.stringify(config, null, 2);
                        var blob = new Blob([json], { type: "application/json" });
                        var url = URL.createObjectURL(blob);
                        var a = document.createElement('a');
                        a.href = url; a.download = `HSC_Backup_${new Date().toISOString().split('T')[0]}.json`;
                        document.body.appendChild(a); a.click(); document.body.removeChild(a); URL.revokeObjectURL(url);
                    });
                });

                var fileInput = view.querySelector('#fileRestoreConfig');
                view.querySelector('#btnRestoreConfigTrigger').addEventListener('click', () => fileInput.click());
                fileInput.addEventListener('change', function (e) {
                    var file = e.target.files[0]; if (!file) return;
                    var reader = new FileReader();
                    reader.onload = function (e) {
                        try {
                            var config = JSON.parse(e.target.result);
                            view.querySelector('#txtTraktClientId').value = config.TraktClientId || '';
                            view.querySelector('#txtMdblistApiKey').value = config.MdblistApiKey || '';
                            var oaEl = view.querySelector('#txtOpenAiApiKey'); if (oaEl) oaEl.value = config.OpenAiApiKey || '';
                            var gmEl = view.querySelector('#txtGeminiApiKey'); if (gmEl) gmEl.value = config.GeminiApiKey || '';
                            view.querySelector('#chkExtendedConsoleOutput').checked = config.ExtendedConsoleOutput || false;
                            view.querySelector('#chkDryRunMode').checked = config.DryRunMode || false;

                            var container = view.querySelector('#tagListContainer'); container.innerHTML = '';
                            var grouped = groupConfigTags(config.Tags);

                            var keys = Object.keys(grouped);
                            keys.forEach((k, i) => renderTagGroup(grouped[k], container, false, i));
                            if (keys.length === 0) renderTagGroup({ Tag: '', Urls: [{ url: '', limit: 50 }], Active: true }, container, false, 0);

                            applyFilters(view);

                            requestAnimationFrame(function () {
                                window.Dashboard.alert("Configuration loaded!");
                                checkFormState();
                            });
                        } catch (err) {
                            window.Dashboard.alert("Failed to parse configuration file. The file may be corrupt or not a valid backup file. Error: " + err.message);
                        }
                        fileInput.value = '';
                    };
                    reader.readAsText(file);
                });

            }

            if (!view.querySelector('.dry-run-warning')) {
                view.insertAdjacentHTML('afterbegin', '<div class="dry-run-warning"><i class="md-icon" style="font-size:1.4em;"></i>DRY RUN MODE IS ACTIVE - NO CHANGES WILL BE SAVED</div>');
            }

            var btnSave = view.querySelector('.btn-save');
            if (btnSave) { btnSave.disabled = true; btnSave.style.opacity = "0.5"; }

            checkForUpdates(view);
            refreshStatus(view);
            statusInterval = setInterval(() => refreshStatus(view), 5000);
            getHseUsers().then(function(users) { _miUsers = users; });

            Promise.all([
                window.ApiClient.getJSON(window.ApiClient.getUrl("Users/" + window.ApiClient.getCurrentUserId() + "/Items", { IncludeItemTypes: "BoxSet", Recursive: true })),
                window.ApiClient.getJSON(window.ApiClient.getUrl("Items", { IncludeItemTypes: "Playlist", Recursive: true })),
                window.ApiClient.getJSON(window.ApiClient.getUrl("Items/Filters2", { UserId: window.ApiClient.getCurrentUserId(), Recursive: true })).catch(function () { return { Tags: [] }; })
            ]).then(responses => {
                cachedCollections = responses[0].Items || [];
                cachedPlaylists = responses[1].Items || [];
                cachedTags = ((responses[2] && responses[2].Tags) || []).slice().sort();

                window.ApiClient.getPluginConfiguration(pluginId).then(config => {
                    lastHscConfig = {
                        HomeSyncEnabled:       config.HomeSyncEnabled       || false,
                        HomeSyncLibraryOrder:  config.HomeSyncLibraryOrder  || false,
                        HomeSyncSourceUserId:  config.HomeSyncSourceUserId  || '',
                        HomeSyncTargetUserIds: config.HomeSyncTargetUserIds || []
                    };
                    savedFilters = config.SavedFilters || [];

                    var container = view.querySelector('#tagListContainer'); container.innerHTML = '';
                    view.querySelector('#txtTraktClientId').value = config.TraktClientId || '';
                    view.querySelector('#txtMdblistApiKey').value = config.MdblistApiKey || '';
                    var oaElInit = view.querySelector('#txtOpenAiApiKey'); if (oaElInit) oaElInit.value = config.OpenAiApiKey || '';
                    var gmElInit = view.querySelector('#txtGeminiApiKey'); if (gmElInit) gmElInit.value = config.GeminiApiKey || '';
                    view.querySelector('#chkExtendedConsoleOutput').checked = config.ExtendedConsoleOutput || false;
                    view.querySelector('#chkDryRunMode').checked = config.DryRunMode || false;
                    if (view.querySelector('#txtSearchTags')) {
                        view.querySelector('#txtSearchTags').value = '';
                        view.querySelector('#btnClearSearch').style.display = 'none';
                    }
                    ['#chkFilterTag','#chkFilterCollection','#chkFilterSchedule','#chkFilterHomeScreen',
                     '#chkFilterSrcExternal','#chkFilterSrcMediaInfo','#chkFilterSrcCollection','#chkFilterSrcPlaylist',
                     '#chkFilterSrcAI','#chkFilterActive','#chkFilterInactive'
                    ].forEach(id => { var el = view.querySelector(id); if (el) el.checked = false; });
                    var lbl = view.querySelector('#filterDropdownLabel'); if (lbl) lbl.textContent = 'Filter';
                    var btn = view.querySelector('#btnFilterDropdown'); if (btn) btn.classList.remove('active');

                    var grouped = groupConfigTags(config.Tags);

                    var keys = Object.keys(grouped);
                    keys.forEach((k, i) => renderTagGroup(grouped[k], container, false, i));

                    if (keys.length === 0) renderTagGroup({ Tag: '', Urls: [{ url: '', limit: 0 }], Active: true }, container, false, 0);

                    var savedSort = localStorage.getItem('HomeScreenCompanion_SortBy') || 'Manual';
                    sortRows(container, savedSort);
                    applyFilters(view);
                    requestAnimationFrame(function () {
                        try {
                            originalConfigState = JSON.stringify(getUiConfig(view, true));
                        } catch (err) {
                            originalConfigState = null;
                        }
                        checkFormState();
                        updateDryRunWarning();
                    });
                });
            });
        });

        view.addEventListener('viewhide', () => { if (statusInterval) clearInterval(statusInterval); });

        view.querySelector('.HomeScreenCompanionForm').addEventListener('submit', e => {
            e.preventDefault();

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
            
            var originalConf = JSON.parse(originalConfigState);
            var originalTags = groupConfigTags(originalConf.Tags);

            configObj.Tags.forEach(tag => {
                var key = tag.Name ? tag.Name + '\x1F' + tag.Tag : tag.Tag;
                var originalTag = originalTags[key];
                
                var currentTagForCompare = Object.assign({}, tag, { LastModified: "CONSTANT_FOR_COMPARISON" });
                var originalTagForCompare = originalTag ? Object.assign({}, originalTag, { LastModified: "CONSTANT_FOR_COMPARISON" }) : null;

                if (!originalTag || JSON.stringify(currentTagForCompare) !== JSON.stringify(originalTagForCompare)) {
                    tag.LastModified = new Date().toISOString();
                } else {
                    tag.LastModified = originalTag.LastModified;
                }
            });

            // Pre-fetch current config to preserve HomeSectionTracked set by the sync task
            // (UI dataset may be stale if sync ran after page load)
            window.ApiClient.getPluginConfiguration(pluginId).catch(function() { return { Tags: [] }; }).then(function(currentConfig) {
                var currentGrouped = groupConfigTags(currentConfig.Tags);
                configObj.Tags.forEach(function(t) {
                    var key = t.Name ? t.Name + '\x1F' + t.Tag : t.Tag;
                    var existing = currentGrouped[key];
                    if (existing && existing.HomeSectionTracked && existing.HomeSectionTracked.length > 0
                        && (!t.HomeSectionTracked || t.HomeSectionTracked.length === 0)) {
                        t.HomeSectionTracked = existing.HomeSectionTracked;
                    }
                });
                return window.ApiClient.updatePluginConfiguration(pluginId, configObj);
            }).then(r => {
                window.Dashboard.processPluginConfigurationUpdateResult(r);

                var newGrouped = groupConfigTags(configObj.Tags);
                view.querySelectorAll('.tag-row').forEach(row => {
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
                    }
                });

                originalConfigState = JSON.stringify(getUiConfig(view, true));
                checkFormState();
                updateDryRunWarning();
            });
        });

        var speedDial = view.querySelector('#runSpeedDial');
        var syncMenu = view.querySelector('#runSyncMenu');
        view.querySelector('#btnRunSync').addEventListener('click', function (e) {
            e.stopPropagation();
            var isOpen = speedDial.classList.toggle('open');
            syncMenu.classList.toggle('open', isOpen);
        });

        function closeSpeedDial() {
            speedDial.classList.remove('open');
            syncMenu.classList.remove('open');
        }
        document.addEventListener('click', closeSpeedDial);

        function runTask(key, label) {
            window.ApiClient.getScheduledTasks().then(function (tasks) {
                var t = tasks.find(function (x) { return x.Key === key; });
                if (t) {
                    window.ApiClient.startScheduledTask(t.Id).then(function () {
                        window.Dashboard.alert(label + ' started!');
                    });
                } else {
                    window.Dashboard.alert('Task not found: ' + key);
                }
            });
            closeSpeedDial();
        }

        view.querySelector('#btnDialTagsCollections').addEventListener('click', function () {
            runTask('HomeScreenCompanionSyncTask', 'Tag sync');
        });

        view.querySelector('#btnDialHomeScreen').addEventListener('click', function () {
            runTask('HomeSectionSyncTask', 'Home screen sync');
        });

        view.querySelector('#btnDialFullSync').addEventListener('click', function () {
            window.ApiClient.getScheduledTasks().then(function (tasks) {
                var tagTask = tasks.find(function (x) { return x.Key === 'HomeScreenCompanionSyncTask'; });
                var hscTask = tasks.find(function (x) { return x.Key === 'HomeSectionSyncTask'; });
                var promises = [];
                if (tagTask) promises.push(window.ApiClient.startScheduledTask(tagTask.Id));
                if (hscTask) promises.push(window.ApiClient.startScheduledTask(hscTask.Id));
                Promise.all(promises).then(function () {
                    window.Dashboard.alert('Full sync started!');
                });
            });
            closeSpeedDial();
        });

        function beforeUnloadHandler(e) {
            if (hasDirtyState()) {
                e.preventDefault();
                e.returnValue = '';
            }
        }
        window.addEventListener('beforeunload', beforeUnloadHandler);

        view.addEventListener('viewhide', function () {
            document.removeEventListener('click', closeSpeedDial);
            window.removeEventListener('beforeunload', beforeUnloadHandler);
        }, { once: true });

        function hasDirtyState() {
            var btnSave = view.querySelector('.btn-save');
            if (btnSave && !btnSave.disabled) return true;
            var tcContainer = view.querySelector('#tcManageContainer');
            if (tcContainer && tcContainer._tcHasPending) return true;
            return false;
        }

        view.querySelectorAll('.page-tab-btn').forEach(function (btn) {
            btn.addEventListener('click', function () {
                var target = this.getAttribute('data-page-tab');
                if (hasDirtyState() && !confirm('You have unsaved changes. Leave this tab and discard changes?')) return;
                view.querySelectorAll('.page-tab-btn').forEach(function (b) { b.classList.remove('active'); });
                this.classList.add('active');
                view.querySelectorAll('.page-tab-content').forEach(function (c) { c.style.display = 'none'; });
                view.querySelector('#tab' + target).style.display = '';

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
                }
            });
        });

        view.addEventListener('change', function (e) {
            var cb = e.target.closest('.chkShowApiKey');
            if (!cb) return;
            var input = view.querySelector('#' + cb.dataset.target);
            if (input) input.type = cb.checked ? 'text' : 'password';
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
            if (!btn) return;
            var target = btn.getAttribute('data-hsc-tab');
            view.querySelectorAll('.hsc-sub-tab-btn').forEach(function (b) { b.classList.remove('active'); });
            btn.classList.add('active');
            view.querySelectorAll('.hsc-sub-tab-content').forEach(function (c) { c.style.display = 'none'; });
            if (target === 'copy') {
                view.querySelector('#hscSubTabCopy').style.display = '';
                var hscContainer = view.querySelector('#hscContainer');
                if (hscContainer && !hscContainer.dataset.loaded) loadHscUsers(view);
            } else if (target === 'manage') {
                view.querySelector('#hscSubTabManage').style.display = '';
                var manageContainer = view.querySelector('#hscManageContainer');
                if (manageContainer && !manageContainer.dataset.loaded) loadHscManageTab(view);
            }
        });


        var origStatusInterval = statusInterval;
        if (origStatusInterval) clearInterval(origStatusInterval);
        statusInterval = setInterval(function () {
            refreshStatus(view);
        }, 5000);
    };
});