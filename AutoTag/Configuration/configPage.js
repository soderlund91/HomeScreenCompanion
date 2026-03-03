define(['emby-input', 'emby-button', 'emby-select', 'emby-checkbox'], function () {
    'use strict';

    var pluginId = "7c10708f-43e4-4d69-923c-77d01802315b";
    var statusInterval = null;
    var originalConfigState = null;
    var statusRequestId = 0;

    var cachedCollections = [];
    var cachedPlaylists = [];
    var cachedTags = [];
    var lastHscConfig = {};

    var customCss = `
    <style id="autoTagCustomCss">
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
        // Year 2001 is NOT a leap year → February is capped at 28, safe for all years.
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

    function parseCriterion(crit) {
        if (!crit) return { prop: 'Resolution', op: '', val: '', not: false };
        var not = crit.charAt(0) === '!';
        if (not) crit = crit.slice(1);
        var parts = crit.split(':');
        if (parts.length === 1) {
            var mapped = MI_CRITERION_MAP[crit];
            return mapped ? { prop: mapped.prop, op: '', val: mapped.val, not: not } : { prop: '', op: '', val: crit, not: not };
        }
        if (parts.length === 2) return { prop: parts[0], op: '', val: parts[1], not: not };
        if (parts.length === 3) return { prop: parts[0], op: parts[1], val: parts[2], not: not };
        return { prop: 'Resolution', op: '', val: '', not: false };
    }

    function buildCriterion(prop, op, val) {
        if (!prop || val === '') return '';
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
        MediaType: [['Movie', 'Movie'], ['Series', 'Show / Series']]
    };
    var MI_NUMERIC_PROPS = ['CommunityRating', 'Year', 'Runtime'];
    var MI_TEXT_PLACEHOLDERS = {
        Title: 'e.g. Batman', Studio: 'e.g. Warner', Genre: 'e.g. Action',
        Actor: 'e.g. Tom Hanks', Director: 'e.g. Nolan', Writer: 'e.g. Tarantino',
        ContentRating: 'e.g. PG-13', ImdbId: 'e.g. tt1234567, tt7654321'
    };

    function propertyOptionsHtml(selected) {
        var groups = [
            { label: 'Video', props: [['Resolution','Resolution'], ['VideoCodec','Video Codec'], ['HDR','HDR']] },
            { label: 'Audio', props: [['AudioFormat','Audio Format'], ['AudioChannels','Audio Channels'], ['AudioLanguage','Audio Language']] },
            { label: 'Content', props: [['MediaType','Media Type'], ['Tag','Tag'], ['Title','Title'], ['Studio','Studio'], ['Genre','Genre'], ['Actor','Actor / Cast'], ['Director','Director'], ['Writer','Writer'], ['ContentRating','Content Rating'], ['ImdbId','IMDB ID']] },
            { label: 'Metrics', props: [['CommunityRating','Community Rating'], ['Year','Year'], ['Runtime','Runtime (minutes)']] }
        ];
        return groups.map(function (g) {
            return '<optgroup label="' + g.label + '">' +
                g.props.map(function (p) {
                    return '<option value="' + p[0] + '"' + (p[0] === selected ? ' selected' : '') + '>' + p[1] + '</option>';
                }).join('') +
                '</optgroup>';
        }).join('');
    }

    function getMiValueHtml(prop, savedOp, savedVal) {
        if (prop === 'Tag') {
            if (cachedTags.length > 0) {
                var tagOpts = cachedTags.map(function (t) {
                    return '<option value="' + t.replace(/"/g, '&quot;') + '"' + (t === savedVal ? ' selected' : '') + '>' + t + '</option>';
                }).join('');
                return '<select class="selMiValue" is="emby-select" style="flex:1;"><option value="">-- Select tag --</option>' + tagOpts + '</select>';
            }
            return '<input class="txtMiValue" is="emby-input" type="text" placeholder="e.g. 4K" value="' + (savedVal || '').replace(/"/g, '&quot;') + '" style="flex:1;" />';
        }
        if (MI_DROPDOWN_OPTIONS[prop]) {
            var opts = MI_DROPDOWN_OPTIONS[prop].map(function (pair) {
                return '<option value="' + pair[0] + '"' + (pair[0] === savedVal ? ' selected' : '') + '>' + pair[1] + '</option>';
            }).join('');
            return '<select class="selMiValue" is="emby-select" style="flex:1;">' + opts + '</select>';
        }
        if (MI_NUMERIC_PROPS.indexOf(prop) >= 0) {
            var ops = ['=', '>', '>=', '<', '<='];
            var opOpts = ops.map(function (o) {
                return '<option value="' + o + '"' + (o === (savedOp || '>=') ? ' selected' : '') + '>' + o + '</option>';
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
            return '<select class="selMiOp" is="emby-select" style="flex:0 0 64px;">' + opOpts + '</select>' +
                infoTooltip +
                '<input class="txtMiNum" is="emby-input" type="number" step="0.01" value="' + (savedVal || '') + '" style="flex:1;" />';
        }
        var ph = MI_TEXT_PLACEHOLDERS[prop] || '';
        return '<input class="txtMiValue" is="emby-input" type="text" placeholder="' + ph + '" value="' + (savedVal || '').replace(/"/g, '&quot;') + '" style="flex:1;" />';
    }

    function getMediaInfoRuleHtml(criterion) {
        var parsed = parseCriterion(criterion || '');
        var prop = parsed.prop || 'Resolution';
        var notActive = parsed.not;
        var notBg = notActive ? 'rgba(200,50,50,0.75)' : 'transparent';
        var notColor = notActive ? '#fff' : '';
        var notBorder = notActive ? '1px solid rgba(200,50,50,0.6)' : '1px solid rgba(128,128,128,0.4)';
        return '<div class="mi-rule" style="display:flex; gap:6px; align-items:center; margin-bottom:6px;">' +
            '<button type="button" class="btnNotToggle" data-not="' + (notActive ? '1' : '0') + '"' +
            ' style="border:' + notBorder + '; border-radius:10px; padding:3px 10px; font-size:0.78em; font-weight:bold; cursor:pointer; letter-spacing:0.5px; flex-shrink:0;' +
            ' background:' + notBg + '; color:' + notColor + ';" title="Negate this rule">NOT</button>' +
            '<select class="selMiProperty" is="emby-select" style="flex:0 0 155px;">' + propertyOptionsHtml(prop) + '</select>' +
            '<div class="mi-value-wrapper" style="flex:1; display:flex; gap:6px; align-items:center;">' + getMiValueHtml(prop, parsed.op, parsed.val) + '</div>' +
            '<button type="button" class="btnRemoveMiRule" style="background:transparent; border:none; color:#cc3333; cursor:pointer; padding:2px 8px; font-size:1em; flex-shrink:0;" title="Remove rule">✕</button>' +
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

    function renderTagGroup(tagConfig, container, prepend, index, isNew) {
        var isChecked = tagConfig.Active !== false ? 'checked' : '';
        var tagName = tagConfig.Tag || '';
        var labelName = tagConfig.Name || '';
        var urls = tagConfig.Urls || (tagConfig.Url ? [{ url: tagConfig.Url, limit: tagConfig.Limit !== undefined ? tagConfig.Limit : 0 }] : [{ url: '', limit: 0 }]);
        var blacklist = (tagConfig.Blacklist || []).join(', ');
        var intervals = tagConfig.ActiveIntervals || [];
        var idx = typeof index !== 'undefined' ? index : 9999;

        var lastMod = tagConfig.LastModified || new Date().toISOString();

        var enableTag = tagConfig.EnableTag !== false ? 'checked' : '';
        var enableColl = tagConfig.EnableCollection ? 'checked' : '';
        var overrideChecked = tagConfig.OverrideWhenActive ? 'checked' : '';

        var collName = tagConfig.CollectionName || '';
        var collDescription = tagConfig.CollectionDescription || '';
        var collPosterPath = tagConfig.CollectionPosterPath || '';

        var sourceType = tagConfig.SourceType || "External";
        var localSources = tagConfig.LocalSources || [];
        if (localSources.length === 0) localSources = [{ id: "", limit: 0 }];

        var mediaInfoLimit = tagConfig.Limit || 0;

        var mediaFilters = (tagConfig.MediaInfoFilters && tagConfig.MediaInfoFilters.length > 0)
            ? tagConfig.MediaInfoFilters
            : ((tagConfig.MediaInfoConditions && tagConfig.MediaInfoConditions.length > 0)
                ? [{ Operator: 'AND', Criteria: tagConfig.MediaInfoConditions }]
                : []);
        var filterGroupsHtml = mediaFilters.map((f, i) => getMediaInfoFilterGroupHtml(f, i, i === 0)).join('');

        var activeText = tagConfig.Active !== false ? "Active" : "Disabled";
        var activeColor = tagConfig.Active !== false ? "#52B54B" : "var(--theme-text-secondary)";

        var indicatorsHtml = '';
        if (intervals.length > 0) {
            indicatorsHtml += `<span class="tag-indicator schedule"><i class="md-icon" style="font-size:1.1em;">calendar_today</i> Schedule</span>`;
        }
        if (tagConfig.EnableCollection) {
            indicatorsHtml += `<span class="tag-indicator collection"><i class="md-icon" style="font-size:1.1em;">library_books</i> Collection</span>`;
        }

        var sourceCount = 0;
        var sourceLabel = "SOURCE(S)";
        if (sourceType === 'External') {
            sourceCount = urls.length;
        } else if (sourceType === 'LocalCollection' || sourceType === 'LocalPlaylist') {
            sourceCount = localSources.length;
        } else if (sourceType === 'MediaInfo') {
            sourceCount = mediaFilters.length;
            sourceLabel = "FILTER(S)";
        }

        var initialStyle = isNew ? 'display:block;' : 'display:none;';
        var initialIcon = isNew ? 'expand_less' : 'expand_more';

        var inactiveClass = tagConfig.Active === false ? "inactive" : "";
        var newClass = isNew ? "just-added" : "";

        var html = `
        <div class="tag-row ${inactiveClass} ${newClass}" data-index="${idx}" data-last-modified="${lastMod}" data-dirty="false">
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
                        <span class="tag-title" style="font-weight:bold; font-size:1.1em;">${labelName || tagName || 'New'}</span>
                        <span class="tag-status" style="margin-left:10px; font-size:0.8em; opacity:0.7;">${sourceCount} ${sourceLabel}</span>
                        <span class="badge-container" style="display:flex; align-items:center;">${indicatorsHtml}</span>
                    </div>
                </div>
                <i class="md-icon expand-icon">${initialIcon}</i>
            </div>
            <div class="tag-body" style="${initialStyle} padding:15px; border-top:1px solid rgba(255,255,255,0.1);">
                <div class="tag-tabs" style="display: flex; gap: 20px; margin-bottom: 15px; border-bottom: 1px solid rgba(255,255,255,0.1);">
                    <div class="tag-tab active" data-tab="general" style="padding: 8px 0; cursor: pointer; font-weight: bold; border-bottom: 2px solid #52B54B;">Source</div>
                    <div class="tag-tab" data-tab="tag" style="padding: 8px 0; cursor: pointer; opacity: 0.6; font-weight: bold; border-bottom: 2px solid transparent;">Tag</div>
                    <div class="tag-tab" data-tab="collection" style="padding: 8px 0; cursor: pointer; opacity: 0.6; font-weight: bold; border-bottom: 2px solid transparent;">Collection</div>
                    <div class="tag-tab" data-tab="schedule" style="padding: 8px 0; cursor: pointer; opacity: 0.6; font-weight: bold; border-bottom: 2px solid transparent;">Schedule</div>
                    <div class="tag-tab" data-tab="advanced" style="padding: 8px 0; cursor: pointer; opacity: 0.6; font-weight: bold; border-bottom: 2px solid transparent;">Blacklist</div>
                </div>
                
                <div class="tab-content general-tab">
                    <div class="inputContainer" style="flex-grow:1;"><input is="emby-input" class="txtEntryLabel" type="text" label="Display Name" value="${labelName}" /></div>
                    
                    <div style="margin-bottom: 15px;">
                        <label class="selectLabel">Source Type</label>
                        <select is="emby-select" class="selSourceType" style="width:100%;">
                            <option value="External" ${sourceType === 'External' ? 'selected' : ''}>External List (Trakt/MDBList)</option>
                            <option value="LocalCollection" ${sourceType === 'LocalCollection' ? 'selected' : ''}>Local Collection</option>
                            <option value="LocalPlaylist" ${sourceType === 'LocalPlaylist' ? 'selected' : ''}>Local Playlist</option>
                            <option value="MediaInfo" ${sourceType === 'MediaInfo' ? 'selected' : ''}>Local Media Information</option>
                        </select>
                    </div>

                    <div class="source-external-container" style="display: ${sourceType === 'External' ? 'block' : 'none'};">
                        <p style="margin:10px 0 10px 0; font-size:0.9em; font-weight:bold; opacity:0.7;">Source URLs</p>
                        <div class="url-list-container">${urls.map(u => getUrlRowHtml(u.url, u.limit)).join('')}</div>
                        <div style="margin-top:10px;"><button is="emby-button" type="button" class="raised btnAddUrl" style="width:100%; background:transparent; border:2px dashed rgba(128,128,128,0.4); color:var(--theme-text-secondary);"><i class="md-icon" style="margin-right:5px;">add</i>Add another URL</button></div>
                    </div>

                    <div class="source-local-container" style="display: ${(sourceType === 'LocalCollection' || sourceType === 'LocalPlaylist') ? 'block' : 'none'};">
                        <p style="margin:10px 0 10px 0; font-size:0.9em; font-weight:bold; opacity:0.7;" class="local-type-label">${sourceType === 'LocalPlaylist' ? 'Select Playlists' : 'Select Collections'}</p>
                        <div class="local-list-container">${localSources.map(ls => getLocalRowHtml(sourceType, ls.id, ls.limit)).join('')}</div>
                        <div style="margin-top:10px;"><button is="emby-button" type="button" class="raised btnAddLocal" style="width:100%; background:transparent; border:2px dashed rgba(128,128,128,0.4); color:var(--theme-text-secondary);"><i class="md-icon" style="margin-right:5px;">add</i>Add another</button></div>
                    </div>

                    <div class="source-mediainfo-container" style="display: ${sourceType === 'MediaInfo' ? 'block' : 'none'};">
                        <div class="mediainfo-filter-list">${filterGroupsHtml}</div>
                        <button type="button" is="emby-button" class="btnAddMediaInfoFilter raised" style="width:100%; background:transparent; border:2px dashed rgba(128,128,128,0.4); color:var(--theme-text-secondary); margin-top:4px;"><i class="md-icon" style="margin-right:5px;">add</i>Add Filter</button>
                        <div style="width:110px; margin-top:15px;">
                            <input is="emby-input" class="txtMediaInfoLimit" type="number" label="Max (0=All)" value="${mediaInfoLimit}" min="0" />
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
                        <div class="inputContainer" style="flex-grow:1;"><input is="emby-input" class="txtTagName" type="text" label="Tag Name" value="${tagName}" /></div>
                        <p style="margin:5px 0 0 0; font-size:0.9em; opacity:0.7;">The tag that will be applied to matched items in Emby.</p>
                    </div>
                    <div class="checkboxContainer checkboxContainer-withDescription" style="margin-top:16px;">
                        <label>
                            <input is="emby-checkbox" type="checkbox" class="chkOverrideWhenActive" ${overrideChecked} />
                            <span>Priority override when active</span>
                        </label>
                        <div class="fieldDescription">When this entry is in schedule, all other entries sharing the same tag or collection are suppressed — only this entry's items keep the tag and collection.</div>
                    </div>
            </div>

                <div class="tab-content schedule-tab" style="display:none;">
                    <p style="margin:0 0 15px 0; font-size:0.9em; opacity:0.8;">Define when this tag should be active. If empty, it's always active.</p>
                    <div class="date-list-container">${intervals.map(i => getDateRowHtml(i)).join('')}</div>
                    <button is="emby-button" type="button" class="btnAddDate" style="width:100%; background:transparent; border:2px dashed rgba(128,128,128,0.4); color:var(--theme-text-secondary); margin-top:10px;"><i class="md-icon" style="margin-right:5px;">event</i>Add Schedule Rule</button>
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
                            <input is="emby-input" type="text" class="txtCollectionName" label="Collection Name" value="${collName}" placeholder="${tagName}" />
                            <div class="fieldDescription">Leave empty to use Tag Name.</div>
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
                        <textarea is="emby-textarea" class="txtTagBlacklist" rows="2" placeholder="tt1234567, tt9876543">${blacklist}</textarea>
                        <div class="fieldDescription">Items with these IDs will never be tagged or added to collection.</div>
                    </div>
                </div>

                <div style="text-align:right; margin-top:20px; border-top:1px solid var(--line-color); padding-top:10px;"><button is="emby-button" type="button" class="raised btnRemoveGroup" style="background:#cc3333 !important; color:#fff;"><i class="md-icon" style="margin-right:5px;">delete</i>Remove Group</button></div>
            </div>
        </div>`;

        if (prepend) container.insertAdjacentHTML('afterbegin', html);
        else container.insertAdjacentHTML('beforeend', html);

        var newRow = prepend ? container.firstElementChild : container.lastElementChild;
        setupRowEvents(newRow);

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

            var html = '';
            if (hasSchedule) {
                html += `<span class="tag-indicator schedule"><i class="md-icon" style="font-size:1.1em;">calendar_today</i> Schedule</span>`;
            }
            if (hasCollection) {
                html += `<span class="tag-indicator collection"><i class="md-icon" style="font-size:1.1em;">library_books</i> Collection</span>`;
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
            });
        });

        row.addEventListener('change', e => {
            if (e.target.classList.contains('selSourceType')) {
                var type = e.target.value;
                row.querySelector('.source-external-container').style.display = type === 'External' ? 'block' : 'none';
                row.querySelector('.source-local-container').style.display = (type === 'LocalCollection' || type === 'LocalPlaylist') ? 'block' : 'none';
                row.querySelector('.source-mediainfo-container').style.display = type === 'MediaInfo' ? 'block' : 'none';

                if (type === 'MediaInfo') {
                    var miList = row.querySelector('.mediainfo-filter-list');
                    if (miList && miList.querySelectorAll('.mediainfo-filter-group').length === 0) {
                        miList.insertAdjacentHTML('beforeend', getMediaInfoFilterGroupHtml({ Operator: 'AND', Criteria: [], GroupOperator: 'AND' }, 0, true));
                    }
                }

                if (type === 'LocalCollection' || type === 'LocalPlaylist') {
                    row.querySelector('.local-list-container').innerHTML = getLocalRowHtml(type, "", 0);
                    row.querySelector('.local-type-label').textContent = type === 'LocalPlaylist' ? "Select Playlists" : "Select Collections";
                }
                updateCount(row);
            }
            
            if (e.target.classList.contains('selMiProperty')) {
                var wrapper = e.target.closest('.mi-rule').querySelector('.mi-value-wrapper');
                wrapper.innerHTML = getMiValueHtml(e.target.value, '', '');
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
            if (e.target.classList.contains('chkEnableTag')) {
                row.querySelector('.tag-settings').style.display = e.target.checked ? 'block' : 'none';
            }
            if (e.target.classList.contains('chkEnableCollection')) {
                var settingsDiv = row.querySelector('.collection-settings');
                settingsDiv.style.display = e.target.checked ? 'block' : 'none';


                updateBadges(row);
            }
        });

        var header = row.querySelector('.tag-header'), body = row.querySelector('.tag-body'), icon = row.querySelector('.expand-icon');
        header.addEventListener('click', e => {
            if (e.target.closest('.header-actions')) return;
            var isHidden = body.style.display === 'none';
            body.style.display = isHidden ? 'block' : 'none';
            icon.innerText = isHidden ? 'expand_less' : 'expand_more';
        });

        var chk = row.querySelector('.chkTagActive'), lblStatus = row.querySelector('.lblActiveStatus');
        chk.addEventListener('change', function () {
            lblStatus.textContent = this.checked ? "Active" : "Disabled";
            lblStatus.style.color = this.checked ? "#52B54B" : "var(--theme-text-secondary)";

            if (this.checked) row.classList.remove('inactive');
            else row.classList.add('inactive');
        });

        row.querySelector('.btnAddUrl').addEventListener('click', () => {
            row.querySelector('.url-list-container').insertAdjacentHTML('beforeend', getUrlRowHtml('', 0));
            updateCount(row);
        });

        row.querySelector('.btnAddLocal').addEventListener('click', () => {
            var st = row.querySelector('.selSourceType').value;
            row.querySelector('.local-list-container').insertAdjacentHTML('beforeend', getLocalRowHtml(st, "", 0));
            updateCount(row);
        });

        row.querySelector('.btnAddDate').addEventListener('click', () => {
            row.querySelector('.date-list-container').insertAdjacentHTML('beforeend', getDateRowHtml({ Type: 'SpecificDate' }));
            updateBadges(row);
        });

        row.addEventListener('click', e => {
            if (e.target.closest('.btnRemoveUrl')) {
                e.target.closest('.url-row').remove();
                updateCount(row);
            }

            if (e.target.closest('.btnRemoveLocal')) {
                e.target.closest('.local-row').remove();
                updateCount(row);
            }

            if (e.target.closest('.btnRemoveDate')) {
                e.target.closest('.date-row').remove();
                updateBadges(row);
            }

            if (e.target.closest('.btnAddMediaInfoFilter')) {
                var list = row.querySelector('.mediainfo-filter-list');
                var idx = list.querySelectorAll('.mediainfo-filter-group').length;
                list.insertAdjacentHTML('beforeend', getMediaInfoFilterGroupHtml({ Operator: 'AND', Criteria: [], GroupOperator: 'AND' }, idx, false));
                updateCount(row);
            }

            if (e.target.closest('.btnRemoveFilterGroup')) {
                e.target.closest('.mediainfo-filter-group').remove();
                var miList = row.querySelector('.mediainfo-filter-list');
                var firstGroup = miList && miList.querySelector('.mediainfo-filter-group');
                if (firstGroup) { var conn = firstGroup.querySelector('.mi-group-connector'); if (conn) conn.remove(); }
                updateCount(row);
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

            if (e.target.closest('.btnAddMiRule')) {
                var rulesList = e.target.closest('.mediainfo-filter-group').querySelector('.mi-rules-list');
                rulesList.insertAdjacentHTML('beforeend', getMediaInfoRuleHtml(''));
                updateCount(row);
            }

            if (e.target.closest('.btnRemoveMiRule')) {
                e.target.closest('.mi-rule').remove();
                updateCount(row);
            }

            if (e.target.closest('.btnRemoveGroup')) {
                if (confirm("Delete this tag group?")) {
                    row.remove();
                }
            }

            var btnTest = e.target.closest('.btnTestUrl');
            if (btnTest) {
                var uRow = btnTest.closest('.url-row');
                var url = uRow.querySelector('.txtTagUrl').value;
                if (!url) return;

                btnTest.disabled = true;
                window.ApiClient.getJSON(window.ApiClient.getUrl("AutoTag/TestUrl", { Url: url, Limit: 1000 })).then(result => {
                    window.Dashboard.alert(result.Message);
                }).finally(() => btnTest.disabled = false);
            }
        });

        function updateTagTitle() {
            var lbl = row.querySelector('.txtEntryLabel').value;
            var tag = row.querySelector('.txtTagName').value;
            row.querySelector('.tag-title').textContent = lbl || tag || 'New';
        }
        row.querySelector('.txtEntryLabel').addEventListener('input', updateTagTitle);
        row.querySelector('.txtTagName').addEventListener('input', updateTagTitle);

        var handle = row.querySelector('.drag-handle');

        handle.addEventListener('mousedown', () => {
            if (localStorage.getItem('AutoTag_SortBy') === 'Manual') {
                row.setAttribute('draggable', 'true');
            }
        });

        handle.addEventListener('mouseup', () => {
            row.setAttribute('draggable', 'false');
        });

        row.addEventListener('dragstart', (e) => {
            if (localStorage.getItem('AutoTag_SortBy') !== 'Manual') { e.preventDefault(); return; }

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
                fetch(window.ApiClient.getUrl('AutoTag/UploadCollectionImage'), {
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

            fetch(window.ApiClient.getUrl('AutoTag/FetchCollectionImageFromUrl'), {
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
    }

    function updateCount(row) {
        var sourceType = row.querySelector('.selSourceType').value;
        var count = 0;
        var label = "SOURCE(S)";
        
        if (sourceType === 'External') {
            count = row.querySelectorAll('.url-row').length;
        } else if (sourceType === 'LocalCollection' || sourceType === 'LocalPlaylist') {
            count = row.querySelectorAll('.local-row').length;
        } else if (sourceType === 'MediaInfo') {
            count = row.querySelectorAll('.mi-rule').length;
            label = "RULE(S)";
        }
        
        row.querySelector('.tag-status').textContent = count + " " + label;
    }

    function refreshStatus(view) {
        var myId = ++statusRequestId;
        Promise.all([
            window.ApiClient.getJSON(window.ApiClient.getUrl("AutoTag/Status")),
            window.ApiClient.getJSON(window.ApiClient.getUrl("AutoTag/Hsc/Status")).catch(function () { return null; })
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
                    allLogs.push({ t: getLogTime(l), text: l.replace(/^(\[\d{2}:\d{2}:\d{2}\]) /, '$1 [TAG] ') });
                });
                (hscResult && hscResult.Logs || []).forEach(function (l) {
                    allLogs.push({ t: getLogTime(l), text: l.replace(/^(\[\d{2}:\d{2}:\d{2}\]) /, '$1 [HSC] ') });
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

    function getUiConfig(view, forComparison) {
        var flatTags = [];
        view.querySelectorAll('.tag-row').forEach(row => {
            var entryLabel = row.querySelector('.txtEntryLabel').value;
            var name = row.querySelector('.txtTagName').value || entryLabel;
            var active = row.querySelector('.chkTagActive').checked;

            var blInput = row.querySelector('.txtTagBlacklist');
            var bl = blInput ? blInput.value.split(',').map(s => s.trim()).filter(s => s.length > 0) : [];

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
                    var val = selVal ? selVal.value : (txtVal ? txtVal.value.trim() : '');
                    var op2 = selOp ? selOp.value : '';
                    var num = txtNum ? txtNum.value.trim() : '';
                    var finalVal = op2 ? num : val;
                    var notBtn = rule.querySelector('.btnNotToggle');
                    var isNot = notBtn && notBtn.dataset.not === '1';
                    var crit = buildCriterion(prop, op2, finalVal);
                    if (crit) criteria.push(isNot ? '!' + crit : crit);
                });
                if (criteria.length > 0) miFilters.push({ Operator: operator, Criteria: criteria, GroupOperator: groupOp });
            });

            var baseTag = {
                Name: entryLabel, Tag: name, Active: active, Blacklist: bl, ActiveIntervals: intervals,
                EnableTag: enableTagChk, EnableCollection: enableColl, CollectionName: collName, CollectionDescription: collDescription, CollectionPosterPath: collPoster, OnlyCollection: false, OverrideWhenActive: overrideWhenActive, LastModified: currentLastMod,
                SourceType: st, MediaInfoFilters: miFilters, MediaInfoConditions: []
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
            } else {
                var miLimitVal = parseInt((row.querySelector('.txtMediaInfoLimit') || {}).value, 10) || 0;
                flatTags.push(Object.assign({}, baseTag, { Url: "", Limit: miLimitVal, LocalSourceId: "" }));
            }
        });

        var hscEnabled = view.querySelector('#chkHscEnabled');
        var hscSource  = view.querySelector('#selHscSourceUser');

        return {
            TraktClientId: view.querySelector('#txtTraktClientId').value,
            MdblistApiKey: view.querySelector('#txtMdblistApiKey').value,
            ExtendedConsoleOutput: view.querySelector('#chkExtendedConsoleOutput').checked,
            DryRunMode: view.querySelector('#chkDryRunMode').checked,
            Tags: flatTags,
            HomeSyncEnabled: hscEnabled ? hscEnabled.checked : (lastHscConfig.HomeSyncEnabled || false),
            HomeSyncSourceUserId: hscSource ? (hscSource.value || '') : (lastHscConfig.HomeSyncSourceUserId || ''),
            HomeSyncTargetUserIds: hscEnabled
                ? Array.from(view.querySelectorAll('.hsc-target-chk:checked')).map(function(c) { return c.value; })
                : (lastHscConfig.HomeSyncTargetUserIds || [])
        };
    }

    function checkFormState() {
        var view = document.querySelector('#AutoTagConfigPage');
        if (!view || !originalConfigState) return;
        
        var isDirty = false;
        try {
            var current = JSON.stringify(getUiConfig(view, true));
            isDirty = current !== originalConfigState;
        } catch (e) {
            isDirty = true;
        }

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
        var view = document.querySelector('#AutoTagConfigPage');
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

        var showScheduleOnly = view.querySelector('#chkFilterSchedule').checked;
        var showCollectionOnly = view.querySelector('#chkFilterCollection').checked;
        var searchTerm = (view.querySelector('#txtSearchTags').value || "").toLowerCase();

        rows.forEach(row => {
            var tagName = (row.querySelector('.txtTagName').value || "").toLowerCase();
            var entryLbl = (row.querySelector('.txtEntryLabel').value || "").toLowerCase();
            var hasSchedule = row.querySelectorAll('.date-row').length > 0;
            var hasCollection = row.querySelector('.chkEnableCollection').checked;

            var matchesSearch = tagName.includes(searchTerm) || entryLbl.includes(searchTerm);
            var matchesFilter = true;
            if (showScheduleOnly && showCollectionOnly) {
                matchesFilter = (hasSchedule || hasCollection);
            } else if (showScheduleOnly) {
                matchesFilter = hasSchedule;
            } else if (showCollectionOnly) {
                matchesFilter = hasCollection;
            }

            row.style.display = (matchesSearch && matchesFilter) ? 'block' : 'none';
        });
    }

    function checkForUpdates(view) {
        if (view.querySelector('#autoTagVersionBadge')) return;

        window.ApiClient.getJSON(window.ApiClient.getUrl("AutoTag/Version")).then(function (result) {
            var currentVer = result.Version || '';
            if (!currentVer) return;

            fetch('https://api.github.com/repos/soderlund91/AutoTag/releases/latest')
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
                    Tag: t.Tag, Name: t.Name || '', Urls: [], LocalSources: [], Active: t.Active, Blacklist: t.Blacklist, ActiveIntervals: t.ActiveIntervals,
                    EnableTag: t.EnableTag !== false, EnableCollection: t.EnableCollection, CollectionName: t.CollectionName, CollectionDescription: t.CollectionDescription || '', CollectionPosterPath: t.CollectionPosterPath || '', OnlyCollection: t.OnlyCollection, OverrideWhenActive: t.OverrideWhenActive || false, LastModified: t.LastModified,
                    SourceType: t.SourceType || "External", MediaInfoConditions: t.MediaInfoConditions || [], MediaInfoFilters: t.MediaInfoFilters || [],
                    Limit: t.Limit || 0
                };
            }
            if (t.SourceType === 'External' && t.Url) grouped[key].Urls.push({ url: t.Url, limit: t.Limit });
            if ((t.SourceType === 'LocalCollection' || t.SourceType === 'LocalPlaylist') && t.LocalSourceId) grouped[key].LocalSources.push({ id: t.LocalSourceId, limit: t.Limit });
            if (t.SourceType === 'MediaInfo') grouped[key].Limit = t.Limit;
        });
        return grouped;
    }

    // ── Home Screen  ─────────────────────────────────────────────────

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

        var enabled   = config.HomeSyncEnabled ? ' checked' : '';

        container.innerHTML = [
            '<div class="hsc-card">',
            '<h3 class="hsc-section-title">Configuration</h3>',
            '<div class="checkboxContainer checkboxContainer-withDescription">',
            '<label><input is="emby-checkbox" type="checkbox" id="chkHscEnabled"' + enabled + ' /><span>Enable Home Screen sync</span></label>',
            '<div class="fieldDescription">When enabled, the plugin syncs all Home Sections from the target user and applies it to those selected. When disabled, the task always skips — even if triggered manually.</div>',
            '</div>',
            '<div class="inputContainer" style="margin-top:16px;">',
            '<select is="emby-select" id="selHscSourceUser" label="Source User">',
            '<option value="">— Select source user —</option>',
            sourceOptions,
            '</select>',
            '<div class="fieldDescription">Home screen sections will be copied FROM this user to all users selected below.</div>',
            '</div>',
            '</div>',

            '<div class="hsc-card">',
            '<h3 class="hsc-section-title">Sync to</h3>',
            '<p class="textMuted" style="font-size:0.88em;margin-bottom:12px;">These users will receive the source user\'s home screen layout on each sync.</p>',
            '<div class="hsc-user-list" id="hscTargetList">',
            targetRows || '<p class="textMuted" style="font-size:0.85em;">No users found.</p>',
            '</div>',
            '</div>',

        ].join('');

        container.dataset.loaded = '1';
    }

    function loadHscUsers(view) {
        var container = view.querySelector('#hscContainer');
        if (!container) return;

        window.ApiClient.getJSON(window.ApiClient.getUrl('Users', { IsDisabled: false }))
            .then(function (users) {
                renderHscTab(container, lastHscConfig, users || []);

                container.querySelectorAll('input, select').forEach(function (el) {
                    el.addEventListener('change', function () { setTimeout(checkFormState, 0); });
                    el.addEventListener('input',  function () { setTimeout(checkFormState, 0); });
                });

            })
            .catch(function () {
                container.innerHTML = '<p class="textMuted" style="padding:20px;">Failed to load users. Check server connection.</p>';
            });
    }

    // ─────────────────────────────────────────────────────────────────────────

    return function (view) {
        view.addEventListener('viewshow', () => {
            if (!document.getElementById('autoTagCustomCss')) {
                document.body.insertAdjacentHTML('beforeend', customCss);
            }

            var form = view.querySelector('.AutoTagForm');
            var isFirstVisit = !view.dataset.autotagInit;
            if (isFirstVisit) view.dataset.autotagInit = '1';

            originalConfigState = null;

            var changeHandler = function() {
                setTimeout(checkFormState, 0);
            };
            if (_formAc) _formAc.abort();
            _formAc = new AbortController();
            var _signal = _formAc.signal;
            form.addEventListener('input', changeHandler, { signal: _signal });
            form.addEventListener('change', changeHandler, { signal: _signal });
            var settingsTab = view.querySelector('#tabSettings');
            settingsTab.addEventListener('input',  changeHandler, { signal: _signal });
            settingsTab.addEventListener('change', changeHandler, { signal: _signal });
            form.addEventListener('click', (e) => {
                if (e.target.closest('.btnRemoveUrl, .btnAddUrl, .btnRemoveLocal, .btnAddLocal, .btnRemoveDate, .btnAddDate, .btnRemoveFilterGroup, .btnAddMediaInfoFilter, .btnGroupOpChoice, .btnGroupInnerOpChoice, .btnAddMiRule, .btnRemoveMiRule, .btnRemoveGroup, .day-toggle, .btnRemovePoster')) {
                    changeHandler();
                }
            }, { signal: _signal });
            
            var container = view.querySelector('#tagListContainer');

            if (isFirstVisit) {
                if (container) {
                    let rafId = null;
                    container.addEventListener('dragover', (e) => {
                        if (localStorage.getItem('AutoTag_SortBy') !== 'Manual') return;
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
                        if (localStorage.getItem('AutoTag_SortBy') !== 'Manual') return;
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

                var headerAction = view.querySelector('.sectionTitleContainer');
                if (headerAction && !view.querySelector('#cbSortTags')) {
                    var savedSort = localStorage.getItem('AutoTag_SortBy') || 'Manual';
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

                        <span class="control-label" style="opacity:0.7; margin-right: 20px; flex-shrink: 0;"> | Filters:</span>

                        <div class="search-input-wrapper" style="width: 200px !important; margin-right: 10px; flex-shrink: 0;">
                            <i class="md-icon search-icon">search</i>
                            <input type="text" id="txtSearchTags" placeholder="Search..." autocomplete="off" style="padding-left: 28px !important; background: rgba(128,128,128,0.06) !important; border: 1px solid var(--line-color) !important; width: 100% !important;" />
                            <i class="md-icon" id="btnClearSearch">close</i>
                        </div>

                        <label class="checkboxContainer" style="display: flex !important; align-items: center !important; margin: 0 10px 0 0 !important; cursor: pointer !important; width: auto !important;">
                            <input type="checkbox" id="chkFilterSchedule" is="emby-checkbox" />
                            <span style="font-size: 0.9em; margin-left: 5px; white-space: nowrap;">Schedule</span>
                        </label>

                        <label class="checkboxContainer" style="display: flex !important; align-items: center !important; margin: 0 !important; cursor: pointer !important; width: auto !important;">
                            <input type="checkbox" id="chkFilterCollection" is="emby-checkbox" />
                            <span style="font-size: 0.9em; margin-left: 5px; white-space: nowrap;">Collection</span>
                        </label>

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
                        localStorage.setItem('AutoTag_SortBy', this.value);
                        sortRows(view.querySelector('#tagListContainer'), this.value);
                    });

                    view.querySelector('#chkFilterSchedule').addEventListener('change', () => applyFilters(view));
                    view.querySelector('#chkFilterCollection').addEventListener('change', () => applyFilters(view));
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
                        a.href = url; a.download = `AutoTag_Backup_${new Date().toISOString().split('T')[0]}.json`;
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
                            view.querySelector('#chkExtendedConsoleOutput').checked = config.ExtendedConsoleOutput || false;
                            view.querySelector('#chkDryRunMode').checked = config.DryRunMode || false;

                            var container = view.querySelector('#tagListContainer'); container.innerHTML = '';
                            var grouped = groupConfigTags(config.Tags);

                            var keys = Object.keys(grouped);
                            keys.forEach((k, i) => renderTagGroup(grouped[k], container, false, i));
                            if (keys.length === 0) renderTagGroup({ Tag: '', Urls: [{ url: '', limit: 50 }], Active: true }, container, false, 0);

                            applyFilters(view);

                            requestAnimationFrame(function () {
                                try {
                                    originalConfigState = JSON.stringify(getUiConfig(view, true));
                                } catch (err) {
                                    originalConfigState = null;
                                }
                                window.Dashboard.alert("Configuration loaded!");
                                checkFormState();
                            });
                        } catch (err) {
                            window.Dashboard.alert("Failed to parse configuration file. The file may be corrupt or not a valid AutoTag backup file. Error: " + err.message);
                        }
                        fileInput.value = '';
                    };
                    reader.readAsText(file);
                });
            }

            if (!view.querySelector('.dry-run-warning')) {
                view.insertAdjacentHTML('afterbegin', '<div class="dry-run-warning"><i class="md-icon" style="font-size:1.4em;"></i>AUTOTAG<br>DRY RUN MODE IS ACTIVE - NO CHANGES WILL BE SAVED</div>');
            }

            var btnSave = view.querySelector('.btn-save');
            if (btnSave) { btnSave.disabled = true; btnSave.style.opacity = "0.5"; }

            checkForUpdates(view);
            refreshStatus(view);
            statusInterval = setInterval(() => refreshStatus(view), 5000);

            Promise.all([
                window.ApiClient.getJSON(window.ApiClient.getUrl("Users/" + window.ApiClient.getCurrentUserId() + "/Items", { IncludeItemTypes: "BoxSet", Recursive: true })),
                window.ApiClient.getJSON(window.ApiClient.getUrl("Users/" + window.ApiClient.getCurrentUserId() + "/Items", { IncludeItemTypes: "Playlist", Recursive: true })),
                window.ApiClient.getJSON(window.ApiClient.getUrl("Items/Filters2", { UserId: window.ApiClient.getCurrentUserId(), Recursive: true })).catch(function () { return { Tags: [] }; })
            ]).then(responses => {
                cachedCollections = responses[0].Items || [];
                cachedPlaylists = responses[1].Items || [];
                cachedTags = ((responses[2] && responses[2].Tags) || []).slice().sort();

                window.ApiClient.getPluginConfiguration(pluginId).then(config => {
                    lastHscConfig = {
                        HomeSyncEnabled:       config.HomeSyncEnabled       || false,
                        HomeSyncSourceUserId:  config.HomeSyncSourceUserId  || '',
                        HomeSyncTargetUserIds: config.HomeSyncTargetUserIds || []
                    };

                    var container = view.querySelector('#tagListContainer'); container.innerHTML = '';
                    view.querySelector('#txtTraktClientId').value = config.TraktClientId || '';
                    view.querySelector('#txtMdblistApiKey').value = config.MdblistApiKey || '';
                    view.querySelector('#chkExtendedConsoleOutput').checked = config.ExtendedConsoleOutput || false;
                    view.querySelector('#chkDryRunMode').checked = config.DryRunMode || false;
                    if (view.querySelector('#txtSearchTags')) {
                        view.querySelector('#txtSearchTags').value = '';
                        view.querySelector('#btnClearSearch').style.display = 'none';
                    }
                    if (view.querySelector('#chkFilterSchedule')) view.querySelector('#chkFilterSchedule').checked = false;
                    if (view.querySelector('#chkFilterCollection')) view.querySelector('#chkFilterCollection').checked = false;

                    var grouped = groupConfigTags(config.Tags);

                    var keys = Object.keys(grouped);
                    keys.forEach((k, i) => renderTagGroup(grouped[k], container, false, i));

                    if (keys.length === 0) renderTagGroup({ Tag: '', Urls: [{ url: '', limit: 0 }], Active: true }, container, false, 0);

                    var savedSort = localStorage.getItem('AutoTag_SortBy') || 'Manual';
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

        view.querySelector('.AutoTagForm').addEventListener('submit', e => {
            e.preventDefault();
            
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

            window.ApiClient.updatePluginConfiguration(pluginId, configObj).then(r => {
                window.Dashboard.processPluginConfigurationUpdateResult(r);

                var newGrouped = groupConfigTags(configObj.Tags);
                view.querySelectorAll('.tag-row').forEach(row => {
                    var name = row.querySelector('.txtEntryLabel').value;
                    var tagName = row.querySelector('.txtTagName').value || name;
                    var key = name ? name + '\x1F' + tagName : tagName;
                    if (newGrouped[key]) {
                        row.dataset.lastModified = newGrouped[key].LastModified;
                    }
                });

                originalConfigState = JSON.stringify(getUiConfig(view, true));
                checkFormState();
                updateDryRunWarning();
            });
        });

        // Speed dial — open/close
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
            runTask('AutoTagSyncTask', 'Tag sync');
        });

        view.querySelector('#btnDialHomeScreen').addEventListener('click', function () {
            runTask('HomeSectionSyncTask', 'Home screen sync');
        });

        view.querySelector('#btnDialFullSync').addEventListener('click', function () {
            window.ApiClient.getScheduledTasks().then(function (tasks) {
                var tagTask = tasks.find(function (x) { return x.Key === 'AutoTagSyncTask'; });
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

        view.addEventListener('viewhide', function () {
            document.removeEventListener('click', closeSpeedDial);
        }, { once: true });

        // ── Page-level tab switching ──────────────────────────────────────────
        view.querySelectorAll('.page-tab-btn').forEach(function (btn) {
            btn.addEventListener('click', function () {
                var target = this.getAttribute('data-page-tab');
                view.querySelectorAll('.page-tab-btn').forEach(function (b) { b.classList.remove('active'); });
                this.classList.add('active');
                view.querySelectorAll('.page-tab-content').forEach(function (c) { c.style.display = 'none'; });
                view.querySelector('#tab' + target).style.display = '';

                if (target === 'HomeCompanion') {
                    var hscContainer = view.querySelector('#hscContainer');
                    if (hscContainer && !hscContainer.dataset.loaded) {
                        loadHscUsers(view);
                    }
                }
            });
        });

        var origStatusInterval = statusInterval;
        if (origStatusInterval) clearInterval(origStatusInterval);
        statusInterval = setInterval(function () {
            refreshStatus(view);
        }, 5000);
    };
});