/**
 * jQuery UI Widget for S3Organizer
 *
 * @copyright 2018-2023 (c) Sahana Software Foundation
 * @license MIT
 *
 * requires jQuery 1.9.1+
 * requires jQuery UI 1.10 widget factory
 * requires jQuery UI datepicker
 * requires moment.js
 * requires fullCalendar
 * requires qTip2
 */

/* jshint esversion: 6 */

(function($, undefined) {

    "use strict";
    var organizerID = 0;

    // ------------------------------------------------------------------------
    // EVENT CACHE

    /**
     * Discontinuous Event Cache to reduce Ajax-calls when browsing dates
     */
    function EventCache() {

        this.items = {};  // {id: item}
        this.slices = []; // [[startMoment, endMoment, {id: item, ...}], ...]
    }

    /**
     * Store events within a certain time interval
     *
     * @param {moment} start - the (inclusive) start date/time of the interval
     * @param {moment} end - the (exclusive) end date/time of the interval
     * @param {Array} items - the event items in this interval
     */
    EventCache.prototype.store = function(start, end, items) {

        // Convert items array into object with item IDs as keys
        let events = {};
        items.forEach(function(item) {
            this.items[item.id] = events[item.id] = item;
        }, this);

        // Add the new slice
        let slices = this.slices,
            slice = [moment(start), moment(end), events];
        slices.push(slice);

        // Sort slices
        slices.sort(function(x, y) {
            if (x[0].isBefore(y[0])) {
                return -1;
            } else if (y[0].isBefore(x[0])) {
                return 1;
            } else {
                if (x[1].isBefore(y[1])) {
                    return -1;
                } else if (y[1].isBefore(x[1])) {
                    return 1;
                }
            }
            return 0;
        });

        // Merge overlapping/adjacent slices
        if (slices.length > 1) {
            let newSlices = [];
            let merged = slices.reduce(function(x, y) {
                if (x[1].isBefore(y[0]) || x[0].isAfter(y[1])) {
                    // Slices do not overlap
                    newSlices.push(x);
                    return y;
                } else {
                    // Slices overlap
                    return [
                        moment.min(x[0], y[0]),
                        moment.max(x[1], y[1]),
                        $.extend({}, x[2], y[2])
                    ];
                }
            });
            newSlices.push(merged);
            this.slices = newSlices;
        }
    };

    /**
     * Retrieve events within a certain time interval
     *
     * @param {moment|Date|string} start - the start of the interval
     * @param {moment|Date|string} end - the end of the interval
     *
     * @returns {Array} - the events within the interval,
     *                    or null if the interval is not completely cached
     */
    EventCache.prototype.retrieve = function(start, end) {

        start = moment(start);
        end = moment(end);

        let slices = this.slices,
            numSlices = slices.length,
            slice,
            events,
            eventID,
            event,
            eventStart,
            items = [];

        for (let i = 0; i < numSlices; i++) {
            slice = slices[i];
            if (slice[0].isSameOrBefore(start) && slice[1].isSameOrAfter(end)) {
                events = slice[2];
                for (eventID in events) {
                    event = events[eventID];
                    eventStart = moment(event.start);
                    if (eventStart.isAfter(end)) {
                        continue;
                    }
                    if (event.end) {
                        if (moment(event.end).isBefore(start)) {
                            continue;
                        }
                    } else {
                        if (eventStart.isSameOrBefore(moment(start).subtract(1, 'days'))) {
                            continue;
                        }
                    }
                    items.push(event);
                }
                return items;
            }
        }
        return null;
    };

    /**
     * Update an item in the cache after it has been dragged&dropped
     * to another date, or resized.
     *
     * NB moving the item to another slice is unnecessary because
     *    items can only ever be moved between or resized within
     *    dates that are visible at the same time, and hence belong
     *    to the same slice (due to slice-merging in store())
     *
     * @param {integer} itemID - the item record ID
     * @param {object} data - the data to update the item with
     */
    EventCache.prototype.updateItem = function(itemID, data) {

        let item = this.items[itemID];

        if (item && data) {
            $.extend(item, data);
        }
    };

    /**
     * Remove an item from the cache
     *
     * @param {integer} itemID - the item record ID
     */
    EventCache.prototype.deleteItem = function(itemID) {

        this.slices.forEach(function(slice) {
            delete slice[2][itemID];
        });
        delete this.items[itemID];
    };

    /**
     * Clear the cache
     */
    EventCache.prototype.clear = function() {

        this.slices = [];
        this.items = {};
    };

    // ------------------------------------------------------------------------
    // UI WIDGET

    /**
     * Organizer
     */
    $.widget('s3.organizer', {

        /**
         * Default options
         *
         * @prop {string} locale - the locale to use
         *                         (one of those in fullcalendar/locale)
         * @prop {integer} timeout - the Ajax timeout (in milliseconds)
         * @prop {Array} resources - the resources, array of resource objects:
         *
         *   @prop {string} resource.start - start date column name
         *   @prop {string} resource.end - end date column name
         *   @prop {string} resource.ajaxURL - URL for Ajax-lookups
         *   @prop {string} resource.baseURL - base URL for modals (create/update)
         *   @prop {boolean} resource.useTime - use time (and hence, agenda views)
         *   @prop {boolean} resource.insertable - new items can be created
         *   @prop {string} resource.labelCreate - CRUD label for create
         *   @prop {boolean} resource.editable - items can be edited
         *   @prop {boolean} resource.startEditable - item start can be changed
         *   @prop {boolean} resource.durationEditable - item duration can be changed
         *   @prop {boolean} resource.deletable - items can be deleted
         *   @prop {boolean} resource.reloadOnUpdate - reload all items after
         *                                             updating start/duration
         *   @prop {string} resource.color - column name to determine item color
         *   @prop {object} resource.colors - mapping of color-column value to color:
         *                                    {value: '#rrggbb'}
         *
         * @prop {float} aspectRatio: the aspect ratio of the calendar
         * @prop {boolean} nowIndicator: show the now-indicator in agenda views
         * @prop {string} slotDuration: the slot size in agenda views
         * @prop {string} defaultTimedEventDuration: the default event duration for
         *                                           timed events without explicit end
         * @prop {object|Array} businessHours: business hours, an object or array of
         *                                     objects of the format:
         *                                     {dow:[0,1,2,3,4,5,6], start: "HH:MM", end: "HH:MM"},
         *                                     - false to disable
         * @prop {string} timeFormat: time format for events
         * @prop {integer} firstDay: first day of the week (0=Sunday, 1=Monday etc.)
         * @prop {boolean} useTime: use date+time for events (rather than just dates)
         * @prop {boolean} yearView: whether to have a year view (only when not using time)
         * @prop {string} labelEdit: label for Edit-button
         * @prop {string} labelDelete: label for the Delete-button
         * @prop {string} labelReload: label for the Reload-button
         * @prop {string} labelGoto: label for the Goto-button
         * @prop {string} deleteConfirmation: the question for the delete-confirmation
         * @prop {string} refreshIconClass: the CSS class for the refresh button icon
         * @prop {string} calendarIconClass: the CSS class for the calendar button icon
         */
        options: {

            locale: 'en',
            timeout: 10000,
            resources: null,

            aspectRatio: 1.8,
            nowIndicator: true,
            slotDuration: '00:30:00',
            snapDuration: '00:15:00',
            defaultTimedEventDuration: '00:30:00',
            businessHours: false,
            weekNumbers: true,
            timeFormat: {
                hour: '2-digit',
                minute: '2-digit'
            },
            firstDay: 1,
            useTime: false,
            yearView: true,

            labelEdit: 'Edit',
            labelDelete: 'Delete',
            labelReload: 'Reload',
            labelGoto: 'Go to Date',
            deleteConfirmation: 'Do you want to delete this entry?',

            refreshIconClass: 'fa fa-refresh',
            calendarIconClass: 'fa fa-calendar'
        },

        /**
         * Create the widget
         */
        _create: function() {

            this.id = organizerID;
            organizerID += 1;

            this.eventNamespace = '.organizer';
        },

        /**
         * Update the widget options
         */
        _init: function() {

            this.calendar = null;

            this.openRequest = null;
            this.loadCount = -1;

            this.refresh();
        },

        /**
         * Remove generated elements & reset other changes
         */
        _destroy: function() {

            if (this.calendar !== null) {
                this.calendar.destroy();
                this.calendar = null;
            }

            $.Widget.prototype.destroy.call(this);
        },

        /**
         * Redraw contents
         */
        refresh: function() {

            this._unbindEvents();

            // Remove any previous calendar
            if (this.calendar !== null) {
                this.calendar.destroy();
                this.calendar = null;
            }

            let opts = this.options;

            // Can records be created for any resource?
            let resourceConfigs = opts.resources,
                insertable = false,
                allDaySlot = false;
            resourceConfigs.forEach(function(resourceConfig) {
                if (resourceConfig.insertable) {
                    insertable = true;
                }
                if (!resourceConfig.useTime) {
                    allDaySlot = true;
                }
            });

            // Determine available views and default view
            let leftHeader,
                defaultView;
            if (opts.useTime) {
                leftHeader = 'dayGridMonth,timeGridWeek,timeGridDay reload';
                defaultView = 'timeGridWeek';
            } else {
                if (opts.yearView) {
                    leftHeader = 'multiMonthYear,dayGridMonth,dayGridWeek reload';
                } else {
                    leftHeader = 'dayGridMonth,dayGridWeek reload';
                }
                defaultView = 'dayGridMonth';
            }

            let datePicker = $('#' + $(this.element).attr('id') + '-date-picker'),
                self = this;

            let calendar = new FullCalendar.Calendar(this.element[0], {

                // General options
                aspectRatio: opts.aspectRatio,
                nowIndicator: opts.nowIndicator,
                slotDuration: opts.slotDuration,
                snapDuration: opts.snapDuration,
                displayEventEnd: false,
                defaultTimedEventDuration: opts.defaultTimedEventDuration,
                allDaySlot: allDaySlot,
                firstDay: opts.firstDay,
                eventTimeFormat: opts.timeFormat,
                slotLabelFormat: opts.timeFormat,
                businessHours: opts.businessHours,
                weekNumbers: opts.weekNumbers,

                // Permitted actions
                selectable: insertable,
                editable: true,

                // Header toolbar
                customButtons: {
                    reload: {
                        text: '',
                        hint: opts.labelReload,
                        click: function() {
                            self.reload();
                        }
                    },
                    calendar: {
                        text: '',
                        hint: opts.labelGoto,
                        click: function() {
                            datePicker.datepicker('show');
                        }
                    }
                },
                headerToolbar: {
                    start: leftHeader,
                    center: 'title',
                    end: 'calendar today prev,next'
                },
                initialView: defaultView,

                // View-specific options
                multiMonthMaxColumns: 2,
                views: {
                    dayGridWeek: {
                        selectable: !opts.useTime,
                        aspectRatio: opts.aspectRatio * 3 / 2
                    },
                    dayGridMonth: {
                        selectable: !opts.useTime
                    },
                    multiMonthYear: {
                        selectable: !opts.useTime,
                        aspectRatio: opts.aspectRatio * 2 / 3
                    }
                },

                // Callbacks
                eventDidMount: function(item) {
                    self._eventRender(item);
                },
                eventWillUnmount: function(item) {
                    self._eventDestroy(item);
                },
                eventDrop: function(updateInfo) {
                    self._updateItem(updateInfo);
                },
                eventResize: function(updateInfo) {
                    self._updateItem(updateInfo);
                },
                select: function(selectInfo) {
                    self._selectDate(selectInfo);
                },
                unselect: function(/* jsEvent, view */) {
                    $(self.element).qtip('destroy', true);
                },
                unselectCancel: '.s3-organizer-create',

                // L10n
                locale: opts.locale,
                timezone: 'local'
            });

            this.calendar = calendar;
            calendar.render();

            // Button icons
            let refreshIcon = $('<i>').addClass(opts.refreshIconClass),
                calendarIcon = $('<i>').addClass(opts.calendarIconClass);

            // Store reloadButton, use icon
            this.reloadButton = $('.fc-reload-button').empty().append(refreshIcon);

            // Move datepicker into header, use icon for calendar button
            let calendarButton = $('.fc-calendar-button').empty().append(calendarIcon);
            datePicker.datepicker('option', {showOn: 'focus', showButtonPanel: true, firstDay: opts.firstDay})
                      .insertBefore(calendarButton)
                      .on('change', function() {
                          let date = datePicker.datepicker('getDate');
                          if (date) {
                              calendar.gotoDate(date);
                          }
                      });

            // Hide the datepicker dialog (sometimes showing after init)
            datePicker.datepicker('widget').hide();

            // Add throbber
            let throbber = $('<div class="inline-throbber">').css({visibility: 'hidden'});
            $('.fc-reload-button', this.element).after(throbber);
            this.throbber = throbber;

            // Configure resources
            this.resources = [];
            if (resourceConfigs) {
                resourceConfigs.forEach(function(resourceConfig, index) {
                    this._addResource(resourceConfig, index);
                }, this);
            }

            this._bindEvents();
        },

        /**
         * Add a resource
         *
         * @param {object} resourceConfig - the resource config from options
         */
        _addResource: function(resourceConfig, index) {

            let resource = $.extend({}, resourceConfig, {_cache: new EventCache()});

            this.resources.push(resource);

            let timeout = resource.timeout;
            if (timeout === undefined) {
                timeout = this.options.timeout;
            }

            let self = this;
            this.calendar.addEventSource({
                id: '' + index, // must be string, falsy gets dropped
                allDayDefault: !resource.useTime,
                editable: !!resource.editable, // can be overridden per-record
                startEditable: !!resource.startEditable,
                durationEditable: !!resource.end && !!resource.durationEditable,
                events: function(fetchInfo, callback) {
                    self._fetchItems(resource, fetchInfo, callback);
                }
            });
        },

        /**
         * Actions after a calendar item has been rendered
         *
         * @param {object} item - the calendar item
         */
        _eventRender: function(item) {

            // Get the element
            let element = item.el;
            if (element === undefined) {
                return;
            }

            // Attach the item popup
            let self = this;
            $(element).qtip({
                content: {
                    title: function(jsEvent, api) {
                        return self._itemTitle(item, api);
                    },
                    text: function(jsEvent, api) {
                        return self._itemDisplay(item, api);
                    },
                    button: true
                },
                position: {
                    at: 'center right',
                    my: 'left center',
                    effect: false,
                    viewport: $(window),
                    adjust: {
                        // horizontal vertical
                        method: 'flip shift'
                    }
                },
                show: {
                    event: 'click',
                    solo: true
                },
                hide: {
                    event: 'click mouseleave',
                    delay: 800,
                    fixed: true
                },
                events: {
                    visible: function(/* jsEvent, api */) {
                        S3.addModals();
                    }
                }
            });
        },

        /**
         * Actions before a calendar item is removed from the DOM
         *
         * @param {object} item - the calendar item
         */
        _eventDestroy: function(item) {

            let element = item.el;
            if (element) {
                // Remove the popup
                $(element).qtip('destroy', true);
            }
        },

        /**
         * Render the popup title for a calendar item
         *
         * @param {object} item - the calendar item
         *
         * @returns {string} - the popup title
         */
        _itemTitle: function(item, api) {

            let locale = this.options.locale || 'en',
                eventInfo = item.event,
                dateFormat = eventInfo.allDay ? 'L' : 'L LT',
                timeFormat = 'LT';

            let dates = [moment(eventInfo.start).locale(locale).format(dateFormat)];

            if (eventInfo.end) {
                let end = moment(eventInfo.end).locale(locale).endOf('minute');
                dates.push(end.format(timeFormat));
            }

            return dates.join(' - ');
        },

        /**
         * Render the popup contents for a calendar item
         *
         * @param {object} item - the calendar item
         * @param {object} api - the qtip-API of the popup
         *
         * @returns {jQuery} - a DOM node with the contents
         */
        _itemDisplay: function(item, api) {

            let eventInfo = item.event,
                contents = $('<div class="s3-organizer-popup">'),
                opts = this.options,
                resource = opts.resources[eventInfo.source.id];

            // Item Title
            $('<h6>').html(eventInfo.popupTitle).appendTo(contents);

            // Item Description
            let columns = resource.columns,
                description = eventInfo.extendedProps.description;
            if (columns && description) {
                columns.forEach(function(column) {
                    let colName = column[0],
                        label = column[1];
                    if (description[colName] !== undefined) {
                        if (label) {
                            $('<label>').text(label).appendTo(contents);
                        }
                        $('<p>').html(description[colName]).appendTo(contents);
                    }
                });
            }

            // Edit/Delete Buttons
            let widgetID = $(this.element).attr('id'),
                ns = this.eventNamespace,
                self = this,
                buttons = [],
                btn,
                baseURL = resource.baseURL;

            if (baseURL) {
                // Edit button
                if (resource.editable && eventInfo.editable !== false) {
                    let link = document.createElement('a');
                    link.href = baseURL;
                    link.pathname += '/' + eventInfo.id + '/update.popup';
                    if (link.search) {
                        link.search += '&refresh=' + widgetID;
                    } else {
                        link.search = '?refresh=' + widgetID;
                    }
                    btn = $('<a class="action-btn s3_modal">').text(opts.labelEdit)
                                                              .attr('href', link.href);
                    btn.on('click' + ns, function() {
                        api.hide();
                    });
                    buttons.push(btn);
                }
                // Delete button
                if (resource.deletable && eventInfo.extendedProps.deletable !== false) {
                    btn = $('<a class="action-btn delete-btn-ajax">').text(opts.labelDelete);
                    btn.on('click' + ns, function() {
                        if (confirm(opts.deleteConfirmation)) {
                            api.hide();
                            self._deleteItem(item, function() {
                                api.destroy();
                            });
                        }
                        return false;
                    });
                    buttons.push(btn);
                }
            }
            if (buttons.length) {
                $('<div>').append(buttons).appendTo(contents);
            }

            return contents;
        },

        /**
         * Actions when a date interval has been selected
         *
         * @param {Object} selectInfo - the selectInfo containing start, end and jsEvent
         */
        _selectDate: function(selectInfo) {

            let self = this;

            $(this.element).qtip({
                content: {
                    'text': function(jsEvent, api) {
                        let start = moment(selectInfo.start),
                            end = moment(selectInfo.end);
                        return self._selectResource(start, end, jsEvent, api);
                    }
                },
                position: {
                    target: 'mouse',
                    at: 'center right',
                    my: 'left center',
                    effect: false,
                    viewport: $(window),
                    adjust: {
                        mouse: false,
                        method: 'flip shift'
                    }
                },
                show: {
                    event: 'click',
                    solo: true
                },
                hide: {
                    event: 'mouseleave',
                    delay: 800,
                    fixed: true
                },
                events: {
                    'visible': function(/* jsEvent, api */) {
                        S3.addModals();
                    }
                }
            });

            $(this.element).qtip('show', selectInfo.jsEvent);
        },

        /**
         * Render the contents of the resource-selector (create-popup)
         *
         * @param {moment} start - the start of the selected interval
         * @param {moment} end - the end of the selected interval
         * @param {event} jsEvent - the event that opened the popup
         * @param {object} api - the qtip-API for the popup
         */
        _selectResource: function(start, end, jsEvent, api) {

            // Add class to attach styles and cancel auto-unselect
            api.set('style.classes', 's3-organizer-create');

            let opts = this.options,
                resources = opts.resources,
                ns = this.eventNamespace,
                widgetID = $(this.element).attr('id'),
                contents = $('<div>');

            resources.forEach(function(resource) {

                // Make sure resource is insertable
                if (!resource.insertable) {
                    return;
                }
                let createButton = $('<a class="action-btn s3_modal">'),
                    label = resource.labelCreate,
                    url = resource.baseURL;

                if (url && label) {

                    let link = createButton.get(0),
                        query = [];

                    // Set path to create-dialog
                    link.href = url;
                    link.pathname += '/create.popup';

                    // Add refresh-target
                    if (widgetID) {
                        query.push('refresh=' + encodeURIComponent(widgetID));
                    }

                    // Add selected date range
                    let dates = start.toISOString() + '--' + moment(end).subtract(1, 'seconds').toISOString();
                    query.push('organizer=' + encodeURIComponent(dates));

                    // Update query part of link URL
                    if (link.search) {
                        link.search += '&' + query.join('&');
                    } else {
                        link.search = '?' + query.join('&');
                    }

                    // Complete the button and append it to popup
                    createButton.text(label)
                                .appendTo(contents)
                                .on('click' + ns, function() {
                                    api.hide();
                                });
                }
            });

            return contents;
        },

        /**
         * Fetch items from server (async)
         *
         * @param {object} resource - the resource configuration
         * @param {Date} start - start date (inclusive) of the interval
         * @param {Date} end - end date (exclusive) of the interval
         * @param {function} callback - the callback to invoke when the
         *                              data are available, function(items)
         */
        _fetchItems: function(resource, fetchInfo, callback) {

            let start = fetchInfo.start,
                end = fetchInfo.end;

            // Try to lookup from cache
            let items = resource._cache.retrieve(start, end);
            if (items) {
                callback(items);
                return;
            }

            let opts = this.options;

            // Show throbber
            this._showThrobber();

            // Get current filters
            let filterForm;
            if (resource.filterForm) {
                filterForm = $('#' + resource.filterForm);
            } else if (opts.filterForm) {
                filterForm = $('#' + opts.filterForm);
            }
            let currentFilters = S3.search.getCurrentFilters(filterForm);

            // Remove filters for start/end
            let filters = currentFilters.filter(function(query) {
                let selector = query[0].split('__')[0];
                return selector !== resource.start && selector !== resource.end;
            });

            // Update ajax URL
            let ajaxURL = resource.ajaxURL;
            if (!ajaxURL) {
                return;
            } else {
                ajaxURL = S3.search.filterURL(ajaxURL, filters);
            }

            // Add interval
            let interval = encodeURIComponent(start.toISOString() + '--' + end.toISOString());
            if (ajaxURL.indexOf('?') != -1) {
                ajaxURL += '&$interval=' + interval;
            } else {
                ajaxURL += '?$interval=' + interval;
            }

            // SearchS3 or AjaxS3?
            let timeout = resource.timeout,
                ajaxMethod = $.ajaxS3;
            if (timeout === undefined) {
                timeout = opts.timeout;
            }
            if ($.searchS3 !== undefined) {
                ajaxMethod = $.searchS3;
            }

            let openRequest = resource.openRequest;
            if (openRequest) {
                // Abort previously open request
                openRequest.onreadystatechange = null;
                openRequest.abort();
            }

            // Request updates for resource from server
            let self = this;
            resource.openRequest = ajaxMethod({
                'timeout': timeout,
                'url': ajaxURL,
                'dataType': 'json',
                'type': 'GET',
                'success': function(data) {

                    data = self._decodeServerData(resource, data);

                    self._hideThrobber();
                    resource._cache.store(start, end, data);
                    callback(data);
                },
                'error': function(jqXHR, textStatus, errorThrown) {

                    self._hideThrobber();
                    let msg;
                    if (errorThrown == 'UNAUTHORIZED') {
                        msg = i18n.gis_requires_login;
                    } else {
                        msg = jqXHR.responseText;
                    }
                    console.log(msg);
                }
            });
        },

        /**
         * Decode server data into fullCalendar events
         *
         * @param {object} resource - the resource from which items are loaded
         * @param {object} data - the data returned from the server
         *
         * @returns {Array} - Array of fullCalendar event objects
         */
        _decodeServerData: function(resource, data) {

            let columns = data.c,
                records = data.r,
                items = [],
                translateCols = 0,
                colors = resource.colors;

            if (columns && columns.constructor === Array) {
                translateCols = columns.length;
            }

            records.forEach(function(record) {

                let description = {},
                    values = record.d;

                if (translateCols && values && values.constructor === Array) {
                    let len = values.length;
                    if (len <= translateCols) {
                        for (let i = 0; i < len; i++) {
                            description[columns[i]] = values[i];
                        }
                    }
                }

                let end = record.e;
                if (end) {
                    // End date in item is exclusive
                    if (resource.useTime) {
                        // Item end date is record end date plus one second
                        end = moment(end).add(1, 'seconds').toISOString();
                    } else {
                        // Item end date is start of next day after record end
                        end = moment(end).add(1, 'days').startOf('day').toISOString();
                    }
                }

                let title = record.t,
                    item = {
                    'id': record.id,
                    title: $('<div>').html(title).text(),
                    start: record.s,
                    end: end,
                    extendedProps: {
                        popupTitle: title,
                        description: description,
                    }
                };

                // Permission overrides (skip if true to let resource-default apply)
                if (!record.pe) {
                    item.editable = false;
                }
                if (!record.pd) {
                    item.extendedProps.deletable = false;
                }

                // Item color
                if (colors && record.c) {
                    let itemColor = colors[record.c];
                    if (itemColor !== undefined) {
                        item.color = itemColor;
                    }
                }

                items.push(item);
            });

            return items;
        },

        /**
         * Update start/end of a calendar item
         *
         * @param {object} updateInfo - update info with event data and revert function
         */
        _updateItem: function(updateInfo) {

            let eventObj = updateInfo.event,
                revertFunc = updateInfo.revert,
                resource = this.resources[eventObj.source.id];

            let data = {
                id: eventObj.id,
            };

            if (resource.useTime) {
                data.s = eventObj.start.toISOString();
            } else {
                // Remove the timezone offset, and convert to bare ISO date without time
                let offset = eventObj.start.getTimezoneOffset();
                data.s = moment(eventObj.start).subtract(offset, 'minutes').toISOString().slice(0, 10);
            }

            // Add end date?
            if (resource.end) {
                // End date in item is exclusive
                if (resource.useTime) {
                    // Record end is one second before item end
                    data.e = moment(eventObj.end).subtract(1, 'seconds').toISOString();
                } else {
                    // Record end is end of previous day before item end
                    let offset = eventObj.start.getTimezoneOffset(),
                        end = moment(eventObj.end).subtract(1, 'days').endOf('day');
                    // Remove the timezone offset, and convert to bare ISO date without time
                    data.e = end.subtract(offset, "minutes").toISOString().slice(0, 10);
                }
            }

            // Update on server, then local
            let self = this;
            this._sendItems(resource, {u: [data]}, function() {
                if (resource.reloadOnUpdate) {
                    self.reload();
                } else {
                    resource._cache.updateItem(eventObj.id, {
                        start: eventObj.start,
                        end: eventObj.end
                    });
                }
            }, revertFunc);
        },

        /**
         * Delete a calendar item
         *
         * @param {object} item - the calendar item
         * @param {function} callback - the callback to invoke upon success
         */
        _deleteItem: function(item, callback) {

            let eventObj = item.event,
                resource = this.resources[eventObj.source.id],
                data = {'id': eventObj.id},
                self = this;

            this._sendItems(resource, {d: [data]}, function() {
                // Remove the event from the calendar
                eventObj.remove();

                // Remove the item from the cache
                resource._cache.deleteItem(eventObj.id);

                // Invoke the callback
                if (typeof callback === 'function') {
                    callback();
                }
            });
        },

        /**
         * Send item updates to the server
         *
         * @param {object} resource - the resource to send updates to
         * @param {object} data - the data to send
         * @param {function} callback - the callback to invoke upon success
         * @param {function} revertFunc - the callback to invoke upon failure
         */
        _sendItems: function(resource, data, callback, revertFunc) {

            let formKey = $('input[name="_formkey"]', this.element).val(),
                jsonData = JSON.stringify($.extend({k: formKey}, data)),
                self = this;

            this._showThrobber();

            $.ajaxS3({
                type: 'POST',
                url: resource.ajaxURL,
                data: jsonData,
                dataType: 'json',
                retryLimit: 0,
                contentType: 'application/json; charset=utf-8',
                success: function() {
                    if (typeof callback === 'function') {
                        callback();
                    }
                    self._hideThrobber();
                },
                error: function() {
                    if (typeof revertFunc === 'function') {
                        revertFunc();
                    }
                    self._hideThrobber();
                }
            });
        },

        /**
         * Clear the cache and re-fetch data (e.g. after filter change)
         */
        reload: function() {

            this.resources.forEach(function(resource) {
                resource._cache.clear();
            });
            this.calendar.refetchEvents();
        },

        /**
         * Show the throbber
         */
        _showThrobber: function() {

            this.throbber.css({visibility: 'visible'});
            this.reloadButton.prop('disabled', true);
        },

        /**
         * Hide the throbber
         */
        _hideThrobber: function() {

            this.throbber.css({visibility: 'hidden'});
            this.reloadButton.prop('disabled', false);
        },

        /**
         * Bind events to generated elements (after refresh)
         */
        _bindEvents: function() {
            return true;
        },

        /**
         * Unbind events (before refresh)
         */
        _unbindEvents: function() {
            return true;
        }
    });
})(jQuery);
