/**
 * MRCMS Registration History Viewer (jQuery UI Widget)
 *
 * @copyright 2024 (c) AHSS
 * @license MIT
 */
(function($, undefined) {

    "use strict";

    var registrationHistoryID = 0;

    /**
     * registrationHistory, instantiated on trigger button
     */
    $.widget('cr.registrationHistory', {

        /**
         * Default options
         *
         * @prop {string} ajaxURL - the URL to send Ajax requests to
         * @prop {string} container - the node ID of the container element
         * @prop {string} label* - localized labels for the viewer
         */
        options: {

            ajaxURL: '',
            container: 'map',

            labelTitle: 'Registration History',
            labelShelter: 'Shelter',
            labelPlanned: 'Planned since',
            labelArrival: 'Arrival',
            labelDeparture: 'Departure',
            labelEmpty: 'No data available',
            labelClose: 'Close',
            labelExport: 'Export Data'
        },

        /**
         * Create the widget
         */
        _create: function() {

            this.id = registrationHistoryID;
            registrationHistoryID += 1;

            // Namespace for events
            this.eventNamespace = '.registrationHistory';
        },

        /**
         * Initializes the widget
         */
        _init: function() {

            this.container = $('#' + this.options.container);
            if (this.container) {
                this.refresh();
            }
        },

        /**
         * Remove generated elements & reset other changes
         */
        _destroy: function() {

            $.Widget.prototype.destroy.call(this);
        },

        /**
         * Redraw contents
         */
        refresh: function() {

            this._unbindEvents()._bindEvents();
        },

        /**
         * Loads the registration details from the server and
         * displays them in the viewer widget
         */
        _showHistory: function() {

            let opts = this.options,
                ajaxURL = opts.ajaxURL,
                container = this.container.empty();

            if (!ajaxURL || !container.length) {
                return;
            }

            // Remove widget and hide trigger
            this._removeWidget(true);

            // Show a trobber, then load the data
            let throbber = $('<div class="inline-throbber">').appendTo(container),
                self = this;
            $.ajaxS3({
                'url': ajaxURL,
                'type': 'GET',
                'dataType': 'json',
                'contentType': 'application/json; charset=utf-8',
                'success': function(data) {
                    throbber.remove();
                    self._renderWidget(data);
                },
                'error': function () {
                    console.log('here');
                    throbber.remove();
                    self._showTrigger();
                }
            });
        },

        /**
         * Hides the trigger (button)
         */
        _hideTrigger: function() {

            $(this.element).hide();
        },

        /**
         * Shows the trigger (button)
         */
        _showTrigger: function() {

            $(this.element).show();
        },

        /**
         * Renders the viewer and appends it to the container
         *
         * @param {Array} data: array of objects with registration details
         */
        _renderWidget: function(data) {

            let opts = this.options,
                widget = $('<div class="rhist">'),
                exportLink;

            // Add the title
            let title = $('<h6>').text(opts.labelTitle).appendTo(widget);


            if (data.length) {
                let history = $('<table>').appendTo(widget);

                // Table Header
                let row = $('<tr class="rhist-headers">').appendTo(history),
                    labels = ['', opts.labelShelter, opts.labelPlanned, opts.labelArrival, '', opts.labelDeparture];
                labels.forEach(function(label) {
                     $('<th>').text(label).appendTo(row);
                });

                // Items
                data.forEach(function(item, idx) {
                    row = $('<tr>').appendTo(history);
                    if (item.c) {
                        row.addClass('rhist-current');
                    }
                    $('<td>').text(idx+1).appendTo(row);
                    $('<td class="rhist-shelter">').text(item.n).appendTo(row);
                    $('<td class="rhist-planned">').text(item.p).appendTo(row);

                    let checkInDate = item.i,
                        checkOutDate = item.o,
                        checkInMissing = false,
                        checkOutMissing = false;
                    if (!item.i && item.o) {
                        checkInDate = '?';
                        checkInMissing = true;
                    }
                    if (!item.c && !item.o) {
                        checkOutDate = '?';
                        checkOutMissing = true;
                    }

                    let checkIn = $('<td class="rhist-in">').text(checkInDate).appendTo(row);
                    if (checkInMissing) {
                        checkIn.attr('title', opts.labelMissing);
                    }
                    let spacer = $('<td class="rhist-to">').appendTo(row);
                    if (checkOutDate) {
                        spacer.html('<i class="fa fa-arrow-right">');
                    }
                    let checkOut = $('<td class="rhist-out">').text(checkOutDate).appendTo(row);
                    if (checkOutMissing) {
                        checkOut.attr('title', opts.labelMissing);
                    }
                });

                // Export Link
                if (opts.xlsxURL) {
                    exportLink = $('<a class="action-btn" href=' + opts.xlsxURL + '>' + opts.labelExport + '<i class="fa fa-file-excel-o"></i></a>');
                }

            } else {
                // Empty-message
                $('<p class="empty">').text(opts.labelEmpty).appendTo(widget);
            }

            // Close-button
            let actions = $('<div>').appendTo(widget),
                closeButton = $('<button type="button" class="action-btn">').text(opts.labelClose).appendTo(actions);

            let ns = this.eventNamespace,
                self = this;
            closeButton.one('click' + ns, function() {
                self._removeWidget();
            });
            if (!!exportLink) {
                exportLink.appendTo(actions);
            }

            this.container.append(widget);
            this._hideTrigger();
        },

        /**
         * Removes the viewer from the container
         *
         * @param {bool} hideTrigger: whether to also hide the trigger
         */
        _removeWidget: function(hideTrigger) {

            this.container.empty();
            if (hideTrigger) {
                this._hideTrigger();
            } else {
                this._showTrigger();
            }
        },

        /**
         * Bind events to generated elements (after refresh)
         */
        _bindEvents: function() {

            let $el = $(this.element),
                ns = this.eventNamespace,
                self = this;

            $el.on('click' + ns, function() {
                self._showHistory();
            });

            return this;
        },

        /**
         * Unbind events (before refresh)
         */
        _unbindEvents: function() {

            let $el = $(this.element),
                ns = this.eventNamespace;

            $el.off(ns);

            return this;
        }
    });
})(jQuery);
