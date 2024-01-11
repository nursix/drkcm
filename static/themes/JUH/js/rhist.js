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
     * registrationHistory, instantiate on trigger button
     */
    $.widget('cr.registrationHistory', {

        /**
         * Default options
         *
         * @prop {string} ajaxURL - the URL to send Ajax requests to
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
            labelClose: 'Close'
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
         * Update the widget options
         */
        _init: function() {

            let opts = this.options;

            this.container = $('#' + opts.container);

            this.refresh();
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

            var opts = this.options;

            this._unbindEvents();

            this._bindEvents();
        },

        /**
         * TODO docstring
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
                    throbber.remove();
                    this._showTrigger();
                }
            });
        },

        /**
         * TODO docstring
         */
        _hideTrigger: function() {
            $(this.element).hide();
        },

        /**
         * TODO docstring
         */
        _showTrigger: function() {
            $(this.element).show();
        },

        /**
         * TODO docstring
         */
        _renderWidget: function(data) {

            let opts = this.options,
                widget = $('<div class="rhist">'),
                title = $('<h6>').text(opts.labelTitle).appendTo(widget);

            if (data.length) {
                let history = $('<table>').appendTo(widget),
                    row = $('<tr class="rhist-headers">').appendTo(history);

                // Table Header
                $('<th>').appendTo(row);
                $('<th>').text(opts.labelShelter).appendTo(row);
                $('<th>').text(opts.labelPlanned).appendTo(row);
                $('<th>').text(opts.labelArrival).appendTo(row);
                $('<th>').appendTo(row);
                $('<th>').text(opts.labelDeparture).appendTo(row);

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
            } else {
                // Empty-message
                $('<p class="empty">').text(opts.labelEmpty).appendTo(widget);
            }

            // Close-button
            let closeButton = $('<button type="button" class="action-btn">').text(opts.labelClose).appendTo(widget),
                ns = this.eventNamespace,
                self = this;
            closeButton.one('click' + ns, function() {
                self._removeWidget();
            });

            this.container.append(widget);
            this._hideTrigger();
        },

        /**
         * TODO docstring
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

            var $el = $(this.element),
                ns = this.eventNamespace,
                self = this;

            $el.on('click' + ns, function() {
                self._showHistory();
            });

            return true;
        },

        /**
         * Unbind events (before refresh)
         */
        _unbindEvents: function() {

            var $el = $(this.element),
                ns = this.eventNamespace;

            $el.off(ns);

            return true;
        }
    });

    // TODO remove?
    $(function() {
        // Document-ready handler
    });

})(jQuery);
