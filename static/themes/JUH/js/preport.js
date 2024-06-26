/**
 * MRCMS Presence Report (jQuery UI Widget)
 *
 * @copyright 2024 (c) AHSS
 * @license MIT
 */
(function($, undefined) {

    "use strict";

    var presenceReportID = 0;

    /**
     * presenceReport, instantiated on form
     */
    $.widget('dvr.presenceReport', {

        /**
         * Default options
         *
         * @prop {string} ajaxURL - the URL to send Ajax requests to
         */
        options: {

            ajaxURL: '',
        },

        /**
         * Create the widget
         */
        _create: function() {

            this.id = presenceReportID;
            presenceReportID += 1;

            // Namespace for events
            this.eventNamespace = '.presenceReport';
        },

        /**
         * Initializes the widget
         */
        _init: function() {

            this.tableContainer = $('#' + this.options.tableContainer);
            if (this.tableContainer) {
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

            this._unbindEvents();

            // TODO read table data from hidden input in el
            // TODO render table

            this._bindEvents();
        },

        _update: function() {

            // Read organisation_id, date, and formKey from form

            // Run Ajax-POST to /dvr/person/last_seen.json

            // use the result to render the table, unless results=0

        },

        // TODO docstring
        _renderTable: function(data) {

            // data = {labels: [array], data: [[col, col]]}

            const labels = data.labels,
                  records = data.records,
                  container = this.tableContainer.empty();

            // TODO handle zero records
            if (!labels || !records) {
                return;
            }

            const table = $('<table>'),
                  thead = $('<thead>').appendTo(table),
                  tbody = $('<tbody>').appendTo(table);

            const labelsRow = $('<tr>').appendTo(thead);
            labels.forEach(function(label) {
                $('<th>').text(label).appendTo(labelsRow);
            });

            var recordRow;
            records.forEach(function(record) {
                recordRow = $('<tr>').appendTo(tbody);
                record.forEach(function(value) {
                    $('<td>').text(value).appendTo(recordRow);
                });
            });

            table.appendTo(container);
        },

        /**
         * Bind events to generated elements (after refresh)
         */
        _bindEvents: function() {

            let $el = $(this.element),
                ns = this.eventNamespace,
                self = this;

            // TODO call updateAjax when any input changes

            // TODO download XLSX with form data when clicking on export button

            return this;
        },

        /**
         * Unbind events (before refresh)
         */
        _unbindEvents: function() {

            let $el = $(this.element),
                ns = this.eventNamespace;

            return this;
        }
    });
})(jQuery);
