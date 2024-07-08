/**
 * MRCMS Table Report (jQuery UI Widget)
 *
 * @copyright 2024 (c) AHSS
 * @license MIT
 */
(function($, undefined) {

    "use strict";

    var tableReportID = 0;

    /**
     * tableReport, instantiated on form
     */
    $.widget('mrcms.tableReport', {

        /**
         * Default options
         *
         * @prop {string} ajaxURL - the URL to send Ajax requests to
         * @prop {string} xlsxURL - the URL to download XLSX format from
         * @prop {string} tableContainer - the DOM ID of the table container
         * @prop {string} label* - localized labels
         */
        options: {
            ajaxURL: '',
            xlsxURL: '',
            tableContainer: 'table-report-data',

            labelNoData: 'No data available'
        },

        /**
         * Create the widget
         */
        _create: function() {

            this.id = tableReportID;
            tableReportID += 1;

            // Namespace for events
            this.eventNamespace = '.tableReport';
        },

        /**
         * Initializes the widget
         */
        _init: function() {

            this.tableContainer = $('#' + this.options.tableContainer);
            if (this.tableContainer.length) {
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

            this._bindEvents();
        },

        /**
         * Renders the report data as table
         *
         * @param {object} data: the data returned from the server, an object
         *                       {labels: [], records: [[]], results: 0}
         */
        _renderTable: function(data) {

            const results = data.results,
                  labels = data.labels,
                  records = data.records,
                  container = this.tableContainer.empty();

            if (!data.results || !labels || !records) {
                $('<p class="empty">').text(this.options.labelNoData).appendTo(container);
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
         * Reads the report parameters from the form
         *
         * @returns {object} - the report parameters as object
         *                     {organisation, shelter, start_date, end_date, _formkey},
         */
        _getParameters: function() {

            const $el = $(this.element),
                  organisationID = $('select[name="organisation_id"]', $el).val(),
                  shelterID = $('select[name="shelter_id"]', $el).val(),
                  formKey = $('input[name="formkey"]', $el).val(),
                  params = {
                    organisation: organisationID,
                    shelter: shelterID,
                    _formkey: formKey
                  };

            var dtName, dtInput, dt, date;
            [['start_date', false], ['end_date', true]].forEach(function(dateField) {
                dtName = dateField[0];
                dtInput = $('input[name="' + dtName + '"]', $el);
                dt = dtInput.length ? dtInput.calendarWidget('getJSDate', dateField[1]) : null;
                if (dt) {
                    params[dtName] = dt.toISOString();
                }
            });

            return params;
        },

        /**
         * Disables the report parameter form
         */
        _disableForm: function() {

            var $this;
            $('select,input,button', $(this.element)).each(function() {
                $this = $(this);
                if (!$this.prop('disabled')) {
                    $this.prop('disabled', true);
                    if (!$this.hasClass('state-enabled')) {
                        $this.addClass('state-enabled');
                    }
                }
            });
        },

        /**
         * Enables the report parameter form
         */
        _enableForm: function() {

            $('select.state-enabled,input.state-enabled,button.state-enabled', $(this.element)).each(function() {
                $(this).removeClass('state-enabled').prop('disabled', false);
            });
        },

        /**
         * Updates the report (table) in the GUI
         */
        _updateReport: function() {

            this._disableForm();

            const container = this.tableContainer.empty();

            // Get request parameters
            const url = this.options.ajaxURL,
                  params = this._getParameters();
            if (!params.organisation || !params.start_date || !params.end_date || !url) {
                this._enableForm();
                return;
            }

            // Run ajax request
            const throbber = $('<div class="inline-throbber">').appendTo(container),
                  self = this;
            $.ajaxS3({
                'url': this.options.ajaxURL,
                'type': 'POST',
                'dataType': 'json',
                'contentType': 'application/json; charset=utf-8',
                'data': JSON.stringify(params),
                'success': function(data) {
                    throbber.remove();
                    self._renderTable(data);
                    self._enableForm();
                },
                'error': function () {
                    throbber.remove();
                    self._enableForm();
                }
            });
        },

        /**
         * Downloads the report as XLSX spreadsheet
         */
        _downloadReport: async function() {

            this._disableForm();

            const url = this.options.xlsxURL;

            // Get request parameters
            const params = this._getParameters();
            if (!params.organisation || !params.start_date || !params.end_date || !url) {
                this._enableForm();
                return;
            }

            // Fetch the report with current parameters
            let response;
            try {
                response = await fetch(url, {
                    method: "POST",
                    body: JSON.stringify(params),
                });
            } catch(error) {
                alert('Download failed');
                this._enableForm();
                return;
            }

            if (undefined !== response && response.ok) {
                // Generate a link to the download
                const blob = await response.blob(),
                      url = window.URL.createObjectURL(blob),
                      a = document.createElement("a");
                a.href = url;

                // Extract the filename from the response
                const disposition = await response.headers.get('content-disposition'),
                      matches = /"([^"]*)"/.exec(disposition),
                      filename = (matches != null && matches[1] ? matches[1] : 'report.xlsx');
                a.download = filename;

                // Start the download
                a.click();

            } else {
                alert('Download failed');
            }

            this._enableForm();
        },

        /**
         * Bind events to generated elements (after refresh)
         */
        _bindEvents: function() {

            let $el = $(this.element),
                ns = this.eventNamespace,
                self = this;

            $('.update-report-btn', $el).on('click' + ns, function() {
                self._updateReport();
            });
            $('.download-report-btn', $el).on('click' + ns, function() {
                self._downloadReport();
            });
            $('input,select', $el).on('change' + ns, function() {
                self.tableContainer.empty();
            });

            return this;
        },

        /**
         * Unbind events (before refresh)
         */
        _unbindEvents: function() {

            let $el = $(this.element),
                ns = this.eventNamespace;

            $('.update-report-btn', $el).off(ns);
            $('.download-report-btn', $el).off(ns);

            return this;
        }
    });
})(jQuery);
