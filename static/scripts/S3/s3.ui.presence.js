/**
 * jQuery UI Widget for RegisterPresence
 *
 * @copyright 2021-2023 (c) Sahana Software Foundation
 * @license MIT
 */
(function($, undefined) {

    "use strict";
    var registerPresenceID = 0;

    /**
     * registerPresence
     */
    $.widget('s3.registerPresence', {

        /**
         * Default options
         *
         * @prop {string} tablename - the tablename used for the form
         * @prop {string} ajaxURL - the URL to send Ajax requests to
         * @prop {string} noPictureAvailable - i18n message when no profile
         *                                     picture is available
         * @prop {string} statusIn - status message for IN
         * @prop {string} statusOut - status message for OUT
         * @prop {string} statusNone - status message for 'unknown'
         * @prop {string} statusLabel - label for status message
         */
        options: {

            tableName: 'site_presence',

            ajaxURL: '',

            noPictureAvailable: 'No picture available',
            statusIn: 'present',
            statusOut: 'not present',
            statusNone: '-',
            statusLabel: 'Status',

            sendOriginalQRInput: true,
        },

        /**
         * Create the widget
         */
        _create: function() {

            var el = $(this.element);

            this.id = registerPresenceID;
            registerPresenceID += 1;

            this.eventNamespace = '.registerPresence';
        },

        /**
         * Update the widget options
         */
        _init: function() {

            // ID prefix for form rows
            this.idPrefix = '#' + this.options.tableName;

            this.personData = $(this.element).find('input[type=hidden][name=data]');

            this.labelInput = $(this.idPrefix + '_label');
            this.hiddenInput = this.labelInput.siblings('.qrinput-hidden');

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

            this._unbindEvents();

            var data = this.personData.val();
            if (data && data != 'null') {
                this._showPersonData(JSON.parse(data));
            } else {
                this._clearForm();
            }

            this._bindEvents();
        },

        _getLabel: function() {

            let labelInput = this.labelInput,
                hiddenInput = this.hiddenInput,
                opts = this.options,
                label = labelInput.val().trim();

            labelInput.val(label);
            if (opts.sendOriginalQRInput) {
                let qrdata = hiddenInput.val();
                if (qrdata) {
                    label = qrdata;
                }
            }
            return label;

        },

        /**
         * Ajax-method to retrieve the person data for the label
         */
        _checkID: function() {

            var prefix = this.idPrefix,
                labelInput = this.labelInput,
                label = this._getLabel();

            // Don't do anything if label is empty
            if (!label) {
                return;
            }

            // Clear form, but keep input
            this._clearForm(false, true);

            // Show throbber
            var personInfo = $(prefix + '_person__row .controls').empty(),
                throbber = $('<div class="inline-throbber">').insertAfter(personInfo);

            // Disable buttons
            $('#submit_record__row').find('.button').prop('disabled', true);

            // Get data from server
            var self = this,
                ajaxURL = this.options.ajaxURL,
                input = {m: 'STATUS', l: label, k: this._formkey()};
            $.ajaxS3({
                'url': ajaxURL,
                'type': 'POST',
                'dataType': 'json',
                'contentType': 'application/json; charset=utf-8',
                'data': JSON.stringify(input),
                'success': function(data) {

                    // Remove the throbber
                    throbber.remove();

                    // Re-enable buttons
                    $('#submit_record__row').find('.button').prop('disabled', false);

                    // Process the data
                    if (data.e) {
                        // Show error message on label input
                        self._showInputError(data.e);
                    } else {
                        // Show advice on label input
                        if (data.q) {
                            self._showInputError(data.q);
                        }

                        // Show person data
                        self._showPersonData(data);

                        // Show alert
                        var alert = data.m;
                        if (alert) {
                            S3.showAlert(alert[0], alert[1]);
                        }
                    }

                },
                'error': function () {

                    // Remove the throbber
                    throbber.remove();

                    // Re-enable buttons
                    $('#submit_record__row').find('.button').prop('disabled', false);

                    // Clear the form, but keep alert
                    self._clearForm(true, false);
                }
            });
        },

        /**
         * Ajax-method to check-in or check-out the person
         *
         * @param {string} method: the method (check-in|check-out)
         */
        _register: function(method) {

            var prefix = this.idPrefix,
                labelInput = this.labelInput,
                label = this._getLabel();

            // Don't do anything if label is empty
            if (!label) {
                return;
            }

            // Clear previous alerts
            this._clearAlert();

            // Show throbber
            var personInfo = $(prefix + '_person__row .controls'),
                throbber = $('<div class="inline-throbber">').insertAfter(personInfo);

            // Disable buttons
            $('#submit_record__row').find('.button').prop('disabled', true);

            // Do we already show the person infos?
            var hasPersonInfo = !!$.trim(personInfo.html()).length;

            // Execute the method
            var self = this,
                ajaxURL = this.options.ajaxURL,
                input = {m: method, l: label, k: this._formkey()};
            $.ajaxS3({
                'url': ajaxURL,
                'type': 'POST',
                'dataType': 'json',
                'contentType': 'application/json; charset=utf-8',
                'data': JSON.stringify(input),
                'success': function(data) {

                    // Remove the throbber
                    throbber.remove();

                    // Re-enable buttons
                    $('#submit_record__row').find('.button').prop('disabled', false);

                    // Process the data
                    if (data.e) {
                        // Show error message on label input
                        self._showInputError(data.e);
                    } else {

                        // Show person data if we have announcements
                        // and have not shown them before, otherwise
                        // clear the form
                        if (data.a && !hasPersonInfo) {
                            self._showPersonData(data);
                        } else {
                            self._clearForm();
                        }

                        // Show alert
                        var alert = data.m;
                        if (alert) {
                            S3.showAlert(alert[0], alert[1]);
                        }
                    }
                },
                'error': function () {

                    // Remove the throbber
                    throbber.remove();

                    // Re-enable buttons
                    $('#submit_record__row').find('.button').prop('disabled', false);

                    // Clear the form, but keep alert
                    self._clearForm(true, false);
                }
            });
        },

        /**
         * Render the person data returned from Ajax-lookup
         *
         * @param {object} - the person data from Ajax-lookup
         */
        _showPersonData: function(data) {

            var prefix = this.idPrefix,
                person = $(prefix + '_person__row .controls'),
                status = $(prefix + '_status__row .controls'),
                info = $(prefix + '_info__row .controls');

            // Show the person details
            person.html(data.d).removeClass('hide').show();

            // Show profile picture
            this._showProfilePicture(data.p);

            // Show status message
            var statusMsg = this._statusMsg(data.s);
            status.append(statusMsg).removeClass('hide').show();

            // Toggle actions according to status
            if (!data.i) {
                // Check-in denied
                this._toggleAction('check-in-btn', 'deny');
            } else if (data.s == 1) {
                // Already checked-in
                this._toggleAction('check-in-btn', 'off');
            }

            if (!data.o) {
                // Check-out denied
                this._toggleAction('check-out-btn', 'deny');
            } else if (data.s == 2) {
                // Already checked-out
                this._toggleAction('check-out-btn', 'off');
            }

            // Show info
            if (data.a) {
                info.html(data.a).removeClass('hide').show();
            }

        },

        /**
         * Shows an error message on the label input
         *
         * @param {string} error: the error message
         */
        _showInputError: function(error) {

            let labelInput = this.labelInput,
                msg = $('<div class="error_wrapper"><div id="label__error" class="error" style="display: block;">' + error + '</div></div>').hide(),
                outer = labelInput.closest('.controls');
            if (outer.length) {
                msg.appendTo(outer).slideDown();
            } else {
                msg.insertAfter(labelInput).slideDown();
            }
        },

        /**
         * Generate HTML for the current check-in status
         *
         * @param {integer} status (IN|OUT|null)
         *
         * @returns {jQuery} - a detached DOM node with the status message
         */
        _statusMsg: function(status) {

            var opts = this.options,
                container = $('<div class="check-in-status">'),
                label = $('<span class="status-label">' + opts.statusLabel + ': </span>').appendTo(container),
                message = $('<span class="status-message">').appendTo(container);

            switch(status) {
                case 'IN':
                    message.html(opts.statusIn);
                    break;
                case 'OUT':
                    message.html(opts.statusOut);
                    break;
                default:
                    message.html(opts.statusNone);
                    break;
            }
            return container;
        },

        /**
         * Clear the form
         *
         * @param {boolean} keepAlerts - don't clear any page alerts
         * @param {boolean} keepInput - don't clear the label input
         */
        _clearForm: function(keepAlerts, keepInput) {

            var prefix = this.idPrefix;

            // Remove all throbbers
            $('.inline-throbber').remove();

            // Clear alerts
            if (!keepAlerts) {
                this._clearAlert();
            }

            // Clear input(s)
            if (!keepInput) {
                this.hiddenInput.val('');
                this.labelInput.val('').trigger('focus');
            }

            // Clear details
            $(prefix + '_person__row .controls').hide().empty();
            $(prefix + '_status__row .controls').hide().empty();
            $(prefix + '_info__row .controls').hide().empty();

            this._hideProfilePicture();

            this._toggleAction('check-in-btn', 'on');
            this._toggleAction('check-out-btn', 'on');

        },

        /**
         * Helper function to hide any alert messages that are currently shown
         */
        _clearAlert: function() {

            $('.alert-error, .alert-warning, .alert-info, .alert-success').fadeOut('fast').remove();
            $('.error_wrapper').fadeOut('fast').remove();
        },

        /**
         * Show the profile picture (or a message if no picture available)
         *
         * @param {string} url - the picture URL
         */
        _showProfilePicture: function(url) {

            var container = $('#profile-picture').empty(),
                panel = $('<div class="panel">').hide().appendTo(container),
                image;

            if (url) {
                image = $('<img>').attr('src', url);
            } else {
                image = $('<p>').text(this.options.noPictureAvailable);
            }
            panel.append(image).show();
        },

        /**
         * Remove the profile picture
         */
        _hideProfilePicture: function() {

            $('#profile-picture').empty();

        },

        /**
         * Toggle action button status
         *
         * @param {string} buttonClass - the button CSS class
         * @param {string} status - the new status: on|off|deny
         */
        _toggleAction: function(buttonClass, status) {

            var button = $('button.' + buttonClass),
                label = button.text(),
                disabled;

            if (!button.length) {
                return;
            }

            button.siblings('.disabled-' + buttonClass).remove();
            switch(status) {
                case 'off':
                    button.prop('disabled', true).show();
                    break;
                case 'deny':
                    disabled = $('<button class="small alert button disabled-' + buttonClass + '" disabled="disabled"><i class="fa fa-ban"></i>' + label + '</button>');
                    button.prop('disabled', true).hide().after(disabled);
                    break;
                default:
                    button.prop('disabled', false).show();
                    break;
            }
        },

        /**
         * Gets the current form key
         */
        _formkey: function() {
            var field = $('input[name="formkey"]', $(this.element)),
                key = null;

            if (field.length) {
                key = field.first().val();
            }

            return key;
        },

        /**
         * Bind events to generated elements (after refresh)
         */
        _bindEvents: function() {

            var self = this,
                form = $(this.element),
                ns = this.eventNamespace,
                prefix = this.idPrefix;

            // Click-Handler for Check-ID button
            form.find('.check-btn').on('click' + ns, function(e) {
                e.preventDefault();
                self._checkID();
            });
            // Click-Handler for Check-in button
            form.find('.check-in-btn').off(ns).on('click' + ns, function(e) {
                e.preventDefault();
                self._register('IN');
            });
            // Click-Handler for Check-out button
            form.find('.check-out-btn').off(ns).on('click' + ns, function(e) {
                e.preventDefault();
                self._register('OUT');
            });
            // Cancel-button to clear the form
            form.find('a.cancel-action, .clear-btn').on('click' + ns, function(e) {
                e.preventDefault();
                self._clearForm();
            });
            $('.qrscan-btn', form).on('click' + ns, function(e) {
                self._clearForm();
            });

            // Events for the label input
            var labelInput = this.labelInput;

            // Changing the label resets form
            labelInput.on('input' + ns, function(e) {
                self._clearForm(false, true);
            });

            // Catch Enter
            labelInput.on('keypress' + ns, function(e) {
                if (e.which == 13) {
                    e.preventDefault();
                    return false;
                }
            });
            // Key events for label field
            labelInput.on('keyup' + ns, function(e) {
                e.preventDefault();
                switch (e.which) {
                    case 27:
                        // Pressing ESC resets the form
                        self._clearForm();
                        break;
                    case 13:
                        // Pressing Enter runs ID check
                        self._checkID();
                        break;
                    default:
                        break;
                }
            });

            return true;
        },

        /**
         * Unbind events (before refresh)
         */
        _unbindEvents: function() {

            var form = $(this.element),
                ns = this.eventNamespace,
                prefix = this.idPrefix;

            $(prefix + '_label').off(ns);

            form.find('.check-btn').off(ns);
            form.find('.check-in-btn').off(ns);
            form.find('.check-out-btn').off(ns);
            form.find('a.cancel-action, .clear-btn').off(ns);

            return true;
        }
    });
})(jQuery);
