/**
 * jQuery UI Widget for QR code input
 *
 * @copyright 2021 (c) Sahana Software Foundation
 * @license MIT
 */

/* jshint esversion: 6 */

(function($, undefined) {

    "use strict";
    var qrInputID = 0;

    /**
     * qrInput
     */
    $.widget('s3.qrInput', {

        /**
         * Default options
         *
         * @todo document options
         */
        options: {

            workerPath: null,

            inputPattern: null, // e.g. '(?<code>\\d+)##.+##.+##.+'
            inputIndex: null, // e.g. 'code'
        },

        /**
         * Create the widget
         */
        _create: function() {

            this.id = qrInputID;
            qrInputID += 1;

            this.eventNamespace = '.qrInput';
        },

        /**
         * Update the widget options
         */
        _init: function() {

            this.container = $(this.element).closest('.qrinput');
            this.scanButton = $('.qrscan-btn', this.container);

            // Set up qr-scanner worker
            let workerPath = this.options.workerPath;
            if (workerPath) {
                QrScanner.WORKER_PATH = workerPath;
            }

            this.refresh();
        },

        /**
         * Remove generated elements & reset other changes
         */
        _destroy: function() {

            let scanner = this.scanner,
                videoInput = this.videoInput;

            if (scanner) {
                scanner.destroy();
                this.scanner = null;
            }

            if (videoInput) {
                videoInput.remove();
                this.videoInput = null;
            }

            $.Widget.prototype.destroy.call(this);
        },

        /**
         * Redraw contents
         */
        refresh: function() {

            let $el = $(this.element),
                self = this;

            this._unbindEvents();

            $('.error_wrapper', this.container).appendTo(this.container);

            if (self.scanButton.length) {

                let postprocess = this.options.postprocess;

                QrScanner.hasCamera().then(function(hasCamera) {

                    let scanButton = self.scanButton;

                    if (!hasCamera) {
                        scanButton.prop('disabled', true);
                        return;
                    } else {
                        scanButton.prop('disabled', false);
                    }

                    let scanner,
                        scanForm = $('<div class="qrinput-scan">'),
                        // TODO make success-message configurable
                        success = $('<div class="qrinput-success">').html('<i class="fa fa-check">').hide().appendTo(scanForm),
                        videoInput = $('<video>').appendTo(scanForm);

                    // TODO make width/height configurable or auto-adapt to screen size
                    videoInput.css({width: '300', height: '300'});

                    let dialog = scanForm.dialog({
                        title: 'Scan QR Code',
                        autoOpen: false,
                        modal: true,
                        'classes': {'ui-dialog': 'qrinput-dialog'},
                        close: function() {
                            if (scanner) {
                                scanner.stop();
                                scanner.destroy();
                                scanner = null;
                            }
                        }
                    });

                    scanButton.on('click', function() {
                        videoInput.show();
                        success.hide();
                        dialog.dialog('open');
                        scanner = new QrScanner(videoInput.get(0),
                            function(result) {
                                scanner.stop();
                                videoInput.hide();
                                success.show();
                                window.navigator.vibrate(100);
                                try {
                                    result = self._parse(result);
                                } catch(e) {
                                    // pass
                                }
                                $el.val(result).trigger('change' + self.eventNamespace);
                                setTimeout(function() {
                                    dialog.dialog('close');
                                }, 400);
                            },
                            function( /* error */ ) {
                                // TODO handle error
                            });

                        scanner.start();
                        // TODO auto-close after timeout?
                    });
                });
            }

            this._bindEvents();
        },

        /**
         * Clear input
         */
        _clearInput: function() {

            $(this.element).val('').trigger('change' + this.eventNamespace);
        },

        /**
         * Parse input
         *
         * @param {string} result - the result from the QR scanning
         */
        _parse: function(result) {

            let opts = this.options,
                pattern = opts.inputPattern;

            if (!result || !pattern) {
                return result;
            }

            let expr = new RegExp(pattern, 'g'),
                parsed = expr.exec(result);
            if (parsed) {
                let index = opts.inputIndex;
                if (typeof index == 'string') {
                    parsed = parsed.groups[index];
                } else {
                    parsed = parsed[index];
                }
            } else {
                // Invalid input - do not expose the contents
                parsed = '';
            }

            return parsed;
        },

        /**
         * Bind events to generated elements (after refresh)
         */
        _bindEvents: function() {

            let $el = $(this.element),
                ns = this.eventNamespace,
                self = this;

            $('.clear-btn', $el.closest('.qrinput')).on('click' + ns, function() {
                self._clearInput();
            });

            return true;
        },

        /**
         * Unbind events (before refresh)
         */
        _unbindEvents: function() {

            let $el = $(this.element),
                ns = this.eventNamespace;

            $('.clear-btn', $el.closest('.qrinput')).off(ns);

            return true;
        }
    });
})(jQuery);
