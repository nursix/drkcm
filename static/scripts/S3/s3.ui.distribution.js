/**
 * jQuery UI Widget for Distribution UI (SUPPLY)
 *
 * @copyright 2024 (c) Sahana Software Foundation
 * @license MIT
 */
(function($, undefined) {

    "use strict";

    var registerDistributionID = 0;

    /**
     * registerDistribution, instantiate on distribution registration form
     */
    $.widget('supply.registerDistribution', {

        /**
         * Default options
         */
        options: {

            // URL for Ajax requests (required)
            ajaxURL: '',

            // Table name to identify form rows
            tablename: 'supply_distribution',

            // Whether to show the beneficiary picture by default
            showPicture: true,

            // L10n action labels
            showPictureLabel: 'Show Picture',
            hidePictureLabel: 'Hide Picture',
            selectDistributionSetLabel: 'Select Distribution Set',
            noDistributionSetsLabel: 'No distribution item sets available',

            // L10n table and columns titles/hints
            distributeLabel: 'Distribution',
            returnLabel: 'Return',
            itemLabel: 'Item',
            quantityLabel: 'Quantity',
            packLabel: 'Pack',
            lossLabel: 'Loss',
            loanLabel: 'Loan',
        },

        /**
         * Creates the widget
         */
        _create: function() {

            this.id = registerDistributionID;
            registerDistributionID += 1;

            // Namespace for events
            this.eventNamespace = '.registerDistribution';
        },

        /**
         * Updates the widget options
         */
        _init: function() {

            const form = $(this.element),
                  widgetID = form.attr('id'),
                  prefix = '#' + this.options.tablename;

            // ID prefix for form rows
            this.idPrefix = prefix;

            // Control elements outside of the form
            this.orgHeader = $('#' + widgetID + '-org-header');
            this.orgSelect = $('#' + widgetID + '-org-select');
            this.distributionSetHeader = $('#' + widgetID + '-event-type-header');
            this.distributionSetSelect = $('#' + widgetID + '-event-type-select');
            this.pictureContainer = $('#' + widgetID + '-picture');

            // Form Rows
            this.personRow = $(prefix + '_person__row', form);
            this.flagInfoRow = $(prefix + '_flaginfo__row', form);
            this.detailsRow = $(prefix + '_details__row', form);

            // Containers
            this.personContainer = $('.controls', this.personRow);
            this.flagInfoContainer = $('.controls', this.flagInfoRow);
            this.detailsContainer = $('.controls', this.detailsRow);

            // Hidden input fields
            this.distributionSet = $('input[type="hidden"][name="distset"]', form);
            this.flagInfo = $('input[type=hidden][name=flags]', form);
            this.imageURL = $('input[type="hidden"][name="image"]', form);
            this.actionDetails = $('input[type="hidden"][name="actions"]', form);

            this.permissionInfo = $('input[type=hidden][name=permitted]', form);
            this.actionableInfo = $('input[type=hidden][name=actionable]', form);

            // Submit label
            this.submitLabel = $('.submit-btn', form).first().val();

            // Profile picture URL
            this.profilePicture = null;

            this.refresh();
        },

        /**
         * Removes generated elements & resets other changes
         */
        _destroy: function() {

            $.Widget.prototype.destroy.call(this);
        },

        /**
         * Redraws contents
         */
        refresh: function() {

            const opts = this.options;

            this._unbindEvents();

            // AjaxURL is required
            if (!opts.ajaxURL) {
                throw new Error('registerDistribution: no ajaxURL provided');
            }

            this._clearForm();
            this._updateDistributionSet();

            this._bindEvents();
        },

        /**
         * Ajax method to identify the person from the label
         */
        _checkID: function() {

            const orgHeader = this.orgHeader,
                  form = $(this.element),
                  prefix = this.idPrefix;

            // Clear form, but keep label
            this._clearForm(false, true);

            let labelInput = $(prefix + '_label'),
                label = labelInput.val().trim(),
                orgID = orgHeader.data('selected');

            // Update label input with trimmed value
            labelInput.val(label);
            if (!label) {
                return;
            }

            let formKey = $('input[type=hidden][name="_formkey"]', form).val(),
                distributionSet = this._getDistributionSet(),
                setID = distributionSet ? distributionSet.id : null;

            var input = {'a': 'check',
                         'k': formKey,
                         'l': label,
                         'o': orgID,
                         't': setID
                         },
                ajaxURL = this.options.ajaxURL,
                // Show a throbber
                throbber = $('<div class="inline-throbber">').insertAfter(this.personContainer),
                self = this;

            // Send the ajax request
            $.ajaxS3({
                'url': ajaxURL,
                'type': 'POST',
                'dataType': 'json',
                'contentType': 'application/json; charset=utf-8',
                'data': JSON.stringify(input),
                'success': function(data) {

                    // Remove the throbber
                    throbber.remove();

                    if (data.a) {
                        // Show error message on ID field
                        // TODO move into function _showInputAdvice
                        var msg = $('<div class="error_wrapper"><div id="label__error" class="error" style="display: block;">' + data.a + '</div></div>').hide();
                        msg.insertAfter($(prefix + '_label').closest('.controls')).slideDown('fast');

                    }

                    if (data.p) {
                        // Show the person details
                        self._showPerson(data.p);

                        // Show picture
                        if (data.b) {
                            self.imageURL.val(data.b);
                            self.profilePicture = data.b;
                            self._showProfilePicture();
                        }

                        // Update flag info
                        var flagInfo = self.flagInfo;
                        if (data.f) {
                            flagInfo.val(JSON.stringify(data.f));
                        } else {
                            flagInfo.val('[]');
                        }

                        // Update permission info
                        var permissionInfo = self.permissionInfo;
                        if (data.s !== undefined) {
                            permissionInfo.val(JSON.stringify(data.s));
                        } else {
                            permissionInfo.val('false');
                        }

                        // Update actionable info
                        var actionableInfo = self.actionableInfo,
                            actionable = data.u;
                        if (actionableInfo.length) {
                            if (actionable !== undefined) {
                                actionableInfo.val(JSON.stringify(actionable));
                            } else {
                                actionableInfo.val('');
                            }
                            self._renderItemSelection();
                        }

                        // Show the flag info
                        self._showFlagInfo();
                    }

                    // Show alerts
                    if (data.e) {
                        S3.showAlert(data.e, 'error');
                    }
                    if (data.w) {
                        S3.showAlert(data.w, 'warning');
                    }
                    if (data.c) {
                        S3.showAlert(data.c, 'success');
                    }
                },
                'error': function () {

                    // Remove throbber
                    throbber.remove();

                    // Clear the form, but keep the alert
                    self._clearForm(true, false);
                }
            });
        },

        /**
         * Ajax method to register the distribution
         */
        _registerDistribution: function() {

            let orgHeader = this.orgHeader,
                orgID = orgHeader.data('selected');

            this._clearAlert();

            let form = $(this.element),
                prefix = this.idPrefix,
                labelInput = $(prefix + '_label'),
                label = labelInput.val().trim(),
                setID = this.distributionSet.val();

            // Update label input with trimmed value
            labelInput.val(label);

            if (!label || !setID) {
                return;
            }

            let formKey = $('input[type=hidden][name="_formkey"]', form).val(),
                input = {'a': 'register',
                         'k': formKey,
                         'l': label,
                         'o': orgID,
                         't': setID
                         },
                ajaxURL = this.options.ajaxURL,
                // Don't clear the person info just yet
                personInfo = $(prefix + '_person__row .controls'),
                // Show a throbber
                throbber = $('<div class="inline-throbber">').insertAfter(personInfo),
                self = this;

            // Add action data (if any) to request JSON
            var actionDetails = this.actionDetails;
            if (actionDetails.length) {
                actionDetails = actionDetails.val();
                if (actionDetails) {
                    input.d = JSON.parse(actionDetails);
                } else {
                    input.d = {};
                }
            }

            $.ajaxS3({
                'url': ajaxURL,
                'type': 'POST',
                'dataType': 'json',
                'contentType': 'application/json; charset=utf-8',
                'data': JSON.stringify(input),
                'success': function(data) {

                    // Remove the throbber
                    throbber.remove();

                    if (data.a) {
                        // Show error message on ID field
                        var msg = $('<div class="error_wrapper"><div id="label__error" class="error" style="display: block;">' + data.a + '</div></div>').hide();
                        msg.insertAfter($(prefix + '_label')).slideDown('fast');

                    } else {
                        // Done - clear the form
                        self._clearForm();
                    }

                    // Show alert/confirmation message
                    if (data.e) {
                        S3.showAlert(data.e, 'error', false);
                    } else if (data.c) {
                        S3.showAlert(data.c, 'success', false);
                    }
                },
                'error': function () {

                    // Remove the throbber
                    throbber.remove();

                    // Clear the form, but keep the alert
                    self._clearForm(true);
                }
            });
        },

        /**
         * Shows the person details
         *
         * @param {html} details: the details HTML
         */
        _showPerson: function(details) {

            this.personRow.show();
            this.personContainer.empty().html(details).removeClass('hide').show();
        },

        /**
         * Removes the person details
         */
        _removePerson: function() {

            this.personRow.hide();
            this.personContainer.empty();
        },

        /**
         * Shows flag information
         */
        _showFlagInfo: function() {

            this.flagInfoRow.hide();

            let flagInfo = this.flagInfo,
                flagInfoContainer = this.flagInfoContainer.empty().removeClass('has-flaginfo'),
                flags;

            if (flagInfo.length) {
                flags = JSON.parse(flagInfo.val());
            } else {
                flags = [];
            }
            if (flags.length) {
                this.flagInfoRow.removeClass('hide').show();
                flagInfoContainer.addClass('has-flaginfo');

                let advise = $('<div class="checkpoint-advise">').hide();
                flags.forEach(function(flag) {
                    let instructions = $('<div class="checkpoint-instructions">').appendTo(advise);
                    $('<h4>' + flag.n + '</h4>').appendTo(instructions);
                    if (flag.i) {
                        $('<p>' + flag.i + '</p>').appendTo(instructions);
                    }
                });
                advise.appendTo(flagInfoContainer).slideDown('fast');
            }
        },

        /**
         * Removes the flag information
         */
        _removeFlagInfo: function() {

            this.flagInfoRow.hide();
            this.flagInfoContainer.empty();
            this.flagInfo.val('[]');
        },

        /**
         * Renders a panel to show the profile picture (automatically loads
         * the picture if options.showPicture is true)
         */
        _showProfilePicture: function() {

            var el = $(this.element),
                opts = this.options,
                profilePicture = this.profilePicture;

            this._removeProfilePicture();

            if (!profilePicture) {
                return;
            }

            var button = $('<button class="small secondary button toggle-picture" type="button">'),
                buttonRow = $('<div class="button-row">').append(button);
            button.text(opts.showPictureLabel);

            let panel = this.pictureContainer;
            if (!panel.length) {
                panel = $('<div class="panel profile-picture">').appendTo(el);
                this.pictureContainer = panel;
            }
            panel.append(buttonRow).data('url', profilePicture);

            if (opts.showPicture) {
                this._togglePicture();
            }
        },

        /**
         * Removes the profile picture panel
         */
        _removeProfilePicture: function() {

            this.imageURL.val('');
            this.profilePicture = null;

            this.pictureContainer.empty();
        },

        /**
         * Shows or hides the profile picture (click handler for toggle button)
         */
        _togglePicture: function() {

            var el = $(this.element),
                opts = this.options,
                container = this.pictureContainer;

            if (container.length) {
                var imageRow = container.find('.image-row'),
                    imageURL = container.data('url'),
                    toggle = container.find('button.toggle-picture');

                if (imageRow.length) {
                    imageRow.remove();
                    toggle.text(opts.showPictureLabel);
                } else {
                    if (imageURL) {
                        var image = $('<img>').attr('src', imageURL);
                        imageRow = $('<div class="image-row">').append(image);
                        container.prepend(imageRow);
                        toggle.text(opts.hidePictureLabel);
                    }
                }
            }
        },

        /**
         * Makes the item selection section visible
         */
        _showItemSelection: function() {

            this.detailsContainer.removeClass('hide').show();
            this.detailsRow.removeClass('hide').show();
        },

        /**
         * Hides the item selection section, without removing its contents
         */
        _hideItemSelection: function() {

            this.detailsRow.hide();
        },

        /**
         * Hides the item selection section and removes its contents
         */
        _removeItemSelection: function() {

            this.detailsRow.hide();
            this.detailsContainer.empty();
        },

        /**
         * Renders the distributable/returnable item tables
         */
        _renderItemSelection: function() {
            // TODO cleanup + simplify
            const opts = this.options;

            this._removeItemSelection();

            let actionableInfo = this.actionableInfo.val();
            if (!actionableInfo) {
                return;
            }

            let actionable = JSON.parse(actionableInfo),
                distributable = actionable.distribute,
                returnable = actionable.return;

            let controls = $('<div class="distribution-details">');

            if (distributable.items && distributable.items.length) {
                // Render distributable items table
                this._renderItemTable(opts.distributeLabel,
                                      'distribute',
                                      distributable.items).appendTo(controls);
            } else if (distributable.msg) {
                // Render distributable items table with message only
                this._renderItemTable(opts.distributeLabel,
                                      'distribute',
                                      null,
                                      distributable.msg).appendTo(controls);
            }

            if (returnable && returnable.length) {
                // Render returnable items table
                this._renderItemTable(opts.returnLabel,
                                      'return',
                                      returnable).appendTo(controls);
            }

            // Update action details from table
            //this.updateActionDetails();

            controls.appendTo(this.detailsContainer);
            this._showItemSelection();

            this._toggleSubmit(true);
        },

        /**
         * Renders an item table (for item selection and quantity input)
         *
         * @param {string} title - the title for the item table
         * @param {string} mode - the distribution mode (distribute|return)
         * @param {Array} items - an array of item data
         * @param {string} msg - a message to show if no items are available
         *
         * @returns {jQuery} - the item table
         */
        _renderItemTable: function(title, mode, items, msg) {
            // TODO cleanup + simplify

            let itemTable = $('<table class="item-table">'),
                tableHead = $('<thead>').appendTo(itemTable),
                tableBody = $('<tbody>').appendTo(itemTable);

            let titleRow = $('<tr class="item-title">').appendTo(tableHead),
                titleText = $('<th colspan="5">').text(title).appendTo(titleRow);

            let self = this;
            if (items) {
                self._renderItemHeader(mode).appendTo(tableHead);
                items.forEach(function(data) {
                    self._renderItemRow(data, mode).appendTo(tableBody);
                });
            } else if (msg) {
                self._renderMessageRow(msg).appendTo(tableBody);
            }

            return itemTable;
        },

        /**
         * Renders the column headers for an item table
         *
         * @param {string} mode - the distribution mode (distribute|return)
         *
         * @returns {jQuery} - a TR element
         */
        _renderItemHeader: function(mode) {

            const opts = this.options;

            let lossHeader = $('<th>'),
                itemHeader = $('<tr class="item-header">').append($('<th>'))
                                                          .append($('<th>').text(opts.itemLabel))
                                                          .append($('<th>').text(opts.quantityLabel))
                                                          .append(lossHeader)
                                                          .append($('<th>').text(opts.packLabel));
            if (mode == 'return') {
                lossHeader.text(opts.lossLabel);
            }
            return itemHeader;

        },

        /**
         * Renders an item row in a selection table
         *
         * @param {object} data - the item data
         * @param {string} mode - the distribution mode (distribute|return)
         */
        _renderItemRow: function(data, mode) {

            const opts = this.options;

            // Determine default quantity
            let defaultQuantity = data.quantity - 0;
            if (isNaN(defaultQuantity)) {
                defaultQuantity = (data.max - 0) || 0;
            }

            // Store initial item data in itemRow
            let itemRow = $('<tr class="dist-item">').data({
                mode: data.mode || (mode == 'return' ? 'RET' : null),
                itemID: data.id,
                packID: data.pack_id,
                quantity: defaultQuantity,
                max: data.max,
                itemQ: defaultQuantity,
                lossQ: 0
            });

            // Checkbox to select item
            let selectCheckbox = $('<input type="checkbox" class="select-item">');
            $('<td>').append(selectCheckbox).appendTo(itemRow);

            // Item name and distribution mode indicator
            let itemName = $('<div>').text(data.name),
                itemNameCol = $('<td>').append(itemName).appendTo(itemRow);
            if (data.mode == 'LOA') {
                // Add mode indicator
                $('<div class="dist-mode">').text(opts.loanLabel).appendTo(itemNameCol);
            }

            // Quantity input
            let quantityInput = $('<input type="number" min="0" class="item-q">').val(defaultQuantity).prop('disabled', true);
            $('<td>').append(quantityInput).appendTo(itemRow);

            // Lost Quantity input
            if (mode == 'return') {
                let lostQuantityInput = $('<input type="number" min="0" class="loss-q" value="0">').prop('disabled', true);
                $('<td>').append(lostQuantityInput).appendTo(itemRow);
            } else {
                $('<td>').appendTo(itemRow);
            }

            // Item pack description
            let itemPack = $('<div>').text(data.pack);
            $('<td>').append(itemPack).appendTo(itemRow);

            return itemRow;
        },

        /**
         * Renders a row with a message if/why distribution is currently
         * not permitted
         *
         * @param {string} msg - the message
         */
        _renderMessageRow: function(msg) {

            let messageRow = $('<tr>').append($('<td class="blocked-msg" colspan="5">').text(msg));

            return messageRow;
        },

        /**
         * Updates action details from selected items
         */
        _updateActionDetails: function() {

            var error = false,
                distributed = [],
                returned = [],
                distribution = {d: distributed, r: returned};

            $('.dist-item.selected', this.detailsContainer).each(function() {
                let $this = $(this),
                    data = $this.data();
                if ($('.invalidinput', $this).length) {
                    error = true;
                } else if (data.itemQ || data.lossQ){
                    if (data.mode == "RET") {
                        returned.push([data.itemID, data.packID, "RET", data.itemQ, data.lossQ]);
                    } else {
                        distributed.push([data.itemID, data.packID, data.mode, data.itemQ]);
                    }
                }
            });

            if (!error && (distributed.length || returned.length)) {
                this.actionDetails.val(JSON.stringify(distribution));
            } else {
                this.actionDetails.val('');
            }
        },

        /**
         * Checks if the input is actionable, i.e. valid action details present
         *
         * @returns {boolean} - whether the distribution can be registered
         */
        _checkActionable: function() {

            let actionable = this.actionDetails.val();

            return !!actionable;
        },

        /**
         * Handles selection of an item
         *
         * @param {DOM|jQuery} checkbox - the checkbox node
         */
        _selectItem: function(checkbox) {

            const $checkbox = $(checkbox),
                  itemRow = $checkbox.closest('tr.dist-item'),
                  quantity = itemRow.data('quantity');

            $('.error', itemRow).remove();
            if ($checkbox.prop('checked')) {
                $('.item-q, .loss-q', itemRow).removeClass('invalidinput');
                $('.item-q', itemRow).val(quantity).prop('disabled', false);
                $('.loss-q', itemRow).val(0).prop('disabled', false);
                itemRow.addClass('selected').data({itemQ: quantity, lossQ: 0});
            } else {
                $('.item-q, .loss-q', itemRow).removeClass('invalidinput')
                                              .prop('disabled', true);
                itemRow.removeClass('selected').data({itemQ: 0, lossQ: 0});
            }

            this._updateActionDetails();
            this._toggleSubmit(true);
        },

        /**
         * Handles quantity modifications
         *
         * @param {DOM|jQuery} inputField - the quantity input field
         */
        _changeQuantity: function(inputField) {

            const $inputField = $(inputField),
                  itemRow = $inputField.closest('tr.dist-item');

            $inputField.removeClass('invalidinput');
            $('.error', itemRow).remove();

            let value = $inputField.val();
            if (!value) {
                $inputField.removeClass('invalidinput').val('');
                return;
            } else {
                value = Math.abs(parseInt(value - 0));
                $inputField.val(value);
            }

            let total = value,
                maxTotal = itemRow.data('max'),
                hasError = false;
            if (maxTotal) {
                let $other;
                if ($inputField.hasClass('loss-q')) {
                    $other = $('.item-q', itemRow);
                } else {
                    $other = $('.loss-q', itemRow);
                }
                if ($other.length && $other.val()) {
                    total += ($other.val() - 0) || 0;
                }
                if (total > maxTotal) {
                    let otherValue = 0;
                    if (value > maxTotal) {
                        $inputField.addClass('invalidinput');
                        $inputField.after($('<div class="error">').text('max ' + maxTotal));
                        hasError = true;
                    } else if ($other.length) {
                        otherValue = maxTotal - value;
                    }
                    if ($other.length) {
                        $other.removeClass('invalidinput').val(otherValue);
                    }
                }
            }

            // Update itemQ and lossQ
            if (hasError) {
                itemRow.data({itemQ: 0, lossQ: 0});
            } else {
                let itemQ = $('.item-q', itemRow).val() - 0 || 0,
                    lossQ = $('.loss-q', itemRow).val() - 0 || 0;
                itemRow.data({itemQ: itemQ, lossQ: lossQ});
            }

            this._updateActionDetails();
            this._toggleSubmit(true);
        },

        /**
         * Helper function to toggle the submit mode of the form
         *
         * @param {bool} submit - true to enable register-button while disabling
         *                        the ID check button, false vice versa
         */
        _toggleSubmit: function(submit) {
            // TODO cleanup

            var form = $(this.element),
                buttons = ['.check-btn', '.submit-btn'],
                permissionInfo = this.permissionInfo,
                actionable = true;

            if (submit) {

                var permitted = false;

                // Check whether form action is permitted
                if (permissionInfo.length) {
                    permissionInfo = permissionInfo.val();
                    if (permissionInfo) {
                        permitted = JSON.parse(permissionInfo);
                    }
                }

                // Check whether the form is actionable
                if (permitted) {
                    actionable = this._checkActionable();
                }

                // Only enable submit if permitted and actionable
                buttons.reverse();
            }

            var active = form.find(buttons[0]),
                disabled = form.find(buttons[1]);

            disabled.prop('disabled', true).hide().insertAfter(active);
            active.prop('disabled', !actionable).hide().removeClass('hide').show();
        },

        /**
         * Helper function to hide any alert messages that are currently shown
         */
        _clearAlert: function() {

            $('.alert-error, .alert-warning, .alert-info, .alert-success').fadeOut('fast');
            $('.error_wrapper').fadeOut('fast').remove();
        },

        /**
         * Helper function to remove the person data and empty the label input,
         * also re-enabling the ID check button while hiding the registration button
         *
         * @param {bool} keepAlerts - do not clear the alert space
         * @param {bool} keepLabel - do not clear the label input field
         */
        _clearForm: function(keepAlerts, keepLabel) {

            // Remove all throbbers
            $('.inline-throbber').remove();

            // Clear alerts
            if (!keepAlerts) {
                this._clearAlert();
            }

            // Clear ID label
            if (!keepLabel) {
                $(this.idPrefix + '_label').val('');
            }

            // Remove all contents
            this._removePerson();
            this._removeFlagInfo();
            this._removeItemSelection();
            this._removeProfilePicture();

            // Remove selection data
            this.actionableInfo.val('');
            this.actionDetails.val('');

            // Reset submit-button label
            $('.submit-btn').val(this.submitLabel);

            // Disable submit
            this._toggleSubmit(false);

            // Focus on label input
            var labelInput = $(this.idPrefix + '_label');
            labelInput.trigger('focus').val(labelInput.val());
        },

        /**
         * Returns the currently selected distribution set
         *
         * @returns {object} - an object identifying the set {id, name},
         *                     or null, if no set selected
         */
        _getDistributionSet: function() {

            // Read the set ID from distribution set input
            const setID = this.distributionSet.val();
            if (!setID) {
                return null;
            }

            // Find the distribution set selector with this set ID
            let selector = $('a.event-type-select', this.distributionSetSelect).filter(
                function() { return $(this).data('id') == setID; }
            );
            if (!selector.length) {
                return null;
            }

            // Return the event set details
            return {
                id: selector.data('id') || null,
                name: selector.data('name') || null,
            };
        },

        /**
         * Applies the selection of a distribution set
         *
         * @param {integer} setID: the distribution set ID
         * @param {string} name: the distribution set name
         */
        _setDistributionSet: function(setID, name) {

            // Store new distribution set in form
            this.distributionSet.val(setID);

            // Update event set in header
            $('.event-type-name', this.distributionSetHeader).text(name);

            this._updateDistributionSet();

            // Re-evaluate input
            this._clearForm(false, true);
            this._checkID();

            // Enable submit if we have a person
            if ($(this.idPrefix + '_person__row .controls').text()) {
                this._toggleSubmit(true);
            }
        },

        /**
         * Removes the current selection of a distribution set
         */
        _clearDistributionSet: function() {

            // Store new event set in form
            this.distributionSet.val('');

            // Update event set in header
            this._updateDistributionSet();

            this._toggleSubmit(false);
        },

        /**
         * Updates the distribution set indicator after setting/clearing the
         * selected distribution type
         */
        _updateDistributionSet: function() {

            let distributionSetHeader = this.distributionSetHeader,
                distributionSetSelect = this.distributionSetSelect,
                numSelectable = $('a.event-type-select', distributionSetSelect).length,
                selected = this.distributionSet.val(),
                opts = this.options,
                label;

            if (!selected) {
                if (numSelectable > 0) {
                    label = opts.selectDistributionSetLabel;
                } else {
                    label = opts.noDistributionSetsLabel;
                }
                $('.event-type-name', distributionSetHeader).text(label);
            } else {
                distributionSetHeader.removeClass('challenge');
            }

            if (numSelectable == 0) {
                distributionSetHeader.addClass('empty').addClass('disabled');
            } else if (numSelectable == 1 && !!selected) {
                distributionSetHeader.removeClass('empty').addClass('disabled');
            } else {
                distributionSetHeader.removeClass('empty').removeClass('disabled');
                if (!selected) {
                    distributionSetHeader.addClass('challenge');
                }
            }
        },

        /**
         * (Re-)populates the distribution set selector buttons
         *
         * @param {object} data: the response JSON from the distribution set lookup:
         *                       {
         *                        "types": [[id, name], ...],
         *                        "default": [id, name],
         *                        }
         */
        _populateDistributionSets: function(data) {

            let distributionSetSelect = this.distributionSetSelect.empty(),
                distributionSets = data.sets,
                defaultDistributionSet = data.default;

            if (!defaultDistributionSet && distributionSets.length == 1) {
                defaultDistributionSet = distributionSets[0];
            }

            distributionSets.forEach(function(distributionSet) {
                let setID = distributionSet[0],
                    name = distributionSet[1],
                    btn = $('<a class="secondary button event-type-select">');

                btn.text(name)
                   .appendTo(distributionSetSelect)
                   .data({'id': setID, 'name': name});
            });

            if (defaultDistributionSet) {
                this._setDistributionSet(defaultDistributionSet[0], defaultDistributionSet[1]);
            } else {
                this._clearDistributionSet();
            }
        },

        /**
         * Handler for organisation selector button to select the respective
         * organisation, and update selectable distribution sets
         *
         * @param {jQuery} btn: the selector button
         */
        _selectOrg: function(btn) {

            let organisationID = btn.data('id'),
                organisationName = btn.data('name'),
                orgHeader = this.orgHeader,
                orgSelect = this.orgSelect;

            this._clearForm();

            orgHeader.data('selected', organisationID).addClass('selected');
            $('h4.org-name', orgHeader).text(organisationName);

            let throbber = $('<div class="inline-throbber">'),
                distributionSetHeader = this.distributionSetHeader.addClass('disabled'),
                distributionSetName = $('.event-type-name', distributionSetHeader).hide().after(throbber),
                ajaxURL = this.options.ajaxURL + '?org=' + organisationID,
                self = this;

            $.ajaxS3({
                'url': ajaxURL,
                'type': 'GET',
                'dataType': 'json',
                'contentType': 'application/json; charset=utf-8',
                'success': function(data) {
                    self._populateDistributionSets(data);
                    throbber.remove();
                    distributionSetName.show();
                    if (data.sets.length > 1) {
                        distributionSetHeader.removeClass('disabled');
                    }
                },
                'error': function () {
                    throbber.remove();
                }
            });

            orgSelect.slideUp('fast');
        },

        /**
         * Handler for distribution set selector button
         *
         * @param {jQuery} btn: the selector button
         */
        _selectDistributionSet: function(btn) {

            let setID = btn.data('id'),
                name = btn.data('name');

            this._setDistributionSet(setID, name);

            // Hide event type selector
            this.distributionSetSelect.slideUp('fast');
        },

        /**
         * Binds events to generated elements (after refresh)
         */
        _bindEvents: function() {

            const form = $(this.element),
                  prefix = this.idPrefix,
                  ns = this.eventNamespace,
                  self = this;

            let orgHeader = this.orgHeader,
                orgSelect = this.orgSelect,
                distributionSetHeader = this.distributionSetHeader,
                distributionSetSelect = this.distributionSetSelect;

            // Organisation selection
            orgHeader.on('click' + ns, function(e) {
                e.preventDefault();
                e.stopPropagation();
                if (orgHeader.hasClass('disabled')) {
                    return false;
                }
                distributionSetSelect.slideUp('fast', function() {
                    if (orgSelect.hasClass('hide')) {
                        orgSelect.hide().removeClass('hide').slideDown('fast');
                    } else {
                        orgSelect.slideToggle('fast');
                    }
                });
            });
            orgSelect.on('click' + ns, 'a.org-select', function(e) {
                e.preventDefault();
                e.stopPropagation();
                self._selectOrg($(this));
            });

            // Distribution set selection
            distributionSetHeader.on('click' + ns, function(e) {
                e.preventDefault();
                e.stopPropagation();
                if (distributionSetHeader.hasClass('disabled')) {
                    return false;
                }
                orgSelect.slideUp('fast', function() {
                    if (distributionSetSelect.hasClass('hide')) {
                        distributionSetSelect.hide().removeClass('hide').slideDown('fast');
                    } else {
                        distributionSetSelect.slideToggle('fast');
                    }
                });
            });
            distributionSetSelect.on('click' + ns, 'a.event-type-select', function(e) {
                e.preventDefault();
                e.stopPropagation();
                self._selectDistributionSet($(this));
            });

            // Show/hide picture
            this.pictureContainer.on('click' + ns, '.toggle-picture', function(e) {
                e.preventDefault();
                self._togglePicture();
            });

            // Never submit the form
            form.off(ns).on('submit' + ns, function(e) {
                e.preventDefault();
                return false;
            });

            // Cancel-button to clear the form
            form.find('a.cancel-action, .clear-btn').on('click' + ns, function(e) {
                e.preventDefault();
                self._clearForm();
            });
            $('.qrscan-btn', form).on('click' + ns, function(e) {
                self._clearForm();
            });

            // Click-Handler for Check-ID button
            form.find('.check-btn').on('click' + ns, function(e) {
                e.preventDefault();
                self._checkID();
            });
            // Click-Handler for Register button
            form.find('.submit-btn').off(ns).on('click' + ns, function(e) {
                e.preventDefault();
                self._registerDistribution();
            });

            // Events for the label input
            var labelInput = $(prefix + '_label');

            // Changing the label resets form
            labelInput.on('input' + ns, function(e) {
                self._clearForm(false, true);
            });

            // Quantity change
            $(form).on('change' + ns, '.select-item', function() {
                self._selectItem(this);
            });
            $(form).on('input' + ns, '.item-q, .loss-q', function() {
                self._changeQuantity(this);
            });

            // Key events for label field
            labelInput.on('keyup' + ns, function(e) {
                e.preventDefault();
                e.stopPropagation();
                switch (e.which) {
                    case 13:
                        // TODO only if check-button is active, otherwise do nothing
                        self._checkID();
                        break;
                    case 27:
                        // Pressing ESC resets the form
                        self._clearForm();
                        break;
                    default:
                        break;
                }
            });

            return true;
        },

        /**
         * Unbinds events (before refresh)
         */
        _unbindEvents: function() {

            const form = $(this.element),
                  ns = this.eventNamespace,
                  prefix = this.idPrefix;

            this.orgHeader.off(ns);
            this.orgSelect.off(ns);

            this.distributionSetHeader.off(ns);
            this.distributionSetSelect.off(ns);

            // TODO clean up these:
            $(prefix + '_label').off(ns);

            form.find('a.cancel-action').off(ns);

            form.find('.check-btn').off(ns);

            form.find('.submit-btn').off(ns);

            form.off(ns);

            return true;
        }
    });
})(jQuery);
