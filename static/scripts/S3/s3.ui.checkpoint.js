/**
 * jQuery UI Widget for Checkpoint UI (DVR)
 *
 * @copyright 2016-2023 (c) Sahana Software Foundation
 * @license MIT
 */
(function($, undefined) {

    "use strict";

    var checkPointID = 0;

    /**
     * checkPoint, instantiate on event registration form
     */
    $.widget('dvr.checkPoint', {

        /**
         * Default options
         *
         * @prop {string} tablename - the tablename used for the form
         * @prop {string} ajaxURL - the URL to send Ajax requests to
         *
         * @prop {boolean} showPicture - true=always show profile picture
         *                               false=show profile picture on demand
         * @prop {string} showPictureText - button label for "Show Picture"
         * @prop {string} hidePictureText - button label for "Hide Picture"
         */
        options: {

            tablename: 'case_event',
            ajaxURL: '',

            showPicture: true,
            showPictureText: 'Show Picture',
            hidePictureText: 'Hide Picture',
            selectAllText: 'Select All',

            noEventsLabel: 'No event types available',
            selectEventLabel: 'Please select an event type',
        },

        /**
         * Create the widget
         */
        _create: function() {

            this.id = checkPointID;
            checkPointID += 1;

            // Namespace for events
            this.eventNamespace = '.checkPoint';
        },

        /**
         * Update the widget options
         */
        _init: function() {

            let form = $(this.element),
                widgetID = form.attr('id');

            // ID prefix for form rows
            this.idPrefix = '#' + this.options.tablename;

            // Control elements outside of the form
            this.orgHeader = $('#' + widgetID + '-org-header');
            this.orgSelect = $('#' + widgetID + '-org-select');
            this.eventTypeHeader = $('#' + widgetID + '-event-type-header');
            this.eventTypeSelect = $('#' + widgetID + '-event-type-select');
            this.pictureContainer = $('#' + widgetID + '-picture');

            // Form Rows
            let prefix = this.idPrefix;
            this.personRow = $(prefix + '_person__row', form);
            this.flagInfoRow = $(prefix + '_flaginfo__row', form);
            this.detailsRow = $(prefix + '_details__row', form);
            this.familyRow = $(prefix + '_family__row', form);

            // Containers
            this.personContainer = $('.controls', this.personRow);
            this.flagInfoContainer = $('.controls', this.flagInfoRow);
            this.detailsContainer = $('.controls', this.detailsRow);
            this.familyContainer = $('.controls', this.familyRow);

            // Hidden input fields
            this.eventType = form.find('input[type="hidden"][name="event"]');

            this.flagInfo = $('input[type=hidden][name=flags]', form);
            this.familyInfo = $('input[type=hidden][name=familyinfo]', form);
            this.blockingInfo = $('input[type="hidden"][name="intervals"]', form);
            this.imageURL = $('input[type="hidden"][name="image"]', form);
            this.actionDetails = $('input[type="hidden"][name="actions"]', form);

            this.permissionInfo = form.find('input[type=hidden][name=permitted]');
            this.actionableInfo = form.find('input[type=hidden][name=actionable]');

            // Submit label
            this.submitLabel = form.find('.submit-btn').first().val();

            // Profile picture URL
            this.profilePicture = null;

            // Get blocked events from hidden input
            var intervals = this.blockingInfo.val();
            if (intervals) {
                this.blockedEvents = JSON.parse(intervals);
            } else {
                this.blockedEvents = {};
            }
            this.blockingMessage = null;

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

            // AjaxURL is required
            if (!opts.ajaxURL) {
                throw new Error('checkPoint: no ajaxURL provided');
            }

            this._clearForm();
            this._updateEventType();

            this._bindEvents();
        },

        /**
         * Ajax method to identify the person from the label
         */
        _checkID: function() {

            let orgHeader = this.orgHeader,
                orgID = orgHeader.data('selected');

            // Clear form, but keep label
            this._clearForm(false, true);

            let form = $(this.element),
                prefix = this.idPrefix,
                labelInput = $(prefix + '_label'),
                label = labelInput.val().trim();

            // Update label input with trimmed value
            labelInput.val(label);
            if (!label) {
                return;
            }

            let formKey = $('input[type=hidden][name="_formkey"]', form).val();
            var input = {'a': 'check',
                         'l': label,
                         'o': orgID,
                         'e': null,
                         'k': formKey,
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
                                actionableInfo.val(JSON.stringify(data.u));
                            } else {
                                actionable = true;
                                actionableInfo.val('true');
                            }
                        }

                        // Render details
                        if (data.d) {
                            self._updateDetails(data.d, actionable);
                        }

                        // Family
                        var family = data.x || [];
                        self.familyInfo.val(JSON.stringify(family));
                        self._showFamily();

                        // Update blocked events
                        self.blockedEvents = data.i || {};
                        self.blockingInfo.val(JSON.stringify(self.blockedEvents));

                        // Attempt to enable submit if we have a valid event type
                        // - this will automatically check whether the registration is
                        //   permitted, actionable and not blocked due to minimum intervals
                        if (self.eventType.val()) {
                            self._toggleSubmit(true);
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
                    if (data.m) {
                        S3.showAlert(data.m, 'success');
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
         * Ajax method to register the event
         */
        _registerEvent: function() {

            let orgHeader = this.orgHeader,
                orgID = orgHeader.data('selected');

            this._clearAlert();

            let form = $(this.element),
                prefix = this.idPrefix,
                labelInput = $(prefix + '_label'),
                label = labelInput.val().trim(),
                eventTypeCode = this.eventType.val();

            // Update label input with trimmed value
            labelInput.val(label);

            if (!label || !eventTypeCode) {
                return;
            }

            let formKey = $('input[type=hidden][name="_formkey"]', form).val(),
                input = {'a': 'register',
                         'l': label,
                         'o': orgID,
                         'e': eventTypeCode,
                         'k': formKey,
                         }, //{'l': label, 't': event},
                ajaxURL = this.options.ajaxURL,
                // Don't clear the person info just yet
                personInfo = $(prefix + '_person__row .controls'),
                // Show a throbber
                throbber = $('<div class="inline-throbber">').insertAfter(personInfo),
                self = this;

            // Check family member selection
            // TODO only if event type permits this
            let family = $('.family-members', this.familyContainer);
            if (family.length) {

                // Selecting family members individually
                let familyIDs = [];

                $('.family-member', family).each(function() {
                    let $this = $(this),
                        selected = $this.find('input.member-select').prop('checked');
                    if (selected) {
                        let memberID = $this.data('member').l;
                        if (memberID) {
                            familyIDs.push(memberID);
                        }
                    }
                });
                input.l = familyIDs;
            }

            // Add action data (if any) to request JSON
            var actionDetails = this.actionDetails;
            if (actionDetails.length) {
                actionDetails = actionDetails.val();
                if (actionDetails) {
                    input.d = JSON.parse(actionDetails);
                } else {
                    input.d = [];
                }
            }

            // Add comments to request JSON
            input.c = $(prefix + 'comments').val();

            $.ajaxS3({
                'url': ajaxURL,
                'type': 'POST',
                'dataType': 'json',
                'contentType': 'application/json; charset=utf-8',
                'data': JSON.stringify(input),
                'success': function(data) {

                    // Remove the throbber
                    throbber.remove();

                    if (data.e) {
                        // Show error message on ID field
                        var msg = $('<div class="error_wrapper"><div id="label__error" class="error" style="display: block;">' + data.e + '</div></div>').hide();
                        msg.insertAfter($(prefix + '_label')).slideDown('fast');

                    } else {
                        // Done - clear the form
                        self._clearForm();
                    }

                    // Show alert/confirmation message
                    if (data.a) {
                        S3.showAlert(data.a, 'error', false);
                    } else if (data.m) {
                        S3.showAlert(data.m, 'success', false);
                    }
                },
                'error': function () {

                    // Remove the throbber
                    throbber.remove();

                    // Clear the form, but keep the alert
                    this._clearForm(true);
                }
            });
        },

        /**
         * Helper method to check for blocked events and show message
         *
         * @returns {boolean} - whether the event can be registered
         */
        _checkBlockedEvents: function() {

            // Get current event type and blocked events
            var event = this.eventType.val(),
                blocked = this.blockedEvents,
                info = blocked[event],
                self = this;

            // Remove existing message, if any
            if (this.blockingMessage) {
                this.blockingMessage.remove();
            }

            // Helper function to render the overall blocked-info
            var checkBlocked = function(info, hasFamily) {
                let permitted = true;
                if (info && (!hasFamily || !info[2])) {
                    let message = $('<h6>').addClass('event-registration-blocked').html(info[0]),
                        date = info[1] ? new Date(info[1]) : null,
                        now = new Date();
                    if (date === null || date > now) {
                        // Event registration is blocked for main ID, show message
                        self.blockingMessage = $('<div>').addClass('small-12-columns')
                                                         .append(message)
                                                         .prependTo($('#submit_record__row'));
                        permitted = false;
                    }
                }
                return permitted;
            };

            if ($('.family-members', this.familyContainer).length) {
                let permitted = checkBlocked(info, true);
                return !!this._updateFamilyStatus(permitted);
            } else {
                return checkBlocked(info, false);
            }
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
         * Helper function to show flag infos
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

        _removeFlagInfo: function() {
            // Remove it (part of clearForm)

            this.flagInfoRow.hide();
            this.flagInfoContainer.empty();
            this.flagInfo.val('[]');
        },

        /**
         * Render a panel to show the profile picture (automatically loads
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
            button.text(opts.showPictureText);

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
         * Remove the profile picture panel
         */
        _removeProfilePicture: function() {

            this.profilePicture = null;
            this.pictureContainer.empty();
        },

        /**
         * Show or hide the profile picture (click handler for toggle button)
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
                    toggle.text(opts.showPictureText);
                } else {
                    if (imageURL) {
                        var image = $('<img>').attr('src', imageURL);
                        imageRow = $('<div class="image-row">').append(image);
                        container.prepend(imageRow);
                        toggle.text(opts.hidePictureText);
                    }
                }
            }
        },

        /**
         * Update buttons to show family member pictures:
         *  - set "showing" class on button if picture is currently shown
         *  - add caption to picture
         */
        _updatePictureButtons: function() {

            var el = $(this.element),
                container = this.pictureContainer,
                pictureShown = container.data('url');

            $('button.member-show-picture').removeClass('showing');
            $('.member-caption').remove();

            if (pictureShown) {
                $('.family-member').each(function() {
                    var $this = $(this),
                        memberInfo = $this.data('member');
                    if (memberInfo.p == pictureShown) {
                        $this.find('button.member-show-picture').addClass('showing');
                        container.find('.button-row')
                                 .before($('<div class="member-caption">' + memberInfo.n + '</div>'));

                    }
                });
            }
        },

        /**
         * Helper function to show the details form fields
         *
         * @param {bool} actionable - whether there are any actionable details
         */
        _showDetails: function(actionable) {

            this.detailsRow.show();

            var prefix = this.idPrefix;

            $(prefix + '_details__row').show();
//             if (actionable) {
//                 $(prefix + '_date__row').show();
//                 $(prefix + '_comments__row').show();
//             }
        },

        /**
         * Helper function to hide the details form fields
         */
        _hideDetails: function() {

            var prefix = this.idPrefix,
                hasPersonInfo = this.personContainer.text();

            if (hasPersonInfo) {
                // Retain the details (showing empty-message)
                this.detailsRow.show();
            } else {
                // Hide the details if there are no person data
                this.detailsRow.hide();
            }
//             // Hide all other details
//             $(prefix + '_date__row').hide();
//             $(prefix + '_comments__row').hide();
        },


        /**
         * Helper function to update the action details in the form
         *
         * @param {object} data - the action details as dict
         * @param {bool} actionable - whether there are any actionable details
         */
        _updateDetails: function(data, actionable) {

            var prefix = this.idPrefix,
                detailsContainer = $(prefix + '_details__row .controls'),
                dateContainer = $(prefix + '_date__row .controls');

            // Update the hidden input
            var actionDetails = this.actionDetails;
            if (actionDetails.length) {
                if (data.h !== '') {
                    actionDetails.val(JSON.stringify(data.h));
                } else {
                    actionDetails.val('[]');
                }
            }

            // Update the visible form fields
            $(prefix + '_details__row .controls').html(data.d);
            $(prefix + '_date__row .controls').html(data.t);

            // Show the form fields
            this._showDetails(actionable);
        },

        /**
         * Helper function to clear the details form fields
         */
        _removeDetails: function() {

            var prefix = this.idPrefix;

            this._hideDetails();

            // Reset flag info and permission info
            this.flagInfo.val('[]');
            this.permissionInfo.val('false');
            $(prefix + '_flaginfo__row .controls').empty();

            // Remove action details, date and comments
            $(prefix + '_details__row .controls').empty();
            $(prefix + '_date__row .controls').empty();
            $(prefix + '_comments').val('');

            // Remove blocked events and message
            this.blockedEvents = {};
            if (this.blockingMessage) {
                this.blockingMessage.remove();
            }
            this.blockingInfo.val(JSON.stringify(this.blockedEvents));
        },

        /**
         * Shows family members
         */
        _showFamily: function() {

            let eventType = this._getEventType();
            if (!eventType || !eventType.multiple) {
                return;
            }

            let family = this.familyInfo;
            if (family.length) {

                let members = JSON.parse(family.val()),
                    familyContainer = this.familyContainer.empty();

                if (members.length) {
                    this.familyRow.show();

                    // Family table
                    let table = $('<table class="family-members">').hide();

                    // Member rows
                    members.forEach(function(member) {
                        this._renderFamilyMember(member).appendTo(table);
                    }, this);

                    // Select-all row
                    let opts = this.options,
                        trow = $('<tr class="family-all">');
                    trow.append($('<td><input class="member-select-all" type="checkbox"/></td>'))
                        .append($('<td colspan="2">' + opts.selectAllText + '</td>'))
                        .appendTo(table);

                    table.appendTo(familyContainer).slideDown('fast');
                } else {
                    this.familyRow.hide();
                }
            }
        },

        /**
         * Hides the family info
         */
        _hideFamily: function() {

            this.familyContainer.empty();
            this.familyRow.hide();
            this.profilePicture = this.imageURL.val();
            this._showProfilePicture();
        },

        /**
         * Removes the family info
         */
        _removeFamily: function() {

            this.familyInfo.val('[]');
            this.familyContainer.empty();
            this.familyRow.hide();
        },

        /**
         * Renders the data of a family member as a table row
         *
         * @returns {jQuery} - the <tr> node
         */
        _renderFamilyMember: function(member) {

            let opts = this.options;

            // Inner HTML for the select-member-column
            let memberSelect = $('<input class="member-select" type="checkbox">');

            // Inner HTML for member-details-column
            let memberID = $('<div class="medium-4 small-12 columns member-id">' + member.l + '</div>'),
                memberName = $('<div class="medium-8 small-12 columns member-name">' + member.n + ' (' + member.d + ')</div>'),
                memberInfo = $('<div class="member-info row">').append(memberID)
                                                               .append(memberName);

            // Inner HTML for show-picture-column
            let memberShowPicture = $('<div class="member-show-picture">');
            if (member.p) {
                $('<button class="tiny secondary button fright member-show-picture" type="button">' + opts.showPictureText + '</button>').appendTo(memberShowPicture);
            }

            // The columns
            let selectColumn = $('<td>').append(memberSelect),
                infoColumn = $('<td class="member-info">').append(memberInfo),
                showPictureColumn = $('<td>').append(memberShowPicture);

            // The table row
            let memberRow = $('<tr class="family-member">').data('member', member)
                                                           .append(selectColumn)
                                                           .append(infoColumn)
                                                           .append(showPictureColumn);
            return memberRow;
        },

        /**
         * Update status for family members (selectable or not)
         *
         * @returns {integer} - the number of selectable family members
         */
        _updateFamilyStatus: function(permitted) {

            let family = this.familyContainer,
                members = $('.family-member', family),
                event = this.eventType.val(),
                selectable = 0,
                now = new Date();

            let eventType = this._getEventType();
            if (!eventType || !eventType.multiple) {
                this._hideFamily();
                this._updateSelectAll();
                return;
            }
            if (!members.length && this.familyInfo.val()) {
                this._showFamily();
            }

            $('.family-member', family).each(function() {

                var trow = $(this),
                    member = trow.data('member'),
                    rules = member.r,
                    blocked = !permitted,
                    message = '';

                if (rules && rules.hasOwnProperty(event)) {

                    var rule = rules[event],
                        date = rule[1] ? new Date(rule[1]) : null;

                    if (date === null || date > now) {
                        blocked = true;
                        message = $('<div class="member-message">' + rule[0] + '</div>');
                    }
                }

                // => remove blocking-message
                trow.find('.member-message').remove();

                if (blocked) {
                    // Event is blocked for this member
                    trow.removeClass('member-selected');
                    $('.member-select', trow).each(function() {
                        $(this).prop('checked', false)
                               .prop('disabled', true);
                    });

                    // => set blocked-class for row
                    trow.addClass("member-blocked");

                    // => show blocking message
                    let alertRow = $('<div class="member-message row">'),
                        alert = $('<div class="columns">').append(message)
                                                          .appendTo(alertRow);
                    trow.find('.member-info.row').after(alertRow);

                } else {
                    selectable++;

                    // => remove blocked-class for row
                    trow.removeClass("member-blocked");

                    // => enable and select checkbox
                    $('.member-select', trow).each(function() {
                        $(this).prop('disabled', false)
                               .prop('checked', true);
                        trow.addClass('member-selected');
                    });
                }
            });

            this._updateSelectAll();
            this._updatePictureButtons();

            return selectable;
        },

        /**
         * Update the status of bulk-select checkbox according to
         * the status of the individual select boxes
         */
        _updateSelectAll: function() {

            var selectable = 0,
                selected = 0,
                allSelected = true;

            // Count selectable and selected members
            $('.member-select').each(function() {
                var $this = $(this);
                if (!$this.prop('disabled')) {
                    selectable++;
                    if (!$this.prop('checked')) {
                        allSelected = false;
                    } else {
                        selected++;
                    }
                }
            });

            // Update select-all checkbox
            var selectAll = $('.member-select-all');
            if (!selectable) {
                selectAll.prop('checked', false)
                         .prop('disabled', true);
            } else {
                selectAll.prop('disabled', false)
                         .prop('checked', allSelected);
            }

            // Update submit-button label
            let submitBtn = $('.submit-btn');
            if ($('.member-select').length) {
                submitBtn.val(this.submitLabel + ' (' + selected + ')');
                if (selected) {
                    submitBtn.prop('disabled', false);
                } else {
                    submitBtn.prop('disabled', true);
                }
            } else {
                submitBtn.val(this.submitLabel);
            }
        },

        /**
         * Select/de-select all family members
         *
         * @param {boolean} select - true to select, false to de-select
         */
        _selectAll: function(select) {

            var selected = 0;

            $('.member-select').each(function() {

                var $this = $(this);

                if (!$this.prop('disabled')) {
                    if (select) {
                        $this.prop('checked', true)
                             .closest('tr.family-member').addClass('member-selected');
                        selected++;
                    } else {
                        $this.prop('checked', false)
                             .closest('tr.family-member').removeClass('member-selected');
                    }
                }
            });

            this._updateSelectAll();
        },

        /**
         * Helper function to toggle the submit mode of the form
         *
         * @param {bool} submit - true to enable event registration while disabling
         *                        the ID check button, false vice versa
         */
        _toggleSubmit: function(submit) {

            var form = $(this.element),
                buttons = ['.check-btn', '.submit-btn'],
                permissionInfo = this.permissionInfo,
                actionableInfo = this.actionableInfo;

            if (submit) {

                var permitted = false,
                    actionable = false;

                // Check whether form action is permitted
                if (permissionInfo.length) {
                    permissionInfo = permissionInfo.val();
                    if (permissionInfo) {
                        permitted = JSON.parse(permissionInfo);
                    }
                }

                // Check whether the form is actionable
                if (permitted) {
                    actionable = true;
                    if (actionableInfo.length) {
                        actionableInfo = actionableInfo.val();
                        if (actionableInfo) {
                            actionable = JSON.parse(actionableInfo);
                        }
                    }
                }

                // Check blocked events
                if (permitted && actionable) {
                    actionable = this._checkBlockedEvents();
                }

                // Only enable submit if permitted and actionable
                if (permitted && actionable) {
                    buttons.reverse();
                }
            }

            var active = form.find(buttons[0]),
                disabled = form.find(buttons[1]);

            disabled.prop('disabled', true).hide().insertAfter(active);
            active.prop('disabled', false).hide().removeClass('hide').show();
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
            this._removeDetails();
            this._removeFamily();
            this._removeProfilePicture();

            // Remove blocking message
            if (this.blockingMessage) {
                this.blockingMessage.remove();
            }

            // Remove the image
            this.imageURL.val('');

            // Reset submit-button label
            $('.submit-btn').val(this.submitLabel);

            // Disable submit
            this._toggleSubmit(false);

            // Focus on label input
            var labelInput = $(this.idPrefix + '_label');
            labelInput.trigger('focus').val(labelInput.val());
        },

        /**
         * Get the currently selected event type
         */
        _getEventType: function() {

            // Read the code from event type input
            let code = this.eventType.val();
            if (!code) {
                return null;
            }

            // Find the event selector with this code
            let selector = $('a.event-type-select', this.eventTypeSelect).filter(
                function() { return $(this).data('code') == code; }
            );
            if (!selector.length) {
                return null;
            }

            // Return the event type details
            return {
                code: selector.data('code') || null,
                name: selector.data('name') || null,
                multiple: selector.data('multiple') == 'T',
            };
        },

        /**
         * Select an event type
         *
         * @param {string} code: the event type code
         * @param {string} name: the event type name
         */
        _setEventType: function(code, name) {

            // Store new event type in form
            $('input[type="hidden"][name="event"]').val(code);

            // Update event type in header
            $('.event-type-name', this.eventTypeHeader).text(name);

            this._updateEventType();

            this._updateFamilyStatus();

            // Enable submit if we have a person
            if ($(this.idPrefix + '_person__row .controls').text()) {
                this._toggleSubmit(true);
            }
        },

        /**
         * Remove the current event type selection
         */
        _clearEventType: function() {

            // Store new event type in form
            $('input[type="hidden"][name="event"]').val('');

            // Update event type in header
            this._updateEventType();

            this._toggleSubmit(false);
        },

        /**
         * Updates the event type indicator after setting/clearing the
         * current event type
         */
        _updateEventType: function() {

            let eventTypeHeader = this.eventTypeHeader,
                eventTypeSelect = this.eventTypeSelect,
                numSelectable = $('a.event-type-select', eventTypeSelect).length,
                selected = $('input[type="hidden"][name="event"]').val(),
                opts = this.options,
                label;

            if (!selected) {
                if (numSelectable > 0) {
                    label = opts.selectEventLabel;
                } else {
                    label = opts.noEventsLabel;
                }
                $('.event-type-name', eventTypeHeader).text(label);
            } else {
                eventTypeHeader.removeClass('challenge');
            }

            if (numSelectable == 0) {
                eventTypeHeader.addClass('empty').addClass('disabled');
            } else if (numSelectable == 1) {
                eventTypeHeader.removeClass('empty').addClass('disabled');
            } else {
                eventTypeHeader.removeClass('empty').removeClass('disabled');
                if (!selected) {
                    eventTypeHeader.addClass('challenge');
                }
            }
        },

        /**
         * (Re-)populates the event selector buttons
         *
         * @param {object} data: the response JSON from the event type lookup:
         *                       {
         *                        "types": [[code, name, multiple], ...],
         *                        "default": [code, name, multiple],
         *                        }
         */
        _populateEventTypes: function(data) {

            // data =

            let eventTypeSelect = this.eventTypeSelect.empty(),
                eventTypes = data.types,
                defaultEventType = data.default;

            if (!defaultEventType && eventTypes.length == 1) {
                defaultEventType = eventTypes[0];
            }

            eventTypes.forEach(function(eventType) {
                let code = eventType[0],
                    name = eventType[1],
                    multiple = eventType[2] ? 'T' : 'F',
                    btn = $('<a class="secondary button event-type-select">');

                btn.text(name)
                   .appendTo(eventTypeSelect)
                   .data({code: code, name: name, multiple: multiple});
            });

            if (defaultEventType) {
                this._setEventType(defaultEventType[0], defaultEventType[1]);
            } else {
                this._clearEventType();
            }
        },

        /**
         * Handler for organisation selector button to select the respective
         * organisation, and update selectable events
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
                eventTypeHeader = this.eventTypeHeader.addClass('disabled'),
                eventTypeName = $('.event-type-name', eventTypeHeader).hide().after(throbber),
                ajaxURL = this.options.ajaxURL + '?org=' + organisationID,
                self = this;

            $.ajaxS3({
                'url': ajaxURL,
                'type': 'GET',
                'dataType': 'json',
                'contentType': 'application/json; charset=utf-8',
                'success': function(data) {
                    self._populateEventTypes(data);
                    throbber.remove();
                    eventTypeName.show();
                    if (data.types.length > 1) {
                        eventTypeHeader.removeClass('disabled');
                    }
                },
                'error': function () {
                    throbber.remove();
                }
            });

            orgSelect.slideUp('fast');
        },

        /**
         * Handler for event selector button to set the respective event type
         *
         * @param {jQuery} btn: the selector button
         */
        _selectEventType: function(btn) {

            let code = btn.data('code'),
                name = btn.data('name');

            this._setEventType(code, name);

            // Hide event type selector
            this.eventTypeSelect.slideUp('fast');
        },

        /**
         * Bind events to generated elements (after refresh)
         */
        _bindEvents: function() {

            var form = $(this.element),
                prefix = this.idPrefix,
                ns = this.eventNamespace,
                self = this;

            let orgHeader = this.orgHeader,
                orgSelect = this.orgSelect,
                eventTypeHeader = this.eventTypeHeader,
                eventTypeSelect = this.eventTypeSelect;

            // Organisation selection
            orgHeader.on('click' + ns, function(e) {
                e.preventDefault();
                e.stopPropagation();
                if (orgHeader.hasClass('disabled')) {
                    return false;
                }
                eventTypeSelect.slideUp('fast', function() {
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

            // Event type selection
            eventTypeHeader.on('click' + ns, function(e) {
                e.preventDefault();
                e.stopPropagation();
                if (eventTypeHeader.hasClass('disabled')) {
                    return false;
                }
                orgSelect.slideUp('fast', function() {
                    if (eventTypeSelect.hasClass('hide')) {
                        eventTypeSelect.hide().removeClass('hide').slideDown('fast');
                    } else {
                        eventTypeSelect.slideToggle('fast');
                    }
                });
            });
            eventTypeSelect.on('click' + ns, 'a.event-type-select', function(e) {
                e.preventDefault();
                e.stopPropagation();
                self._selectEventType($(this));
            });

            // Show/hide picture
            this.pictureContainer.on('click' + ns, '.toggle-picture', function(e) {
                e.preventDefault();
                self._togglePicture();
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
                self._registerEvent();
            });

            // Events for the label input
            var labelInput = $(prefix + '_label');

            // Changing the label resets form
            labelInput.on('input' + ns, function(e) {
                self._clearForm(false, true);
            });

            // Key events for label field
            labelInput.on('keyup' + ns, function(e) {
                switch (e.which) {
                    case 27:
                        // Pressing ESC resets the form
                        self._clearForm();
                        break;
                    default:
                        break;
                }
            });

            // Family member selection
            form.on('change' + ns, '.member-select', function() {
                var $this = $(this),
                    trow = $this.closest('tr.family-member');
                if ($this.is(':checked')) {
                    trow.addClass('member-selected');
                } else {
                    trow.removeClass('member-selected');
                }
                self._updateSelectAll();
            });
            form.on('click' + ns, '.member-info', function(e) {
                e.preventDefault();
                var checkbox = $(this).closest('tr.family-member')
                                      .find('.member-select').first();
                if (!checkbox.prop('disabled')) {
                    checkbox.prop('checked', !checkbox.prop('checked')).change();
                }
                return false;
            });

            // Family member bulk-selection
            form.on('change' + ns, '.member-select-all', function() {
                if ($(this).is(':checked')) {
                    self._selectAll(true);
                } else {
                    self._selectAll(false);
                }
            });
            form.on('click' + ns, 'tr.family-all', function(e) {
                if (!$(e.target).hasClass('member-select-all')) {
                    e.preventDefault();
                    var checkbox = $(this).find('.member-select-all').first();
                    if (!checkbox.prop('disabled')) {
                        checkbox.prop('checked', !checkbox.prop('checked')).change();
                    }
                    return false;
                }
            });

            // Family member show picture
            form.on('click' + ns, 'button.member-show-picture', function() {
                var member = $(this).closest('.family-member'),
                    memberInfo = member.data('member');
                if (memberInfo.p) {
                    self.profilePicture = memberInfo.p;
                    self._showProfilePicture();
                } else {
                    self._removeProfilePicture();
                }
                self._updatePictureButtons();
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

            this.orgHeader.off(ns);
            this.orgSelect.off(ns);

            $('#event-type-toggle').off(ns);
            $('#event-type-selector').find('a.event-type-selector').off(ns);

            $(prefix + '_label').off(ns);

            form.find('a.cancel-action').off(ns);

            form.find('.check-btn').off(ns);

            form.find('.submit-btn').off(ns);

            form.off(ns);

            return true;
        }
    });
})(jQuery);
