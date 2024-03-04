/**
 * MRCMS Resident List (jQuery UI Widget)
 *
 * @copyright 2024 (c) AHSS
 * @license MIT
 */
(function($, undefined) {

    "use strict";

    var residentsListID = 0;

    /**
     * residentsList, instantiated on trigger button
     */
    $.widget('cr.residentsList', {

        /**
         * Default options
         */
        options: {

        },

        /**
         * Create the widget
         */
        _create: function() {

            this.id = residentsListID;
            residentsListID += 1;

            // Namespace for events
            this.eventNamespace = '.residentsList';
        },

        /**
         * Initializes the widget
         */
        _init: function() {

            let $el = $(this.element);

            this.searchField = $('.units-search input', $el);

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

            this._unbindEvents()._bindEvents();
        },

        /**
         * Filters the housing units, i.e. hides all housing units not
         * matching the search string
         *
         * @param {string} searchString: the search string
         *
         * @note: multiple comma-separated name fragments can be given
         *        in searchString, where the unit name must match at
         *        least one of them
         */
        _search: function(searchString) {

            let items = searchString.replace('\\,', '__comma__').split(','),
                applicable = [];

            items.forEach(function(item) {
                let trimmed = item.replace('__comma__', ',').trim();
                if (!trimmed) {
                    return;
                }
                applicable.push(trimmed);
            });

            let searchField = this.searchField.prop('disabled', true),
                units = $('tbody.unit', $(this.element));
            if (!applicable.length) {
                units.show();
                searchField.removeClass('filtered');
            } else {
                units.each(function() {
                    let $this = $(this),
                        name = $this.data('name').trim().toLocaleLowerCase(),
                        show = false;
                    for (let i=0; i < applicable.length; i++) {
                        if (name.indexOf(applicable[i]) != -1) {
                            show = true;
                            break;
                        }
                    }
                    if (show) {
                        $this.show();
                    } else {
                        $this.hide();
                    }
                });
                searchField.addClass('filtered');
            }
            searchField.prop('disabled', false);
        },

        /**
         * Bind events to generated elements (after refresh)
         */
        _bindEvents: function() {

            let $el = $(this.element),
                ns = this.eventNamespace,
                self = this;

            this.searchField.on('input' + ns, function() {
                let $this = $(this),
                    searchString = $this.val();

                if (searchString) {
                    searchString = searchString.trim().toLocaleLowerCase();
                }

                let timer = $this.data('filterTimeout');
                if (timer) {
                    clearTimeout(timer);
                }
                timer = setTimeout(function() { self._search(searchString); }, 750);
                $this.data('filterTimeout', timer);
            });

            $('.clear-search', $el).on('click' + ns, function() {
                self.searchField.val('').removeClass('filtered');
                $('tbody.unit', $el).show();
            });

            $('.expand-all', $el).on('click' + ns, function() {
                $('.unit', $el).removeClass('collapsed');
            });
            $('.collapse-all', $el).on('click' + ns, function() {
                $('.unit', $el).addClass('collapsed');
            });

            $('.unit-link', $el).on('click' + ns, function(e) {
                e.stopPropagation();
                return true;
            });

            $('.unit-header', $el).on('click' + ns, function(e) {
                let unit = $(this).closest('.unit');
                if (unit.hasClass('collapsed')) {
                    unit.removeClass('collapsed');
                } else {
                    unit.addClass('collapsed');
                }
            });

            return this;
        },

        /**
         * Unbind events (before refresh)
         */
        _unbindEvents: function() {

            let $el = $(this.element),
                ns = this.eventNamespace;

            $('.expand-all', $el).off(ns);
            $('.collapse-all', $el).off(ns);
            $('.unit-header', $el).off(ns);
            $('.unit-link', $el).off(ns);
            $('.clear-search', $el).off(ns);
            this.searchField.off(ns);

            return this;
        }
    });
})(jQuery);
