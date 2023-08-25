/**
 * Used by the Autocomplete Widgets (core/ui/widgets)
 * This script is in Static to allow caching
 * Dynamic constants (e.g. Internationalised strings) are set in server-generated script
 */

// Module pattern to hide internal vars
(function () {

    S3.autocomplete = {};

    /**
     * S3AutocompleteWidget
     */
    S3.autocomplete.normal = function(fieldname, module, resourcename, input, filter, link, postprocess, delay, min_length) {
        var dummy = 'dummy_' + input;
        var dummy_input = $('#' + dummy);

        if (!dummy_input.length) {
            return;
        }

        var url = S3.Ap.concat('/', module, '/', resourcename, '/search_ac.json?field=', fieldname);
        if (filter) {
            url += '&' + filter;
        }
        if (link) {
            url += '&link=' + link;
        }

        var real_input = $('#' + input).hide(),
            value = real_input.val(),
            existing;
        if (value) {
            // Store existing data in case of cancel
            existing = {value: value, label: dummy_input.val()};
        }
        real_input.data('existing', existing);
        // Have the URL editable after setup
        real_input.data('url', url);
        /*
        if (real_input.parent().hasClass('controls')) {
            // Bootstrap or Foundation
            var create = real_input.next().find('.s3_add_resource_link');
        } else {
            // Other Theme
            var create = real_input.parent().next().find('.s3_add_resource_link');
        }
        */

        var throbber = $('#' + dummy + '_throbber');

        // Optional args
        if (delay == undefined) {
            delay = 800;
        }
        if (min_length == undefined) {
            min_length = 2;
        }
        var datastore = {
            val: dummy_input.val(),
            accept: false
        };
        dummy_input.autocomplete({
            delay: delay,
            minLength: min_length,
            source: function(request, response) {
                // Patch the source so that we can handle No Matches
                $.ajax({
                    url: real_input.data('url'),
                    data: {
                        term: request.term
                    }
                }).done(function (data) {
                    var extra = {id: 0};
                    if (data.length == 0) {
                        // No Match
                        real_input.val('').trigger('change');

                        // New Entry?
                        //if (create.length) {
                            // Open popup to create new entry
                            // @ToDo: prepopulate name field
                        //    create.trigger('click');
                        //} else {

                        // No link to create new (e.g. no permission to do so)
                        extra[fieldname] = i18n.no_matching_records;
                        data.push(extra);
                        //}
                    } else {
                        extra[fieldname] = i18n.none_of_the_above;
                        data.push(extra);
                    }
                    response(data);
                });
            },
            search: function(event, ui) {
                throbber.removeClass('hide').show();
                return true;
            },
            response: function(event, ui, content) {
                throbber.hide();
                return content;
            },
            focus: function(event, ui) {
                return false;
            },
            select: function(event, ui) {
                var item = ui.item;
                if (item.id) {
                    dummy_input.val(item[fieldname]);
                    real_input.val(item.id).trigger('change');
                    // Update existing, so blur does not remove
                    // the selection again:
                    existing = {value: item.id,
                                label: item[fieldname]
                                };
                } else {
                    // No Match & no ability to create new
                    dummy_input.val('');
                    real_input.val('').trigger('change');
                }
                if (postprocess) {
                    // postprocess has to be able to handle the 'no match' option
                    eval(postprocess);
                }
                return false;
            }
        })
        .data('ui-autocomplete')._renderItem = function(ul, item) {
            var label;
            if (item.label) {
                // No Match or Too Many Results
                label = item.label;
            } else {
                label = item[fieldname];
            }
            return $('<li>').data('item.autocomplete', item)
                            .append('<a>' + label + '</a>')
                            .appendTo(ul);
        };
        dummy_input.on('blur', function() {
            if (existing && existing.label != dummy_input.val()) {
                // New Entry - without letting AC complete (e.g. tab out)
                real_input.val('').trigger('change');
                // @ToDo: Something better!
                //if (create.length) {
                    // Open popup to create new entry
                    // @ToDo: prepopulate name field
                //    create.trigger('click');
                //}
            }
        });
    };

    /**
     * S3GenericAutocompleteTemplate
     * - not currently used
     */
    S3.autocomplete.generic = function(url, input, postprocess, delay, min_length) {
        var dummy = 'dummy_' + input;
        var dummy_input = $('#' + dummy);

        if (!dummy_input.length) {
            return;
        }

        var real_input = $('#' + input).hide(),
            value = real_input.val(),
            existing;
        if (value) {
            // Store existing data in case of cancel
            existing = {value: value, name: dummy_input.val()};
        }
        real_input.data('existing', existing);
        // Have the URL editable after setup
        real_input.data('url', url);
        /*
        if (real_input.parent().hasClass('controls')) {
            // Bootstrap or Foundation
            var create = real_input.next().find('.s3_add_resource_link');
        } else {
            // Other Theme
            var create = real_input.parent().next().find('.s3_add_resource_link');
        }
        */

        var throbber = $('#' + dummy + '_throbber');

        if (delay == undefined) {
            delay = 800;
        }
        if (min_length == undefined) {
            min_length = 2;
        }
        dummy_input.autocomplete({
            delay: delay,
            minLength: min_length,
            source: function(request, response) {
                // Patch the source so that we can handle No Matches
                $.ajax({
                    url: real_input.data('url'),
                    data: {
                        term: request.term
                    }
                }).done(function (data) {
                    if (data.length == 0) {
                        // No Match
                        real_input.val('').trigger('change');
                        // New Entry?
                        //if (create.length) {
                            // Open popup to create new entry
                            // @ToDo: prepopulate name field
                        //    create.trigger('click');
                        //} else {
                            // No link to create new (e.g. no permission to do so)
                            data.push({
                                id: 0,
                                value: '',
                                label: i18n.no_matching_records
                            });
                        //}
                    } else {
                        data.push({
                            id: 0,
                            value: '',
                            label: i18n.none_of_the_above
                        });
                    }
                    response(data);
                });
            },
            search: function(event, ui) {
                throbber.removeClass('hide').show();
                return true;
            },
            response: function(event, ui, content) {
                throbber.hide();
                return content;
            },
            focus: function(event, ui) {
                return false;
            },
            select: function(event, ui) {
                var item = ui.item;
                var id = item.id;
                if (id) {
                    dummy_input.val(item.name);
                    real_input.val(id).trigger('change');
                    // Update existing, so blur does not remove
                    // the selection again:
                    existing = {value: item.id,
                                name: item.name
                                };
                } else {
                    // No Match & no ability to create new
                    dummy_input.val('');
                    real_input.val('').trigger('change');
                }
                if (postprocess) {
                    // postprocess has to be able to handle the 'no match' option
                    eval(postprocess);
                }
                return false;
            }
        })
        .data('ui-autocomplete')._renderItem = function(ul, item) {
            var label;
            if (item.label) {
                // No Match
                label = item.label;
            } else if (item.matchString) {
                // back-ends upgraded like org_search_ac
                label = item.matchString + '<b>' + item.nextString + '</b>';
                if (item.context) {
                    label += ' - ' + item.context;
                }
            } else {
                // Legacy AC
                label = item.name;
            }
            return $('<li>').data('item.autocomplete', item)
                            .append('<a>' + label + '</a>')
                            .appendTo(ul);
        };
        dummy_input.on('blur', function() {
            if (existing && existing.name != dummy_input.val()) {
                // New Entry - without letting AC complete (e.g. tab out)
                real_input.val('').trigger('change');
                // @ToDo: Something better!
                //if (create.length) {
                    // Open popup to create new entry
                    // @ToDo: prepopulate name field
                //    create.trigger('click');
                //}
            }
        });
    };

    /*
     * Represent a Location
     */
    var represent_location = function(item) {
        if (item.label != undefined) {
            // No Match or too many results
            return item.label;
        }
        var name;
        if (item.name) {
            if (item.match_type) {
                if (item.next_string) {
                    name = item.match_string + '<b>' + item.next_string + '</b>';
                }
                else {
                    name = item.match_string;
                }
            }
            else {
                name = item.name;
            }
        } else {
            // Site contents
            name = '';
        }
        if (item.addr) {
            if (name) {
                name += ', ' + item.addr;
            } else {
                name = item.addr;
            }
        }
        if (item.L5) {
            if (name) {
                name += ', ' + item.L5;
            } else {
                name = item.L5;
            }
        }
        if (item.L4) {
            if (name) {
                name += ', ' + item.L4;
            } else {
                name = item.L4;
            }
        }
        if (item.L3) {
            if (name) {
                name += ', ' + item.L3;
            } else {
                name = item.L3;
            }
        }
        if (item.L2) {
            if (name) {
                name += ', ' + item.L2;
            } else {
                name = item.L2;
            }
        }
        if (item.L1) {
            if (name) {
                name += ', ' + item.L1;
            } else {
                name = item.L1;
            }
        }
        if (item.L0) {
            if (name) {
                name += ', ' + item.L0;
            } else {
                name = item.L0;
            }
        }
        return name;
    };

    /**
     * S3LocationAutocompleteWidget
     * - uses name_l10n & Lx
     */
    S3.autocomplete.location = function(input, level, min_length, delay, postprocess) {
        var dummy = 'dummy_' + input;
        var dummy_input = $('#' + dummy);

        if (!dummy_input.length) {
            return;
        }

        var represent = represent_location;
        var url = S3.Ap.concat('/gis/location/search_ac.json');

        var real_input = $('#' + input).hide(),
            value = real_input.val(),
            existing;
        if (value) {
            // Store existing data in case of cancel
            existing = {value: value, name: dummy_input.val()};
        }
        real_input.data('existing', existing);
        // Have the URL editable after setup
        real_input.data('url', url);
        /*
        if (real_input.parent().hasClass('controls')) {
            // Bootstrap or Foundation
            var create = real_input.next().find('.s3_add_resource_link');
        } else {
            // Other Theme
            var create = real_input.parent().next().find('.s3_add_resource_link');
        }
        */

        var throbber = $('#' + dummy + '_throbber');

        // Optional args
        if (level) {
            url += '?level=' + level;
        }
        if (delay == undefined) {
            delay = 800;
        }
        if (min_length == undefined) {
            min_length = 2;
        }
        dummy_input.autocomplete({
            delay: delay,
            minLength: min_length,
            source: function(request, response) {
                // Patch the source so that we can handle No Matches
                $.ajax({
                    url: real_input.data('url'),
                    data: {
                        term: request.term
                    }
                }).done(function (data) {
                   if (data.length == 0) {
                        // No Match
                        real_input.val('').trigger('change');
                        // New Entry?
                        //if (create.length) {
                            // Open popup to create new entry
                            // @ToDo: prepopulate name field
                        //    create.trigger('click');
                        //} else {
                            // No link to create new (e.g. no permission to do so)
                            data.push({
                                id: 0,
                                value: '',
                                label: i18n.no_matching_records
                            });
                        //}
                    } else {
                        data.push({
                            id: 0,
                            value: '',
                            label: i18n.none_of_the_above
                        });
                    }
                    response(data);
                });
            },
            search: function(event, ui) {
                throbber.removeClass('hide').show();
                return true;
            },
            response: function(event, ui, content) {
                throbber.hide();
                return content;
            },
            focus: function(event, ui) {
                return false;
            },
            select: function(event, ui) {
                var item = ui.item;
                if (item.id) {
                    dummy_input.val(ui.item.name);
                    real_input.val(item.id).trigger('change');
                    // Update existing, so blur does not remove
                    // the selection again:
                    existing = {value: item.id,
                                name: ui.item.name
                                };
                } else {
                    // No Match & no ability to create new
                    dummy_input.val('');
                    real_input.val('').trigger('change');
                }
                if (postprocess) {
                    // postprocess has to be able to handle the 'no match' option
                    eval(postprocess);
                }
                return false;
            }
        })
        .data('ui-autocomplete')._renderItem = function(ul, item) {
            var label = represent_location(item);
            return $('<li>').data('item.autocomplete', item)
                            .append('<a>' + label + '</a>')
                            .appendTo(ul);
        };
        dummy_input.on('blur', function() {
            if (existing && existing.name != dummy_input.val()) {
                // New Entry - without letting AC complete (e.g. tab out)
                real_input.val('').trigger('change');
                // @ToDo: Something better!
                //if (create.length) {
                    // Open popup to create new entry
                    // @ToDo: prepopulate name field
                //    create.trigger('click');
                //}
            }
        });
    };

    /*
     * Represent a Person or Human Resource
     */
    var represent_person = function(item) {
        if (item.label != undefined) {
            // No Match or too many results
            return item.label;
        }
        var name;
        if (item.org || item.job) {
            // Represent the Person as an HR
            name = represent_hr(item);
        } else {
            name = item.name;
        }
        return name;
    };

    /**
     * S3PersonAutocompleteWidget & hence PersonSelector
     * - used first/middle/last, but anything non-generic left?
     */
    S3.autocomplete.person = function(controller, fn, input, ajax_filter, postprocess, delay, min_length) {
        var dummy = 'dummy_' + input;
        var dummy_input = $('#' + dummy);

        if (!dummy_input.length) {
            return;
        }

        var represent = represent_person;
        var url = S3.Ap.concat('/', controller, '/', fn, '/search_ac.json');

        if (ajax_filter) {
            url += "?" + ajax_filter;
        }

        var real_input = $('#' + input).hide(),
            value = real_input.val(),
            existing;
        if (value) {
            // Store existing data in case of cancel
            existing = {value: value, name: dummy_input.val()};
        }
        real_input.data('existing', existing);
        // Have the URL editable after setup
        real_input.data('url', url);
        /*
        if (real_input.parent().hasClass('controls')) {
            // Bootstrap or Foundation
            var create = real_input.next().find('.s3_add_resource_link');
        } else {
            // Other Theme
            var create = real_input.parent().next().find('.s3_add_resource_link');
        }
        */

        var throbber = $('#' + dummy + '_throbber');

        // Optional args
        if (delay == undefined) {
            delay = 800;
        }
        if (min_length == undefined) {
            min_length = 2;
        }
        dummy_input.autocomplete({
            delay: delay,
            minLength: min_length,
            source: function(request, response) {
                // Patch the source so that we can handle No Matches
                $.ajax({
                    url: real_input.data('url'),
                    data: {
                        term: request.term
                    }
                }).done(function (data) {
                    if (data.length == 0) {
                        // No Match
                        real_input.val('').trigger('change');
                        // New Entry?
                        //if (create.length) {
                            // Open popup to create new entry
                            // @ToDo: prepopulate name field
                        //    create.trigger('click');
                        //} else {
                            // No link to create new (e.g. no permission to do so)
                            data.push({
                                id: 0,
                                value: '',
                                label: i18n.no_matching_records
                            });
                        //}
                    } else {
                        data.push({
                            id: 0,
                            value: '',
                            label: i18n.none_of_the_above
                        });
                    }
                    response(data);
                });
            },
            search: function(event, ui) {
                throbber.removeClass('hide').show();
                return true;
            },
            response: function(event, ui, content) {
                throbber.hide();
                return content;
            },
            focus: function(event, ui) {
                return false;
            },
            select: function(event, ui) {
                var item = ui.item;
                if (item.id) {
                    var name = represent(item);
                    dummy_input.val(name);
                    real_input.val(item.id).trigger('change');
                    // Update existing, so blur does not remove
                    // the selection again:
                    existing = {value: item.id,
                                name: name
                                };
                } else {
                    // No Match & no ability to create new
                    dummy_input.val('');
                    real_input.val('').trigger('change');
                }
                if (postprocess) {
                    // postprocess has to be able to handle the 'no match' option
                    eval(postprocess);
                }
                return false;
            }
        }).data('ui-autocomplete')._renderItem = function(ul, item) {
            var label = represent(item);
            return $('<li>').data('item.autocomplete', item)
                            .append('<a>' + label + '</a>')
                            .appendTo(ul);
        };
        dummy_input.on('blur', function() {
            if (existing && existing.name != dummy_input.val()) {
                // New Entry - without letting AC complete (e.g. tab out)
                real_input.val('').trigger('change');
                // @ToDo: Something better!
                //if (create.length) {
                    // Open popup to create new entry
                    // @ToDo: prepopulate name field
                //    create.trigger('click');
                //}
            }
        });
    };

    /**
     * S3PentityAutocompleteWidget
     */
    S3.autocomplete.pentity = function(controller, fn, input, postprocess, delay, min_length, types) {
        var dummy = 'dummy_' + input;
        var dummy_input = $('#' + dummy);

        if (!dummy_input.length) {
            return;
        }

        var url = S3.Ap.concat('/', controller, '/', fn, '/search_ac.json');

        var real_input = $('#' + input).hide(),
            value = real_input.val(),
            existing;
        if (value) {
            // Store existing data in case of cancel
            existing = {value: value, name: dummy_input.val()};
        }
        real_input.data('existing', existing);
        // Have the URL editable after setup
        real_input.data('url', url);
        /*
        if (real_input.parent().hasClass('controls')) {
            // Bootstrap or Foundation
            var create = real_input.next().find('.s3_add_resource_link');
        } else {
            // Other Theme
            var create = real_input.parent().next().find('.s3_add_resource_link');
        }
        */

        var throbber = $('#' + dummy + '_throbber');

        // Optional args
        if (delay == undefined) {
            delay = 800;
        }
        if (min_length == undefined) {
            min_length = 2;
        }

        dummy_input.autocomplete({
            delay: delay,
            minLength: min_length,
            source: function(request, response) {
                url = real_input.data('url');
                if (types) {
                    url += '?types=' + types;
                }

                // Patch the source so that we can handle No Matches
                $.ajax({
                    url: url,
                    data: {
                        term: request.term
                    }
                }).done(function (data) {
                    if (data.length == 0) {
                        // No Match
                        real_input.val('').trigger('change');
                        // New Entry?
                        //if (create.length) {
                            // Open popup to create new entry
                            // @ToDo: prepopulate name field
                        //    create.trigger('click');
                        //} else {
                            // No link to create new (e.g. no permission to do so)
                            data.push({
                                id: 0,
                                value: '',
                                label: i18n.no_matching_records
                            });
                        //}
                    } else {
                        data.push({
                            id: 0,
                            value: '',
                            label: i18n.none_of_the_above
                        });
                    }
                    response(data);
                });
            },
            search: function(event, ui) {
                throbber.removeClass('hide').show();
                return true;
            },
            response: function(event, ui, content) {
                throbber.hide();
                return content;
            },
            focus: function(event, ui) {
                return false;
            },
            select: function(event, ui) {
                var item = ui.item;
                if (item.id) {
                    dummy_input.val(item.name);
                    real_input.val(item.id).trigger('change');
                    // Update existing, so blur does not remove
                    // the selection again:
                    existing = {value: item.id,
                                name: item.name
                                };
                } else {
                    // No Match & no ability to create new
                    dummy_input.val('');
                    real_input.val('').trigger('change');
                }
                if (postprocess) {
                    // postprocess has to be able to handle the 'no match' option
                    eval(postprocess);
                }
                return false;
            }
        })
        .data('ui-autocomplete')._renderItem = function(ul, item) {
            var label;
            if (item.label) {
                // No Match
                label = item.label;
            } else {
                label = item.name;
            }
            return $('<li>').data('item.autocomplete', item)
                            .append('<a>' + label + '</a>')
                            .appendTo(ul);
        };
        dummy_input.on('blur', function() {
            if (existing && existing.name != dummy_input.val()) {
                // New Entry - without letting AC complete (e.g. tab out)
                real_input.val('').trigger('change');
                // @ToDo: Something better!
                //if (create.length) {
                    // Open popup to create new entry
                    // @ToDo: prepopulate name field
                //    create.trigger('click');
                //}
            }
        });
    };

    /*
     * Represent a Human Resource
     */
    var represent_hr = function(item) {
        if (item.label != undefined) {
            // No Match or too many results
            return item.label;
        }
        var name = item.name; // Person
        var org = item.org;
        var job = item.job;
        if (org || job) {
            if (job) {
                name += ' (' + job;
                if (org) {
                    name += ', ' + org;
                }
                name += ')';
            } else {
                name += ' (' + org + ')';
            }
        }
        return name;
    };

    /**
     * S3HumanResourceAutocompleteWidget
     * - uses name, organisation & job role
     */
    S3.autocomplete.hrm = function(group, input, postprocess, delay, min_length) {
        var dummy = 'dummy_' + input;
        var dummy_input = $('#' + dummy);

        if (!dummy_input.length) {
            return;
        }

        var url;
        if (group == 'staff') {
            // Search Staff
            url = S3.Ap.concat('/hrm/hr_search/search_ac?group=staff');
        } else if (group == 'volunteer') {
            // Search Volunteers
            url = S3.Ap.concat('/vol/hr_search/search_ac');
        } else if (group == 'deploy') {
            // Search Deployables
            url = S3.Ap.concat('/deploy/hr_search/search_ac');
        } else {
            // Search all HRs
            url = S3.Ap.concat('/hrm/hr_search/search_ac');
        }

        var real_input = $('#' + input).hide(),
            value = real_input.val(),
            existing;
        if (value) {
            // Store existing data in case of cancel
            existing = {value: value, name: dummy_input.val()};
        }
        real_input.data('existing', existing);
        // Have the URL editable after setup
        real_input.data('url', url);
        /*
        if (real_input.parent().hasClass('controls')) {
            // Bootstrap or Foundation
            var create = real_input.next().find('.s3_add_resource_link');
        } else {
            // Other Theme
            var create = real_input.parent().next().find('.s3_add_resource_link');
        }
        */

        var throbber = $('#' + dummy + '_throbber');

        // Optional args
        if (delay == undefined) {
            delay = 800;
        }
        if (min_length == undefined) {
            min_length = 2;
        }
        dummy_input.autocomplete({
            delay: delay,
            minLength: min_length,
            source: function(request, response) {
                // Patch the source so that we can handle No Matches
                $.ajax({
                    url: real_input.data('url'),
                    data: {
                        term: request.term
                    }
                }).done(function (data) {
                    if (data.length == 0) {
                        // No Match
                        real_input.val('').trigger('change');
                        // New Entry?
                        //if (create.length) {
                            // Open popup to create new entry
                            // @ToDo: prepopulate name field
                        //    create.trigger('click');
                        //} else {
                            // No link to create new (e.g. no permission to do so)
                            data.push({
                                id: 0,
                                value: '',
                                label: i18n.no_matching_records
                            });
                        //}
                    } else {
                        data.push({
                            id: 0,
                            value: '',
                            label: i18n.none_of_the_above
                        });
                    }
                    response(data);
                });
            },
            search: function(event, ui) {
                throbber.removeClass('hide').show();
                return true;
            },
            response: function(event, ui, content) {
                throbber.hide();
                return content;
            },
            focus: function(event, ui) {
                return false;
            },
            select: function(event, ui) {
                var item = ui.item;
                if (item.id) {
                    var name = represent_person(item);
                    dummy_input.val(name);
                    real_input.val(item.id).trigger('change');
                    // Update existing, so blur does not remove
                    // the selection again:
                    existing = {value: item.id,
                                name: name
                                };
                } else {
                    // No Match & no ability to create new
                    dummy_input.val('');
                    real_input.val('').trigger('change');
                }
                if (postprocess) {
                    // postprocess has to be able to handle the 'no match' option
                    eval(postprocess);
                }
                return false;
            }
        })
        .data('ui-autocomplete')._renderItem = function(ul, item) {
            var label = represent_hr(item);
            return $('<li>').data('item.autocomplete', item)
                            .append('<a>' + label + '</a>')
                            .appendTo(ul);
        };
        dummy_input.on('blur', function() {
            if (existing && existing.name != dummy_input.val()) {
                // New Entry - without letting AC complete (e.g. tab out)
                real_input.val('').trigger('change');
                // @ToDo: Something better!
                //if (create.length) {
                    // Open popup to create new entry
                    // @ToDo: prepopulate name field
                //    create.trigger('click');
                //}
            }
        });
    };

    /*
     * Represent an Organisation
     */
    var represent_org = function(item) {
        if (item.label != undefined) {
            // No Match or too many results
            return item.label;
        }

        var label;
        if (item.matchString) {
            // org_search_ac
            // http://eden.sahanafoundation.org/ticket/1412
            if (item.match == 'acronym') {
                label = item.name;
                if (item.parent) {
                    label = item.parent + ' > ' + label;
                }
                label += ' - ' + item.matchString + '<b>' + item.nextString + '</b>';
            } else {
                // Name match
                label = item.matchString + '<b>' + item.nextString + '</b>';
                if (item.parent) {
                    label = item.parent + ' > ' + label;
                } else if (item.acronym) {
                    label += ' - ' + item.acronym;
                }
            }
        } else {
            // Non org_search_ac (no cases yet)
            label = item.name;
            if (item.parent) {
                label = item.parent + ' > ' + item.name;
            } else if (item.acronym) {
                label += ' (' + item.acronym + ')';
            }
        }
        return label;
    };

    /**
     * S3OrganisationAutocompleteWidget
     */
    S3.autocomplete.org = function(input, postprocess, delay, min_length) {
        var dummy = 'dummy_' + input;
        var dummy_input = $('#' + dummy);

        if (!dummy_input.length) {
            return;
        }

        var url = S3.Ap.concat('/org/organisation/search_ac.json');

        var real_input = $('#' + input).hide(),
            value = real_input.val(),
            existing;
        if (value) {
            // Store existing data in case of cancel
            existing = {value: value, name: dummy_input.val()};
        }
        real_input.data('existing', existing);
        // Have the URL editable after setup (e.g. to Filter by Organisation)
        real_input.data('url', url);
        /*
        if (real_input.parent().hasClass('controls')) {
            // Bootstrap or Foundation
            var create = real_input.next().find('.s3_add_resource_link');
        } else {
            // Other Theme
            var create = real_input.parent().next().find('.s3_add_resource_link');
        }
        */

        var throbber = $('#' + dummy + '_throbber');

        // Optional args
        if (delay == undefined) {
            delay = 800;
        }
        if (min_length == undefined) {
            min_length = 2;
        }
        dummy_input.autocomplete({
            delay: delay,
            minLength: min_length,
            source: function(request, response) {
                // Patch the source so that we can handle No Matches
                $.ajax({
                    url: real_input.data('url'),
                    data: {
                        term: request.term
                    }
                }).done(function (data) {
                    if (data.length == 0) {
                        // No Match
                        real_input.val('').trigger('change');
                        // New Entry?
                        //if (create.length) {
                            // Open popup to create new entry
                            // Prepopulate name field
                        //    var old_url = create.attr('href');
                        //    var new_url = old_url + '&name=' + dummy_input.val();
                        //    create.attr('href', new_url);
                        //    create.trigger('click');
                            // Restore URL
                        //    create.attr('href', old_url);
                        //} else {
                            // No link to create new (e.g. no permission to do so)
                            data.push({
                                id: 0,
                                value: '',
                                label: i18n.no_matching_records
                            });
                        //}
                    } else {
                        data.push({
                            id: 0,
                            value: '',
                            label: i18n.none_of_the_above
                        });
                    }
                    response(data);
                });
            },
            search: function(event, ui) {
                throbber.removeClass('hide').show();
                return true;
            },
            response: function(event, ui, content) {
                throbber.hide();
                return content;
            },
            focus: function(event, ui) {
                return false;
            },
            select: function(event, ui) {
                var item = ui.item;
                if (item.id) {
                    dummy_input.val(item.name);
                    real_input.val(item.id).trigger('change');
                    // Update existing, so blur does not remove
                    // the selection again:
                    existing = {value: item.id,
                                name: item.name
                                };
                } else {
                    // No Match & no ability to create new
                    dummy_input.val('');
                    real_input.val('').trigger('change');
                }
                if (postprocess) {
                    // postprocess has to be able to handle the 'no match' option
                    eval(postprocess);
                }
                return false;
            }
        })
        .data('ui-autocomplete')._renderItem = function(ul, item) {
            var label = represent_org(item);
            return $('<li>').data('item.autocomplete', item)
                            .append('<a>' + label + '</a>')
                            .appendTo(ul);
        };
        dummy_input.on('blur', function() {
            if (existing && existing.name != dummy_input.val()) {
                // New Entry - without letting AC complete (e.g. tab out)
                real_input.val('').trigger('change');
                // @ToDo: Something better!
                //if (create.length) {
                    // Open popup to create new entry
                    // @ToDo: Prepopulate name field
                //    create.trigger('click');
                //}
            }
        });
    };

    /*
     * Represent a Site
     */
    var represent_site = function(item) {
        if (item.label != undefined) {
            // No Match or too many results
            return item.label;
        }

        var label,
            instance_type,
            org,
            Lx,
            addr,
            context;
        if (item.match_type) {
            // Use next gen site autocomplete
            label = '<b>' + item.match_string + '</b>';
            if (item.pre_string) {
                // i.e. Address match
                label = item.pre_string + label;
            }
            if (item.next_string) {
                label += item.next_string;
            }
            if (item.match_type == 'name') {
                // Provide the rest of the data as context
                addr = item.addr;
                Lx = item.L4 || item.L3 || item.L2 || item.L1;
                if (addr || Lx) {
                    if (addr) {
                        label += ' (' + addr;
                        if (Lx) {
                            label += ', ' + Lx;
                        }
                        label += ')';
                    } else {
                        label += ' (' + Lx + ')';
                    }
                }
                org = item.org;
                instance_type = item.instance_type;
                if (org || instance_type) {
                    if (instance_type) {
                        label += ' (' + S3.org_site_types[instance_type];
                        if (org) {
                            label += ', ' + org;
                        }
                        label += ')';
                    } else {
                        label += ' (' + org + ')';
                    }
                }
            } else if (item.match_type == 'addr') {
                // Provide the rest of the data as context
                label = item.name + ' (' + label;
                Lx = item.L4 || item.L3 || item.L2 || item.L1;
                if (Lx) {
                    label += ', ' + Lx + ')';
                } else {
                    label += ')';
                }
                org = item.org;
                instance_type = item.instance_type;
                if (org || instance_type) {
                    if (instance_type) {
                        label += ' (' + S3.org_site_types[instance_type];
                        if (org) {
                            label += ', ' + org;
                        }
                        label += ')';
                    } else {
                        label += ' (' + org + ')';
                    }
                }
            } else if (item.match_type == 'org') {
                // Provide the rest of the data as context
                context = item.name;
                addr = item.addr;
                Lx = item.L4 || item.L3 || item.L2 || item.L1;
                if (addr || Lx) {
                    if (addr) {
                        context += ' (' + addr;
                        if (Lx) {
                            context += ', ' + Lx;
                        }
                        context += ')';
                    } else {
                        context += ' (' + Lx + ')';
                    }
                }
                instance_type = item.instance_type;
                if (instance_type) {
                    label = context + ' (' + S3.org_site_types[instance_type] + ', ' + label + ')';
                } else {
                    label = context + ' (' + label + ')';
                }
            } else {
                // Match = Lx
                // Provide the rest of the data as context
                context = item.name;
                addr = item.addr;
                if (addr) {
                    label = context + ' (' + addr + ', ' + label + ')';
                } else {
                    label = context + ' (' + label + ')';
                }
                org = item.org;
                instance_type = item.instance_type;
                if (org || instance_type) {
                    if (instance_type) {
                        label += ' (' + S3.org_site_types[instance_type];
                        if (org) {
                            label += ', ' + org;
                        }
                        label += ')';
                    } else {
                        label += ' (' + org + ')';
                    }
                }
            }
        } else {
            // Fallback
            label = item.name;
        }
        return label;
    };

    /**
     * S3SiteAutocompleteWidget
     * - uses name & type
     */
    S3.autocomplete.site = function(input, postprocess, delay, min_length) {
        var dummy = 'dummy_' + input;
        var dummy_input = $('#' + dummy);

        if (!dummy_input.length) {
            return;
        }

        var url = S3.Ap.concat('/org/site/search_ac.json');

        var real_input = $('#' + input).hide(),
            value = real_input.val(),
            existing;
        if (value) {
            // Store existing data in case of cancel
            existing = {value: value, name: dummy_input.val()};
        }
        real_input.data('existing', existing);
        // Have the URL editable after setup (e.g. to Filter by Organisation)
        real_input.data('url', url);
        /*
        if (real_input.parent().hasClass('controls')) {
            // Bootstrap or Foundation
            var create = real_input.next().find('.s3_add_resource_link');
        } else {
            // Other Theme
            var create = real_input.parent().next().find('.s3_add_resource_link');
        }
        */

        var throbber = $('#' + dummy + '_throbber');

        // Optional args
        if (delay == undefined) {
            delay = 800;
        }
        if (min_length == undefined) {
            min_length = 2;
        }
        dummy_input.autocomplete({
            delay: delay,
            minLength: min_length,
            source: function(request, response) {
                // Patch the source so that we can handle No Matches
                $.ajax({
                    url: real_input.data('url'),
                    data: {
                        term: request.term
                    }
                }).done(function (data) {
                    if (data.length == 0) {
                        // No Match
                        real_input.val('').trigger('change');
                        // New Entry?
                        //if (create.length) {
                            // Open popup to create new entry
                            // Prepopulate name field
                        //    var old_url = create.attr('href');
                        //    var new_url = old_url + '&name=' + dummy_input.val();
                        //    create.attr('href', new_url);
                        //    create.trigger('click');
                            // Restore URL
                        //    create.attr('href', old_url);
                        //} else {
                            // No link to create new (e.g. no permission to do so)
                            data.push({
                                id: 0,
                                value: '',
                                label: i18n.no_matching_records
                            });
                        //}
                    } else {
                        data.push({
                            id: 0,
                            value: '',
                            label: i18n.none_of_the_above
                        });
                    }
                    response(data);
                });
            },
            search: function(event, ui) {
                throbber.removeClass('hide').show();
                return true;
            },
            response: function(event, ui, content) {
                throbber.hide();
                return content;
            },
            focus: function(event, ui) {
                return false;
            },
            select: function(event, ui) {
                var item = ui.item;
                if (item.id) {
                    dummy_input.val(item.name);
                    real_input.val(item.id).trigger('change');
                    // Update existing, so blur does not remove
                    // the selection again:
                    existing = {value: item.id,
                                name: item.name
                                };
                } else {
                    // No Match & no ability to create new
                    dummy_input.val('');
                    real_input.val('').trigger('change');
                }
                if (postprocess) {
                    // postprocess has to be able to handle the 'no match' option
                    eval(postprocess);
                }
                return false;
            }
        })
        .data('ui-autocomplete')._renderItem = function(ul, item) {
            var label = represent_site(item);
            return $('<li>').data('item.autocomplete', item)
                            .append('<a>' + label + '</a>')
                            .appendTo(ul);
        };
        dummy_input.on('blur', function() {
            if (existing && existing.name != dummy_input.val()) {
                // New Entry - without letting AC complete (e.g. tab out)
                real_input.val('').trigger('change');
                // @ToDo: Something better!
                //if (create.length) {
                    // Open popup to create new entry
                    // @ToDo: Prepopulate name field
                //    create.trigger('click');
                //}
            }
        });
    };

}());
// END ========================================================================
