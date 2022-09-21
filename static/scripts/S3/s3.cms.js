/**
 * CMS Actions
 *
 * @copyright 2021 (c) Sahana Software Foundation
 * @license MIT
 */
(function($, undefined) {

    "use strict";

    $(function() {

        var path = window.location.pathname.split('/');
        var handler = function(method, confirmation) {
            return function() {
                if (!confirm || confirm(confirmation)) {
                    var recordID = $(this).attr('db_id');
                    if (recordID) {
                        var action = path.slice(0, 2).concat(['cms', 'newsletter', recordID, method]).join('/'),
                            form = document.createElement('form');
                        form.action = action;
                        form.method = 'POST';
                        form.target = '_self';
                        form.enctype = 'multipart/form-data';
                        form.style.display = 'none';

                        // Include formkey
                        var inp = document.createElement('input');
                        inp.type = "hidden";
                        inp.name = "_formkey";
                        inp.value = $(this).data('key');
                        form.append(inp);

                        // Send form
                        document.body.appendChild(form);
                        form.submit();
                    }
                }
            };
        };
        $('.newsletter-update-btn:not([disabled])').on('click', handler("update_recipients", i18n.update_recipients));
        $('.newsletter-remove-btn:not([disabled])').on('click', handler("remove_recipients", i18n.remove_recipients));
        $('.newsletter-send-btn:not([disabled])').on('click', handler("send", i18n.send_newsletter));
    });
})(jQuery);
