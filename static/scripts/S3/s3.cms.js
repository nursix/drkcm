/**
 * CMS Actions
 *
 * @copyright 2021 (c) Sahana Software Foundation
 * @license MIT
 */
(function($, undefined) {

    "use strict";

    $(document).ready(function() {

        var path = window.location.pathname.split('/');
        var handler = function(method, confirmation) {
            return function() {
                if (!confirm || confirm(confirmation)) {
                    var recordID = $(this).attr('db_id');
                    if (recordID) {
                        var action = path.slice(0, 2).concat(['cms', 'newsletter', recordID, method]).join('/'),
                            form = document.createElement('form');
                        // TODO include formkey in post-data
                        var inp = document.createElement('input');
                        inp.type = "hidden";
                        inp.name = "_formkey";
                        inp.value = $(this).data('key');
                        form.append(inp);

                        form.action = action;
                        form.method = 'POST';
                        form.target = '_self';
                        form.enctype = 'multipart/form-data';
                        form.style.display = 'none';
                        document.body.appendChild(form);
                        form.submit();
                    }
                }
            };
        };
        $('.newsletter-send-btn').on('click', handler("send", i18n.send_newsletter));
    });
})(jQuery);
