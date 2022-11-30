/**
 * Commission verification - client-side logic
 *
 * @copyright 2022 (c) AHSS
 * @license MIT
 */
(function($, undefined) {

    "use strict";

    var reset = function() {
        $('.data, .loading').hide();
        $('.data-empty, .scan').removeClass('hide').show();
        $('#verification-result').css({color: ''});
        $('#verification-result, #provider-id, #provider-name, #start-date, #end-date, #status, #status-date').text('--');
    };

    var showPending = function() {
        $('.data, .scan').hide();
        $('.data-empty, .loading').removeClass('hide').show();
    };

    var showResult = function() {
        $('.data-empty, .loading').hide();
        $('.data, .scan').removeClass('hide').show();
    };

    var verify = function() {

        showPending();

        var input = $('#vcode'),
            ajaxURL = input.data('url'),
            vCode = input.val();

        if (!ajaxURL || !vCode) {
            reset();
            return;
        }

        // Send code to backend via Ajax
        $.ajaxS3({
            'url': ajaxURL,
            'type': 'POST',
            'dataType': 'json',
            'data': JSON.stringify({vcode: vCode}),
            'contentType': 'application/json; charset=utf-8',
            'timeout' : 5000,
            'retryLimit': 0,
            'success': function(response) {

                // Prefer represented over raw data
                var data = response.repr;
                if (data === undefined) {
                    data = response.data;
                }

                // Update the data display
                $('#verification-result').text(response.signature);

                if (response.signature == 'VALID') {
                    $('#verification-result').css({color: ''});
                    $('#provider-id').html(data.organisation_id);
                    $('#provider-name').html(data.organisation);
                    $('#start-date').html(data.start);
                    $('#end-date').html(data.end);
                    $('#status').html(data.status);
                    $('#status-date').html(data.status_date);
                } else {
                    $('#verification-result').css({color: '#db1b1b'});
                    $('#provider-id, #provider-name, #start-date, #end-date, #status, #status-date').text('--');
                }
                showResult();
            },
            'error': function () {
                // ajaxS3 does the error reporting
                reset();
            }
        });
    };

    $(function() {

        reset();
        $('.qrscan-btn').off('.verification').on('click.verification', reset);
        $('#vcode').off('.qrInput').on('change.qrInput', verify);
    });

})(jQuery);
