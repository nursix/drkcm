/**
 * Commission verification - client-side logic
 *
 * @copyright 2022 (c) AHSS
 * @license MIT
 */
(function($, undefined) {

    "use strict";

    var reset = function() {
        $('.data, .reset, .throbber').hide();
        $('.scan, .data-empty').removeClass('hide').show();
        $('#verification-result, #provider-id, #provider-name, #start-date, #end-date, #commission-status').text('--');
    };

    var showPending = function() {
        $('.data, .reset, .scan').hide();
        $('.data-empty, .loading').removeClass('hide').show();
    };

    var showResult = function() {
        $('.data-empty, .loading, .scan').hide();
        $('.data, .reset').removeClass('hide').show();
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
            'success': function(response) {
                $('#verification-result').text(response.signature);
                $('#provider-id').text(response.organisation_id);
                $('#provider-name').text(response.organisation);
                $('#start-date').text(response.start);
                $('#end-date').text(response.end);
                $('#commission-status').text(response.status);
                showResult();
            },
            'error': function () {
                alert('error!');
                reset();
            }
        });
    };

    $(function() {

        reset();
        $('#vcode').off('.qrInput').on('change.qrInput', verify);
        $('.reset').off('.verification').on('click.verification', reset);
    });

})(jQuery);
