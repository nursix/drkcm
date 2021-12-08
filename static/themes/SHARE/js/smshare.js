$(document).ready(function() {
    // Social Media 'share' buttons
    if ($('#socialmedia_share').length > 0) {
        // DIV exists (deployment_setting on)
        var currenturl = document.location.href;
        // Linked-In
        $('#socialmedia_share').append("<div class='socialmedia_element'><script src='//platform.linkedin.com/in.js'></script><script type='IN/Share' data-counter='right'></script></div>");
        // Twitter
        $('#socialmedia_share').append("<div class='socialmedia_element'><a href='https://twitter.com/share' class='twitter-share-button' data-count='none' data-hashtags='sahana-eden'>Tweet</a><script>!function(d,s,id){var js,fjs=d.getElementsByTagName(s)[0];if(!d.getElementById(id)){js=d.createElement(s);js.id=id;js.src='//platform.twitter.com/widgets.js';fjs.parentNode.insertBefore(js,fjs);}}(document,'script','twitter-wjs');</script></div>");
        // Facebook
        $('#socialmedia_share').append("<div class='socialmedia_element'><div id='fb-root'></div><script>(function(d, s, id) { var js, fjs = d.getElementsByTagName(s)[0]; if (d.getElementById(id)) return; js = d.createElement(s); js.id = id; js.src = '//connect.facebook.net/en_US/all.js#xfbml=1'; fjs.parentNode.insertBefore(js, fjs); }(document, 'script', 'facebook-jssdk'));</script> <div class='fb-like' data-send='false' data-layout='button_count' data-show-faces='true' data-href='" + currenturl + "'></div></div>");
    }
});

