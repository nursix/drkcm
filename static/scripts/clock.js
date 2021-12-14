// Inspired by https://codepen.io/kylewetton/pen/QJbOjw
// Copyright (c) 2021 by Kyle Wetton, License MIT
(function() {
    var currentSec = getSecondsToday(),
        seconds = (currentSec / 60) % 1,
        minutes = (currentSec / 3600) % 1,
        hours = (currentSec / 43200) % 1;
    setTime(60 * seconds, 's');
    setTime(3600 * minutes, 'm');
    setTime(43200 * hours, 'h');
    function setTime(left, hand) {
        $('.wac__' + hand).css('animation-delay', '' + left * -1 + 's');
    }
    function getSecondsToday() {
        let now = new Date(),
            today = new Date(now.getFullYear(), now.getMonth(), now.getDate()),
            diff = now - today;
        return Math.round(diff / 1000);
    }
})();
