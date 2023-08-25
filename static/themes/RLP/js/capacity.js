/**
 * GIMS Capacity Overview - client-side logic
 *
 * @copyright 2022 (c) AHSS
 * @license MIT
 */

(function($, undefined) {

    "use strict";

    var getChartData = function(start, population) {

        var chartData = [];
        Object.keys(population).forEach(function(key) {
            var series = population[key],
                values = [];
            series.forEach(function(value, day) {
                var timestamp = (start + 86400 * (day + 1)) * 1000;
                values.push([timestamp, value]);
            });
            chartData.push({key: key, values: values});
        });

        return chartData;
    };

    var renderChart = function() {

        var container = $('#history-chart').empty(),
            data = JSON.parse($('#history-data').val());

        if (data === undefined) {
            return;
        }
        var chartData = getChartData(data.start, data.population);

        // Set the height of the chart container
        container.css({height: '480px'});

        // Create SVG and define the chart
        var canvas = d3.select(container.get(0)).append('svg').attr('class', 'nv'),
            chart = nv.models.lineChart()
                             .x(function(d) { return d[0]; })
                             .y(function(d) { return d[1]; })
                             .interpolate('linear')
                             .margin({right: 50, bottom: 80})
                             .duration(50)
                             .showLegend(true)
                             .useInteractiveGuideline(true)
                             .forceY([0, 500]);

        // Configure axes
        chart.yAxis.axisLabel(data.type).ticks(12).tickFormat(function(d) {
            return d !== undefined ? Math.floor(d) : 0;
        });
        chart.xAxis.rotateLabels(-45).ticks(12).tickFormat(function(d) {
            return d !== undefined ? new Date(d).toLocaleDateString() : '-';
        });
        var seriesLabel = function(key) { return data.labels[key]; };
        chart.legend.keyFormatter(seriesLabel);
        chart.interactiveLayer.tooltip.keyFormatter(seriesLabel);

        // Render chart
        nv.addGraph(function() {
            canvas.datum(chartData).transition().duration(50).call(chart);
            // Redraw when window is resized
            $(window).off('resize.capacity').on('resize.capacity', function(e) {
                chart.update(e);
            });
            return canvas;
        });
    };

    var downloadData = function() {
        // Get the year from #data-year
        var year = $('#data-year').val(),
            url = $('#data-download').data('url');
        if (url) {
            var link = document.createElement('a');
            link.href = url;
            if (year) {
                var query = 'year=' + year;
                if (link.search) {
                    link.search += '&' + query;
                } else {
                    link.search = query;
                }
            }
            window.open(link.href);
        }
    };

    $(function() {
        renderChart();
        $('#data-download').off('.capacity').on('click.capacity', downloadData);
    });

})(jQuery);
