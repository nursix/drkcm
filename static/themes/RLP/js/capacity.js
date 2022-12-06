(function($, undefined) {

    "use strict";

    var renderChart = function() {

        var container = $('#history-chart'),
            data = JSON.parse($('#history-data').val());

        if (data === undefined) {
            container.empty();
            return;
        }

        var chartData = [];
        Object.keys(data.population).forEach(function(key) {
            var series = data.population[key],
                values = [];
            series.forEach(function(value, day) {
                values.push([(data.start + 86400 * (day + 1)) * 1000, value]);
            });
            chartData.push({key: key, values: values});
        });

        // Set the height of the chart container
        container.css({height: '480px'});

        // Create SVG
        var canvas = d3.select(container.get(0)).append('svg').attr('class', 'nv');

        var legend = function(d) {
            return d;
        };

        // Define the chart
        var chart = nv.models.lineChart()
                             .x(function(d) { return d[0]; })
                             .y(function(d) { return d[1]; })
                             .interpolate('step')
                             .margin({right: 50, bottom: 80})
                             .duration(50)
                             .showLegend(legend)
                             .useInteractiveGuideline(true)
                             .forceY([0, 100]);

        // Set value and tick formatters
        chart.yAxis.ticks(12).tickFormat(function(d) {
            return d !== undefined ? Math.floor(d) : 0;
        }).axisLabel('Belegung');
        chart.xAxis.ticks(12).tickFormat(function(d) {
            return d !== undefined ? new Date(d).toLocaleDateString() : '-';
        }).rotateLabels(-45);
        chart.legend.keyFormatter(function(key) {
            return data.labels[key];
        });
        chart.interactiveLayer.tooltip.keyFormatter(function(key) {
            return data.labels[key];
        });

        // Render chart
        nv.addGraph(function() {
            canvas.datum(chartData)
                  .transition()
                  .duration(50)
                  .call(chart);
            $(window).off('resize.capacity').on('resize.capacity', function(e) {
                chart.update(e);
            });
            return canvas;
        });
    };

    $(function() {

        renderChart();

    });

})(jQuery);
