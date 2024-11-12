// Path: static/src/js/chart_init.js
odoo.define("rule_book.chart_init", function (require) {
  "use strict";

  var AbstractAction = require("web.AbstractAction");
  var core = require("web.core");
  var rpc = require("web.rpc");

  var ChartDashboard = AbstractAction.extend({
    template: "rule_book.alert_chart_template",

    start: function () {
      var self = this;
      return this._super.apply(this, arguments).then(function () {
        self._initChart();
      });
    },

    _initChart: function () {
      var ctx = this.el.querySelector("#myChart");
      if (ctx) {
        var xValues = [
          "Early Submission",
          "Right on Time",
          "Late Submission",
          "Not Responded",
        ];
        var yValues = [5, 29, 24, 14];
        var barColors = ["green", "blue", "orange", "red"];

        new Chart(ctx, {
          type: "bar",
          data: {
            labels: xValues,
            datasets: [
              {
                label: "Responses",
                backgroundColor: barColors,
                data: yValues,
              },
            ],
          },
          options: {
            responsive: true,
            legend: { display: false },
            title: {
              display: true,
              text: "Response Timing Reports",
            },
            scales: {
              yAxes: [
                {
                  ticks: {
                    beginAtZero: true,
                  },
                },
              ],
            },
          },
        });
      }
    },
  });

  core.action_registry.add("rule_book_chart_dashboard", ChartDashboard);

  return ChartDashboard;
});
