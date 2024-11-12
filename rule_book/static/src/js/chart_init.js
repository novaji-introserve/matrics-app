// static/src/js/chart_component.js
/** @odoo-module **/

import { registry } from "@web/core/registry";
import { Component, onMounted, useRef } from "@odoo/owl";

class ChartComponent extends Component {
  setup() {
    this.chartRef = useRef("chart");

    onMounted(() => {
      this._initChart();
    });
  }

  _initChart() {
    const ctx = this.chartRef.el;
    const xValues = [
      "Early Submission",
      "Right on Time",
      "Late Submission",
      "Not Responded",
    ];
    const yValues = [5, 29, 24, 14];
    const barColors = ["green", "blue", "orange", "red"];

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
          y: {
            beginAtZero: true,
          },
        },
      },
    });
  }
}

ChartComponent.template = "rule_book.ChartComponent";
ChartComponent.props = {};

registry.category("view_components").add("chart_container", ChartComponent);
