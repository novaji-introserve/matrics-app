/** @odoo-module */

import { loadJS } from "@web/core/assets";
const { Component, onWillStart, useRef, onMounted, useEffect, onWillUnmount } =
  owl;
import { useService } from "@web/core/utils/hooks";

export class ChartRenderer extends Component {
  setup() {
    this.navigate = useService("action");
    this.chartRef = useRef("compliance_chart");
    onWillStart(async () => {
      await loadJS(
        "https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js"
      );
    });


    useEffect(
      () => {
        this.props.data && this.renderChart();
      },
      () => [this.props.data]
    );

    onWillUnmount(() => {
      if (this.mychart) {
        this.mychart.destroy();
        this.mychart = null;
      }
    });
  }

  
  renderChart() {
    
    // Prepare data for the chart
    const labels = this.props.data.map((item) => item.scope);
    const counts = this.props.data.map((item) => item.count);

    if (this.mychart) {
      this.mychart.destroy();
      this.mychart = null;
    }

    this.mychart = new Chart(this.chartRef.el, {
      type: this.props.type,
      data: {
        labels: labels,
        datasets: [
          {
            label: this.props.title,
            data: [34, 67, 89],
            fill: false,
            backgroundColor: "rgba(128, 0, 128, 0.1)",
            borderColor: "rgb(100, 202, 192)",
            tension: 0.1,
          },
        ],
      },
      options: {
        responsive: true,
        scales: {
          y: {
            ticks: {
              stepSize: 1,
              callback: function (value) {
                // Simplified callback
                return value;
              },
            },
          },
        },
        maintainAspectRatio: false,
        plugins: {
          legend: {
            position: this.props.type === "doughnut" ? "right" : "top",
          },
          title: {
            display: true,
            text: this.props.title,
            position: "bottom",
          },
        },
      },
    });
  }
}

ChartRenderer.template = "owl.ChartRender";
