/** @odoo-module */

import { loadJS } from "@web/core/assets";
const { Component, onWillStart, useRef, onMounted, useEffect, onWillUnmount } =
  owl;
import { useService } from "@web/core/utils/hooks";

export class ChartRenderer extends Component {
  setup() {
    this.navigate = useService("action");
    this.chartRef = useRef("icomply_chart");
    onWillStart(async () => {
      await loadJS(
        "https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js"
      );
    });

    onMounted(() => this.renderChart());

    useEffect(
      () => {
        this.renderChart();
      },
      () => [
        
      ]
    );

    onWillUnmount(() => {
      if (this.mychart) {
        this.mychart.destroy();
        this.mychart = null;
      }
    });
  }

  
  renderChart() {
    if (this.mychart) {
      this.mychart.destroy();
      this.mychart = null;
    }

    this.mychart = new Chart(this.chartRef.el, {
      type: this.props.type,
      data: {
        labels: this.props.type !== "doughnut" &&
          this.props.type !== "pie" && [
            "January",
            "February",
            "March",
            "April",
            "May",
            "June",
            "July",
            "August",
            "September",
            "October",
            "November",
            "December",
          ],
        datasets: [
          {
            label: "My First Dataset",
            data: [65, 59, 80, 81, 56, 55, 40],
            fill: false,
            borderColor: "rgb(75, 192, 192)",
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
            position: this.props.type === "pie" ? "right" : "top",
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
