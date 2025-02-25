/** @odoo-module */

import { loadJS } from "@web/core/assets";
const { Component, onWillStart, useRef, useEffect, onWillUnmount } = owl;
import { useService } from "@web/core/utils/hooks";

// Consider using a constant for CDN URLs for maintainability
const CHARTJS_CDN =
  "https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js";


// Define constants for date formats to ensure consistency
const DATE_FORMAT_YYYY_MM_DD = "YYYY-MM-DD";
const TIME_00_00_00 = "00:00:00";
const TIME_23_59_59 = "23:59:59";

export class ChartRenderer extends Component {
  setup() {
    this.navigate = useService("action");
    this.chartRef = useRef("compliance_chart");
    this.myChartInstance = null; // Renamed to avoid shadowing and be more explicit

    onWillStart(async () => {
      await loadJS(CHARTJS_CDN);
    });

    useEffect(
      () => {
        if (this.props.data) {
          this.renderChart();
        }
      },
      () => [this.props.data, this.props.type, this.props.title] // Add type and title to dependencies if they can change and require re-render
    );

    onWillUnmount(() => {
      this.destroyChart(); // Use a dedicated method for chart destruction
    });
  }

  destroyChart() {
    if (this.myChartInstance) {
      this.myChartInstance.destroy();
      this.myChartInstance = null;
    }
  }

  renderChart() {
    this.destroyChart(); // Destroy existing chart before rendering a new one

    if (this.props.title === "Top 10 Branches By Customer") {
      // Use constant for comparison
      this.renderTopBranchesChart();
    } else if (this.props.title === "Top 10 Screened Transaction By Rules") {
      this.renderScreenedChart();
    }
  }


  renderTopBranchesChart() {


    if (!this.props.data) {
      return; // Exit if no data to render
    }

    let labels = [];
    let values = [];
    let branch_ids = [];

    for (let item of this.props.data) {
      labels.push(item.branch_name );
      values.push(item.customer_count);
      branch_ids.push(item.id);
    }


    

    const formatDate = (date) => date.toISOString().slice(0, 10);

    let prevDate, currentDate;

    if (this.props.date > 0) {
      prevDate = moment()
        .subtract(this.props.date, "days")
        .format(DATE_FORMAT_YYYY_MM_DD); // Use constant for date format
      currentDate = formatDate(new Date());
    } else {
      currentDate = formatDate(new Date());
      prevDate = currentDate;
    }

    const odooPrevDate = `${prevDate} ${TIME_00_00_00}`; // Use constants for time and date format
    const odooCurrentDate = `${currentDate} ${TIME_23_59_59}`;
    

    this.myChartInstance = new Chart(this.chartRef.el, {
      type: this.props.type,
      data: {
        labels: labels,
        datasets: [
          {
            label: "", // Consider making label configurable if needed
            data: values,
            backgroundColor: [
              "#d9d9d9",
              "#FFD700",
              "#66B2FF",
              "#C8102E",
              "#4CAF50",
            ],
          },
        ],
      },
      options: {
        ...this.getDefaultChartOptions(), // Start with default options
        onClick: (event, elements) => {
          if (!elements || elements.length === 0) return;

          const clickedIndex = elements[0].index;
          const filter = branch_ids[clickedIndex];

          let action = {
            type: "ir.actions.act_window",
            name: "Top 10 Branches", // Use constant for action name
            res_model: "res.partner", // Use constant for model name
            domain: [
              ["branch_id", "=", filter],
              ["create_date", ">=", odooPrevDate],
              ["create_date", "<=", odooCurrentDate],
            ],
            views: [
              [false, "tree"], // Use constants for view types
              [false, "form"],
            ],
          };

          this.navigate.doAction(action);
        },
        scales: {
          y: {
            ticks: {
              stepSize: 100,
              callback: function (value) {
                return value;
              },
            },
          },
        },
        plugins: {
          title: {
            text: this.props.title,
          },
        },
      },
    });
  }
  renderScreenedChart() {


    if (!this.props.data) {
      return; // Exit if no data to render
    }

    let labels = [];
    let values = [];
    let rules_ids = [];

    for (let item of this.props.data) {
      labels.push(item.name );
      values.push(item.count);
      rules_ids.push(item.id);
    }


    

    const formatDate = (date) => date.toISOString().slice(0, 10);

    let prevDate, currentDate;

    if (this.props.date > 0) {
      prevDate = moment()
        .subtract(this.props.date, "days")
        .format(DATE_FORMAT_YYYY_MM_DD); // Use constant for date format
      currentDate = formatDate(new Date());
    } else {
      currentDate = formatDate(new Date());
      prevDate = currentDate;
    }


    this.myChartInstance = new Chart(this.chartRef.el, {
      type: this.props.type,
      data: {
        labels: labels,
        datasets: [
          {
            label: "", // Consider making label configurable if needed
            data: values,
            backgroundColor: "#d9d9d9", // Set the background color to grey
  
          },
        ],
      },
      options: {
        ...this.getDefaultChartOptions(), // Start with default options
        onClick: (event, elements) => {
          if (!elements || elements.length === 0) return;

          const clickedIndex = elements[0].index;
          const filter = rules_ids[clickedIndex];

          let action = {
            type: "ir.actions.act_window",
            name: "Top 10 Screened Transaction By Rules", // Use constant for action name
            res_model: "res.customer.transaction", // Use constant for model name
            domain: [
              ["rule_id", "=", filter],
              ["create_date", ">=", prevDate],
              ["create_date", "<=", currentDate],
            ],
            views: [
              [false, "tree"], // Use constants for view types
              [false, "form"],
            ],
          };

          this.navigate.doAction(action);
        },
        scales: {
          y: {
            ticks: {
              stepSize: 100,
              callback: function (value) {
                return value;
              },
            },
          },
        },
        plugins: {
          title: {
            text: this.props.title,
          },
        },
      },
    });
  }

  getDefaultChartOptions() {
    return {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          position: "top",
        },
        title: {
          display: true,
          position: "bottom",
        },
      },
    };
  }
}

ChartRenderer.template = "owl.ChartRender";
