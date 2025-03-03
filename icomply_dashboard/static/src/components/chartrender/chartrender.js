// /** @odoo-module */

// import { loadJS } from "@web/core/assets";
// const { Component, onWillStart, useRef, onMounted, useEffect, onWillUnmount } = owl;
// import { useService } from "@web/core/utils/hooks";
// export class ChartRenderer extends Component {
  //   setup() {
    //     this.navigate = useService("action");
    // this.chartRef = useRef("chart");
//     onWillStart(async () => {
//       await loadJS(
//         "https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js"
//       );
//     });

//     onMounted(() => this.renderChart());

//     useEffect(
//       () => {
//         this.renderChart();
//       },
//       () => [
//         this.props.config,
//         this.props.datepicked,
//         this.props.admin,
//         this.props.branch,
//         this.props.current_datepicked,
//       ]
//     );

//     onWillUnmount(() => {
//       if (this.mychart) {
//         this.mychart.destroy();
//         this.mychart = null;
//       }
//     });
//   }

//   getDomain(filter) {
//     // Make getDomain a method of the component
//     let domain = [["risk_level", "=", filter]]; // Start with the common condition

//     if (this.props.datepicked > 0) {
//       domain.push([
//         "create_date",
//         ">=",
//         new Date(this.props.current_datepicked),
//       ]);
//     }

//     if (!this.props.admin) {
//       // Simplified condition
//       domain.push(["branch_id", "in", Array.from(this.props.branch)]);
//     }

//     return domain;
//   }

//   renderChart() {
//     if (this.mychart) {
//       this.mychart.destroy();
//       this.mychart = null;
//     }

//     this.mychart = new Chart(this.chartRef.el, {
//       type: this.props.type,
//       data: this.props.config,
//       options: {
//         responsive: true,
//         onClick: (event, elements) => {
//           if (!elements || elements.length === 0) return; // Handle no element click

//           const clickedIndex = elements[0].index;
//           const filter = this.props.config.labels[clickedIndex];

//           let action = {
//             type: "ir.actions.act_window",
//             views: [
//               [false, "tree"],
//               [false, "form"],
//             ],
//           };

//           if (this.props.title === "Rating") {
//             action.name = "Transaction by priority";
//             action.res_model = "res.customer.transaction";
//             action.domain = this.getDomain(filter); // Use component's method
//           } else if (this.props.title === "Customer") {
//             action.name = "Customer by priority";
//             action.res_model = "res.partner";
//             action.domain = this.getDomain(filter); // Use component's method
//           } else {
//             // Transaction state
//             action.name = "Transaction state";
//             action.res_model = "res.customer.transaction";
//             action.domain = [["state", "=", filter]]; // Simplified for this case. You can use getDomain if needed
//             if (this.props.datepicked > 0) {
//               action.domain.push([
//                 "create_date",
//                 ">=",
//                 new Date(this.props.current_datepicked),
//               ]);
//             }
//           }

//           this.navigate.doAction(action);
//         },
//         scales: {
//           y: {
//             ticks: {
//               stepSize: 1,
//               callback: function (value) {
//                 // Simplified callback
//                 return value;
//               },
//             },
//           },
//         },
//         maintainAspectRatio: false,
//         plugins: {
//           legend: {
//             position: this.props.type === "pie" ? "right" : "top",
//           },
//           title: {
//             display: true,
//             text: this.props.title,
//             position: "bottom",
//           },
//         },
//       },
//     });
//   }
// }

// ChartRenderer.template = "owl.ChartRenderer";


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
    this.chartRef = useRef("chart");
    this.myChartInstance = null; // Renamed to avoid shadowing and be more explicit

    onWillStart(async () => {
      await loadJS(CHARTJS_CDN);
    });

    useEffect(
      () => {
        if (this.props.config) {
          this.renderChart();
        }
      },
      () => [this.props.config, this.props.datepicked, this.props.admin] // Add type and title to dependencies if they can change and require re-render
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
      this.renderTopScreenedChart();
    } else {
      this.renderHighRiskchart();
    }
  }

  renderTopBranchesChart() {
    if (!this.props.config) {
      return; // Exit if no data to render
    }

    let labels = [];
    let values = [];
    let branch_ids = [];

    for (let item of this.props.config) {
      labels.push(item.branch_name.split(" ")[0]);
      values.push(item.customer_count);
      branch_ids.push(item.id);
    }

    const formatDate = (date) => date.toISOString().slice(0, 10);

    let prevDate, currentDate;

    if (this.props.datepicked > 0) {
      prevDate = moment()
        .subtract(this.props.datepicked, "days")
        .format(DATE_FORMAT_YYYY_MM_DD); // Use constant for date format
      currentDate = formatDate(new Date());
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
          },
        ],
      },
      options: {
        ...this.getDefaultChartOptions(), // Start with default options
        onClick: (event, elements) => {
          if (!elements || elements.length === 0) return;

          const clickedIndex = elements[0].index;
          const filter = branch_ids[clickedIndex];

          let domain = [["branch_id", "=", filter]];

          if (this.props.datepicked > 0) {
            domain.push(["create_date", ">=", odooPrevDate]);
            domain.push(["create_date", "<=", odooCurrentDate]);
          }

          // Admin Check and Branch Filtering
          if (!this.props.admin) {
            domain.push(["branch_id", "in", this.props.branch]);
          }

          let action = {
            type: "ir.actions.act_window",
            name: "Top 10 Branches", // Use constant for action name
            res_model: "res.partner", // Use constant for model name
            domain: domain,
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

  renderTopScreenedChart() {
    if (!this.props.config) {
      return; // Exit if no data to render
    }

    let labels = [];
    let values = [];
    let trans_id = [];

    for (let item of this.props.config) {
      labels.push(item.name);
      values.push(item.count);
      trans_id.push(item.id);
    }

    const formatDate = (date) => date.toISOString().slice(0, 10);

    let prevDate, currentDate;

    if (this.props.datepicked > 0) {
      prevDate = moment()
        .subtract(this.props.datepicked, "days")
        .format(DATE_FORMAT_YYYY_MM_DD); // Use constant for date format
      currentDate = formatDate(new Date());
    } 

    this.myChartInstance = new Chart(this.chartRef.el, {
      type: this.props.type,
      data: {
        labels: labels,
        datasets: [
          {
            label: "", // Consider making label configurable if needed
            data: values,
          },
        ],
      },
      options: {
        ...this.getDefaultChartOptions(), // Start with default options
        onClick: (event, elements) => {
          if (!elements || elements.length === 0) return;

          const clickedIndex = elements[0].index;
          const filter = trans_id[clickedIndex];

          let domain = [["rule_id", "=", filter]];

          if (this.props.datepicked > 0) {
            domain.push(["date_created", ">=", prevDate]);
            domain.push(["date_created", "<=", currentDate]);
          }
          // Admin Check and Branch Filtering
          if (!this.props.admin) {
            domain.push(["branch_id", "in", this.props.branch]);
          }


          let action = {
            type: "ir.actions.act_window",
            name: "Top 10 Screened Transaction By Rules", // Use constant for action name
            res_model: "res.customer.transaction", // Use constant for model name
            domain: domain,
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
              stepSize: 1000,
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
  renderHighRiskchart() {
    if (!this.props.config) {
      return; // Exit if no data to render
    }

    let labels = [];
    let values = [];
    let branch_ids = [];

    for (let item of this.props.config) {
      labels.push(item.name.split(" ")[0]);
      values.push(item.count);
      branch_ids.push(item.id);
    }


    

    const formatDate = (date) => date.toISOString().slice(0, 10);

    let prevDate, currentDate;

    if (this.props.datepicked > 0) {
      prevDate = moment()
        .subtract(this.props.datepicked, "days")
        .format(DATE_FORMAT_YYYY_MM_DD); // Use constant for date format
      currentDate = formatDate(new Date());
    } 

    this.myChartInstance = new Chart(this.chartRef.el, {
      type: this.props.type,
      data: {
        labels: labels,
        datasets: [
          {
            label: "", // Consider making label configurable if needed
            data: values,
            border: "none",
          },
        ],
      },
      options: {
        ...this.getDefaultChartOptions(), // Start with default options
        onClick: (event, elements) => {
          if (!elements || elements.length === 0) return;

          const clickedIndex = elements[0].index;
          const filter = branch_ids[clickedIndex];

          let domain = [
            ["branch_id", "=", filter],
            ["risk_level", "=", "high"],
          ];

          if (this.props.datepicked > 0) {
            domain.push(["create_date", ">=", prevDate]);
            domain.push(["create_date", "<=", currentDate]);
          }

          // Admin Check and Branch Filtering
          if (!this.props.admin) {
            domain.push(["branch_id", "in", this.props.branch]);
          }

          let action = {
            type: "ir.actions.act_window",
            name: "Top 10 High-Risk Branches", // Use constant for action name
            res_model: "res.partner", // Use constant for model name
            domain: domain,
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
          legend: {
            position: "right",
            align: "center",
          },
        },
      },
    });
  }

  getDefaultChartOptions() {
    return {
      responsive: true,
      maintainAspectRatio: false,
    };
  }
}

ChartRenderer.template = "owl.ChartRenderer";

