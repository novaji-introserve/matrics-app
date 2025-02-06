/** @odoo-module */

import { loadJS } from "@web/core/assets";
const { Component, onWillStart, useRef, onMounted, useEffect, onWillUnmount } = owl;
import { useService } from "@web/core/utils/hooks";
export class ChartRenderer extends Component {
  setup() {
    this.navigate = useService("action");
    this.chartRef = useRef("chart");
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
        this.props.config,
        this.props.datepicked,
        this.props.admin,
        this.props.branch,
        this.props.current_datepicked,
      ]
    );

    onWillUnmount(() => {
      if (this.mychart) {
        this.mychart.destroy();
        this.mychart = null;
      }
    });
  }

  getDomain(filter) {
    // Make getDomain a method of the component
    let domain = [["risk_level", "=", filter]]; // Start with the common condition

    if (this.props.datepicked > 0) {
      domain.push([
        "create_date",
        ">=",
        new Date(this.props.current_datepicked),
      ]);
    }

    if (!this.props.admin) {
      // Simplified condition
      domain.push(["branch_id", "in", Array.from(this.props.branch)]);
    }

    return domain;
  }

  renderChart() {
    if (this.mychart) {
      this.mychart.destroy();
      this.mychart = null;
    }

    this.mychart = new Chart(this.chartRef.el, {
      type: this.props.type,
      data: this.props.config,
      options: {
        responsive: true,
        onClick: (event, elements) => {
          if (!elements || elements.length === 0) return; // Handle no element click

          const clickedIndex = elements[0].index;
          const filter = this.props.config.labels[clickedIndex];

          let action = {
            type: "ir.actions.act_window",
            views: [
              [false, "tree"],
              [false, "form"],
            ],
          };

          if (this.props.title === "Rating") {
            action.name = "Transaction by priority";
            action.res_model = "res.customer.transaction";
            action.domain = this.getDomain(filter); // Use component's method
          } else if (this.props.title === "Customer") {
            action.name = "Customer by priority";
            action.res_model = "res.partner";
            action.domain = this.getDomain(filter); // Use component's method
          } else {
            // Transaction state
            action.name = "Transaction state";
            action.res_model = "res.customer.transaction";
            action.domain = [["state", "=", filter]]; // Simplified for this case. You can use getDomain if needed
            if (this.props.datepicked > 0) {
              action.domain.push([
                "create_date",
                ">=",
                new Date(this.props.current_datepicked),
              ]);
            }
          }

          this.navigate.doAction(action);
        },
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

ChartRenderer.template = "owl.ChartRenderer";
