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
    
    let {labels, values}= this.props.data 

    const formatDate = (date) => date.toISOString().slice(0, 10);

    let prevDate, currentDate;

    if (this.props.date > 0) {
      prevDate = moment()
        .subtract(this.props.date, "days")
        .format("YYYY-MM-DD");
      currentDate = formatDate(new Date()); // Today's date
    } else {
      currentDate = formatDate(new Date()); // Today's date
      prevDate = currentDate; // Same as today if datepicked is 0
    }

    const odooPrevDate = `${prevDate} 00:00:00`; // For Odoo's datetime field
    const odooCurrentDate = `${currentDate} 23:59:59`; // For Odoo's datetime field
 

    if (this.mychart) {
      this.mychart.destroy();
      this.mychart = null;

    }else{
      this.mychart = new Chart(this.chartRef.el, {
        type: this.props.type,
        data: {
          labels: labels,
          datasets: [
            {
              label: "",
              data: Array.from(values),
            },
          ],
        },
        options: {
          responsive: true,
          onClick: (event, elements) => {
            if (!elements || elements.length === 0) return; // Handle no element click

            const clickedIndex = elements[0].index;

            const filter = labels[clickedIndex];

            let action = {
              type: "ir.actions.act_window",
              views: [
                [false, "tree"],
                [false, "form"],
              ],
            };

            if (this.props.title === "Transaction By Risk Level") {
              action.name = "Transaction By Risk Level";
              action.res_model = "res.customer.transaction";
              action.domain = [
                ["risk_level", "=", filter],
                ["create_date", ">=", odooPrevDate], // Use the formatted dates
                ["create_date", "<=", odooCurrentDate], // Use <= for inclusive end date
              ];
            } 

            this.navigate.doAction(action);
          },
          scales: {
            y: {
              ticks: {
                stepSize: 100,
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
              position: this.props.type === "doughnut" ? "bottom" : "top",
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
}

ChartRenderer.template = "owl.ChartRender";
