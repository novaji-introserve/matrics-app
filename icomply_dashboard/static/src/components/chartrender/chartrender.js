/** @odoo-module */

import { registry } from "@web/core/registry";
import { loadJS } from "@web/core/assets";
const { Component, onWillStart, useRef, onMounted, useEffect, onWillUnmount } = owl;
import { useService } from "@web/core/utils/hooks";
export class ChartRenderer extends Component {
  setup() {
    this.navigate = useService("action")
    this.chartRef = useRef("chart");
    onWillStart(async () => {
      await loadJS(
        "https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js"
      );
    });

    onMounted(() => this.renderChart());

    useEffect(() => {
      this.renderChart()

    }, () => [this.props.config])

    onWillUnmount(() =>{
      if(this.mychart){
        this.mychart.destroy();
        this.mychart = null;
      }
    })


  }
  


  renderChart() {

    if(this.mychart){
      this.mychart.destroy();
      this.mychart = null;
    }
   this.mychart = new Chart(this.chartRef.el, {
      type: this.props.type,
      data: this.props.config,
      options: {
        responsive: true,
        onClick: (event, elements) =>{
           const clickedIndex = elements[0].index;
           const filter = this.props.config.labels[clickedIndex];

          if(filter == "High" || filter == "Medium" || filter == "Low"){
            this.navigate.doAction({
              type: "ir.actions.act_window",
              name: "open_chart_by_priority",
              res_model: "case.management",
              domain:
                this.props.datepicked > 0
                  ? [["priority_level_id.name", "=", filter],["created_at", ">=", this.props.current_datepicked]]
                  : [["priority_level_id.name", "=", filter]],
              views: [
                [false, "tree"],
                [false, "form"],
              ],
            });
          }else{
            this.navigate.doAction({
              type: "ir.actions.act_window",
              name: "open_chart_by_priority",
              res_model: "case.management",
              domain:
                this.props.datepicked > 0
                  ? [["case_status_id.name", "=", filter],["created_at", ">=", this.props.current_datepicked]]
                  : [["case_status_id.name", "=", filter]],
              views: [
                [false, "tree"],
                [false, "form"],
              ],
            });
          }
          
        },
        scales: {
            y: {
                ticks: {
                  stepSize: 1,
                    // Include a dollar sign in the ticks
                    callback: function(value, index, ticks) {
                        return value;
                    }
                }
            }
        },
        maintainAspectRatio: false,
        plugins: {
          legend: {
            position: this.props.type == "pie" ? "right" : "top",
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
