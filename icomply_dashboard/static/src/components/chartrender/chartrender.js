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

          if(this.props.title == "Rating"){
            
            this.navigate.doAction({
              type: "ir.actions.act_window",
              name: "Transaction by priority",
              res_model: "res.customer.transaction",
              domain:
                this.props.datepicked > 0
                  ? [["risk_level", "=", filter],["created_at", ">=", this.props.current_datepicked]]
                  : [["risk_level", "=", filter]],
              views: [
                [false, "tree"],
                [false, "form"],
              ],
            });
          }else if(this.props.title == "Customer"){
             
            this.navigate.doAction({
              type: "ir.actions.act_window",
              name: "Customer by priority",
              res_model: "res.partner",
              domain:
                this.props.datepicked > 0
                  ? [
                      ["risk_level", "=", filter],
                      ["created_at", ">=", this.props.current_datepicked],
                    ]
                  : [["risk_level", "=", filter]],
              views: [
                [false, "tree"],
                [false, "form"],
              ],
            });
          }
          else{
           
            this.navigate.doAction({
              type: "ir.actions.act_window",
              name: "Transaction state",
              res_model: "res.customer.transaction",
              domain:
                this.props.datepicked > 0
                  ? [["state", "=", filter],["created_at", ">=", this.props.current_datepicked]]
                  : [["state", "=", filter]],
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
