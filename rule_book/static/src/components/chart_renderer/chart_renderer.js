/** @odoo-module */

import { registry } from "@web/core/registry";
import { loadJS } from "@web/core/assets";
const { Component, onWillStart, useRef, onMounted, useEffect, onWillUnmount } =
  owl;
  import {useService} from "@web/core/utils/hooks"

export class ChartRenderer extends Component {
  setup() {
    this.chartRef = useRef("chart");
    this.actionService = useService("action");

    onWillStart(async () => {
      await loadJS(
        "https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js"
      );
    });

    useEffect(
      () => {
        this.renderChart();
      },
      () => [this.props.config]
    );

    onWillUnmount(() => {
      if (this.chart) {
        this.chart.destroy();
      }
    });

    onMounted(() => this.renderChart());
  }

  renderChart() {
    if (this.chart) {
      this.chart.destroy();
    }
    this.chart = new Chart(this.chartRef.el, {
      type: this.props.type,
      data: this.props.config.data,
      options: {
        // onClick: (e) => {
        //   const active = e.chart.getActiveElements();

        //   if (active) {
        //     const label= e.chart.data.labels[active[0].index]
        //     this.actionService.doAction({
        //       type: "ir.actions.act_window",
        //       name: this.props.title,
        //       res_model: "reply.log",
        //       views: [
        //         [false, "list"],
        //         [false, "form"],
        //       ],
        //     });
            
        //   }
        // },
        responsive: true,
        plugins: {
          legend: {
            position: "bottom",
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
