/** @odoo-module */

import { registry } from "@web/core/registry";
import { Component, useRef, onWillStart, onMounted, onWillUpdateProps } from "@odoo/owl";
import { loadJS } from "@web/core/assets";

class ChartRenderer extends Component {
    setup() {
        this.chartRef = useRef("chart");
        this.chartInstance = null;

        onWillStart(async () => {
            await loadJS("https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js");
        });

        onMounted(() => {
            this.renderChart();
            this.chartRef.el.onclick = this.onChartClick.bind(this);
        });

        // Add this hook to handle prop updates
        onWillUpdateProps(() => {
            // Destroy old chart instance to prevent memory leaks
            if (this.chartInstance) {
                this.chartInstance.destroy();
                this.chartInstance = null;
            }
            // Re-render with new data on next tick
            setTimeout(() => this.renderChart(), 0);
        });
    }

    renderChart() {
        if (!this.chartRef.el) return;

        try {
            const labels = this.props.tags || [];
            const dataset1 = this.props.data || [];
            const dataset2 = this.props.data2 || [];
            const dataset3 = this.props.data3 || [];
            const backgroundColor = this.props.backgroundColor || ['rgb(255, 205, 86)', 'rgb(54, 162, 235)', 'rgb(255, 99, 132)'];

            this.chartInstance = new Chart(this.chartRef.el, {
                type: this.props.type || 'doughnut',
                data: {
                    labels,
                    datasets: [
                        {
                            label: this.props.label_1 || 'Dataset 1',
                            data: dataset1,
                            backgroundColor,
                            hoverOffset: 4
                        },
                        {
                            label: this.props.label_2 || 'Dataset 2',
                            data: dataset2,
                            backgroundColor,
                            hoverOffset: 4
                        },
                        {
                            label: this.props.label_3 || 'Dataset 3',
                            data: dataset3,
                            backgroundColor,
                            hoverOffset: 4
                        }
                    ],
                },
                options: {
                    responsive: true,
                    plugins: {
                        legend: { position: 'bottom' },
                        title: { 
                            display: true, 
                            text: this.props.text || '', 
                            position: 'bottom' 
                        },
                    }
                },
            });
        } catch (error) {
            console.error('Error rendering chart:', error);
        }
    }

            onChartClick(event) {
            if (!this.chartInstance) return;

            const elements = this.chartInstance.getElementsAtEventForMode(
                event,
                'nearest',
                { intersect: true },
                true
            );

            if (elements.length > 0) {
                const index = elements[0].index;
                const label = this.chartInstance.data.labels[index];
                const status = label.toLowerCase();

                // Prevent navigation for non-status or "all" labels
                if (status && status !== 'all') {
                    this.goToView(status);
                }
            }
        }

        goToView(status) {
            this.env.services.action.doAction({
                type: 'ir.actions.act_window',
                name: `${status.charAt(0).toUpperCase() + status.slice(1)} Cases`,
                res_model: 'case',
                view_mode: 'tree,form',
                domain: [['status_id.name', '=', status]],
                views: [[false, 'tree'], [false, 'form']],
            });
        }
    }

    ChartRenderer.template = "owl.ChartRenderer";
    registry.category("components").add("owl.ChartRenderer", ChartRenderer);

    export { ChartRenderer };


































// import { registry } from "@web/core/registry";
// import { Component, useRef, onWillStart, onMounted } from "@odoo/owl";
// import { loadJS } from "@web/core/assets";

// class ChartRenderer extends Component {
//     setup() {
//         this.chartRef = useRef("chart");
//         this.chartInstance = null;

//         onWillStart(async () => {
//             await loadJS("https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js");
//         });

//         onMounted(() => {
//             this.renderChart();
//             this.chartRef.el.onclick = this.onChartClick.bind(this);
//         });
//     }

//     renderChart() {
//         if (!this.chartRef.el) return;

//         try {
//             const labels = this.props.tags || [];
//             const dataset1 = this.props.data || [];
//             const dataset2 = this.props.data2 || [];
//             const dataset3 = this.props.data3 || [];
//             const backgroundColor = this.props.backgroundColor || ['rgb(255, 205, 86)', 'rgb(54, 162, 235)', 'rgb(255, 99, 132)'];

//             this.chartInstance = new Chart(this.chartRef.el, {
//                 type: this.props.type || 'doughnut',
//                 data: {
//                     labels,
//                     datasets: [
//                         {
//                             label: this.props.label_1 || 'Dataset 1',
//                             data: dataset1,
//                             backgroundColor,
//                             hoverOffset: 4
//                         },
//                         {
//                             label: this.props.label_2 || 'Dataset 2',
//                             data: dataset2,
//                             backgroundColor,
//                             hoverOffset: 4
//                         },
//                         {
//                             label: this.props.label_3 || 'Dataset 3',
//                             data: dataset3,
//                             backgroundColor,
//                             hoverOffset: 4
//                         }
//                     ],
//                 },
//                 options: {
//                     responsive: true,
//                     plugins: {
//                         legend: { position: 'bottom' },
//                         title: { 
//                             display: true, 
//                             text: this.props.text || '', 
//                             position: 'bottom' 
//                         },
//                     }
//                 },
//             });
//         } catch (error) {
//             console.error('Error rendering chart:', error);
//         }
//     }

//     onChartClick(event) {
//         if (!this.chartInstance) return;

//         const elements = this.chartInstance.getElementsAtEventForMode(
//             event,
//             'nearest',
//             { intersect: true },
//             true
//         );

//         if (elements.length > 0) {
//             const index = elements[0].index;
//             const label = this.chartInstance.data.labels[index];
//             const status = label.toLowerCase();

//             // Prevent navigation for non-status or "all" labels
//             if (status && status !== 'all') {
//                 this.goToView(status);
//             }
//         }
//     }

//     goToView(status) {
//         this.env.services.action.doAction({
//             type: 'ir.actions.act_window',
//             name: `${status.charAt(0).toUpperCase() + status.slice(1)} Cases`,
//             res_model: 'case',
//             view_mode: 'tree,form',
//             domain: [['status_id.name', '=', status]],
//             views: [[false, 'tree'], [false, 'form']],
//         });
//     }
// }

// ChartRenderer.template = "owl.ChartRenderer";
// registry.category("components").add("owl.ChartRenderer", ChartRenderer);

// export { ChartRenderer };












// import { registry } from "@web/core/registry";
// import { Component, useRef, onWillStart, onMounted } from "@odoo/owl";
// import { loadJS } from "@web/core/assets";

// class ChartRenderer extends Component {
//     setup() {
//         // Ref to a DOM element
//         this.chartRef = useRef("chart");

//         // Load External JS Before Rendering
//         onWillStart(async () => {
//             console.log("ChartRenderer is about to start...");
//             await loadJS("https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js");
//         });

//         // Perform Actions After Mounting
//         onMounted(() => this.renderChart());
//     }

//     renderChart() {
//         // Add null checks and error handling
//         if (this.chartRef.el) {
//             try {
//                 // Ensure all required props exist and are valid
//                 const labels = this.props.tags || [];
//                 const data = this.props.data || [];
//                 const backgroundColor = this.props.backgroundColor || ['rgb(255, 205, 86)',
//                                         'rgb(54, 162, 235)',
//                                         'rgb(255, 99, 132)',
//                                         ];

//                 new Chart(this.chartRef.el, {
//                     type: this.props.type || 'doughnut',
//                     data: {
//                         labels: labels,
//                         datasets: [
//                             {
//                                 label: this.props.label_1 || 'Dataset 1',
//                                 data: data,
//                                 backgroundColor: backgroundColor,
//                                 hoverOffset: 4
//                             },
//                             {
//                                 label: this.props.label_2 || 'Dataset 2',
//                                 data: [], // Ensure data exists
//                                 backgroundColor: backgroundColor,
//                                 hoverOffset: 4
//                             },
//                             {
//                                 label: this.props.label_3 || 'Dataset 3',
//                                 data: [], // Ensure data exists
//                                 backgroundColor: backgroundColor,
//                                 hoverOffset: 4
//                             }
//                         ],
//                     },
//                     options: {
//                         responsive: true,
//                         plugins: {
//                             legend: { position: 'bottom' },
//                             title: { 
//                                 display: true, 
//                                 text: this.props.text || '', 
//                                 position: 'bottom' 
//                             },
//                         }
//                     },
//                 });
//             } catch (error) {
//                 console.error('Error rendering chart:', error);
//             }
//         } else {
//             console.warn('Chart element not found');
//         }
//     }
// }

// // Assign Template
// ChartRenderer.template = "owl.ChartRenderer";

// // Register ChartRenderer in the Component Registry
// registry.category("components").add("owl.ChartRenderer", ChartRenderer);

// export { ChartRenderer };

