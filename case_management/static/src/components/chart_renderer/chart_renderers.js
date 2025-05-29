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

            // Set a custom ID for the canvas to identify which chart is being clicked
            this.chartRef.el.id = this.props.chartType === 'severity' ? 'severity_chart' : 'case_rate_chart';

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
            const element = elements[0];
            const index = element.index;
            const chartId = this.chartRef.el.id;

            // Add the chart instance to the element to match the expected format
            // in the CaseDashboard.onChartClick method
            element._chart = {
                canvas: {
                    id: chartId
                }
            };

            // Dispatch the click event to the parent component
            this.env.bus.trigger('chart-clicked', { event, elements, chartId });
            
            // For backward compatibility, also use the default behavior
            if (this.props.chartType === 'severity') {
                const severity = this.chartInstance.data.labels[index];
                this.goToSeverityView(severity);
            } else {
                const status = this.chartInstance.data.labels[index].toLowerCase();
                if (status && status !== 'all') {
                    this.goToView(status);
                }
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

    goToSeverityView(severity) {
        const severityMap = {
            'low': '1',
            'medium': '2',
            'high': '3',
        };
        const titleValue = severityMap[severity.toLowerCase()];
    
        if (!titleValue) {
            console.warn(`Invalid severity: ${severity}`);
            return;
        }
    
        this.env.services.action.doAction({
            type: 'ir.actions.act_window',
            name: `${severity} Severity Cases`,
            res_model: 'case',
            view_mode: 'tree,form',
            domain: [['title', '=', titleValue]],
            views: [[false, 'tree'], [false, 'form']],
        });
    }
    

    // goToSeverityView(severity) {
    //     this.env.services.action.doAction({
    //         type: 'ir.actions.act_window',
    //         name: `${severity} Severity Cases`,
    //         res_model: 'case',
    //         view_mode: 'tree,form',
    //         domain: [['severity', '=', severity.toLowerCase()]],
    //         views: [[false, 'tree'], [false, 'form']],
    //     });
    // }
}

ChartRenderer.template = "owl.ChartRenderer";
registry.category("components").add("owl.ChartRenderer", ChartRenderer);

export { ChartRenderer };
































