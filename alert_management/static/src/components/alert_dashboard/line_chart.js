/** @odoo-module */

import { loadJS } from "@web/core/assets";

const { Component, onMounted, onWillUnmount, onWillUpdateProps, useRef } = owl;

export class AlertDashboardChart extends Component {
    setup() {
        this.canvasRef = useRef("canvas");
        this.chart = null;

        onMounted(async () => {
            await this.loadChartLibrary();
            this.renderChart();
        });

        onWillUpdateProps(async () => {
            await this.loadChartLibrary();
            this.renderChart();
        });

        onWillUnmount(() => {
            if (this.chart) {
                this.chart.destroy();
                this.chart = null;
            }
        });
    }

    async loadChartLibrary() {
        if (!window.Chart) {
            await loadJS("/web/static/lib/Chart/Chart.js");
        }
    }

    renderChart() {
        if (this.chart) {
            this.chart.destroy();
        }

        this.chart = new Chart(this.canvasRef.el.getContext("2d"), {
            type: "line",
            data: {
                labels: this.props.labels || [],
                datasets: [
                    {
                        label: "Alerts",
                        data: this.props.values || [],
                        borderColor: "#2563eb",
                        backgroundColor: "rgba(37, 99, 235, 0.14)",
                        fill: true,
                        tension: 0.32,
                        pointRadius: 4,
                        pointHoverRadius: 6,
                    },
                ],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                onClick: (_event, elements) => {
                    if (!elements.length || !this.props.onPointClick) {
                        return;
                    }

                    const index = elements[0].index;
                    this.props.onPointClick({
                        date: this.props.dates?.[index],
                        label: this.props.labels?.[index],
                        value: this.props.values?.[index] ?? 0,
                    });
                },
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        backgroundColor: "rgba(15, 23, 42, 0.92)",
                        titleColor: "#fff",
                        bodyColor: "#e2e8f0",
                        padding: 12,
                        cornerRadius: 12,
                    },
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: {
                            precision: 0,
                        },
                        grid: {
                            color: "rgba(148, 163, 184, 0.16)",
                        },
                    },
                    x: {
                        grid: {
                            display: false,
                        },
                    },
                },
            },
        });
    }
}

AlertDashboardChart.template = "alert_management.AlertDashboardChart";
AlertDashboardChart.props = {
    dates: { type: Array, optional: true },
    labels: { type: Array, optional: true },
    values: { type: Array, optional: true },
    onPointClick: { type: Function, optional: true },
};
