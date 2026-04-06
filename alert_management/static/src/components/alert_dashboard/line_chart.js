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

        const chartData = this.props.data || {};
        const chartType = chartData.type || "line";
        const labels = Array.isArray(chartData.labels)
            ? chartData.labels.map((label) => String(label ?? ""))
            : [];
        const datasets = Array.isArray(chartData.datasets)
            ? chartData.datasets.map((dataset, index) =>
                  this.normalizeDataset(dataset, chartType, index)
              )
            : [];

        this.chart = new Chart(this.canvasRef.el.getContext("2d"), {
            type: chartType,
            data: {
                labels,
                datasets,
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
                        chartId: chartData?.id,
                        label: labels?.[index],
                        value: chartData?.datasets?.[0]?.data?.[index] ?? 0,
                        id: chartData?.ids?.[index],
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

    normalizeDataset(dataset, chartType, index) {
        const colorFallbacks = ["#2563eb", "#0f766e", "#b45309", "#dc2626"];
        const fallbackColor = colorFallbacks[index % colorFallbacks.length];
        const normalized = {
            ...dataset,
            label: dataset?.label || "Alerts",
            data: Array.isArray(dataset?.data) ? dataset.data : [],
        };

        if (chartType === "line") {
            const backgroundSource = Array.isArray(dataset?.backgroundColor)
                ? dataset.backgroundColor[0]
                : dataset?.backgroundColor;
            const borderSource = Array.isArray(dataset?.borderColor)
                ? dataset.borderColor[0]
                : dataset?.borderColor;
            normalized.borderColor = borderSource || backgroundSource || fallbackColor;
            normalized.backgroundColor =
                backgroundSource || "rgba(37, 99, 235, 0.14)";
            normalized.fill = dataset?.fill ?? true;
            normalized.tension = dataset?.tension ?? 0.32;
            normalized.pointRadius = dataset?.pointRadius ?? 4;
            normalized.pointHoverRadius = dataset?.pointHoverRadius ?? 6;
        } else if (chartType === "bar") {
            normalized.backgroundColor =
                dataset?.backgroundColor || ["#b45309", "#d97706", "#92400e"];
            normalized.borderColor = dataset?.borderColor || [];
            normalized.borderWidth = dataset?.borderWidth ?? 1;
        }

        return normalized;
    }
}

AlertDashboardChart.template = "alert_management.AlertDashboardChart";
AlertDashboardChart.props = {
    data: { type: Object, optional: false },
    onPointClick: { type: Function, optional: true },
};
