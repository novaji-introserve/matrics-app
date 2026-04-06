/** @odoo-module */

import { loadJS } from "@web/core/assets";

const { Component, onMounted, onWillUnmount, onWillUpdateProps, useRef } = owl;

const MODERN_PALETTES = {
    accounts: [
        "#2563eb",
        "#3b82f6",
        "#60a5fa",
        "#93c5fd",
        "#1d4ed8",
        "#0ea5e9",
        "#38bdf8",
        "#0284c7",
        "#6366f1",
        "#8b5cf6",
    ],
    highRisk: [
        "#f97316",
        "#fb7185",
        "#ef4444",
        "#f59e0b",
        "#ea580c",
        "#dc2626",
        "#f43f5e",
        "#fb923c",
        "#f87171",
        "#fdba74",
    ],
    exceptions: [
        "#059669",
        "#0ea5e9",
        "#8b5cf6",
        "#f97316",
        "#ef4444",
        "#eab308",
        "#14b8a6",
        "#6366f1",
        "#ec4899",
        "#84cc16",
    ],
    exceptionLine: {
        stroke: "#059669",
        fill: "rgba(5, 150, 105, 0.14)",
        points: "#10b981",
    },
    neutralBorder: "rgba(15, 23, 42, 0.08)",
    neutralGrid: "rgba(15, 23, 42, 0.08)",
    text: "#334155",
};

export class AlertDashboardChart extends Component {
    setup() {
        this.chartRef = useRef("chart");
        this.chart = null;
        this.resizeObserver = null;

        onMounted(async () => {
            await this.loadChartLibrary();
            this.renderChart();
            this.setupResizeHandler();
        });

        onWillUpdateProps(async () => {
            await this.loadChartLibrary();
            this.renderChart();
        });

        onWillUnmount(() => {
            this.cleanupResources();
        });
    }

    async loadChartLibrary() {
        if (window.Chart) {
            return;
        }
        await loadJS("/web/static/lib/Chart/Chart.js");
    }

    setupResizeHandler() {
        if (!window.ResizeObserver || !this.chartRef.el?.parentElement) {
            return;
        }
        this.resizeObserver = new ResizeObserver(this.handleResize.bind(this));
        this.resizeObserver.observe(this.chartRef.el.parentElement);
    }

    handleResize() {
        if (this.chart) {
            this.chart.resize();
        }
    }

    cleanupResources() {
        if (this.chart) {
            this.chart.destroy();
            this.chart = null;
        }
        if (this.resizeObserver) {
            this.resizeObserver.disconnect();
            this.resizeObserver = null;
        }
    }

    renderChart() {
        if (this.chart) {
            this.chart.destroy();
        }
        if (!this.chartRef.el) {
            return;
        }

        const chartData = this.props.data || {};
        const typeName = String(chartData.type || "line").replace(/'/g, "");
        const rawLabels = Array.isArray(chartData.labels) ? chartData.labels : [];
        const labels = rawLabels.map((label) => this.formatLabel(label, typeName));
        const ids = Array.isArray(chartData.ids) ? chartData.ids : [];
        const normalizedData = this.normalizeChartData(chartData, typeName, rawLabels.length);
        const datasets = normalizedData.datasets || [];

        const chartConfig = {
            type: typeName,
            data: {
                labels,
                datasets,
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: {
                    duration: 550,
                    easing: "easeOutQuart",
                },
                interaction: {
                    mode: "nearest",
                    axis: typeName === "pie" || typeName === "doughnut" ? undefined : "x",
                    intersect: false,
                },
                layout: {
                    padding: {},
                },
                elements: {
                    point: {
                        radius: typeName === "line" ? 4 : 0,
                        hoverRadius: 6,
                        borderWidth: 2,
                    },
                    line: {
                        tension: 0.35,
                    },
                    bar: {
                        borderWidth: 1,
                        borderRadius: 4,
                    },
                },
                hover: {
                    mode: "nearest",
                    intersect: false,
                },
                plugins: {
                    legend: {
                        display: typeName !== "bar",
                        position: typeName === "pie" || typeName === "doughnut" ? "right" : "top",
                        labels: {
                            color: MODERN_PALETTES.text,
                            usePointStyle: true,
                            boxWidth: 10,
                        },
                    },
                    tooltip: {
                        enabled: true,
                        backgroundColor: "rgba(15, 23, 42, 0.92)",
                        titleColor: "#fff",
                        bodyColor: "#e2e8f0",
                        padding: 12,
                        cornerRadius: 12,
                        displayColors: typeName !== "line",
                        callbacks: {
                            title: (items) => this.getTooltipTitle(items, rawLabels, ids),
                            label: (context) => {
                                const value = context.parsed?.y ?? context.parsed;
                                return `${context.label}: ${value}`;
                            },
                        },
                    },
                },
                events: ["mousemove", "mouseout", "click", "touchstart", "touchmove"],
                scales: {
                    x: {
                        grid: {
                            display: false,
                        },
                        ticks: {
                            color: MODERN_PALETTES.text,
                            maxRotation: typeName === "line" ? 0 : 18,
                            minRotation: 0,
                        },
                    },
                    y: {
                        beginAtZero: true,
                        grid: {
                            color: MODERN_PALETTES.neutralGrid,
                        },
                        ticks: {
                            color: MODERN_PALETTES.text,
                            precision: 0,
                        },
                    },
                },
                onClick: (_evt, elements) => {
                    if (!elements || elements.length === 0 || !this.props.onPointClick) {
                        return;
                    }
                    const dataIndex = elements[0].index;
                    this.props.onPointClick({
                        chartId: chartData.id,
                        label: labels[dataIndex],
                        value: datasets?.[0]?.data?.[dataIndex] ?? 0,
                        id: ids[dataIndex],
                    });
                },
            },
        };

        this.chart = new Chart(this.chartRef.el.getContext("2d"), chartConfig);
    }

    getPaletteKey(title) {
        const lowerTitle = String(title || "").toLowerCase();
        if (lowerTitle.includes("risk")) {
            return "highRisk";
        }
        if (lowerTitle.includes("alert") || lowerTitle.includes("day")) {
            return "exceptions";
        }
        return "accounts";
    }

    buildBarColors(palette, count) {
        return Array.from({ length: count }, (_, index) => palette[index % palette.length]);
    }

    normalizeChartData(sourceData, chartType, labelCount) {
        const paletteKey = this.getPaletteKey(sourceData.title);
        const datasets = (sourceData.datasets || []).map((dataset) => {
            if (chartType === "line") {
                return {
                    ...dataset,
                    data: Array.isArray(dataset?.data) ? dataset.data : [],
                    borderColor: MODERN_PALETTES.exceptionLine.stroke,
                    backgroundColor: MODERN_PALETTES.exceptionLine.fill,
                    pointBackgroundColor: MODERN_PALETTES.exceptionLine.points,
                    pointBorderColor: "#ffffff",
                    pointHoverBackgroundColor: MODERN_PALETTES.exceptionLine.stroke,
                    pointHoverBorderColor: "#ffffff",
                    pointRadius: 4,
                    pointHoverRadius: 6,
                    borderWidth: 3,
                    tension: 0.35,
                    fill: true,
                    spanGaps: true,
                };
            }

            const palette = MODERN_PALETTES[paletteKey] || MODERN_PALETTES.accounts;
            const colors = this.buildBarColors(palette, labelCount);
            return {
                ...dataset,
                data: Array.isArray(dataset?.data) ? dataset.data : [],
                backgroundColor: colors,
                borderColor: colors.map(() => MODERN_PALETTES.neutralBorder),
                borderWidth: 1,
                borderRadius: 4,
                borderSkipped: false,
                hoverBackgroundColor: colors,
                maxBarThickness: 38,
                spanGaps: true,
            };
        });

        return {
            ...sourceData,
            datasets,
        };
    }

    formatLabel(label, chartType) {
        const value = String(label ?? "");
        if (chartType !== "line" || !/^\d{4}-\d{2}-\d{2}$/.test(value)) {
            return value;
        }
        const parsed = new Date(`${value}T00:00:00`);
        if (Number.isNaN(parsed.getTime())) {
            return value;
        }
        return parsed.toLocaleDateString(undefined, {
            month: "short",
            day: "numeric",
        });
    }

    getTooltipTitle(items, rawLabels, ids) {
        const index = items?.[0]?.dataIndex;
        const rawValue = String(ids?.[index] || rawLabels?.[index] || "");
        if (!/^\d{4}-\d{2}-\d{2}$/.test(rawValue)) {
            return rawValue;
        }
        const parsed = new Date(`${rawValue}T00:00:00`);
        if (Number.isNaN(parsed.getTime())) {
            return rawValue;
        }
        return parsed.toLocaleDateString(undefined, {
            year: "numeric",
            month: "short",
            day: "numeric",
        });
    }
}

AlertDashboardChart.template = "alert_management.AlertDashboardChart";
AlertDashboardChart.props = {
    data: { type: Object, optional: false },
    onPointClick: { type: Function, optional: true },
};
