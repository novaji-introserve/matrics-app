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

export class InterbankChart extends Component {
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
                };
            }

            const palette = MODERN_PALETTES[paletteKey] || MODERN_PALETTES.accounts;
            const colors = this.buildBarColors(palette, labelCount || 0);
            if (chartType === "pie" || chartType === "doughnut") {
                return {
                    ...dataset,
                    data: Array.isArray(dataset?.data) ? dataset.data : [],
                    backgroundColor: colors,
                    borderColor: colors.map(() => "#ffffff"),
                    borderWidth: 2,
                    hoverOffset: 10,
                };
            }

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
            };
        });

        return {
            ...sourceData,
            datasets,
        };
    }

    formatLabel(label, typeName) {
        if (label === null || label === undefined) {
            return "Unknown";
        }
        const stringLabel = String(label);
        if (typeName === "line") {
            return stringLabel;
        }
        return stringLabel.length > 28 ? `${stringLabel.slice(0, 28)}...` : stringLabel;
    }

    getTooltipTitle(items, rawLabels, ids) {
        const item = items?.[0];
        if (!item) {
            return "";
        }
        const index = item.dataIndex;
        const rawLabel = rawLabels[index];
        const recordId = ids[index];
        if (recordId) {
            return `${rawLabel} (#${recordId})`;
        }
        return String(rawLabel ?? "");
    }
}

InterbankChart.template = "owl.InterbankChart";
InterbankChart.props = {
    data: { type: Object, optional: true },
};
