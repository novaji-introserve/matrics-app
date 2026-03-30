/** @odoo-module */

import { registry } from "@web/core/registry";
import { useService } from "@web/core/utils/hooks";
import { Component, useState, onWillStart, useRef, useEffect, onWillUnmount } from "@odoo/owl";

const ANIMATION_DELAY = 50;
const MAX_INITIAL_ITEMS = 10;

const CHART_COLORS = [
    "rgba(13, 110, 253, 0.8)",
    "rgba(25, 135, 84, 0.8)",
    "rgba(255, 193, 7, 0.8)",
    "rgba(220, 53, 69, 0.8)",
    "rgba(13, 202, 240, 0.8)",
    "rgba(111, 66, 193, 0.8)",
    "rgba(253, 126, 20, 0.8)",
];

/**
 * Alert Chart page: full-page chart using compliance-style logic (Chart.js, progressive
 * loading, drill-down on click). Data comes from alert statistics; our own setup in fsdh_addons.
 */
export class AlertChart extends Component {
    setup() {
        this.rpc = useService("rpc");
        this.action = useService("action");
        this.serverCache = useService("server_cache");
        this.chartRef = useRef("alert_chart_canvas");
        this.chartInstance = null;
        this.animationTimeouts = [];

        this.state = useState({
            stats: [],
            loading: true,
            cco: null,
            branches_id: [],
            uniqueId: null,
            isInitializing: false,
            isAnimating: false,
            error: null,
            loadedElements: 0,
            totalElements: 0,
            emptyChart: false,
        });

        onWillStart(() => this.initialize());
        useEffect(
            () => {
                if (this.state.stats.length && !this.state.loading) {
                    const id = setTimeout(() => this.buildAndRenderChart(), 0);
                    return () => clearTimeout(id);
                } else {
                    this.destroyChart();
                }
            },
            () => [this.state.stats.length, this.state.loading]
        );
        onWillUnmount(() => {
            this.destroyChart();
            this.clearAllAnimationTimeouts();
        });
    }

    async initialize() {
        this.state.loading = true;
        this.state.error = null;
        try {
            await this.getCurrentUser();
            await this.loadStats();
        } catch (e) {
            this.state.error = "Failed to load chart data";
        }
        this.state.loading = false;
    }

    async getCurrentUser() {
        try {
            const result = await this.rpc("/dashboard/user", {});
            if (result) {
                this.state.branches_id = result.branch || [];
                this.state.cco = result.is_cco || result.is_co || false;
                this.state.uniqueId = result.unique_id || null;
            }
        } catch (e) {
            // ignore
        }
    }

    async loadStats() {
        try {
            const result = await this.rpc("/alert/dashboard/stats", {});
            if (result && Array.isArray(result.data)) {
                this.state.stats = result.data;
            } else {
                this.state.stats = [];
            }
        } catch (e) {
            this.state.stats = [];
        }
    }

    parseStatValue(val) {
        if (val === undefined || val === null) return 0;
        const s = String(val).replace(/,/g, "").trim();
        const n = parseFloat(s);
        return isNaN(n) ? 0 : n;
    }

    /**
     * Build chart data in compliance format: labels, datasets, type, title.
     */
    getChartData() {
        const stats = this.state.stats;
        if (!stats.length) return null;
        const labels = stats.map((s) => s.name || s.code || "—");
        const values = stats.map((s) => this.parseStatValue(s.val));
        const backgroundColor = values.map((_, i) => CHART_COLORS[i % CHART_COLORS.length]);
        const borderColor = backgroundColor.map((c) => c.replace("0.8", "1"));
        return {
            labels,
            type: "bar",
            title: "Alert Statistics Overview",
            datasets: [
                {
                    label: "Value",
                    data: values,
                    backgroundColor,
                    borderColor,
                    borderWidth: 1,
                },
            ],
        };
    }

    clearAllAnimationTimeouts() {
        this.animationTimeouts.forEach((id) => clearTimeout(id));
        this.animationTimeouts = [];
    }

    destroyChart() {
        if (this.chartInstance) {
            try {
                this.chartInstance.destroy();
            } catch (e) {}
            this.chartInstance = null;
        }
    }

    /**
     * Compliance-style: optionally progressive load (first batch then animate rest).
     * For few stats we render once; for many we use progressive like compliance.
     */
    buildAndRenderChart() {
        if (typeof window.Chart === "undefined" || !this.chartRef.el) {
            this.destroyChart();
            return;
        }
        const chartData = this.getChartData();
        if (!chartData || !chartData.labels.length) {
            this.state.emptyChart = true;
            this.destroyChart();
            return;
        }
        this.state.emptyChart = false;
        this.state.totalElements = chartData.labels.length;
        const useProgressive = chartData.labels.length > MAX_INITIAL_ITEMS;
        if (useProgressive) {
            this.state.isInitializing = true;
            this.createProgressiveChart(chartData);
            setTimeout(() => this.animateRemainingElements(chartData), 100);
            this.state.isInitializing = false;
        } else {
            this.createFullChart(chartData);
        }
    }

    createFullChart(chartData) {
        this.destroyChart();
        const stats = this.state.stats;
        this.chartInstance = new window.Chart(this.chartRef.el, {
            type: "bar",
            data: {
                labels: chartData.labels,
                datasets: chartData.datasets,
            },
            options: {
                onClick: (event, elements) => this.handleChartClick(elements),
                responsive: true,
                maintainAspectRatio: true,
                animation: { duration: 400 },
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: { label: (ctx) => ` ${ctx.raw}` },
                    },
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: { precision: 0 },
                        grid: { color: "rgba(0,0,0,0.06)" },
                    },
                    x: {
                        grid: { display: false },
                        ticks: {
                            maxRotation: 45,
                            minRotation: 25,
                            font: { size: 11 },
                        },
                    },
                },
            },
        });
    }

    createProgressiveChart(chartData) {
        this.destroyChart();
        const initialCount = Math.min(MAX_INITIAL_ITEMS, chartData.labels.length);
        const initialLabels = chartData.labels.slice(0, initialCount);
        const initialDatasets = chartData.datasets.map((ds) => ({
            ...ds,
            data: ds.data.slice(0, initialCount),
            backgroundColor: Array.isArray(ds.backgroundColor)
                ? ds.backgroundColor.slice(0, initialCount)
                : ds.backgroundColor,
            borderColor: Array.isArray(ds.borderColor)
                ? ds.borderColor.slice(0, initialCount)
                : ds.borderColor,
        }));
        this.state.loadedElements = initialCount;
        this.chartInstance = new window.Chart(this.chartRef.el, {
            type: "bar",
            data: { labels: initialLabels, datasets: initialDatasets },
            options: {
                onClick: (event, elements) => this.handleChartClick(elements),
                responsive: true,
                maintainAspectRatio: true,
                animation: { duration: 400 },
                plugins: {
                    legend: { display: false },
                    tooltip: { callbacks: { label: (ctx) => ` ${ctx.raw}` } },
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: { precision: 0 },
                        grid: { color: "rgba(0,0,0,0.06)" },
                    },
                    x: {
                        grid: { display: false },
                        ticks: {
                            maxRotation: 45,
                            minRotation: 25,
                            font: { size: 11 },
                        },
                    },
                },
            },
        });
    }

    animateRemainingElements(chartData) {
        if (!this.chartInstance || this.state.loadedElements >= this.state.totalElements) return;
        this.state.isAnimating = true;
        const startIndex = this.state.loadedElements;
        for (let i = startIndex; i < chartData.labels.length; i++) {
            const timeoutId = setTimeout(() => {
                this.chartInstance.data.labels.push(chartData.labels[i]);
                chartData.datasets.forEach((ds, di) => {
                    this.chartInstance.data.datasets[di].data.push(ds.data[i]);
                    if (Array.isArray(ds.backgroundColor) && i < ds.backgroundColor.length) {
                        this.chartInstance.data.datasets[di].backgroundColor.push(ds.backgroundColor[i]);
                    }
                    if (Array.isArray(ds.borderColor) && i < ds.borderColor.length) {
                        this.chartInstance.data.datasets[di].borderColor.push(ds.borderColor[i]);
                    }
                });
                this.state.loadedElements = i + 1;
                this.chartInstance.update();
                if (i === chartData.labels.length - 1) {
                    this.state.isAnimating = false;
                }
            }, (i - startIndex) * ANIMATION_DELAY);
            this.animationTimeouts.push(timeoutId);
        }
    }

    /**
     * Drill-down on bar click: open list view for that statistic's query (compliance-style).
     */
    async handleChartClick(elements) {
        if (!elements || elements.length === 0) return;
        const clickedIndex = elements[0].index;
        const stats = this.state.stats;
        const stat = stats[clickedIndex];
        if (!stat) return;
        await this.displayOdooView(stat.query, stat.name);
    }

    async displayOdooView(query, title) {
        try {
            if (!query) return;
            const cacheKey = `dynamic_sql_${this.state.cco}_${JSON.stringify(
                this.state.branches_id
            )}_${encodeURIComponent(query)}_${this.state.uniqueId}`;
            let response = await this.serverCache.getCache(cacheKey);
            if (!response) {
                response = await this.rpc("/dashboard/dynamic_sql", {
                    sql_query: query,
                    branches_id: this.state.branches_id,
                    cco: this.state.cco,
                });
                if (response && !response.error) {
                    await this.serverCache.setCache(cacheKey, response);
                }
            }
            if (!response || response.error || !response.domain) return;
            const displayTitle = title || "Chart Results";
            this.action.doAction({
                type: "ir.actions.act_window",
                res_model: response.table.replace(/_/g, "."),
                name: displayTitle,
                domain: response.domain,
                views: [
                    [false, "tree"],
                    [false, "form"],
                ],
            });
        } catch (e) {
            // ignore
        }
    }

    openStatisticsList() {
        this.action.doAction("fsdh_addons.action_alert_stat");
    }
}

AlertChart.template = "owl.AlertChart";

registry.category("actions").add("owl.alert_chart", AlertChart);
