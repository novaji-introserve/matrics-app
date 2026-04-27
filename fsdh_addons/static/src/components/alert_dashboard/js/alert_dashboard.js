/** @odoo-module */

import { registry } from "@web/core/registry";
import { useService } from "@web/core/utils/hooks";
import { Component, useState, onWillStart, useRef, useEffect, onWillUnmount } from "@odoo/owl";

export class AlertDashboard extends Component {
    setup() {
        this.rpc = useService("rpc");
        this.action = useService("action");
        this.serverCache = useService("server_cache");
        this.user = useService("user");
        this.chartRef = useRef("chartCanvas");
        this.chartInstance = null;
        this.state = useState({
            stats: [],
            loading: true,
            cco: null,
            branches_id: [],
            uniqueId: null,
        });
        onWillStart(() => this.initialize());
        useEffect(
            () => {
                if (this.state.stats.length) {
                    const id = setTimeout(() => this.renderChart(), 0);
                    return () => clearTimeout(id);
                } else {
                    this.destroyChart();
                }
            },
            () => [this.state.stats]
        );
        onWillUnmount(() => this.destroyChart());
    }

    parseStatValue(val) {
        if (val === undefined || val === null) return 0;
        const s = String(val).replace(/,/g, "").trim();
        const n = parseFloat(s);
        return isNaN(n) ? 0 : n;
    }

    destroyChart() {
        if (this.chartInstance) {
            try {
                this.chartInstance.destroy();
            } catch (e) {}
            this.chartInstance = null;
        }
    }

    renderChart() {
        if (typeof window.Chart === "undefined" || !this.chartRef.el || !this.state.stats.length) {
            this.destroyChart();
            return;
        }
        this.destroyChart();
        const labels = this.state.stats.map((s) => s.name || s.code || "—");
        const values = this.state.stats.map((s) => this.parseStatValue(s.val));
        const colors = [
            "rgba(13, 110, 253, 0.8)",
            "rgba(25, 135, 84, 0.8)",
            "rgba(255, 193, 7, 0.8)",
            "rgba(220, 53, 69, 0.8)",
            "rgba(13, 202, 240, 0.8)",
            "rgba(111, 66, 193, 0.8)",
            "rgba(253, 126, 20, 0.8)",
        ];
        const backgroundColor = values.map((_, i) => colors[i % colors.length]);
        this.chartInstance = new window.Chart(this.chartRef.el, {
            type: "bar",
            data: {
                labels,
                datasets: [
                    {
                        label: "Value",
                        data: values,
                        backgroundColor,
                        borderColor: backgroundColor.map((c) => c.replace("0.8", "1")),
                        borderWidth: 1,
                    },
                ],
            },
            options: {
                responsive: true,
                maintainAspectRatio: true,
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            label: (ctx) => ` ${ctx.raw}`,
                        },
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

    async initialize() {
        await this.getCurrentUser();
        await this.loadStats();
    }

    async getCurrentUser() {
        try {
            const result = await this.rpc("/dashboard/user");
            if (result) {
                this.state.branches_id = result.branch || [];
                this.state.cco = result.is_cco || result.is_co || false;
                this.state.uniqueId = result.unique_id || null;
            }
            return result;
        } catch (e) {
            return null;
        }
    }

    async loadStats() {
        this.state.loading = true;
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
        this.state.loading = false;
    }

    openStatisticsList() {
        this.action.doAction("fsdh_addons.action_alert_stat");
    }

    /**
     * Open Odoo list view for a given SQL query, mirroring Compliance dashboard logic
     * (uses /dashboard/dynamic_sql and server_cache).
     */
    async displayOdooView(query, title) {
        try {
            if (!query) {
                return;
            }

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

            if (!response || response.error) {
                return;
            }

            const displayTitle = title || "Card Results";

            if (!response.domain || !Array.isArray(response.domain)) {
                return;
            }

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
            // ignore navigation errors
        }
    }

    /**
     * When a card is clicked, reuse displayOdooView so behavior matches Compliance.
     * If the query is invalid or aggregate-only, fall back to the Statistics list.
     */
    async openCardView(stat) {
        if (!stat || !stat.query) {
            this.openStatisticsList();
            return;
        }
        const before = this.state.stats.length; // just to use state so lints don't complain
        await this.displayOdooView(stat.query, stat.name);
        // if something went wrong (no navigation because response invalid), keep a safe fallback
        if (!before && this.state.stats.length === before) {
            this.openStatisticsList();
        }
    }

    scopeLabel(scope) {
        const labels = { bank: "Bank", branch: "BR", alert: "AL" };
        return scope ? (labels[scope] || String(scope).slice(0, 2).toUpperCase()) : "--";
    }
}

AlertDashboard.template = "owl.AlertDashboard";

registry.category("actions").add("owl.alert_dashboard", AlertDashboard);
