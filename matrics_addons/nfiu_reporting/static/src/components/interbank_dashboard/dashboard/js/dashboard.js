/** @odoo-module */

import { registry } from "@web/core/registry";
import { useService } from "@web/core/utils/hooks";
import { InterbankChart } from "../../chart/js/chart";

const { Component, onWillStart, onWillUnmount, useState } = owl;

const PERIOD_STORAGE_KEY = "nfiu_interbank_dashboard_period";
const REFRESH_STORAGE_KEY = "nfiu_interbank_dashboard_refresh_rate";

export class InterbankDashboard extends Component {
    setup() {
        this.rpc = useService("rpc");
        this.state = useState({
            datepicked: 7,
            refreshRate: 5,
            loading: true,
            stats: [],
            charts: [],
            generatedAt: null,
        });
        this.refreshTimer = null;

        onWillStart(async () => {
            this._loadPeriodPreference();
            this._loadRefreshPreference();
            await this.fetchDashboard();
            this._restartRefreshTimer();
        });

        onWillUnmount(() => {
            if (this.refreshTimer) {
                clearInterval(this.refreshTimer);
                this.refreshTimer = null;
            }
        });
    }

    get currentPeriodLabel() {
        const period = Number(this.state.datepicked);
        if (period === 0) {
            return "Today";
        }
        if (period === 1) {
            return "Yesterday";
        }
        if (period === 7) {
            return "Last 7 days";
        }
        if (period === 30) {
            return "Last 1 month";
        }
        return "Last 7 days";
    }

    get formattedUpdatedAt() {
        if (!this.state.generatedAt) {
            return "Not yet refreshed";
        }
        return new Date(this.state.generatedAt).toLocaleString();
    }

    formatStatValue(stat) {
        const numericValue = Number(stat.value ?? stat.val ?? 0);
        if (stat.is_currency) {
            return new Intl.NumberFormat(undefined, {
                minimumFractionDigits: 2,
                maximumFractionDigits: 2,
            }).format(numericValue);
        }
        return new Intl.NumberFormat().format(numericValue);
    }

    _loadPeriodPreference() {
        try {
            const saved = Number(window.localStorage.getItem(PERIOD_STORAGE_KEY));
            if ([0, 1, 7, 30].includes(saved)) {
                this.state.datepicked = saved;
            }
        } catch {
            // Ignore storage access failures.
        }
    }

    _loadRefreshPreference() {
        try {
            const saved = Number(window.localStorage.getItem(REFRESH_STORAGE_KEY));
            if ([1, 5, 30, 60].includes(saved)) {
                this.state.refreshRate = saved;
            }
        } catch {
            // Ignore storage access failures.
        }
    }

    _savePeriodPreference() {
        try {
            window.localStorage.setItem(PERIOD_STORAGE_KEY, String(this.state.datepicked));
        } catch {
            // Ignore storage access failures.
        }
    }

    _saveRefreshPreference() {
        try {
            window.localStorage.setItem(
                REFRESH_STORAGE_KEY,
                String(this.state.refreshRate)
            );
        } catch {
            // Ignore storage access failures.
        }
    }

    _restartRefreshTimer() {
        if (this.refreshTimer) {
            clearInterval(this.refreshTimer);
            this.refreshTimer = null;
        }

        if (![1, 5, 30, 60].includes(Number(this.state.refreshRate))) {
            return;
        }

        this.refreshTimer = setInterval(() => {
            this.fetchDashboard();
        }, Number(this.state.refreshRate) * 60 * 1000);
    }

    async fetchDashboard() {
        this.state.loading = true;
        try {
            const [statsResult, chartsResult] = await Promise.all([
                this.rpc("/dashboard/stats", {
                    cco: true,
                    branches_id: [],
                    datepicked: this.state.datepicked,
                    dashboard_scope: "interbank",
                }),
                this.rpc("/dashboard/focused_charts", {
                    cco: true,
                    branches_id: [],
                    datepicked: this.state.datepicked,
                    dashboard_scope: "interbank",
                }),
            ]);
            this.state.stats = (statsResult.data || []).map((stat) => ({
                id: stat.id,
                label: stat.name,
                val: stat.val,
                scope: stat.scope,
                summary: stat.display_summary,
                accent: stat.scope_color || "#0f766e",
                is_currency: /value|amount/i.test(stat.name || ""),
            }));
            this.state.charts = chartsResult || [];
            this.state.generatedAt = new Date().toISOString();
        } finally {
            this.state.loading = false;
        }
    }

    async onDateChange(ev) {
        this.state.datepicked = Number(ev.target.value);
        this._savePeriodPreference();
        await this.fetchDashboard();
    }

    onRefreshRateChange(ev) {
        this.state.refreshRate = Number(ev.target.value);
        this._saveRefreshPreference();
        this._restartRefreshTimer();
    }
}

InterbankDashboard.template = "owl.InterbankDashboard";
InterbankDashboard.components = { InterbankChart };

registry.category("actions").add("owl.nfiu_interbank_dashboard", InterbankDashboard);
