/** @odoo-module */

import { registry } from "@web/core/registry";
import { useService } from "@web/core/utils/hooks";
import { AlertDashboardChart } from "./line_chart";

const { Component, onWillStart, onWillUnmount, useState } = owl;

export class AlertDashboard extends Component {
    setup() {
        this.orm = useService("orm");
        this.action = useService("action");
        this.state = useState({
            isLoading: true,
            period: 7,
            refreshRate: 5,
            cards: [],
            trend: { dates: [], labels: [], values: [] },
        });
        this.refreshTimer = null;

        this.onPeriodChange = this.onPeriodChange.bind(this);
        this.onRefreshRateChange = this.onRefreshRateChange.bind(this);
        this.openCard = this.openCard.bind(this);
        this.openTrendDay = this.openTrendDay.bind(this);
        this.handleVisibilityRefresh = this.handleVisibilityRefresh.bind(this);

        onWillStart(async () => {
            this.loadRefreshPreference();
            await this.loadDashboard();
            this.startAutoRefresh();
        });

        window.addEventListener("focus", this.handleVisibilityRefresh);
        document.addEventListener("visibilitychange", this.handleVisibilityRefresh);

        onWillUnmount(() => {
            this.stopAutoRefresh();
            window.removeEventListener("focus", this.handleVisibilityRefresh);
            document.removeEventListener("visibilitychange", this.handleVisibilityRefresh);
        });
    }

    get currentPeriodLabel() {
        if (this.state.period === 0) {
            return "Today";
        }
        if (this.state.period === 1) {
            return "Yesterday";
        }
        if (this.state.period === 7) {
            return "Last 7 Days";
        }
        return "Last 1 Month";
    }

    async loadDashboard() {
        this.state.isLoading = true;
        try {
            const data = await this.orm.call("alert.history", "get_alert_dashboard_data", [], {
                period: this.state.period,
            });
            this.state.cards = data.cards || [];
            this.state.trend = data.trend || { dates: [], labels: [], values: [] };
        } finally {
            this.state.isLoading = false;
        }
    }

    loadRefreshPreference() {
        try {
            const savedRate = Number(
                window.localStorage.getItem("alert_dashboard_refresh_rate")
            );
            if ([1, 5, 10, 30, 60].includes(savedRate)) {
                this.state.refreshRate = savedRate;
            }
        } catch (error) {
            console.error("Could not load alert refresh preference:", error);
        }
    }

    saveRefreshPreference() {
        try {
            window.localStorage.setItem(
                "alert_dashboard_refresh_rate",
                String(this.state.refreshRate)
            );
        } catch (error) {
            console.error("Could not save alert refresh preference:", error);
        }
    }

    startAutoRefresh() {
        this.stopAutoRefresh();
        const refreshRate = Number(this.state.refreshRate);
        if (![1, 5, 10, 30, 60].includes(refreshRate)) {
            return;
        }
        this.refreshTimer = setInterval(() => {
            this.loadDashboard().catch((error) => {
                console.error("Error refreshing alert dashboard:", error);
            });
        }, refreshRate * 60 * 1000);
    }

    stopAutoRefresh() {
        if (this.refreshTimer) {
            clearInterval(this.refreshTimer);
            this.refreshTimer = null;
        }
    }

    async onPeriodChange(ev) {
        this.state.period = Number(ev.target.value || 7);
        await this.loadDashboard();
    }

    async onRefreshRateChange(ev) {
        this.state.refreshRate = Number(ev.target.value || 5);
        this.saveRefreshPreference();
        this.startAutoRefresh();
    }

    async handleVisibilityRefresh() {
        if (document.visibilityState === "hidden") {
            return;
        }
        await this.loadDashboard();
    }

    openCard(card) {
        if (!card) {
            return;
        }
        if (card.id === "alert_total_today") {
            const today = new Date();
            const start = `${today.getFullYear()}-${String(today.getMonth() + 1).padStart(2, "0")}-${String(today.getDate()).padStart(2, "0")} 00:00:00`;
            const tomorrow = new Date(today);
            tomorrow.setDate(today.getDate() + 1);
            const end = `${tomorrow.getFullYear()}-${String(tomorrow.getMonth() + 1).padStart(2, "0")}-${String(tomorrow.getDate()).padStart(2, "0")} 00:00:00`;
            this.action.doAction({
                type: "ir.actions.act_window",
                name: "Today's Alerts",
                res_model: "alert.history",
                view_mode: "tree,form",
                domain: [
                    ["create_date", ">=", start],
                    ["create_date", "<", end],
                ],
                target: "current",
            });
            return;
        }
        if (card.action_xmlid) {
            this.action.doAction(card.action_xmlid);
        }
    }

    openTrendDay(payload) {
        if (!payload || !payload.date) {
            return;
        }

        const start = `${payload.date} 00:00:00`;
        const selectedDate = new Date(`${payload.date}T00:00:00`);
        selectedDate.setDate(selectedDate.getDate() + 1);
        const end = `${selectedDate.getFullYear()}-${String(selectedDate.getMonth() + 1).padStart(2, "0")}-${String(selectedDate.getDate()).padStart(2, "0")} 00:00:00`;

        this.action.doAction({
            type: "ir.actions.act_window",
            name: `Alerts on ${payload.label || payload.date}`,
            res_model: "alert.history",
            view_mode: "tree,form",
            domain: [
                ["create_date", ">=", start],
                ["create_date", "<", end],
            ],
            target: "current",
        });
    }
}

AlertDashboard.template = "alert_management.AlertDashboard";
AlertDashboard.components = { AlertDashboardChart };

registry.category("actions").add("alert_management_dashboard", AlertDashboard);
