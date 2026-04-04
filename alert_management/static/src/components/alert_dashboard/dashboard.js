/** @odoo-module */

import { registry } from "@web/core/registry";
import { useService } from "@web/core/utils/hooks";
import { AlertDashboardChart } from "./line_chart";

const { Component, onWillStart, useState } = owl;

export class AlertDashboard extends Component {
    setup() {
        this.orm = useService("orm");
        this.action = useService("action");
        this.state = useState({
            isLoading: true,
            period: 7,
            cards: [],
            trend: { dates: [], labels: [], values: [] },
        });

        this.onPeriodChange = this.onPeriodChange.bind(this);
        this.openCard = this.openCard.bind(this);
        this.openTrendDay = this.openTrendDay.bind(this);

        onWillStart(async () => {
            await this.loadDashboard();
        });
    }

    get currentPeriodLabel() {
        if (this.state.period === 0) {
            return "Today";
        }
        if (this.state.period === 7) {
            return "Last 7 Days";
        }
        return "Last 30 Days";
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

    async onPeriodChange(ev) {
        this.state.period = Number(ev.target.value || 7);
        await this.loadDashboard();
    }

    openCard(card) {
        if (!card) {
            return;
        }

        if (card.id === "alerts_today") {
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
                    ["date_created", ">=", start],
                    ["date_created", "<", end],
                ],
                target: "current",
            });
            return;
        }

        const xmlIds = {
            alert_groups: "alert_management.action_alert_group",
            alert_rules: "alert_management.action_alert_rules",
            sql_queries: "alert_management.action_process_sql",
        };

        const xmlId = xmlIds[card.id];
        if (xmlId) {
            this.action.doAction(xmlId);
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
                ["date_created", ">=", start],
                ["date_created", "<", end],
            ],
            target: "current",
        });
    }
}

AlertDashboard.template = "alert_management.AlertDashboard";
AlertDashboard.components = { AlertDashboardChart };

registry.category("actions").add("alert_management_dashboard", AlertDashboard);
