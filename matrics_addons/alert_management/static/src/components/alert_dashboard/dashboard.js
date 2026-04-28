/** @odoo-module */

import { registry } from "@web/core/registry";
import { useService } from "@web/core/utils/hooks";
import { AlertDashboardChart } from "./line_chart";

const { Component, onWillStart, onWillUnmount, useState } = owl;

export class AlertDashboard extends Component {
    setup() {
        this.orm = useService("orm");
        this.action = useService("action");
        this.isUnmounted = false;
        this.loadRequestId = 0;
        this.state = useState({
            isLoading: true,
            period: 7,
            refreshRate: 5,
            cards: [],
            charts: [],
        });
        this.refreshTimer = null;

        this.onPeriodChange = this.onPeriodChange.bind(this);
        this.onRefreshRateChange = this.onRefreshRateChange.bind(this);
        this.openCard = this.openCard.bind(this);
        this.openCardById = this.openCardById.bind(this);
        this.onCardClick = this.onCardClick.bind(this);
        this.openChartPoint = this.openChartPoint.bind(this);
        this.handleVisibilityRefresh = this.handleVisibilityRefresh.bind(this);

        onWillStart(async () => {
            this.loadRefreshPreference();
            await this.loadDashboard();
            this.startAutoRefresh();
        });

        window.addEventListener("focus", this.handleVisibilityRefresh);
        document.addEventListener("visibilitychange", this.handleVisibilityRefresh);

        onWillUnmount(() => {
            this.isUnmounted = true;
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
        if (this.isUnmounted) {
            return;
        }
        const requestId = ++this.loadRequestId;
        this.state.isLoading = true;
        try {
            const [data, chartDefinitions] = await Promise.all([
                this.orm.call("alert.history", "get_alert_dashboard_data", [], {
                    period: this.state.period,
                }),
                this.orm.searchRead(
                    "res.dashboard.charts",
                    [
                        ["scope", "=", "alert"],
                        ["state", "=", "active"],
                        ["active", "=", true],
                        ["is_visible", "=", true],
                    ],
                    ["name", "code", "display_summary", "chart_type", "display_order"],
                    { order: "display_order asc, id asc" }
                ),
            ]);
            if (this.isUnmounted || requestId !== this.loadRequestId) {
                return;
            }
            this.state.cards = this.normalizeCards(data.cards);
            this.state.charts = this.normalizeCharts(chartDefinitions, data.charts);
        } catch (error) {
            if (!this.isUnmounted && error?.message !== "Component is destroyed") {
                throw error;
            }
        } finally {
            if (!this.isUnmounted && requestId === this.loadRequestId) {
                this.state.isLoading = false;
            }
        }
    }

    normalizeCards(records) {
        if (!Array.isArray(records)) {
            return [];
        }
        return records
            .filter((card) => card && typeof card === "object")
            .map((card, index) => ({
                id: card.id ?? `alert_card_${index}`,
                title: card.title || "",
                value: card.value ?? 0,
                display_summary: card.display_summary || "",
                resource_model_uri: card.resource_model_uri || false,
                search_view_id: card.search_view_id || false,
                domain: Array.isArray(card.domain) ? card.domain : false,
            }));
    }

    normalizeCharts(definitions, payloads) {
        const payloadList = Array.isArray(payloads) ? payloads : [];
        const payloadByCode = new Map(
            payloadList
                .filter((chart) => chart && typeof chart === "object")
                .map((chart) => [String(chart.id), chart])
        );
        const definitionList = Array.isArray(definitions) ? definitions : [];

        return definitionList.map((definition) => {
            const payload = payloadByCode.get(String(definition.code)) || {};
            return {
                id: payload.id || definition.code,
                title: payload.title || definition.name || "",
                display_summary:
                    payload.display_summary || definition.display_summary || "",
                type: payload.type || definition.chart_type || "bar",
                model_name: payload.model_name || "alert.history",
                filter: payload.filter || false,
                labels: Array.isArray(payload.labels) ? payload.labels : [],
                ids: Array.isArray(payload.ids) ? payload.ids : [],
                point_domains: Array.isArray(payload.point_domains)
                    ? payload.point_domains
                    : [],
                additional_domain: Array.isArray(payload.additional_domain)
                    ? payload.additional_domain
                    : [],
                datasets: Array.isArray(payload.datasets)
                    ? payload.datasets
                    : [{ data: [], backgroundColor: [] }],
            };
        });
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
            if (this.isUnmounted) {
                return;
            }
            this.loadDashboard().catch((error) => {
                if (error?.message !== "Component is destroyed") {
                    console.error("Error refreshing alert dashboard:", error);
                }
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
        if (this.isUnmounted || document.visibilityState === "hidden") {
            return;
        }
        await this.loadDashboard();
    }

    async openCard(card) {
        if (!card) {
            return;
        }
        if (card.resource_model_uri && Array.isArray(card.domain)) {
            this.action.doAction({
                type: "ir.actions.act_window",
                name: card.title,
                res_model: card.resource_model_uri,
                domain: card.domain,
                search_view_id: card.search_view_id || undefined,
                context: {
                    search_default_active: 0,
                    search_default_inactive: 0,
                    search_default_state: 0,
                },
                views: [
                    [false, "tree"],
                    [false, "form"],
                ],
                target: "current",
            });
        }
    }

    openCardById(cardId) {
        const selectedCard = this.state.cards.find(
            (card) => String(card.id) === String(cardId)
        );
        this.openCard(selectedCard);
    }

    onCardClick(ev) {
        const cardId = ev.currentTarget?.dataset?.cardId;
        if (!cardId) {
            return;
        }
        this.openCardById(cardId);
    }

    getCardClass(card) {
        const isClickable =
            card && card.resource_model_uri && Array.isArray(card.domain);
        return `alert-kpi-card${isClickable ? " alert-kpi-card--clickable" : ""}`;
    }

    openDashboardChartPoint(chart, payload) {
        if (!chart || !payload) {
            return;
        }
        const filterField = chart.filter;
        const filterValue = payload.id ?? payload.label;
        if (!chart.model_name || !filterField || filterValue === undefined || filterValue === null) {
            return;
        }

        const domain = Array.isArray(chart.additional_domain) ? [...chart.additional_domain] : [];
        if (Array.isArray(payload.domain) && payload.domain.length) {
            domain.push(...payload.domain);
        } else if (filterField === "create_date") {
            const start = `${filterValue} 00:00:00`;
            const selectedDate = new Date(`${filterValue}T00:00:00`);
            selectedDate.setDate(selectedDate.getDate() + 1);
            const end = `${selectedDate.getFullYear()}-${String(selectedDate.getMonth() + 1).padStart(2, "0")}-${String(selectedDate.getDate()).padStart(2, "0")} 00:00:00`;
            domain.push([filterField, ">=", start]);
            domain.push([filterField, "<", end]);
        } else {
            domain.push([filterField, "=", filterValue]);
        }

        this.action.doAction({
            type: "ir.actions.act_window",
            name: chart.title || "Alert Chart",
            res_model: chart.model_name,
            domain,
            views: [
                [false, "tree"],
                [false, "form"],
            ],
            target: "current",
        });
    }

    openChartPoint(payload) {
        const chart = this.state.charts.find(
            (record) => String(record.id) === String(payload?.chartId)
        );
        if (chart && payload && payload.id !== undefined) {
            const pointIndex = chart.ids.findIndex((value) => String(value) === String(payload.id));
            if (pointIndex >= 0 && Array.isArray(chart.point_domains)) {
                payload.domain = chart.point_domains[pointIndex] || [];
            }
        }
        this.openDashboardChartPoint(chart, payload);
    }
}

AlertDashboard.template = "alert_management.AlertDashboard";
AlertDashboard.components = { AlertDashboardChart };

registry.category("actions").add("alert_management_dashboard", AlertDashboard);
