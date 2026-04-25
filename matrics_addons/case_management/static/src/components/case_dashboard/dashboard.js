/** @odoo-module */

import { registry } from "@web/core/registry";
import { KpiCard } from "../kpi_card/kpi_card";
import { ChartRenderer } from "../chart_renderer/chart_renderer";
import { useService } from "@web/core/utils/hooks";

const { Component, onWillStart, onWillUnmount, useState } = owl;

export class CaseDashboard extends Component {
  setup() {
    this.state = useState({
      stats: [],
      period: 7,
      refreshRate: 5,
      caseStatusChart: {},
      caseRatingChart: {},
      isLoading: true,
    });
    this.refreshTimer = null;

    this.orm = useService("orm");
    this.rpc = useService("rpc");
    this.actionService = useService("action");
    this.user = useService("user");

    this.OnChangePeriod = this.OnChangePeriod.bind(this);
    this.onRefreshRateChange = this.onRefreshRateChange.bind(this);
    this.handleVisibilityRefresh = this.handleVisibilityRefresh.bind(this);
    this.openCard = this.openCard.bind(this);
    this.openCardById = this.openCardById.bind(this);

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
    const period = Number(this.state.period);
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
    return `${period} day window`;
  }

  getBaseDomain() {
    return [
      "|",
      "|",
      ["create_uid", "=", this.user.userId],
      ["officer_responsible", "=", this.user.userId],
      ["supervisors", "in", [this.user.userId]],
    ];
  }

  getDates() {
    const period = Number(this.state.period);
    let startDate;
    let endDate;

    if (period === 0) {
      startDate = moment().startOf("day");
      endDate = moment().endOf("day");
    } else if (period === 1) {
      startDate = moment().subtract(1, "day").startOf("day");
      endDate = moment().subtract(1, "day").endOf("day");
    } else if (period === 7) {
      startDate = moment().subtract(6, "days").startOf("day");
      endDate = moment().endOf("day");
    } else {
      startDate = moment().subtract(29, "days").startOf("day");
      endDate = moment().endOf("day");
    }

    this.state.current_date = startDate.format("YYYY/MM/DD HH:mm:ss");
    this.state.current_end_date = endDate.format("YYYY/MM/DD HH:mm:ss");
  }

  applyPeriodDomain(domain) {
    domain.push(["create_date", ">=", this.state.current_date]);
    domain.push(["create_date", "<=", this.state.current_end_date]);
  }

  async loadDashboard() {
    try {
      this.state.isLoading = true;
      this.getDates();
      await Promise.all([
        this.getCardStats(),
        this.getCaseStatusChart(),
        this.getCaseRatingChart(),
      ]);
    } catch (error) {
      console.error("Error loading case dashboard:", error);
    } finally {
      this.state.isLoading = false;
    }
  }

  loadRefreshPreference() {
    try {
      const savedRate = Number(
        window.localStorage.getItem("case_dashboard_refresh_rate")
      );
      if ([1, 5, 10, 30, 60].includes(savedRate)) {
        this.state.refreshRate = savedRate;
      }
    } catch (error) {
      console.error("Could not load case refresh preference:", error);
    }
  }

  saveRefreshPreference() {
    try {
      window.localStorage.setItem(
        "case_dashboard_refresh_rate",
        String(this.state.refreshRate)
      );
    } catch (error) {
      console.error("Could not save case refresh preference:", error);
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
        console.error("Error refreshing case dashboard:", error);
      });
    }, refreshRate * 60 * 1000);
  }

  stopAutoRefresh() {
    if (this.refreshTimer) {
      clearInterval(this.refreshTimer);
      this.refreshTimer = null;
    }
  }

  async OnChangePeriod() {
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

  async getCardStats() {
    try {
      const result = await this.rpc("/case_dashboard/stats", {
        period: Number(this.state.period),
      });
      this.state.stats = this.normalizeStats(result?.data);
    } catch (error) {
      console.error("Error fetching case card stats:", error);
      this.state.stats = [];
    }
  }

  normalizeStats(records) {
    if (!Array.isArray(records)) {
      return [];
    }
    return records
      .filter((stat) => stat && typeof stat === "object")
      .map((stat, index) => ({
        id: stat.id ?? `case_stat_${index}`,
        name: stat.name || "",
        display_summary: stat.display_summary || "",
        val: stat.val ?? 0,
        percentage: stat.percentage ?? "0.00",
        resource_model_uri: stat.resource_model_uri || false,
        search_view_id: stat.search_view_id || false,
        domain: Array.isArray(stat.domain) ? stat.domain : false,
      }));
  }

  async openCard(stat) {
    if (!stat) {
      return;
    }

    if (stat.resource_model_uri && Array.isArray(stat.domain)) {
      this.actionService.doAction({
        type: "ir.actions.act_window",
        name: stat.name,
        res_model: stat.resource_model_uri,
        domain: stat.domain,
        search_view_id: stat.search_view_id || undefined,
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

  openCardById(statId) {
    const selectedStat = this.state.stats.find(
      (stat) => String(stat.id) === String(statId)
    );
    this.openCard(selectedStat);
  }

  getKpiCardProps(stat) {
    const isClickable =
      stat && stat.resource_model_uri && Array.isArray(stat.domain);
    return {
      itemId: stat?.id,
      onClick: this.openCardById,
      statName: stat?.name,
      summary: stat?.display_summary,
      value: stat?.val,
      percentage: stat?.percentage,
      isClickable: Boolean(isClickable),
    };
  }

  async getCaseStatusChart() {
    try {
      const domain = this.getBaseDomain();

      this.applyPeriodDomain(domain);

      const context = { active_test: false };
      const groupedResults = await this.orm.readGroup(
        "case.manager",
        domain,
        ["case_status_count:count(id)"],
        ["case_status"],
        { lazy: false, context: context }
      );

      const statusCounts = {
        draft: 0,
        open: 0,
        closed: 0,
        overdue: 0,
        archived: 0,
      };

      groupedResults.forEach((group) => {
        const status = group.case_status;
        if (status && status in statusCounts) {
          statusCounts[status] = group.case_status_count;
        }
      });

      const archivedDomain = [...this.getBaseDomain()];
      archivedDomain.push(
        ["case_status", "=", "archived"],
        ["active", "=", false]
      );
      this.applyPeriodDomain(archivedDomain);
      const archivedCount = await this.orm.searchCount(
        "case.manager",
        archivedDomain
      );

      statusCounts.archived = archivedCount;

      const labels = ["Draft", "Open", "Closed", "Overdue", "Archived"];
      const counts = [
        statusCounts.draft,
        statusCounts.open,
        statusCounts.closed,
        statusCounts.overdue,
        statusCounts.archived,
      ];

      const backgroundColors = [
        "rgba(100, 116, 139, 0.82)",
        "rgba(37, 99, 235, 0.82)",
        "rgba(16, 185, 129, 0.82)",
        "rgba(239, 68, 68, 0.82)",
        "rgba(249, 115, 22, 0.82)",
      ];

      const borderColors = [
        "rgb(71, 85, 105)",
        "rgb(29, 78, 216)",
        "rgb(5, 150, 105)",
        "rgb(220, 38, 38)",
        "rgb(234, 88, 12)",
      ];

      let filteredLabels = [...labels];
      let filteredCounts = [...counts];
      let filteredBackgroundColors = [...backgroundColors];
      let filteredBorderColors = [...borderColors];

      if (counts.some((count) => count > 0)) {
        const nonZeroIndices = counts
          .map((count, index) => (count > 0 ? index : -1))
          .filter((index) => index !== -1);

        filteredLabels = nonZeroIndices.map((index) => labels[index]);
        filteredCounts = nonZeroIndices.map((index) => counts[index]);
        filteredBackgroundColors = nonZeroIndices.map(
          (index) => backgroundColors[index]
        );
        filteredBorderColors = nonZeroIndices.map(
          (index) => borderColors[index]
        );
      }

      this.state.caseStatusChart = {
        data: {
          labels: filteredLabels,
          datasets: [
            {
              label: "Case Status",
              data: filteredCounts,
              backgroundColor: filteredBackgroundColors,
              borderColor: filteredBorderColors,
              borderWidth: 1,
              hoverOffset: 15,
            },
          ],
        },
        allLabels: labels,
        allCounts: counts,
        domain,
        options: {
          onClick: (e, activeElements) => {
            if (activeElements && activeElements.length > 0) {
              const index = activeElements[0].index;
              if (index !== undefined && index < filteredLabels.length) {
                this.viewByStatus(filteredLabels[index]);
              }
            }
          },
          plugins: {
            legend: {
              position: "right",
              labels: {
                font: {
                  size: 11,
                },
                padding: 16,
                usePointStyle: true,
              },
            },
            tooltip: {
              enabled: true,
              backgroundColor: "rgba(15, 23, 42, 0.92)",
              titleColor: "#ffffff",
              bodyColor: "#e2e8f0",
              padding: 12,
              cornerRadius: 12,
              callbacks: {
                label: function (context) {
                  return `${context.label}: ${context.raw} cases`;
                },
              },
            },
          },
          responsive: true,
          maintainAspectRatio: false,
        },
      };
    } catch (error) {
      console.error("Error fetching case status data:", error);
      this.state.caseStatusChart = { data: { labels: [], datasets: [] } };
    }
  }

  async getCaseRatingChart() {
    try {
      const domain = this.getBaseDomain();

      this.applyPeriodDomain(domain);

      const groupedResults = await this.orm.readGroup(
        "case.manager",
        domain,
        ["case_rating_count:count(id)"],
        ["case_rating"],
        { lazy: false, context: { active_test: false } }
      );

      const ratingCounts = {
        low: 0,
        medium: 0,
        high: 0,
      };

      groupedResults.forEach((group) => {
        const rating = group.case_rating;
        if (rating && rating in ratingCounts) {
          ratingCounts[rating] = group.case_rating_count;
        }
      });

      const labels = ["Low", "Medium", "High"];
      const counts = [ratingCounts.low, ratingCounts.medium, ratingCounts.high];

      const backgroundColors = [
        "rgba(14, 165, 233, 0.82)",
        "rgba(245, 158, 11, 0.82)",
        "rgba(244, 63, 94, 0.82)",
      ];

      const borderColors = [
        "rgb(2, 132, 199)",
        "rgb(217, 119, 6)",
        "rgb(225, 29, 72)",
      ];

      this.state.caseRatingChart = {
        data: {
          labels: labels,
          datasets: [
            {
              label: "Case Rating",
              data: counts,
              backgroundColor: backgroundColors,
              borderColor: borderColors,
              borderWidth: 1,
              borderRadius: 3,
              borderSkipped: false,
              maxBarThickness: 56,
              hoverBackgroundColor: backgroundColors.map((color) =>
                color.replace("0.82", "0.96")
              ),
            },
          ],
        },
        domain,
        options: {
          onClick: (e, activeElements) => {
            if (activeElements && activeElements.length > 0) {
              const index = activeElements[0].index;
              if (index !== undefined && index < labels.length) {
                this.viewByRating(labels[index]);
              }
            }
          },
          plugins: {
            legend: {
              display: false,
            },
            tooltip: {
              enabled: true,
              backgroundColor: "rgba(15, 23, 42, 0.92)",
              titleColor: "#ffffff",
              bodyColor: "#e2e8f0",
              padding: 12,
              cornerRadius: 12,
              callbacks: {
                label: function (context) {
                  return `${context.label} Rating: ${context.raw} cases`;
                },
              },
            },
          },
          responsive: true,
          maintainAspectRatio: false,
          scales: {
            y: {
              beginAtZero: true,
              ticks: {
                precision: 0,
                font: {
                  size: 12,
                },
              },
              grid: {
                display: true,
                color: "rgba(15, 23, 42, 0.08)",
              },
            },
            x: {
              ticks: {
                font: {
                  size: 12,
                  weight: "bold",
                },
                color: "#334155",
              },
            },
          },
        },
      };
    } catch (error) {
      console.error("Error fetching case rating data:", error);
      this.state.caseRatingChart = { data: { labels: [], datasets: [] } };
    }
  }

  async viewByStatus(status) {
    if (!status) {
      return;
    }

    let statusValue;
    if (typeof status === "string") {
      statusValue = status;
    } else if (status && status.label) {
      statusValue = status.label;
    } else {
      return;
    }

    const statusMap = {
      Draft: "case_management.action_draft_cases",
      Open: "case_management.action_open_cases",
      Closed: "case_management.action_closed_cases",
      Overdue: "case_management.action_overdue_cases",
      Archived: "case_management.action_archived_cases",
    };

    const actionId = statusMap[statusValue];
    if (actionId) {
      this.actionService.doAction(actionId);
      return;
    }

    const domain = this.getBaseDomain();
    domain.push(["case_status", "=", statusValue.toLowerCase()]);

    this.applyPeriodDomain(domain);

    this.actionService.doAction({
      type: "ir.actions.act_window",
      name: `${statusValue} Cases`,
      res_model: "case.manager",
      domain: domain,
      views: [
        [false, "list"],
        [false, "form"],
      ],
      target: "current",
      context:
        statusValue.toLowerCase() === "archived"
          ? { active_test: false }
          : {},
    });
  }

  async viewByRating(rating) {
    if (!rating) {
      return;
    }

    let ratingValue;
    if (typeof rating === "string") {
      ratingValue = rating.toLowerCase();
    } else if (rating && rating.label) {
      ratingValue = rating.label.toLowerCase();
    } else {
      return;
    }

    const domain = this.getBaseDomain();
    domain.push(["case_rating", "=", ratingValue]);

    this.applyPeriodDomain(domain);

    this.actionService.doAction({
      type: "ir.actions.act_window",
      name: `${rating} Priority Cases`,
      res_model: "case.manager",
      domain: domain,
      views: [
        [false, "list"],
        [false, "form"],
      ],
      target: "current",
    });
  }
}

CaseDashboard.template = "CaseDashboard";
CaseDashboard.components = { KpiCard, ChartRenderer };

registry.category("actions").add("owl_case_dashboard", CaseDashboard);
