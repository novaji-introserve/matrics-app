/** @odoo-module */

import { loadJS } from "@web/core/assets";
import { registry } from "@web/core/registry";
import { useService } from "@web/core/utils/hooks";

const { Component, onWillStart, onWillUnmount, useEffect, useRef, useState } = owl;

const CHARTJS_CDN =
  "https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js";

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

const CHART_ORDER = [
  "transactions_by_branch",
  "transactions_by_currency",
  "transaction_volume_by_currency",
];

const CHART_REFS = {
  transactions_by_branch: "transactionsByBranchChart",
  transactions_by_currency: "transactionsByCurrencyChart",
  transaction_volume_by_currency: "transactionVolumeByCurrencyChart",
};

export class Dashboard extends Component {
  setup() {
    this.rpc = useService("rpc");
    this.action = useService("action");

    this.transactionsByBranchRef = useRef("transactionsByBranchChart");
    this.transactionsByCurrencyRef = useRef("transactionsByCurrencyChart");
    this.transactionVolumeByCurrencyRef = useRef("transactionVolumeByCurrencyChart");

    this.charts = {};
    this.state = useState({
      loaded: false,
      loading: true,
      datepicked: 0,
      refreshRate: 5,
      cc: false,
      branches_id: [],
      stats: [],
      focusedCharts: [],
      loadingStates: {
        stats: true,
        charts: true,
      },
    });
    this.refreshTimer = null;

    onWillStart(async () => {
      await loadJS(CHARTJS_CDN);
      this._loadPeriodPreference();
      this._loadRefreshPreference();
      await this.loadContext();
      await this.refreshDashboard();
    });

    useEffect(
      () => {
        if (this.state.loaded) {
          this.renderCharts();
        }
      },
      () => [this.state.loaded, this.state.datepicked, JSON.stringify(this.state.focusedCharts)]
    );

    onWillUnmount(() => {
      this.destroyCharts();
      if (this.refreshTimer) {
        clearInterval(this.refreshTimer);
        this.refreshTimer = null;
      }
    });
  }

  async loadContext() {
    const result = await this.rpc("/dashboard/user");
    this.state.cc = result.group;
    this.state.branches_id = result.branch || [];
  }

  get payload() {
    return {
      cco: this.state.cc,
      branches_id: this.state.branches_id,
      datepicked: Number(this.state.datepicked),
      dashboard_scope: "interbank",
    };
  }

  get currentPeriodLabel() {
    const days = Number(this.state.datepicked);
    if (days === 0) {
      return "Today";
    }
    if (days === 7) {
      return "Last 1 week";
    }
    if (days === 30) {
      return "Last 1 month";
    }
    return "Today";
  }

  get selectedCharts() {
    return CHART_ORDER.map((code) => this.getChart(code)).filter(Boolean);
  }

  get hasStats() {
    return Array.isArray(this.state.stats) && this.state.stats.length > 0;
  }

  getChart(code) {
    return (this.state.focusedCharts || []).find((chart) => chart.id === code);
  }

  hasChartData(chart) {
    if (!chart || !Array.isArray(chart.datasets) || !chart.datasets.length) {
      return false;
    }
    return chart.datasets.some(
      (dataset) => Array.isArray(dataset.data) && dataset.data.some((value) => Number(value || 0) !== 0)
    );
  }

  async refreshDashboard(options = {}) {
    const { silent = false } = options;
    this.state.loading = !silent;
    await Promise.all([this.fetchStats({ silent }), this.fetchCharts({ silent })]);
    this.state.loaded = true;
    this.state.loading = false;
    this._restartRefreshTimer();
  }

  async onDateChange(ev) {
    this.state.datepicked = Number(ev.target.value);
    this._savePeriodPreference();
    await this.refreshDashboard();
  }

  async onRefreshRateChange(ev) {
    this.state.refreshRate = Number(ev.target.value);
    this._saveRefreshPreference();
    this._restartRefreshTimer();
  }

  destroyCharts() {
    Object.values(this.charts).forEach((chart) => chart && chart.destroy());
    this.charts = {};
  }

  _loadRefreshPreference() {
    try {
      const savedRate = Number(
        window.localStorage.getItem("internal_control_dashboard_refresh_rate")
      );
      if ([1, 5, 30, 60].includes(savedRate)) {
        this.state.refreshRate = savedRate;
      }
    } catch (_error) {}
  }

  _loadPeriodPreference() {
    try {
      const savedPeriod = Number(
        window.localStorage.getItem("internal_control_dashboard_period")
      );
      if ([0, 7, 30].includes(savedPeriod)) {
        this.state.datepicked = savedPeriod;
      }
    } catch (_error) {}
  }

  _saveRefreshPreference() {
    try {
      window.localStorage.setItem(
        "internal_control_dashboard_refresh_rate",
        String(this.state.refreshRate)
      );
    } catch (_error) {}
  }

  _savePeriodPreference() {
    try {
      window.localStorage.setItem(
        "internal_control_dashboard_period",
        String(this.state.datepicked)
      );
    } catch (_error) {}
  }

  _restartRefreshTimer() {
    if (this.refreshTimer) {
      clearInterval(this.refreshTimer);
      this.refreshTimer = null;
    }

    const refreshRate = Number(this.state.refreshRate);
    if (![1, 5, 30, 60].includes(refreshRate)) {
      return;
    }

    this.refreshTimer = setInterval(() => {
      this.refreshDashboard({ silent: true }).catch(() => {});
    }, refreshRate * 60 * 1000);
  }

  _normalizeStats(records) {
    if (!Array.isArray(records)) {
      return [];
    }
    return records
      .filter((item) => item && typeof item === "object")
      .map((item) => ({
        id: item.id ?? null,
        name: item.name ?? "",
        scope: item.scope ?? "",
        val: item.val ?? 0,
        scope_color: item.scope_color ?? "#2563eb",
        display_summary: item.display_summary ?? "",
        resource_model_uri: item.resource_model_uri ?? false,
        search_view_id: item.search_view_id ?? false,
        domain: item.domain ?? false,
      }))
      .filter((item) => item.id !== null);
  }

  async fetchStats(options = {}) {
    const { silent = false } = options;
    if (!silent) {
      this.state.loadingStates.stats = true;
    }
    try {
      const result = await this.rpc("/dashboard/stats", this.payload);
      this.state.stats = this._normalizeStats(result?.data || []);
    } catch (_error) {
      this.state.stats = [];
    } finally {
      this.state.loadingStates.stats = false;
    }
  }

  async fetchCharts(options = {}) {
    const { silent = false } = options;
    if (!silent) {
      this.state.loadingStates.charts = true;
    }
    try {
      const focusedCharts = await this.rpc("/dashboard/focused_charts", this.payload);
      this.state.focusedCharts = Array.isArray(focusedCharts) ? focusedCharts : [];
    } catch (_error) {
      this.state.focusedCharts = [];
    } finally {
      this.state.loadingStates.charts = false;
    }
  }

  getScopeLabel(scope) {
    if (scope === "interbank") {
      return "Transaction Monitoring";
    }
    return scope || "Metric";
  }

  formatStatValue(value) {
    if (value === null || value === undefined || value === "") {
      return "0";
    }

    const normalized = String(value).replace(/,/g, "").trim();
    const numericValue = Number(normalized);
    if (Number.isNaN(numericValue)) {
      return String(value);
    }

    const hasFraction = normalized.includes(".");
    return new Intl.NumberFormat("en-US", {
      minimumFractionDigits: hasFraction ? 2 : 0,
      maximumFractionDigits: hasFraction ? 2 : 0,
    }).format(numericValue);
  }

  getScopeBadge(scope) {
    if (scope === "interbank") {
      return "TM";
    }
    return (scope || "--").slice(0, 2).toUpperCase();
  }

  getStatCardClass(stat) {
    const isClickable = Boolean(stat?.resource_model_uri && stat?.domain !== false);
    return `cm-kpi-card${isClickable ? " cm-kpi-card--clickable" : ""}`;
  }

  openStatCard(stat) {
    if (!stat?.resource_model_uri || !Array.isArray(stat.domain)) {
      return;
    }

    this.action.doAction({
      type: "ir.actions.act_window",
      res_model: stat.resource_model_uri,
      name: stat.name || "Card Results",
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
    });
  }

  onStatCardClick(ev) {
    const statId = String(ev?.currentTarget?.dataset?.statId || "");
    const stat = (this.state.stats || []).find((item) => String(item.id) === statId);
    if (stat) {
      this.openStatCard(stat);
    }
  }

  renderCharts() {
    for (const code of CHART_ORDER) {
      this.renderChart(code);
    }
  }

  renderChart(code) {
    const chart = this.getChart(code);
    const ref = this[this._refPropertyName(code)];
    if (this.charts[code]) {
      this.charts[code].destroy();
      delete this.charts[code];
    }
    if (!chart || !ref || !ref.el || !this.hasChartData(chart)) {
      return;
    }

    const chartType = chart.type || "bar";
    const normalizedChart = this._normalizeChartData(chart, chartType);

    this.charts[code] = new Chart(ref.el, {
      type: chartType,
      data: {
        labels: normalizedChart.labels || [],
        datasets: normalizedChart.datasets || [],
      },
      options: {
        ...this.buildCommonOptions(normalizedChart, chartType),
        onClick: (event, elements) => {
          const clickedIndex = this._resolveChartClickIndex(code, event, elements);
          if (clickedIndex < 0) {
            return;
          }
          this.openChartRecord(chart, clickedIndex);
        },
      },
    });
  }

  _refPropertyName(code) {
    const refName = CHART_REFS[code];
    return refName ? refName.replace(/Chart$/, "Ref") : null;
  }

  buildCommonOptions(chart, chartType) {
    const isCircularChart = chartType === "pie" || chartType === "doughnut";
    return {
      responsive: true,
      maintainAspectRatio: false,
      animation: {
        duration: 550,
        easing: "easeOutQuart",
      },
      interaction: {
        mode: "nearest",
        axis: isCircularChart ? undefined : "x",
        intersect: false,
      },
      hover: {
        mode: "nearest",
        intersect: false,
      },
      plugins: {
        legend: {
          display: chartType !== "bar",
          position: isCircularChart ? "right" : "top",
          labels: {
            color: MODERN_PALETTES.text,
            usePointStyle: true,
            boxWidth: 10,
          },
        },
        title: {
          display: false,
        },
        subtitle: {
          display: false,
        },
        tooltip: {
          enabled: true,
          backgroundColor: "rgba(15, 23, 42, 0.92)",
          titleColor: "#ffffff",
          bodyColor: "#e2e8f0",
          padding: 12,
          cornerRadius: 12,
          displayColors: chartType !== "line",
        },
      },
      scales: {
        x: {
          grid: {
            display: isCircularChart,
          },
          ticks: {
            color: MODERN_PALETTES.text,
            maxRotation: 0,
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
          },
        },
      },
    };
  }

  _getPaletteKey(title) {
    const lowerTitle = String(title || "").toLowerCase();
    if (lowerTitle.includes("high risk")) {
      return "highRisk";
    }
    if (lowerTitle.includes("exception") || lowerTitle.includes("screened")) {
      return "exceptions";
    }
    return "accounts";
  }

  _buildBarColors(palette, count) {
    return Array.from({ length: count }, (_, index) => palette[index % palette.length]);
  }

  _normalizeChartData(sourceData, chartType) {
    const paletteKey = this._getPaletteKey(sourceData.title);
    const labels = sourceData.labels || [];
    const datasets = (sourceData.datasets || []).map((dataset) => {
      if (chartType === "line") {
        return {
          ...dataset,
          label: dataset.label || sourceData.title,
          data: dataset.data || [],
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
      const colors = this._buildBarColors(palette, labels.length);
      if (chartType === "pie" || chartType === "doughnut") {
        return {
          ...dataset,
          label: dataset.label || sourceData.title,
          data: dataset.data || [],
          backgroundColor: colors,
          borderColor: colors.map(() => "#ffffff"),
          borderWidth: 2,
          hoverOffset: 10,
        };
      }
      return {
        ...dataset,
        label: dataset.label || sourceData.title,
        data: dataset.data || [],
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
      labels,
      datasets,
    };
  }

  openChartRecord(chart, index) {
    if (!chart || !chart.model_name) {
      return;
    }
    const domain = Array.isArray(chart.additional_domain) ? [...chart.additional_domain] : [];
    const filterValue = this._resolveChartFilterValue(chart, index);
    if (chart.filter && filterValue !== undefined && filterValue !== null && filterValue !== "") {
      domain.push([chart.filter, "=", filterValue]);
    }
    this.action.doAction({
      type: "ir.actions.act_window",
      name: chart.title || "Dashboard Chart",
      res_model: chart.model_name,
      domain,
      views: [
        [false, "tree"],
        [false, "form"],
      ],
      target: "current",
    });
  }

  _resolveChartClickIndex(code, event, elements) {
    if (Array.isArray(elements) && elements.length) {
      const directIndex = elements[0]?.index;
      if (Number.isInteger(directIndex)) {
        return directIndex;
      }
    }

    const chartInstance = this.charts[code];
    if (!chartInstance || !event) {
      return -1;
    }

    const nearestElements = chartInstance.getElementsAtEventForMode(
      event,
      "nearest",
      { intersect: false, axis: "xy" },
      true
    );
    const nearestIndex = nearestElements?.[0]?.index;
    return Number.isInteger(nearestIndex) ? nearestIndex : -1;
  }

  _resolveChartFilterValue(chart, index) {
    if (Array.isArray(chart.ids)) {
      const explicitId = chart.ids[index];
      if (explicitId !== undefined && explicitId !== null && explicitId !== "") {
        return explicitId;
      }
    }
    return Array.isArray(chart.labels) ? chart.labels[index] : undefined;
  }
}

Dashboard.template = "owl.Dashboard";

registry.category("actions").add("owl.dashboard", Dashboard);
