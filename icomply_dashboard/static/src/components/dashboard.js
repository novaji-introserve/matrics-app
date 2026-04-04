/** @odoo-module */

import { loadJS } from "@web/core/assets";
import { registry } from "@web/core/registry";
import { useService } from "@web/core/utils/hooks";

const { Component, onWillStart, onWillUnmount, useEffect, useRef, useState } = owl;

const CHARTJS_CDN =
  "https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js";

function formatDate(date) {
  return date.toISOString().slice(0, 10);
}

function subtractDays(days) {
  const date = new Date();
  date.setDate(date.getDate() - days);
  return formatDate(date);
}

export class Dashboard extends Component {
  setup() {
    this.rpc = useService("rpc");
    this.action = useService("action");

    this.topBranchesRef = useRef("topBranchesChart");
    this.highRiskBranchesRef = useRef("highRiskBranchesChart");
    this.exceptionsRef = useRef("exceptionsChart");

    this.charts = {};
    this.state = useState({
      loaded: false,
      loading: true,
      datepicked: 14,
      cc: false,
      branches_id: [],
      topBranchesByAccounts: [],
      topHighRiskBranchesByAccounts: [],
      topTransactionExceptions: [],
    });

    onWillStart(async () => {
      await loadJS(CHARTJS_CDN);
      await this.loadContext();
      await this.refreshDashboard();
    });

    useEffect(
      () => {
        if (this.state.loaded) {
          this.renderCharts();
        }
      },
      () => [
        this.state.loaded,
        this.state.datepicked,
        JSON.stringify(this.state.topBranchesByAccounts),
        JSON.stringify(this.state.topHighRiskBranchesByAccounts),
        JSON.stringify(this.state.topTransactionExceptions),
      ]
    );

    onWillUnmount(() => {
      this.destroyCharts();
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
    };
  }

  get currentPeriodLabel() {
    const days = Number(this.state.datepicked);
    if (days === 0) {
      return "Today";
    }
    if (days === 7) {
      return "Last 7 days";
    }
    if (days === 14) {
      return "Last 14 days";
    }
    if (days === 30) {
      return "Last 30 days";
    }
    return `Last ${days} days`;
  }

  async refreshDashboard() {
    this.state.loading = true;
    const [topBranchesByAccounts, topHighRiskBranchesByAccounts, topTransactionExceptions] =
      await Promise.all([
        this.rpc("/dashboard/top_branches_by_accounts", this.payload),
        this.rpc("/dashboard/top_high_risk_branches_by_accounts", this.payload),
        this.rpc("/dashboard/get_top_screening_rules", this.payload),
      ]);

    this.state.topBranchesByAccounts = topBranchesByAccounts || [];
    this.state.topHighRiskBranchesByAccounts = topHighRiskBranchesByAccounts || [];
    this.state.topTransactionExceptions = topTransactionExceptions || [];
    this.state.loaded = true;
    this.state.loading = false;
  }

  async onDateChange(ev) {
    this.state.datepicked = Number(ev.target.value);
    await this.refreshDashboard();
  }

  destroyCharts() {
    Object.values(this.charts).forEach((chart) => chart && chart.destroy());
    this.charts = {};
  }

  renderCharts() {
    this.renderTopBranchesChart();
    this.renderHighRiskBranchesChart();
    this.renderExceptionsChart();
  }

  buildCommonOptions(title, subtitle) {
    return {
      responsive: true,
      maintainAspectRatio: false,
      animation: {
        duration: 500,
      },
      plugins: {
        legend: {
          display: false,
        },
        title: {
          display: true,
          text: title,
          color: "#14213d",
          font: {
            size: 16,
            weight: "600",
          },
          padding: {
            bottom: 4,
          },
        },
        subtitle: {
          display: !!subtitle,
          text: subtitle,
          color: "#5c677d",
          font: {
            size: 11,
          },
          padding: {
            bottom: 18,
          },
        },
      },
      scales: {
        x: {
          grid: {
            display: false,
          },
          ticks: {
            color: "#3c4858",
            maxRotation: 0,
            minRotation: 0,
          },
        },
        y: {
          beginAtZero: true,
          grid: {
            color: "rgba(20, 33, 61, 0.08)",
          },
          ticks: {
            color: "#3c4858",
            precision: 0,
          },
        },
      },
    };
  }

  renderTopBranchesChart() {
    if (this.charts.topBranches) {
      this.charts.topBranches.destroy();
    }

    const labels = this.state.topBranchesByAccounts.map((item) => item.name);
    const values = this.state.topBranchesByAccounts.map((item) => item.account_count);

    this.charts.topBranches = new Chart(this.topBranchesRef.el, {
      type: "bar",
      data: {
        labels,
        datasets: [
          {
            data: values,
            borderRadius: 10,
            backgroundColor: "#2563eb",
            hoverBackgroundColor: "#1d4ed8",
            maxBarThickness: 36,
          },
        ],
      },
      options: {
        ...this.buildCommonOptions(
          "Top 10 Branch by Accounts Opened",
          "Branches ranked by account volume"
        ),
        onClick: (_event, elements) => {
          if (!elements.length) {
            return;
          }
          const branch = this.state.topBranchesByAccounts[elements[0].index];
          this.openAccounts(branch.id);
        },
      },
    });
  }

  renderHighRiskBranchesChart() {
    if (this.charts.highRiskBranches) {
      this.charts.highRiskBranches.destroy();
    }

    const labels = this.state.topHighRiskBranchesByAccounts.map((item) => item.name);
    const values = this.state.topHighRiskBranchesByAccounts.map(
      (item) => item.high_risk_accounts
    );

    this.charts.highRiskBranches = new Chart(this.highRiskBranchesRef.el, {
      type: "bar",
      data: {
        labels,
        datasets: [
          {
            data: values,
            borderRadius: 10,
            backgroundColor: "#f97316",
            hoverBackgroundColor: "#ea580c",
            maxBarThickness: 36,
          },
        ],
      },
      options: {
        ...this.buildCommonOptions(
          "Top 10 High Risk Branch by Accounts",
          "Branches with the highest concentration of high-risk accounts"
        ),
        onClick: (_event, elements) => {
          if (!elements.length) {
            return;
          }
          const branch = this.state.topHighRiskBranchesByAccounts[elements[0].index];
          this.openAccounts(branch.id, true);
        },
      },
    });
  }

  renderExceptionsChart() {
    if (this.charts.exceptions) {
      this.charts.exceptions.destroy();
    }

    const labels = this.state.topTransactionExceptions.map((item) => item.name);
    const values = this.state.topTransactionExceptions.map((item) => item.count);

    this.charts.exceptions = new Chart(this.exceptionsRef.el, {
      type: "line",
      data: {
        labels,
        datasets: [
          {
            data: values,
            borderColor: "#059669",
            backgroundColor: "rgba(5, 150, 105, 0.14)",
            pointBackgroundColor: "#059669",
            pointBorderColor: "#ffffff",
            pointRadius: 4,
            pointHoverRadius: 6,
            fill: true,
            tension: 0.35,
          },
        ],
      },
      options: {
        ...this.buildCommonOptions(
          "Top Transaction Exception",
          "Most-triggered screening rules in the selected period"
        ),
        onClick: (_event, elements) => {
          if (!elements.length) {
            return;
          }
          const rule = this.state.topTransactionExceptions[elements[0].index];
          this.openTransactions(rule.id);
        },
      },
    });
  }

  getAccountDomain(branchId, highRiskOnly = false) {
    const domain = [["branch_id", "=", branchId]];
    if (highRiskOnly) {
      domain.push(["risk_level", "=", "high"]);
    }
    if (Number(this.state.datepicked) > 0) {
      domain.push(["date_created", ">=", subtractDays(Number(this.state.datepicked))]);
      domain.push(["date_created", "<=", formatDate(new Date())]);
    }
    if (!this.state.cc && this.state.branches_id.length) {
      domain.push(["branch_id", "in", this.state.branches_id]);
    }
    return domain;
  }

  getTransactionDomain(ruleId) {
    const domain = [["rule_id", "=", ruleId]];
    if (Number(this.state.datepicked) > 0) {
      domain.push(["date_created", ">=", `${subtractDays(Number(this.state.datepicked))} 00:00:00`]);
      domain.push(["date_created", "<=", `${formatDate(new Date())} 23:59:59`]);
    }
    if (!this.state.cc && this.state.branches_id.length) {
      domain.push(["branch_id", "in", this.state.branches_id]);
    }
    return domain;
  }

  openAccounts(branchId, highRiskOnly = false) {
    this.action.doAction({
      type: "ir.actions.act_window",
      name: highRiskOnly ? "High Risk Accounts" : "Branch Accounts",
      res_model: "res.partner.account",
      domain: this.getAccountDomain(branchId, highRiskOnly),
      views: [
        [false, "tree"],
        [false, "form"],
      ],
    });
  }

  openTransactions(ruleId) {
    this.action.doAction({
      type: "ir.actions.act_window",
      name: "Transaction Exceptions",
      res_model: "res.customer.transaction",
      domain: this.getTransactionDomain(ruleId),
      views: [
        [false, "tree"],
        [false, "form"],
      ],
    });
  }
}

Dashboard.template = "owl.Dashboard";

registry.category("actions").add("owl.dashboard", Dashboard);
