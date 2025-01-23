/** @odoo-module */

import { registry } from "@web/core/registry";
import { KpiCard } from "./kpi/kpiCard";
import { ChartRenderer } from "./chartrender/chartrender";
import { useService } from "@web/core/utils/hooks";

const { Component, useState, onMounted, onWillStart } = owl;

export class IcomplyDashboard extends Component {
  setup() {
    this.api = useService("orm");
    this.navigate = useService("action");
    this.state = useState({
      kpi: {
        lowrisk: 0,
        mediumrisk: 0,
        highrisk: 0,
        totaltransaction: 0,
        alertrultstotal: 0,
        highriskbypercent: "0",
        mediumriskbypercent: "0",
        lowriskpercentage: "0",
        totalrulespercentage: "0",
        lowriskinRespectToTotalRulesPercent: "0",
        mediumriskinRespectToTotalRulesPercentage: "0",
        highriskinRespectToTotalRulesPercentage: "0",
      },
      datepicked: 0,
    });

    onMounted(() => this.filterByDate());
    // onWillStart(() => this.loadInitialData());
  }

  // Helper function to fetch transaction counts based on domain
  async fetchTransactionCounts(domain = []) {
    const [
      lowriskCount,
      mediumriskCount,
      highriskCount,
      totalScreenedTransactionCount,
      totalTransactionCount,
    ] = await Promise.all([
      this.api.searchCount("res.customer.transaction", [
        ["risk_level", "=", "low"],
        ...domain,
      ]),
      this.api.searchCount("res.customer.transaction", [
        ["risk_level", "=", "medium"],
        ...domain,
      ]),
      this.api.searchCount("res.customer.transaction", [
        ["risk_level", "=", "high"],
        ...domain,
      ]),
      this.api.searchCount("res.customer.transaction", [
        ["rule_id", "!=", null],
        ...domain,
      ]),
      this.api.searchCount("res.customer.transaction", domain),
    ]);
    return {
      lowriskCount,
      mediumriskCount,
      highriskCount,
      totalScreenedTransactionCount,
      totalTransactionCount,
    };
  }

  // Filter by the selected date range
  filterByDate = async () => {
    const currentDate = moment().subtract(this.state.datepicked, "days");
    const previousDate = moment().subtract(this.state.datepicked * 2, "days");
    this.state.current_datepicked = currentDate.format("L");
    this.state.previous_datepicked = previousDate.format("L");
    this.fetchcasestatus();
  };

  // Fetch case status data based on the date range selected
  fetchcasestatus = async () => {
    try {
      const domain =
        this.state.datepicked > 0
          ? [["create_date", ">=", this.state.current_datepicked]]
          : [];

      const {
        lowriskCount,
        mediumriskCount,
        highriskCount,
        totalScreenedTransactionCount,
        totalTransactionCount,
      } = await this.fetchTransactionCounts(domain);

      this.state.kpi = {
        lowrisk: lowriskCount,
        mediumrisk: mediumriskCount,
        highrisk: highriskCount,
        totalScreenedTransactionCount,
        totaltransaction: totalTransactionCount,
        alertrulestotal: await this.api.searchCount("alert.rules", domain),
        lowriskinRespectToTotalTransaction: this.calculatePercentage(
          lowriskCount,
          totalTransactionCount
        ),
        mediumriskinRespectToTotalTransaction: this.calculatePercentage(
          mediumriskCount,
          totalTransactionCount
        ),
        highriskinRespectToTotalTransaction: this.calculatePercentage(
          highriskCount,
          totalTransactionCount
        ),
      };

      // Update chart data
      await this.getTransactionRiskRatingChart();
      await this.getCustomerRatingChart();
      await this.getTransactionStateChart();
    } catch (error) {
      console.error("Error fetching alert rules count:", error);
    }
  };

  // Calculate the percentage of a value in respect to the total
  calculatePercentage = (count, total) => {
    return total === 0 ? "0%" : `${((count / total) * 100).toFixed(1)}%`;
  };

  // Load initial data for charts
  loadInitialData = async () => {
    await this.getTransactionRiskRatingChart();
    await this.getCustomerRatingChart();
    await this.getTransactionStateChart();
  };

  // Display transactions based on risk level
  displayTransactionsByRisk = (riskLevel = "") => {
    const domain =
      this.state.datepicked > 0
        ? [
            ["created_at", ">=", this.state.current_datepicked],
            riskLevel == "screened"
              ? ["rule_id", "!=", null]
              : ["risk_level", "=", riskLevel],
          ]
        : [
            riskLevel == "screened"
              ? ["rule_id", "!=", null]
              : ["risk_level", "=", riskLevel],
          ];

    if (riskLevel == "") {
      return;
    } else if (riskLevel == "process") {
      this.navigate.doAction({
        type: "ir.actions.act_window",
        res_model: "alert.rules",
        name: "processes",
        domain: this.state.datepicked > 0
        ? [["created_at", ">=", this.state.current_datepicked]]
        : [],
        views: [
          [false, "tree"],
          [false, "form"],
        ],
      });
    } else {
      this.navigate.doAction({
        type: "ir.actions.act_window",
        res_model: "res.customer.transaction",
        name: `${
          riskLevel.charAt(0).toUpperCase() + riskLevel.slice(1)
        } Transaction`,
        domain,
        views: [
          [false, "tree"],
          [false, "form"],
        ],
      });
    }
  };

  // Unified chart rendering function
  async getChartData(model, field, domain) {
    const results = await this.api.searchRead(model, domain, [field]);
    const groupedData = results.reduce((acc, record) => {
      const key = record[field];
      acc[key] = acc[key] || { count: 0, name: key };
      acc[key].count++;
      return acc;
    }, {});

    const labels = Object.values(groupedData).map((data) => data.name);
    const counts = Object.values(groupedData).map((data) => data.count);

    return { labels, counts };
  }

  // Transaction Risk Rating Chart
  getTransactionRiskRatingChart = async () => {
    const domain =
      this.state.datepicked > 0
        ? [["date_created", ">=", this.state.current_datepicked]]
        : [];

    const { labels, counts } = await this.getChartData(
      "res.customer.transaction",
      "risk_level",
      domain
    );
    this.state.riskratingchart = {
      labels,
      datasets: [{ label: "", data: counts, hoverOffset: 4 }],
    };
  };

  // Customer Risk Rating Chart
  getCustomerRatingChart = async () => {
    const domain =
      this.state.datepicked > 0
        ? [["date_created", ">=", this.state.current_datepicked]]
        : [];

    const { labels, counts } = await this.getChartData(
      "res.partner",
      "risk_level",
      domain
    );
    this.state.customerchart = {
      labels,
      datasets: [{ label: "", data: counts, hoverOffset: 4 }],
    };
  };

  // Transaction State Chart
  getTransactionStateChart = async () => {
    const domain =
      this.state.datepicked > 0
        ? [["date_created", ">=", this.state.current_datepicked]]
        : [];

    const { labels, counts } = await this.getChartData(
      "res.customer.transaction",
      "state",
      domain
    );
    this.state.frequencychart = {
      labels,
      datasets: [
        {
          label: "",
          data: counts,
          backgroundColor: [
            "rgba(75, 192, 0, 0.5)",
            "rgba(255, 99, 132, 1)",
            "rgba(54, 162, 235, 0.2)",
          ],
          borderColor: [
            "rgba(75, 192, 192, 1)",
            "rgba(255, 99, 132, 1)",
            "rgba(54, 162, 235, 1)",
          ],
          borderWidth: 1,
        },
      ],
      options: {
        responsive: true,
        maintainAspectRatio: false,
        legend: { position: "right" },
      },
    };
  };
}

IcomplyDashboard.template = "owl.IcomplyDashboard";
IcomplyDashboard.components = { KpiCard, ChartRenderer };

registry.category("actions").add("owl.icomply_dashboard", IcomplyDashboard);
