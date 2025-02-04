/** @odoo-module */

import { registry } from "@web/core/registry";
import { KpiCard } from "./kpi/kpiCard";
import { ChartRenderer } from "./chartrender/chartrender";
import { useService } from "@web/core/utils/hooks";

const { Component, useState, onMounted, onWillStart } = owl;
import { session } from "@web/session"


export class IcomplyDashboard extends Component {
  setup() {
    this.api = useService("orm");
    this.rpc = useService("rpc");
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
      branches_id: [],
      cc:false,
      chartDomain: [],
      current_datepicked: null,
      previous_datepicked: null,
      riskratingchart: null,
      customerchart: null,
      transationstatechart: null,
    });

    onMounted(async () => {
      await this.loadInitialData();
    });
    
    onWillStart(async () => {
       
      await this.getcurrentuser();
      this.filterByDate();
    });
  }

  async getcurrentuser() {

    let result = await this.rpc("/dashboard/user");
    this.state.branches_id = result.branch;
    this.state.cc = result.group

    
  }

  async fetchTransactionCounts(domain) {
    const searchCounts = async (riskLevel) => {
      return this.api.searchCount("res.customer.transaction", [
        ["risk_level", "=", riskLevel],
        ...domain,
      ]);
    };

    const screenedCount = async () => {
      return this.api.searchCount("res.customer.transaction", [
        ["rule_id", "!=", null],
        ...domain,
      ]);
    };

    const totalCount = async () => {
      return this.api.searchCount("res.customer.transaction", domain);
    };

    const [
      lowriskCount,
      mediumriskCount,
      highriskCount,
      totalScreenedTransactionCount,
      totalTransactionCount,
    ] = await Promise.all([
      searchCounts("low"),
      searchCounts("medium"),
      searchCounts("high"),
      screenedCount(),
      totalCount(),
    ]);

    return {
      lowriskCount,
      mediumriskCount,
      highriskCount,
      totalScreenedTransactionCount,
      totalTransactionCount,
    };
  }

  filterByDate = async () => {
    const currentDate = moment().subtract(this.state.datepicked, "days");
    const previousDate = moment().subtract(this.state.datepicked * 2, "days");

    this.state.current_datepicked = currentDate.format("YYYY-MM-DD"); // YYYY-MM-DD format
    this.state.previous_datepicked = previousDate.format("YYYY-MM-DD"); // YYYY-MM-DD format

    await this.fetchcasestatus(this.state.branches_id);
    
  };

  fetchcasestatus = async (ids) => {

    try {
      const dateFilter =
        this.state.datepicked > 0
          ? [["create_date", ">=", this.state.current_datepicked]]
          : [];

      const branchFilter =
        ids.length > 0  && this.state.cc == false ? [["branch_id", "in", Array.from(ids)]] : [];
      const domain = [...dateFilter, ...branchFilter];

      this.state.chartDomain = domain

    

      const {
        lowriskCount,
        mediumriskCount,
        highriskCount,
        totalScreenedTransactionCount,
        totalTransactionCount,
      } = await this.fetchTransactionCounts(domain);

      this.state.kpi = {
        ...this.state.kpi,
        lowrisk: lowriskCount,
        mediumrisk: mediumriskCount,
        highrisk: highriskCount,
        totalScreenedTransactionCount,
        totaltransaction: totalTransactionCount,
        alertrulestotal: await this.api.searchCount("alert.rules", [...dateFilter]),
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

      await this.getTransactionRiskRatingChart();
      await this.getCustomerRatingChart();
      await this.getTransactionStateChart();
    } catch (error) {
      console.error("Error fetching data:", error);
    }
  };

  calculatePercentage = (count, total) => {
    return total === 0 ? "0%" : `${((count / total) * 100).toFixed(1)}%`;
  };

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
            [this.state.cc == false && "branch_id", "in", Array.from(this.state.branches_id)],
            riskLevel == "screened"
              ? ["rule_id", "!=", null]
              : ["risk_level", "=", riskLevel],
          ]
        : [
            [this.state.cc == false && "branch_id", "in", Array.from(this.state.branches_id)],
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
        domain:
          this.state.datepicked > 0
            ? [
                ["created_at", ">=", this.state.current_datepicked],
                [this.state.cc == false && "branch_id", "in", Array.from(this.state.branches_id)],
              ]
            : [[this.state.cc == false && "branch_id", "in", Array.from(this.state.branches_id)]],
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
   
     let domain = this.state.chartDomain;


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
    
    let domain = this.state.chartDomain
    
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
    let domain = this.state.chartDomain;

    const { labels, counts } = await this.getChartData(
      "res.customer.transaction",
      "state",
      domain
    );
    this.state.transationstatechart = {
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
