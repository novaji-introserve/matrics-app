/** @odoo-module */

import { registry } from "@web/core/registry";
import { KpiCard } from "./kpi/kpiCard";
import { ChartRenderer } from "./chartrender/chartrender";
import { useService } from "@web/core/utils/hooks";

const { Component, useState, onMounted, onWillStart } = owl;

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
        alertrulestotal: 0,
        highriskbypercent: "0",
        mediumriskbypercent: "0",
        lowriskpercentage: "0",
        totalrulespercentage: "0",
        lowriskinRespectToTotalRulesPercent: "0",
        mediumriskinRespectToTotalRulesPercentage: "0",
        highriskinRespectToTotalRulesPercentage: "0",
      },
      datepicked: 14,
      branches_id: [],
      cc: false,
      alert_rules_domain: null,
      chartDomain: [],
      current_datepicked: null,
      previous_datepicked: null,
      topbranch: [],
      topscreened: [],
      highriskcustomer: [],
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
    console.log(result);
    
    this.state.branches_id = result.branch;
    this.state.cc = result.group;
    this.state.alert_rules_domain = result.alert_rules_domain;
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
    const currentDate = moment();
    const previousDate = moment().subtract(this.state.datepicked, "days");

    this.state.current_datepicked = currentDate.format("YYYY-MM-DD"); // YYYY-MM-DD format
    this.state.previous_datepicked = previousDate.format("YYYY-MM-DD"); // YYYY-MM-DD format

    await this.fetchcasestatus(this.state.branches_id);
  };

  fetchcasestatus = async (ids) => {
    try {
      const dateFilter =
        this.state.datepicked > 0
          ? [
              ["date_created", ">=", this.state.previous_datepicked],
              ["date_created", "<=", this.state.current_datepicked],
            ]
          : [];

      const branchFilter =
        ids.length > 0 && this.state.cc == false
          ? [["branch_id", "in", Array.from(ids)]]
          : [];
      const domain = [...dateFilter, ...branchFilter];

      this.state.chartDomain = domain;

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
        alertrulestotal: await this.api.searchCount("alert.rules", dateFilter),

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

      await this.TopBranches();
      await this.TopTransactionRules();
      await this.highriskcustomer();
      
    } catch (error) {
      console.error("Error fetching data:", error);
    }
  };

  calculatePercentage = (count, total) => {
    return total === 0 ? "0%" : `${((count / total) * 100).toFixed(1)}%`;
  };

  loadInitialData = async () => {
    await this.TopBranches();
    await this.TopTransactionRules();
    await this.highriskcustomer();
    
  };
  // Display transactions based on risk level
  displayTransactionsByRisk = (riskLevel = "") => {
     const dateFilter =
       this.state.datepicked > 0
         ? [
             ["date_created", ">=", this.state.previous_datepicked],
             ["date_created", "<=", this.state.current_datepicked],
           ]
         : [];

     const branchFilter =
       this.state.branches_id.length > 0 && this.state.cc == false
         ? [["branch_id", "in", Array.from(this.state.branches_id)]]
         : [];

      const domain = [
        ...dateFilter,
        ...branchFilter
      ];

      if (riskLevel === "") {
    //   // Use strict equality
          return;

      }else if(riskLevel === "screened"){

        domain.push(["rule_id", "!=", null]);

       this.navigate.doAction({
        type: "ir.actions.act_window",
        res_model: "res.customer.transaction",
        name: "Screened Transaction",
        domain: domain, // Use the correctly constructed domain
        views: [
          [false, "tree"],
          [false, "form"],
        ],
       })

      }else{
        domain.push(["risk_level", "=", riskLevel]);
      this.navigate.doAction({
        type: "ir.actions.act_window",
        res_model: "res.customer.transaction",
        name: `${
          riskLevel.charAt(0).toUpperCase() + riskLevel.slice(1)
        } Transaction`,
        domain: domain, // Use the correctly constructed domain
        views: [
          [false, "tree"],
          [false, "form"],
        ],
      });
    }

  };

  displayProcessOdooView = () =>{
    const dateFilter =
       this.state.datepicked > 0
         ? [
             ["date_created", ">=", this.state.previous_datepicked],
             ["date_created", "<=", this.state.current_datepicked],
           ]
         : [];

        const domain = [...dateFilter];

        this.navigate.doAction({
        type: "ir.actions.act_window",
        res_model: "alert.rules",
        name: "processes",
        domain: domain, // Use the correctly constructed domain
        views: [
          [false, "tree"],
          [false, "form"],
        ],
      });
      
  }

  // Unified chart rendering function

  // top branches
  async TopBranches() {
    const response = await this.rpc("/dashboard/branch_by_customer", {
      cco: this.state.cc,
      branches_id: this.state.branches_id,
      datepicked: Number(this.state.datepicked),
    });
    
    
    this.state.topbranch = response;
  }
  async TopTransactionRules() {
    const response = await this.rpc("/dashboard/get_top_screening_rules", {
      cco: this.state.cc,
      branches_id: this.state.branches_id,
      datepicked: Number(this.state.datepicked),
    });

    this.state.topscreened = response;
  }
  async highriskcustomer() {
    const response = await this.rpc(
      "/dashboard/get_high_risk_customer_by_branch",
      {
        cco: this.state.cc,
        branches_id: this.state.branches_id,
        datepicked: Number(this.state.datepicked),
      }
    );

  
    
    this.state.highriskcustomer = response;
  }

 
}

IcomplyDashboard.template = "owl.IcomplyDashboard";
IcomplyDashboard.components = { KpiCard, ChartRenderer };

registry.category("actions").add("owl.icomply_dashboard", IcomplyDashboard);






