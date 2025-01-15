/** @odoo-module */

import { registry } from "@web/core/registry";
import { KpiCard } from "./kpi/kpiCard";

import { ChartRenderer } from "./chartrender/chartrender";
import { useService } from "@web/core/utils/hooks";

const { Component, useState, onWillStart, onWillUnmount, useRef, onMounted } =
  owl;

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

    onMounted(async () => {
      await this.filterByDate();
    });

    onWillStart(async () => {
      await this.getTransactionRiskRatingChart();
      await this.getCustomerRatingChart();
      await this.getTransactionStateChart();
    });
  }

  filterByDate = async () => {
    this.state.current_datepicked = moment()
      .subtract(this.state.datepicked, "days")
      .format("L");
    this.state.previous_datepicked = moment()
      .subtract(this.state.datepicked * 2, "days")
      .format("L");

    this.fetchcasestatus();
  };

  fetchcasestatus = async () => {
    try {
      if (
        new Date().toDateString() ==
        new Date(this.state.current_datepicked).toDateString()
      ) {
        let alerttulesCount = await this.api.searchCount("alert.rules", []);
  
        let lowriskCount = await this.api.searchCount(
          "res.customer.transaction",
          [["risk_level", "=", "low"]]
        );

        let mediumriskCount = await this.api.searchCount(
          "res.customer.transaction",
          [["risk_level", "=", "medium"]]
        );
        let highriskCount = await this.api.searchCount(
          "res.customer.transaction",
          [["risk_level", "=", "high"]]
        );
        let totalScreenedTransactionCount = await this.api.searchCount(
          "res.customer.transaction",
          [["rule_id", "!=", null]]
        );
        let totalTransactionCount = await this.api.searchCount(
          "res.customer.transaction",
          []
        );
        this.state.kpi.lowrisk = lowriskCount;
        this.state.kpi.mediumrisk = mediumriskCount;
        this.state.kpi.highrisk = highriskCount;
        this.state.kpi.totalScreenedTransactionCount =
          totalScreenedTransactionCount;
        this.state.kpi.totaltransaction = totalTransactionCount;
        this.state.kpi.alertrulestotal = alerttulesCount;
        // each risk count in respect to all records
        this.state.kpi.lowriskinRespectToTotalTransaction = `${(
          (lowriskCount / totalTransactionCount) *
          100
        ).toFixed(1)}%`;
        this.state.kpi.mediumriskinRespectToTotalTransaction = `${(
          (mediumriskCount / totalTransactionCount) *
          100
        ).toFixed(1)}%`;
        this.state.kpi.highriskinRespectToTotalTransaction = `${(
          (highriskCount / totalTransactionCount) *
          100
        ).toFixed(1)}%`;
      } else {
        let alerttulesCount = await this.api.searchCount("alert.rules", [
          ["create_date", ">=", this.state.current_datepicked],
        ]);
        
        let totalTransactionCount = await this.api.searchCount("res.customer.transaction", [
          ["create_date", ">=", this.state.current_datepicked],
        ]);
       
        

        // let lowriskCount = await this.api.searchCount(
        //   "res.customer.transaction",
        //   [
        //     ["risk_rating", "=", "low"],
        //     ["create_date", ">=", this.state.current_datepicked],
        //   ]
        // );
        
        let mediumriskCount = await this.api.searchCount(
          "res.customer.transaction",
          [
            ["risk_rating", "=", "medium"],
            ["create_date", ">=", this.state.current_datepicked],
          ]
        );
        // let highriskCount = await this.api.searchCount(
        //   "res.customer.transaction",
        //   [
        //     ["risk_rating", "=", "high"],
        //     ["create_date", ">=", this.state.current_datepicked],
        //   ]
        // );
        // let totalScreenedTransactionCount = await this.api.searchCount(
        //   "res.customer.transaction",
        //   [
        //     ["rule_id", "!=", null],
        //     ["create_date", ">=", this.state.current_datepicked],
        //   ]
        // );

        alert(lowriskCount)
        // alert(totalScreenedTransactionCount)
        // alert(highriskCount)

        // this.state.kpi.lowrisk = lowriskCount;
        // this.state.kpi.mediumrisk = mediumriskCount;
        // this.state.kpi.highrisk = highriskCount;
        // this.state.kpi.totalScreenedTransactionCount =
        //   totalScreenedTransactionCount;
        // this.state.kpi.totaltransaction = totalTransactionCount;
        // this.state.kpi.alertrulestotal = alerttulesCount;

        // alert("this is lowrisk "+lowriskCount);
        // alert("this is medium "+mediumriskCount);
        // alert("this is highrisk "+highriskCount);
        // alert("this is totalscreened "+totalScreenedTransactionCount);
        // alert("this is totaltransaction "+totalTransactionCount);
        // alert("this is totaltransaction "+alerttulesCount);
        // // each risk count in respect to all records
        // this.state.kpi.lowriskinRespectToTotalTransaction = `${(
        //   (lowriskCount / totalTransactionCount) *
        //   100
        // ).toFixed(1)}%`;
        // this.state.kpi.mediumriskinRespectToTotalTransaction = `${(
        //   (mediumriskCount / totalTransactionCount) *
        //   100
        // ).toFixed(1)}%`;
        // this.state.kpi.highriskinRespectToTotalTransaction = `${(
        //   (highriskCount / totalTransactionCount) *
        //   100
        // ).toFixed(1)}%`;
        // // this calculate total data in range of the date filtered and last occurrence o the date
        // let total_rules_prev_count = await this.api.searchCount("res.customer.transaction", [
        //   ["created_date", "<", this.state.current_datepicked],
        //   ["created_date", ">=", this.state.previous_datepicked],
        // ]);


        // let low_risk_prev_count = await this.api.searchCount("res.customer.transaction", [
        //   ["risk_rating", "=", "low"],
        //   ["created_date", "<", this.state.current_datepicked],
        //   ["created_date", ">=", this.state.previous_datepicked],
        // ]);
        // let medium_risk_prev_count = await this.api.searchCount("res.customer.transaction", [
        //   ["risk_rating", "=", "medium"],
        //   ["created_date", "<", this.state.current_datepicked],
        //   ["created_date", ">=", this.state.previous_datepicked],
        // ]);
        // let high_risk_prev_count = await this.api.searchCount("res.customer.transaction", [
        //   ["risk_rating", "=", "high"],
        //   ["created_date", "<", this.state.current_datepicked],
        //   ["created_date", ">=", this.state.previous_datepicked],
        // ]);
        // this.state.kpi.totaltransactionpercentage =
        //   total_rules_prev_count == 0
        //     ? 0
        //     : (
        //         ((totalTransactionCount - total_rules_prev_count) /
        //           total_rules_prev_count) *
        //         100
        //       ).toFixed(2);
        // this.state.kpi.lowriskpercentage =
        //   low_risk_prev_count == 0
        //     ? 0
        //     : (
        //         ((lowriskCount - low_risk_prev_count) / low_risk_prev_count) *
        //         100
        //       ).toFixed(2);
        // this.state.kpi.mediumriskbypercent =
        //   medium_risk_prev_count == 0
        //     ? 0
        //     : (
        //         ((mediumriskCount - medium_risk_prev_count) /
        //           medium_risk_prev_count) *
        //         100
        //       ).toFixed(2);
        // this.state.kpi.highriskbypercent =
        //   high_risk_prev_count == 0
        //     ? 0
        //     : (
        //         ((highriskCount - high_risk_prev_count) /
        //           high_risk_prev_count) *
        //         100
        //       ).toFixed(2);
      }
      // reload chart on select change
      await this.getTransactionRiskRatingChart();
      await this.getCustomerRatingChart();
      await this.getTransactionStateChart();
    } catch (error) {
      console.error("Error fetching alert rules count:", error);
    }
  };

  displayHighTransaction() {
    this.navigate.doAction({
      type: "ir.actions.act_window",
      res_model: "res.customer.transaction",
      name: "High Transaction",
      domain:
        this.state.datepicked > 0
          ? [
              ["created_at", ">=", this.state.current_datepicked],
              ["risk_level", "=", "high"],
            ]
          : [["risk_level", "=", "high"]],
      views: [
        [false, "tree"],
        [false, "form"],
      ],
    });
  }
  displayMediumTransaction() {
    this.navigate.doAction({
      type: "ir.actions.act_window",
      res_model: "res.customer.transaction",
      name: "Medium Transaction",
      domain:
        this.state.datepicked > 0
          ? [
              ["created_at", ">=", this.state.current_datepicked],
              ["risk_level", "=", "medium"],
            ]
          : [["risk_level", "=", "medium"]],
      views: [
        [false, "tree"],
        [false, "form"],
      ],
    });
  }
  displayLowTransaction() {
    this.navigate.doAction({
      type: "ir.actions.act_window",
      res_model: "res.customer.transaction",
      name: "Low Transaction",
      domain:
        this.state.datepicked > 0
          ? [
              ["created_at", ">=", this.state.current_datepicked],
              ["risk_level", "=", "low"],
            ]
          : [["risk_level", "=", "low"]],
      views: [
        [false, "tree"],
        [false, "form"],
      ],
    });
  }
  displayScreenedTransaction() {
    this.navigate.doAction({
      type: "ir.actions.act_window",
      res_model: "res.customer.transaction",
      name: "Screened Transaction",
      domain:
        this.state.datepicked > 0
          ? [
              ["created_at", ">=", this.state.current_datepicked],
              ["rule_id", "!=", null],
            ]
          : [["rule_id", "!=", null]],
      views: [
        [false, "tree"],
        [false, "form"],
      ],
    });
  }
  displayTotalProcess() {
    this.navigate.doAction({
      type: "ir.actions.act_window",
      res_model: "alert.rules",
      name: "Processes",
      domain:
        this.state.datepicked > 0
          ? [["created_at", ">=", this.state.current_datepicked]]
          : [],
      views: [
        [false, "tree"],
        [false, "form"],
      ],
    });
  }
  displayNothing() {}

  // chart implementation

  getTransactionRiskRatingChart = async () => {
    if (this.state.datepicked == 0) {
      // Use read_group to perform aggregation and group by priority_level_id
      const results = await this.api.searchRead(
        "res.customer.transaction", // The model to query
        [], // No specific domain/filter (can be customized)
        ["risk_level"] // Fields to group by (priority_level_id)
      );

      console.log(results);
      

      const groupedData = {};
      results.forEach((record) => {
        const Name = record.risk_level; // Get priority_level_id.name (Name)
        if (!groupedData[Name]) {
          groupedData[Name] = { count: 0, name: Name };
        }
        groupedData[Name].count++;
      });

      // Prepare the data for Chart.js
      const labels = [];
      const counts = [];

      

      Object.values(groupedData).forEach((data) => {
        labels.push(data.name); // Priority name
        counts.push(data.count); // Priority count
      });

      this.state.riskratingchart = {
        labels: labels,
        datasets: [
          {
            label: "",
            data: counts,
            hoverOffset: 4,
          },
        ],
      };
    } else if (this.state.datepicked > 0) {
      const results = await this.api.searchRead(
        "res.customer.transaction", // The model to query
        [["date_created", ">=", this.state.current_datepicked]], // No specific domain/filter (can be customized)
        ["risk_level"] // Fields to group by (priority_level_id)
      );

       console.log(results);

      const groupedData = {};
      results.forEach((record) => {
        const Name = record.risk_level; // Get priority_level_id.name (Name)
        if (!groupedData[Name]) {
          groupedData[Name] = { count: 0, name: Name };
        }
        groupedData[Name].count++;
      });

      // Prepare the data for Chart.js
      const labels = [];
      const counts = [];

      Object.values(groupedData).forEach((data) => {
        labels.push(data.name); // Priority name
        counts.push(data.count); // Priority count
      });

 

      this.state.riskratingchart = {
        labels: labels,
        datasets: [
          {
            label: "",
            data: counts,
            hoverOffset: 4,
          },
        ],
      };
    }
  };

  getCustomerRatingChart = async () => {
    if (this.state.datepicked == 0) {
      // Use read_group to perform aggregation and group by priority_level_id
      const results = await this.api.searchRead(
        "res.partner", // The model to query
        [], // No specific domain/filter (can be customized)
        ["risk_level"]
      );

      const groupedData = {};
      results.forEach(async (record) => {
        const Name = record.risk_level; // Get process_category_id (ID)

        // Get status_level_id.name (Name)
        if (!groupedData[Name]) {
          groupedData[Name] = { count: 0, Name: Name };
        }
        groupedData[Name].count++;
      });

      // Prepare the data for Chart.js
      const labels = [];
      const counts = [];

      Object.values(groupedData).forEach(async (data) => {
        labels.push(data.Name); // Priority name
        counts.push(data.count); // Priority count
      });

      this.state.customerchart = {
        labels: labels,
        datasets: [
          {
            label: "",
            data: counts,
            hoverOffset: 4,
          },
        ],
      };
    } else if (this.state.datepicked > 0) {
      const results = await this.api.searchRead(
        "res.partner", // The model to query
        [["date_created", ">=", this.state.current_datepicked]], // No specific domain/filter (can be customized)
        ["risk_level"] // Fields to group by (priority_level_id)
      );

      const groupedData = {};
      results.forEach(async (record) => {
        const Name = record.risk_level; // Get process_category_id (ID)

        // Get status_level_id.name (Name)
        if (!groupedData[Name]) {
          groupedData[Name] = { count: 0, Name: Name };
        }
        groupedData[Name].count++;
      });

      // Prepare the data for Chart.js
      const labels = [];
      const counts = [];

      Object.values(groupedData).forEach(async (data) => {
        labels.push(data.Name); // Priority name
        counts.push(data.count); // Priority count
      });

      this.state.customerchart = {
        labels: labels,
        datasets: [
          {
            label: "",
            data: counts,
            hoverOffset: 4,
          },
        ],
      };
    }
  };

  getTransactionStateChart = async () => {
    if (this.state.datepicked == 0) {
      // Use read_group to perform aggregation and group by priority_level_id
      const results = await this.api.searchRead(
        "res.customer.transaction", // The model to query
        [],
        ["state"] // No specific domain/filter (can be customized)
      );

      const groupedData = {};
      results.forEach((record) => {
        const Name = record.state; // Get status_level_id.name (Name)
        if (!groupedData[Name]) {
          groupedData[Name] = { count: 0, name: Name };
        }
        groupedData[Name].count++;
      });

      // Prepare the data for Chart.js
      const labels = [];
      const counts = [];

      Object.values(groupedData).forEach((data) => {
        labels.push(data.name); // Priority name
        counts.push(data.count); // Priority count
      });

      this.state.frequencychart = {
        labels: labels,
        datasets: [
          {
            label: "",
            data: counts,
            backgroundColor: [
              "rgba(75, 192, 0, 0.5)", // Color for first segment
              "rgba(255, 99, 132, 1)", // Color for second segment
              "rgba(54, 162, 235, 0.2)", // Color for third segment
            ],
            borderColor: [
              "rgba(75, 192, 192, 1)", // Border color for first segment
              "rgba(255, 99, 132, 1)", // Border color for second segment
              "rgba(54, 162, 235, 1)", // Border color for third segment
            ],
            borderWidth: 1,
          },
        ],
        options: {
          responsive: true,
          maintainAspectRatio: false,
          legend: {
            position: "right",
          },
        },
      };
    } else {
      const results = await this.api.searchRead(
        "res.customer.transaction", // The model to query
        [["date_created", ">=", this.state.current_datepicked]], // No specific domain/filter (can be customized)
        ["state"] // Fields to group by (priority_level_id)
      );
      const groupedData = {};
      results.forEach((record) => {
        const Name = record.state; // Get status_level_id.name (Name)
        if (!groupedData[Name]) {
          groupedData[Name] = { count: 0, name: Name };
        }
        groupedData[Name].count++;
      });

      // Prepare the data for Chart.js
      const labels = [];
      const counts = [];

      Object.values(groupedData).forEach((data) => {
        labels.push(data.name); // Priority name
        counts.push(data.count); // Priority count
      });

      this.state.frequencychart = {
        labels: labels,
        datasets: [
          {
            label: "",
            data: counts,
            backgroundColor: [
              "rgba(75, 192, 0, 0.5)", // Color for first segment
              "rgba(255, 99, 132, 1)", // Color for second segment
              "rgba(54, 162, 235, 0.2)", // Color for third segment
            ],
            borderColor: [
              "rgba(75, 192, 192, 1)", // Border color for first segment
              "rgba(255, 99, 132, 1)", // Border color for second segment
              "rgba(54, 162, 235, 1)", // Border color for third segment
            ],
            borderWidth: 1,
          },
        ],
        options: {
          responsive: true,
          maintainAspectRatio: false,
          legend: {
            position: "right",
          },
        },
      };
    }
  };
}

IcomplyDashboard.template = "owl.IcomplyDashboard";
IcomplyDashboard.components = { KpiCard, ChartRenderer };

registry.category("actions").add("owl.icomply_dashboard", IcomplyDashboard);
