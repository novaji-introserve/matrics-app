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
      await this.getRiskRatingChart();
      await this.getProcessCategoryChart();
      await this.getFrequencyChart();
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
    if (
      this.state.current_datepicked === new Date().toLocaleDateString("en-US")
    ) {
      let alerttulesCount = await this.api.searchCount(
        "alert.rules",
        []
      );
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
      this.state.kpi.totalScreenedTransactionCount = totalScreenedTransactionCount;
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
    } 
    else {
      let alerttulesCount = await this.api.searchCount(
         "alert.rules",
        ["date_created", ">=", this.state.current_datepicked]
      );
      let totalTransactionCount = await this.api.searchCount("alert.rules", [
        ["date_created", ">=", this.state.current_datepicked],
      ]);
      let lowriskCount = await this.api.searchCount("alert.rules", [
        ["risk_rating", "=", "low"],
        ["date_created", ">=", this.state.current_datepicked],
      ]);
      let mediumriskCount = await this.api.searchCount("alert.rules", [
        ["risk_rating", "=", "medium"],
        ["date_created", ">=", this.state.current_datepicked],
      ]);
      let highriskCount = await this.api.searchCount("alert.rules", [
        ["risk_rating", "=", "high"],
        ["date_created", ">=", this.state.current_datepicked],
      ]);

       let totalScreenedTransactionCount = await this.api.searchCount(
         "res.customer.transaction",
         [
           ["rule_id", "!=", null],
           ["date_created", ">=", this.state.current_datepicked]
         ]
       );

       novaji_admin_dev1

      this.state.kpi.lowrisk = lowriskCount;
      this.state.kpi.mediumrisk = mediumriskCount;
      this.state.kpi.highrisk = highriskCount;
      // this.state.kpi.totalScreenedTransactionCount = totalScreenedTransactionCount;
      this.state.kpi.totaltransaction = totalTransactionCount;
      this.state.kpi.alertrulestotal = alerttulesCount;

      // each risk count in respect to all records

      this.state.kpi.lowriskinRespectToTotalTransaction = `${(
        (lowriskCount / totalTransactionCount) *
        100
      ).toFixed(1)}%`;

      this.state.kpi.mediumriskiinRespectToTotalTransaction = `${(
        (mediumriskCount / totalTransactionCount) *
        100
      ).toFixed(1)}%`;

      this.state.kpi.highriskiinRespectToTotalTransaction = `${(
        (highriskCount / totalTransactionCount) *
        100
      ).toFixed(1)}%`;

      // this calculate total data in range of the date filtered and last occurrence o the date

      let total_rules_prev_count = await this.api.searchCount("alert.rules", [
        ["date_created", "<", this.state.current_datepicked],
        ["date_created", ">=", this.state.previous_datepicked],
      ]);

      let low_risk_prev_count = await this.api.searchCount("alert.rules", [
        ["risk_rating", "=", "low"],
        ["date_created", "<", this.state.current_datepicked],
        ["date_created", ">=", this.state.previous_datepicked],
      ]);

      let medium_risk_prev_count = await this.api.searchCount("alert.rules", [
        ["risk_rating", "=", "medium"],
        ["date_created", "<", this.state.current_datepicked],
        ["date_created", ">=", this.state.previous_datepicked],
      ]);

      let high_risk_prev_count = await this.api.searchCount("alert.rules", [
        ["risk_rating", "=", "high"],
        ["date_created", "<", this.state.current_datepicked],
        ["date_created", ">=", this.state.previous_datepicked],
      ]);

      this.state.kpi.totaltransactionpercentage =
        total_rules_prev_count == 0
          ? 0
          : (
              ((totalTransactionCount - total_rules_prev_count) /
                total_rules_prev_count) *
              100
            ).toFixed(2);

      this.state.kpi.lowriskpercentage =
        low_risk_prev_count == 0
          ? 0
          : (
              ((lowriskCount - low_risk_prev_count) / low_risk_prev_count) *
              100
            ).toFixed(2);

      this.state.kpi.mediumriskbypercent =
        medium_risk_prev_count == 0
          ? 0
          : (
              ((mediumriskCount - medium_risk_prev_count) /
                medium_risk_prev_count) *
              100
            ).toFixed(2);

      this.state.kpi.highriskbypercent =
        high_risk_prev_count == 0
          ? 0
          : (
              ((highriskCount - high_risk_prev_count) / high_risk_prev_count) *
              100
            ).toFixed(2);
    }
    // reload chart on select change
    await this.getRiskRatingChart();
    await this.getProcessCategoryChart();
    await this.getFrequencyChart();
  };

  displayAllCases() {
    this.navigate.doAction({
      type: "ir.actions.act_window",
      res_model: "case.management",
      name: "case_management_owl_action",
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
  displayAllOpenCases() {
    this.navigate.doAction({
      type: "ir.actions.act_window",
      res_model: "case.management",
      name: "case_management_owl_action",
      domain:
        this.state.datepicked > 0
          ? [
              ["case_status_id.name", "=", "Open"],
              ["created_at", ">=", this.state.current_datepicked],
            ]
          : [["case_status_id.name", "=", "Open"]],
      views: [
        [false, "tree"],
        [false, "form"],
      ],
    });
  }
  displayAllCloseCases() {
    this.navigate.doAction({
      type: "ir.actions.act_window",
      res_model: "case.management",
      name: "case_management_owl_action",
      domain:
        this.state.datepicked > 0
          ? [
              ["case_status_id.name", "=", "Closed"],
              ["created_at", ">=", this.state.current_datepicked],
            ]
          : [["case_status_id.name", "=", "Closed"]],
      views: [
        [false, "tree"],
        [false, "form"],
      ],
    });
  }
  displayAllOverdueCases() {
    this.navigate.doAction({
      type: "ir.actions.act_window",
      res_model: "case.management",
      name: "case_management_owl_action",
      domain:
        this.state.datepicked > 0
          ? [
              ["case_status_id.name", "=", "Overdue"],
              ["created_at", ">=", this.state.current_datepicked],
            ]
          : [["case_status_id.name", "=", "Overdue"]],
      views: [
        [false, "tree"],
        [false, "form"],
      ],
    });
  }
  displayNothing() {}

  // chart implementation

  getRiskRatingChart = async () => {
    if (this.state.datepicked == 0) {
      // Use read_group to perform aggregation and group by priority_level_id
      const results = await this.api.searchRead(
        "alert.rules", // The model to query
        [], // No specific domain/filter (can be customized)
        ["risk_rating"] // Fields to group by (priority_level_id)
      );

      const groupedData = {};
      results.forEach((record) => {
        const Name = record.risk_rating; // Get priority_level_id.name (Name)
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
        "alert.rules", // The model to query
        [["date_created", ">=", this.state.current_datepicked]], // No specific domain/filter (can be customized)
        ["risk_rating"] // Fields to group by (priority_level_id)
      );

      const groupedData = {};
      results.forEach((record) => {
        const Name = record.risk_rating; // Get priority_level_id.name (Name)
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

      console.log(labels);
      console.log(counts);

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

  getProcessCategoryChart = async () => {
    if (this.state.datepicked == 0) {
      // Use read_group to perform aggregation and group by priority_level_id
      const results = await this.api.searchRead(
        "alert.rules", // The model to query
        [], // No specific domain/filter (can be customized)
        ["process_category_id"] // Fields to group by (priority_level_id)
      );


        const groupedData = {};
        results.forEach((record) => {
          const id = record.process_category_id[0]; // Get process_category_id (ID)
          const Name = record.process_category_id[1]; // Get status_level_id.name (Name)
          if (!groupedData[id]) {
            groupedData[id] = { count: 0, name: Name };
          }
          groupedData[id].count++;
        });

        // Prepare the data for Chart.js
        const labels = [];
        const counts = [];

        Object.values(groupedData).forEach((data) => {
          labels.push(data.name); // Priority name
          counts.push(data.count); // Priority count
        });

        this.state.categorychart = {
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
      else if (this.state.datepicked > 0) {
        const results = await this.api.searchRead(
          "alert.rules", // The model to query
          [["date_created", ">=", this.state.current_datepicked]], // No specific domain/filter (can be customized)
          ["process_category_id"] // Fields to group by (priority_level_id)
        );
         const groupedData = {};
         results.forEach((record) => {
           const id = record.process_category_id[0]; // Get process_category_id (ID)
           const Name = record.process_category_id[1]; // Get status_level_id.name (Name)
           if (!groupedData[id]) {
             groupedData[id] = { count: 0, name: Name };
           }
           groupedData[id].count++;
         });

         // Prepare the data for Chart.js
         const labels = [];
         const counts = [];

         Object.values(groupedData).forEach((data) => {
           labels.push(data.name); // Priority name
           counts.push(data.count); // Priority count
         });

         this.state.categorychart = {
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

  getFrequencyChart = async () => {
    if (this.state.datepicked == 0) {
      // Use read_group to perform aggregation and group by priority_level_id
      const results = await this.api.searchRead(
        "alert.rules", // The model to query
        [], // No specific domain/filter (can be customized)
        ["frequency_id"] // Fields to group by (priority_level_id)
      );

      const groupedData = {};
      results.forEach((record) => {
        const id = record.frequency_id[0]; // Get frequency_id (ID)
        const Name = record.frequency_id[1]; // Get status_level_id.name (Name)
        if (!groupedData[id]) {
          groupedData[id] = { count: 0, name: Name };
        }
        groupedData[id].count++;
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
        "alert.rules", // The model to query
        [["date_created", ">=", this.state.current_datepicked]], // No specific domain/filter (can be customized)
        ["frequency_id"] // Fields to group by (priority_level_id)
      );

      const groupedData = {};
      results.forEach((record) => {
        const id = record.frequency_id[0]; // Get frequency_id (ID)
        const Name = record.frequency_id[1]; // Get status_level_id.name (Name)
        if (!groupedData[id]) {
          groupedData[id] = { count: 0, name: Name };
        }
        groupedData[id].count++;
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
