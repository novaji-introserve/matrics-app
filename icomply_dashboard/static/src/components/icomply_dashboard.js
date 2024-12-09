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
        opencases: 0,
        closecases: 0,
        allcases: 0,
        overdue: 0,
        opencasebypercent: "0%",
        closecasebypercent: "0%",
      },
      datepicked: 0,
    });

    onMounted(async () => {
      await this.filterByDate();
    });

    onWillStart(async () => {
      await this.getCaseSeverityChart();
      await this.getCaseByStatusChart();
      await this.getCaseByCategoryChart();
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
      let allcasestotal = await this.api.searchCount("case.management", []);
      let openCasesCount = await this.api.searchCount("case.management", [
        ["case_status_id.name", "=", "Open"],
      ]);
      let closeCasesCount = await this.api.searchCount("case.management", [
        ["case_status_id.name", "=", "Closed"],
      ]);
      let overdueCasesCount = await this.api.searchCount("case.management", [
        ["case_status_id.name", "=", "Overdue"],
      ]);

      this.state.kpi.allcases = allcasestotal;
      this.state.kpi.opencases = openCasesCount;
      this.state.kpi.closecases = closeCasesCount;
      this.state.kpi.overdue = overdueCasesCount;
      this.state.kpi.allcasespercentage = 0;
      this.state.kpi.opencasespercentage = 0;
      this.state.kpi.closecasespercentage = 0;
      this.state.kpi.overduecasespercentage = 0;
      this.state.kpi.opencaseinRespectToAllCase = `${(
        (openCasesCount / allcasestotal) *
        100
      ).toFixed(1)}%`;
      this.state.kpi.closecaseinRespectToAllCase = `${(
        (closeCasesCount / allcasestotal) *
        100
      ).toFixed(1)}%`;
    } else {
      let allcasestotal = await this.api.searchCount("case.management", [
        ["created_at", ">=", this.state.current_datepicked],
      ]);
      let openCasesCount = await this.api.searchCount("case.management", [
        ["case_status_id.name", "=", "Open"],
        ["created_at", ">=", this.state.current_datepicked],
      ]);
      let closeCasesCount = await this.api.searchCount("case.management", [
        ["case_status_id.name", "=", "Closed"],
        ["created_at", ">=", this.state.current_datepicked],
      ]);
      let overdueCasesCount = await this.api.searchCount("case.management", [
        ["case_status_id.name", "=", "Overdue"],
        ["created_at", ">=", this.state.current_datepicked],
      ]);

      this.state.kpi.allcases = allcasestotal;
      this.state.kpi.opencases = openCasesCount;
      this.state.kpi.closecases = closeCasesCount;
      this.state.kpi.overdue = overdueCasesCount;

      // this calculate total data in range of the date filtered and last occurrence o the date

      let allcasestotal_prev = await this.api.searchCount("case.management", [
        ["created_at", "<", this.state.current_datepicked],
        ["created_at", ">=", this.state.previous_datepicked],
      ]);
      let openCasesCount_prev = await this.api.searchCount("case.management", [
        ["case_status_id.name", "=", "Open"],
        ["created_at", "<", this.state.current_datepicked],
        ["created_at", ">=", this.state.previous_datepicked],
      ]);
      let closeCasesCount_prev = await this.api.searchCount("case.management", [
        ["case_status_id.name", "=", "Closed"],
        ["created_at", "<", this.state.current_datepicked],
        ["created_at", ">=", this.state.previous_datepicked],
      ]);
      let overdueCasesCount_prev = await this.api.searchCount(
        "case.management",
        [
          ["case_status_id.name", "=", "Overdue"],
          ["created_at", "<", this.state.current_datepicked],
          ["created_at", ">=", this.state.previous_datepicked],
        ]
      );

      this.state.kpi.allcasespercentage =
        allcasestotal_prev == 0
          ? 0
          : (
              ((allcasestotal - allcasestotal_prev) / allcasestotal_prev) *
              100
            ).toFixed(2);
      this.state.kpi.opencasespercentage =
        openCasesCount_prev == 0
          ? 0
          : (
              ((openCasesCount - openCasesCount_prev) / openCasesCount_prev) *
              100
            ).toFixed(2);
      this.state.kpi.closecasespercentage =
        closeCasesCount_prev == 0
          ? 0
          : (
              ((closeCasesCount - closeCasesCount_prev) /
                closeCasesCount_prev) *
              100
            ).toFixed(2);
      this.state.kpi.overduecasespercentage =
        overdueCasesCount_prev == 0
          ? 0
          : (
              ((overdueCasesCount - overdueCasesCount_prev) /
                overdueCasesCount_prev) *
              100
            ).toFixed(2);
    }
    // reload chart on select change
    await this.getCaseSeverityChart();
    await this.getCaseByStatusChart();
    await this.getCaseByCategoryChart();
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

  getCaseSeverityChart = async () => {
    if (this.state.datepicked == 0) {
      // Use read_group to perform aggregation and group by priority_level_id
      const results = await this.api.searchRead(
        "case.management", // The model to query
        [], // No specific domain/filter (can be customized)
        ["priority_level_id"] // Fields to group by (priority_level_id)
      );

      const groupedData = {};
      results.forEach((record) => {
        const priorityId = record.priority_level_id[0]; // Get priority_level_id (ID)
        const priorityName = record.priority_level_id[1]; // Get priority_level_id.name (Name)
        if (!groupedData[priorityId]) {
          groupedData[priorityId] = { count: 0, name: priorityName };
        }
        groupedData[priorityId].count++;
      });

      // Prepare the data for Chart.js
      const labels = [];
      const counts = [];

      Object.values(groupedData).forEach((data) => {
        labels.push(data.name); // Priority name
        counts.push(data.count); // Priority count
      });

      this.state.openchart = {
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
        "case.management", // The model to query
        [["created_at", ">=", this.state.current_datepicked]], // No specific domain/filter (can be customized)
        ["priority_level_id"] // Fields to group by (priority_level_id)
      );

      const groupedData = {};
      results.forEach((record) => {
        const priorityId = record.priority_level_id[0]; // Get priority_level_id (ID)
        const priorityName = record.priority_level_id[1]; // Get priority_level_id.name (Name)
        if (!groupedData[priorityId]) {
          groupedData[priorityId] = { count: 0, name: priorityName };
        }
        groupedData[priorityId].count++;
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

      this.state.openchart = {
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

  getCaseByStatusChart = async () => {
    if (this.state.datepicked == 0) {
      // Use read_group to perform aggregation and group by priority_level_id
      const results = await this.api.searchRead(
        "case.management", // The model to query
        [], // No specific domain/filter (can be customized)
        ["case_status_id"] // Fields to group by (priority_level_id)
      );

      const groupedData = {};
      results.forEach((record) => {
        const statusId = record.case_status_id[0]; // Get case_status_id (ID)
        const statusName = record.case_status_id[1]; // Get status_level_id.name (Name)
        if (!groupedData[statusId]) {
          groupedData[statusId] = { count: 0, name: statusName };
        }
        groupedData[statusId].count++;
      });

      // Prepare the data for Chart.js
      const labels = [];
      const counts = [];

      Object.values(groupedData).forEach((data) => {
        labels.push(data.name); // Priority name
        counts.push(data.count); // Priority count
      });

      this.state.statuschart = {
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
        "case.management", // The model to query
        [["created_at", ">=", this.state.current_datepicked]], // No specific domain/filter (can be customized)
        ["case_status_id"] // Fields to group by (priority_level_id)
      );

      const groupedData = {};
      results.forEach((record) => {
        const statusId = record.case_status_id[0]; // Get case_status_id (ID)
        const statusName = record.case_status_id[1]; // Get status_level_id.name (Name)
        if (!groupedData[statusId]) {
          groupedData[statusId] = { count: 0, name: statusName };
        }
        groupedData[statusId].count++;
      });

      // Prepare the data for Chart.js
      const labels = [];
      const counts = [];

      Object.values(groupedData).forEach((data) => {
        labels.push(data.name); // Priority name
        counts.push(data.count); // Priority count
      });

      this.state.statuschart = {
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

  getCaseByCategoryChart = async () => {
    if (this.state.datepicked == 0) {
      // Use read_group to perform aggregation and group by priority_level_id
      const results = await this.api.searchRead(
        "case.management", // The model to query
        [], // No specific domain/filter (can be customized)
        ["case_status_id"] // Fields to group by (priority_level_id)
      );

      const groupedData = {};
      results.forEach((record) => {
        const statusId = record.case_status_id[0]; // Get case_status_id (ID)
        const statusName = record.case_status_id[1]; // Get status_level_id.name (Name)
        if (!groupedData[statusId]) {
          groupedData[statusId] = { count: 0, name: statusName };
        }
        groupedData[statusId].count++;
      });

      // Prepare the data for Chart.js
      const labels = [];
      const counts = [];

      Object.values(groupedData).forEach((data) => {
        labels.push(data.name); // Priority name
        counts.push(data.count); // Priority count
      });

      this.state.categoryChart = {
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
        "case.management", // The model to query
        [["created_at", ">=", this.state.current_datepicked]], // No specific domain/filter (can be customized)
        ["case_status_id"] // Fields to group by (priority_level_id)
      );

      const groupedData = {};
      results.forEach((record) => {
        const statusId = record.case_status_id[0]; // Get case_status_id (ID)
        const statusName = record.case_status_id[1]; // Get status_level_id.name (Name)
        if (!groupedData[statusId]) {
          groupedData[statusId] = { count: 0, name: statusName };
        }
        groupedData[statusId].count++;
      });

      // Prepare the data for Chart.js
      const labels = [];
      const counts = [];

      Object.values(groupedData).forEach((data) => {
        labels.push(data.name); // Priority name
        counts.push(data.count); // Priority count
      });

      this.state.categoryChart = {
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
