/** @odoo-module */

import { registry } from "@web/core/registry";
import { KpiCard } from "../kpi_card/kpi_card";
import { ChartRenderer } from "../chart_renderer/chart_renderer";
import { useService } from "@web/core/utils/hooks";

const { Component, onWillStart, useState } = owl;

export class CaseDashboard extends Component {
  setup() {
    this.state = useState({
      allCases: {
        value: 0,
        percentage: 0,
      },
      openCases: {
        value: 0,
        percentage: 0,
      },
      overdueCases: {
        value: 0,
        percentage: 0,
      },
      closedCases: {
        value: 0,
        percentage: 0,
      },
      archivedCases: {
        value: 0,
        percentage: 0,
      },
      period: 0,
      caseStatusChart: {},
      caseRatingChart: {},
      isLoading: true,
    });

    this.orm = useService("orm");
    this.actionService = useService("action");
    this.user = useService("user");

    onWillStart(async () => {
      try {
        this.state.isLoading = true;
        this.getDates();

        // Execute these in parallel using Promise.all for better performance
        await Promise.all([
          this.getAllCases(),
          this.getOpenCases(),
          this.getOverdueCases(),
          this.getClosedCases(),
          this.getArchivedCases(),
          this.getCaseStatusChart(),
          this.getCaseRatingChart(),
        ]);
      } catch (error) {
        console.error("Error loading dashboard data:", error);
      } finally {
        this.state.isLoading = false;
      }
    });
  }

  // Base domain to apply user access restrictions
  getBaseDomain() {
    return [
      "|",
      "|",
      ["create_uid", "=", this.user.userId],
      ["officer_responsible", "=", this.user.userId],
      ["supervisors", "in", [this.user.userId]],
    ];
  }

  // Get Dates for filtering
  getDates() {
    const calculatedDate = moment()
      .subtract(this.state.period, "days")
      .startOf("day")
      .format("YYYY/MM/DD HH:mm:ss");

    this.state.current_date = calculatedDate;

    const PreviousDate = moment()
      .subtract(this.state.period * 2, "days")
      .startOf("day")
      .format("YYYY/MM/DD HH:mm:ss");

    this.state.previous_date = PreviousDate;
  }

  // On Change Period
  async OnChangePeriod() {
    try {
      this.state.isLoading = true;
      this.getDates();

      // Execute these in parallel for better performance
      await Promise.all([
        this.getAllCases(),
        this.getOpenCases(),
        this.getOverdueCases(),
        this.getClosedCases(),
        this.getArchivedCases(),
        this.getCaseStatusChart(),
        this.getCaseRatingChart(),
      ]);
    } catch (error) {
      console.error("Error updating dashboard data:", error);
    } finally {
      this.state.isLoading = false;
    }
  }

  // Get All Cases
  async getAllCases() {
    try {
      const current_date = this.state.current_date;
      const period = this.state.period;
      const previous_date = this.state.previous_date;

      // Apply base domain
      const domain = this.getBaseDomain();

      if (period > 0) {
        domain.push(["create_date", ">", current_date]);
      }

      // Define context to include inactive (archived) records
      // const context = { active_test: false };

      // // Pass the context to the searchCount method
      // const data = await this.orm.searchCount("case.manager", domain, {
      //   context,
      // });
      // this.state.allCases.value = data;
      const context = {
        active_test: false,
        // Add additional performance optimization flags
        bin_size: true, // Only return file sizes instead of content
        prefetch_fields: false, // Disable prefetching of fields we don't need
        defer_parent_store_compute: true, // Defer parent store computation
      };

      // Use a more efficient count method when possible
      const data = await this.orm.searchCount("case.manager", domain, {
        context,
      });
      
      this.state.allCases.value = data;


      // Apply base domain to previous period as well
      const prev_domain = this.getBaseDomain();
      if (period > 0) {
        prev_domain.push(
          ["create_date", ">", previous_date],
          ["create_date", "<=", current_date]
        );
      }

      // Also pass the context to the previous period search
      const prev_data = await this.orm.searchCount(
        "case.manager",
        prev_domain,
        { context }
      );
      const percentage = prev_data ? ((data - prev_data) / prev_data) * 100 : 0;
      this.state.allCases.percentage = isFinite(percentage)
        ? percentage.toFixed(2)
        : "0.00";
    } catch (error) {
      console.error("Error fetching all cases:", error);
      this.state.allCases.value = 0;
      this.state.allCases.percentage = "0.00";
    }
  }

  // Get Open Cases
  async getOpenCases() {
    try {
      const current_date = this.state.current_date;
      const period = this.state.period;
      const previous_date = this.state.previous_date;

      // Apply base domain with case_status filter
      const domain = this.getBaseDomain();
      domain.push(["case_status", "=", "open"]);

      if (period > 0) {
        domain.push(["create_date", ">", current_date]);
      }

      const data = await this.orm.searchCount("case.manager", domain);
      this.state.openCases.value = data;

      // Apply base domain to previous period as well
      const prev_domain = this.getBaseDomain();
      prev_domain.push(["case_status", "=", "open"]);

      if (period > 0) {
        prev_domain.push(
          ["create_date", ">", previous_date],
          ["create_date", "<=", current_date]
        );
      }

      const prev_data = await this.orm.searchCount("case.manager", prev_domain);
      const percentage = prev_data ? ((data - prev_data) / prev_data) * 100 : 0;
      this.state.openCases.percentage = isFinite(percentage)
        ? percentage.toFixed(2)
        : "0.00";
    } catch (error) {
      console.error("Error fetching open cases:", error);
      this.state.openCases.value = 0;
      this.state.openCases.percentage = "0.00";
    }
  }

  // Get Overdue Cases
  async getOverdueCases() {
    try {
      const current_date = this.state.current_date;
      const period = this.state.period;
      const previous_date = this.state.previous_date;

      // Apply base domain with case_status filter
      const domain = this.getBaseDomain();
      domain.push(["case_status", "=", "overdue"]);

      if (period > 0) {
        domain.push(["create_date", ">", current_date]);
      }

      const data = await this.orm.searchCount("case.manager", domain);
      this.state.overdueCases.value = data;

      // Apply base domain to previous period as well
      const prev_domain = this.getBaseDomain();
      prev_domain.push(["case_status", "=", "overdue"]);

      if (period > 0) {
        prev_domain.push(
          ["create_date", ">", previous_date],
          ["create_date", "<=", current_date]
        );
      }

      const prev_data = await this.orm.searchCount("case.manager", prev_domain);
      const percentage = prev_data ? ((data - prev_data) / prev_data) * 100 : 0;
      this.state.overdueCases.percentage = isFinite(percentage)
        ? percentage.toFixed(2)
        : "0.00";
    } catch (error) {
      console.error("Error fetching overdue cases:", error);
      this.state.overdueCases.value = 0;
      this.state.overdueCases.percentage = "0.00";
    }
  }

  // Get Closed Cases
  async getClosedCases() {
    try {
      const current_date = this.state.current_date;
      const period = this.state.period;
      const previous_date = this.state.previous_date;

      // Apply base domain with case_status filter
      const domain = this.getBaseDomain();
      domain.push(["case_status", "=", "closed"]);

      if (period > 0) {
        domain.push(["create_date", ">", current_date]);
      }

      const data = await this.orm.searchCount("case.manager", domain);
      this.state.closedCases.value = data;

      // Apply base domain to previous period as well
      const prev_domain = this.getBaseDomain();
      prev_domain.push(["case_status", "=", "closed"]);

      if (period > 0) {
        prev_domain.push(
          ["create_date", ">", previous_date],
          ["create_date", "<=", current_date]
        );
      }

      const prev_data = await this.orm.searchCount("case.manager", prev_domain);
      const percentage = prev_data ? ((data - prev_data) / prev_data) * 100 : 0;
      this.state.closedCases.percentage = isFinite(percentage)
        ? percentage.toFixed(2)
        : "0.00";
    } catch (error) {
      console.error("Error fetching closed cases:", error);
      this.state.closedCases.value = 0;
      this.state.closedCases.percentage = "0.00";
    }
  }

  // Get Archived Cases
  async getArchivedCases() {
    try {
      const current_date = this.state.current_date;
      const period = this.state.period;
      const previous_date = this.state.previous_date;

      // Apply base domain with case_status filter and active=false
      const domain = this.getBaseDomain();
      domain.push(["case_status", "=", "archived"], ["active", "=", false]);

      if (period > 0) {
        domain.push(["create_date", ">", current_date]);
      }

      const data = await this.orm.searchCount("case.manager", domain);
      this.state.archivedCases.value = data;

      // Apply base domain to previous period as well
      const prev_domain = this.getBaseDomain();
      prev_domain.push(
        ["case_status", "=", "archived"],
        ["active", "=", false]
      );

      if (period > 0) {
        prev_domain.push(
          ["create_date", ">", previous_date],
          ["create_date", "<=", current_date]
        );
      }

      const prev_data = await this.orm.searchCount("case.manager", prev_domain);
      const percentage = prev_data ? ((data - prev_data) / prev_data) * 100 : 0;
      this.state.archivedCases.percentage = isFinite(percentage)
        ? percentage.toFixed(2)
        : "0.00";
    } catch (error) {
      console.error("Error fetching archived cases:", error);
      this.state.archivedCases.value = 0;
      this.state.archivedCases.percentage = "0.00";
    }
  }

  // Case Status Chart (Pie)

  async getCaseStatusChart() {
    try {
      // Apply base domain - BUT DON'T FILTER BY ACTIVE YET
      const domain = this.getBaseDomain();

      if (this.state.period > 0) {
        domain.push(["create_date", ">", this.state.current_date]);
      }

      // IMPORTANT: Disable the active filter for this search
      // This is needed to find all records including archived ones
      const context = { active_test: false };

      // Use read_group for server-side aggregation - MUCH more efficient
      const groupedResults = await this.orm.readGroup(
        "case.manager",
        domain,
        ["case_status_count:count(id)"], // Count records
        ["case_status"], // Group by this field
        { lazy: false, context: context } // Don't use lazy loading and disable active test
      );

      // Initialize data structure with all possible statuses
      const statusCounts = {
        open: 0,
        closed: 0,
        overdue: 0,
        archived: 0,
      };

      // Fill with actual data
      groupedResults.forEach((group) => {
        const status = group.case_status;
        if (status && status in statusCounts) {
          statusCounts[status] = group.case_status_count;
        }
      });

      // Additionally, make a specific query for archived records
      const archivedDomain = [...this.getBaseDomain()];
      archivedDomain.push(
        ["case_status", "=", "archived"],
        ["active", "=", false]
      );
      if (this.state.period > 0) {
        archivedDomain.push(["create_date", ">", this.state.current_date]);
      }
      const archivedCount = await this.orm.searchCount(
        "case.manager",
        archivedDomain
      );

      // Update the status counts with the specific archived count
      statusCounts.archived = archivedCount;

      // ALWAYS include all statuses in consistent order for the chart
      const labels = ["Open", "Closed", "Overdue", "Archived"];
      const counts = [
        statusCounts.open,
        statusCounts.closed,
        statusCounts.overdue,
        statusCounts.archived,
      ];

      // For debug - check if we have archived cases
      console.log("Status counts:", statusCounts);

      const backgroundColors = [
        "rgba(255,172,0,0.7)", //  - Yellow
        "rgba(40,167,69,0.7)", //  - Green
        "rgba(220,53,69,0.7)", //  - Red
        "rgba(233,233,233,0.7)", //  - Blue
      ];

      const borderColors = [
        "rgb(255,172,0)", //  - Yellow
        "rgb(40,167,69)", //  - Green
        "rgb(220,53,69)", //  - Red
        "rgb(233,233,233)", //  - Blue
      ];

      // For pie charts, we need to filter out zero values to avoid empty segments
      // BUT keep at least one value to avoid empty chart
      let filteredLabels = [...labels];
      let filteredCounts = [...counts];
      let filteredBackgroundColors = [...backgroundColors];
      let filteredBorderColors = [...borderColors];

      // Only filter if we have at least one non-zero value
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
          // Use the filtered data for the chart
          labels: filteredLabels,
          datasets: [
            {
              label: "Case Status",
              data: filteredCounts,
              backgroundColor: filteredBackgroundColors,
              borderColor: filteredBorderColors,
              borderWidth: 1,
              hoverOffset: 15, // Make segments move outward more on hover
            },
          ],
        },
        // But keep full data for click handlers
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
              position: "top",
              labels: {
                font: {
                  size: 12,
                },
                padding: 20,
              },
            },
            tooltip: {
              enabled: true, // Make sure tooltips are enabled
              callbacks: {
                label: function (context) {
                  return context.label + ": " + context.raw + " cases";
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

  // Case Rating Chart (Bar)
  async getCaseRatingChart() {
    try {
      const domain = this.getBaseDomain();

      if (this.state.period > 0) {
        domain.push(["create_date", ">", this.state.current_date]);
      }

      // Use read_group for server-side aggregation
      const groupedResults = await this.orm.readGroup(
        "case.manager",
        domain,
        ["case_rating_count:count(id)"], // Count records
        ["case_rating"], // Group by this field
        { lazy: false, context: { active_test: false } } // Include inactive records
      );

      // Initialize data structure with all possible ratings
      const ratingCounts = {
        low: 0,
        medium: 0,
        high: 0,
      };

      // Fill with actual data
      groupedResults.forEach((group) => {
        const rating = group.case_rating;
        if (rating && rating in ratingCounts) {
          ratingCounts[rating] = group.case_rating_count;
        }
      });

      // Prepare chart data
      const labels = ["Low", "Medium", "High"];
      const counts = [ratingCounts.low, ratingCounts.medium, ratingCounts.high];

      const backgroundColors = [
        "rgba(40,167,69,0.7)", // Closed - Green
        "rgba(255,172,0,0.7)", // Archived - Yellow
        "rgba(220,53,69,0.7)", // Overdue - Red
      ];

      const borderColors = [
        "rgba(0, 128, 0)", // Low - Green
        "rgba(255, 255, 0)", // Medium - Yellow
        "rgba(255, 0, 0)", // High - Red
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
              borderRadius: 4, // Rounded bars
              maxBarThickness: 60, // Control max thickness
              hoverBackgroundColor: backgroundColors.map((color) =>
                color.replace("0.7", "0.9")
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
              callbacks: {
                label: function (context) {
                  return context.label + " Rating: " + context.raw + " cases";
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
                color: "rgba(0,0,0,0.05)",
              },
            },
            x: {
              ticks: {
                font: {
                  size: 12,
                  weight: "bold",
                },
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

  // View functions for card clicks
  async viewAllCases() {
    this.actionService.doAction("case_management_v2.action_case_manager");
  }

  async viewOpenCases() {
    this.actionService.doAction("case_management_v2.action_open_cases");
  }

  async viewOverdueCases() {
    this.actionService.doAction("case_management_v2.action_overdue_cases");
  }

  async viewClosedCases() {
    this.actionService.doAction("case_management_v2.action_closed_cases");
  }

  async viewArchivedCases() {
    this.actionService.doAction("case_management_v2.action_archived_cases");
  }

  // Chart click handlers
  async viewByStatus(status) {
    console.log("Clicked on status:", status);

    // Guard against undefined status
    if (!status) {
      console.error("Status is undefined or null");
      return;
    }

    // Get the status value - handle both string and object cases
    let statusValue;
    if (typeof status === "string") {
      statusValue = status;
    } else if (status && status.label) {
      // Handle if we get a chart element object instead
      statusValue = status.label;
    } else {
      console.error("Invalid status format:", status);
      return;
    }

    const statusMap = {
      Open: "case_management_v2.action_open_cases",
      Closed: "case_management_v2.action_closed_cases",
      Overdue: "case_management_v2.action_overdue_cases",
      Archived: "case_management_v2.action_archived_cases",
    };

    const actionId = statusMap[statusValue];
    if (actionId) {
      this.actionService.doAction(actionId);
    } else {
      // Fallback if status doesn't have a predefined action
      const domain = this.getBaseDomain();
      domain.push(["case_status", "=", statusValue.toLowerCase()]);

      if (this.state.period > 0) {
        domain.push(["create_date", ">", this.state.current_date]);
      }

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
  }

  async viewByRating(rating) {
    console.log("Clicked on rating:", rating);

    // Guard against undefined rating
    if (!rating) {
      console.error("Rating is undefined or null");
      return;
    }

    // Get the rating value - handle both string and object cases
    let ratingValue;
    if (typeof rating === "string") {
      ratingValue = rating.toLowerCase();
    } else if (rating && rating.label) {
      // Handle if we get a chart element object instead
      ratingValue = rating.label.toLowerCase();
    } else {
      console.error("Invalid rating format:", rating);
      return;
    }

    // Apply base domain with case_rating filter
    const domain = this.getBaseDomain();
    domain.push(["case_rating", "=", ratingValue]);

    if (this.state.period > 0) {
      domain.push(["create_date", ">", this.state.current_date]);
    }

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
