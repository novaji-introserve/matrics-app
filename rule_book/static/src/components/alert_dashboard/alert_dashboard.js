/** @odoo-module */

import { registry } from "@web/core/registry";
import { KpiCard } from "../kpi_card/kpi_card";
import { ChartRenderer } from "../chart_renderer/chart_renderer";
import { loadJS } from "@web/core/assets";
import { useService } from "@web/core/utils/hooks";

const { Component, onWillStart, useRef, onMounted, useState } = owl;

export class AlertDashboard extends Component {
  async getRulebookResponseTiming() {
    try {
      // Perform search_read on reply.log model
      const domain = [
        [
          "submission_timing",
          "in",
          ["early", "on_time", "late", "not_responded"],
        ],
      ];

      if (this.state.period > 0) {
        domain.push(["reply_date", ">", this.state.current_date]);
      }
      const results = await this.orm.searchRead("reply.log", domain, [
        "submission_timing",
      ]);

      // Initialize counters for each status
      const groupedData = {
        early: { count: 0, name: "Early Submission" },
        on_time: { count: 0, name: "Right on Time" },
        late: { count: 0, name: "Late Submission" },
        not_responded: { count: 0, name: "Not Responded" },
      };

      // Count occurrences
      results.forEach((record) => {
        if (record.submission_timing) {
          // Just use the submission_timing value directly
          if (groupedData[record.submission_timing]) {
            groupedData[record.submission_timing].count++;
          }
        }
      });

      // Prepare data for Chart.js
      const labels = [];
      const counts = [];
      const backgroundColors = [
        "rgba(66, 255, 51, 0.3)", // Early - Green
        "rgba(51, 100, 255, 0.3)", // On Time - Blue
        "rgba(255, 73, 51, 0.3)", // Late - Red
        "rgba(255, 233, 51, 0.3)", // Not Responded - Yellow
      ];
      const borderColors = [
        "rgba(0, 128, 0)", // Early - Green
        "rgba(0, 0, 255)", // On Time - Blue
        "rgba(255, 0, 0)", // Late - Red
        "rgba(255, 255, 0)", // Not Responded - Yellow
      ];

      // Process grouped data
      Object.values(groupedData).forEach((data) => {
        labels.push(data.name);
        counts.push(data.count);
      });

      console.log(
        "labels ",
        labels,
        " counts ",
        counts,
        "group data",
        groupedData
      );

      // Update state with chart data
      this.state.ResponseTiming = {
        data: {
          labels: labels,
          datasets: [
            {
              label: "Submission Timing",
              data: counts,
              backgroundColor: backgroundColors.slice(0, labels.length),
              borderColor: borderColors.slice(0, labels.length),
              borderWidth: 1,
            },
          ],
        },
      };
    } catch (error) {
      console.error("Error fetching submission timing data:", error);
    }
  }

  async getRulebookState() {
    // this.state.RulebookState = {};

    try {
      // Perform search_read on reply.log model
      const domain = [
        [
          "rulebook_status",
          "in",
          ["pending", "submitted", "reviewed", "completed"],
        ],
      ];

      if (this.state.period > 0) {
        domain.push(["reply_date", ">", this.state.current_date]);
      }

      const results = await this.orm.searchRead("reply.log", domain, [
        "rulebook_status",
      ]);

      // Initialize counters for each status
      const groupedData = {
        pending: { count: 0, name: "Pending" },
        submitted: { count: 0, name: "Submitted" },
        reviewed: { count: 0, name: "Reviewed" },
        completed: { count: 0, name: "Completed" },
      };

      // Count occurrences
      results.forEach((record) => {
        if (record.rulebook_status) {
          // Just use the submission_timing value directly
          if (groupedData[record.rulebook_status]) {
            groupedData[record.rulebook_status].count++;
          }
        }
      });

      // Prepare data for Chart.js
      const labels = [];
      const counts = [];
      const backgroundColors = [
        "rgba(66, 255, 51, 0.3)", // Early - Green
        "rgba(51, 100, 255, 0.3)", // On Time - Blue
        "rgba(255, 73, 51, 0.3)", // Late - Red
        "rgba(255, 233, 51, 0.3)", // Not Responded - Yellow
      ];
      const borderColors = [
        "rgba(0, 128, 0)", // Early - Green
        "rgba(0, 0, 255)", // On Time - Blue
        "rgba(255, 0, 0)", // Late - Red
        "rgba(255, 255, 0)", // Not Responded - Yellow
      ];

      // Process grouped data
      Object.values(groupedData).forEach((data) => {
        labels.push(data.name);
        counts.push(data.count);
      });

      console.log(
        "labels ",
        labels,
        " counts ",
        counts,
        "group data",
        groupedData
      );

      // Update state with chart data
      this.state.RulebookState = {
        data: {
          labels: labels,
          datasets: [
            {
              label: "Status Report",
              data: counts,
              backgroundColor: backgroundColors.slice(0, labels.length),
              borderColor: borderColors.slice(0, labels.length),
              borderWidth: 1,
            },
          ],
        },
        domain,
      };
    } catch (error) {
      console.error("Error fetching submission timing data:", error);
    }
  }

  setup() {
    this.state = useState({
      pendingReply: {
        value: 10,
        percentage: 6,
      },
      lateReply: {
        value: 10,
        percentage: 6,
      },
      earlyReply: {
        value: 1,
        percentage: 1,
      },
      noReply: {
        value: 1,
        percentage: 1,
      },
      pending: {
        value: 1,
        percentage: 1,
      },
      submitted: {
        value: 1,
        percentage: 1,
      },
      completed: {
        value: 1,
        percentage: 1,
      },
      reviewed: {
        value: 1,
        percentage: 1,
      },
      period: 7,
    });

    this.orm = useService("orm");
    this.actionService = useService("action");

    onWillStart(async () => {
      this.getDates();
      //   await this.getQuotations();
      await this.getLateReply();
      await this.getEarlyReply();
      await this.getNoReply();
      await this.getPendingReply();

      // await this.getPendingReply();
      await this.getRulebookResponseTiming();
      //   Rulebook State Chart
      await this.getRulebookState();
      //   Rulebook State Card
      await this.getPendingRulebook();
      await this.getCompletedRulebook();
      await this.getReviewedRulebook();
      await this.getSubmittedRulebook();
    });
  }

  //   Get Dates
  getDates() {
    const calculatedDate = moment()
      .subtract(this.state.period, "days")
      .startOf("day")
      .format("YYYY/MM/DD HH:mm:ss");

    console.log("Calculated Date:", calculatedDate);

    this.state.current_date = calculatedDate;

    const PreviousDate = moment()
      .subtract(this.state.period * 2, "days")
      .startOf("day")
      .format("YYYY/MM/DD HH:mm:ss");

    console.log("Calculated Date:", PreviousDate);

    this.state.previous_date = PreviousDate;
  }

  //   ON Change Period
  async OnChangePeriod() {
    this.getDates();
    await this.getLateReply();
    await this.getEarlyReply();
    await this.getNoReply();
    //
    await this.getRulebookState();
    //
    await this.getRulebookResponseTiming();
    //
    await this.getPendingRulebook();
    await this.getCompletedRulebook();
    await this.getReviewedRulebook();
    await this.getSubmittedRulebook();

    await this.getPendingReply();
  }

  async getPendingReply() {
    const current_date = this.state.current_date;
    const period = this.state.period;
    const previous_date = this.state.previous_date;

    const domain = [["submission_timing", "in", ["on_time"]]];

    if (period > 0) {
      domain.push(["reply_date", ">", current_date]);
    }

    const data = await this.orm.searchCount("reply.log", domain);

    this.state.pendingReply.value = data;

    const prev_domain = [["submission_timing", "in", ["on_time"]]];

    if (period > 0) {
      prev_domain.push(
        ["reply_date", ">", previous_date],
        ["reply_date", "<=", previous_date]
      );
    }

    const prev_data = await this.orm.searchCount("reply.log", prev_domain);
    const percentage = ((data - prev_data) / prev_data) * 100;
    // this.state.earlyReply.percentage = percentage.toFixed(2);
    this.state.pendingReply.percentage = isFinite(percentage)
      ? percentage.toFixed(2)
      : "0.00";

    console.log("current date", current_date, "previous date", previous_date);
  }

  // Get Rulebooks with Early Reply
  async getEarlyReply() {
    const current_date = this.state.current_date;
    const period = this.state.period;
    const previous_date = this.state.previous_date;

    const domain = [["submission_timing", "in", ["early"]]];

    if (period > 0) {
      domain.push(["reply_date", ">", current_date]);
    }
    // Debug log
    console.log("domain: ", domain);
    console.log("Period: ", period);
    console.log("fORMATTED DATE: ", current_date);

    const data = await this.orm.searchCount("reply.log", domain);

    this.state.earlyReply.value = data;

    const prev_domain = [["submission_timing", "in", ["early"]]];

    if (period > 0) {
      prev_domain.push(
        ["reply_date", ">", previous_date],
        ["reply_date", "<=", previous_date]
      );
    }
    // Debug log
    console.log("prev domain: ", prev_domain);
    console.log("prev fORMATTED DATE: ", previous_date);

    const prev_data = await this.orm.searchCount("reply.log", prev_domain);
    const percentage = ((data - prev_data) / prev_data) * 100;
    // this.state.earlyReply.percentage = percentage.toFixed(2);
    this.state.noReply.percentage = isFinite(percentage)
      ? percentage.toFixed(2)
      : "0.00";

    console.log("current date", current_date, "previous date", previous_date);
  }

  //Get Rulebooks with Late Reply
  async getLateReply() {
    const current_date = this.state.current_date;
    const period = this.state.period;
    const previous_date = this.state.previous_date;

    const domain = [["submission_timing", "in", ["late"]]];

    if (period > 0) {
      domain.push(["reply_date", ">", current_date]);
    }
    // Debug log
    console.log("domain: ", domain);
    console.log("Period: ", period);
    console.log("fORMATTED DATE: ", current_date);

    const data = await this.orm.searchCount("reply.log", domain);

    this.state.lateReply.value = data;

    const prev_domain = [["submission_timing", "in", ["late"]]];

    if (period > 0) {
      prev_domain.push(
        ["reply_date", ">", previous_date],
        ["reply_date", "<=", previous_date]
      );
    }
    // Debug log
    console.log("prev domain: ", prev_domain);
    console.log("prev fORMATTED DATE: ", previous_date);

    const prev_data = await this.orm.searchCount("reply.log", prev_domain);
    const percentage = ((data - prev_data) / prev_data) * 100;
    // this.state.lateReply.percentage = percentage.toFixed(2);
    this.state.noReply.percentage = isFinite(percentage)
      ? percentage.toFixed(2)
      : "0.00";

    console.log("current date", current_date, "previous date", previous_date);
  }

  // Get Rulebooks with No Reply
  async getNoReply() {
    const current_date = this.state.current_date;
    const period = this.state.period;
    const previous_date = this.state.previous_date;

    const domain = [["submission_timing", "in", ["not_responded"]]];

    if (period > 0) {
      domain.push(["reply_date", ">", current_date]);
    }
    // Debug log
    console.log("domain: ", domain);
    console.log("Period: ", period);
    console.log("fORMATTED DATE: ", current_date);

    const data = await this.orm.searchCount("reply.log", domain);

    this.state.noReply.value = data;

    const prev_domain = [["submission_timing", "in", ["not_responded"]]];

    if (period > 0) {
      prev_domain.push(
        ["reply_date", ">", previous_date],
        ["reply_date", "<=", previous_date]
      );
    }
    // Debug log
    console.log("prev domain: ", prev_domain);
    console.log("prev fORMATTED DATE: ", previous_date);

    const prev_data = await this.orm.searchCount("reply.log", prev_domain);
    const percentage = ((data - prev_data) / prev_data) * 100;
    // this.state.noReply.percentage = percentage.toFixed(2);
    this.state.noReply.percentage = isFinite(percentage)
      ? percentage.toFixed(2)
      : "0.00";

    console.log("current date", current_date, "previous date", previous_date);
  }

  //   Rulebook that was replied to Late
  async viewLateReply() {
    const domain = [["submission_timing", "in", ["late"]]];

    if (this.state.period > 0) {
      domain.push(["reply_date", ">", this.state.current_date]);
    }

    let list_view = await this.orm.searchRead(
      "ir.model.data",
      [["name", "=", "view_reply_log_tree"]],
      ["res_id"]
    );

    this.actionService.doAction({
      type: "ir.actions.act_window",
      name: "Late Replies",
      res_model: "reply.log",
      domain,
      views: [
        [list_view.length > 0 ? list_view[0].res_id : false, "list"],
        [false, "form"],
      ],
    });
  }

  //   Rulebook that was replied to Early
  async viewEarlyReply() {
    const domain = [["submission_timing", "in", ["early"]]];

    if (this.state.period > 0) {
      domain.push(["reply_date", ">", this.state.current_date]);
    }

    let list_view = await this.orm.searchRead(
      "ir.model.data",
      [["name", "=", "view_reply_log_tree"]],
      ["res_id"]
    );

    this.actionService.doAction({
      type: "ir.actions.act_window",
      name: "Early Replies",
      res_model: "reply.log",
      domain,
      views: [
        [list_view.length > 0 ? list_view[0].res_id : false, "list"],
        [false, "form"],
      ],
    });
  }

  //   Rulebook that has not been responded to
  async viewNoReply() {
    const domain = [["submission_timing", "in", ["not_responded"]]];

    if (this.state.period > 0) {
      domain.push(["reply_date", ">", this.state.current_date]);
    }

    let list_view = await this.orm.searchRead(
      "ir.model.data",
      [["name", "=", "view_reply_log_tree"]],
      ["res_id"]
    );

    this.actionService.doAction({
      type: "ir.actions.act_window",
      name: "Not Respoded",
      res_model: "reply.log",
      domain,
      views: [
        [list_view.length > 0 ? list_view[0].res_id : false, "list"],
        [false, "form"],
      ],
    });
  }

  //   Rulebook that is not yet due (pending)
  async viewPendingReply() {
    const domain = [["submission_timing", "in", ["on_time"]]];

    if (this.state.period > 0) {
      domain.push(["reply_date", ">", this.state.current_date]);
    }

    let list_view = await this.orm.searchRead(
      "ir.model.data",
      [["name", "=", "view_reply_log_tree"]],
      ["res_id"]
    );

    this.actionService.doAction({
      type: "ir.actions.act_window",
      name: "Pending",
      res_model: "reply.log",
      domain,
      views: [
        [list_view.length > 0 ? list_view[0].res_id : false, "list"],
        [false, "form"],
      ],
    });
  }

  //   Rulebook Status View Count

  async getPendingRulebook() {
    const current_date = this.state.current_date;
    const period = this.state.period;
    const previous_date = this.state.previous_date;

    const domain = [["rulebook_status", "in", ["pending"]]];

    if (period > 0) {
      domain.push(["reply_date", ">", current_date]);
    }

    const data = await this.orm.searchCount("reply.log", domain);

    this.state.pending.value = data;

    const prev_domain = [["rulebook_status", "in", ["pending"]]];

    if (period > 0) {
      prev_domain.push(
        ["reply_date", ">", previous_date],
        ["reply_date", "<=", previous_date]
      );
    }

    const prev_data = await this.orm.searchCount("reply.log", prev_domain);
    const percentage = ((data - prev_data) / prev_data) * 100;
    // this.state.earlyReply.percentage = percentage.toFixed(2);
    this.state.pending.percentage = isFinite(percentage)
      ? percentage.toFixed(2)
      : "0.00";

    console.log("current date", current_date, "previous date", previous_date);
  }

  async getReviewedRulebook() {
    const current_date = this.state.current_date;
    const period = this.state.period;
    const previous_date = this.state.previous_date;

    const domain = [["rulebook_status", "in", ["reviewed"]]];

    if (period > 0) {
      domain.push(["reply_date", ">", current_date]);
    }

    const data = await this.orm.searchCount("reply.log", domain);

    this.state.reviewed.value = data;

    const prev_domain = [["rulebook_status", "in", ["reviewed"]]];

    if (period > 0) {
      prev_domain.push(
        ["reply_date", ">", previous_date],
        ["reply_date", "<=", previous_date]
      );
    }

    const prev_data = await this.orm.searchCount("reply.log", prev_domain);
    const percentage = ((data - prev_data) / prev_data) * 100;
    // this.state.earlyReply.percentage = percentage.toFixed(2);
    this.state.submitted.reviewed = isFinite(percentage)
      ? percentage.toFixed(2)
      : "0.00";

    console.log("current date", current_date, "previous date", previous_date);
  }

  async getSubmittedRulebook() {
    const current_date = this.state.current_date;
    const period = this.state.period;
    const previous_date = this.state.previous_date;

    const domain = [["rulebook_status", "in", ["submitted"]]];

    if (period > 0) {
      domain.push(["reply_date", ">", current_date]);
    }

    const data = await this.orm.searchCount("reply.log", domain);

    this.state.submitted.value = data;

    const prev_domain = [["rulebook_status", "in", ["submitted"]]];

    if (period > 0) {
      prev_domain.push(
        ["reply_date", ">", previous_date],
        ["reply_date", "<=", previous_date]
      );
    }

    const prev_data = await this.orm.searchCount("reply.log", prev_domain);
    const percentage = ((data - prev_data) / prev_data) * 100;
    // this.state.earlyReply.percentage = percentage.toFixed(2);
    this.state.submitted.percentage = isFinite(percentage)
      ? percentage.toFixed(2)
      : "0.00";

    console.log("current date", current_date, "previous date", previous_date);
  }

  async getCompletedRulebook() {
    const current_date = this.state.current_date;
    const period = this.state.period;
    const previous_date = this.state.previous_date;

    const domain = [["rulebook_status", "in", ["completed"]]];

    if (period > 0) {
      domain.push(["reply_date", ">", current_date]);
    }

    const data = await this.orm.searchCount("reply.log", domain);

    this.state.completed.value = data;

    const prev_domain = [["rulebook_status", "in", ["completed"]]];

    if (period > 0) {
      prev_domain.push(
        ["reply_date", ">", previous_date],
        ["reply_date", "<=", previous_date]
      );
    }

    const prev_data = await this.orm.searchCount("reply.log", prev_domain);
    const percentage = ((data - prev_data) / prev_data) * 100;
    // this.state.earlyReply.percentage = percentage.toFixed(2);
    this.state.completed.percentage = isFinite(percentage)
      ? percentage.toFixed(2)
      : "0.00";

    console.log("current date", current_date, "previous date", previous_date);
  }

  //   Rulebook Status View
  async viewPending() {
    const domain = [["rulebook_status", "in", ["pending"]]];

    if (this.state.period > 0) {
      domain.push(["reply_date", ">", this.state.current_date]);
    }

    let list_view = await this.orm.searchRead(
      "ir.model.data",
      [["name", "=", "view_reply_log_tree"]],
      ["res_id"]
    );

    this.actionService.doAction({
      type: "ir.actions.act_window",
      name: "Pending Rulebook",
      res_model: "reply.log",
      domain,
      views: [
        [list_view.length > 0 ? list_view[0].res_id : false, "list"],
        [false, "form"],
      ],
    });
  }

  async viewReviewed() {
    const domain = [["rulebook_status", "in", ["reviewed"]]];

    if (this.state.period > 0) {
      domain.push(["reply_date", ">", this.state.current_date]);
    }

    let list_view = await this.orm.searchRead(
      "ir.model.data",
      [["name", "=", "view_reply_log_tree"]],
      ["res_id"]
    );

    this.actionService.doAction({
      type: "ir.actions.act_window",
      name: "Reviewed Rulebook",
      res_model: "reply.log",
      domain,
      views: [
        [list_view.length > 0 ? list_view[0].res_id : false, "list"],
        [false, "form"],
      ],
    });
  }

  async viewSubmitted() {
    const domain = [["rulebook_status", "in", ["submitted"]]];

    if (this.state.period > 0) {
      domain.push(["reply_date", ">", this.state.current_date]);
    }

    let list_view = await this.orm.searchRead(
      "ir.model.data",
      [["name", "=", "view_reply_log_tree"]],
      ["res_id"]
    );

    this.actionService.doAction({
      type: "ir.actions.act_window",
      name: "Submitted Rulebook",
      res_model: "reply.log",
      domain,
      views: [
        [list_view.length > 0 ? list_view[0].res_id : false, "list"],
        [false, "form"],
      ],
    });
  }

  async viewCompleted() {
    const domain = [["rulebook_status", "in", ["completed"]]];

    if (this.state.period > 0) {
      domain.push(["reply_date", ">", this.state.current_date]);
    }

    let list_view = await this.orm.searchRead(
      "ir.model.data",
      [["name", "=", "view_reply_log_tree"]],
      ["res_id"]
    );

    this.actionService.doAction({
      type: "ir.actions.act_window",
      name: "Completed Rulebook",
      res_model: "reply.log",
      domain,
      views: [
        [list_view.length > 0 ? list_view[0].res_id : false, "list"],
        [false, "form"],
      ],
    });
  }


}

AlertDashboard.template = "AlertDashboard";
AlertDashboard.components = { KpiCard, ChartRenderer };

registry.category("actions").add("alert_dashboard", AlertDashboard);
