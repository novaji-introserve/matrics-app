/** @odoo-module **/

import { registry } from "@web/core/registry";
import { Component } from "@odoo/owl";
import { useService } from "@web/core/utils/hooks";

export class Dashboard extends Component {
  setup() {
    // Initialize services
    this.orm = useService("orm");
    this.user = useService("user");

    // Initialize state
    this.state = {
      hasPermission: false,
      isLoading: true,
      counts: {
        rulebooks: { total: 0, today: 0 },
        titles: { total: 0, today: 0 },
        themes: { total: 0, today: 0 },
        replies: { total: 0, today: 0 },
        sources: { total: 0, today: 0 },
        chatLogs: { total: 0, today: 0 },
      },
      dueDates: {
        escalation: [],
        internal: [],
        regulatory: [],
      },
      awaitingReplies: [],
      newlyUploadedTitle: [],
      mostAskedQuestion: [],
    };

    this._initializeData();
  }

  async _initializeData() {
    try {
      await this.checkPermissions();

      if (this.state.hasPermission) {
        await Promise.all([
          this.fetchAllCounts(),
          this.fetchDueDates(),
          this.fetchAwaitingReplies(),
          this.fetchNewlyUploadedTitle(),
          this.fetchMostAskedAiQuestion(),
        ]);
      }
    } catch (error) {
      console.error("Error initializing dashboard:", error);
    } finally {
      this.state.isLoading = false;
    }
  }

  // async checkPermissions() {
  //   try {
  //     user=this.state.hasPermission = await this.user.hasGroup(
  //       "group_compliance_manager_"
  //     );
  //     console.log("Logged this data successfully", user);
  //   } catch (error) {
  //     console.error("Error checking permissions:", error);
  //     this.state.hasPermission = false;
  //   }
  // }
  async checkPermissions() {
    try {
      const user = await this.user.hasGroup("group_compliance_manager_");
      this.state.hasPermission = user;
      console.log("Logged this data successfully", user);
    } catch (error) {
      console.error("Error checking permissions:", error);
      this.state.hasPermission = false;
    }
  }

  async fetchAllCounts() {
    const today = new Date().toISOString().split("T")[0] + " 00:00:00";
    const models = {
      rulebooks: "rulebook",
      titles: "rulebook.title",
      themes: "rulebook.theme",
      replies: "reply.log",
      sources: "rulebook.sources",
      chatLogs: "pdf.chat.log",
    };

    for (const [key, model] of Object.entries(models)) {
      try {
        const [total, todayCount] = await Promise.all([
          this._fetchCount(model),
          this._fetchCount(model, { created_on: ">=", today }),
        ]);

        this.state.counts[key] = { total, today: todayCount };
      } catch (error) {
        console.error(`Error fetching counts for ${model}:`, error);
        this.state.counts[key] = { total: 0, today: 0 };
      }
    }
  }

  async _fetchCount(modelName, filters = {}) {
    const domain = filters.created_on
      ? [["create_date", filters.created_on, filters.today]]
      : [];
    try {
      return await this.orm.searchCount(modelName, domain);
    } catch (error) {
      console.error(`Error fetching count for ${modelName}:`, error);
      return 0;
    }
  }

  async fetchDueDates() {
    const commonDomain = [["status", "=", "active"]];
    const commonFields = ["id", "type_of_return", "responsible_id"];
    const commonParams = { limit: 5 };

    try {
      const [escalation, internal, regulatory] = await Promise.all([
        this.orm.searchRead(
          "rulebook",
          [...commonDomain, ["escalation_date", "!=", false]],
          [...commonFields, "escalation_date"],
          { ...commonParams, order: "escalation_date asc" }
        ),
        this.orm.searchRead(
          "rulebook",
          [...commonDomain, ["due_date", "!=", false]],
          [...commonFields, "due_date"],
          { ...commonParams, order: "due_date asc" }
        ),
        this.orm.searchRead(
          "rulebook",
          [...commonDomain, ["computed_date", "!=", false]],
          [...commonFields, "computed_date"],
          { ...commonParams, order: "computed_date asc" }
        ),
      ]);

      this.state.dueDates = {
        escalation: this.formatDueDates(escalation, "escalation_date"),
        internal: this.formatDueDates(internal, "due_date"),
        regulatory: this.formatDueDates(regulatory, "computed_date"),
      };
    } catch (error) {
      console.error("Error fetching due dates:", error);
    }
  }

  formatDueDates(dates, dateField) {
    return dates.map((date) => ({
      ...date,
      [dateField]: new Date(date[dateField]).toLocaleDateString("en-US", {
        year: "numeric",
        month: "long",
        day: "numeric",
      }),
      link: this.getRulebookLink(date.id),
      type_of_return: date.type_of_return.replace(/<[^>]*>/g, ""),
    }));
  }

  async fetchAwaitingReplies() {
    try {
      this.state.awaitingReplies = await this.orm.call(
        "reply.log",
        "get_awaiting_replies",
        []
      );
    } catch (error) {
      console.error("Failed to fetch awaiting replies:", error);
      this.state.awaitingReplies = [];
    }
  }

  async fetchNewlyUploadedTitle() {
    try {
      this.state.newlyUploadedTitle = await this.orm.call(
        "rulebook.title",
        "fetch_new_ai_titles",
        []
      );
    } catch (error) {
      console.error("Failed to fetch newly uploaded title:", error);
      this.state.newlyUploadedTitle = [];
    }
  }

  async fetchMostAskedAiQuestion() {
    try {
      this.state.mostAskedQuestion = await this.orm.call(
        "pdf.chat.log",
        "get_most_asked_questions",
        []
      );
    } catch (error) {
      console.error("Failed to fetch most asked questions:", error);
      this.state.mostAskedQuestion = [];
    }
  }

  getRulebookLink(id) {
    return `/web#id=${id}&model=rulebook&view_type=form`;
  }
}

Dashboard.template = "rule_book.Dashboard";

registry
  .category("actions")
  .add("rule_book.action_customer_details_js", Dashboard);
