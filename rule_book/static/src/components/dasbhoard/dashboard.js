/** @odoo-module **/
// import { session } from '@web/session';
import { registry } from "@web/core/registry";
const { Component, useState, onWillStart, useRef } = owl;
import { useService } from "@web/core/utils/hooks";
import { session } from "@web/session";


export class Dashboard extends Component {
  setup() {
    this.state = useState({
      escalationDueDates: [],
      internalDueDates: [],
      reminderDueDates: [],
      regulatoryDueDates: [],
      isLoading: true,
      totalRulebooks: 0,
      newRulebooksToday: 0,
      totalTitle: 0,
      newTitleToday: 0,
      totalThemes: 0,
      newThemesToday: 0,
      totalReplies: 0,
      newRepliesToday: 0,
      totalSources: 0,
      newSourcesToday: 0,
      totalChatLogs: 0,
      newChatLogsToday: 0,
      awaitingReplies: [],
      newlyUploadedTitle: [],
      mostAskedQuestion: [],
      //   hasPermission: true,
      hasPermission: false, // Check user group
    });

    // Initialize the orm service
    this.orm = useService("orm");
    this.user = useService("user");

    onWillStart(async () => {
      this.state.hasPermission = await this.user.hasGroup(
        "rule_book.group_compliance_manager_"
      );
      await this.fetchCounts();
      await this.fetchData();

      if (this.state.hasPermission) {
        await this.fetchAwaitingReplies();
        await this.fetchNewlyUploadedTitle();
        await this.fetchMostAskedAiQuestion();
      }
    });
  }

  async fetchAwaitingReplies() {
    try {
      // Using the ORM to fetch awaiting replies
      console.log("Using the ORM to fetch awaiting replies");
      const replies = await this.orm.call(
        "reply.log",
        "get_awaiting_replies",
        []
      );
      this.state.awaitingReplies = replies;
      console.log(replies); // Print the replies
    } catch (error) {
      console.error("Failed to fetch awaiting replies:", error);
    }
  }
  async fetchNewlyUploadedTitle() {
    try {
      // Using the ORM to fetch awaiting replies
      const newlyUploadedTitle = await this.orm.call(
        "rulebook.title",
        "fetch_new_ai_titles",
        []
      );
      this.state.newlyUploadedTitle = newlyUploadedTitle;
      console.log(newlyUploadedTitle); // Print the replies
    } catch (error) {
      console.error("Failed to fetch newly uploaded title:", error);
    }
  }
  async fetchMostAskedAiQuestion() {
    try {
      // Using the ORM to fetch awaiting replies
      const mostAskedQuestion = await this.orm.call(
        "pdf.chat.log",
        "get_most_asked_questions",
        []
      );
      this.state.mostAskedQuestion = mostAskedQuestion;
      console.log(mostAskedQuestion); // Print the replies
    } catch (error) {
      console.error("Failed to fetch newly uploaded title:", error);
    }
  }
  async fetchCounts() {
    // Fetch total rulebooks
    this.state.totalRulebooks = await this._fetchCount("rulebook");

    // Fetch new rulebooks created today
    this.state.newRulebooksToday = await this._fetchCount("rulebook", {
      created_on: ">=",
      today: new Date().toISOString().split("T")[0] + " 00:00:00", // Start of today
    });

    this.state.totalTitle = await this._fetchCount("rulebook.title");

    // Fetch new rulebooks created today
    this.state.newTitleToday = await this._fetchCount("rulebook.title", {
      created_on: ">=",
      today: new Date().toISOString().split("T")[0] + " 00:00:00", // Start of today
    });

    // Fetch total themes
    this.state.totalThemes = await this._fetchCount("rulebook.theme");

    // Fetch new themes created today
    this.state.newThemesToday = await this._fetchCount("rulebook.theme", {
      created_on: ">=",
      today: new Date().toISOString().split("T")[0] + " 00:00:00",
    });

    // Fetch total replies
    this.state.totalReplies = await this._fetchCount("reply.log");

    // Fetch new replies created today
    this.state.newRepliesToday = await this._fetchCount("reply.log", {
      created_on: ">=",
      today: new Date().toISOString().split("T")[0] + " 00:00:00",
    });

    // Fetch total sources
    this.state.totalSources = await this._fetchCount("rulebook.sources");

    // Fetch new sources created today
    this.state.newSourcesToday = await this._fetchCount("rulebook.sources", {
      created_on: ">=",
      today: new Date().toISOString().split("T")[0] + " 00:00:00",
    });

    // Fetch total chat logs
    this.state.totalChatLogs = await this._fetchCount("pdf.chat.log");

    // Fetch new chat logs created today
    this.state.newChatLogsToday = await this._fetchCount("pdf.chat.log", {
      created_on: ">=",
      today: new Date().toISOString().split("T")[0] + " 00:00:00",
    });

    // Update the component state or trigger a re-render
    this.render();
  }

  async _fetchCount(modelName, filters = {}) {
    const domain = [];

    // Check if filters have a valid created_on property
    if (filters.created_on && filters.today) {
      domain.push(["create_date", ">=", filters.today]);
    }

    try {
      const count = await this.orm.searchCount(modelName, domain);

      return count;
    } catch (error) {
      console.error(`Error fetching count for ${modelName}:`, error);
      return 0; // Return 0 or handle the error as appropriate
    }
  }

  async fetchData() {
    try {
      // Fetch first 10 Escalation Due Dates
      const today = new Date(); // Get the current date
      today.setHours(0, 0, 0, 0); // Set to the start of the day


      const userIsComplianceManager = await this.env.services.user.hasGroup(
        "rule_book.group_compliance_manager_"
      );
     
      const userId = session.uid;
      
  
      const userDepartmentId = await this.orm.searchRead(
        "hr.employee",
        [["user_id", "=", userId]],
        ["department_id"],
        {
          limit: 1,
        }
      );
      const departmentId = userDepartmentId?.[0]?.department_id?.[0] || null;

      console.log(`user department id `, departmentId);
      console.log(`user userIsComplianceManager `, userIsComplianceManager);
      console.log("User ID " , session.uid);

      // Base domain for active records
      const baseDomain = [["status", "=", "active"]];

      // If the user is not a compliance manager, restrict by department
      if (!userIsComplianceManager) {
         baseDomain.push(["department_id", "=", departmentId]); // Filter by department for non-compliance managers
      }

      // Fetch Escalation Due Dates
      const escalationDueDates = await this.orm.searchRead(
        "reply.log",
        [
          ...baseDomain,
          ["escalation_date", "!=", false], // Ensure escalation_date is valid (not false)
          ["escalation_date", ">=", today.toISOString()], // Optional: Filter for dates greater than or equal to today
        ],
        ["id", "rulebook_name", "escalation_date", "department_id"],
        {
          limit: 5,
          order: "escalation_date asc",
        }
      );

      // Fetch Internal Due Dates (rulebook_compute_date)
      const internalDueDates = await this.orm.searchRead(
        "reply.log",
        [
          ...baseDomain,
          ["rulebook_compute_date", "!=", false], // Ensure rulebook_compute_date is valid (not false)
          ["rulebook_compute_date", ">=", today.toISOString()], // Optional: Filter for dates greater than or equal to today
        ],
        ["id", "rulebook_name", "rulebook_compute_date", "department_id"],
        {
          limit: 5,
          order: "rulebook_compute_date asc",
        }
      );

      // Fetch Reminder Due Dates
      const reminderDueDates = await this.orm.searchRead(
        "reply.log",
        [
          ...baseDomain,
          ["reminder_due_date", "!=", false], // Ensure reminder_due_date is valid (not false)
          ["reminder_due_date", ">=", today.toISOString()], // Optional: Filter for dates greater than or equal to today
        ],
        ["id", "rulebook_name", "reminder_due_date", "department_id"],
        {
          limit: 5,
          order: "reminder_due_date asc",
        }
      );

      // Fetch Regulatory Due Dates (reg_due_date)
      const regulatoryDueDates = await this.orm.searchRead(
        "reply.log",
        [
          ...baseDomain,
          ["reg_due_date", "!=", false], // Ensure reg_due_date is valid (not false)
          ["reg_due_date", ">=", today.toISOString()], // Optional: Filter for dates greater than or equal to today
        ],
        ["id", "rulebook_name", "reg_due_date", "department_id"],
        {
          limit: 5,
          order: "reg_due_date asc",
        }
      );

      // const escalationDueDates = await this.orm.searchRead(
      //   "reply.log",
      //   [
      //     ["escalation_date", "!=", false],
      //     ["status", "=", "active"],
      //     ["escalation_date", ">=", today.toISOString()],
      //   ],
      //   ["id", "rulebook_name", "escalation_date", "department_id"],
      //   { limit: 5, order: "escalation_date asc" }
      // );

      // // Fetch first 10 Internal Due Dates
      // const regulatoryDueDates = await this.orm.searchRead(
      //   "reply.log",
      //   [
      //     ["reg_due_date", "!=", false],
      //     ["status", "=", "active"],
      //     ["reg_due_date", ">=", today.toISOString()],
      //   ],
      //   ["id", "rulebook_name", "reg_due_date", "department_id"],
      //   { limit: 5, order: "reg_due_date asc" }
      // );

      // // Fetch first 10 Reminder Due Dates
      // const reminderDueDates = await this.orm.searchRead(
      //   "reply.log",
      //   [
      //     ["reminder_due_date", "!=", false],
      //     ["status", "=", "active"],
      //     ["reminder_due_date", ">=", today.toISOString()],
      //   ],
      //   ["id", "rulebook_name", "reminder_due_date", "department_id"],
      //   { limit: 5, order: "reminder_due_date asc" }
      // );

      // // Fetch first 10 Regulatory Due Dates (computed_date)

      // const internalDueDates = await this.orm.searchRead(
      //   // regulatoryDueDates
      //   "reply.log",
      //   [
      //     ["rulebook_compute_date", "!=", false],
      //     ["rulebook_compute_date", ">=", today.toISOString()], // Filter for dates greater than or equal to today
      //     ["status", "=", "active"],
      //   ],
      //   ["id", "rulebook_name", "rulebook_compute_date", "department_id"],
      //   {
      //     limit: 5,
      //     order: "rulebook_compute_date asc",
      //   }
      // );
      console.log("date time for today ", today.toISOString(), "");

      this.state.escalationDueDates = escalationDueDates.map((date) => ({
        ...date,
        escalation_date: new Date(date.escalation_date).toLocaleDateString(
          "en-US",
          {
            year: "numeric",
            month: "long",
            day: "numeric",
          }
        ),
        link: this.getRulebookLink(date.id),
        type_of_return: date.rulebook_name.replace(/<[^>]*>/g, ""),
      }));

      this.state.internalDueDates = internalDueDates.map((date) => ({
        ...date,
        internal_due_date: new Date(
          date.rulebook_compute_date
        ).toLocaleDateString("en-US", {
          year: "numeric",
          month: "long",
          day: "numeric",
        }),
        link: this.getRulebookLink(date.id),
        type_of_return: date.rulebook_name.replace(/<[^>]*>/g, ""),
      }));

      this.state.reminderDueDates = reminderDueDates.map((date) => ({
        ...date,
        reminder_due_date: new Date(date.reminder_due_date).toLocaleDateString(
          "en-US",
          {
            year: "numeric",
            month: "long",
            day: "numeric",
          }
        ),
        link: this.getRulebookLink(date.id),
        type_of_return: date.rulebook_name.replace(/<[^>]*>/g, ""),
      }));

      this.state.regulatoryDueDates = regulatoryDueDates.map((date) => ({
        ...date,
        reg_due_date: new Date(date.reg_due_date).toLocaleDateString("en-US", {
          year: "numeric",
          month: "long",
          day: "numeric",
        }),
        link: this.getRulebookLink(date.id),
        type_of_return: date.rulebook_name.replace(/<[^>]*>/g, ""),
      }));
    } catch (error) {
      console.error("Error:", error);
      this.state.error = "Failed to fetch due dates.";
    } finally {
      this.state.isLoading = false;
    }
  }

  getRulebookLink(id) {
    // Constructs a URL to the rulebook's form view
    return `/web#id=${id}&model=reply.log&view_type=form`;
  }
}

// Dashboard.template = "rule_book.Dashboard";
Dashboard.template = "rule_book.DashboardMain";
registry
  .category("actions")
  .add("rule_book.action_cusotomer_details_js", Dashboard);
