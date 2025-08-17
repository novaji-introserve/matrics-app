// static/src/js/view_access_manager.js
odoo.define("view_access_control.manager", function (require) {
  "use strict";

  const { Component, useState, useRef, onMounted, onWillStart } = owl;
  const { useService } = require("@web/core/utils/hooks");
  const { registry } = require("@web/core/registry");

  class ViewAccessManager extends Component {
    setup() {
      this.state = useState({
        models: [],
        actions: [],
        groups: [],
        rules: [],
        selectedModel: null,
        selectedActions: [],
        selectedGroups: [],
        ruleName: "",
        filteredActions: [],
        loading: false,
        message: "",
        messageType: "success",
      });

      this.rpc = useService("rpc");
      this.formRef = useRef("form");

      onWillStart(async () => {
        await this.loadData();
      });
    }

    async loadData() {
      this.state.loading = true;
      try {
        // Load all data in parallel
        const [models, actions, groups, rules] = await Promise.all([
          this.rpc("/view_access/get_available_models"),
          this.rpc("/view_access/get_available_actions"),
          this.rpc("/view_access/get_available_groups"),
          this.rpc("/web/dataset/search_read", {
            model: "view.access.rule",
            fields: ["name", "model_id", "action_ids", "group_ids", "active"],
            domain: [],
          }),
        ]);

        this.state.models = models;
        this.state.actions = actions;
        this.state.groups = groups;
        this.state.rules = rules.records || [];
        this.state.loading = false;
      } catch (error) {
        this.showMessage(`Error loading data: ${error.message}`, "danger");
        this.state.loading = false;
      }
    }

    onModelChange(ev) {
      const modelId = parseInt(ev.target.value);
      this.state.selectedModel = modelId || null;

      if (modelId) {
        const model = this.state.models.find((m) => m.id === modelId);
        if (model) {
          this.state.filteredActions = this.state.actions.filter(
            (action) => action.model === model.model
          );
        }
      } else {
        this.state.filteredActions = [];
      }

      // Reset selected actions when model changes
      this.state.selectedActions = [];
    }

    onActionChange(ev) {
      const selectedOptions = Array.from(ev.target.selectedOptions);
      this.state.selectedActions = selectedOptions.map((option) =>
        parseInt(option.value)
      );
    }

    onGroupChange(ev) {
      const selectedOptions = Array.from(ev.target.selectedOptions);
      this.state.selectedGroups = selectedOptions.map((option) =>
        parseInt(option.value)
      );
    }

    onNameChange(ev) {
      this.state.ruleName = ev.target.value;
    }

    async createRule() {
      if (!this.state.ruleName.trim()) {
        this.showMessage("Please enter a rule name", "warning");
        return;
      }

      if (!this.state.selectedModel) {
        this.showMessage("Please select a model", "warning");
        return;
      }

      if (this.state.selectedGroups.length === 0) {
        this.showMessage("Please select at least one group", "warning");
        return;
      }

      this.state.loading = true;
      try {
        // Create the rule
        await this.rpc("/web/dataset/call_kw", {
          model: "view.access.rule",
          method: "create",
          args: [
            {
              name: this.state.ruleName,
              model_id: this.state.selectedModel,
              action_ids:
                this.state.selectedActions.length > 0
                  ? [[6, 0, this.state.selectedActions]]
                  : false,
              group_ids: [[6, 0, this.state.selectedGroups]],
              active: true,
            },
          ],
          kwargs: {},
        });

        // Reload rules
        await this.loadData();

        // Clear form
        this.state.ruleName = "";
        this.state.selectedModel = null;
        this.state.selectedActions = [];
        this.state.selectedGroups = [];
        this.state.filteredActions = [];

        this.showMessage("Rule created successfully", "success");
      } catch (error) {
        this.showMessage(`Error creating rule: ${error.message}`, "danger");
      } finally {
        this.state.loading = false;
      }
    }

    async deleteRule(ruleId) {
      if (confirm("Are you sure you want to delete this rule?")) {
        this.state.loading = true;
        try {
          await this.rpc("/web/dataset/call_kw", {
            model: "view.access.rule",
            method: "unlink",
            args: [ruleId],
            kwargs: {},
          });

          await this.loadData();
          this.showMessage("Rule deleted successfully", "success");
        } catch (error) {
          this.showMessage(`Error deleting rule: ${error.message}`, "danger");
        } finally {
          this.state.loading = false;
        }
      }
    }

    showMessage(message, type = "info") {
      this.state.message = message;
      this.state.messageType = type;

      // Clear message after 5 seconds
      setTimeout(() => {
        this.state.message = "";
      }, 5000);
    }

    getModelName(modelId) {
      const model = this.state.models.find((m) => m.id === modelId);
      return model ? model.name : "";
    }

    getActionNames(actionIds) {
      if (!actionIds || actionIds.length === 0) {
        return "All Actions";
      }

      return actionIds
        .map((id) => {
          const action = this.state.actions.find((a) => a.id === id);
          return action ? action.name : "";
        })
        .join(", ");
    }

    getGroupNames(groupIds) {
      return groupIds
        .map((id) => {
          const group = this.state.groups.find((g) => g.id === id);
          return group ? group.name : "";
        })
        .join(", ");
    }
  }

  ViewAccessManager.template = "view_access_control.ViewAccessManager";

  registry.category("actions").add("view_access.manager", ViewAccessManager);

  return ViewAccessManager;
});
