// /** @odoo-module **/
// import { registry } from "@web/core/registry";
// import { ImportFormComponent } from "./import_form_component";
// import { TerminalComponent } from "./terminal_component";

// // Register the client action with full properties
// registry.category("actions").add("compliance_management.csv_import_form", async (env) => {
//     return {
//         type: "ir.actions.client",  // This line is critical
//         tag: "compliance_management.csv_import_form",
//         Component: ImportFormComponent,
//         target: "current",
//     };
// });

// // Register components if needed
// registry.category("components").add("TerminalComponent", TerminalComponent);


/** @odoo-module **/
import { registry } from "@web/core/registry";
import { ImportFormComponent } from "./import_form_component";
import { TerminalComponent } from "./terminal_component";

// Ensure action type is always defined
registry.category("actions").add("compliance_management.csv_import_form",
    function (env, action = {}) {  // Default empty object to prevent undefined
        if (!action || !action.type) {
            console.warn("Received action with no type:", action);
            action = { ...action, type: "ir.actions.client" };  // Ensure default type
        }
        return {
            Component: ImportFormComponent,
            props: { action },
        };
    }
);

// Register additional components
registry.category("components").add("TerminalComponent", TerminalComponent);

// /** @odoo-module **/
// import { registry } from "@web/core/registry";
// import { ImportFormComponent } from "./import_form_component";
// import { TerminalComponent } from "./terminal_component";
// import { WebClient } from "@web/webclient/webclient";
// import { browser } from "@web/core/browser/browser";
// import { _t } from "@web/core/l10n/translation";

// // Register components
// registry.category("components").add("TerminalComponent", TerminalComponent);

// // Register the client action - using a factory function format that works across versions
// registry.category("actions").add("csv_import_form", function (env) {
//     return {
//         type: "ir.actions.client",
//         tag: "csv_import_form",
//         Component: ImportFormComponent,
//     };
// });

// // Add to import menu if it exists
// const menuRegistry = registry.category("web_import_actions");
// menuRegistry.add("csv_import", {
//     sequence: 10,
//     action: "compliance_management.action_csv_import",
//     name: _t("CSV Import"),
//     description: _t("Import CSV or Excel files"),
// });

// // Patch WebClient
// const originalStart = WebClient.prototype.start;
// WebClient.prototype.start = async function (...args) {
//     await originalStart.call(this, ...args);

//     browser.addEventListener("keydown", (event) => {
//         if (event.ctrlKey && event.shiftKey && event.code === "KeyI") {
//             event.preventDefault();
//             this.env.services.action.doAction("compliance_management.action_csv_import");
//         }
//     });
// };

// export { ImportFormComponent, TerminalComponent };

