// /** @odoo-module **/
// import { registry } from "@web/core/registry";
// import { ImportFormComponent } from "./import_form_component";
// import { TerminalComponent } from "./terminal_component";

// // Register the TerminalComponent
// registry.category("components").add("TerminalComponent", TerminalComponent);

// // Register the client action with the EXACT format expected by Odoo
// registry.category("actions").add("compliance_management.csv_import_form", async function (env, action) {
//     console.log("CSV import client action called with action:", action);

//     // This exact structure is required by Odoo's ActionManager
//     return {
//         component: ImportFormComponent,  // Note the lowercase 'c' in component
//         props: { action },
//         target: action.target || 'current',
//     };
// });

// export { ImportFormComponent, TerminalComponent };


/** @odoo-module **/

import { registry } from '@web/core/registry';
import { ImportFormComponent } from './import_form_component';

// Debug logging to verify loading
console.log("REGISTERING CSV IMPORT CLIENT ACTION");

// Register the client action directly - this is the CRITICAL fix
registry.category("actions").add("compliance_management.csv_import_form", ImportFormComponent);

console.log("CSV IMPORT CLIENT ACTION REGISTERED SUCCESSFULLY");


/** @odoo-module **/

// import { registry } from '@web/core/registry';
// import { ImportFormComponent } from './import_form_component';
// import { TerminalComponent } from './terminal_component';
// import { chunkedUploader } from './chunked_uploader';
// import { terminalService } from './terminal';

// // Register services first
// const serviceRegistry = registry.category("services");
// serviceRegistry.add("terminal", terminalService);
// serviceRegistry.add("chunkedUploader", chunkedUploader);

// // Then register the client action - make sure the tag exactly matches the XML
// registry.category("actions").add("compliance_management.csv_import_form", ImportFormComponent);

// // Register additional components
// registry.category("components").add("TerminalComponent", TerminalComponent);

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
//         component: ImportFormComponent,
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

