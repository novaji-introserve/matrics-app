/** @odoo-module */

import { registry } from "@web/core/registry";
import { Dashboard } from "./dashboard";

registry.category("actions").add("owl.icomply_dashboard", Dashboard, { force: true });





