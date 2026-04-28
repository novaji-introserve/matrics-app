/** @odoo-module **/

/**
 * URL Encryption Main Module
 * ==========================
 * Main entry point for the URL encryption module.
 * Registers the service and initializes early protection.
 */

import { registry } from "@web/core/registry";
import { URLEncryptionService } from "./url_encryption_service";
import { initializeEarlyProtection } from "./url_masking";

initializeEarlyProtection();

let urlEncryptionService = null;

registry.category("services").add("url_encryption", {
    dependencies: ["rpc"],
    start(env, { rpc }) {
        if (!urlEncryptionService) {
            urlEncryptionService = new URLEncryptionService(env);
        }
        return urlEncryptionService;
    },
});

export { urlEncryptionService };
