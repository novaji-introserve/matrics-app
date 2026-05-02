/** @odoo-module **/

import { useService } from "@web/core/utils/hooks";
import { patch } from "@web/core/utils/patch";
import { FormController } from "@web/views/form/form_controller";

const { useEffect } = owl;
const CHANNEL = "dashboard_refresh_channel";

/**
 * Patches FormController so that res.partner (customer) form views
 * automatically reload their data when another session writes to the
 * same record, without requiring a manual page refresh.
 *
 * How it works:
 *  - customer.py write() already broadcasts to 'dashboard_refresh_channel'
 *    with the list of changed ids in the payload.
 *  - Here we subscribe to that same string channel and, when a notification
 *    arrives whose ids array contains the currently open record, call
 *    model.root.load() + model.notify() to pull fresh data from the server.
 */
patch(FormController.prototype, "compliance_management.CustomerFormRefresh", {
    setup() {
        this._super(...arguments);

        if (this.props.resModel !== "res.partner") {
            return;
        }

        const bus = useService("bus_service");

        useEffect(
            () => {
                bus.addChannel(CHANNEL);

                const handler = async (ev) => {
                    if (!this.props.resId) return;
                    for (const notification of ev.detail) {
                        const payload = notification.payload || {};
                        if (
                            payload.model === "res.partner" &&
                            Array.isArray(payload.ids) &&
                            payload.ids.includes(this.props.resId)
                        ) {
                            try {
                                await this.model.root.load();
                                this.model.notify();
                            } catch (_) {
                                // Component was unmounted before load completed
                            }
                            break;
                        }
                    }
                };

                bus.addEventListener("notification", handler);

                return () => {
                    bus.removeEventListener("notification", handler);
                    bus.deleteChannel(CHANNEL);
                };
            },
            () => [bus]
        );
    },
});
