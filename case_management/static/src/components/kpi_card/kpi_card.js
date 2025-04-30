/** @odoo-module */

import { registry } from "@web/core/registry";
import { Component } from "@odoo/owl";

class KpiCard extends Component {
    handleClick() {
        if (this.props.onClick) {
            this.props.onClick(this.props.status);
        }
    }
}

KpiCard.template = "owl.KpiCard";

//  Register KpiCard in the components registry
registry.category("components").add("KpiCard", KpiCard);

export { KpiCard };  //  Make sure it's exported









// /** @odoo-module */

// import { registry } from "@web/core/registry";
// import { Component, xml } from "@odoo/owl";


// class KpiCard extends Component {};

// KpiCard.template = "owl.KpiCard"