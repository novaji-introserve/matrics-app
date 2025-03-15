/** @odoo-module **/
import { Component, useState } from "@odoo/owl";
import { registry } from "@web/core/registry";

class TestComponent extends Component {
    setup() {
        this.state = useState({
            message: "Test Action Loaded Successfully!"
        });
    }
}

TestComponent.template = 'compliance_management.TestAction';
TestComponent.components = {};

// Register the action
registry.category("actions").add("compliance_management.test_action", (env, action = {}) => {
    return {
        Component: TestComponent,
        props: { action },
    };
});
