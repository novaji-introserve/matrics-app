/** @odoo-module **/

import { registry } from '@web/core/registry';
const { Component, useState, onWillStart, useRef } = owl;
import { useService } from "@web/core/utils/hooks";


export class Dashboard extends Component {

    setup() {
        this.state = useState({
            ippis_number: '',
            name: '',
            civil_servant: {},
            loan_details: {},
            repayment_schedule: [],
            error: null,
            dsr_value: 0,
            eligibile_amount:0,
            search_field: false
        });

        onWillStart(async () => {
            console.log('Testing')
        })
        this.searchInput = useRef("search-input")
        this.orm = useService("orm")
    }

    
}

Dashboard.template = 'rule_book.Dashboard'
registry.category('actions').add('rule_book.action_cusotomer_details_js', Dashboard);