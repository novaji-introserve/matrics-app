/** @odoo-module **/

import { registry } from '@web/core/registry';
const { Component, useState, onWillStart, useRef } = owl;
import { useService } from "@web/core/utils/hooks";

export class Dashboard extends Component {

    setup() {
        this.state = useState({
            escalationDueDates: [],
            internalDueDates: [],
            regulatoryDueDates: [],
            isLoading: true,
            totalRulebooks :0,
            newRulebooksToday: 0,
            totalTitle:0,
            newTitleToday:0,
            totalThemes :0,
            newThemesToday :0,
            totalReplies :0,
            newRepliesToday :0,
            totalSources :0,
            newSourcesToday :0,
            totalChatLogs : 0,
            newChatLogsToday: 0,
            awaitingReplies: [],
        });

        // Initialize the orm service
        this.orm = useService("orm");

        onWillStart(async () => {
            await this.fetchCounts();
            await this.fetchData();
            await this.fetchAwaitingReplies()
        });
    }

    async fetchAwaitingReplies() {
        try {
            // Using the ORM to fetch awaiting replies
            const replies = await this.orm.call('reply.log', 'get_awaiting_replies', []);
            this.state.awaitingReplies = replies;
            console.log(replies); // Print the replies
        } catch (error) {
            console.error('Failed to fetch awaiting replies:', error);
        }
    }
    async fetchCounts() {
        // Fetch total rulebooks
        this.state.totalRulebooks = await this._fetchCount('rulebook');
        
        // Fetch new rulebooks created today
        this.state.newRulebooksToday = await this._fetchCount('rulebook', {
            created_on: '>=',
            today: new Date().toISOString().split('T')[0] + ' 00:00:00', // Start of today
        });
        this.state.totalTitle = await this._fetchCount('rulebook.title');

        // Fetch new rulebooks created today
        this.state.newTitleToday = await this._fetchCount('rulebook.title', {
            created_on: '>=',
            today: new Date().toISOString().split('T')[0] + ' 00:00:00', // Start of today
        });

        // Fetch total themes
        this.state.totalThemes = await this._fetchCount('rulebook.theme');

        // Fetch new themes created today
        this.state.newThemesToday = await this._fetchCount('rulebook.theme', {
            created_on: '>=',
            today: new Date().toISOString().split('T')[0] + ' 00:00:00',
        });

        // Fetch total replies
        this.state.totalReplies = await this._fetchCount('reply.log');

        // Fetch new replies created today
        this.state.newRepliesToday = await this._fetchCount('reply.log', {
            created_on: '>=',
            today: new Date().toISOString().split('T')[0] + ' 00:00:00',
        });

        // Fetch total sources
        this.state.totalSources = await this._fetchCount('rulebook.sources');

        // Fetch new sources created today
        this.state.newSourcesToday = await this._fetchCount('rulebook.sources', {
            created_on: '>=',
            today: new Date().toISOString().split('T')[0] + ' 00:00:00',
        });

        // Fetch total chat logs
        this.state.totalChatLogs = await this._fetchCount('pdf.chat.log');

        // Fetch new chat logs created today
        this.state.newChatLogsToday = await this._fetchCount('pdf.chat.log', {
            created_on: '>=',
            today: new Date().toISOString().split('T')[0] + ' 00:00:00',
        });

        // Update the component state or trigger a re-render
        this.render();
    }

    async _fetchCount(modelName, filters = {}) {
        const domain = [];

        // Check if filters have a valid created_on property
        if (filters.created_on && filters.today) {
            domain.push(["create_date", '>=', filters.today]);
        }

        try {
            const count = await this.orm.searchCount(
                modelName,domain
            );
         
            return count;
        } catch (error) {
            console.error(`Error fetching count for ${modelName}:`, error);
            return 0; // Return 0 or handle the error as appropriate
        }
    }


    async fetchData() {
        try {
            // Fetch first 10 Escalation Due Dates
            const escalationDueDates = await this.orm.searchRead('rulebook',
                [['escalation_date', '!=', false],["status",'=',"active"]],
                ['id', 'type_of_return', 'escalation_date', 'responsible_id'],
                { limit: 5, order: 'escalation_date asc' }
            );

            // Fetch first 10 Internal Due Dates
            const internalDueDates = await this.orm.searchRead('rulebook',
                [['internal_due_date', '!=', false]],
                ['id', 'type_of_return', 'internal_due_date', 'responsible_id'],
                { limit: 5, order: 'internal_due_date asc' }
           );

            // Fetch first 10 Regulatory Due Dates (computed_date)
            const regulatoryDueDates = await this.orm.searchRead('rulebook',
                [['computed_date', '!=', false]],
                ['id', 'type_of_return', 'computed_date', 'responsible_id'],
                { limit: 5, order: 'computed_date asc' }
                );
            
          
            this.state.escalationDueDates = escalationDueDates.map(date => ({
                ...date,
                escalation_date: new Date(date.escalation_date).toLocaleDateString('en-US', {
                    year: 'numeric',
                    month: 'long',
                    day: 'numeric'
                }),
                link: this.getRulebookLink(date.id),
                type_of_return: date.type_of_return.replace(/<[^>]*>/g, '')
            }));
            this.state.internalDueDates = internalDueDates.map(date => ({
                ...date,
                internal_due_date: new Date(date.internal_due_date).toLocaleDateString('en-US', {
                    year: 'numeric',
                    month: 'long',
                    day: 'numeric'
                }),
                link: this.getRulebookLink(date.id),
                type_of_return: date.type_of_return.replace(/<[^>]*>/g, '')
            }));
            this.state.regulatoryDueDates = regulatoryDueDates.map(date => ({
                ...date,
                computed_date: new Date(date.computed_date).toLocaleDateString('en-US', {
                    year: 'numeric',
                    month: 'long',
                    day: 'numeric'
                }),
                link: this.getRulebookLink(date.id),
                type_of_return: date.type_of_return.replace(/<[^>]*>/g, '')
            }));
        } catch (error) {
            console.error('Error fetching escalation due dates:', error);
            this.state.error = 'Failed to fetch due dates.';
        } finally {
            this.state.isLoading = false;
        }
    }

    // formatDate(dateStr) {
    //     if (!dateStr) return '';
    //     const date = new Date(dateStr);
    //     return date.toLocaleDateString();
    // }

    getRulebookLink(id) {
        // Constructs a URL to the rulebook's form view
        return `/web#id=${id}&model=rulebook&view_type=form`;
    }

}

Dashboard.template = 'rule_book.Dashboard'
registry.category('actions').add('rule_book.action_cusotomer_details_js', Dashboard);