

/** @odoo-module */



import { registry } from "@web/core/registry";
import { Component, useRef, onWillStart, onMounted, useState } from "@odoo/owl";
import { KpiCard } from "./kpi_card/kpi_card";
import { ChartRenderer } from "./chart_renderer/chart_renderers";
import { loadJS, useService } from "@web/core/utils/hooks";

class CaseDashboard extends Component {
    setup() {
        // this.props = useState(this.props);
        // onMounted(() => this.renderChart());
        // onWillUpdateProps(() => this.renderChart());
        this.chartRef = useRef("chart");
        this.rpc = useService("rpc");
        this.action = useService("action"); //  For navigation
        this.dashboardData = useState({ kpi_data: {}, chart_data: {} });
        this.periodSelector = useRef("periodSelector");

        onWillStart(async () => {
            console.log("CaseDashboard is about to fetch data...");
            try {
                const data = await this.rpc("/case_dashboard/data");
                Object.assign(this.dashboardData, data); // Preserve reactivity
                //this.dashboardData = result;
                console.log("Dashboard data fetched:", this.dashboardData);
            } catch (error) {
                console.error("Error fetching dashboard data:", error);
                this.dashboardData.error = error.message || "Failed to load dashboard data.";
            }
        });

        onMounted(() => {
            console.log("CaseDashboard mounted with data:", JSON.stringify(this.dashboardData, null, 2));

            // Attach event listener to the period selector for handling period change
            // const periodSelectorElement = this.periodSelector.el; // Get reference to the DOM element
            // if (periodSelectorElement) {
            //     periodSelectorElement.addEventListener('change', this.onPeriodChange.bind(this));
            // }
        });
    }

    // Period change handler
    onPeriodChange(event) {
        const selectedPeriod = event.target.value; // Get the selected period value
        console.log("Period selected:", selectedPeriod);  // Check if the correct value is logged
        this.loadDashboardData(selectedPeriod);  // Reload the data for the selected period
    }

    // Load data based on the selected period
    async loadDashboardData(period) {
        try {
            const result = await this.rpc("/case_dashboard/data", { period });
            
            // Update the reactive state properly
            // Create a fresh copy of the data to ensure reactivity
            // Replace the entire dashboardData state object
            const freshData = {
                kpi_data: result.kpi_data,
                chart_data: result.chart_data
            };
            
            // Create a new state object to force a full refresh
            Object.assign(this.dashboardData, {});
            
            // Now assign the new data
            this.dashboardData.kpi_data = freshData.kpi_data;
            this.dashboardData.chart_data = freshData.chart_data;
            


            
            
            console.log("Success", result);
        } catch (error) {
            console.error("Error loading dashboard data:", error);
        }
    }



    






        goToView(status) {
            let action = {
                type: "ir.actions.act_window",
                res_model: "case",
                view_mode: "tree,form",
                views: [[false, "tree"], [false, "form"]],
            };
        
            if (status && status !== "all") {
                action.name = `${status.charAt(0).toUpperCase() + status.slice(1)} Cases`;
                action.domain = [["status_id.name", "=", status]];
            } else {
                action.name = "All Cases";
                // No domain — show all
            }
        
            this.action.doAction(action);
        }
        
        onChartClick(event, chartElements) {
            const element = chartElements[0];
            if (!element) return;
        
            // Determine which chart was clicked by checking the dataset label or other properties
            // For case_rate chart
            if (element._chart && element._chart.canvas.id.includes("case_rate")) {
                const label = this.dashboardData.chart_data.case_rate.labels[element.index];
                const status = label.toLowerCase();
                this.goToView(status);
            } 
            // For cases_by_severity chart
            else if (element._chart && element._chart.canvas.id.includes("severity")) {
                const severityLabels = ["Low", "Medium", "High"];
                const severity = severityLabels[element.index];
                this.goToSeverityView(severity);
            }
        }
        
        goToSeverityView(severity) {
            let action = {
                type: "ir.actions.act_window",
                res_model: "case",
                view_mode: "tree,form",
                views: [[false, "tree"], [false, "form"]],
                name: `${severity} Severity Cases`,
                domain: [["severity", "=", severity.toLowerCase()]]
            };
            
            this.action.doAction(action);
        }


        // onChartClick(event, chartElements) {
        //     const element = chartElements[0];
        //     if (!element) return;
        
        //     const label = this.dashboardData.chart_data.case_rate.labels[element.index];
        //     const status = label.toLowerCase();
        //     this.goToView(status);  // Always call goToView
        // }
        
        onCardClick(status) {
            this.goToView(status);  // Always call goToView
        }
        
    }

CaseDashboard.template = "owl.CaseDashboard";
CaseDashboard.components = { KpiCard, ChartRenderer };

registry.category("actions").add("owl.case_dashboard", CaseDashboard);
















// import { registry } from "@web/core/registry";
// import { Component, useRef, onWillStart, onMounted, useState } from "@odoo/owl";
// import { KpiCard } from "./kpi_card/kpi_card";
// import { ChartRenderer } from "./chart_renderer/chart_renderers";
// import { loadJS, useService } from "@web/core/utils/hooks";

// class CaseDashboard extends Component {
//     setup() {
//         this.chartRef = useRef("chart");
//         this.rpc = useService("rpc");
//         this.action = useService("action"); // 👈 For navigation
//         this.dashboardData = useState({ kpi_data: {}, chart_data: {} });
//         this.periodSelector = useRef("periodSelector");


//         onWillStart(async () => {
//             console.log("📡 CaseDashboard is about to fetch data...");
//             try {
//                 const data = await this.rpc("/case_dashboard/data");
//                 Object.assign(this.dashboardData, data); // Preserve reactivity
//                 console.log("✅ Dashboard data fetched:", this.dashboardData);
//             } catch (error) {
//                 console.error("❌ Error fetching dashboard data:", error);
//                 this.dashboardData.error = error.message || "Failed to load dashboard data.";
//             }
//         });

//         onMounted(() => {
//             console.log("📊 CaseDashboard mounted with data:", JSON.stringify(this.dashboardData, null, 2));
//         });
//     }

//     async loadDashboardData(period) {
//         try {
//             const result = await this.env.services.rpc({
//                 route: '/case_dashboard/data', // This should match the controller route
//                 params: { period },            // Send the period as parameter to filter data
//             });
    
//             // Handle the response (result) and update your dashboard state
//             this.dashboardData = result;  // Assuming this stores your dashboard data
//             this.render();  // Re-render the dashboard after data is loaded
//         } catch (error) {
//             console.error("Error loading dashboard data:", error);
//         }
//     }
    

//     goToView(status) {
//         if (status === "all") return;
//         this.action.doAction({
//             type: "ir.actions.act_window",
//             name: `${status.charAt(0).toUpperCase() + status.slice(1)} Cases`,
//             res_model: "case",
//             view_mode: "tree,form",
//             domain: [["status_id.name", "=", status]],
//             views: [[false, "tree"], [false, "form"]],
//         });
//     }

//     onChartClick(event, chartElements) {
//         const element = chartElements[0];
//         if (!element) return;

//         const label = this.dashboardData.chart_data.case_rate.labels[element.index];
//         const status = label.toLowerCase();
//         if (status !== "all") this.goToView(status);
//     }

//     onCardClick(status) {
//         if (status !== "all") this.goToView(status);
//     }
// }

// CaseDashboard.template = "owl.CaseDashboard";
// CaseDashboard.components = { KpiCard, ChartRenderer };

// registry.category("actions").add("owl.case_dashboard", CaseDashboard);


// // import { registry } from "@web/core/registry";
// // import { Component, useRef, onWillStart, onMounted, useState } from "@odoo/owl";
// // import { KpiCard } from "./kpi_card/kpi_card";
// // import { ChartRenderer } from "./chart_renderer/chart_renderers";
// // import { loadJS, useService } from "@web/core/utils/hooks"; // Import useService

// // class CaseDashboard extends Component {
// //     setup() {
// //         this.chartRef = useRef("chart");
// //         this.rpc = useService("rpc");
// //         this.dashboardData = useState({ kpi_data: {}, chart_data: {} });

// //         onWillStart(async () => {
// //             console.log("📡 CaseDashboard is about to fetch data...");
// //             try {
// //                 const data = await this.rpc("/case_dashboard/data");
// //                 Object.assign(this.dashboardData, data);  //  Preserve reactivity
// //                 console.log(" Dashboard data fetched:", this.dashboardData);
// //             } catch (error) {
// //                 console.error(" Error fetching dashboard data:", error);
// //                 this.dashboardData.error = error.message || "Failed to load dashboard data.";
// //             }
// //         });

// //         onMounted(() => {
// //             //  Log the final dashboardData to verify structure
// //             console.log("CaseDashboard mounted with data:", JSON.stringify(this.dashboardData, null, 2));
// //         });
// //     }
// // }

// // CaseDashboard.template = "owl.CaseDashboard";
// // CaseDashboard.components = { KpiCard, ChartRenderer };

// // registry.category("actions").add("owl.case_dashboard", CaseDashboard);





