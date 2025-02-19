/** @odoo-module */

import { registry } from "@web/core/registry";
import { useService } from "@web/core/utils/hooks";
import { Card } from "./card/card";
import { ChartRenderer } from "./chart";
const { Component, useState, useEffect, useRef, onMounted, onWillStart } = owl;


export class ComplianceDashboard extends Component {
  setup() {
    this.api = useService("orm");
    this.rpc = useService("rpc");
    this.navigate = useService("action");
    this.state = useState({
      isCategorySortingEnabled: false,
      cco: false,
      branches_id: [],
      stats: [],
      totalstat: 0,
      datepicked: 0,
      chartData: [],
      scrollLeft: true,
      scroll: false,
    });


    useEffect(
      () => {
      
        let cardContainer = document.querySelector(".card-container");
        cardContainer.addEventListener(
          "scroll",
          this._onHorizontalScroll.bind(this)
        );

      },
      () => []
    );

    onWillStart(async () => {
      await this.getcurrentuser();
      await this.filterByDate();
    });

    this.displayOdooView = this.displayOdooView.bind(this); // Bind the function!
  }

  _onHorizontalScroll = () => {
    const container = document.querySelector(".card-container");

    if (!container) {
      return; // Container might not be available yet
    }

    const atRight =
      container.scrollLeft + container.clientWidth >= container.scrollWidth - 5; // -5 buffer

    const atLeft = container.scrollLeft <= 5; // Left end

    if (atRight) {
      this.state.scroll = true;
      this.state.scrollLeft = false;
      
      // Your logic here (e.g., load more data)
    } 
    
    if(atLeft){
      this.state.scrollLeft = true;
      this.state.scroll = false;
     
    }
  };

  async displayOdooView(category) {
    const formatDate = (date) => date.toISOString().slice(0, 10);

    let prevDate, currentDate;

    if (this.state.datepicked > 0) {
      prevDate = moment()
        .subtract(this.state.datepicked, "days")
        .format("YYYY-MM-DD");
      currentDate = formatDate(new Date()); // Today's date
    } else {
      currentDate = formatDate(new Date()); // Today's date
      prevDate = currentDate; // Same as today if datepicked is 0
    }

    const odooPrevDate = `${prevDate} 00:00:00`; // For Odoo's datetime field
    const odooCurrentDate = `${currentDate} 23:59:59`; // For Odoo's datetime field

    this.state.prevDate = odooPrevDate; // Update the state
    this.state.currentDate = odooCurrentDate; // Update the state

    let domain = [
      ["create_date", ">=", odooPrevDate], // Use the formatted dates
      ["create_date", "<=", odooCurrentDate], // Use <= for inclusive end date
      ["scope", "=", category],
    ];

    this.navigate.doAction({
      type: "ir.actions.act_window",
      res_model: "res.compliance.stat",
      name: category,
      domain: domain,
      views: [
        [false, "tree"],
        [false, "form"],
      ],
    });
  }
  async getcurrentuser() {
    let result = await this.rpc("/dashboard/user");
    this.state.branches_id = result.branch;
    this.state.cco = result.group;
  }
  async getAllStats() {
    let result = await this.rpc(`/dashboard/stats`, {
      cco: this.state.cco,
      branches_id: this.state.branches_id,
      datepicked: Number(this.state.datepicked),
    });

    this.state.stats = result.data;
    this.state.totalstat = result.total;
  }
  async getAllStatsByCategory(name) {
    let result = await this.rpc(`/dashboard/statsbycategory`, {
      cco: this.state.cco,
      branches_id: this.state.branches_id,
      category: name,
      datepicked: Number(this.state.datepicked),
    });

    this.state.stats = result.data;
    this.state.totalstat = result.total;
  }

  displaybycategory = async (name) => {
    this.state.isCategorySortingEnabled = false;

    if (name == "all") {
      await this.getAllStats();
      this.state.isCategorySortingEnabled = false;
    } else {
      this.state.isCategorySortingEnabled = true;
      await this.getAllStatsByCategory(name);
    }
  };

  filterByDate = async () => {
    await this.getAllStats();
    // await this.fetchChartData();

    await this.fetchTransChart();
  };

  // async fetchChartData() {
  //   const response = await this.rpc("/dashboard/get_scope_data", {
  //     cco: this.state.cco,
  //     branches_id: this.state.branches_id,
  //     datepicked: Number(this.state.datepicked),
  //   });

  //   this.state.chartData = Array.from(response)?.map((item) => item);
  // }

  async fetchTransChart() {
    const response = await this.rpc("/dashboard/get_transaction", {
      cco: this.state.cco,
      branches_id: this.state.branches_id,
      datepicked: Number(this.state.datepicked),
    });  

    this.state.transactionchart = response
  }
}

ComplianceDashboard.template = "owl.ComplianceDashboard";
ComplianceDashboard.components = { Card, ChartRenderer };

registry.category("actions").add("owl.compliance_dashboard", ComplianceDashboard);
