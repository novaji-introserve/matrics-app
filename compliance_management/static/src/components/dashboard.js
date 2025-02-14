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
      datepicked: 7,
      chartData: []
    });

    onMounted(async () => {
    });
    
    onWillStart(async () => {
      await this.getcurrentuser();
      await this.filterByDate();
      
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
    await this.fetchChartData();

    
  };

  async fetchChartData() {
    const response = await this.rpc("/dashboard/get_scope_data", {
      cco: this.state.cco,
      branches_id: this.state.branches_id,
      datepicked: Number(this.state.datepicked)
    });

  
    
    this.state.chartData = Array.from(response)?.map((item) => item);
   
  }
}

ComplianceDashboard.template = "owl.ComplianceDashboard";
ComplianceDashboard.components = { Card, ChartRenderer };

registry.category("actions").add("owl.compliance_dashboard", ComplianceDashboard);
