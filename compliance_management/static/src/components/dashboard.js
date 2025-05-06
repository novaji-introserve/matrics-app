/** @odoo-module */

import { registry } from "@web/core/registry";
import { useService } from "@web/core/utils/hooks";
import { Card } from "./card/card";
import { ChartRenderer } from "./chart";
const { Component, useState, useEffect, useRef, onMounted, onWillStart } = owl;


export function useBusListener(channelName, callback) {
  const bus = useService("bus_service");
  
  useEffect(
    () => {
      // Add the channel we want to listen to
      bus.addChannel(channelName);
      
      // Define the handler function
      const handler = (ev) => {
        const notifications = ev.detail;
        for (const notification of notifications) {
          
          // Check if this is a 3-part notification with the channel we care about
          if (Array.isArray(notification) && notification[0] === channelName) {
            // Pass the message (third parameter) to the callback
            callback(notification[2]);

          }else if(!Array.isArray(notification) && notification.payload && notification.payload.channelName === channelName){
            callback(notification.payload);
          }
        }
      };
    
      // Add the event listener
      bus.addEventListener('notification', handler);

     
      // Cleanup function
      return () => {
        bus.removeEventListener('notification', handler);
        bus.deleteChannel(channelName);
      };
    },
    () => [bus, channelName, callback]
  );
}


export class ComplianceDashboard extends Component {
  setup() {
    this.left_indicator = useRef("left");
    this.api = useService("orm");
    this.rpc = useService("rpc");
    this.navigate = useService("action");
    this.state = useState({
      isCategorySortingEnabled: false,
      cco: false,
      branches_id: [],
      stats: [],
      totalstat: 0,
      datepicked: 20000,
      chartData: [],
      scrollLeft: sessionStorage.getItem("user_scroll_left")
        ? sessionStorage.getItem("user_scroll_left")
        : true,
      scrollRight: sessionStorage.getItem("user_scroll_right")
        ? sessionStorage.getItem("user_scroll_right")
        : false,
      screenedchart: [],
      highriskchart: [],
      topbranch: [],
      dynamic_chart: []
    });

    // Models that should trigger a refresh
    this.refreshModels = ['res.partner', 'res.branch'];

     // Setup bus listener for refreshing the dashboard
    useBusListener('dashboard_refresh_channel', this.handleRefreshNotification.bind(this));
    
    


    useEffect(() => {
      
        let cardContainer = document.querySelector(".card-container");
        if(cardContainer){

          cardContainer.addEventListener("scroll",this._onHorizontalScroll.bind(this));
        }
         return () => {
           if (cardContainer) {
             cardContainer.removeEventListener(
               "scroll",
               this._onHorizontalScroll
             );
           }
         };

      },
      () => []
    );

    onMounted( async() =>(
      await this.getcurrentuser().then(async() => await this.filterByDate())
    ));

    this.displayOdooView = this.displayOdooView.bind(this); // Bind the function!
    
  }

    /**
     * Handle notifications from the bus
     * @param {Object} notification - The notification payload
     */
    async handleRefreshNotification(notification) {
      console.log("Received notification:", notification);
  
      if (notification.type === 'refresh' && this.refreshModels.includes(notification.model)) {
          // Reload the view
          await this.filterByDate();
          this.render();
      }
  }

  _onHorizontalScroll = () => {
    const container = document.querySelector(".card-container");

    if (!container) {
      return; // Container might not be available yet
    }

    const atRight =
      container.scrollLeft + container.clientWidth >= container.scrollWidth - 5; // -5 buffer

    const atLeft = container.scrollLeft <= 5; // Left end

    if (atRight && !sessionStorage.getItem("user_scroll_left")) {
      this.state.scrollRight = true;
      this.state.scrollLeft = false;
      sessionStorage.setItem("user_scroll_right", true)
    }else{
      this.state.scrollLeft = true;
    } 
    
    if(atLeft){
      this.state.scrollLeft = true;
      sessionStorage.setItem("user_scroll_left", true);
    }
  };

  async displayOdooView(category, query, branch_filter, branch_field, title) {
    
    const response = await this.rpc("/dashboard/dynamic_sql", { sql_query: query, branches_id: this.state.branches_id, cco: this.state.cco });          
        
    if(!response) return;

    // Create a properly formatted title in sentence case
    const displayTitle = title || (category ? 
      category.charAt(0).toUpperCase() + category.slice(1).toLowerCase() : 
      "Card Results");

    console.log(response.domain);
    
    this.navigate.doAction({
      type: "ir.actions.act_window",
      res_model: response.table.replace(/_/g, "."),
      name: displayTitle,
      // name: `${category[0].toUpperCase()}${category.slice(1,)}`,
      domain: response.domain,
      views: [
        [false, "tree"],
        [false, "form"],
      ],
    });

    
  }
  async getcurrentuser() {
    let result = await this.rpc("/dashboard/user");

    console.log(result);
    
    
    this.state.branches_id = result.branch;
    this.state.cco = result.group;
  }
  // async getAllStats() {
  //   let result = await this.rpc(`/dashboard/stats`, {
  //     cco: this.state.cco,
  //     branches_id: this.state.branches_id,
  //     datepicked: Number(this.state.datepicked),
  //   });
    
  //   this.state.stats = [...result.data];
  //   this.state.totalstat = result.total;
  // }

  async getAllStats() {
    // Create a unique cache key based on the request parameters
    const cacheKey = `stats_${this.state.cco}_${this.state.branches_id.join(',')}_${this.state.datepicked}`;
    
    try {
      // Check if we have this data cached and it's not expired
      const cachedData = this.getFromCache(cacheKey);
      if (cachedData) {
        console.log('Using cached dashboard stats data');
        this.state.stats = [...cachedData.data];
        this.state.totalstat = cachedData.total;
        return;
      }
      
      // If no valid cache, make the RPC call
      console.log('Fetching fresh dashboard stats data');
      let result = await this.rpc(`/dashboard/stats`, {
        cco: this.state.cco,
        branches_id: this.state.branches_id,
        datepicked: Number(this.state.datepicked),
      });
      
      // Update state with the new data
      this.state.stats = [...result.data];
      this.state.totalstat = result.total;
      
      // Store the result in cache (default 5 minutes)
      this.saveToCache(cacheKey, result);
    } catch (error) {
      console.error('Error fetching dashboard stats:', error);
      // Optionally show user-friendly error message
    }
  }
  
  /**
   * Retrieves data from the cache if available and not expired
   * @param {string} key - The cache key
   * @param {number} [maxAge=300000] - Maximum age in milliseconds (default 5 minutes)
   * @returns {object|null} - The cached data or null if not found/expired
   */
  getFromCache(key, maxAge = 5 * 60 * 1000) {
    try {
      // Try to get cached item
      const cachedItem = localStorage.getItem(key);
      if (!cachedItem) return null;
      
      // Parse the stored data
      const { data, timestamp } = JSON.parse(cachedItem);
      
      // Check if the cached data is still valid
      const now = new Date().getTime();
      if (now - timestamp < maxAge) {
        return data;
      } else {
        // Clear expired cache
        localStorage.removeItem(key);
        return null;
      }
    } catch (e) {
      console.error('Error retrieving cached data:', e);
      // Clean up potentially corrupted cache item
      localStorage.removeItem(key);
      return null;
    }
  }
  
  /**
   * Saves data to cache with expiration
   * @param {string} key - The cache key
   * @param {object} data - The data to cache
   * @param {number} [expiresIn=300000] - Expiration time in milliseconds (default 5 minutes)
   */
  saveToCache(key, data, expiresIn = 60 * 60 * 1000) {
    const cacheItem = {
      data,
      timestamp: new Date().getTime()
    };
    
    try {
      localStorage.setItem(key, JSON.stringify(cacheItem));
    } catch (e) {
      console.error('Error saving to cache:', e);
      
      // Check if it's a quota exceeded error
      if (e.name === 'QuotaExceededError' || e.code === 22) {
        this.clearOldCache();
        
        // Try again after clearing some space
        try {
          localStorage.setItem(key, JSON.stringify(cacheItem));
        } catch (retryError) {
          console.error('Still unable to save to cache after cleanup:', retryError);
        }
      }
    }
  }
  
  /**
   * Clears oldest cached items when storage limit is reached
   * @param {number} [percentage=20] - Percentage of oldest items to remove
   */
  clearOldCache(percentage = 20) {
    console.log('Clearing old cache entries to free up space');
    
    // Find all cache keys and their timestamps
    const cacheEntries = [];
    for (let i = 0; i < localStorage.length; i++) {
      const key = localStorage.key(i);
      if (key.startsWith('stats_')) {
        try {
          const item = JSON.parse(localStorage.getItem(key));
          cacheEntries.push({ key, timestamp: item.timestamp });
        } catch (e) {
          // Remove invalid entries
          localStorage.removeItem(key);
        }
      }
    }
    
    // Sort by timestamp (oldest first)
    cacheEntries.sort((a, b) => a.timestamp - b.timestamp);
    
    // Remove the oldest percentage of items
    const toRemove = Math.max(1, Math.floor(cacheEntries.length * (percentage / 100)));
    console.log(`Removing ${toRemove} oldest cache entries`);
    
    for (let i = 0; i < toRemove; i++) {
      if (cacheEntries[i]) {
        localStorage.removeItem(cacheEntries[i].key);
      }
    }
  }
  
  /**
   * Clears all cached  data
   */
  clearAllStatsCache() {
    for (let i = 0; i < localStorage.length; i++) {
      const key = localStorage.key(i);
      if (key.startsWith('stats_')) {
        localStorage.removeItem(key);
      }
    }
    console.log('All stats cache cleared');
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
    // await this.fetchScreenedChart();
    // await this.TopBranches();
    // await this.highRiskBranches();
    await this.fetchDashboardCharts()
  };


  // async fetchScreenedChart() {
  //   const response = await this.rpc("/dashboard/get_top_screened_rules", {
  //     cco: this.state.cco,
  //     branches_id: this.state.branches_id,
  //     datepicked: Number(this.state.datepicked),
  //   });  

   
  //   this.state.screenedchart = response
    

  // }
  // async TopBranches() {
  //   const response = await this.rpc("/dashboard/get_branch_by_customer", {
  //     cco: this.state.cco,
  //     branches_id: this.state.branches_id,
  //     datepicked: Number(this.state.datepicked),
  //   });  


    

  //   this.state.topbranch = response

  // }
  // async highRiskBranches() {
  //   const response = await this.rpc("/dashboard/get_high_risk_customer_by_branch",
  //     {
  //       cco: this.state.cco,
  //       branches_id: this.state.branches_id,
  //       datepicked: Number(this.state.datepicked),
  //     }
  //   );  
    
    
  //   this.state.highriskchart = response
    
  // }
  
  // async fetchDashboardCharts(){
    
  //   const response = await this.rpc(`/dashboard/dynamic_charts/`,
  //     {
  //       cco: this.state.cco,
  //       branches_id: this.state.branches_id,
  //       datepicked: Number(this.state.datepicked),
  //     }
  //   );  
    

  //   if (response && response.error) {
  //     alert(`Error fetching dashboard charts: ${response.error}`);
  //     this.state.dynamic_chart = []; // Or some other appropriate error state
  //   } else if (response) {
  //     this.state.dynamic_chart = response;
  //   } else {
  //     alert("Error: Empty response received while fetching dashboard charts.");
  //     this.state.dynamic_chart = [];
  //   }
    
      
  //   }
  async fetchDashboardCharts() {
    // Create a unique cache key based on the request parameters
    const cacheKey = `charts_${this.state.cco}_${this.state.branches_id.join(',')}_${this.state.datepicked}`;
    
    try {
      // Check if we have this data cached and it's not expired (1 hour cache)
      const oneHour = 60 * 60 * 1000; // 1 hour in milliseconds
      const cachedData = this.getFromCache(cacheKey, oneHour);
      
      if (cachedData) {
        console.log('Using cached dashboard charts data');
        this.state.dynamic_chart = cachedData;
        return;
      }
      
      // If no valid cache, make the RPC call
      console.log('Fetching fresh dashboard charts data');
      const response = await this.rpc(`/dashboard/dynamic_charts/`, {
        cco: this.state.cco,
        branches_id: this.state.branches_id,
        datepicked: Number(this.state.datepicked),
      });
      
      // Handle the response appropriately
      if (response && response.error) {
        alert(`Error fetching dashboard charts: ${response.error}`);
        this.state.dynamic_chart = []; // Or some other appropriate error state
      } else if (response) {
        this.state.dynamic_chart = response;
        
        // Store the successful result in cache with 1 hour expiration
        this.saveToCache(cacheKey, response, oneHour);
      } else {
        alert("Error: Empty response received while fetching dashboard charts.");
        this.state.dynamic_chart = [];
      }
    } catch (error) {
      console.error('Error in fetchDashboardCharts:', error);
      alert("An unexpected error occurred while fetching dashboard charts.");
      this.state.dynamic_chart = [];
    }
  }
  }


ComplianceDashboard.template = "owl.ComplianceDashboard";
ComplianceDashboard.components = { Card, ChartRenderer };

registry.category("actions").add("owl.compliance_dashboard", ComplianceDashboard);
