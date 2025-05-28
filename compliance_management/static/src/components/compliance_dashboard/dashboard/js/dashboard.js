/** @odoo-module */

import { registry } from "@web/core/registry";
import { useService } from "@web/core/utils/hooks";
import { Card } from "../../card/js/card";
import { ChartRenderer } from "../../chart/js/chart";
const { Component, useState, useEffect, useRef, onWillStart, onWillUnmount } = owl;

// Debug mode - set to false for production
const DEBUG = true;
function logDebug(...args) {
  if (DEBUG) console.log(...args);
}

/**
 * Custom hook for bus service notifications
 * @param {string} channelName - Channel to listen on
 * @param {Function} callback - Callback on notification
 */
export function useBusListener(channelName, callback) {
  const bus = useService("bus_service");
  
  useEffect(
    () => {
      bus.addChannel(channelName);
      
      const handler = (ev) => {
        const notifications = ev.detail;
        for (const notification of notifications) {
          if (Array.isArray(notification) && notification[0] === channelName) {
            callback(notification[2]);
          } else if (!Array.isArray(notification) && notification.payload && notification.payload.channelName === channelName) {
            callback(notification.payload);
          }
        }
      };
    
      bus.addEventListener('notification', handler);
     
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
    this.serverCache = useService("server_cache");
    this.user = useService("user");
    
    // Initialize state with proper defaults
    this.state = useState({
      isCategorySortingEnabled: false,
      cco: null, // Set to null initially, will be set after getCurrentUser()
      branches_id: [],
      stats: [],
      totalstat: 0,
      datepicked: 20000,
      dynamic_chart: [],
      scrollLeft: true,
      scrollRight: false,
      uniqueId: null,
      loadingStates: {
        stats: true,
        charts: true
      }
    });
    
    // Models that should trigger a refresh
    this.refreshModels = ['res.partner', 'res.branch'];

    // Setup bus listener for refreshing the dashboard
    useBusListener('dashboard_refresh_channel', this.handleRefreshNotification.bind(this));
    
    // Initialize component with progressive loading
    onWillStart(async () => {
      try {
        this._hideGlobalLoadingIndicator();
        await this.getCurrentUser();
        this._loadDataProgressively();
      } catch (error) {
        console.error("Error in component initialization:", error);
        this._clearLoadingStates();
      }
    });
    
    // Set up scroll event listener
    useEffect(() => {
      let cardContainer = document.querySelector(".card-container");
      if (cardContainer) {
        cardContainer.addEventListener("scroll", this._onHorizontalScroll.bind(this));
      }
      return () => {
        if (cardContainer) {
          cardContainer.removeEventListener("scroll", this._onHorizontalScroll);
        }
      };
    }, () => []);
    
    // Bind methods
    this.displayOdooView = this.displayOdooView.bind(this);
    this.displaybycategory = this.displaybycategory.bind(this);
    this.filterByDate = this.filterByDate.bind(this);
    this._onHorizontalScroll = this._onHorizontalScroll.bind(this);
  }

  /**
   * Clear all loading states
   */
  _clearLoadingStates() {
    this.state.loadingStates.stats = false;
    this.state.loadingStates.charts = false;
  }

  /**
   * Generate consistent cache key for stats - matching backend format exactly
   */
  _generateStatsCacheKey() {
    const cco = String(this.state.cco || false).toLowerCase();
    const branches = JSON.stringify(this.state.branches_id || []);
    const datepicked = String(this.state.datepicked || 20000);
    const uniqueId = String(this.state.uniqueId || '');
    
    // Match backend format exactly: all_stats_{cco}_{branches}_{datepicked}_{unique_id}
    return `all_stats_${cco}_${branches}_${datepicked}_${uniqueId}`;
  }

  /**
   * Generate consistent cache key for charts - matching backend format exactly
   */
  _generateChartsCacheKey() {
    const cco = String(this.state.cco || false).toLowerCase();
    const branches = JSON.stringify(this.state.branches_id || []);
    const datepicked = String(this.state.datepicked || 20000);
    const uniqueId = String(this.state.uniqueId || '');
    
    // Match backend format exactly: charts_data_{cco}_{branches}_{datepicked}_{unique_id}
    return `charts_data_${cco}_${branches}_${datepicked}_${uniqueId}`;
  }

  /**
   * Generate consistent cache key for category stats - matching backend format
   */
  _generateCategoryStatsCacheKey(category) {
    const cco = String(this.state.cco || false).toLowerCase();
    const branches = JSON.stringify(this.state.branches_id || []);
    const datepicked = String(this.state.datepicked || 20000);
    const uniqueId = String(this.state.uniqueId || '');
    
    // Match backend format: stats_category_{cco}_{branches}_{category}_{datepicked}_{unique_id}
    return `stats_category_${cco}_${branches}_${category}_${datepicked}_${uniqueId}`;
  }

  /**
   * Handle notifications from the bus
   */
  async handleRefreshNotification(notification) {
    logDebug("Received notification:", notification);

    if (notification.type === 'refresh' && this.refreshModels.includes(notification.model)) {
      await this.serverCache.invalidateCache();
      
      try {
        await this.filterByDate(true);
      } catch (error) {
        console.error("Error refreshing dashboard:", error);
      }
    }
  }

  /**
   * Hide Odoo's global loading spinner
   */
  _hideGlobalLoadingIndicator() {
    const loader = document.querySelector('.o_loading');
    if (loader) {
      loader.style.display = 'none';
    }
  }

  /**
   * Load data progressively - stats first, then charts
   */
  _loadDataProgressively() {
    // Only start loading if we have user info (CCO value is set)
    if (this.state.cco === null) {
      logDebug('Waiting for user info before loading data');
      return;
    }
    
    // Load stats first
    this.getAllStats().finally(() => {
      // After stats are loaded, load charts with a small delay
      setTimeout(() => {
        this.fetchDashboardCharts().catch(error => {
          console.error("Error loading charts:", error);
        });
      }, 100);
    });
  }

  /**
   * Handle horizontal scroll events
   */
  _onHorizontalScroll(event) {
    const container = event.target || event.currentTarget;
    if (!container) return;

    const atRight = container.scrollLeft + container.clientWidth >= container.scrollWidth - 5;
    const atLeft = container.scrollLeft <= 5;
    
    if (atRight) {
      this.state.scrollRight = true;
      this.state.scrollLeft = false;
    } else if (atLeft) {
      this.state.scrollLeft = true;
      this.state.scrollRight = false;
    } else {
      this.state.scrollLeft = false;
      this.state.scrollRight = false;
    }
  }

  /**
   * Get current user info and clean up stale cache entries
   */
  async getCurrentUser() {
    try {
      logDebug('Fetching current user');
      const result = await this.rpc("/dashboard/user");
      if (result) {
        logDebug('Got user data:', result);
        this.state.branches_id = result.branch || [];
        
        // Match backend logic: CCO = true if user is CCO OR CO
        // Backend does: if is_co or is_cco: cco = True
        const newCcoValue = result.is_cco || result.is_co || false;
        const oldCcoValue = this.state.cco;
        
        this.state.cco = newCcoValue;
        this.state.uniqueId = result.unique_id || null;
        
        // Debug CCO value to understand cache key inconsistencies
        logDebug(`User roles - is_cco: ${result.is_cco}, is_co: ${result.is_co}`);
        logDebug(`Final CCO value set to: ${this.state.cco} (matches backend logic)`);
        
        // If CCO value changed (and it wasn't the initial null), clear potentially stale cache entries
        if (oldCcoValue !== null && oldCcoValue !== newCcoValue) {
          logDebug(`CCO value changed from ${oldCcoValue} to ${newCcoValue}, clearing stale cache`);
          await this._clearStaleCache(oldCcoValue);
        }
      }
      return result;
    } catch (error) {
      console.error("Error fetching current user:", error);
      return null;
    }
  }

  /**
   * Clear stale cache entries with old CCO value
   */
  async _clearStaleCache(oldCcoValue) {
    try {
      const branches = JSON.stringify(this.state.branches_id || []);
      const datepicked = String(this.state.datepicked || 20000);
      const uniqueId = String(this.state.uniqueId || '');
      const oldCcoStr = String(oldCcoValue).toLowerCase();
      
      // Generate old cache keys that should be cleared
      const staleCacheKeys = [
        `all_stats_${oldCcoStr}_${branches}_${datepicked}_${uniqueId}`,
        `charts_data_${oldCcoStr}_${branches}_${datepicked}_${uniqueId}`,
      ];
      
      // Clear stale cache entries using RPC
      for (const key of staleCacheKeys) {
        try {
          await this.rpc("/dashboard/cache/invalidate", { key: key });
          logDebug(`Cleared stale cache key: ${key}`);
        } catch (error) {
          logDebug(`Failed to clear stale cache key ${key}:`, error);
        }
      }
    } catch (error) {
      console.error("Error clearing stale cache:", error);
    }
  }

  /**
   * Validate stats data structure - flexible validation
   */
  _validateStatsData(data) {
    if (!data) {
      return false;
    }

    // Accept any object or array - let the backend determine what's valid
    if (typeof data === 'object') {
      return true;
    }

    return false;
  }

  /**
   * Get all stats with proper caching and error handling
   */
  async getAllStats() {
    try {
      this.state.loadingStates.stats = true;
      
      const cacheKey = this._generateStatsCacheKey();
      logDebug(`Fetching stats with cache key: ${cacheKey}`);
      
      // Try to get from cache
      const cachedData = await this.serverCache.getCache(cacheKey);
      
      if (cachedData && this._validateStatsData(cachedData)) {
        logDebug('Using cached stats data');
        
        if (Array.isArray(cachedData.data)) {
          this.state.stats = [...cachedData.data];
          this.state.totalstat = cachedData.total || cachedData.data.length;
        } else if (Array.isArray(cachedData)) {
          this.state.stats = [...cachedData];
          this.state.totalstat = cachedData.length;
        }
        
        this.state.loadingStates.stats = false;
        return cachedData;
      }
      
      // Cache miss or invalid data, fetch from server
      logDebug('Fetching stats from server');
      const result = await this.rpc(`/dashboard/stats`, {
        cco: this.state.cco,
        branches_id: this.state.branches_id,
        datepicked: Number(this.state.datepicked),
      });
      
      if (result && this._validateStatsData(result)) {
        logDebug('Got valid stats data:', result);
        
        if (Array.isArray(result.data)) {
          this.state.stats = [...result.data];
          this.state.totalstat = result.total || result.data.length;
        } else if (Array.isArray(result)) {
          this.state.stats = [...result];
          this.state.totalstat = result.length;
          // Normalize for caching
          const normalizedResult = { data: result, total: result.length };
          await this.serverCache.setCache(cacheKey, normalizedResult);
        }
        
        // Cache the valid result
        if (result.data) {
          await this.serverCache.setCache(cacheKey, result);
        }
      } else {
        logDebug('No valid stats data returned');
        this.state.stats = [];
        this.state.totalstat = 0;
      }
      
      this.state.loadingStates.stats = false;
      return result;
    } catch (error) {
      console.error("Error fetching stats:", error);
      this.state.loadingStates.stats = false;
      this.state.stats = [];
      this.state.totalstat = 0;
      return null;
    }
  }

  /**
   * Get stats by category with caching
   */
  async getStatsByCategory(category) {
    try {
      this.state.loadingStates.stats = true;
      
      const cacheKey = this._generateCategoryStatsCacheKey(category);
      logDebug(`Fetching category stats with cache key: ${cacheKey}`);
      
      // Try to get from cache
      const cachedData = await this.serverCache.getCache(cacheKey);
      
      if (cachedData && this._validateStatsData(cachedData)) {
        logDebug('Using cached category stats data');
        
        // Handle different possible data structures flexibly
        if (cachedData.data && Array.isArray(cachedData.data)) {
          this.state.stats = [...cachedData.data];
          this.state.totalstat = cachedData.total || cachedData.data.length;
        } else if (Array.isArray(cachedData)) {
          this.state.stats = [...cachedData];
          this.state.totalstat = cachedData.length;
        } else {
          this.state.stats = [];
          this.state.totalstat = 0;
        }
        
        this.state.loadingStates.stats = false;
        return cachedData;
      }
      
      // Cache miss, fetch from server
      logDebug('Fetching category stats from server');
      const result = await this.rpc(`/dashboard/statsbycategory`, {
        cco: this.state.cco,
        branches_id: this.state.branches_id,
        category: category,
        datepicked: Number(this.state.datepicked),
      });
      
      if (result && this._validateStatsData(result)) {
        // Handle different possible response structures flexibly
        if (result.data && Array.isArray(result.data)) {
          this.state.stats = [...result.data];
          this.state.totalstat = result.total || result.data.length;
        } else if (Array.isArray(result)) {
          this.state.stats = [...result];
          this.state.totalstat = result.length;
        } else {
          this.state.stats = [];
          this.state.totalstat = 0;
        }
        
        // Cache the result
        await this.serverCache.setCache(cacheKey, result);
      } else {
        this.state.stats = [];
        this.state.totalstat = 0;
      }
      
      this.state.loadingStates.stats = false;
      return result;
    } catch (error) {
      console.error("Error fetching category stats:", error);
      this.state.loadingStates.stats = false;
      this.state.stats = [];
      this.state.totalstat = 0;
      return null;
    }
  }

  /**
   * Validate chart data structure - flexible validation  
   */
  _validateChartData(data) {
    if (!data) {
      return false;
    }

    // Accept any array - let the backend determine what's valid
    if (Array.isArray(data)) {
      return true;
    }

    return false;
  }

  /**
   * Fetch dashboard charts with proper caching
   */
  async fetchDashboardCharts() {
    try {
      this.state.loadingStates.charts = true;
      
      const cacheKey = this._generateChartsCacheKey();
      logDebug(`Fetching charts with cache key: ${cacheKey}`);
      
      // Try to get from cache
      const cachedData = await this.serverCache.getCache(cacheKey);
      
      if (cachedData && this._validateChartData(cachedData)) {
        logDebug('Using cached chart data');
        this.state.dynamic_chart = [...cachedData];
        this.state.loadingStates.charts = false;
        return cachedData;
      }
      
      // Cache miss, fetch from server
      logDebug('Fetching charts from server');
      const result = await this.rpc(`/dashboard/dynamic_charts/`, {
        cco: this.state.cco,
        branches_id: this.state.branches_id,
        datepicked: Number(this.state.datepicked),
      });
      
      if (result && this._validateChartData(result)) {
        logDebug('Got valid chart data:', result);
        this.state.dynamic_chart = [...result];
        // Cache the result
        await this.serverCache.setCache(cacheKey, result);
      } else {
        logDebug('No valid chart data returned');
        this.state.dynamic_chart = [];
      }
      
      this.state.loadingStates.charts = false;
      return result;
    } catch (error) {
      console.error("Error fetching charts:", error);
      this.state.loadingStates.charts = false;
      this.state.dynamic_chart = [];
      return null;
    }
  }

  /**
   * Filter by date with proper error handling
   */
  async filterByDate(forceRefresh = false) {
    try {
      // If forcing refresh, clear ALL cache for this user to avoid conflicts
      if (forceRefresh) {
        await this._clearAllUserCache();
      }
      
      // Reset loading states
      this.state.loadingStates.stats = true;
      this.state.loadingStates.charts = true;
      
      // Load stats and charts in parallel
      const [statsResult, chartsResult] = await Promise.allSettled([
        this.getAllStats(),
        this.fetchDashboardCharts()
      ]);
      
      if (statsResult.status === 'rejected') {
        console.error('Stats loading failed:', statsResult.reason);
      }
      
      if (chartsResult.status === 'rejected') {
        console.error('Charts loading failed:', chartsResult.reason);
      }
      
      return true;
    } catch (error) {
      console.error("Error in filterByDate:", error);
      this._clearLoadingStates();
      return false;
    }
  }

  /**
   * Clear all cache for current user
   */
  async _clearAllUserCache() {
    try {
      await this.rpc("/dashboard/cache/invalidate", {});
      logDebug('Cleared all user cache');
    } catch (error) {
      console.error("Error clearing all user cache:", error);
    }
  }

  /**
   * Display by category with proper error handling
   */
  async displaybycategory(category) {
    try {
      this.state.isCategorySortingEnabled = category !== "all";
      
      if (category === "all") {
        await this.getAllStats();
      } else {
        await this.getStatsByCategory(category);
      }
      
      return true;
    } catch (error) {
      console.error("Error in displayByCategory:", error);
      return false;
    }
  }

  /**
   * Display Odoo view based on query results with caching
   */
  async displayOdooView(category, query, branch_filter, branch_field, title) {
    try {
      // Generate cache key for this query
      const cacheKey = `dynamic_sql_${this.state.cco}_${JSON.stringify(this.state.branches_id)}_${encodeURIComponent(query)}_${this.state.uniqueId}`;
      
      // Try to get from cache
      let response = await this.serverCache.getCache(cacheKey);
      
      if (!response) {
        // Cache miss, fetch from server
        response = await this.rpc("/dashboard/dynamic_sql", { 
          sql_query: query, 
          branches_id: this.state.branches_id, 
          cco: this.state.cco 
        });
        
        // Cache the result
        if (response && !response.error) {
          await this.serverCache.setCache(cacheKey, response);
        }
      }
      
      if (!response || response.error) {
        console.error("Error in dynamic_sql response:", response?.error);
        return;
      }

      // Create a properly formatted title
      const displayTitle = title || (category ? 
        category.charAt(0).toUpperCase() + category.slice(1).toLowerCase() : 
        "Card Results");
      
      // Fix domain issues for date fields
      if (response.domain && Array.isArray(response.domain)) {
        response.domain = response.domain.map(item => {
          if (Array.isArray(item) && item.length === 3) {
            const [field, operator, value] = item;
            
            const isDateField = field.endsWith('_date') || 
                               field.endsWith('_datetime') || 
                               field === 'date' || 
                               field === 'datetime';
            
            if (isDateField && (value === '0001-01-01' || value === false || value === null)) {
              return [field, '=', false];
            }
          }
          return item;
        });
      }

      // Navigate to the view
      this.navigate.doAction({
        type: "ir.actions.act_window",
        res_model: response.table.replace(/_/g, "."),
        name: displayTitle,
        domain: response.domain,
        views: [
          [false, "tree"],
          [false, "form"],
        ],
      });
    } catch (error) {
      console.error("Error in displayOdooView:", error);
    }
  }
}

ComplianceDashboard.template = "owl.ComplianceDashboard";
ComplianceDashboard.components = { Card, ChartRenderer };

registry.category("actions").add("owl.compliance_dashboard", ComplianceDashboard);


// /** @odoo-module */

// import { registry } from "@web/core/registry";
// import { useService } from "@web/core/utils/hooks";
// import { Card } from "../../card/js/card";
// import { ChartRenderer } from "../../chart/js/chart";
// const { Component, useState, useEffect, useRef, onWillStart, onWillUnmount } = owl;

// // Debug mode - set to false for production
// const DEBUG = true;
// function logDebug(...args) {
//   if (DEBUG) console.log(...args);
// }

// /**
//  * Custom hook for bus service notifications
//  * @param {string} channelName - Channel to listen on
//  * @param {Function} callback - Callback on notification
//  */
// export function useBusListener(channelName, callback) {
//   const bus = useService("bus_service");
  
//   useEffect(
//     () => {
//       bus.addChannel(channelName);
      
//       const handler = (ev) => {
//         const notifications = ev.detail;
//         for (const notification of notifications) {
//           if (Array.isArray(notification) && notification[0] === channelName) {
//             callback(notification[2]);
//           } else if (!Array.isArray(notification) && notification.payload && notification.payload.channelName === channelName) {
//             callback(notification.payload);
//           }
//         }
//       };
    
//       bus.addEventListener('notification', handler);
     
//       return () => {
//         bus.removeEventListener('notification', handler);
//         bus.deleteChannel(channelName);
//       };
//     },
//     () => [bus, channelName, callback]
//   );
// }

// export class ComplianceDashboard extends Component {
//   setup() {
//     this.left_indicator = useRef("left");
//     this.api = useService("orm");
//     this.rpc = useService("rpc");
//     this.navigate = useService("action");
//     this.serverCache = useService("server_cache");
//     this.user = useService("user");
    
//     // Initialize state with empty arrays (not null) and loading indicators
//     this.state = useState({
//       isCategorySortingEnabled: false,
//       cco: false,
//       branches_id: [],
//       stats: [], // Empty array, not null
//       totalstat: 0,
//       datepicked: 20000,
//       dynamic_chart: [], // Empty array, not null
//       scrollLeft: true,
//       scrollRight: false,
//       loadingStates: {
//         stats: true,
//         charts: true
//       },
//       useDynamicLoading: true, // Flag to enable progressive loading
//       pageSize: 50
//     });
    
//     // Models that should trigger a refresh
//     this.refreshModels = ['res.partner', 'res.branch'];

//     // Setup bus listener for refreshing the dashboard
//     useBusListener('dashboard_refresh_channel', this.handleRefreshNotification.bind(this));
    
//     // Initialize component with progressive loading
//     onWillStart(async () => {
//       try {
//         // Hide Odoo's global loading spinner immediately
//         this._hideGlobalLoadingIndicator();
        
//         // Get user info first (essential for everything else)
//         await this.getCurrentUser();
        
//         // Start loading data progressively without awaiting
//         this._loadDataProgressively();
        
//       } catch (error) {
//         console.error("Error in component initialization:", error);
//         // Clear loading states on error
//         this.state.loadingStates.stats = false;
//         this.state.loadingStates.charts = false;
//       }
//     });
    
//     // Set up auto-refresh every 5 minutes
//     // useEffect(() => {
//     //   const refreshTimer = setInterval(async () => {
//     //     logDebug("Auto-refreshing dashboard data...");
//     //     await this.filterByDate(true);
//     //   }, 5 * 60 * 1000); // 5 minutes
      
//     //   return () => {
//     //     clearInterval(refreshTimer);
//     //   };
//     // }, () => []);
    
//     // Set up scroll event listener
//     useEffect(() => {
//       let cardContainer = document.querySelector(".card-container");
//       if (cardContainer) {
//         cardContainer.addEventListener("scroll", this._onHorizontalScroll.bind(this));
//       }
//       return () => {
//         if (cardContainer) {
//           cardContainer.removeEventListener("scroll", this._onHorizontalScroll);
//         }
//       };
//     }, () => []);
    
//     // Bind methods
//     this.displayOdooView = this.displayOdooView.bind(this);
//     this.displaybycategory = this.displaybycategory.bind(this);
//     this.filterByDate = this.filterByDate.bind(this);
//     this._onHorizontalScroll = this._onHorizontalScroll.bind(this);
//   }

//   /**
//    * Handle notifications from the bus
//    */
//   async handleRefreshNotification(notification) {
//     logDebug("Received notification:", notification);

//     if (notification.type === 'refresh' && this.refreshModels.includes(notification.model)) {
//       // Invalidate cache
//       await this.serverCache.invalidateCache();
      
//       // Reload the view
//       try {
//         await this.filterByDate(true); // Force refresh
//       } catch (error) {
//         console.error("Error refreshing dashboard:", error);
//       }
//     }
//   }

//   /**
//    * Hide Odoo's global loading spinner
//    */
//   _hideGlobalLoadingIndicator() {
//     const loader = document.querySelector('.o_loading');
//     if (loader) {
//       loader.style.display = 'none';
//     }
//   }

//   /**
//    * Load data progressively - cards first, then charts
//    */
//   _loadDataProgressively() {
//     // First load cards (don't await)
//     this.getAllStats().finally(() => {
//       // After cards are loaded, start loading charts with a timeout
//       // Adding a small delay allows the cards to render completely
//       setTimeout(() => {
//         // Set a timeout for chart loading
//         const chartPromise = this.fetchDashboardCharts();
        
//         // Add a timeout of 60 seconds in case charts take too long
//         const timeoutPromise = new Promise((resolve) => {
//           setTimeout(() => {
//             // If charts are still loading after 60 seconds, mark as not loading
//             if (this.state.loadingStates.charts) {
//               this.state.loadingStates.charts = false;
//               console.warn("Chart loading timeout - please check server performance");
//             }
//             resolve(null);
//           }, 60000);
//         });
        
//         // Race the chart loading and timeout
//         Promise.race([chartPromise, timeoutPromise]);
//       }, 100);
//     });
//   }

//   /**
//    * Handle horizontal scroll events
//    */
//   _onHorizontalScroll(event) {
//     const container = event.target || event.currentTarget;
//     if (!container) return;

//     const atRight = container.scrollLeft + container.clientWidth >= container.scrollWidth - 5;
//     const atLeft = container.scrollLeft <= 5;
    
//     if (atRight) {
//       this.state.scrollRight = true;
//       this.state.scrollLeft = false;
//     } else if (atLeft) {
//       this.state.scrollLeft = true;
//       this.state.scrollRight = false;
//     } else {
//       this.state.scrollLeft = false;
//       this.state.scrollRight = false;
//     }
//   }

//   /**
//    * Get current user info
//    */
//   async getCurrentUser() {
//     try {
//       logDebug('Fetching current user');
//       const result = await this.rpc("/dashboard/user");
//       if (result) {
//         logDebug('Got user data:', result);
//         this.state.branches_id = result.branch;
//         this.state.cco = result.group;
//         this.state.uniqueId = result.unique_id;
//       }
//       return result;
//     } catch (error) {
//       console.error("Error fetching current user:", error);
//       return null;
//     }
//   }

//   /** 
//  * Normalize cache key components to match server format
//  */
// _normalizeCacheKeyComponents(cco, branchesId, datepicked, uniqueId) {
//   // Convert boolean to lowercase string
//   const ccoStr = String(cco).toLowerCase();
  
//   // Format branches array to match Python's list representation with spaces
//   let branchesStr = "[]";
//   if (Array.isArray(branchesId) && branchesId.length > 0) {
//     // Sort branches for consistency
//     const sortedBranches = [...branchesId].sort((a, b) => a - b);
//     // Format to match Python's list representation with spaces after commas
//     branchesStr = JSON.stringify(sortedBranches).replace(/,/g, ', ');
//   }
  
//   // Convert datepicked to string
//   const datepickedStr = String(datepicked);
  
//   return { ccoStr, branchesStr, datepickedStr, uniqueId };
// }

// /**
//  * Get all stats with consistent cache key format and proper error handling
//  */
// async getAllStats() {
//   try {
//     // Mark as loading
//     this.state.loadingStates.stats = true;
    
//     // Normalize components for cache key
//     const { ccoStr, branchesStr, datepickedStr, uniqueId } = this._normalizeCacheKeyComponents(
//       this.state.cco,
//       this.state.branches_id,
//       this.state.datepicked,
//       this.state.uniqueId
//     );
    
//     // Generate cache key to match server format
//     const cacheKey = `all_stats_${ccoStr}_${branchesStr}_${datepickedStr}_${uniqueId}`;
    
//     console.log(`Checking cache for: ${cacheKey}`);
    
//     // Try to get from cache
//     const cachedData = await this.serverCache.getCache(cacheKey);
    
//     if (cachedData) {
//       logDebug('Using cached stats data', cachedData);
      
//       // Validate cached data structure
//       if (cachedData && typeof cachedData === 'object') {
//         // Handle different possible cached data structures
//         if (Array.isArray(cachedData.data)) {
//           // Expected structure: { data: [...], total: number }
//           this.state.stats = [...cachedData.data];
//           this.state.totalstat = cachedData.total || 0;
//         } else if (Array.isArray(cachedData)) {
//           // Cached data is directly an array
//           this.state.stats = [...cachedData];
//           this.state.totalstat = cachedData.length;
//         } else if (cachedData.data && Array.isArray(cachedData.data)) {
//           // Nested structure
//           this.state.stats = [...cachedData.data];
//           this.state.totalstat = cachedData.total || cachedData.data.length;
//         } else {
//           // Unexpected structure, log and fetch fresh
//           console.warn('Unexpected cached data structure:', cachedData);
//           throw new Error('Invalid cached data structure');
//         }
        
//         this.state.loadingStates.stats = false;
//         return cachedData;
//       } else {
//         // Invalid cached data, continue to fetch fresh
//         console.warn('Invalid cached data, fetching fresh data');
//       }
//     }
    
//     // Not in cache or invalid cache, fetch from server
//     logDebug('Fetching stats from server');
//     const result = await this.rpc(`/dashboard/stats`, {
//       cco: this.state.cco,
//       branches_id: this.state.branches_id,
//       datepicked: Number(this.state.datepicked),
//     });
    
//     if (result && typeof result === 'object') {
//       logDebug('Got stats data:', result);
      
//       // Validate server response structure
//       if (Array.isArray(result.data)) {
//         this.state.stats = [...result.data];
//         this.state.totalstat = result.total || result.data.length;
        
//         // Cache the result only if it's valid
//         await this.serverCache.setCache(cacheKey, result);
//       } else if (Array.isArray(result)) {
//         // Server returned array directly
//         this.state.stats = [...result];
//         this.state.totalstat = result.length;
        
//         // Cache in expected format
//         const cacheData = { data: result, total: result.length };
//         await this.serverCache.setCache(cacheKey, cacheData);
//       } else {
//         console.warn('Unexpected server response structure:', result);
//         this.state.stats = [];
//         this.state.totalstat = 0;
//       }
//     } else {
//       logDebug('No stats data returned from API');
//       this.state.stats = [];
//       this.state.totalstat = 0;
//     }
    
//     this.state.loadingStates.stats = false;
//     return result;
//   } catch (error) {
//     console.error("Error fetching all stats:", error);
//     this.state.loadingStates.stats = false;
//     this.state.stats = []; // Ensure it's an empty array on error
//     this.state.totalstat = 0;
//     return null;
//   }
// }

//   // /**
//   //  * Get all stats with caching
//   //  */
//   // async getAllStats() {
//   //   try {
//   //     // Mark as loading
//   //     this.state.loadingStates.stats = true;
      
//   //     // Generate cache key
//   //     const cacheKey = `all_stats_${this.state.cco}_${JSON.stringify(this.state.branches_id)}_${this.state.datepicked}_${this.state.uniqueId}`;
      
//   //     // Try to get from cache
//   //     const cachedData = await this.serverCache.getCache(cacheKey);
      
//   //     if (cachedData) {
//   //       logDebug('Using cached stats data');
//   //       this.state.stats = [...cachedData.data];
//   //       this.state.totalstat = cachedData.total;
//   //       this.state.loadingStates.stats = false;
//   //       return cachedData;
//   //     }
      
//   //     // Not in cache, fetch from server
//   //     logDebug('Fetching stats from server');
//   //     const result = await this.rpc(`/dashboard/stats`, {
//   //       cco: this.state.cco,
//   //       branches_id: this.state.branches_id,
//   //       datepicked: Number(this.state.datepicked),
//   //     });
      
//   //     if (result) {
//   //       logDebug('Got stats data:', result);
//   //       this.state.stats = [...result.data];
//   //       this.state.totalstat = result.total;
//   //       // Cache the result
//   //       await this.serverCache.setCache(cacheKey, result);
//   //     } else {
//   //       logDebug('No stats data returned from API');
//   //       this.state.stats = []; // Ensure it's an empty array, not null
//   //     }
      
//   //     this.state.loadingStates.stats = false;
//   //     return result;
//   //   } catch (error) {
//   //     console.error("Error fetching all stats:", error);
//   //     this.state.loadingStates.stats = false;
//   //     this.state.stats = []; // Ensure it's an empty array on error
//   //     return null;
//   //   }
//   // }

//   /**
//    * Get stats by category with caching
//    */
//   async getStatsByCategory(name) {
//     try {
//       this.state.loadingStates.stats = true;
      
//       // Generate cache key
//       const cacheKey = `stats_category_${this.state.cco}_${JSON.stringify(this.state.branches_id)}_${name}_${this.state.datepicked}_${this.state.uniqueId}`;
      
//       // Try to get from cache
//       const cachedData = await this.serverCache.getCache(cacheKey);
      
//       if (cachedData) {
//         logDebug('Using cached category stats data');
//         this.state.stats = cachedData.data;
//         this.state.totalstat = cachedData.total;
//         this.state.loadingStates.stats = false;
//         return cachedData;
//       }
      
//       // Not in cache, fetch from server
//       logDebug('Fetching category stats from server');
//       const result = await this.rpc(`/dashboard/statsbycategory`, {
//         cco: this.state.cco,
//         branches_id: this.state.branches_id,
//         category: name,
//         datepicked: Number(this.state.datepicked),
//       });
      
//       if (result) {
//         this.state.stats = result.data;
//         this.state.totalstat = result.total;
//         // Cache the result
//         await this.serverCache.setCache(cacheKey, result);
//       }
      
//       this.state.loadingStates.stats = false;
//       return result;
//     } catch (error) {
//       console.error("Error fetching stats by category:", error);
//       this.state.loadingStates.stats = false;
//       return null;
//     }
//   }

//   // /**
//   //  * Fetch dashboard charts without any timeout - we'll handle progressive rendering
//   //  */
//   // async fetchDashboardCharts() {
//   //   try {
//   //     // Mark as loading
//   //     this.state.loadingStates.charts = true;
      
//   //     // Generate cache key
//   //     const cacheKey = `dynamic_charts_${this.state.cco}_${JSON.stringify(this.state.branches_id)}_${this.state.datepicked}_${this.state.uniqueId}`;
      
//   //     // Try to get from cache
//   //     const cachedData = await this.serverCache.getCache(cacheKey);
      
//   //     if (cachedData) {
//   //       logDebug('Using cached chart data');
//   //       this.state.dynamic_chart = cachedData;
//   //       this.state.loadingStates.charts = false;
//   //       return cachedData;
//   //     }
      
//   //     // Not in cache, fetch from server
//   //     logDebug('Fetching chart data from server');
//   //     const response = await this.rpc(`/dashboard/dynamic_charts/`, {
//   //       cco: this.state.cco,
//   //       branches_id: this.state.branches_id,
//   //       datepicked: Number(this.state.datepicked),
//   //     });  
      
//   //     if (response && response.error) {
//   //       console.error(`Error fetching dashboard charts: ${response.error}`);
//   //       this.state.dynamic_chart = [];
//   //     } else if (response) {
//   //       this.state.dynamic_chart = response;
//   //       // Cache the result
//   //       await this.serverCache.setCache(cacheKey, response);
//   //     } else {
//   //       console.error("Error: Empty response received while fetching dashboard charts.");
//   //       this.state.dynamic_chart = [];
//   //     }
      
//   //     this.state.loadingStates.charts = false;
//   //     return response;
//   //   } catch (error) {
//   //     console.error("Error in fetchDashboardCharts:", error);
//   //     this.state.dynamic_chart = []; 
//   //     this.state.loadingStates.charts = false;
//   //     return null;
//   //   }
//   // }


//   /**
//    * Fetch dashboard charts metadata first, then load charts progressively
//    */
//   async fetchDashboardStats() {
//     try {
//       // Mark as loading
//       this.state.loadingStates.stats = true;
      
//       // Generate cache key - using your existing format
//       const uniqueId = this.state.uniqueId; 
//       const cache_key = `all_stats_${this.state.cco}_${JSON.stringify(this.state.branches_id)}_${this.state.datepicked}_${uniqueId}`;
      
//       console.log(`Checking cache for: ${cache_key}`);
      
//       // Get from cache
//       let statsData = await this.serverCache.getCache(cache_key);
      
//       if (!statsData) {
//         console.log(`Cache miss for ${cache_key}`);
//         // Nothing in cache, fetch fresh data
//         statsData = await this.rpc('/dashboard/all_stats/', {
//           cco: this.state.cco,
//           branches_id: this.state.branches_id,
//           datepicked: Number(this.state.datepicked),
//         });
        
//         console.log("Got stats data: ", statsData);
        
//         // Cache the results if valid - use 600 seconds (10 minutes) TTL
//         if (statsData && !statsData.error) {
//           console.log(`Cache set for ${cache_key}`);
//           await this.serverCache.setCache(cache_key, statsData);
//         }
//       } else {
//         console.log(`Cache hit for ${cache_key}`);
//       }
      
//       // Update state with stats data
//       if (statsData && statsData.error) {
//         console.error(`Error fetching stats: ${statsData.error}`);
//         this.state.all_stats = {data: [], total: 0};
//       } else if (statsData) {
//         this.state.all_stats = statsData;
//       } else {
//         console.error("Error: Empty response received while fetching stats.");
//         this.state.all_stats = {data: [], total: 0};
//       }
      
//       this.state.loadingStates.stats = false;
//       return statsData;
      
//     } catch (error) {
//       console.error("Error in fetchDashboardStats:", error);
//       this.state.all_stats = {data: [], total: 0};
//       this.state.loadingStates.stats = false;
//       return null;
//     }
//   }


//   /**
//    * Fetch dashboard charts with caching and performance optimizations
//    */
//   async fetchDashboardCharts() {
//   try {
//     // Mark as loading
//     this.state.loadingStates.charts = true;
    
//     // Normalize components for cache key - reuse the same function used for stats
//     const { ccoStr, branchesStr, datepickedStr, uniqueId } = this._normalizeCacheKeyComponents(
//       this.state.cco,
//       this.state.branches_id,
//       this.state.datepicked,
//       this.state.uniqueId
//     );
    
//     // Generate cache key to match server format
//     const cacheKey = `charts_data_${ccoStr}_${branchesStr}_${datepickedStr}_${uniqueId}`;
    
//     console.log(`Checking cache for charts: ${cacheKey}`);
    
//     // Try to get from cache - use the same pattern as getAllStats
//     const cachedData = await this.serverCache.getCache(cacheKey);
    
//     if (cachedData) {
//       logDebug('Using cached chart data');
//       // Simply set the state without JSON.parse/stringify which can cause issues
//       this.state.dynamic_chart = cachedData;
//       this.state.loadingStates.charts = false;
//       return cachedData;
//     }
    
//     // Not in cache, fetch from server - use the same pattern as getAllStats
//     logDebug('Fetching chart data from server');
//     const result = await this.rpc(`/dashboard/dynamic_charts/`, {
//       cco: this.state.cco,
//       branches_id: this.state.branches_id,
//       datepicked: Number(this.state.datepicked),
//     });
    
//     if (result) {
//       logDebug('Got chart data:', result);
//       // Directly assign to state without transformation
//       this.state.dynamic_chart = result;
//       // Cache the result
//       await this.serverCache.setCache(cacheKey, result);
//     } else {
//       logDebug('No chart data returned from API');
//       this.state.dynamic_chart = []; // Ensure it's an empty array, not null
//     }
    
//     this.state.loadingStates.charts = false;
//     return result;
//   } catch (error) {
//     console.error("Error fetching dashboard charts:", error);
//     this.state.loadingStates.charts = false;
//     this.state.dynamic_chart = []; // Ensure it's an empty array on error
//     return null;
//   }
// }
//   // async fetchDashboardCharts() {
//   //   try {
//   //     // Mark as loading
//   //     this.state.loadingStates.charts = true;
      
//   //     // Generate cache key - using your existing format
//   //     const uniqueId = this.state.uniqueId;
//   //     const cache_key = `charts_data_${this.state.cco}_${JSON.stringify(this.state.branches_id)}_${this.state.datepicked}_${uniqueId}`;
      
//   //     console.log(`Checking cache for: ${cache_key}`);
      
//   //     // Check if we have valid cache for this user
//   //     let chartsData = await this.serverCache.getCache(cache_key);
      
//   //     if (!chartsData) {
//   //       console.log(`Cache miss for ${cache_key}`);
//   //       // No cache, fetch fresh data
//   //       chartsData = await this.rpc('/dashboard/dynamic_charts/', {
//   //         cco: this.state.cco,
//   //         branches_id: this.state.branches_id,
//   //         datepicked: Number(this.state.datepicked),
//   //       });
        
//   //       // Cache the results with 600 seconds (10 minutes) TTL
//   //       if (chartsData && !chartsData.error) {
//   //         console.log(`Cache set for ${cache_key}`);
//   //         await this.serverCache.setCache(cache_key, chartsData);
//   //       }
//   //     } else {
//   //       console.log(`Cache hit for ${cache_key}`);
//   //     }
      
//   //     // Update state with chart data
//   //     if (chartsData && chartsData.error) {
//   //       console.error(`Error fetching charts: ${chartsData.error}`);
//   //       this.state.dynamic_chart = [];
//   //     } else if (chartsData) {
//   //       this.state.dynamic_chart = chartsData;
//   //     } else {
//   //       console.error("Error: Empty response received while fetching charts.");
//   //       this.state.dynamic_chart = [];
//   //     }
      
//   //     this.state.loadingStates.charts = false;
//   //     return chartsData;
      
//   //   } catch (error) {
//   //     console.error("Error in fetchDashboardCharts:", error);
//   //     this.state.dynamic_chart = [];
//   //     this.state.loadingStates.charts = false;
//   //     return null;
//   //   }
//   // }
//   // async fetchDashboardCharts() {
//   //   try {
//   //     // Mark as loading
//   //     this.state.loadingStates.charts = true;
      
//   //     // Generate cache key
//   //     const cacheKey = `dynamic_charts_${this.state.cco}_${JSON.stringify(this.state.branches_id)}_${this.state.datepicked}_${this.state.uniqueId}`;
      
//   //     // Try to get from cache
//   //     const cachedData = await this.serverCache.getCache(cacheKey);
      
//   //     if (cachedData) {
//   //       logDebug('Using cached chart data');
//   //       this.state.dynamic_chart = cachedData;
//   //       this.state.loadingStates.charts = false;
//   //       return cachedData;
//   //     }
      
//   //     // Not in cache, fetch from server with timeout
//   //     logDebug('Fetching chart data from server');
      
//   //     // Create a timeout promise that rejects after 30 seconds
//   //     const timeout = new Promise((_, reject) => {
//   //       setTimeout(() => reject(new Error('Chart data fetch timeout')), 30000);
//   //     });
      
//   //     // Race the fetch and timeout
//   //     const response = await Promise.race([
//   //       this.rpc(`/dashboard/dynamic_charts/`, {
//   //         cco: this.state.cco,
//   //         branches_id: this.state.branches_id,
//   //         datepicked: Number(this.state.datepicked),
//   //       }),
//   //       timeout
//   //     ]).catch(error => {
//   //       console.error("Chart data fetch error or timeout:", error);
//   //       // Return empty array on timeout
//   //       return [];
//   //     });
      
//   //     if (response && response.error) {
//   //       console.error(`Error fetching dashboard charts: ${response.error}`);
//   //       this.state.dynamic_chart = [];
//   //     } else if (response) {
//   //       this.state.dynamic_chart = response;
//   //       // Cache the result
//   //       await this.serverCache.setCache(cacheKey, response);
//   //     } else {
//   //       console.error("Error: Empty response received while fetching dashboard charts.");
//   //       this.state.dynamic_chart = [];
//   //     }
      
//   //     this.state.loadingStates.charts = false;
//   //     return response;
//   //   } catch (error) {
//   //     console.error("Error in fetchDashboardCharts:", error);
//   //     this.state.dynamic_chart = []; // Ensure it's an empty array on error
//   //     this.state.loadingStates.charts = false;
//   //     return null;
//   //   }
//   // }

//   /**
//    * Filter by date with parallel requests
//    */
//   async filterByDate(forceRefresh = false) {
//     try {
//       // If forcing refresh, invalidate cache
//       if (forceRefresh) {
//         await this.serverCache.invalidateCache();
//       }
      
//       // Reset loading states
//       this.state.loadingStates = {
//         stats: true,
//         charts: true
//       };
      
//       // Using Promise.all for parallel execution
//       await Promise.all([
//         this.getAllStats(),
//         // this.fetchDashboardCharts()
//       ]);
      
//       return true;
//     } catch (error) {
//       console.error("Error in filterByDate:", error);
//       // Clear loading states on error
//       this.state.loadingStates.stats = false;
//       this.state.loadingStates.charts = false;
//       return false;
//     }
//   }

//   /**
//    * Display by category with proper error handling
//    */
//   async displaybycategory(name) {
//     try {
//       this.state.isCategorySortingEnabled = name !== "all";
      
//       if (name === "all") {
//         await this.getAllStats();
//       } else {
//         await this.getStatsByCategory(name);
//       }
      
//       return true;
//     } catch (error) {
//       console.error("Error in displayByCategory:", error);
//       return false;
//     }
//   }

//   /**
//    * Display Odoo view based on query results with caching
//    */
//   async displayOdooView(category, query, branch_filter, branch_field, title) {
//     try {
//       // Generate cache key for this query
//       const cacheKey = `dynamic_sql_${this.state.cco}_${JSON.stringify(this.state.branches_id)}_${query}_${this.state.uniqueId}`;
      
//       // Try to get from cache
//       let response = await this.serverCache.getCache(cacheKey);
      
//       if (!response) {
//         // Cache miss, fetch from server
//         response = await this.rpc("/dashboard/dynamic_sql", { 
//           sql_query: query, 
//           branches_id: this.state.branches_id, 
//           cco: this.state.cco 
//         });
        
//         // Cache the result
//         if (response) {
//           await this.serverCache.setCache(cacheKey, response);
//         }
//       }
      
//       if (!response) {
//         console.error("Empty response from dynamic_sql");
//         return;
//       }

//       // Create a properly formatted title in sentence case
//       const displayTitle = title || (category ? 
//         category.charAt(0).toUpperCase() + category.slice(1).toLowerCase() : 
//         "Card Results");
      
//       // Fix for date-related issues in domain
//       if (response.domain && Array.isArray(response.domain)) {
//         // Process the domain to handle date fields properly
//         response.domain = response.domain.map(item => {
//           // Check if we're dealing with a date comparison
//           if (Array.isArray(item) && item.length === 3) {
//             const [field, operator, value] = item;
            
//             // Check if field ends with _date or _datetime or is date/datetime
//             const isDateField = field.endsWith('_date') || 
//                                field.endsWith('_datetime') || 
//                                field === 'date' || 
//                                field === 'datetime';
            
//             // If it's a date field and the value is problematic
//             if (isDateField && (value === '0001-01-01' || value === false || value === null)) {
//               // Replace with a safer condition for filtering
//               return [field, '=', false];
//             }
//           }
//           return item;
//         });
//       }

//       // Now navigate with the fixed domain
//       this.navigate.doAction({
//         type: "ir.actions.act_window",
//         res_model: response.table.replace(/_/g, "."),
//         name: displayTitle,
//         domain: response.domain,
//         views: [
//           [false, "tree"],
//           [false, "form"],
//         ],
//       });
//     } catch (error) {
//       console.error("Error in displayOdooView:", error);
//     }
//   }
// }

// ComplianceDashboard.template = "owl.ComplianceDashboard";
// ComplianceDashboard.components = { Card, ChartRenderer };

// registry.category("actions").add("owl.compliance_dashboard", ComplianceDashboard);







