/** @odoo-module */

import { registry } from "@web/core/registry";
import { useService } from "@web/core/utils/hooks";
import { Card } from "../../card/js/card";
import { ChartRenderer } from "../../chart/js/chart";
const { Component, useState, useEffect, useRef, onWillStart, onWillUnmount } = owl;

// Debug mode - set to false for production
const DEBUG = false;
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
    
    this.state = useState({
      isCategorySortingEnabled: false,
      cco: null, 
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
        logDebug("Error in component initialization:", error);
        this._clearLoadingStates();
      }
    });
    
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
        logDebug("Error refreshing dashboard:", error);
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
          logDebug("Error loading charts:", error);
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
      logDebug("Error fetching current user:", error);
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
      
      const staleCacheKeys = [
        `all_stats_${oldCcoStr}_${branches}_${datepicked}_${uniqueId}`,
        `charts_data_${oldCcoStr}_${branches}_${datepicked}_${uniqueId}`,
      ];
      
      for (const key of staleCacheKeys) {
        try {
          await this.rpc("/dashboard/cache/invalidate", { key: key });
          logDebug(`Cleared stale cache key: ${key}`);
        } catch (error) {
          logDebug(`Failed to clear stale cache key ${key}:`, error);
        }
      }
    } catch (error) {
      logDebug("Error clearing stale cache:", error);
    }
  }

  /**
   * Validate stats data structure - flexible validation
   */
  _validateStatsData(data) {
    if (!data) {
      return false;
    }

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
          const normalizedResult = { data: result, total: result.length };
          await this.serverCache.setCache(cacheKey, normalizedResult);
        }
        
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
      logDebug("Error fetching stats:", error);
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
      
      const cachedData = await this.serverCache.getCache(cacheKey);
      
      if (cachedData && this._validateStatsData(cachedData)) {
        logDebug('Using cached category stats data');
        
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
      
      logDebug('Fetching category stats from server');
      const result = await this.rpc(`/dashboard/statsbycategory`, {
        cco: this.state.cco,
        branches_id: this.state.branches_id,
        category: category,
        datepicked: Number(this.state.datepicked),
      });
      
      if (result && this._validateStatsData(result)) {
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
        await this.serverCache.setCache(cacheKey, result);
      } else {
        this.state.stats = [];
        this.state.totalstat = 0;
      }
      
      this.state.loadingStates.stats = false;
      return result;
    } catch (error) {
      logDebug("Error fetching category stats:", error);
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
      const cachedData = await this.serverCache.getCache(cacheKey);
      
      if (cachedData && this._validateChartData(cachedData)) {
        logDebug('Using cached chart data');
        this.state.dynamic_chart = [...cachedData];
        this.state.loadingStates.charts = false;
        return cachedData;
      }
      logDebug('Fetching charts from server');
      const result = await this.rpc(`/dashboard/dynamic_charts/`, {
        cco: this.state.cco,
        branches_id: this.state.branches_id,
        datepicked: Number(this.state.datepicked),
      });
      
      if (result && this._validateChartData(result)) {
        logDebug('Got valid chart data:', result);
        this.state.dynamic_chart = [...result];
        await this.serverCache.setCache(cacheKey, result);
      } else {
        logDebug('No valid chart data returned');
        this.state.dynamic_chart = [];
      }
      
      this.state.loadingStates.charts = false;
      return result;
    } catch (error) {
      logDebug("Error fetching charts:", error);
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
      if (forceRefresh) {
        await this._clearAllUserCache();
      }
      
      this.state.loadingStates.stats = true;
      this.state.loadingStates.charts = true;
      
      const [statsResult, chartsResult] = await Promise.allSettled([
        this.getAllStats(),
        this.fetchDashboardCharts()
      ]);
      
      if (statsResult.status === 'rejected') {
        logDebug('Stats loading failed:', statsResult.reason);
      }
      
      if (chartsResult.status === 'rejected') {
        logDebug('Charts loading failed:', chartsResult.reason);
      }
      
      return true;
    } catch (error) {
      logDebug("Error in filterByDate:", error);
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
      logDebug("Error clearing all user cache:", error);
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
      logDebug("Error in displayByCategory:", error);
      return false;
    }
  }

  /**
   * Display Odoo view based on query results with caching
   */
  async displayOdooView(category, query, branch_filter, branch_field, title) {
    try {
      if (!query) {
            logDebug("No query provided for displayOdooView, skipping action");
            return;
        }

      const cacheKey = `dynamic_sql_${this.state.cco}_${JSON.stringify(this.state.branches_id)}_${encodeURIComponent(query)}_${this.state.uniqueId}`;
      let response = await this.serverCache.getCache(cacheKey);
      
      if (!response) {
        response = await this.rpc("/dashboard/dynamic_sql", { 
          sql_query: query, 
          branches_id: this.state.branches_id, 
          cco: this.state.cco 
        });
        if (response && !response.error) {
          await this.serverCache.setCache(cacheKey, response);
        }
      }
      
      if (!response || response.error) {
        logDebug("Error in dynamic_sql response:", response?.error);
        return;
      }

      const displayTitle = title || (category ? 
        category.charAt(0).toUpperCase() + category.slice(1).toLowerCase() : 
        "Card Results");
      
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
      } else {
            logDebug("No valid domain in response, skipping navigation");
            return;
      }

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
      logDebug("Error in displayOdooView:", error);
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
// const DEBUG = false;
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
    
//     this.state = useState({
//       isCategorySortingEnabled: false,
//       cco: null, 
//       branches_id: [],
//       stats: [],
//       totalstat: 0,
//       datepicked: 20000,
//       dynamic_chart: [],
//       scrollLeft: true,
//       scrollRight: false,
//       uniqueId: null,
//       loadingStates: {
//         stats: true,
//         charts: true
//       }
//     });
    
//     // Models that should trigger a refresh
//     this.refreshModels = ['res.partner', 'res.branch'];

//     // Setup bus listener for refreshing the dashboard
//     useBusListener('dashboard_refresh_channel', this.handleRefreshNotification.bind(this));
    
//     // Initialize component with progressive loading
//     onWillStart(async () => {
//       try {
//         this._hideGlobalLoadingIndicator();
//         await this.getCurrentUser();
//         this._loadDataProgressively();
//       } catch (error) {
//         console.error("Error in component initialization:", error);
//         this._clearLoadingStates();
//       }
//     });
    
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
//    * Clear all loading states
//    */
//   _clearLoadingStates() {
//     this.state.loadingStates.stats = false;
//     this.state.loadingStates.charts = false;
//   }

//   /**
//    * Generate consistent cache key for stats - matching backend format exactly
//    */
//   _generateStatsCacheKey() {
//     const cco = String(this.state.cco || false).toLowerCase();
//     const branches = JSON.stringify(this.state.branches_id || []);
//     const datepicked = String(this.state.datepicked || 20000);
//     const uniqueId = String(this.state.uniqueId || '');
    
//     return `all_stats_${cco}_${branches}_${datepicked}_${uniqueId}`;
//   }

//   /**
//    * Generate consistent cache key for charts - matching backend format exactly
//    */
//   _generateChartsCacheKey() {
//     const cco = String(this.state.cco || false).toLowerCase();
//     const branches = JSON.stringify(this.state.branches_id || []);
//     const datepicked = String(this.state.datepicked || 20000);
//     const uniqueId = String(this.state.uniqueId || '');
    
//     return `charts_data_${cco}_${branches}_${datepicked}_${uniqueId}`;
//   }

//   /**
//    * Generate consistent cache key for category stats - matching backend format
//    */
//   _generateCategoryStatsCacheKey(category) {
//     const cco = String(this.state.cco || false).toLowerCase();
//     const branches = JSON.stringify(this.state.branches_id || []);
//     const datepicked = String(this.state.datepicked || 20000);
//     const uniqueId = String(this.state.uniqueId || '');
    
//     return `stats_category_${cco}_${branches}_${category}_${datepicked}_${uniqueId}`;
//   }

//   /**
//    * Handle notifications from the bus
//    */
//   async handleRefreshNotification(notification) {
//     logDebug("Received notification:", notification);

//     if (notification.type === 'refresh' && this.refreshModels.includes(notification.model)) {
//       await this.serverCache.invalidateCache();
      
//       try {
//         await this.filterByDate(true);
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
//    * Load data progressively - stats first, then charts
//    */
//   _loadDataProgressively() {
//     // Only start loading if we have user info (CCO value is set)
//     if (this.state.cco === null) {
//       logDebug('Waiting for user info before loading data');
//       return;
//     }
    
//     // Load stats first
//     this.getAllStats().finally(() => {
//       // After stats are loaded, load charts with a small delay
//       setTimeout(() => {
//         this.fetchDashboardCharts().catch(error => {
//           console.error("Error loading charts:", error);
//         });
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
//    * Get current user info and clean up stale cache entries
//    */
//   async getCurrentUser() {
//     try {
//       logDebug('Fetching current user');
//       const result = await this.rpc("/dashboard/user");
//       if (result) {
//         logDebug('Got user data:', result);
//         this.state.branches_id = result.branch || [];
        
//         // Match backend logic: CCO = true if user is CCO OR CO
//         // Backend does: if is_co or is_cco: cco = True
//         const newCcoValue = result.is_cco || result.is_co || false;
//         const oldCcoValue = this.state.cco;
        
//         this.state.cco = newCcoValue;
//         this.state.uniqueId = result.unique_id || null;
        
//         logDebug(`User roles - is_cco: ${result.is_cco}, is_co: ${result.is_co}`);
//         logDebug(`Final CCO value set to: ${this.state.cco} (matches backend logic)`);
        
//         // If CCO value changed (and it wasn't the initial null), clear potentially stale cache entries
//         if (oldCcoValue !== null && oldCcoValue !== newCcoValue) {
//           logDebug(`CCO value changed from ${oldCcoValue} to ${newCcoValue}, clearing stale cache`);
//           await this._clearStaleCache(oldCcoValue);
//         }
//       }
//       return result;
//     } catch (error) {
//       console.error("Error fetching current user:", error);
//       return null;
//     }
//   }

//   /**
//    * Clear stale cache entries with old CCO value
//    */
//   async _clearStaleCache(oldCcoValue) {
//     try {
//       const branches = JSON.stringify(this.state.branches_id || []);
//       const datepicked = String(this.state.datepicked || 20000);
//       const uniqueId = String(this.state.uniqueId || '');
//       const oldCcoStr = String(oldCcoValue).toLowerCase();
      
//       // Generate old cache keys that should be cleared
//       const staleCacheKeys = [
//         `all_stats_${oldCcoStr}_${branches}_${datepicked}_${uniqueId}`,
//         `charts_data_${oldCcoStr}_${branches}_${datepicked}_${uniqueId}`,
//       ];
      
//       // Clear stale cache entries using RPC
//       for (const key of staleCacheKeys) {
//         try {
//           await this.rpc("/dashboard/cache/invalidate", { key: key });
//           logDebug(`Cleared stale cache key: ${key}`);
//         } catch (error) {
//           logDebug(`Failed to clear stale cache key ${key}:`, error);
//         }
//       }
//     } catch (error) {
//       console.error("Error clearing stale cache:", error);
//     }
//   }

//   /**
//    * Validate stats data structure - flexible validation
//    */
//   _validateStatsData(data) {
//     if (!data) {
//       return false;
//     }

//     if (typeof data === 'object') {
//       return true;
//     }

//     return false;
//   }

//   /**
//    * Get all stats with proper caching and error handling
//    */
//   async getAllStats() {
//     try {
//       this.state.loadingStates.stats = true;
      
//       const cacheKey = this._generateStatsCacheKey();
//       logDebug(`Fetching stats with cache key: ${cacheKey}`);
      
//       const cachedData = await this.serverCache.getCache(cacheKey);
      
//       if (cachedData && this._validateStatsData(cachedData)) {
//         logDebug('Using cached stats data');
        
//         if (Array.isArray(cachedData.data)) {
//           this.state.stats = [...cachedData.data];
//           this.state.totalstat = cachedData.total || cachedData.data.length;
//         } else if (Array.isArray(cachedData)) {
//           this.state.stats = [...cachedData];
//           this.state.totalstat = cachedData.length;
//         }
        
//         this.state.loadingStates.stats = false;
//         return cachedData;
//       }
      
//       logDebug('Fetching stats from server');
//       const result = await this.rpc(`/dashboard/stats`, {
//         cco: this.state.cco,
//         branches_id: this.state.branches_id,
//         datepicked: Number(this.state.datepicked),
//       });
      
//       if (result && this._validateStatsData(result)) {
//         logDebug('Got valid stats data:', result);
        
//         if (Array.isArray(result.data)) {
//           this.state.stats = [...result.data];
//           this.state.totalstat = result.total || result.data.length;
//         } else if (Array.isArray(result)) {
//           this.state.stats = [...result];
//           this.state.totalstat = result.length;
          
//           const normalizedResult = { data: result, total: result.length };
//           await this.serverCache.setCache(cacheKey, normalizedResult);
//         }
        
        
//         if (result.data) {
//           await this.serverCache.setCache(cacheKey, result);
//         }
//       } else {
//         logDebug('No valid stats data returned');
//         this.state.stats = [];
//         this.state.totalstat = 0;
//       }
      
//       this.state.loadingStates.stats = false;
//       return result;
//     } catch (error) {
//       console.error("Error fetching stats:", error);
//       this.state.loadingStates.stats = false;
//       this.state.stats = [];
//       this.state.totalstat = 0;
//       return null;
//     }
//   }

//   /**
//    * Get stats by category with caching
//    */
//   async getStatsByCategory(category) {
//     try {
//       this.state.loadingStates.stats = true;
      
//       const cacheKey = this._generateCategoryStatsCacheKey(category);
//       logDebug(`Fetching category stats with cache key: ${cacheKey}`);
      
//       const cachedData = await this.serverCache.getCache(cacheKey);
      
//       if (cachedData && this._validateStatsData(cachedData)) {
//         logDebug('Using cached category stats data');
        
//         if (cachedData.data && Array.isArray(cachedData.data)) {
//           this.state.stats = [...cachedData.data];
//           this.state.totalstat = cachedData.total || cachedData.data.length;
//         } else if (Array.isArray(cachedData)) {
//           this.state.stats = [...cachedData];
//           this.state.totalstat = cachedData.length;
//         } else {
//           this.state.stats = [];
//           this.state.totalstat = 0;
//         }
        
//         this.state.loadingStates.stats = false;
//         return cachedData;
//       }
      
//       logDebug('Fetching category stats from server');
//       const result = await this.rpc(`/dashboard/statsbycategory`, {
//         cco: this.state.cco,
//         branches_id: this.state.branches_id,
//         category: category,
//         datepicked: Number(this.state.datepicked),
//       });
      
//       if (result && this._validateStatsData(result)) {
//         if (result.data && Array.isArray(result.data)) {
//           this.state.stats = [...result.data];
//           this.state.totalstat = result.total || result.data.length;
//         } else if (Array.isArray(result)) {
//           this.state.stats = [...result];
//           this.state.totalstat = result.length;
//         } else {
//           this.state.stats = [];
//           this.state.totalstat = 0;
//         }
        
//         await this.serverCache.setCache(cacheKey, result);
//       } else {
//         this.state.stats = [];
//         this.state.totalstat = 0;
//       }
      
//       this.state.loadingStates.stats = false;
//       return result;
//     } catch (error) {
//       console.error("Error fetching category stats:", error);
//       this.state.loadingStates.stats = false;
//       this.state.stats = [];
//       this.state.totalstat = 0;
//       return null;
//     }
//   }

//   /**
//    * Validate chart data structure - flexible validation  
//    */
//   _validateChartData(data) {
//     if (!data) {
//       return false;
//     }

//     if (Array.isArray(data)) {
//       return true;
//     }

//     return false;
//   }

//   /**
//    * Fetch dashboard charts with proper caching
//    */
//   async fetchDashboardCharts() {
//     try {
//       this.state.loadingStates.charts = true;
      
//       const cacheKey = this._generateChartsCacheKey();
//       logDebug(`Fetching charts with cache key: ${cacheKey}`);
      
//       const cachedData = await this.serverCache.getCache(cacheKey);
      
//       if (cachedData && this._validateChartData(cachedData)) {
//         logDebug('Using cached chart data');
//         this.state.dynamic_chart = [...cachedData];
//         this.state.loadingStates.charts = false;
//         return cachedData;
//       }
      
//       logDebug('Fetching charts from server');
//       const result = await this.rpc(`/dashboard/dynamic_charts/`, {
//         cco: this.state.cco,
//         branches_id: this.state.branches_id,
//         datepicked: Number(this.state.datepicked),
//       });
      
//       if (result && this._validateChartData(result)) {
//         logDebug('Got valid chart data:', result);
//         this.state.dynamic_chart = [...result];
//         await this.serverCache.setCache(cacheKey, result);
//       } else {
//         logDebug('No valid chart data returned');
//         this.state.dynamic_chart = [];
//       }
      
//       this.state.loadingStates.charts = false;
//       return result;
//     } catch (error) {
//       console.error("Error fetching charts:", error);
//       this.state.loadingStates.charts = false;
//       this.state.dynamic_chart = [];
//       return null;
//     }
//   }

//   /**
//    * Filter by date with proper error handling
//    */
//   async filterByDate(forceRefresh = false) {
//     try {
//       if (forceRefresh) {
//         await this._clearAllUserCache();
//       }
      
//       this.state.loadingStates.stats = true;
//       this.state.loadingStates.charts = true;
      
//       // Load stats and charts in parallel
//       const [statsResult, chartsResult] = await Promise.allSettled([
//         this.getAllStats(),
//         this.fetchDashboardCharts()
//       ]);
      
//       if (statsResult.status === 'rejected') {
//         console.error('Stats loading failed:', statsResult.reason);
//       }
      
//       if (chartsResult.status === 'rejected') {
//         console.error('Charts loading failed:', chartsResult.reason);
//       }
      
//       return true;
//     } catch (error) {
//       console.error("Error in filterByDate:", error);
//       this._clearLoadingStates();
//       return false;
//     }
//   }

//   /**
//    * Clear all cache for current user
//    */
//   async _clearAllUserCache() {
//     try {
//       await this.rpc("/dashboard/cache/invalidate", {});
//       logDebug('Cleared all user cache');
//     } catch (error) {
//       console.error("Error clearing all user cache:", error);
//     }
//   }

//   /**
//    * Display by category with proper error handling
//    */
//   async displaybycategory(category) {
//     try {
//       this.state.isCategorySortingEnabled = category !== "all";
      
//       if (category === "all") {
//         await this.getAllStats();
//       } else {
//         await this.getStatsByCategory(category);
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
//       const cacheKey = `dynamic_sql_${this.state.cco}_${JSON.stringify(this.state.branches_id)}_${encodeURIComponent(query)}_${this.state.uniqueId}`;
//       e
//       let response = await this.serverCache.getCache(cacheKey);
      
//       if (!response) {
        
//         response = await this.rpc("/dashboard/dynamic_sql", { 
//           sql_query: query, 
//           branches_id: this.state.branches_id, 
//           cco: this.state.cco 
//         });
        
//         if (response && !response.error) {
//           await this.serverCache.setCache(cacheKey, response);
//         }
//       }
      
//       if (!response || response.error) {
//         console.error("Error in dynamic_sql response:", response?.error);
//         return;
//       }

//       const displayTitle = title || (category ? 
//         category.charAt(0).toUpperCase() + category.slice(1).toLowerCase() : 
//         "Card Results");
      
//       if (response.domain && Array.isArray(response.domain)) {
//         response.domain = response.domain.map(item => {
//           if (Array.isArray(item) && item.length === 3) {
//             const [field, operator, value] = item;
            
//             const isDateField = field.endsWith('_date') || 
//                                field.endsWith('_datetime') || 
//                                field === 'date' || 
//                                field === 'datetime';
            
//             if (isDateField && (value === '0001-01-01' || value === false || value === null)) {
//               return [field, '=', false];
//             }
//           }
//           return item;
//         });
//       }

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
