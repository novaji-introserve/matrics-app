/** @odoo-module */

import { registry } from "@web/core/registry";
import { useService } from "@web/core/utils/hooks";
import { Card } from "./card/card";
import { ChartRenderer } from "./chart";
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
    
    // Get current user ID
    this.userId = this.user.userId;
    logDebug('Dashboard initializing for user ID:', this.userId);
    
    // Initialize state with loading indicators and debug info
    this.state = useState({
      isCategorySortingEnabled: false,
      cco: false,
      branches_id: [],
      stats: [],
      totalstat: 0,
      datepicked: 20000,
      chartData: [],
      screenedchart: [],
      highriskchart: [],
      topbranch: [],
      dynamic_chart: [],
      scrollLeft: true,
      scrollRight: false,
      loadingStates: {
        stats: true,
        charts: true
      },
      debug: DEBUG ? {
        lastError: null,
        dataLoaded: false,
        lastUpdate: new Date().toISOString(),
        cardsVisible: false
      } : null
    });
    
    // Models that should trigger a refresh
    this.refreshModels = ['res.partner', 'res.branch'];

    // Setup bus listener for refreshing the dashboard
    useBusListener('dashboard_refresh_channel', this.handleRefreshNotification.bind(this));
    
    // Initialize component
    onWillStart(async () => {
      try {
        logDebug('Dashboard starting initialization');
        await this.getCurrentUser();
        await this.filterByDate();
        if (this.state.debug) {
          this.state.debug.dataLoaded = true;
          this.state.debug.lastUpdate = new Date().toISOString();
        }
        logDebug('Data loaded successfully', this.state.stats);
      } catch (error) {
        console.error("Error in component initialization:", error);
        if (this.state.debug) {
          this.state.debug.lastError = error.message;
        }
        this.state.loadingStates.stats = false;
        this.state.loadingStates.charts = false;
      }
    });
    
    // Set up auto-refresh every 5 minutes
    useEffect(() => {
      const refreshTimer = setInterval(async () => {
        logDebug("Auto-refreshing dashboard data...");
        await this.filterByDate(true);
        if (this.state.debug) {
          this.state.debug.lastUpdate = new Date().toISOString();
        }
      }, 5 * 60 * 1000); // 5 minutes
      
      return () => {
        clearInterval(refreshTimer);
      };
    }, () => []);
    
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
    
    // Debug effect to log state changes
    if (DEBUG) {
      useEffect(() => {
        logDebug('State updated:', 
          'stats length:', this.state.stats?.length,
          'loading:', this.state.loadingStates.stats
        );
        // Check if cards should be visible
        if (this.state.debug) {
          this.state.debug.cardsVisible = 
            this.state.stats && 
            this.state.stats.length > 0 && 
            !this.state.loadingStates.stats;
        }
      });
    }
    
    // Bind methods
    this.displayOdooView = this.displayOdooView.bind(this);
    this.displaybycategory = this.displaybycategory.bind(this);
    this.filterByDate = this.filterByDate.bind(this);
    this._onHorizontalScroll = this._onHorizontalScroll.bind(this);
  }

  /**
   * Handle notifications from the bus
   * @param {Object} notification - The notification payload
   */
  async handleRefreshNotification(notification) {
    logDebug("Received notification:", notification);

    if (notification.type === 'refresh' && this.refreshModels.includes(notification.model)) {
      // Invalidate cache
      await this.serverCache.invalidateCache();
      
      // Reload the view
      try {
        await this.filterByDate(true); // Force refresh
        if (this.state.debug) {
          this.state.debug.lastUpdate = new Date().toISOString();
        }
      } catch (error) {
        console.error("Error refreshing dashboard:", error);
        if (this.state.debug) {
          this.state.debug.lastError = error.message;
        }
      }
    }
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
   * Get current user info
   */
  async getCurrentUser() {
    try {
      logDebug('Fetching current user');
      const result = await this.rpc("/dashboard/user");
      if (result) {
        logDebug('Got user data:', result);
        this.state.branches_id = result.branch;
        this.state.cco = result.group;
      }
      return result;
    } catch (error) {
      console.error("Error fetching current user:", error);
      if (this.state.debug) {
        this.state.debug.lastError = "Failed to fetch user: " + error.message;
      }
      return null;
    }
  }

  /**
   * Get all stats with caching
   */
  async getAllStats() {
    const params = {
      cco: this.state.cco,
      branches_id: this.state.branches_id,
      datepicked: Number(this.state.datepicked),
    };
    
    logDebug('Getting all stats with params:', params);
    
    // Generate cache key
    const cacheKey = `all_stats_${this.state.cco}_${JSON.stringify(this.state.branches_id)}_${this.state.datepicked}`;
    
    try {
      // Mark as loading
      this.state.loadingStates.stats = true;
      
      // Try to get from cache
      const cachedData = await this.serverCache.getCache(cacheKey);
      
      if (cachedData) {
        logDebug('Using cached stats data');
        this.state.stats = [...cachedData.data];
        this.state.totalstat = cachedData.total;
        this.state.loadingStates.stats = false;
        return cachedData;
      }
      
      // Not in cache, fetch from server
      logDebug('Fetching stats from server');
      const result = await this.rpc(`/dashboard/stats`, params);
      
      if (result) {
        logDebug('Got stats data:', result);
        this.state.stats = [...result.data];
        this.state.totalstat = result.total;
        // Cache the result
        await this.serverCache.setCache(cacheKey, result);
      } else {
        logDebug('No stats data returned from API');
      }
      
      this.state.loadingStates.stats = false;
      return result;
    } catch (error) {
      console.error("Error fetching all stats:", error);
      if (this.state.debug) {
        this.state.debug.lastError = "Failed to fetch stats: " + error.message;
      }
      this.state.loadingStates.stats = false;
      return null;
    }
  }

  /**
   * Get stats by category with caching
   */
  async getStatsByCategory(name) {
    const params = {
      cco: this.state.cco,
      branches_id: this.state.branches_id,
      category: name,
      datepicked: Number(this.state.datepicked),
    };
    
    // Generate cache key
    const cacheKey = `stats_category_${this.state.cco}_${JSON.stringify(this.state.branches_id)}_${name}_${this.state.datepicked}`;
    
    try {
      this.state.loadingStates.stats = true;
      
      // Try to get from cache
      const cachedData = await this.serverCache.getCache(cacheKey);
      
      if (cachedData) {
        logDebug('Using cached category stats data');
        this.state.stats = cachedData.data;
        this.state.totalstat = cachedData.total;
        this.state.loadingStates.stats = false;
        return cachedData;
      }
      
      // Not in cache, fetch from server
      logDebug('Fetching category stats from server');
      const result = await this.rpc(`/dashboard/statsbycategory`, params);
      
      if (result) {
        this.state.stats = result.data;
        this.state.totalstat = result.total;
        // Cache the result
        await this.serverCache.setCache(cacheKey, result);
      }
      
      this.state.loadingStates.stats = false;
      return result;
    } catch (error) {
      console.error("Error fetching stats by category:", error);
      if (this.state.debug) {
        this.state.debug.lastError = "Failed to fetch category stats: " + error.message;
      }
      this.state.loadingStates.stats = false;
      return null;
    }
  }

  /**
   * Fetch dashboard charts with caching
   */
  async fetchDashboardCharts() {
    const params = {
      cco: this.state.cco,
      branches_id: this.state.branches_id,
      datepicked: Number(this.state.datepicked),
    };
    
    // Generate cache key
    const cacheKey = `dynamic_charts_${this.state.cco}_${JSON.stringify(this.state.branches_id)}_${this.state.datepicked}`;
    
    try {
      this.state.loadingStates.charts = true;
      
      // Try to get from cache
      const cachedData = await this.serverCache.getCache(cacheKey);
      
      if (cachedData) {
        logDebug('Using cached chart data');
        this.state.dynamic_chart = cachedData;
        this.state.loadingStates.charts = false;
        return cachedData;
      }
      
      // Not in cache, fetch from server
      logDebug('Fetching chart data from server');
      const response = await this.rpc(`/dashboard/dynamic_charts/`, params);  
      
      if (response && response.error) {
        console.error(`Error fetching dashboard charts: ${response.error}`);
        if (this.state.debug) {
          this.state.debug.lastError = "Chart error: " + response.error;
        }
        this.state.dynamic_chart = [];
      } else if (response) {
        this.state.dynamic_chart = response;
        // Cache the result
        await this.serverCache.setCache(cacheKey, response);
      } else {
        console.error("Error: Empty response received while fetching dashboard charts.");
        if (this.state.debug) {
          this.state.debug.lastError = "Empty chart response";
        }
        this.state.dynamic_chart = [];
      }
      
      this.state.loadingStates.charts = false;
      return response;
    } catch (error) {
      console.error("Error in fetchDashboardCharts:", error);
      if (this.state.debug) {
        this.state.debug.lastError = "Failed to fetch charts: " + error.message;
      }
      this.state.dynamic_chart = [];
      this.state.loadingStates.charts = false;
      return null;
    }
  }

  /**
   * Filter by date with parallel requests
   */
  async filterByDate(forceRefresh = false) {
    try {
      // If forcing refresh, invalidate cache
      if (forceRefresh) {
        await this.serverCache.invalidateCache();
      }
      
      // Reset loading states
      this.state.loadingStates = {
        stats: true,
        charts: true
      };
      
      // Using Promise.all for parallel execution
      await Promise.all([
        this.getAllStats(),
        this.fetchDashboardCharts()
      ]);
      
      if (this.state.debug) {
        this.state.debug.lastUpdate = new Date().toISOString();
      }
      
      return true;
    } catch (error) {
      console.error("Error in filterByDate:", error);
      if (this.state.debug) {
        this.state.debug.lastError = "Filter error: " + error.message;
      }
      // Clear loading states on error
      this.state.loadingStates.stats = false;
      this.state.loadingStates.charts = false;
      return false;
    }
  }

  /**
   * Display by category with proper error handling
   */
  async displaybycategory(name) {
    try {
      this.state.isCategorySortingEnabled = name !== "all";
      
      if (name === "all") {
        await this.getAllStats();
      } else {
        await this.getStatsByCategory(name);
      }
      
      if (this.state.debug) {
        this.state.debug.lastUpdate = new Date().toISOString();
      }
      
      return true;
    } catch (error) {
      console.error("Error in displayByCategory:", error);
      if (this.state.debug) {
        this.state.debug.lastError = "Category display error: " + error.message;
      }
      return false;
    }
  }

  /**
   * Display Odoo view based on query results with caching
   */
  async displayOdooView(category, query, branch_filter, branch_field, title) {
    try {
      // Generate cache key for this query
      const cacheKey = `dynamic_sql_${this.state.cco}_${JSON.stringify(this.state.branches_id)}_${query}`;
      
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
        if (response) {
          await this.serverCache.setCache(cacheKey, response);
        }
      }
      
      if (!response) {
        console.error("Empty response from dynamic_sql");
        if (this.state.debug) {
          this.state.debug.lastError = "Empty SQL response";
        }
        return;
      }

      // Create a properly formatted title in sentence case
      const displayTitle = title || (category ? 
        category.charAt(0).toUpperCase() + category.slice(1).toLowerCase() : 
        "Card Results");
      
        console.log(response.domain);
        

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
      if (this.state.debug) {
        this.state.debug.lastError = "View error: " + error.message;
      }
    }
  }
}

ComplianceDashboard.template = "owl.ComplianceDashboard";
ComplianceDashboard.components = { Card, ChartRenderer };

registry.category("actions").add("owl.compliance_dashboard", ComplianceDashboard);


// /** @odoo-module */

// import { registry } from "@web/core/registry";
// import { useService } from "@web/core/utils/hooks";
// import { Card } from "./card/card";
// import { ChartRenderer } from "./chart";
// const { Component, useState, useEffect, useRef, onMounted, onWillStart, onWillUnmount } = owl;

// // Cache service for dashboard data
// const CACHE_PREFIX = 'compliance_dashboard_';
// const CACHE_TTL = 2 * 60 * 60 * 1000; // 2 hours

// /**
//  * Cache service for persisting dashboard data with user isolation
//  */
// class CacheService {
//   constructor() {
//     this.memoryCache = {};
//   }

//   // Get user-specific prefix based on current user info
//   getUserPrefix(cco, branches_id) {
//     try {
//       // Create a unique identifier for the current user based on their
//       // cco status and branch IDs - this will be unique per user type
//       const branchKey = Array.isArray(branches_id) ? branches_id.join('-') : 'all';
//       return `${cco ? 'cco' : 'noncco'}_${branchKey}`;
//     } catch (error) {
//       console.error('Error creating user prefix:', error);
//       // Fallback to a timestamp to prevent errors
//       return `user_${Date.now()}`;
//     }
//   }

//   // Set item in both memory and localStorage with user isolation
//   setItem(key, value, userInfo, ttl = CACHE_TTL) {
//     try {
//       const userPrefix = this.getUserPrefix(userInfo.cco, userInfo.branches_id);
//       const userKey = `${CACHE_PREFIX}${userPrefix}_${key}`;
      
//       const item = {
//         value,
//         expiry: Date.now() + ttl,
//         userPrefix
//       };
      
//       // Store in memory cache
//       this.memoryCache[userKey] = item;
      
//       // Store in localStorage with expiry and user info
//       localStorage.setItem(userKey, JSON.stringify(item));
      
//       return true;
//     } catch (error) {
//       console.error('Cache set error:', error);
//       return false;
//     }
//   }

//   // Get item from cache (memory first, then localStorage) with user isolation
//   getItem(key, userInfo) {
//     try {
//       const userPrefix = this.getUserPrefix(userInfo.cco, userInfo.branches_id);
//       const userKey = `${CACHE_PREFIX}${userPrefix}_${key}`;
      
//       // Try memory cache first
//       let item = this.memoryCache[userKey];
      
//       // If not in memory, check localStorage
//       if (!item) {
//         const storedItem = localStorage.getItem(userKey);
//         if (storedItem) {
//           item = JSON.parse(storedItem);
          
//           // Verify this item belongs to the current user
//           if (item.userPrefix !== userPrefix) {
//             return null;
//           }
          
//           // Add to memory cache
//           this.memoryCache[userKey] = item;
//         }
//       }
      
//       // Check if item exists and is not expired
//       if (item && item.expiry > Date.now()) {
//         return item.value;
//       }
      
//       // Item doesn't exist or is expired
//       this.removeItem(key, userInfo);
//       return null;
//     } catch (error) {
//       console.error('Cache get error:', error);
//       return null;
//     }
//   }

//   // Remove item from both caches
//   removeItem(key, userInfo) {
//     try {
//       const userPrefix = this.getUserPrefix(userInfo.cco, userInfo.branches_id);
//       const userKey = `${CACHE_PREFIX}${userPrefix}_${key}`;
      
//       delete this.memoryCache[userKey];
//       localStorage.removeItem(userKey);
//     } catch (error) {
//       console.error('Cache remove error:', error);
//     }
//   }

//   // Generate a cache key based on parameters
//   generateKey(prefix, params) {
//     try {
//       return `${prefix}_${JSON.stringify(params)}`;
//     } catch (error) {
//       console.error('Key generation error:', error);
//       return `${prefix}_${Date.now()}`;
//     }
//   }
// }

// // Create singleton cache service
// const cacheService = new CacheService();

// /**
//  * Custom hook to manage bus service listeners
//  * @param {string} channelName - The channel to listen to
//  * @param {Function} callback - Callback function when notification received
//  */
// export function useBusListener(channelName, callback) {
//   const bus = useService("bus_service");
  
//   useEffect(
//     () => {
//       // Add the channel we want to listen to
//       bus.addChannel(channelName);
      
//       // Define the handler function
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
    
//       // Add the event listener
//       bus.addEventListener('notification', handler);
     
//       // Cleanup function
//       return () => {
//         bus.removeEventListener('notification', handler);
//         bus.deleteChannel(channelName);
//       };
//     },
//     () => [bus, channelName, callback]
//   );
// }

// /**
//  * Custom hook for dashboard data fetching with user-specific caching
//  * @param {Object} params - Parameters for data fetching
//  * @returns {Object} - Object with data and fetch functions
//  */
// function useDashboardData(params) {
//   const { rpc, state } = params;
  
//   // Data fetching methods with caching
//   const fetchData = {
//     // Get current user info
//     async getCurrentUser() {
//       try {
//         const result = await rpc("/dashboard/user");
//         if (result) {
//           state.branches_id = result.branch;
//           state.cco = result.group;
//         }
//         return result;
//       } catch (error) {
//         console.error("Error fetching current user:", error);
//         return null;
//       }
//     },

//     // Get user info object for cache keys
//     getUserInfo() {
//       return {
//         cco: state.cco,
//         branches_id: state.branches_id
//       };
//     },

//     // Get all stats with caching
//     async getAllStats() {
//       const params = {
//         cco: state.cco,
//         branches_id: state.branches_id,
//         datepicked: Number(state.datepicked),
//       };
      
//       // Generate cache key based on parameters
//       const cacheKey = cacheService.generateKey('all_stats', params);
//       const cachedData = cacheService.getItem(cacheKey, this.getUserInfo());
      
//       if (cachedData) {
//         state.stats = [...cachedData.data];
//         state.totalstat = cachedData.total;
//         return cachedData;
//       }
      
//       try {
//         const result = await rpc(`/dashboard/stats`, params);
        
//         if (result) {
//           state.stats = [...result.data];
//           state.totalstat = result.total;
//           // Cache the result
//           cacheService.setItem(cacheKey, result, this.getUserInfo());
//         }
//         return result;
//       } catch (error) {
//         console.error("Error fetching all stats:", error);
//         return null;
//       }
//     },

//     // Get stats by category with caching
//     async getStatsByCategory(name) {
//       const params = {
//         cco: state.cco,
//         branches_id: state.branches_id,
//         category: name,
//         datepicked: Number(state.datepicked),
//       };
      
//       // Generate cache key
//       const cacheKey = cacheService.generateKey('stats_by_category', params);
//       const cachedData = cacheService.getItem(cacheKey, this.getUserInfo());
      
//       if (cachedData) {
//         state.stats = cachedData.data;
//         state.totalstat = cachedData.total;
//         return cachedData;
//       }
      
//       try {
//         const result = await rpc(`/dashboard/statsbycategory`, params);
        
//         if (result) {
//           state.stats = result.data;
//           state.totalstat = result.total;
//           // Cache the result
//           cacheService.setItem(cacheKey, result, this.getUserInfo());
//         }
//         return result;
//       } catch (error) {
//         console.error("Error fetching stats by category:", error);
//         return null;
//       }
//     },

//     // Fetch dashboard charts with caching
//     async fetchDashboardCharts() {
//       const params = {
//         cco: state.cco,
//         branches_id: state.branches_id,
//         datepicked: Number(state.datepicked),
//       };
      
//       // Generate cache key
//       const cacheKey = cacheService.generateKey('dynamic_charts', params);
//       const cachedData = cacheService.getItem(cacheKey, this.getUserInfo());
      
//       if (cachedData) {
//         state.dynamic_chart = cachedData;
//         return cachedData;
//       }
      
//       try {
//         const response = await rpc(`/dashboard/dynamic_charts/`, params);  
        
//         if (response && response.error) {
//           console.error(`Error fetching dashboard charts: ${response.error}`);
//           state.dynamic_chart = [];
//         } else if (response) {
//           state.dynamic_chart = response;
//           // Cache the result
//           cacheService.setItem(cacheKey, response, this.getUserInfo());
//         } else {
//           console.error("Error: Empty response received while fetching dashboard charts.");
//           state.dynamic_chart = [];
//         }
        
//         return response;
//       } catch (error) {
//         console.error("Error in fetchDashboardCharts:", error);
//         state.dynamic_chart = [];
//         return null;
//       }
//     },

//     // Filter by date with parallel requests
//     async filterByDate(forceRefresh = false) {
//       // If forcing refresh, clear relevant caches
//       if (forceRefresh) {
//         const keysToRemove = [
//           'all_stats',
//           'dynamic_charts'
//         ];
        
//         keysToRemove.forEach(key => {
//           const cacheKey = cacheService.generateKey(key, {
//             cco: state.cco,
//             branches_id: state.branches_id,
//             datepicked: Number(state.datepicked),
//           });
//           cacheService.removeItem(cacheKey, this.getUserInfo());
//         });
//       }
      
//       try {
//         // Using Promise.all for parallel execution
//         await Promise.all([
//           this.getAllStats(),
//           this.fetchDashboardCharts()
//         ]);
        
//         return true;
//       } catch (error) {
//         console.error("Error in filterByDate:", error);
//         return false;
//       }
//     },

//     // Display by category
//     async displayByCategory(name) {
//       state.isCategorySortingEnabled = name !== "all";
      
//       try {
//         if (name === "all") {
//           await this.getAllStats();
//         } else {
//           await this.getStatsByCategory(name);
//         }
//         return true;
//       } catch (error) {
//         console.error("Error in displayByCategory:", error);
//         return false;
//       }
//     }
//   };

//   return fetchData;
// }

// export class ComplianceDashboard extends Component {
//   setup() {
//     this.left_indicator = useRef("left");
//     this.api = useService("orm");
//     this.rpc = useService("rpc");
//     this.navigate = useService("action");
    
//     // Initialize state
//     this.state = useState({
//       isCategorySortingEnabled: false,
//       cco: false,
//       branches_id: [],
//       stats: [],
//       totalstat: 0,
//       datepicked: 20000,
//       chartData: [],
//       screenedchart: [],
//       highriskchart: [],
//       topbranch: [],
//       dynamic_chart: [],
//       isLoading: true,       // Loading state
//       scrollLeft: true,
//       scrollRight: false
//     });
    
//     // Models that should trigger a refresh
//     this.refreshModels = ['res.partner', 'res.branch'];

//     // Setup bus listener for refreshing the dashboard
//     useBusListener('dashboard_refresh_channel', this.handleRefreshNotification.bind(this));
    
//     // Setup dashboard data
//     this.dashboardData = useDashboardData({
//       rpc: this.rpc,
//       state: this.state
//     });

//     // Initialize component
//     onWillStart(async () => {
//       this.state.isLoading = true;
//       try {
//         await this.dashboardData.getCurrentUser();
//         await this.dashboardData.filterByDate();
//       } catch (error) {
//         console.error("Error in component initialization:", error);
//       } finally {
//         this.state.isLoading = false;
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
//    * Handle notifications from the bus
//    * @param {Object} notification - The notification payload
//    */
//   async handleRefreshNotification(notification) {
//     console.log("Received notification:", notification);

//     if (notification.type === 'refresh' && this.refreshModels.includes(notification.model)) {
//       // Reload the view
//       this.state.isLoading = true;
//       try {
//         await this.dashboardData.filterByDate(true); // Force refresh
//         this.render();
//       } catch (error) {
//         console.error("Error refreshing dashboard:", error);
//       } finally {
//         this.state.isLoading = false;
//       }
//     }
//   }

//   /**
//    * Handle horizontal scroll events
//    */
//   _onHorizontalScroll(event) {
//     const container = event.target || event.currentTarget;
//     if (!container) return;

//     const atRight = container.scrollLeft + container.clientWidth >= container.scrollWidth - 5;
//     const atLeft = container.scrollLeft <= 5;

//     // Get user prefix for storage keys
//     const userPrefix = this.state.cco ? 'cco' : 'noncco';
    
//     if (atRight) {
//       this.state.scrollRight = true;
//       this.state.scrollLeft = false;
//       localStorage.setItem(`${userPrefix}_scroll_right`, "true");
//       localStorage.setItem(`${userPrefix}_scroll_left`, "false");
//     } else if (atLeft) {
//       this.state.scrollLeft = true;
//       this.state.scrollRight = false;
//       localStorage.setItem(`${userPrefix}_scroll_left`, "true");
//       localStorage.setItem(`${userPrefix}_scroll_right`, "false");
//     } else {
//       this.state.scrollLeft = false;
//       this.state.scrollRight = false;
//       localStorage.setItem(`${userPrefix}_scroll_left`, "false");
//       localStorage.setItem(`${userPrefix}_scroll_right`, "false");
//     }
//   }

//   /**
//    * Display by category with proper error handling
//    */
//   async displaybycategory(name) {
//     this.state.isLoading = true;
//     try {
//       await this.dashboardData.displayByCategory(name);
//     } catch (error) {
//       console.error("Error in displayByCategory:", error);
//     } finally {
//       this.state.isLoading = false;
//     }
//   }
  
//   /**
//    * Filter by date with proper error handling
//    */
//   async filterByDate() {
//     this.state.isLoading = true;
//     try {
//       await this.dashboardData.filterByDate(true); // Force refresh on date change
//     } catch (error) {
//       console.error("Error in filterByDate:", error);
//     } finally {
//       this.state.isLoading = false;
//     }
//   }

//   /**
//    * Display Odoo view based on query results with caching
//    */
//   async displayOdooView(category, query, branch_filter, branch_field, title) {
//     this.state.isLoading = true;
//     try {
//       // Generate cache key for this query
//       const cacheKey = cacheService.generateKey('dynamic_sql', {
//         query,
//         branches_id: this.state.branches_id,
//         cco: this.state.cco
//       });
      
//       // Check cache
//       let response = cacheService.getItem(cacheKey, 
//         { cco: this.state.cco, branches_id: this.state.branches_id }
//       );
      
//       if (!response) {
//         // Cache miss, fetch from server
//         response = await this.rpc("/dashboard/dynamic_sql", { 
//           sql_query: query, 
//           branches_id: this.state.branches_id, 
//           cco: this.state.cco 
//         });
        
//         // Cache the result
//         if (response) {
//           cacheService.setItem(cacheKey, response, 
//             { cco: this.state.cco, branches_id: this.state.branches_id }
//           );
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
//     } finally {
//       this.state.isLoading = false;
//     }
//   }
// }

// ComplianceDashboard.template = "owl.ComplianceDashboard";
// ComplianceDashboard.components = { Card, ChartRenderer };

// registry.category("actions").add("owl.compliance_dashboard", ComplianceDashboard);





// /** @odoo-module */

// import { registry } from "@web/core/registry";
// import { useService } from "@web/core/utils/hooks";
// import { Card } from "./card/card";
// import { ChartRenderer } from "./chart";
// const { Component, useState, useEffect, useRef, onMounted, onWillStart } = owl;


// export function useBusListener(channelName, callback) {
//   const bus = useService("bus_service");
  
//   useEffect(
//     () => {
//       // Add the channel we want to listen to
//       bus.addChannel(channelName);
      
//       // Define the handler function
//       const handler = (ev) => {
//         const notifications = ev.detail;
//         for (const notification of notifications) {
          
//           // Check if this is a 3-part notification with the channel we care about
//           if (Array.isArray(notification) && notification[0] === channelName) {
//             // Pass the message (third parameter) to the callback
//             callback(notification[2]);

//           }else if(!Array.isArray(notification) && notification.payload && notification.payload.channelName === channelName){
//             callback(notification.payload);
//           }
//         }
//       };
    
//       // Add the event listener
//       bus.addEventListener('notification', handler);

     
//       // Cleanup function
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
//     this.state = useState({
//       isCategorySortingEnabled: false,
//       cco: false,
//       branches_id: [],
//       stats: [],
//       totalstat: 0,
//       datepicked: 20000,
//       chartData: [],
//       scrollLeft: sessionStorage.getItem("user_scroll_left")
//         ? sessionStorage.getItem("user_scroll_left")
//         : true,
//       scrollRight: sessionStorage.getItem("user_scroll_right")
//         ? sessionStorage.getItem("user_scroll_right")
//         : false,
//       screenedchart: [],
//       highriskchart: [],
//       topbranch: [],
//       dynamic_chart: []
//     });

//     // Models that should trigger a refresh
//     this.refreshModels = ['res.partner', 'res.branch'];

//      // Setup bus listener for refreshing the dashboard
//     useBusListener('dashboard_refresh_channel', this.handleRefreshNotification.bind(this));
    
    


//     useEffect(() => {
      
//         let cardContainer = document.querySelector(".card-container");
//         if(cardContainer){

//           cardContainer.addEventListener("scroll",this._onHorizontalScroll.bind(this));
//         }
//          return () => {
//            if (cardContainer) {
//              cardContainer.removeEventListener(
//                "scroll",
//                this._onHorizontalScroll
//              );
//            }
//          };

//       },
//       () => []
//     );

//     onWillStart(async () => {
//       await this.getcurrentuser();
//       await this.filterByDate();
//     });

//     this.displayOdooView = this.displayOdooView.bind(this); // Bind the function!
    
//   }

//     /**
//      * Handle notifications from the bus
//      * @param {Object} notification - The notification payload
//      */
//     async handleRefreshNotification(notification) {
//       console.log("Received notification:", notification);
  
//       if (notification.type === 'refresh' && this.refreshModels.includes(notification.model)) {
//           // Reload the view
//           await this.filterByDate();
//           this.render();
//       }
//   }

//   _onHorizontalScroll = () => {
//     const container = document.querySelector(".card-container");

//     if (!container) {
//       return; // Container might not be available yet
//     }

//     const atRight =
//       container.scrollLeft + container.clientWidth >= container.scrollWidth - 5; // -5 buffer

//     const atLeft = container.scrollLeft <= 5; // Left end

//     if (atRight && !sessionStorage.getItem("user_scroll_left")) {
//       this.state.scrollRight = true;
//       this.state.scrollLeft = false;
//       sessionStorage.setItem("user_scroll_right", true)
//     }else{
//       this.state.scrollLeft = true;
//     } 
    
//     if(atLeft){
//       this.state.scrollLeft = true;
//       sessionStorage.setItem("user_scroll_left", true);
//     }
//   };

//   async displayOdooView(category, query, branch_filter, branch_field, title) {
    
//     const response = await this.rpc("/dashboard/dynamic_sql", { sql_query: query, branches_id: this.state.branches_id, cco: this.state.cco });          
        
//     if(!response) return;

//     // Create a properly formatted title in sentence case
//     const displayTitle = title || (category ? 
//       category.charAt(0).toUpperCase() + category.slice(1).toLowerCase() : 
//       "Card Results");

//     console.log(response.domain);
    
//     this.navigate.doAction({
//       type: "ir.actions.act_window",
//       res_model: response.table.replace(/_/g, "."),
//       name: displayTitle,
//       // name: `${category[0].toUpperCase()}${category.slice(1,)}`,
//       domain: response.domain,
//       views: [
//         [false, "tree"],
//         [false, "form"],
//       ],
//     });

    
//   }
//   async getcurrentuser() {
//     let result = await this.rpc("/dashboard/user");

//     console.log(result);
    
    
//     this.state.branches_id = result.branch;
//     this.state.cco = result.group;
//   }
//   async getAllStats() {
//     let result = await this.rpc(`/dashboard/stats`, {
//       cco: this.state.cco,
//       branches_id: this.state.branches_id,
//       datepicked: Number(this.state.datepicked),
//     });
    
//     this.state.stats = [...result.data];
//     this.state.totalstat = result.total;
//   }
//   async getAllStatsByCategory(name) {
//     let result = await this.rpc(`/dashboard/statsbycategory`, {
//       cco: this.state.cco,
//       branches_id: this.state.branches_id,
//       category: name,
//       datepicked: Number(this.state.datepicked),
//     });
    

//     this.state.stats = result.data;
//     this.state.totalstat = result.total;
//   }

//   displaybycategory = async (name) => {

//     this.state.isCategorySortingEnabled = false;

//     if (name == "all") {
//       await this.getAllStats();
//       this.state.isCategorySortingEnabled = false;
//     } else {
//       this.state.isCategorySortingEnabled = true;
//       await this.getAllStatsByCategory(name);
//     }
//   };

//   filterByDate = async () => {
//     await this.getAllStats();
//     // await this.fetchScreenedChart();
//     // await this.TopBranches();
//     // await this.highRiskBranches();
//     await this.fetchDashboardCharts()
//   };


//   // async fetchScreenedChart() {
//   //   const response = await this.rpc("/dashboard/get_top_screened_rules", {
//   //     cco: this.state.cco,
//   //     branches_id: this.state.branches_id,
//   //     datepicked: Number(this.state.datepicked),
//   //   });  

   
//   //   this.state.screenedchart = response
    

//   // }
//   // async TopBranches() {
//   //   const response = await this.rpc("/dashboard/get_branch_by_customer", {
//   //     cco: this.state.cco,
//   //     branches_id: this.state.branches_id,
//   //     datepicked: Number(this.state.datepicked),
//   //   });  


    

//   //   this.state.topbranch = response

//   // }
//   // async highRiskBranches() {
//   //   const response = await this.rpc("/dashboard/get_high_risk_customer_by_branch",
//   //     {
//   //       cco: this.state.cco,
//   //       branches_id: this.state.branches_id,
//   //       datepicked: Number(this.state.datepicked),
//   //     }
//   //   );  
    
    
//   //   this.state.highriskchart = response
    
//   // }
  
//   async fetchDashboardCharts(){
    
//     const response = await this.rpc(`/dashboard/dynamic_charts/`,
//       {
//         cco: this.state.cco,
//         branches_id: this.state.branches_id,
//         datepicked: Number(this.state.datepicked),
//       }
//     );  
    

//     if (response && response.error) {
//       alert(`Error fetching dashboard charts: ${response.error}`);
//       this.state.dynamic_chart = []; // Or some other appropriate error state
//     } else if (response) {
//       this.state.dynamic_chart = response;
//     } else {
//       alert("Error: Empty response received while fetching dashboard charts.");
//       this.state.dynamic_chart = [];
//     }
    
      
//     }
    
//   }


// ComplianceDashboard.template = "owl.ComplianceDashboard";
// ComplianceDashboard.components = { Card, ChartRenderer };

// registry.category("actions").add("owl.compliance_dashboard", ComplianceDashboard);
