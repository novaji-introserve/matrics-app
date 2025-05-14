/** @odoo-module */

const { Component, onWillStart, useRef, useEffect, onWillUnmount, useState } = owl;
import { useService } from "@web/core/utils/hooks";

// Constants
const CHARTJS_CDN = "https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js";
const ANIMATION_DELAY = 50; // Milliseconds delay between adding each element
const MAX_INITIAL_ITEMS = 10; // Show these items immediately, then animate the rest

/**
 * Chart renderer component with progressive loading animation
 */
export class ChartRenderer extends Component {
  setup() {
    this.navigate = useService("action");
    this.chartRef = useRef("compliance_chart");
    this.myChartInstance = null;
    
    // Loading animation timeout IDs
    this.animationTimeouts = [];
    
    // State for progressive loading
    this.state = useState({
      isLibraryLoading: true,
      isInitializing: false,
      isAnimating: false,
      error: null,
      loadedElements: 0,
      totalElements: 0,
      emptyChart: false
    });
    
    onWillStart(async () => {
      // Check if Chart.js is already loaded
      if (typeof Chart === 'undefined') {
        try {
          await loadJS(CHARTJS_CDN);
        } catch (error) {
          console.error("Failed to load Chart.js:", error);
          this.state.error = "Failed to load chart library";
        }
      }
      this.state.isLibraryLoading = false;
    });
    
    // When data is available and library is loaded, initialize chart
    useEffect(
      () => {
        if (!this.state.isLibraryLoading && this.props.data && !this.myChartInstance) {
          this.initializeChart();
        }
      },
      () => [this.state.isLibraryLoading, this.props.data]
    );
    
    // Cleanup on unmount
    onWillUnmount(() => {
      this.destroyChart();
      this.clearAllAnimationTimeouts();
    });
  }
  
  /**
   * Initialize the chart and start progressive loading
   */
  initializeChart() {
    if (!this.props.data || !this.chartRef.el) {
      return;
    }
    
    this.state.isInitializing = true;
    
    try {
      // Check if the chart has data
      const labels = this.props.data.labels || [];
      const datasets = this.props.data.datasets || [];
      
      this.state.totalElements = labels.length;
      
      // Handle empty charts
      if (labels.length === 0 || datasets.length === 0 || 
          (datasets[0] && datasets[0].data && datasets[0].data.length === 0)) {
        this.state.emptyChart = true;
        this.createEmptyChart();
        this.state.isInitializing = false;
        return;
      }
      
      // Create the chart with initial batch of data
      this.createProgressiveChart();
      
      // Start animating the rest of the elements after a short delay
      setTimeout(() => this.animateRemainingElements(), 100);
      
    } catch (error) {
      console.error("Error initializing chart:", error);
      this.state.error = "Failed to initialize chart";
    }
    
    this.state.isInitializing = false;
  }
  
  /**
   * Create an empty chart when no data is available
   */
  createEmptyChart() {
    if (!this.chartRef.el) return;
    
    // Destroy any existing chart
    this.destroyChart();
    
    try {
      const chartType = this.props.data?.type || this.props.type || 'bar';
      
      this.myChartInstance = new Chart(this.chartRef.el, {
        type: chartType,
        data: {
          labels: [],
          datasets: [{
            data: [],
            backgroundColor: [],
            borderColor: [],
            borderWidth: 1
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: true,
          scales: {
            y: {
              beginAtZero: true,
              ticks: {
                callback: function(value) {
                  return value;
                }
              }
            }
          },
          plugins: {
            legend: {
              display: false
            },
            title: {
              display: true,
              text: this.props.data?.title || this.props.title || '',
              position: 'bottom'
            },
            subtitle: {
              display: true,
              text: 'No data available',
              padding: {
                bottom: 10
              }
            }
          }
        }
      });
    } catch (error) {
      console.error("Error creating empty chart:", error);
      this.state.error = "Failed to create chart";
    }
  }
  
  /**
   * Create chart with initial batch of data for progressive animation
   */
  createProgressiveChart() {
    if (!this.chartRef.el || !this.props.data) return;
    
    // Destroy any existing chart
    this.destroyChart();
    
    try {
      const chartType = this.props.data.type || this.props.type || 'bar';
      const sourceData = this.props.data;
      
      // Get initial batch of data (first few items for immediate display)
      const initialCount = Math.min(MAX_INITIAL_ITEMS, sourceData.labels.length);
      
      // Prepare initial datasets
      const initialDatasets = sourceData.datasets.map(dataset => {
        const initialData = dataset.data.slice(0, initialCount);
        const initialColors = Array.isArray(dataset.backgroundColor) ? 
          dataset.backgroundColor.slice(0, initialCount) : 
          dataset.backgroundColor;
        
        let initialBorderColors;
        if (chartType === 'line') {
          // For line charts, use the entire borderColor (usually a single color)
          initialBorderColors = dataset.borderColor;
        } else if (Array.isArray(dataset.borderColor)) {
          // For charts with borderColor arrays, get the initial batch
          initialBorderColors = dataset.borderColor.slice(0, initialCount);
        } else {
          initialBorderColors = [];
        }
        
        return {
          ...dataset,
          data: initialData,
          backgroundColor: initialColors,
          borderColor: initialBorderColors,
          borderWidth: dataset.borderWidth || 1
        };
      });
      
      // Create chart with initial batch
      this.myChartInstance = new Chart(this.chartRef.el, {
        type: chartType,
        data: {
          labels: sourceData.labels.slice(0, initialCount),
          datasets: initialDatasets
        },
        options: {
          onClick: (event, elements) => this.handleChartClick(event, elements),
          responsive: true,
          maintainAspectRatio: true,
          animation: {
            duration: 400, // Shorter animations for better performance
          },
          scales: {
            y: {
              beginAtZero: true,
              ticks: {
                callback: function(value) {
                  return value;
                }
              }
            }
          },
          plugins: {
            legend: {
              display: chartType !== 'bar',
              position: 'top'
            },
            title: {
              display: true,
              text: sourceData.title || this.props.title || '',
              position: 'bottom'
            },
            tooltip: {
              enabled: true
            },
            subtitle: {
              display: true,
              text: initialCount < sourceData.labels.length ? 
                    `Loading chart elements...` : 
                    `Loaded ${initialCount} items`,
              padding: {
                bottom: 10
              }
            }
          }
        }
      });
      
      // Update loaded elements count
      this.state.loadedElements = initialCount;
      
    } catch (error) {
      console.error("Error creating progressive chart:", error);
      this.state.error = "Failed to create chart";
    }
  }
  
  /**
   * Animate the remaining chart elements one by one
   */
  animateRemainingElements() {
    if (!this.myChartInstance || !this.props.data || 
        this.state.loadedElements >= this.state.totalElements) {
      return;
    }
    
    this.state.isAnimating = true;
    
    // Clear any existing animation timeouts
    this.clearAllAnimationTimeouts();
    
    // Get the source data
    const sourceData = this.props.data;
    const chartType = sourceData.type || 'bar';
    
    // Start from where we left off
    const startIndex = this.state.loadedElements;
    
    // Animate each remaining element with a delay
    for (let i = startIndex; i < sourceData.labels.length; i++) {
      const timeoutId = setTimeout(() => {
        // Add the label
        this.myChartInstance.data.labels.push(sourceData.labels[i]);
        
        // Add data for each dataset
        sourceData.datasets.forEach((dataset, datasetIndex) => {
          // Add data point
          this.myChartInstance.data.datasets[datasetIndex].data.push(dataset.data[i]);
          
          // Add colors based on chart type and color format
          if (Array.isArray(dataset.backgroundColor)) {
            // Array of colors (common for pie/bar charts)
            if (i < dataset.backgroundColor.length) {
              this.myChartInstance.data.datasets[datasetIndex].backgroundColor.push(
                dataset.backgroundColor[i]
              );
            }
          } else if (dataset.backgroundColor && !this.myChartInstance.data.datasets[datasetIndex].backgroundColor) {
            // Single color (common for line charts)
            this.myChartInstance.data.datasets[datasetIndex].backgroundColor = dataset.backgroundColor;
          }
          
          // Handle border color based on chart type
          if (chartType === 'line') {
            // For line charts, borderColor is usually a single color
            if (dataset.borderColor && !this.myChartInstance.data.datasets[datasetIndex].borderColor) {
              this.myChartInstance.data.datasets[datasetIndex].borderColor = dataset.borderColor;
            }
          } else if (Array.isArray(dataset.borderColor) && i < dataset.borderColor.length) {
            // For other charts that use borderColor arrays
            this.myChartInstance.data.datasets[datasetIndex].borderColor.push(
              dataset.borderColor[i]
            );
          }
        });
        
        // Update loaded count
        this.state.loadedElements = i + 1;
        
        // Update subtitle to show progress
        if (this.myChartInstance.options.plugins.subtitle) {
          const percent = Math.round((this.state.loadedElements / this.state.totalElements) * 100);
          this.myChartInstance.options.plugins.subtitle.text = 
            `Loading: ${percent}% (${this.state.loadedElements} of ${this.state.totalElements})`;
        }
        
        // Update the chart
        this.myChartInstance.update();
        
        // Check if animation is complete
        if (i === sourceData.labels.length - 1) {
          this.state.isAnimating = false;
          
          // Update subtitle to show completion
          if (this.myChartInstance.options.plugins.subtitle) {
            this.myChartInstance.options.plugins.subtitle.text = 
              `Loaded ${this.state.loadedElements} items`;
            this.myChartInstance.update();
          }
        }
      }, (i - startIndex) * ANIMATION_DELAY);
      
      this.animationTimeouts.push(timeoutId);
    }
  }
  
  /**
   * Clear all animation timeouts
   */
  clearAllAnimationTimeouts() {
    this.animationTimeouts.forEach(timeoutId => clearTimeout(timeoutId));
    this.animationTimeouts = [];
  }
  
  /**
   * Handle chart click events
   */
  handleChartClick(event, elements) {
    if (!elements || elements.length === 0 || !this.props.data) return;
    
    const clickedIndex = elements[0].index;
    const chartData = this.props.data;
    
    // Get the data needed for domain construction
    const modelName = chartData.model_name;
    const filterColumn = chartData.filter;
    const filterID = chartData.ids?.[clickedIndex];
    
    if (!modelName || !filterColumn || filterID === undefined) {
      console.warn("Missing data for chart click action");
      return;
    }
    
    let dateField = "";
    const splitDateField = chartData.datefield?.split(".") || [];

    if(splitDateField.length > 1) {
      dateField = splitDateField[1];
    } else {
      dateField = chartData.datefield || '';
    }
    
    let domain = [[filterColumn, "=", filterID], ...(chartData.domain_filter || [])];
    
    if (this.props.date > 0) {
      const today = new Date();
      const prevDate = new Date();
      prevDate.setDate(today.getDate() - this.props.date);
      
      const formatDate = date => {
        return date.toISOString().split('T')[0];
      };
      
      const odooPrevDate = `${formatDate(prevDate)} 00:00:00`;
      const odooCurrentDate = `${formatDate(today)} 23:59:59`;
      
      domain.push([dateField, ">=", odooPrevDate]);
      domain.push([dateField, "<=", odooCurrentDate]);
    }

    const selectedLabel = chartData.labels[clickedIndex] || "Unknown";
    
    const chartTitle = chartData.title ? 
      chartData.title.charAt(0).toUpperCase() + chartData.title.slice(1).toLowerCase() : 
      "Chart results";
      
    const displayTitle = `${chartTitle} - ${selectedLabel}`;
     
    let action = {
      type: "ir.actions.act_window",
      name: displayTitle,
      res_model: modelName,
      domain: domain,
      views: [
        [false, "tree"],
        [false, "form"],
      ],
    };

    this.navigate.doAction(action);
  }
  
  /**
   * Destroy existing chart instance
   */
  destroyChart() {
    if (this.myChartInstance) {
      try {
        this.myChartInstance.destroy();
      } catch (error) {
        console.error("Error destroying chart:", error);
      }
      this.myChartInstance = null;
    }
  }
}

ChartRenderer.template = "owl.ChartRender";
ChartRenderer.props = {
  admin: { type: Boolean, optional: true },
  branches_id: { type: Array, optional: true },
  date: { type: Number, optional: true, default: 0 },
  type: { type: String, optional: true, default: 'bar' },
  title: { type: String, optional: true },
  data: { type: Object, optional: true },
  dynamic: { type: Boolean, optional: true, default: true }, // Add this line
  isLoading: { type: Boolean, optional: true, default: false }
};


























// /** @odoo-module */

// const { Component, onWillStart, useRef, useEffect, onWillUnmount } = owl;
// import { useService } from "@web/core/utils/hooks";

// // Constants
// const CHARTJS_CDN = "https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js";

// /**
//  * Chart renderer component with progressive rendering for large datasets
//  */
// export class ChartRenderer extends Component {
//   setup() {
//     this.navigate = useService("action");
//     this.chartRef = useRef("compliance_chart");
//     this.myChartInstance = null;
//     this.renderedItems = 0; // Track how many elements we've rendered

//     // Local state for progressive loading
//     this.state = {
//       isLibraryLoading: true,
//       isDataRendering: false,
//       progress: 0,
//       totalItems: 0,
//       currentRenderingItem: null
//     };

//     onWillStart(async () => {
//       // Load Chart.js
//       if (typeof Chart === 'undefined') {
//         await loadJS(CHARTJS_CDN);
//       }
//       this.state.isLibraryLoading = false;
//     });

//     useEffect(
//       () => {
//         // If chart library is loaded and we have data, start progressive rendering
//         if (!this.state.isLibraryLoading && this.props.data && !this.myChartInstance) {
//           this.startProgressiveRendering();
//         }
//       },
//       () => [this.props.data, this.state.isLibraryLoading]
//     );

//     onWillUnmount(() => {
//       this.destroyChart();
//     });
//   }

//   /**
//    * Start the progressive rendering process
//    */
//   startProgressiveRendering() {
//     if (!this.props.data || !this.chartRef.el) return;
    
//     // Clean up any existing chart
//     this.destroyChart();
    
//     // Initialize with empty data sets
//     this.state.isDataRendering = true;
    
//     // Determine what type of chart and how many items
//     const chartType = this.props.data.type || 'bar';
//     const labels = this.props.data.labels || [];
//     const datasets = this.props.data.datasets || [];
    
//     this.state.totalItems = labels.length;
//     this.renderedItems = 0;
    
//     // Create chart with empty datasets
//     const emptyDatasets = datasets.map(dataset => ({
//       ...dataset,
//       data: new Array(labels.length).fill(0) // Start with zeros
//     }));
    
//     // Create initial chart with empty data
//     this.myChartInstance = new Chart(this.chartRef.el, {
//       type: chartType,
//       data: {
//         labels: labels,
//         datasets: emptyDatasets
//       },
//       options: {
//         animation: {
//           duration: 800, // Faster animations
//         },
//         onClick: this.createClickHandler(),
//         responsive: true,
//         maintainAspectRatio: true,
//         scales: {
//           y: {
//             ticks: {
//               stepSize: 100,
//               callback: function (value) {
//                 return value;
//               },
//             },
//           },
//         },
//         plugins: {
//           legend: {
//             display: chartType === "bar" || chartType === "line" || chartType === "radar" ? false : true,
//             position: "top",
//           },
//           title: {
//             display: true,
//             position: "bottom",
//           },
//         },
//       },
//     });
    
//     // Now start adding data progressively
//     this.progressivelyAddData();
//   }
  
//   /**
//    * Progressively add data to the chart
//    */
//   progressivelyAddData() {
//     if (!this.myChartInstance || !this.props.data || this.renderedItems >= this.state.totalItems) {
//       this.state.isDataRendering = false;
//       return;
//     }
    
//     // Update progress state
//     this.state.progress = Math.round((this.renderedItems / this.state.totalItems) * 100);
//     this.state.currentRenderingItem = this.props.data.labels[this.renderedItems];
    
//     // For each dataset, add the real data for the current item
//     this.props.data.datasets.forEach((dataset, datasetIndex) => {
//       if (dataset.data && dataset.data[this.renderedItems] !== undefined) {
//         // Update just this data point
//         this.myChartInstance.data.datasets[datasetIndex].data[this.renderedItems] = 
//           dataset.data[this.renderedItems];
//       }
//     });
    
//     // Increment counter
//     this.renderedItems++;
    
//     // Update the chart
//     this.myChartInstance.update();
    
//     // Schedule the next item with a slight delay
//     setTimeout(() => {
//       this.progressivelyAddData();
//     }, 100); // Add one item every 100ms
//   }

//   /**
//    * Destroy existing chart instance
//    */
//   destroyChart() {
//     if (this.myChartInstance) {
//       try {
//         this.myChartInstance.destroy();
//       } catch (error) {
//         console.error("Error destroying chart:", error);
//       }
//       this.myChartInstance = null;
//     }
//   }

//   /**
//    * Create click handler function for chart
//    */
//   createClickHandler() {
//     return (event, elements) => {
//       if (!elements || elements.length === 0 || !this.props.data) return;

//       const clickedIndex = elements[0].index;
//       const chartData = this.props.data;
//       const modelName = chartData.model_name;
//       const filterColumn = chartData.filter;
//       const filterID = chartData.ids?.[clickedIndex];
      
//       if (!modelName || !filterColumn || filterID === undefined) {
//         console.warn("Missing data for chart click action");
//         return;
//       }
      
//       let dateField = "";
//       let splitDateField = chartData.datefield?.split(".") || [];

//       if(splitDateField.length > 1) {
//         dateField = splitDateField[1];
//       } else {
//         dateField = chartData.datefield || '';
//       }
      
//       let domain = [[filterColumn, "=", filterID], ...(chartData.domain_filter || [])];
      
//       if (this.props.date > 0) {
//         const today = new Date();
//         const prevDate = new Date();
//         prevDate.setDate(today.getDate() - this.props.date);
        
//         const formatDate = date => {
//           return date.toISOString().split('T')[0];
//         };
        
//         const odooPrevDate = `${formatDate(prevDate)} 00:00:00`;
//         const odooCurrentDate = `${formatDate(today)} 23:59:59`;
        
//         domain.push([dateField, ">=", odooPrevDate]);
//         domain.push([dateField, "<=", odooCurrentDate]);
//       }

//       const selectedLabel = chartData.labels?.[clickedIndex] || "Unknown";
      
//       const chartTitle = chartData.title ? 
//         chartData.title.charAt(0).toUpperCase() + chartData.title.slice(1).toLowerCase() : 
//         "Chart results";
        
//       const displayTitle = `${chartTitle} - ${selectedLabel}`;
       
//       let action = {
//         type: "ir.actions.act_window",
//         name: displayTitle,
//         res_model: modelName,
//         domain: domain,
//         views: [
//           [false, "tree"],
//           [false, "form"],
//         ],
//       };

//       this.navigate.doAction(action);
//     };
//   }
// }

// ChartRenderer.template = "owl.ChartRender";
// ChartRenderer.props = {
//   admin: { type: Boolean, optional: true },
//   branches_id: { type: Array, optional: true },
//   date: { type: Number, optional: true, default: 0 },
//   type: { type: String, optional: true, default: 'bar' },
//   title: { type: String, optional: true },
//   data: { type: Object, optional: true },
//   dynamic: { type: Boolean, optional: true, default: false },
//   isLoading: { type: Boolean, optional: true, default: false }
// };




















// /** @odoo-module */

// const { Component, onWillStart, useRef, useEffect, onWillUnmount } = owl;
// import { useService } from "@web/core/utils/hooks";

// /**
//  * Chart renderer component
//  */
// export class ChartRenderer extends Component {
//   setup() {
//     this.navigate = useService("action");
//     this.chartRef = useRef("compliance_chart");
//     this.myChartInstance = null;

//     // No need to load Chart.js from CDN
//     // It's already included in the assets bundle
    
//     useEffect(
//       () => {
//         // Only render chart if data exists and is not loading
//         if (this.props.data && !this.props.isLoading) {
//           // Add small delay to prevent blocking the main thread
//           setTimeout(() => this.renderChart(), 50);
//         }
//       },
//       () => [this.props.data, this.props.type, this.props.title, this.props.isLoading]
//     );

//     onWillUnmount(() => {
//       this.destroyChart();
//     });
//   }

//   /**
//    * Destroy existing chart instance
//    */
//   destroyChart() {
//     if (this.myChartInstance) {
//       try {
//         this.myChartInstance.destroy();
//       } catch (error) {
//         console.error("Error destroying chart:", error);
//       }
//       this.myChartInstance = null;
//     }
//   }

//   /**
//    * Render the appropriate chart type
//    */
//   renderChart() {
//     try {
//       this.destroyChart();
      
//       // Verify Chart.js is available (it should be since we bundled it)
//       if (typeof Chart === 'undefined') {
//         console.error('Chart.js not loaded in bundle');
//         this.renderErrorMessage('Chart library not available');
//         return;
//       }
      
//       if (this.props.dynamic) {
//         this.renderDynamicChart();
//       }
//     } catch (error) {
//       console.error("Error rendering chart:", error);
//       this.renderErrorMessage('Error loading chart');
//     }
//   }
  
//   /**
//    * Render error message on canvas
//    */
//   renderErrorMessage(message) {
//     if (this.chartRef.el) {
//       const ctx = this.chartRef.el.getContext('2d');
//       if (ctx) {
//         ctx.clearRect(0, 0, this.chartRef.el.width, this.chartRef.el.height);
//         ctx.font = '14px Arial';
//         ctx.fillStyle = '#666';
//         ctx.textAlign = 'center';
//         ctx.fillText(message, this.chartRef.el.width / 2, this.chartRef.el.height / 2);
//       }
//     }
//   }

//   /**
//    * Render dynamic chart with data from props
//    */
//   renderDynamicChart() {
//     // Ensure we have data and a chart reference
//     if (!this.props.data || !this.chartRef.el) {
//       return;
//     }

//     try {
//       // Create a safe version of the chart data
//       const chartData = {
//         type: this.props.data.type || 'bar',
//         labels: Array.isArray(this.props.data.labels) ? this.props.data.labels : [],
//         datasets: Array.isArray(this.props.data.datasets) ? this.props.data.datasets : [],
//         title: this.props.data.title || '',
//         model_name: this.props.data.model_name || '',
//         filter: this.props.data.filter || '',
//         ids: Array.isArray(this.props.data.ids) ? this.props.data.ids : [],
//         datefield: this.props.data.datefield || '',
//         domain_filter: Array.isArray(this.props.data.domain_filter) ? this.props.data.domain_filter : []
//       };

//       // Create the chart with safe data
//       this.myChartInstance = new Chart(this.chartRef.el, {
//         type: chartData.type,
//         data: {
//           labels: chartData.labels,
//           datasets: chartData.datasets
//         },
//         options: {
//           animation: {
//             duration: 500 // Faster animations
//           },
//           onClick: (event, elements) => {
//             if (!elements || elements.length === 0) return;

//             const clickedIndex = elements[0].index;
//             const modelName = chartData.model_name;
//             const filterColumn = chartData.filter;
//             const filterID = chartData.ids[clickedIndex];
            
//             if (!modelName || !filterColumn || filterID === undefined) {
//               console.warn("Missing data for chart click action");
//               return;
//             }
            
//             let dateField = "";
//             let splitDateField = chartData.datefield.split(".");

//             if(splitDateField.length > 1) {
//               dateField = splitDateField[1];
//             } else {
//               dateField = chartData.datefield;
//             }
            
//             let domain = [[filterColumn, "=", filterID], ...chartData.domain_filter];
            
//             if (this.props.date > 0) {
//               const today = new Date();
//               const prevDate = new Date();
//               prevDate.setDate(today.getDate() - this.props.date);
              
//               const formatDate = date => {
//                 return date.toISOString().split('T')[0];
//               };
              
//               const odooPrevDate = `${formatDate(prevDate)} 00:00:00`;
//               const odooCurrentDate = `${formatDate(today)} 23:59:59`;
              
//               domain.push([dateField, ">=", odooPrevDate]);
//               domain.push([dateField, "<=", odooCurrentDate]);
//             }

//             const selectedLabel = chartData.labels[clickedIndex] || "Unknown";
            
//             const chartTitle = chartData.title ? 
//               chartData.title.charAt(0).toUpperCase() + chartData.title.slice(1).toLowerCase() : 
//               "Chart results";
              
//             const displayTitle = `${chartTitle} - ${selectedLabel}`;
             
//             let action = {
//               type: "ir.actions.act_window",
//               name: displayTitle,
//               res_model: modelName,
//               domain: domain,
//               views: [
//                 [false, "tree"],
//                 [false, "form"],
//               ],
//             };

//             this.navigate.doAction(action);
//           },
//           responsive: true,
//           maintainAspectRatio: true,
//           scales: {
//             y: {
//               ticks: {
//                 stepSize: 100,
//                 callback: function (value) {
//                   return value;
//                 },
//               },
//             },
//           },
//           plugins: {
//             legend: {
//               display: chartData.type === "bar" || chartData.type === "line" || chartData.type === "radar" ? false : true,
//               position: "top",
//             },
//             title: {
//               display: true,
//               position: "bottom",
//             },
//           },
//         },
//       });
//     } catch (error) {
//       console.error("Error creating chart:", error);
//       this.renderErrorMessage('Error rendering chart');
//     }
//   }
// }

// ChartRenderer.template = "owl.ChartRender";
// ChartRenderer.props = {
//   admin: { type: Boolean, optional: true },
//   branches_id: { type: Array, optional: true },
//   date: { type: Number, optional: true, default: 0 },
//   type: { type: String, optional: true, default: 'bar' },
//   title: { type: String, optional: true },
//   data: { type: Object, optional: true },
//   dynamic: { type: Boolean, optional: true, default: false },
//   isLoading: { type: Boolean, optional: true, default: false }
// };



















/** @odoo-module */

// import { loadJS } from "@web/core/assets";
// const { Component, onWillStart, useRef, useEffect, onWillUnmount } = owl;
// import { useService } from "@web/core/utils/hooks";

// // Constants
// const CHARTJS_CDN = "https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js";

// /**
//  * Chart renderer component
//  * @class ChartRenderer
//  */
// export class ChartRenderer extends Component {
//   setup() {
//     this.navigate = useService("action");
//     this.chartRef = useRef("compliance_chart");
//     this.myChartInstance = null;

//     onWillStart(async () => {
//       await loadJS(CHARTJS_CDN);
//     });

//     useEffect(
//       () => {
//         // Only render chart if data exists and is not loading
//         if (this.props.data && !this.props.isLoading) {
//           this.renderChart();
//         }
//       },
//       () => [this.props.data, this.props.type, this.props.title, this.props.isLoading]
//     );

//     onWillUnmount(() => {
//       this.destroyChart();
//     });
//   }

//   /**
//    * Destroy existing chart instance
//    */
//   destroyChart() {
//     if (this.myChartInstance) {
//       this.myChartInstance.destroy();
//       this.myChartInstance = null;
//     }
//   }

//   /**
//    * Render the appropriate chart type
//    */
//   renderChart() {
//     this.destroyChart();
//     if (this.props.dynamic) {
//       this.renderDynamicChart();
//     }
//   }

//   /**
//    * Render dynamic chart with data from props
//    */
//   renderDynamicChart() {
//     if (!this.props.data) {
//       return;
//     }

//     this.myChartInstance = new Chart(this.chartRef.el, {
//       type: this.props.data.type,
//       data: {
//         labels: this.props.data.labels,
//         datasets: this.props.data.datasets
//       },
//       options: {
//         onClick: (event, elements) => {
//           if (!elements || elements.length === 0) return;

//           const clickedIndex = elements[0].index;
//           const modelName = this.props.data.model_name;
//           const filterColumn = this.props.data.filter;
//           const filterID = this.props.data.ids[clickedIndex];
          
//           let dateField = "";
//           let splitDateField = this.props.data.datefield.split(".");

//           if(splitDateField.length > 1){
//             dateField = splitDateField[1];
//           } else {
//             dateField = this.props.data.datefield;
//           }
          
//           let domain = [[filterColumn, "=", filterID], ...this.props.data.domain_filter];
          
//           if (this.props.date > 0) {
//             const today = new Date();
//             const prevDate = new Date();
//             prevDate.setDate(today.getDate() - this.props.date);
            
//             const formatDate = date => {
//               return date.toISOString().split('T')[0];
//             };
            
//             const odooPrevDate = `${formatDate(prevDate)} 00:00:00`;
//             const odooCurrentDate = `${formatDate(today)} 23:59:59`;
            
//             domain.push([dateField, ">=", odooPrevDate]);
//             domain.push([dateField, "<=", odooCurrentDate]);
//           }

//           const selectedLabel = this.props.data.labels[clickedIndex];
          
//           const chartTitle = this.props.data.title ? 
//             this.props.data.title.charAt(0).toUpperCase() + this.props.data.title.slice(1).toLowerCase() : 
//             "Chart results";
            
//           const displayTitle = `${chartTitle} - ${selectedLabel}`;
           
//           let action = {
//             type: "ir.actions.act_window",
//             name: displayTitle,
//             res_model: modelName,
//             domain: domain,
//             views: [
//               [false, "tree"],
//               [false, "form"],
//             ],
//           };

//           this.navigate.doAction(action);
//         },
//         responsive: true,
//         maintainAspectRatio: true,
//         scales: {
//           y: {
//             ticks: {
//               stepSize: 100,
//               callback: function (value) {
//                 return value;
//               },
//             },
//           },
//         },
//         plugins: {
//           legend: {
//             display: this.props.data.type === "bar" || this.props.data.type === "line" || this.props.data.type === "radar" ? false : true,
//             position: "top",
//           },
//           title: {
//             display: true,
//             position: "bottom",
//           },
//         },
//       },
//     });
//   }

//   /**
//    * Get default chart options
//    * @returns {Object} Default chart options
//    */
//   getDefaultChartOptions() {
//     return {
//       responsive: true,
//       maintainAspectRatio: false,
//       plugins: {
//         legend: {
//           display: true,
//           position: "top",
//         },
//         title: {
//           display: true,
//           position: "bottom",
//         },
//       },
//     };
//   }
// }

// ChartRenderer.template = "owl.ChartRender";































// /** @odoo-module */

// import { loadJS } from "@web/core/assets";
// const { Component, onWillStart, useRef, useEffect, onWillUnmount } = owl;
// import { useService } from "@web/core/utils/hooks";

// // Consider using a constant for CDN URLs for maintainability
// const CHARTJS_CDN =
//   "https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js";


// // Define constants for date formats to ensure consistency
// const DATE_FORMAT_YYYY_MM_DD = "YYYY-MM-DD";
// const TIME_00_00_00 = "00:00:00";
// const TIME_23_59_59 = "23:59:59";

// export class ChartRenderer extends Component {
//   setup() {
//     this.navigate = useService("action");
//     this.chartRef = useRef("compliance_chart");
//     this.myChartInstance = null; // Renamed to avoid shadowing and be more explicit

//     onWillStart(async () => {
//       await loadJS(CHARTJS_CDN);
//     });

//     useEffect(
//       () => {
//         if (this.props.data) {
//           this.renderChart();
//         }
//       },
//       () => [this.props.data, this.props.type, this.props.title] // Add type and title to dependencies if they can change and require re-render
//     );

//     onWillUnmount(() => {
//       this.destroyChart(); // Use a dedicated method for chart destruction
//     });
//   }

//   destroyChart() {
//     if (this.myChartInstance) {
//       this.myChartInstance.destroy();
//       this.myChartInstance = null;
//     }
//   }

//   renderChart() {
//     this.destroyChart(); // Destroy existing chart before rendering a new one

//     // if (this.props.title === "Top 10 Branches By Customer" && !this.props.dynamic) {
//     //   // Use constant for comparison
//     //   this.renderTopBranchesChart();
//     // } else if (this.props.title === "Top 10 Screened Transaction By Rules" && !this.props.dynamic) {
//     //   this.renderScreenedChart();
//     // } else if (this.props.title === "Top 10 High-Risk Customer By Branch" && !this.props.dynamic) {
//     //   this.renderHighRiskchart()
    
//     // } 
//     if (this.props.dynamic) {
//       this.renderDynamicChart()
//     }
//   }

//   renderTopBranchesChart() {
//     if (!this.props.data) {
//       return; // Exit if no data to render
//     }

//     let labels = [];
//     let values = [];
//     let branch_ids = [];

//     for (let item of this.props.data) {
//       labels.push(item.branch_name);
//       values.push(item.customer_count);
//       branch_ids.push(item.id);
//     }

//     const formatDate = (date) => date.toISOString().slice(0, 10);

//     let prevDate, currentDate;

//     if (this.props.date > 0) {
//       prevDate = moment()
//         .subtract(this.props.date, "days")
//         .format(DATE_FORMAT_YYYY_MM_DD); // Use constant for date format
//       currentDate = formatDate(new Date());
//     } else {
//       currentDate = formatDate(new Date());
//       prevDate = currentDate;
//     }

//     const odooPrevDate = `${prevDate} ${TIME_00_00_00}`; // Use constants for time and date format
//     const odooCurrentDate = `${currentDate} ${TIME_23_59_59}`;

//     this.myChartInstance = new Chart(this.chartRef.el, {
//       type: this.props.type,
//       data: {
//         labels: labels,
//         datasets: [
//           {
//             label: "", // Consider making label configurable if needed
//             data: values,
//             backgroundColor: [
//               "#d9d9d9",
//               "#FFD700",
//               "#66B2FF",
//               "#C8102E",
//               "#4CAF50",
//             ],
//           },
//         ],
//       },
//       options: {
//         ...this.getDefaultChartOptions(), // Start with default options
//         onClick: (event, elements) => {
//           if (!elements || elements.length === 0) return;

//           const clickedIndex = elements[0].index;
//           const filter = branch_ids[clickedIndex];

//            let domain = [["branch_id", "=", filter]];

//            if (this.props.date > 0) {
//              domain.push(["create_date", ">=", prevDate]);
//              domain.push(["create_date", "<=", currentDate]);
//            }

//            // Admin Check and Branch Filtering
//            if (!this.props.admin) {
//              domain.push(["branch_id", "in", this.props.branches_id]);
//            }

//           let action = {
//             type: "ir.actions.act_window",
//             name: "Top 10 Branches", // Use constant for action name
//             res_model: "res.partner", // Use constant for model name
//             domain: domain,
//             views: [
//               [false, "tree"], // Use constants for view types
//               [false, "form"],
//             ],
//           };

//           this.navigate.doAction(action);
//         },
//         scales: {
//           y: {
//             ticks: {
//               stepSize: 100,
//               callback: function (value) {
//                 return value;
//               },
//             },
//           },
//         },
//         plugins: {
//           title: {
//             text: this.props.title,
//           },
//         },
//       },
//     });
//   }
//   renderScreenedChart() {
//     if (!this.props.data) {
//       return; // Exit if no data to render
//     }

//     let labels = [];
//     let values = [];
//     let rules_ids = [];

//     for (let item of this.props.data) {
//       labels.push(item.name);
//       values.push(item.count);
//       rules_ids.push(item.id);
//     }

//     const formatDate = (date) => date.toISOString().slice(0, 10);

//     let prevDate, currentDate;

//     if (this.props.date > 0) {
//       prevDate = moment()
//         .subtract(this.props.date, "days")
//         .format(DATE_FORMAT_YYYY_MM_DD); // Use constant for date format
//       currentDate = formatDate(new Date());
//     } else {
//       currentDate = formatDate(new Date());
//       prevDate = currentDate;
//     }

//     this.myChartInstance = new Chart(this.chartRef.el, {
//       type: this.props.type,
//       data: {
//         labels: labels,
//         datasets: [
//           {
//             label: "", // Consider making label configurable if needed
//             data: values,
//             backgroundColor: "#d9d9d9", // Set the background color to grey
//           },
//         ],
//       },
//       options: {
//         ...this.getDefaultChartOptions(), // Start with default options
//         onClick: (event, elements) => {
//           if (!elements || elements.length === 0) return;

//           const clickedIndex = elements[0].index;
//           const filter = rules_ids[clickedIndex];

  
//            let domain = [
//               ["rule_id", "=", filter]
//            ];

//            if (this.props.date > 0) {
//              domain.push(["date_created", ">=", prevDate]);
//              domain.push(["date_created", "<=", currentDate]);
//            }

//            // Admin Check and Branch Filtering
//            if (!this.props.admin) {
//              domain.push(["branch_id", "in", this.props.branches_id]);
//            }

  
           

//           let action = {
//             type: "ir.actions.act_window",
//             name: "Top 10 Screened Transaction By Rules", // Use constant for action name
//             res_model: "res.customer.transaction", // Use constant for model name
//             domain: domain,
//             views: [
//               [false, "tree"], // Use constants for view types
//               [false, "form"],
//             ],
//           };

//           this.navigate.doAction(action);
//         },
//         scales: {
//           y: {
//             ticks: {
//               stepSize: 100,
//               callback: function (value) {
//                 return value;
//               },
//             },
//           },
//         },
//         plugins: {
//           title: {
//             text: this.props.title,
//           },
//         },
//       },
//     });
//   }

//   renderHighRiskchart() {
//     if (!this.props.data) {
//       return; // Exit if no data to render
//     }

//     let labels = [];
//     let values = [];
//     let branch_ids = [];

//     for (let item of this.props.data) {
//       labels.push(item.name.split(" ")[0]);
//       values.push(item.count);
//       branch_ids.push(item.id);
//     }

//     const formatDate = (date) => date.toISOString().slice(0, 10);

//     let prevDate, currentDate;

//     if (this.props.date > 0) {
//       prevDate = moment()
//         .subtract(this.props.date, "days")
//         .format(DATE_FORMAT_YYYY_MM_DD); // Use constant for date format
//       currentDate = formatDate(new Date());
//     }

//     this.myChartInstance = new Chart(this.chartRef.el, {
//       type: this.props.type,
//       data: {
//         labels: labels,
//         datasets: [
//           {
//             label: "", // Consider making label configurable if needed
//             data: values,
//             border: "none",
//           },
//         ],
//       },
//       options: {
//         ...this.getDefaultChartOptions(), // Start with default options
//         onClick: (event, elements) => {
//           if (!elements || elements.length === 0) return;

//           const clickedIndex = elements[0].index;
//           const filter = branch_ids[clickedIndex];

//           let domain = [
//             ["branch_id", "=", filter],
//             ["risk_level", "=", "high"],
//           ];

//           if (this.props.date > 0) {
//             domain.push(["create_date", ">=", prevDate]);
//             domain.push(["create_date", "<=", currentDate]);
//           }

//           // Admin Check and Branch Filtering
//           if (!this.props.admin) {
//             domain.push(["branch_id", "in", this.props.branches_id]);
//           }

//           let action = {
//             type: "ir.actions.act_window",
//             name: "Top 10 High-Risk Branches", // Use constant for action name
//             res_model: "res.partner", // Use constant for model name
//             domain: domain,
//             views: [
//               [false, "tree"], // Use constants for view types
//               [false, "form"],
//             ],
//           };

//           this.navigate.doAction(action);
//         },
//         scales: {
//           y: {
//             ticks: {
//               stepSize: 100,
//               callback: function (value) {
//                 return value;
//               },
//             },
//           },
//         },
//         plugins: {
//           title: {
//             text: this.props.title,
//           },
//           legend: {
//             position: "top",
//             align: "center",
//           },
//         },
//       },
//     });
//   }

//   renderDynamicChart() {
//     if (!this.props.data) {
//       return; // Exit if no data to render
//     }

//     const formatDate = (date) => date.toISOString().slice(0, 10);

//     let prevDate, currentDate;

//     if (this.props.date > 0) {
//       prevDate = moment()
//         .subtract(this.props.date, "days")
//         .format(DATE_FORMAT_YYYY_MM_DD); // Use constant for date format
//       currentDate = formatDate(new Date());
//     } else {
//       currentDate = formatDate(new Date());
//       prevDate = currentDate;
//     }

//     const odooPrevDate = `${prevDate} ${TIME_00_00_00}`; // Use constants for time and date format
//     const odooCurrentDate = `${currentDate} ${TIME_23_59_59}`;

//     this.myChartInstance = new Chart(this.chartRef.el, {
//       type: this.props.data.type,
//       data: {
//         labels: this.props.data.labels,
//         datasets: this.props.data.datasets
//       },
//       options: {
//         onClick: (event, elements) => {
//           if (!elements || elements.length === 0) return;

//           let dateField = ""
//           const clickedIndex = elements[0].index;
//           const modelName = this.props.data.model_name
//           const filterColumn = this.props.data.filter
//           const filterID = this.props.data.ids[clickedIndex];
          
//           let splitDateField = this.props.data.datefield.split(".") 

//           if(splitDateField.length > 1){
//             dateField = splitDateField[1]
//           }else{
//             dateField = this.props.data.datefield
//           }

          
          
//           let domain = [[filterColumn, "=", filterID], ...this.props.data.domain_filter]

  
//           console.log(modelName);
          
//           console.log(domain);
          
          
//            if (this.props.date > 0) {
//              domain.push([dateField, ">=", odooPrevDate]);
//              domain.push([dateField, "<=", odooCurrentDate]);
//            }

//            const replacedString = modelName.replaceAll(".", "_");
//            const firstChar = replacedString.charAt(0).toUpperCase();
//            const restOfString = replacedString.slice(1);
          
//           const selectedLabel = this.props.data.labels[clickedIndex];
          
//           const chartTitle = this.props.data.title ? 
//             this.props.data.title.charAt(0).toUpperCase() + this.props.data.title.slice(1).toLowerCase() : 
//             "Chart results";
            
//           const displayTitle = `${chartTitle} - ${selectedLabel}`;
           
//           let action = {
//             type: "ir.actions.act_window",
//             // name: this.props.data.title,
//             name: displayTitle,
//             // name: firstChar + restOfString, // Use constant for action name
//             res_model: modelName, // Use constant for .model name
//             domain: domain,
//             views: [
//               [false, "tree"], // Use constants for view types
//               [false, "form"],
//             ],
//           };

//           this.navigate.doAction(action);
//         },
//         responsive: true,
//         maintainAspectRatio: true,
//         scales: {
//           y: {
//             ticks: {
//               stepSize: 100,
//               callback: function (value) {
//                 return value;
//               },
//             },
//           },
//         },
//         plugins: {
//           legend: {
//             display: this.props.data.type === "bar" || this.props.data.type === "line" || this.props.data.type === "radar"  ? false : true,
//             position: "top",
//           },
//           title: {
//             display: true,
//             position: "bottom",
//           },
//         },
//       },
//     });
//   }

//   getDefaultChartOptions() {
//     return {
//       responsive: true,
//       maintainAspectRatio: false,
//       plugins: {
//         legend: {
//           display: true,
//           position: "top",
//         },
//         title: {
//           display: true,
//           position: "bottom",
//         },
//       },
//     };
//   }
// }

// ChartRenderer.template = "owl.ChartRender";
