/** @odoo-module */
import { loadJS } from "@web/core/assets";
const { Component, onMounted, onWillUnmount, useRef } = owl;

export class ChartRenderer extends Component {
  setup() {
    this.chartRef = useRef("chart");
    this.chart = null;
    this.resizeObserver = null;

    onMounted(async () => {
      await this.loadChartLibrary();
      this.renderChart();
      this.setupResizeHandler();
    });

    onWillUnmount(() => {
      this.cleanupResources();
    });
  }

  async loadChartLibrary() {
    // Load Chart.js only once if needed
    if (window.Chart) {
      return;
    }
    await loadJS("/web/static/lib/Chart/Chart.js");
  }

  setupResizeHandler() {
    // Use ResizeObserver for efficient resize handling
    if (window.ResizeObserver) {
      this.resizeObserver = new ResizeObserver(this.handleResize.bind(this));
      this.resizeObserver.observe(this.chartRef.el.parentElement);
    }
  }

  handleResize() {
    if (this.chart) {
      // Just trigger resize without full redraw
      this.chart.resize();
    }
  }

  cleanupResources() {
    if (this.chart) {
      this.chart.destroy();
      this.chart = null;
    }

    if (this.resizeObserver) {
      this.resizeObserver.disconnect();
      this.resizeObserver = null;
    }
  }

  renderChart() {
    if (this.chart) {
      this.chart.destroy();
    }

    // Get type and data from props
    const chartType = this.props.type || "bar";
    const typeName = chartType.replace(/'/g, "");
    const config = this.props.config || {};

    // Performance optimizations for Chart.js
    const performanceOptions = {
      responsive: true,
      maintainAspectRatio: false,
      animation: {
        duration: 300, // Short animation duration
      },
      elements: {
        point: {
          radius: typeName === "line" ? 3 : 0, // Show points for line charts
          hoverRadius: 7,
          borderWidth: 2,
        },
        line: {
          tension: 0.3, // Slight curve for line charts
        },
        bar: {
          borderWidth: 1,
          borderRadius: 2,
        },
      },
      // Enable hover animations but keep them quick
      hover: {
        mode: "nearest",
        intersect: false,
        animationDuration: 150,
      },
      // Optimize tooltips - CRITICAL for hover functionality
      plugins: {
        tooltip: {
          enabled: true,
          mode: "nearest",
          intersect: false,
          backgroundColor: "rgba(15, 23, 42, 0.92)",
          titleColor: "#fff",
          titleFont: {
            weight: "bold",
          },
          bodyColor: "#e2e8f0",
          borderColor: "rgba(255,255,255,0.12)",
          borderWidth: 1,
          padding: 12,
          cornerRadius: 12,
          displayColors: true,
          callbacks: config.options?.plugins?.tooltip?.callbacks || {},
        },
      },
      // IMPORTANT: Include both click and mousemove events for tooltips to work
      events: ["mousemove", "mouseout", "click", "touchstart", "touchmove"],
      onClick: this.handleClick.bind(this),
    };

    // Merge performance options with user options
    const userOptions = config.options || {};
    const chartOptions = {
      ...performanceOptions,
      plugins: {
        ...performanceOptions.plugins,
        ...userOptions.plugins,
        tooltip: {
          ...performanceOptions.plugins.tooltip,
          ...userOptions.plugins?.tooltip,
        },
      },
      scales: userOptions.scales || {},
    };

    // Create chart configuration
    const chartConfig = {
      type: typeName,
      data: this.optimizeDataset(config.data || {}, typeName),
      options: chartOptions,
    };

    // Create the chart
    this.chart = new Chart(this.chartRef.el.getContext("2d"), chartConfig);
  }

  optimizeDataset(data, chartType) {
    // If data is large, implement decimation or sampling
    if (!data || !data.datasets) return data;

    const optimizedData = { ...data };
    optimizedData.datasets = data.datasets.map((dataset) => {
      // Return optimized dataset with performance attributes
      return {
        ...dataset,
        spanGaps: true, // Skip missing data
        // Make data points clearly visible for pie/doughnut
        hoverBorderWidth:
          chartType === "pie" || chartType === "doughnut" ? 3 : undefined,
        hoverBorderColor: "rgba(255,255,255,0.8)",
      };
    });

    return optimizedData;
  }

  handleClick(evt, elements) {
    if (!elements || elements.length === 0 || !this.props.onClick) {
      return;
    }

    const element = elements[0];
    const dataIndex = element.index;

    // Get the label of the clicked element
    if (
      dataIndex !== undefined &&
      this.chart.data.labels &&
      dataIndex < this.chart.data.labels.length
    ) {
      const label = this.chart.data.labels[dataIndex];

      if (label) {
        this.props.onClick(label);
      }
    }
  }
}

ChartRenderer.template = "ChartRenderer";
ChartRenderer.props = {
  type: { type: String, optional: true },
  title: { type: String, optional: true },
  config: { type: Object, optional: true },
  onClick: { type: Function, optional: true },
};
