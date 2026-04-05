/** @odoo-module */

const { Component, onWillStart, useRef, useEffect, onWillUnmount, useState } = owl;
import { useService } from "@web/core/utils/hooks";

const ANIMATION_DELAY = 50;
const MAX_INITIAL_ITEMS = 10;

const MODERN_PALETTES = {
  accounts: [
    "#2563eb",
    "#3b82f6",
    "#60a5fa",
    "#93c5fd",
    "#1d4ed8",
    "#0ea5e9",
    "#38bdf8",
    "#0284c7",
    "#6366f1",
    "#8b5cf6",
  ],
  highRisk: [
    "#f97316",
    "#fb7185",
    "#ef4444",
    "#f59e0b",
    "#ea580c",
    "#dc2626",
    "#f43f5e",
    "#fb923c",
    "#f87171",
    "#fdba74",
  ],
  exceptions: [
    "#059669",
    "#0ea5e9",
    "#8b5cf6",
    "#f97316",
    "#ef4444",
    "#eab308",
    "#14b8a6",
    "#6366f1",
    "#ec4899",
    "#84cc16",
  ],
  exceptionLine: {
    stroke: "#059669",
    fill: "rgba(5, 150, 105, 0.14)",
    points: "#10b981",
  },
  neutralBorder: "rgba(15, 23, 42, 0.08)",
  neutralGrid: "rgba(15, 23, 42, 0.08)",
  text: "#334155",
  title: "#0f172a",
};

/**
 * Chart renderer component with progressive loading animation
 */
export class ChartRenderer extends Component {
  setup() {
    this.navigate = useService("action");
    this.chartRef = useRef("compliance_chart");
    this.chartInstance = null;
    this.normalizedData = null;

    this.animationTimeouts = [];

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
      this.state.isLibraryLoading = false;
    });

    useEffect(
      () => {
        if (this.state.isLibraryLoading || !this.props.data) {
          return;
        }

        this.clearAllAnimationTimeouts();
        this.destroyChart();
        this.state.error = null;
        this.state.emptyChart = false;
        this.state.loadedElements = 0;
        this.state.totalElements = 0;
        this.state.isAnimating = false;

        if (this.chartRef.el) {
          this.initializeChart();
        }
      },
      () => [this.state.isLibraryLoading, this.props.data]
    );

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
      const labels = this.props.data.labels || [];
      const datasets = this.props.data.datasets || [];

      this.state.totalElements = labels.length;

      if (labels.length === 0 || datasets.length === 0 ||
        (datasets[0] && datasets[0].data && datasets[0].data.length === 0)) {
        this.state.emptyChart = true;
        this.createEmptyChart();
        this.state.isInitializing = false;
        return;
      }

      this.createProgressiveChart();

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

    this.destroyChart();

    try {
      const chartType = this.props.data?.type || this.props.type || 'bar';
      const options = this._buildChartOptions(chartType, this.props.data);

      this.chartInstance = new Chart(this.chartRef.el, {
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
        options
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

    this.destroyChart();

    try {
      const chartType = this.props.data.type || this.props.type || 'bar';
      const sourceData = this.props.data;
      const normalizedData = this._normalizeChartData(sourceData, chartType);
      this.normalizedData = normalizedData;

      const initialCount = Math.min(MAX_INITIAL_ITEMS, normalizedData.labels.length);

      const initialDatasets = normalizedData.datasets.map(dataset => {
        const initialData = dataset.data.slice(0, initialCount);
        const initialColors = Array.isArray(dataset.backgroundColor) ?
          dataset.backgroundColor.slice(0, initialCount) :
          dataset.backgroundColor;

        let initialBorderColors;
        if (chartType === 'line') {
          initialBorderColors = dataset.borderColor;
        } else if (Array.isArray(dataset.borderColor)) {
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

      const options = this._buildChartOptions(chartType, sourceData);

      this.chartInstance = new Chart(this.chartRef.el, {
        type: chartType,
        data: {
          labels: normalizedData.labels.slice(0, initialCount),
          datasets: initialDatasets
        },
        options
      });

      this.state.loadedElements = initialCount;

    } catch (error) {
      console.error("Error creating progressive chart:", error);
      this.state.error = "Failed to create chart";
    }
  }

  _getPaletteKey(title) {
    const lowerTitle = String(title || "").toLowerCase();
    if (lowerTitle.includes("high risk")) {
      return "highRisk";
    }
    if (lowerTitle.includes("exception") || lowerTitle.includes("screened")) {
      return "exceptions";
    }
    return "accounts";
  }

  _buildBarColors(palette, count) {
    return Array.from({ length: count }, (_, index) => palette[index % palette.length]);
  }

  _normalizeChartData(sourceData, chartType) {
    const paletteKey = this._getPaletteKey(sourceData.title);
    const labels = sourceData.labels || [];
    const datasets = (sourceData.datasets || []).map((dataset) => {
      if (chartType === "line") {
        return {
          ...dataset,
          borderColor: MODERN_PALETTES.exceptionLine.stroke,
          backgroundColor: MODERN_PALETTES.exceptionLine.fill,
          pointBackgroundColor: MODERN_PALETTES.exceptionLine.points,
          pointBorderColor: "#ffffff",
          pointHoverBackgroundColor: MODERN_PALETTES.exceptionLine.stroke,
          pointHoverBorderColor: "#ffffff",
          pointRadius: 4,
          pointHoverRadius: 6,
          borderWidth: 3,
          tension: 0.35,
          fill: true,
        };
      }

      const palette = MODERN_PALETTES[paletteKey] || MODERN_PALETTES.accounts;
      const colors = this._buildBarColors(palette, labels.length);
      if (chartType === "pie" || chartType === "doughnut") {
        return {
          ...dataset,
          backgroundColor: colors,
          borderColor: colors.map(() => "#ffffff"),
          borderWidth: 2,
          hoverOffset: 10,
        };
      }
      return {
        ...dataset,
        backgroundColor: colors,
        borderColor: colors.map(() => MODERN_PALETTES.neutralBorder),
        borderWidth: 1,
        borderRadius: 4,
        borderSkipped: false,
        hoverBackgroundColor: colors,
        maxBarThickness: 38,
      };
    });

    return {
      ...sourceData,
      labels,
      datasets,
    };
  }

  _buildChartOptions(chartType, sourceData) {
    const isCircularChart = chartType === "pie" || chartType === "doughnut";
    const baseOptions = {
      onClick: (event, elements) => this.handleChartClick(event, elements),
      responsive: true,
      maintainAspectRatio: false,
      interaction: {
        mode: isCircularChart ? 'nearest' : 'nearest',
        axis: isCircularChart ? undefined : 'x',
        intersect: false,
      },
      hover: {
        mode: 'nearest',
        intersect: false,
      },
      animation: {
        duration: 550,
        easing: 'easeOutQuart',
      },
      plugins: {
        legend: {
          display: chartType !== 'bar',
          position: isCircularChart ? 'right' : 'top',
          labels: {
            color: MODERN_PALETTES.text,
            usePointStyle: true,
            boxWidth: 10,
          },
        },
        title: {
          display: false,
          text: sourceData?.title || this.props.title || '',
          position: 'bottom'
        },
        tooltip: {
          enabled: true,
          backgroundColor: 'rgba(15, 23, 42, 0.92)',
          titleColor: '#ffffff',
          bodyColor: '#e2e8f0',
          padding: 12,
          cornerRadius: 12,
          displayColors: chartType !== 'line',
          callbacks: {
            label: function (context) {
              const value = context.parsed?.y ?? context.parsed;
              return `${context.label}: ${value}`;
            },
          },
        },
        subtitle: {
          display: false,
          text: 'Loading chart elements...',
          padding: {
            bottom: 10
          }
        }
      }
    };

    if (!isCircularChart) {
      baseOptions.scales = {
        y: {
          beginAtZero: true,
          grid: {
            color: MODERN_PALETTES.neutralGrid,
          },
          ticks: {
            color: MODERN_PALETTES.text,
            callback: function (value) {
              return value;
            }
          }
        },
        x: {
          grid: {
            display: false,
          },
          ticks: {
            color: MODERN_PALETTES.text,
            maxRotation: chartType === 'line' ? 0 : 18,
            minRotation: 0,
          },
        }
      };
    }

    return baseOptions;
  }

  /**
   * Animate the remaining chart elements one by one
   */
  animateRemainingElements() {
    if (!this.chartInstance || !this.props.data ||
      this.state.loadedElements >= this.state.totalElements) {
      return;
    }

    this.state.isAnimating = true;

    this.clearAllAnimationTimeouts();

    const sourceData = this.normalizedData || this.props.data;
    const chartType = sourceData.type || 'bar';

    const startIndex = this.state.loadedElements;

    for (let i = startIndex; i < sourceData.labels.length; i++) {
      const timeoutId = setTimeout(() => {
        this.chartInstance.data.labels.push(sourceData.labels[i]);

        sourceData.datasets.forEach((dataset, datasetIndex) => {
          this.chartInstance.data.datasets[datasetIndex].data.push(dataset.data[i]);

          if (Array.isArray(dataset.backgroundColor)) {
            if (i < dataset.backgroundColor.length) {
              this.chartInstance.data.datasets[datasetIndex].backgroundColor.push(
                dataset.backgroundColor[i]
              );
            }
          } else if (dataset.backgroundColor && !this.chartInstance.data.datasets[datasetIndex].backgroundColor) {
            this.chartInstance.data.datasets[datasetIndex].backgroundColor = dataset.backgroundColor;
          }

          if (chartType === 'line') {
            if (dataset.borderColor && !this.chartInstance.data.datasets[datasetIndex].borderColor) {
              this.chartInstance.data.datasets[datasetIndex].borderColor = dataset.borderColor;
            }
          } else if (Array.isArray(dataset.borderColor) && i < dataset.borderColor.length) {
            this.chartInstance.data.datasets[datasetIndex].borderColor.push(
              dataset.borderColor[i]
            );
          }
        });

        this.state.loadedElements = i + 1;

        if (this.chartInstance.options.plugins.subtitle) {
          const percent = Math.round((this.state.loadedElements / this.state.totalElements) * 100);
          this.chartInstance.options.plugins.subtitle.text =
            `Loading: ${percent}% (${this.state.loadedElements} of ${this.state.totalElements})`;
        }

        this.chartInstance.update();

        if (i === sourceData.labels.length - 1) {
          this.state.isAnimating = false;

          if (this.chartInstance.options.plugins.subtitle) {
            this.chartInstance.options.plugins.subtitle.text =
              `Loaded ${this.state.loadedElements} items`;
            this.chartInstance.update();
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

    const modelName = chartData.model_name;
    const filterColumn = chartData.filter;
    const filterID = chartData.ids?.[clickedIndex];

    if (!modelName || !filterColumn || filterID === undefined) {
      return;
    }

    let domain = [[filterColumn, "=", filterID]];

    if (chartData.additional_domain && Array.isArray(chartData.additional_domain)) {
      chartData.additional_domain.forEach(condition => {
        if (Array.isArray(condition) && condition.length >= 3) {
          domain.push(condition);
        }
      });
    }

    this.navigate.doAction({
      type: "ir.actions.act_window",
      name: `${chartData.title} - ${chartData.labels[clickedIndex] || "Unknown"}`,
      res_model: modelName,
      domain,
      views: [[false, "tree"], [false, "form"]]
    });
  }

  /**
   * Destroy existing chart instance
   */
  destroyChart() {
    if (this.chartInstance) {
      try {
        this.chartInstance.destroy();
      } catch (error) {
        console.error("Error destroying chart:", error);
      }
      this.chartInstance = null;
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
  dynamic: { type: Boolean, optional: true, default: true },
  isLoading: { type: Boolean, optional: true, default: false }
};
