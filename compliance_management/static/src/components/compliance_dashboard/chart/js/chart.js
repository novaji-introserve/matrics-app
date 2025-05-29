/** @odoo-module */

const { Component, onWillStart, useRef, useEffect, onWillUnmount, useState } = owl;
import { useService } from "@web/core/utils/hooks";

const ANIMATION_DELAY = 50;
const MAX_INITIAL_ITEMS = 10;

/**
 * Chart renderer component with progressive loading animation
 */
export class ChartRenderer extends Component {
  setup() {
    this.navigate = useService("action");
    this.chartRef = useRef("compliance_chart");
    this.chartInstance = null;

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
        if (!this.state.isLibraryLoading && this.props.data && !this.chartInstance) {
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
        options: {
          responsive: true,
          maintainAspectRatio: true,
          scales: {
            y: {
              beginAtZero: true,
              ticks: {
                callback: function (value) {
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

    this.destroyChart();

    try {
      const chartType = this.props.data.type || this.props.type || 'bar';
      const sourceData = this.props.data;

      const initialCount = Math.min(MAX_INITIAL_ITEMS, sourceData.labels.length);

      const initialDatasets = sourceData.datasets.map(dataset => {
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

      this.chartInstance = new Chart(this.chartRef.el, {
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
            duration: 400,
          },
          scales: {
            y: {
              beginAtZero: true,
              ticks: {
                callback: function (value) {
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
              display: false,
              text: sourceData.title || this.props.title || '',
              position: 'bottom'
            },
            tooltip: {
              enabled: true
            },
            subtitle: {
              display: false,
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
    if (!this.chartInstance || !this.props.data ||
      this.state.loadedElements >= this.state.totalElements) {
      return;
    }

    this.state.isAnimating = true;

    this.clearAllAnimationTimeouts();

    const sourceData = this.props.data;
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

    console.log("Chart click:", { chartData, clickedIndex });

    const modelName = chartData.model_name;
    const filterColumn = chartData.filter;
    const filterID = chartData.ids?.[clickedIndex];

    if (!modelName || !filterColumn || filterID === undefined) {
      console.warn("Missing data for chart click action");
      return;
    }

    let domain = [[filterColumn, "=", filterID]];

    if (chartData.additional_domain && Array.isArray(chartData.additional_domain)) {
      console.log("Adding conditions:", chartData.additional_domain);

      chartData.additional_domain.forEach(condition => {
        if (Array.isArray(condition) && condition.length >= 3) {
          domain.push(condition);
        }
      });
    }

    console.log("Navigating with action:", {
      type: "ir.actions.act_window",
      name: `${chartData.title} - ${chartData.labels[clickedIndex] || "Unknown"}`,
      res_model: modelName,
      domain,
      views: [[false, "tree"], [false, "form"]]
    });

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
