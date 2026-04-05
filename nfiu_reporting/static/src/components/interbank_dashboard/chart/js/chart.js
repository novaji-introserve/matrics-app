/** @odoo-module */

const { Component, onWillUnmount, useEffect, useRef, useState } = owl;

export class InterbankChart extends Component {
    setup() {
        this.chartRef = useRef("chart");
        this.chartInstance = null;
        this.state = useState({ error: null });

        useEffect(
            () => {
                this.renderChart();
                return () => this.destroyChart();
            },
            () => [this.props.data]
        );

        onWillUnmount(() => this.destroyChart());
    }

    renderChart() {
        this.destroyChart();
        this.state.error = null;

        if (!this.chartRef.el || !this.props.data) {
            return;
        }

        if (typeof Chart === "undefined") {
            this.state.error = "Chart library is not available.";
            return;
        }

        const dataset = (this.props.data.datasets || [])[0] || {};
        const labels = this.props.data.labels || [];
        const hasData = Array.isArray(dataset.data) && dataset.data.length;
        if (!labels.length || !hasData) {
            return;
        }

        this.chartInstance = new Chart(this.chartRef.el, {
            type: this.props.data.type || "bar",
            data: {
                labels,
                datasets: this.props.data.datasets,
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: {
                    duration: 350,
                },
                plugins: {
                    legend: {
                        display: false,
                    },
                    tooltip: {
                        backgroundColor: "#0f172a",
                        titleColor: "#f8fafc",
                        bodyColor: "#e2e8f0",
                        padding: 12,
                    },
                },
                scales: {
                    x: {
                        grid: {
                            display: false,
                        },
                        ticks: {
                            color: "#475569",
                        },
                    },
                    y: {
                        beginAtZero: true,
                        grid: {
                            color: "rgba(148, 163, 184, 0.18)",
                        },
                        ticks: {
                            color: "#475569",
                            precision: 0,
                        },
                    },
                },
            },
        });
    }

    destroyChart() {
        if (this.chartInstance) {
            this.chartInstance.destroy();
            this.chartInstance = null;
        }
    }
}

InterbankChart.template = "owl.InterbankChart";
InterbankChart.props = {
    data: { type: Object, optional: true },
};
