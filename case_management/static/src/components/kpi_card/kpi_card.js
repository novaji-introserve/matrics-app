/** @odoo-module */

const { Component } = owl

export class KpiCard extends Component {
  get percentageValue() {
    const value = parseFloat(this.props.percentage ?? 0);
    return Number.isFinite(value) ? value : 0;
  }

  get percentageLabel() {
    return `${this.percentageValue.toFixed(2)}% vs previous window`;
  }

  get deltaClass() {
    return this.percentageValue >= 0
      ? "case-kpi-card__delta case-kpi-card__delta--up"
      : "case-kpi-card__delta case-kpi-card__delta--down";
  }
}

KpiCard.template = "owl.KpiCard"
