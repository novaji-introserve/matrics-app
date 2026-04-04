/** @odoo-module */

const { Component } = owl;

export class Card extends Component {
    get accentColor() {
        return this.props.bgcolor || "#2563eb";
    }

    get cardClassName() {
        return `cm-kpi-card${this.props.isClickable ? " cm-kpi-card--clickable" : ""}`;
    }

    handleClick() {
        if (this.props.isClickable && this.props.onClick) {
            this.props.onClick();
        }
    }
}

Card.template = "owl.card";
Card.props = {
    onClick: { type: Function, optional: true },
    title: { optional: true },
    scope: { optional: true },
    total: { optional: true },
    bgcolor: { optional: true },
    isClickable: { type: Boolean, optional: true }
};
