/** @odoo-module */

const { Component } = owl;

export class Card extends Component {}

Card.template = "owl.card";
Card.props = {
    onClick: { type: Function, optional: true },
    title: { optional: true },
    scope: { optional: true },
    total: { optional: true },
    bgcolor: { optional: true }
};





// /** @odoo-module */

// const { Component } = owl;

// export class Card extends Component {}

// Card.template = "owl.card";
