/** @odoo-module */

import { Component } from "@odoo/owl";
import { AbstractField } from "web.AbstractField";
import { fieldRegistry } from "web.field_registry";

class DynamicSlider extends AbstractField {
    static template = "DynamicSliderTemplate";

    setup() {
        super.setup();

        // Parse min/max/step from attributes
        this.min = this.node.attrs.min ? parseFloat(this.node.attrs.min) : 0;
        this.max = this.node.attrs.max ? parseFloat(this.node.attrs.max) : 100;
        this.step = this.node.attrs.step ? parseFloat(this.node.attrs.step) : 1;

        // Get dynamic field names for min/max
        this.minField = this.node.attrs.min_field || null;
        this.maxField = this.node.attrs.max_field || null;
    }

    async willStart() {
        await super.willStart();
        this._updateSliderValue();
    }

    start() {
        this.$input = this.$("input[type='range']");
        this.$label = this.$(".o_slider_value");

        this.$input.on("input", this._onInput.bind(this));
        this._updateSliderValue();
        return super.start();
    }

    _renderEdit() {
        this._updateSliderBounds();
        this.$input.val(this.value);
        this._updateLabel();
    }

    _updateSliderBounds() {
        const newMin = this.getParentValue(this.minField) ?? this.min;
        const newMax = this.getParentValue(this.maxField) ?? this.max;

        this.$input.attr({ min: newMin, max: newMax, step: this.step });
    }

    _updateSliderValue() {
        this.$input.val(this.value);
        this._updateLabel();
    }

    _updateLabel() {
        if (this.$label.length) {
            this.$label.text(this.value);
        }
    }

    _onInput(ev) {
        const value = parseFloat(ev.target.value);
        this._setValue(value);
        this._updateLabel();
    }

    getParentValue(fieldName) {
        if (!fieldName || !this.recordData) return null;
        return this.recordData[fieldName];
    }
}

DynamicSlider.template = "DynamicSliderTemplate";

fieldRegistry.add("dynamic_slider", DynamicSlider);

export default DynamicSlider;