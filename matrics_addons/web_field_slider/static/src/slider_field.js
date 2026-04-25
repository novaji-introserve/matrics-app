/** @odoo-module **/

import { registry } from "@web/core/registry";
import { standardFieldProps } from "@web/views/fields/standard_field_props";
import { Component, useState } from "@odoo/owl";

export class SliderField extends Component {
    setup() {
        const min = this.parseConfigValue(this.props.min, 0);
        const max = this.parseConfigValue(this.props.max, 100);
        const initialValue = this.normalizeValue(this.props.value ?? min);
        this.state = useState({
            value: initialValue,
            minValue: min,
            maxValue: Math.max(max, min),
            isDragging: false,
        });
        this.pendingValue = initialValue;
    }

    parseConfigValue(value, fallback) {
        if (value === undefined || value === null || value === "") {
            return fallback;
        }
        if (typeof value === "number") {
            return value;
        }
        const numericValue = parseFloat(value);
        if (!Number.isNaN(numericValue) && String(numericValue) === String(value).trim()) {
            return numericValue;
        }
        try {
            return py.eval(value, this.props.record.evalContext);
        } catch (error) {
            return fallback;
        }
    }

    normalizeValue(rawValue) {
        const numericValue = parseFloat(rawValue);
        if (!Number.isFinite(numericValue)) {
            return this.state ? this.state.minValue : 0;
        }
        return this.fieldType === "integer" ? Math.round(numericValue) : numericValue;
    }

    onInput(ev) {
        if (this.props.readonly) {
            return;
        }
        const value = this.normalizeValue(ev.target.value);
        this.pendingValue = value;
        this.state.value = value;
    }

    onChange(ev) {
        if (this.props.readonly) {
            return;
        }
        const value = this.normalizeValue(ev.target.value);
        this.pendingValue = value;
        this.state.value = value;
        this.commitValue(value);
    }

    onDragStart() {
        if (!this.props.readonly) {
            this.state.isDragging = true;
        }
    }

    onDragEnd() {
        if (!this.props.readonly) {
            this.state.isDragging = false;
            this.commitValue(this.pendingValue);
        }
    }

    onKeyDown() {
        if (!this.props.readonly) {
            this.state.isDragging = true;
        }
    }

    commitValue(value) {
        const normalizedValue = this.normalizeValue(value);
        if (normalizedValue === this.props.value) {
            return;
        }
        this.props.update(normalizedValue);
    }

    get fieldType() {
        return this.props.type || "float";
    }

    get sliderStep() {
        return this.parseConfigValue(this.props.step, 1);
    }

    get formattedValue() {
        const step = Number(this.sliderStep || 1);
        const decimals = Number.isInteger(step) ? 0 : (String(step).split(".")[1] || "").length;
        return this.pendingValue.toFixed(decimals);
    }

    get progressPercent() {
        const range = this.state.maxValue - this.state.minValue;
        if (range <= 0) {
            return 0;
        }
        return ((this.pendingValue - this.state.minValue) / range) * 100;
    }

    get badgeClass() {
        const range = this.state.maxValue - this.state.minValue;
        const lowThreshold = this.state.minValue + range * 0.33;
        const highThreshold = this.state.minValue + range * 0.66;
        if (this.pendingValue <= lowThreshold) {
            return "bg-success";
        } else if (this.pendingValue <= highThreshold) {
            return "bg-warning";
        }
        return "bg-danger";
    }

    get sliderStyle() {
        const accentColor = this.badgeClass === "bg-success"
            ? "#15803d"
            : this.badgeClass === "bg-warning"
                ? "#c2410c"
                : "#b91c1c";
        const accentSoftColor = this.badgeClass === "bg-success"
            ? "#86efac"
            : this.badgeClass === "bg-warning"
                ? "#fdba74"
                : "#fca5a5";
        return `--slider-progress:${this.progressPercent}%;--slider-accent:${accentColor};--slider-accent-soft:${accentSoftColor};`;
    }

    get tooltipStyle() {
        return `left:${this.progressPercent}%;`;
    }
}

SliderField.template = "web_field_slider.SliderField";
SliderField.props = {
    ...standardFieldProps,
    step: { type: String, optional: true },
    min: { type: String, optional: true },
    max: { type: String, optional: true },
};
SliderField.defaultProps = {
    step: "1",
    min: "0",
    max: "100",
};
SliderField.isEmpty = () => false;
SliderField.extractProps = ({ attrs }) => ({
    step: attrs.step,
    min: attrs.min,
    max: attrs.max,
});
SliderField.supportedTypes = ["float", "integer"];

registry.category("fields").add("slider", SliderField);
