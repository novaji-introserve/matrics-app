/** @odoo-module **/

import { registry } from '@web/core/registry';
import { standardFieldProps } from '@web/views/fields/standard_field_props';
import { Component, useState, onWillUpdateProps } from '@odoo/owl';
import { useInputField } from '@web/views/fields/input_field_hook';

export class SliderField extends Component {

    setup() {
        // Initialize local state with the current prop value
        this.state = useState({
            localValue: this.getSafeValue(this.props.value)
        });

        useInputField({
            getValue: () => this.state.localValue,
            refName: 'input',
        });

        // Sync local state if the value changes from the server/outside
        onWillUpdateProps((nextProps) => {
            const newVal = this.getSafeValue(nextProps.value);
            // Only update if the prop is effectively different to avoid jitter
            if (newVal !== this.state.localValue) {
                this.state.localValue = newVal;
            }
        });
    }

    // Helper to parse values safely
    getSafeValue(val) {
        return parseFloat(val) || 0;
    }

    // 1. FAST: Updates the UI only (Smooth Dragging)
    _onInput(ev) {
        this.state.localValue = parseFloat(ev.target.value);
    }

    // 2. SLOW: Commits to Odoo Server (Happens only on mouse release)
    _onChange(ev) {
        const newValue = parseFloat(ev.target.value);
        this.props.update(newValue);
    }

    // Get props for slider with proper defaults
    get sliderProps() {
        return {
            min: this.resolveFieldReference(this.props.min, 0),
            max: this.resolveFieldReference(this.props.max, 100),
            step: this.resolveFieldReference(this.props.step, 1)
        };
    }

    // Resolves a field reference if it's a field name, otherwise treats it as a static value
    resolveFieldReference(value, defaultVal) {
        if (!value) {
            return defaultVal;
        }

        // Try to check if it's a field name
        if (typeof value === 'string' && this.props.record && this.props.record.data) {
            if (value in this.props.record.data) {
                const fieldValue = this.props.record.data[value];
                return fieldValue !== undefined && fieldValue !== false ?
                    parseFloat(fieldValue) || defaultVal : defaultVal;
            }
        }

        // If not a field name, try to parse as number
        const parsed = parseFloat(value);
        return isNaN(parsed) ? defaultVal : parsed;
    }
}

SliderField.template = 'web_field_slider.SliderField';

SliderField.props = {
    ...standardFieldProps,
    step: { type: String, optional: true },
    min: { type: String, optional: true },
    max: { type: String, optional: true },
};

SliderField.defaultProps = {
    step: 1,
    min: 0,
    max: 100,
};

SliderField.extractProps = ({ attrs, field }) => {
    return {
        step: attrs.step,
        min: attrs.min,
        max: attrs.max,
    };
};

registry.category('fields').add('slider', SliderField);