/** @odoo-module */

import { registry } from "@web/core/registry";
import { useService } from "@web/core/utils/hooks";
import { standardFieldProps } from "@web/views/fields/standard_field_props";
const { Component, useState, onWillStart, onWillUpdateProps } = owl;

export class RiskSliderField extends Component {
  setup() {
    // Parse options from the view definition
    this.state = useState({
      value: this.props.record.data[this.props.name] || 0,
      minValue: 0,
      maxValue: 15,
      isDragging: false,
      reverseColors: false,
      readonly: false,
    });
    
    // Apply options
    this.updateStateFromProps(this.props);

    // Special handling for control_effectiveness_score field - always use reverse colors
    if (this.props.name === 'control_effectiveness_score') {
      this.state.reverseColors = true;
    }

    this.orm = useService("orm");
    this.notification = useService("notification");

    onWillStart(async () => {
      try {
        const minValue = await this.orm.call(
          'ir.config_parameter',
          'get_param',
          ['risk_management.min_slider_score', '0']
        );
        const maxValue = await this.orm.call(
          'ir.config_parameter',
          'get_param',
          ['risk_management.max_slider_score', '15']
        );

        this.state.minValue = parseFloat(minValue);
        this.state.maxValue = parseFloat(maxValue);
        this.state.value = this.props.record.data[this.props.name] !== undefined
          ? this.props.record.data[this.props.name]
          : this.state.minValue;
      } catch (error) {
        this.notification.add("Error Loading Slider Parameter", {
          type: "danger",
        });
      }
    });

    onWillUpdateProps((nextProps) => {
      // Update state.value when the record's value changes
      const newValue = nextProps.record.data[this.props.name];
      if (newValue !== undefined && newValue !== this.state.value) {
        this.state.value = parseFloat(newValue);
      }
      
      // Update options if they change
      this.updateStateFromProps(nextProps);
      
      // Always ensure control_effectiveness_score has reverse colors
      if (this.props.name === 'control_effectiveness_score') {
        this.state.reverseColors = true;
      }
    });
  }
  
  /**
   * Helper to update state from props and options
   */
  updateStateFromProps(props) {
    // Check if we have options in props
    const options = props.options || {};
    
    // Handle string values in JSON format (as they come from the view XML)
    if (typeof options === 'string') {
      try {
        const parsedOptions = JSON.parse(options);
        // Skip reverseColors setting for control_effectiveness_score as we'll handle it separately
        if (props.name !== 'control_effectiveness_score') {
          this.state.reverseColors = parsedOptions.reverseColors === true;
        }
        this.state.readonly = props.readonly || parsedOptions.readonly === true;
      } catch (e) {
        console.error("Error parsing options:", e);
      }
    } else {
      // Handle direct object options
      // Skip reverseColors setting for control_effectiveness_score as we'll handle it separately
      if (props.name !== 'control_effectiveness_score') {
        this.state.reverseColors = options.reverseColors === true;
      }
      this.state.readonly = props.readonly || options.readonly === true;
    }
  }

  /**
   * Handle slider value change and tooltip position
   * @param {Event} ev - The DOM event
   */
  onSliderInput(ev) {
    const value = parseFloat(ev.target.value);
    this.state.value = value;
    if (!this.state.readonly) {
      this.props.record.update({ [this.props.name]: value });
    }

    // Update tooltip position
    if (this.state.isDragging) {
      const input = ev.target;
      const min = parseFloat(input.min);
      const max = parseFloat(input.max);
      const percentage = (value - min) / (max - min) * 100;
      const tooltip = input.nextElementSibling;
      if (tooltip) {
        tooltip.style.left = `${percentage}%`;
      }
    }
  }

  /**
   * Handle drag start
   */
  onDragStart() {
    if (!this.state.readonly) {
      this.state.isDragging = true;
    }
  }

  /**
   * Handle drag end
   */
  onDragEnd() {
    this.state.isDragging = false;
  }

  /**
   * Get dynamic badge class based on value and reverseColors state
   */
  get badgeClass() {
    const range = this.state.maxValue - this.state.minValue;
    const lowThreshold = this.state.minValue + range * 0.33;
    const highThreshold = this.state.minValue + range * 0.66;
    
    // Determine if colors should be reversed (either by setting or field name)
    const useReverseColors = this.state.reverseColors || this.props.name === 'control_effectiveness_score';

    if (useReverseColors) {
      // Reversed: Low = red, medium = yellow, high = green
      if (this.state.value <= lowThreshold) {
        return 'bg-danger'; // Red for low
      } else if (this.state.value <= highThreshold) {
        return 'bg-warning'; // Yellow for medium
      } else {
        return 'bg-success'; // Green for high
      }
    } else {
      // Standard: Low = green, medium = yellow, high = red
      if (this.state.value <= lowThreshold) {
        return 'bg-success'; // Green for low
      } else if (this.state.value <= highThreshold) {
        return 'bg-warning'; // Yellow for medium
      } else {
        return 'bg-danger'; // Red for high
      }
    }
  }

  get formattedValue() {
    return this.state.value.toFixed(2);
  }
}

RiskSliderField.template = "risk_assessment.RiskSliderField";
RiskSliderField.props = {
  ...standardFieldProps,
  step: { type: Number, optional: true },
  showValue: { type: Boolean, optional: true },
  options: { type: Object, optional: true },
};

RiskSliderField.defaultProps = {
  step: 1,
  showValue: true,
  options: {},
};

RiskSliderField.supportedTypes = ['float', 'integer'];

registry.category("fields").add("risk_slider", RiskSliderField);