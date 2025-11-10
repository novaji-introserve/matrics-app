/** @odoo-module */

import { registry } from "@web/core/registry";
import { useService } from "@web/core/utils/hooks";
import { standardFieldProps } from "@web/views/fields/standard_field_props";
const { Component, useState, onWillStart, onWillUpdateProps } = owl;

// Utility to debounce a function
function debounce(func, wait) {
  let timeout;
  return function (...args) {
    clearTimeout(timeout);
    timeout = setTimeout(() => func.apply(this, args), wait);
  };
}

export class RiskSliderField extends Component {
  setup() {
    this.state = useState({
      value: this.props.record.data[this.props.name] || 0,
      minValue: 0,
      maxValue: 15,
      isDragging: false,
      reverseColors: false,
      readonly: false,
    });

    // Track pending value during dragging
    this.pendingValue = this.state.value;

    // Apply options
    this.updateStateFromProps(this.props);

    if (this.props.name === 'control_effectiveness_score') {
      this.state.reverseColors = true;
    }

    this.orm = useService("orm");
    this.notification = useService("notification");

    onWillStart(async () => {
      try {
        const [scoreRecord] = await this.orm.searchRead(
          'res.fcra.score',
          [],
          ['min_score', 'max_score'],
          { limit: 1 }
        );

        // Load thresholds from res.compliance.settings
      const settings = await this.orm.searchRead(
        'res.compliance.settings',
        [['code', 'in', ['low_risk_threshold', 'medium_risk_threshold']]],
        ['code', 'val']
      );

      const thresholdMap = {};
      for (const s of settings) {
        if (s.code === 'low_risk_threshold') thresholdMap.low = parseFloat(s.val);
        if (s.code === 'medium_risk_threshold') thresholdMap.medium = parseFloat(s.val);
      }

      this.state.thresholds = thresholdMap;

  
        
        this.state.minValue = parseFloat(scoreRecord?.min_score || 0);
        this.state.maxValue = parseFloat(scoreRecord?.max_score || 9);
        this.state.value = this.props.record.data[this.props.name] !== undefined
          ? this.props.record.data[this.props.name]
          : this.state.minValue;
        this.pendingValue = this.state.value;
      } catch (error) {
        this.notification.add("Error Loading Slider Parameter", {
          type: "danger",
        });
      }
    });

    onWillUpdateProps((nextProps) => {
      const newValue = nextProps.record.data[this.props.name];
      
      
      if (newValue !== undefined && newValue !== this.state.value) {
        this.state.value = parseFloat(newValue);
        this.pendingValue = this.state.value;
      }
      this.updateStateFromProps(nextProps);
      if (this.props.name === 'control_effectiveness_score') {
        this.state.reverseColors = true;
      }
    });

    // Debounced input handler
    this.onSliderInput = debounce(this._onSliderInput.bind(this), 50);
  }

  updateStateFromProps(props) {
    const options = props.options || {};
    if (typeof options === 'string') {
      try {
        const parsedOptions = JSON.parse(options);
        if (props.name !== 'control_effectiveness_score') {
          this.state.reverseColors = parsedOptions.reverseColors === true;
        }
        this.state.readonly = props.readonly || parsedOptions.readonly === true;
      } catch (e) {
        // console.error("Error parsing options:", e);
      }
    } else {
      if (props.name !== 'control_effectiveness_score') {
        this.state.reverseColors = options.reverseColors === true;
      }
      this.state.readonly = props.readonly || options.readonly === true;
    }
  }

  _onSliderInput(ev) {
    const value = parseFloat(ev.target.value);
    this.pendingValue = value;

    // Update tooltip position without triggering re-render
    const input = ev.target;
    const min = parseFloat(input.min);
    const max = parseFloat(input.max);
    const percentage = ((value - min) / (max - min)) * 100;
    const tooltip = input.nextElementSibling;
    if (tooltip) {
      tooltip.style.left = `${percentage}%`;
      tooltip.textContent = value.toFixed(1);
    }

    // Update state.value only if not readonly (but defer record update)
    if (!this.state.readonly) {
      this.state.value = value;
    }
  }

  onDragStart() {
    if (!this.state.readonly) {
      this.state.isDragging = true;
    }
  }

  onDragEnd() {
    if (!this.state.readonly) {
      this.state.isDragging = false;
      // Update record only when dragging ends
      this.props.record.update({ [this.props.name]: this.pendingValue });
    }
  }


  get badgeClass() {
    const low = this.state.thresholds?.low ?? (this.state.minValue + (this.state.maxValue - this.state.minValue) * 0.33);
    const medium = this.state.thresholds?.medium ?? (this.state.minValue + (this.state.maxValue - this.state.minValue) * 0.66);

    const value = this.pendingValue;
    const useReverseColors =
      this.state.reverseColors || this.props.name === 'control_effectiveness_score';

    // Color mapping logic
    if (useReverseColors) {
      if (value <= low) return 'bg-danger';
      if (value <= medium) return 'bg-warning';
      return 'bg-success';
    } else {
      // Normal meaning (low = good)
      if (value <= low) return 'bg-success';
      if (value <= medium) return 'bg-warning';
      return 'bg-danger';
    }
  }


  get formattedValue() {
    return this.pendingValue.toFixed(1);
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
  step: 0.1,
  showValue: true,
  options: {},
};

RiskSliderField.supportedTypes = ['float', 'integer'];

registry.category("fields").add("risk_slider", RiskSliderField);