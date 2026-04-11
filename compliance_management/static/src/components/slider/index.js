/** @odoo-module **/

import { registry } from "@web/core/registry";
import { useService } from "@web/core/utils/hooks";
import { standardFieldProps } from "@web/views/fields/standard_field_props";
import { Component, useState, onWillStart, onWillUpdateProps } from "@odoo/owl";

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
    this.pendingValue = this.state.value;
    this.updateStateFromProps(this.props);

    if (this.props.name === "control_effectiveness_score") {
      this.state.reverseColors = true;
    }

    this.orm = useService("orm");
    this.notification = useService("notification");

    onWillStart(async () => {
      try {
        const [scoreRecord] = await this.orm.searchRead(
          "res.fcra.score",
          [],
          ["min_score", "max_score"],
          { limit: 1 }
        );
        this.state.minValue = parseFloat(scoreRecord?.min_score || 0);
        this.state.maxValue = parseFloat(scoreRecord?.max_score || 9);
        this.state.value = this.props.record.data[this.props.name] !== undefined
          ? this.props.record.data[this.props.name]
          : this.state.minValue;
        this.pendingValue = this.state.value;
        this.updateStateFromProps(this.props);
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
      if (nextProps.name === "control_effectiveness_score") {
        this.state.reverseColors = true;
      }
    });
  }

  updateStateFromProps(props) {
    const options = props.options || {};
    const recordData = props.record?.data || {};

    const applyDynamicBounds = (parsedOptions) => {
      if (parsedOptions.min_field && recordData[parsedOptions.min_field] !== undefined) {
        this.state.minValue = parseFloat(recordData[parsedOptions.min_field] || 0);
      }
      if (parsedOptions.max_field && recordData[parsedOptions.max_field] !== undefined) {
        this.state.maxValue = parseFloat(recordData[parsedOptions.max_field] || 0);
      }
      if (this.state.maxValue < this.state.minValue) {
        this.state.maxValue = this.state.minValue;
      }
    };

    if (typeof options === "string") {
      try {
        const parsedOptions = JSON.parse(options);
        applyDynamicBounds(parsedOptions);
        if (props.name !== "control_effectiveness_score") {
          this.state.reverseColors = parsedOptions.reverseColors === true;
        }
        this.state.readonly = props.readonly || parsedOptions.readonly === true;
      } catch (e) {
        // Ignore invalid option payloads.
      }
    } else {
      applyDynamicBounds(options);
      if (props.name !== "control_effectiveness_score") {
        this.state.reverseColors = options.reverseColors === true;
      }
      this.state.readonly = props.readonly || options.readonly === true;
    }
  }

  onInput(ev) {
    if (this.state.readonly) {
      return;
    }
    const value = this.normalizeValue(ev.target.value);
    this.pendingValue = value;
    this.state.value = value;
  }

  onDragStart() {
    if (!this.state.readonly) {
      this.state.isDragging = true;
    }
  }

  onDragEnd() {
    if (!this.state.readonly) {
      this.state.isDragging = false;
      this.commitValue(this.pendingValue);
    }
  }

  onKeyDown() {
    if (!this.state.readonly) {
      this.state.isDragging = true;
    }
  }

  onChange(ev) {
    if (this.state.readonly) {
      return;
    }
    const value = this.normalizeValue(ev.target.value);
    this.pendingValue = value;
    this.state.value = value;
    this.commitValue(value);
  }

  normalizeValue(rawValue) {
    const numericValue = parseFloat(rawValue);
    if (!Number.isFinite(numericValue)) {
      return this.state.minValue;
    }
    return this.fieldType === "integer" ? Math.round(numericValue) : numericValue;
  }

  commitValue(value) {
    const normalizedValue = this.normalizeValue(value);
    if (normalizedValue === this.props.record.data[this.props.name]) {
      return;
    }
    this.props.record.update({ [this.props.name]: normalizedValue });
  }

  get badgeClass() {
    const range = this.state.maxValue - this.state.minValue;
    const lowThreshold = this.state.minValue + range * 0.33;
    const highThreshold = this.state.minValue + range * 0.66;
    const useReverseColors = this.state.reverseColors || this.props.name === "control_effectiveness_score";

    if (useReverseColors) {
      if (this.pendingValue <= lowThreshold) {
        return "bg-danger";
      } else if (this.pendingValue <= highThreshold) {
        return "bg-warning";
      }
      return "bg-success";
    }

    if (this.pendingValue <= lowThreshold) {
      return "bg-success";
    } else if (this.pendingValue <= highThreshold) {
      return "bg-warning";
    }
    return "bg-danger";
  }

  get formattedValue() {
    const step = Number(this.sliderStep || 1);
    const decimals = Number.isInteger(step) ? 0 : (String(step).split(".")[1] || "").length;
    return this.pendingValue.toFixed(decimals);
  }

  get sliderStep() {
    const options = this.props.options || {};
    if (typeof options === "string") {
      try {
        const parsedOptions = JSON.parse(options);
        return parsedOptions.step || this.props.step;
      } catch (e) {
        return this.props.step;
      }
    }
    return options.step || this.props.step;
  }

  get shouldShowValue() {
    const options = this.props.options || {};
    if (typeof options === "string") {
      try {
        const parsedOptions = JSON.parse(options);
        return parsedOptions.show_value !== undefined ? parsedOptions.show_value : this.props.showValue;
      } catch (e) {
        return this.props.showValue;
      }
    }
    return options.show_value !== undefined ? options.show_value : this.props.showValue;
  }

  get progressPercent() {
    const range = this.state.maxValue - this.state.minValue;
    if (range <= 0) {
      return 0;
    }
    return ((this.pendingValue - this.state.minValue) / range) * 100;
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

  get fieldType() {
    return this.props.type || this.props.record?.fields?.[this.props.name]?.type || "float";
  }
}

RiskSliderField.template = "risk_assessment.RiskSliderField";
RiskSliderField.props = {
  ...standardFieldProps,
  step: { type: String, optional: true },
  showValue: { type: Boolean, optional: true },
  options: { type: Object, optional: true },
};
RiskSliderField.defaultProps = {
  step: "0.1",
  showValue: true,
  options: {},
};
RiskSliderField.supportedTypes = ["float", "integer"];
RiskSliderField.extractProps = ({ attrs, options }) => ({
  step: attrs.step,
  options: options || {},
});

registry.category("fields").add("risk_slider", RiskSliderField);
