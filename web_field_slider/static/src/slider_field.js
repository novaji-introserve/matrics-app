/** @odoo-module **/

import {registry} from '@web/core/registry';
import {standardFieldProps} from '@web/views/fields/standard_field_props';
import {Component, useState} from '@odoo/owl';
import {useInputField} from '@web/views/fields/input_field_hook';

export class SliderField extends Component {

    setup() {
        useInputField({
            getValue: () => this.props.value,
            refName: 'input',
        });
        this.state = useState({
            value: this.props.value,
        })
    }

    _onInput() {
        this.props.update(this.inputRef.el.value);
    }

    eval(expression) {
        return py.eval(expression, this.props.record.evalContext)
    }

}

SliderField.template = 'web_field_slider.SliderField';

SliderField.props = {
    ...standardFieldProps,
    step: {type: String, optional: true},
    min: {type: String, optional: true},
    max: {type: String, optional: true},
};

SliderField.defaultProps = {
    step: 1,
    min: 0,
    max: 100,
};

SliderField.isEmpty = () => false;
SliderField.extractProps = ({attrs, field}) => {
    return {
        step: attrs.step,
        min: attrs.min,
        max: attrs.max,
    };
};

registry.category('fields').add('slider', SliderField);
