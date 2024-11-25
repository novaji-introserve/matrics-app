/** @odoo-module **/

import { registry } from "@web/core/registry";
import { EmailField } from "@web/views/fields/email_field";
import { useRef } from "@odoo/owl";


class TextFieldWidget extends EmailField {

    setup(){
        super.setup();
        this.htmlRef = useRef(null);
        alert('hello world');
        
    }

}

TextFieldWidget.supportedTypes = ["text"]
registry.category("fields").add('htmlwidget', TextFieldWidget)