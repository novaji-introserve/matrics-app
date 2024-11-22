odoo.define("internal_control.tinymce_custom", function (require) {
  "use strict";

  const fieldRegistry = require("web.field_registry");
  const FieldHtml = fieldRegistry.get("html");
  const core = require("web.core");

  const QWeb = core.qweb;

  alert(87678)

  const TinyMCEField = FieldHtml.extend({
    start: function () {
      // Call parent start method
      const result = this._super.apply(this, arguments);

      // Initialize TinyMCE after the field is rendered
      this.$textarea = this.$el.find("textarea");
      this.initTinyMCE();

      return result;
    },

    initTinyMCE: function () {
      // Destroy TinyMCE if it already exists
      if (tinymce.get(this.$textarea.attr("id"))) {
        tinymce.remove(`#${this.$textarea.attr("id")}`);
      }

      // Initialize TinyMCE
      tinymce.init({
        target: this.$textarea[0],
        height: 400,
        menubar: false,
        plugins: [
          "advlist autolink lists link image charmap print preview anchor",
          "searchreplace visualblocks code fullscreen",
          "insertdatetime media table paste code help wordcount",
        ],
        toolbar:
          "undo redo | formatselect | bold italic backcolor | \
                          alignleft aligncenter alignright alignjustify | \
                          bullist numlist outdent indent | removeformat | help",
      });
    },

    destroy: function () {
      // Destroy TinyMCE instance when field is destroyed
      if (tinymce.get(this.$textarea.attr("id"))) {
        tinymce.remove(`#${this.$textarea.attr("id")}`);
      }
      this._super.apply(this, arguments);
    },
  });

  fieldRegistry.add("html", TinyMCEField);
});
