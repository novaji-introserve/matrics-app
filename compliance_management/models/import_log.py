from odoo import models, fields, api
import logging
from datetime import datetime

_logger = logging.getLogger(__name__)

class ImportLog(models.Model):
    _name = "import.log"
    _description = "Import Log"
    _order = "create_date desc"
    _inherit = ["mail.thread", "mail.activity.mixin"]

    name = fields.Char(string="Name", required=True, default="New Import")
    file_name = fields.Char(string="File Name")
    original_filename = fields.Char(string="Original Filename")
    content_type = fields.Char(string="Content Type")
    
    # Fix: Use ondelete='cascade' since the field is required
    ir_model_id = fields.Many2one(
        "ir.model", string="Target Model", required=True, ondelete="cascade"
    )
    model_name = fields.Char(
        related="ir_model_id.model", string="Model Name", store=True
    )
    file = fields.Binary(string="File", attachment=True)
    total_records = fields.Integer(string="Total Records", default=0)
    successful_records = fields.Integer(string="Successful Records", default=0)
    failed_records = fields.Integer(string="Failed Records", default=0)
    status = fields.Selection(
        [
            ("pending", "Pending"),
            ("processing", "Processing"),
            ("completed", "Completed"),
            ("failed", "Failed"),
        ],
        string="Status",
        default="pending",
        tracking=True,
    )
    uploaded_by = fields.Many2one(
        "res.users", string="Uploaded By", default=lambda self: self.env.user.id
    )
    create_date = fields.Datetime(string="Created Date", readonly=True)
    completed_at = fields.Datetime(string="Completed At")
    error_message = fields.Text(string="Error Message")
    technical_details = fields.Text(string="Technical Details")
    batch_folder = fields.Char(string="Batch Folder")
    duplicate_records = fields.Integer(string="Duplicate Records", default=0)
    mapping_ids = fields.One2many(
        "import.field.mapping", "import_log_id", string="Field Mappings"
    )

    @api.model_create_multi
    def create(self, vals_list):
        for vals in vals_list:
            if not vals.get("name") or vals.get("name") == "New Import":
                vals["name"] = (
                    f"Import {self.env['ir.sequence'].next_by_code('import.log.sequence') or 'New'}"
                )
        return super(ImportLog, self).create(vals_list)

    def process_file(self):
        """Process the imported file"""
        self.ensure_one()
        # Update status to processing
        self.write({"status": "processing"})

        # Import the CSV processor
        try:
            from ..services.csv_processor import CSVProcessor

            processor = CSVProcessor(self)
            result = processor.process()

            # Update the import log with results
            if result.get("success"):
                self.write(
                    {
                        "status": "completed",
                        "total_records": result.get("total_records", 0),
                        "successful_records": result.get("successful_records", 0),
                        "failed_records": result.get("failed_records", 0),
                        "duplicate_records": result.get("duplicate_records", 0),
                        "completed_at": fields.Datetime.now(),
                    }
                )
                return True
            else:
                self.write(
                    {
                        "status": "failed",
                        "error_message": result.get("error_message", "Unknown error"),
                        "technical_details": result.get("technical_details", ""),
                        "completed_at": fields.Datetime.now(),
                    }
                )
                return False

        except Exception as e:
            import traceback

            _logger.error(f"Error processing file: {str(e)}")
            self.write(
                {
                    "status": "failed",
                    "error_message": str(e),
                    "technical_details": traceback.format_exc(),
                    "completed_at": fields.Datetime.now(),
                }
            )
            return False

    def get_importable_fields(self):
        """Get fields that can be imported for this model"""
        self.ensure_one()

        # Get all fields for the model that can be imported
        model_obj = self.env[self.model_name]
        importable_fields = []

        for field_name, field in model_obj._fields.items():
            # Skip non-storable fields, many2many fields, one2many fields and compute fields without inverse
            if (
                not field.store
                or field.type in ["many2many", "one2many", "binary", "reference"]
                or (field.compute and not field.inverse)
            ):
                continue

            field_info = {
                "name": field_name,
                "string": field.string,
                "type": field.type,
                "required": field.required,
                "relation": (
                    field.comodel_name
                    if field.type in ["many2one", "many2many"]
                    else False
                ),
            }
            importable_fields.append(field_info)

        return importable_fields

class ImportFieldMapping(models.Model):
    _name = "import.field.mapping"
    _description = "Import Field Mapping"

    import_log_id = fields.Many2one(
        "import.log", string="Import Log", ondelete="cascade"
    )
    csv_field = fields.Char(string="CSV Field", required=True)
    model_field = fields.Char(string="Model Field", required=True)
    field_type = fields.Char(string="Field Type")
    default_value = fields.Char(string="Default Value")
    required = fields.Boolean(string="Required")
    notes = fields.Text(string="Notes")

# from odoo import models, fields, api
# from datetime import datetime
# import logging

# _logger = logging.getLogger(__name__)

# """This code defines a Python class called `ImportModel` which is a subclass of `models.Model`. It represents an importable model with various fields and functionalities.

# Attributes:
#     _name (str): The name of the model.
#     _description (str): The description of the model.
#     _order (str): The default sorting order for the records of this model.

# Fields:
#     name (Char): The name of the importable model.
#     model_id (Many2one): A reference to the associated `ir.model` record.
#     model_name (Char): The technical name of the associated model.
#     description (Text): The description of the importable model.
#     active (Boolean): Indicates whether the model is active or not.
#     field_ids (Many2many): The required fields for the importable model.
#     template_file (Binary): The template file associated with the model.
#     template_filename (Char): The name of the template file.

# Methods:
#     _onchange_model_id: An `@api.onchange` decorator method that updates the name of the importable model based on the selected `model_id`.
#     get_importable_fields: Retrieves the importable fields for the model.

# SQL Constraints:
#     unique_model: Enforces a unique constraint on the `model_id` field, ensuring that the model is not already configured for import.

# Note: This code is written in Python and is part of a larger application or module. It utilizes the `models.Model` class from an external library or framework."""
# class CsvImportModel(models.Model):
#     _name = "csv.import.model"
#     _description = "Importable Model"
#     _order = "name"

#     name = fields.Char(string="Name", required=True)
#     model_id = fields.Many2one(
#         "ir.model", string="Model", required=True, ondelete="cascade"
#     )
#     model_name = fields.Char(related="model_id.model", string="Technical Name")
#     description = fields.Text(string="Description")
#     active = fields.Boolean(string="Active", default=True)
#     field_ids = fields.Many2many(
#         "ir.model.fields",
#         string="Required Fields",
#         domain="[('model_id', '=', model_id)]",
#     )
#     template_file = fields.Binary(string="Template File", attachment=True)
#     template_filename = fields.Char(string="Template Filename")

#     _sql_constraints = [
#         (
#             "unique_model",
#             "unique(model_id)",
#             "This model is already configured for import!",
#         )
#     ]

#     @api.onchange("model_id")
#     def _onchange_model_id(self):
#         if self.model_id:
#             self.name = self.model_id.name

#     def get_importable_fields(self):
#         """Get fields that can be imported for this model"""
#         self.ensure_one()

#         # Get all fields for the model that can be imported
#         model_obj = self.env[self.model_name]
#         importable_fields = []

#         for field_name, field in model_obj._fields.items():
#             # Skip non-storable fields, many2many fields, one2many fields and compute fields without inverse
#             if (
#                 not field.store
#                 or field.type in ["many2many", "one2many", "binary", "reference"]
#                 or (field.compute and not field.inverse)
#             ):
#                 continue

#             field_info = {
#                 "name": field_name,
#                 "string": field.string,
#                 "type": field.type,
#                 "required": field.required,
#                 "relation": (
#                     field.comodel_name
#                     if field.type in ["many2one", "many2many"]
#                     else False
#                 ),
#             }
#             importable_fields.append(field_info)

#         return importable_fields


# class ImportLog(models.Model):
#     _name = "import.log"
#     _description = "Import Log"
#     _order = "create_date desc"
#     _inherit = ["mail.thread", "mail.activity.mixin"]

#     name = fields.Char(string="Name", required=True, default="New Import")
#     file_name = fields.Char(string="File Name")
#     original_filename = fields.Char(string="Original Filename")
#     content_type = fields.Char(string="Content Type")
#     import_model_id = fields.Many2one(
#         "csv.import.model", string="Import Model", required=True
#     )
#     model_name = fields.Char(
#         related="import_model_id.model_name", string="Model Name", store=True
#     )
#     file = fields.Binary(string="File", attachment=True)
#     total_records = fields.Integer(string="Total Records", default=0)
#     successful_records = fields.Integer(string="Successful Records", default=0)
#     failed_records = fields.Integer(string="Failed Records", default=0)
#     status = fields.Selection(
#         [
#             ("pending", "Pending"),
#             ("processing", "Processing"),
#             ("completed", "Completed"),
#             ("failed", "Failed"),
#         ],
#         string="Status",
#         default="pending",
#         tracking=True,
#     )
#     uploaded_by = fields.Many2one(
#         "res.users", string="Uploaded By", default=lambda self: self.env.user.id
#     )
#     create_date = fields.Datetime(string="Created Date", readonly=True)
#     completed_at = fields.Datetime(string="Completed At")
#     error_message = fields.Text(string="Error Message")
#     technical_details = fields.Text(string="Technical Details")
#     batch_folder = fields.Char(string="Batch Folder")
#     duplicate_records = fields.Integer(string="Duplicate Records", default=0)
#     mapping_ids = fields.One2many(
#         "import.field.mapping", "import_log_id", string="Field Mappings"
#     )

#     @api.model_create_multi
#     def create(self, vals_list):
#         for vals in vals_list:
#             if not vals.get("name") or vals.get("name") == "New Import":
#                 vals["name"] = (
#                     f"Import {self.env['ir.sequence'].next_by_code('import.log.sequence') or 'New'}"
#                 )
#         return super(ImportLog, self).create(vals_list)

#     def process_file(self):
#         """Process the imported file"""
#         self.ensure_one()
#         # Update status to processing
#         self.write({"status": "processing"})

#         # Import the CSV processor
#         try:
#             from ..services.csv_processor import CSVProcessor

#             processor = CSVProcessor(self)
#             result = processor.process()

#             # Update the import log with results
#             if result.get("success"):
#                 self.write(
#                     {
#                         "status": "completed",
#                         "total_records": result.get("total_records", 0),
#                         "successful_records": result.get("successful_records", 0),
#                         "failed_records": result.get("failed_records", 0),
#                         "duplicate_records": result.get("duplicate_records", 0),
#                         "completed_at": fields.Datetime.now(),
#                     }
#                 )
#                 return True
#             else:
#                 self.write(
#                     {
#                         "status": "failed",
#                         "error_message": result.get("error_message", "Unknown error"),
#                         "technical_details": result.get("technical_details", ""),
#                         "completed_at": fields.Datetime.now(),
#                     }
#                 )
#                 return False

#         except Exception as e:
#             import traceback

#             _logger.error(f"Error processing file: {str(e)}")
#             self.write(
#                 {
#                     "status": "failed",
#                     "error_message": str(e),
#                     "technical_details": traceback.format_exc(),
#                     "completed_at": fields.Datetime.now(),
#                 }
#             )
#             return False

# """
# This code defines a class called `ImportFieldMapping` which is a model in a Python framework (possibly Odoo). It represents the mapping between fields in a CSV file and fields in a model.

# Attributes:
# - `_name`: The name of the model, which is "import.field.mapping".
# - `_description`: A description of the model, which is "Import Field Mapping".
# - `import_log_id`: A many-to-one field that relates the `ImportFieldMapping` to an `import.log` model. The attribute `string` is set to "Import Log" and `ondelete` is set to "cascade".
# - `csv_field`: A character field that represents the CSV field. The attribute `string` is set to "CSV Field" and `required` is set to `True`.
# - `model_field`: A character field that represents the model field. The attribute `string` is set to "Model Field" and `required` is set to `True`.
# - `field_type`: A character field that represents the type of the field.
# - `default_value`: A character field that represents the default value of the field.
# - `required`: A boolean field that indicates whether the field is required or not.
# - `notes`: A text field that contains additional notes.

# Note: This code is incomplete and may require additional imports and class definitions to function properly.
# """
# class ImportFieldMapping(models.Model):
#     _name = "import.field.mapping"
#     _description = "Import Field Mapping"

#     import_log_id = fields.Many2one(
#         "import.log", string="Import Log", ondelete="cascade"
#     )
#     csv_field = fields.Char(string="CSV Field", required=True)
#     model_field = fields.Char(string="Model Field", required=True)
#     field_type = fields.Char(string="Field Type")
#     default_value = fields.Char(string="Default Value")
#     required = fields.Boolean(string="Required")
#     notes = fields.Text(string="Notes")
