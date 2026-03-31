from odoo import fields, models


class ChangeDataCapture(models.Model):
    _name = "change.data.capture"
    _description = "Change Data Capture"
    _order = "create_date desc, id desc"

    name = fields.Char(string="Name", required=True)
    model = fields.Char(string="Model", required=True, index=True)
    res_id = fields.Integer(string="Record ID", required=True, index=True)
    field_name = fields.Char(string="Field Name", required=True, index=True)
    old_val = fields.Text(string="Old Value")
    new_val = fields.Text(string="New Value")
    updated_at = fields.Datetime(
        string="Updated At",
        required=True,
        default=fields.Datetime.now,
        index=True,
    )
