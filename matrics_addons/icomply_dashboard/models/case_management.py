# -*- coding: utf-8 -*-

from odoo import models, fields, api


class CaseManagement(models.Model):
    _name = 'case.management'
    _description = 'case management'
    _rec_name = 'create_date'
    
    user_id = fields.Many2one("res.users", string="user", required=True)
    case_status_id = fields.Many2one('case.status', string="status", required=True)
    priority_level_id = fields.Many2one("case.rating", string="priority", required=True)
    description = fields.Text(string="description")
    reason_for_close = fields.Text(string="reason")
    created_at = fields.Datetime(string="created_at", default=fields.Datetime.now())
    updated_at = fields.Datetime(string="updated_at", default=fields.Datetime.now())
    assigned_user = fields.Many2one("res.users", required=True)
    case_action = fields.Text(string="action")
    department_id = fields.Many2one("hr.department")
    process_categoryid = fields.Integer()
    assigned_user_response = fields.Text(string="assigned_user_response")
    alert_id = fields.Integer()
    supervisor_1 = fields.Many2one("res.users")
    supervisor_2 = fields.Many2one("res.users")
    supervisor_3 = fields.Many2one("res.users")
    event_date = fields.Datetime(required=True)
    title = fields.Char(string="title", required=True)
    exception_process_id = fields.Integer(required=True)
    process_id = fields.Integer()
    attachment = fields.Binary(string="attachment")
    attachment_filename = fields.Char(string="filename")
    exception_log_id = fields.Integer()
    tran_id = fields.Char()
    customer_id = fields.Char()
    assigned_user_response1 = fields.Char()
    staff_dept = fields.Char()
    attachment_download_link = fields.Char()
    branch_code = fields.Char()
    data_source = fields.Char()
    root_category = fields.Integer()
    root_category_processes = fields.Integer()
    _sql_constraints = [
        ('ref_unique', 'UNIQUE(ref)', 'the ref must be unique'),
    ]
    
 