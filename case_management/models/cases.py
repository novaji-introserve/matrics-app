from odoo import models, fields,api


class Case(models.Model):
    _name = "case.management"
    _description = "Case Management"

    title = fields.Char(string="Title", required=True)
    description = fields.Text(string="Case Description")
    created_at = fields.Datetime(
        string="Created At", default=fields.Datetime.now, readonly=True
    )
    updated_at = fields.Datetime(
        string="Updated At", default=fields.Datetime.now, readonly=True
    )
    # status_id = fields.Many2one("case.status", string="Status")
    # alert_id = fields.Many2one("alert", string="Alert")
    # team_id = fields.Many2one("hr.team", string="Team")
    # staff_id = fields.Many2one("hr.employee", string="Staff")
    action = fields.Char(string="Case Action")
    user_id = fields.Many2one("res.users", string="User")
    event_date = fields.Datetime(string="Event Date")
    rating_id = fields.Many2one("case.rating", string="Rating")
    # exception_process_id = fields.Many2one(
    #     "exception.process", string="Exception Process"
    # )
    response_note = fields.Text(string="Response Note")

    # Missing Fields
    count = fields.Integer(string="Count")
    # category_id = fields.Many2one("case.category", string="Category")
    open_cases = fields.Integer(string="Open Cases", compute="_compute_open_cases")
    avg_resolution_time = fields.Float(
        string="Avg Resolution Time", compute="_compute_avg_resolution_time"
    )
    case_count = fields.Integer(string="Case Count")

    # @api.depends("staff_id")
    def _compute_open_cases(self):
        pass
        # for record in self:
        #     open_cases = self.env["case.management.case"].search_count(
        #         [("staff_id", "=", record.staff_id.id), ("status_id.name", "=", "Open")]
        #     )
        #     record.open_cases = open_cases

    # @api.depends("staff_id")
    def _compute_avg_resolution_time(self):
        # for record in self:
        #     cases = self.env["case.management.case"].search(
        #         [
        #             ("staff_id", "=", record.staff_id.id),
        #             ("status_id.name", "=", "Closed"),
        #         ]
        #     )
            pass
            # total_resolution_time = sum([case.resolution_time for case in cases])
            # record.avg_resolution_time = (
            #     total_resolution_time / len(cases) if cases else 0.0
            # )

    # @api.depends("category_id")
    # def _compute_case_count(self):
    #     pass
        # for record in self:
        #     case_count = self.env["case.management.case"].search_count(
        #         [("category_id", "=", record.category_id.id)]
        #     )
        #     record.case_count = case_count

    # # Methods
    # @api.depends("status_id", "rating_id", "category_id")
    # def _compute_case_count(self):
    #     """Computes the count of cases for dashboard charts."""
    #     for record in self:
    #         # Logic to compute count, this can be updated depending on your needs
    #         record.count = self.env["case.management"].search_count(
    #             [
    #                 ("status_id", "=", record.status_id.id),
    #                 ("rating_id", "=", record.rating_id.id),
    #                 ("category_id", "=", record.category_id.id),
    #             ]
    #         )
