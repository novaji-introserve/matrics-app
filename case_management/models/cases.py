from odoo import models, fields, api
from odoo.exceptions import UserError
from datetime import timedelta
import logging
import uuid


_logger = logging.getLogger(__name__)




class CaseResponse(models.Model):
    _name = 'case.response'
    _description = 'Case Responses'
    _order = 'create_date desc'
    _rec_name = 'response' 
    
    case_id = fields.Many2one('case', string='Case', required=True, ondelete='cascade')
    response = fields.Text(string='Response', required=True)
    #quick_response = fields.Text(string="Response",  store=False)  # Transient field, not stored in database


    create_date = fields.Datetime(string='Response Date', default=fields.Datetime.now, readonly=True)
    #response_date = fields.Datetime(string='Response Date', default=fields.Datetime.now)
    create_uid = fields.Many2one('res.users', string='Responder', readonly=True)
    
    
    @api.model
    def default_get(self, fields):
        defaults = super().default_get(fields)
        active_id = self.env.context.get('default_case_id') or self.env.context.get('active_id')
        if active_id:
            case = self.env['case'].browse(active_id)
            if case.quick_response:
                defaults['response'] = case.quick_response
        return defaults



class CaseResponseWizard(models.TransientModel):
    _name = 'case.response.wizard'
    _description = 'Add Response to Case'
    
    case_id = fields.Many2one('case', string='Case', required=True)
    response = fields.Text(string='Response', required=True)
    
    # Using the quick_response from the case model, read-only
    quick_response = fields.Text(
        string="Quick Response",
        related='case_id.quick_response',
        readonly=True  # Set to True if you don't want it editable
    )
    
    def action_submit_response(self):
        self.ensure_one()

        # Check if user is the assigned staff
        if self.env.user != self.case_id.staff_id:
            raise UserError("Only the assigned staff can add responses to this case.")

        try:
            # Get the quick_response from the related case
            quick_response_text = self.case_id.quick_response

            # Create a response record using the case's quick_response
            response = self.env['case.response'].create({
                'case_id': self.case_id.id,
                'response': quick_response_text,  # Store quick_response in response
            })

            _logger.info(f"Response created from wizard: {response.id}")

            # Invalidate and recompute
            self.case_id.invalidate_cache()
            self.case_id._compute_has_responses()

            # Optional: send alert
            # try:
            #     alert_sent = self.case_id._send_case_response_alert(response)
            #     if alert_sent:
            #         _logger.info(f"Response alert sent successfully from wizard for case {self.case_id.id}")
            #     else:
            #         _logger.warning(f"Failed to send response alert from wizard for case {self.case_id.id}")
            # except Exception as e:
            #     _logger.error(f"Error sending response alert from wizard for case {self.case_id.id}: {str(e)}")

            # Log in chatter
            self.case_id.message_post(
                body=f"<p>New response added:</p><p>{quick_response_text}</p>",
                subtype_xmlid='mail.mt_note'
            )

            return {
                'type': 'ir.actions.client',
                'tag': 'reload',
            }

        except Exception as e:
            _logger.error(f"Error in wizard action_submit_response for case {self.case_id.id}: {str(e)}")
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'message': f"Error submitting response: {str(e)}",
                    'type': 'danger',
                    'sticky': True,
                }
            }









class Cases(models.Model):
    _name = 'case'
    _description = 'Case'
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _rec_name = 'name'
    _order = 'created_at desc'

    # Basic Info
    name = fields.Char(string="Name", compute='_compute_name', store=True, tracking=True)
    # Reference to Transaction model from compliance_management module
    transaction_id = fields.Many2one(
        comodel_name='res.customer.transaction',  # This is the _name of the Transaction model
        string='Related Transaction',
        index=True
    )
    
    # Related field to access transaction name
    transaction_reference = fields.Char(
        string='Transaction Reference Number',
        related='transaction_id.name',
        store=True,  # Optional: store the value in database
        readonly=False
    )
    #transaction_reference = fields.Char(string='Transaction Reference')
    #transaction_reference = fields.Many2one('res.customer.transaction', string='Transaction Reference', required=False)
   #cases_description = fields.Text(string='Narration', compute='_compute_description', store=True)
    cases_description = fields.Text(string='Narration', required = False)
    further_description = fields.Html(string='Further Description')
    data_source = fields.Text(string="Data Source", required=False)
    customer_id = fields.Many2one('res.partner', string='Customer', required=False)
    # attachment_ids = fields.Many2many('ir.attachment', string='Attachments')
    # attachment_ids = fields.Binary(string="Attachment", required=False)
    attachment = fields.Binary(string="Attachment")
    filename = fields.Char(string="Filename")


    # Responses
    response_ids = fields.One2many('case.response', 'case_id', string='Responses')
    has_responses = fields.Boolean(string='Has Responses', compute='_compute_has_responses', store=True)
    
    quick_response = fields.Text(string="Quick Response", 
                                store=False)  # Transient field, not stored in database


    # Severity
    title = fields.Selection([
        ('1', 'Low'),
        ('2', 'Medium'),
        ('3', 'High')
    ], string='Severity', required=True)
    title_html = fields.Html(string='Severity Badge', compute='_compute_rating_html', sanitize=False)
    rating_id = fields.Many2one('case.rating', string='Severity Rating', readonly=True)
    severity_level = fields.Integer(string="Severity Level", compute="_compute_severity_level", store=True)
    

    # Status
    status_id = fields.Many2one('case.status', string='Status', required=False, default=lambda self: self._default_status())
    status_name = fields.Char(string="Status Name", compute="_compute_status_name", store=True)
    statuses = fields.Selection(related='status_id.name', string='Status Name', store=True) # for a modification in created by me
    status_html = fields.Html(compute='_compute_status_html', string='Status')
    status_code = fields.Selection(related='status_id.name', store=True, string='Status Code')


    # Dates
    created_at = fields.Datetime(string='Created_at', default=fields.Datetime.now)
    event_date = fields.Datetime(string='Event Date', required=True)
    updated_at = fields.Datetime(string='Updated At')
    start_date = fields.Datetime(string="Start Date", compute="_compute_start_date", store=True)
    end_date = fields.Datetime(string="End Date", compute="_compute_end_date", store=True)
    current_date = fields.Datetime(string='Current Date', compute='_compute_current_date')
    created_at_formatted_datetime = fields.Char(string='Case Creation Date', compute='_compute_created_at_formatted_datetime')
    event_date_formatted_datetime = fields.Char(string='Date of Event', compute='_compute_event_date_formatted_datetime')


    # Relations
    branch_id = fields.Many2one('branch', string='Branch')
    staff_id = fields.Many2one('res.users', string='Staff', required=True)
    team_id = fields.Many2one('hr.department', string='Team / Department')
    team_id = fields.Many2one('hr.department', string='Department', compute='_compute_team_id', store=True)
    branch_id = fields.Many2one('res.branch', string='Branch', ondelete='set null')
    staff_user_id = fields.Many2one('res.users', string='Staff User', related='staff_id.user_id', store=True, readonly=True)
    user_id = fields.Many2one('res.users', string='Created By', default=lambda self: self.env.user, readonly=True)
    supervisor_one_id = fields.Many2one('res.users', string='Supervisor One', required=True)
    supervisor_two_id = fields.Many2one('res.users', string='Supervisor Two')
    supervisor_three_id = fields.Many2one('res.users', string='Supervisor Three')
    alert_id = fields.Many2one('alert', string='Alert')
    
    
    new_process_category_id = fields.Many2one('exception.process.type', string='Exception Process Type')
    new_process_id = fields.Many2one('exception.process', string='Exception Process')
    
#     new_process_category_id = fields.Many2one('exception.process.type', 
#                                              string='Exception Process Type')
#     new_process_id = fields.Many2one('exception.process', 
#                                     string='Exception Process',
#                                     domain="[('type_id', '=', new_process_category_id.num_id)]")
#     new_process_category_num_id = fields.Float(
#     related='new_process_category_id.num_id',
#     store=True
# )


    # new_process_category_id = fields.Many2one('exception.process.type', string='Exception Process Type')
    # new_process_id = fields.Many2one('exception.process', string='Exception Process')
    # new_process_id = fields.Many2one('exception.process', string='Exception Process')
    # new_process_category_id = fields.Many2one('exception.process.type', string='Exception Process Type')
    process_category_id = fields.Many2one('exception.category', string='Exception Process Type')
    process_id = fields.Many2one('exception.process', string='Exception Process')
    root_category_id = fields.Many2one('exception.process.type', string='Root Category')
    root_category_process_id = fields.Many2one('exception.process.type', string='Root Category Process')

    # Actions & Notes
    cases_action = fields.Text(string='Action (Text)', required=True)

    # UI Helper Fields
    is_creator = fields.Boolean(compute='_compute_user_roles', compute_sudo=False)
    is_assigned_staff = fields.Boolean(compute='_compute_user_roles', compute_sudo=False)
    is_supervisor = fields.Boolean(compute='_compute_is_supervisor', store=False)
    response_text = fields.Text(string="Response", compute="_compute_latest_response", store=False)

    
    
        
        
    
    # is_creator = fields.Boolean(compute='_compute_user_roles', store=True)
    # is_assigned_staff = fields.Boolean(compute='_compute_user_roles', store=True)
    
    
    def _create_quick_response(self, note):
        self.ensure_one()
        response = self.env['case.response'].create({
            'case_id': self.id,
            'response': note,
            'create_uid': self.env.uid,
        })

        self.message_post(
            body=f"<p>Quick response added:</p><p>{note}</p>",
            subtype_xmlid='mail.mt_note'
        )

        self._compute_has_responses()

        try:
            self._send_case_response_alert(response)
        except Exception as e:
            _logger.warning(f"Failed to send alert for quick response on case {self.id}: {e}")

        
    
    # ### HAS QUICK RESPONSE FIELD ###
    def action_submit_response(self):
        self.ensure_one()
        if self.quick_response and self.is_assigned_staff:
            # Create a new response record
            self.env['case.response'].create({
                'case_id': self.id,
                'response': self.quick_response,
            })
            # Clear the quick response field
            self.quick_response = False
            # The has_responses field will be computed automatically via the dependency
            # Force a recomputation of the field
            self._compute_has_responses()
            return {
                'type': 'ir.actions.client',
                'tag': 'reload',
            }
        return True
        
    
    #------Formatted Date and Time for human readability------#
    
    def get_created_at_formatted_datetime(self):
        self.ensure_one()
        if self.created_at:
            day = self.created_at.strftime('%d')
            if day[-1] == '1' and day[-2:] not in ('11', '12', '13'):
                suffix = 'st'
            elif day[-1] == '2' and day[-2:] not in ('11', '12', '13'):
                suffix = 'nd'
            elif day[-1] == '3' and day[-2:] not in ('11', '12', '13'):
                suffix = 'rd'
            else:
                suffix = 'th'
            formatted_datetime = self.created_at.strftime(f'%B, %Y at %I:%M%p').replace(' 0', ' ')
            return f"{day}{suffix} {formatted_datetime}"
        return False
    
    def get_event_date_formatted_datetime(self):
        self.ensure_one()
        if self.event_date:
            day = self.event_date.strftime('%d')
            if day[-1] == '1' and day[-2:] not in ('11', '12', '13'):
                suffix = 'st'
            elif day[-1] == '2' and day[-2:] not in ('11', '12', '13'):
                suffix = 'nd'
            elif day[-1] == '3' and day[-2:] not in ('11', '12', '13'):
                suffix = 'rd'
            else:
                suffix = 'th'
            formatted_datetime = self.event_date.strftime(f'%B, %Y at %I:%M%p').replace(' 0', ' ')
            return f"{day}{suffix} {formatted_datetime}"
        return False

    # ------------------- COMPUTES -------------------
    
    @api.depends('staff_id')
    def _compute_team_id(self):
        for rec in self:
            employee = self.env['hr.employee'].search([('user_id', '=', rec.staff_id.id)], limit=1)
            rec.team_id = employee.department_id.id if employee and employee.department_id else False

    @api.depends('response_ids')
    def _compute_latest_response(self):
        for rec in self:
            if rec.response_ids:
                rec.response_text = rec.response_ids[-1].response  # or [0] for first
            else:
                rec.response_text = ''

    @api.depends('supervisor_one_id', 'supervisor_two_id', 'supervisor_three_id')
    def _compute_is_supervisor(self):
        for record in self:
            user = self.env.user
            record.is_supervisor = user.id in [record.supervisor_one_id.id, 
                                              record.supervisor_two_id.id, 
                                              record.supervisor_three_id.id]
        
    @api.depends('created_at')
    def _compute_created_at_formatted_datetime(self):
        for record in self:
            record.created_at_formatted_datetime = record.get_created_at_formatted_datetime()
            
    @api.depends('event_date')
    def _compute_event_date_formatted_datetime(self):
        for record in self:
            record.event_date_formatted_datetime = record.get_event_date_formatted_datetime()

    @api.depends('title')
    def _compute_name(self):
        for rec in self:
            label = dict(self._fields['title'].selection).get(rec.title, '')
            rec.name = f"{label} Case"

    @api.depends('title')
    def _compute_description(self):
        for rec in self:
            rec.cases_description = {
                '1': 'This is a Low case',
                '2': 'This is a Medium case',
                '3': 'This is a High case'
            }.get(rec.title, '')

    #@api.depends('title')
    @api.depends('rating_id.ref', 'title')
    def _compute_severity_level(self):
        for rec in self:
            rec.severity_level = rec.rating_id.ref or 0
            
            # _logger.info("Computed severity level for record %s: %s", rec.id, rec.severity_level)
        
            print(f"Computed severity level for record {rec.id}: {rec.severity_level}")

    @api.depends('status_id')
    def _compute_status_name(self):
        for rec in self:
            rec.status_name = rec.status_id.name if rec.status_id else ''
            print(rec.status_name)
            print(rec.status_id.name)
            
            
    @api.depends('status_id')
    def _compute_status_html(self):
        for rec in self:
            color = {
                'overdue': 'danger',
                'closed': 'success',
                'open': 'primary',
            }.get(rec.status_id.name, 'warning')
            status = rec.status_id.name.capitalize() if rec.status_id else 'Unknown'
            rec.status_html = f'<span class="badge bg-{color}" style="color: white; font-weight: bold;">{status}</span>'
            
    @api.depends('title')
    def _compute_rating_html(self):
        for rec in self:
            color = {
                '1': 'success',
                '2': 'warning',
                '3': 'danger'
            }.get(rec.title, 'primary')
            title_label = dict(self.fields_get(allfields=['title'])['title']['selection']).get(rec.title, 'Unknown')

            rec.title_html = (
                f'<span class="badge bg-{color}" '
                f'style="color: white; font-weight: bold;">{title_label}</span>'
            )
    # @api.depends('status_id')
    # def _compute_status_html(self):
    #     for rec in self:
    #         color = {
    #             'overdue': 'danger',
    #             'closed': 'success',
    #             'open': 'primary',
    #         }.get(rec.status_id.name, 'warning')
    #         status = rec.status_id.name.capitalize() if rec.status_id else 'Unknown'
    #         rec.status_html = f'<span class="badge bg-{color}">{status}</span>'

    @api.depends('created_at')
    def _compute_start_date(self):
        for rec in self:
            rec.start_date = rec.created_at or fields.Datetime.now()

    @api.depends('start_date')
    def _compute_end_date(self):
        for rec in self:
            rec.end_date = rec.start_date + timedelta(hours=1) if rec.start_date else False

    @api.depends()
    def _compute_current_date(self):
        now = fields.Datetime.now()
        for rec in self:
            rec.current_date = now

    
    @api.depends('response_ids')
    def _compute_has_responses(self):
        for rec in self:
            # Get all responses for this case
            responses = self.env['case.response'].search([('case_id', '=', rec.id)])
            responses_count = len(responses)
            
            # Log the response count
            _logger.info(f"Case {rec.id}: Computing has_responses = {responses_count > 0} (responses count: {responses_count})")
            
            # Store previous value to detect changes
            old_has_responses = rec.has_responses
            
            # Update the has_responses field
            rec.has_responses = responses_count > 0
            
            # Check if this is a new response being added (not just recomputing the field)
            if responses_count > 0 and not old_has_responses:
                _logger.info(f"Case {rec.id}: First response detected - sending notification")
                # Send notification for the newest response
                newest_response = responses.sorted(key=lambda r: r.create_date, reverse=True)[0]
                #rec._send_case_response_alert(newest_response)
            # Or if there's a new response added to existing responses
            elif responses_count > 0 and old_has_responses:
                # Get the most recent response
                newest_response = responses.sorted(key=lambda r: r.create_date, reverse=True)[0]
                
                # Check if this response was just created (within last minute)
                one_minute_ago = fields.Datetime.now() - timedelta(minutes=1)
                if newest_response.create_date >= one_minute_ago:
                    _logger.info(f"Case {rec.id}: New response detected - sending notification for response {newest_response.id}")
                   # rec._send_case_response_alert(newest_response)
                else:
                    _logger.info(f"Case {rec.id}: No new responses detected")
        
        
    
    
    # @api.depends('response_ids')
    # def _compute_has_responses(self):
    #     for rec in self:
    #         responses_count = self.env['case.response'].search_count([('case_id', '=', rec.id)])
    #         rec.has_responses = responses_count > 0
    #         _logger.info(f"Case {rec.id}: Computing has_responses = {rec.has_responses} (responses count: {responses_count})")
    
    
    
        
    # @api.depends('response_ids')
    # def _compute_has_responses(self):
    #     for rec in self:
    #         _logger.info(f"HAS RESPONSES CHECK: {rec.name} has {len(rec.response_ids)} responses")
    #         rec.has_responses = bool(rec.response_ids)
    
    
    # @api.depends('user_id', 'staff_id')
    # def _compute_user_roles(self):
    #     current_user_id = self.env.user.id
    #     for rec in self:
    #         rec.is_creator = current_user_id == rec.user_id.id  # Compare IDs
    #         rec.is_assigned_staff = current_user_id == rec.staff_id.id  # Compare IDs
    
    
    @api.depends('user_id', 'staff_id')
    @api.depends_context('uid')  
    def _compute_user_roles(self):
        current_user_id = self.env.uid  # More reliable than self.env.user.id
        for rec in self:
            rec.is_creator = (rec.user_id.id == current_user_id)
            rec.is_assigned_staff = (rec.staff_id.id == current_user_id)
            _logger.info(f"Computing roles for case {rec.id}: Current user={current_user_id}, "
                        f"Creator={rec.user_id.id} (is_creator={rec.is_creator}), "
                        f"Staff={rec.staff_id.id} (is_assigned_staff={rec.is_assigned_staff})")
        


    # ------------------- ONCHANGES -------------------
    
    
    @api.onchange('new_process_category_id')
    def _onchange_process_category_id(self):
        if self.new_process_category_id:
            domain = [('type_id', '=', self.new_process_category_id.id)]
            return {'domain': {'new_process_id': domain}}
        return {'domain': {'new_process_id': []}}
    
    # @api.onchange('new_process_category_id')
    # def _onchange_process_category_id(self):
    #     """When process type changes, reset the process selection"""
    #     self.new_process_id = False
    
    # @api.onchange('new_process_id')
    # def _onchange_process_id(self):
    #     """When process is selected, ensure the process type matches"""
    #     if self.new_process_id and self.new_process_id.type_id:
    #         matching_type = self.env['exception.process.type'].search(
    #             [('num_id', '=', self.new_process_id.type_id)], limit=1)
    #         if matching_type:
    #             self.new_process_category_id = matching_type.id

    
    # @api.onchange('new_process_category_id')
    # def _onchange_process_category_id(self):
    #     if self.new_process_category_id:
    #         related_processes = self.env['exception.process'].search([
    #             ('type_id', '=', self.new_process_category_id.num_id)
    #         ])
    #         return {'domain': {'new_process_id': [('id', 'in', related_processes.ids)]}}

    # @api.onchange('new_process_id')
    # def _onchange_process_id(self):
    #     if self.new_process_id:
    #         related_type = self.env['exception.process.type'].search([
    #             ('num_id', '=', self.new_process_id.type_id)
    #         ], limit=1)
    #         self.new_process_category_id = related_type


    @api.onchange('title')
    def _onchange_title_set_rating(self):
        if self.title:
            rating = self.env['case.rating'].search([('ref', '=', int(self.title))], limit=1)
            self.rating_id = rating
        else:
            self.rating_id = False

        # Reset supervisors
        self.supervisor_one_id = False
        self.supervisor_two_id = False
        self.supervisor_three_id = False
        
    @api.onchange('user_id')
    def _onchange_user_id(self):
        self._compute_user_roles()

   # Staff & Department auto-population
    @api.onchange('staff_id')
    def _onchange_staff_id(self):
        self._compute_user_roles()
        self._compute_team_id()
        # for rec in self:
        #     # Fetch the employee linked to the selected user (staff)
        #     employee = self.env['hr.employee'].search([('user_id', '=', rec.staff_id.id)], limit=1)
            
        #     # Auto-populate team (department) only if employee found
        #     if employee:
        #         rec.team_id = employee.department_id.id if employee.department_id else False
        #     else:
        #         # Reset the team if no employee is found (in case of incorrect staff selection)
        #         rec.team_id = False

    # @api.onchange('staff_id')
    # def _onchange_staff_id(self):
    #     for record in self:
    #         employee = self.env['hr.employee'].search([('user_id', '=', record.staff_id.id)], limit=1)
    #         record.team_id = employee.department_id.id if employee else False
    # @api.onchange('staff_id')
    # def _onchange_staff_id(self):
    #     for rec in self:
    #         employee = self.env['hr.employee'].search([
    #             ('user_id', '=', rec.staff_id.id)
    #         ], limit=1)
    #         rec.team_id = employee.department_id.id if employee and employee.department_id else False
    

    # ------------------- DEFAULT -------------------

    @api.model
    def _default_status(self):
        return self.env['case.status'].search([('name', '=', 'open')], limit=1).id

    # ------------------- ACTIONS -------------------
    
    
    
    def action_open_cases(self):
        # Get the current user
        user = self.env.user
        user_id = user.id
        
        # First, fetch ALL open cases regardless of access
        open_cases = self.search([('status_id.name', '=', 'open')])
        
        # Now filter these by user access rights
        staff_accessible = open_cases.filtered(lambda c: c.user_id.id == user_id or c.staff_id.id == user_id)
        
        supervisor_accessible = open_cases.filtered(lambda c: 
            user_id in [c.supervisor_one_id.id, c.supervisor_two_id.id, c.supervisor_three_id.id]
        )
        
        # Combine accessible cases
        accessible_cases = (staff_accessible | supervisor_accessible)
        
        # Double-check we only have open cases
        final_cases = accessible_cases.filtered(lambda c: c.status_id.name == 'open')
        
        # Return the action with a very specific domain
        return {
            'type': 'ir.actions.act_window',
            'name': 'Open Cases',
            'res_model': 'case',
            'view_mode': 'tree,form',
            'domain': [('id', 'in', final_cases.ids), ('status_id.name', '=', 'open')],
            'context': {'accessible_cases': final_cases.ids},
            'views': [(self.env.ref('case_management.view_open_cases_tree').id, 'tree'),
                    (self.env.ref('case_management.view_case_form_staff_only').id, 'form')],
        }
        
    





    def action_open_response_wizard(self):
        """Opens a wizard for the assigned staff to add a response"""
        self.ensure_one()
        
        if self.env.user.id != self.staff_id.id:
            raise UserError("Only the assigned staff can report on this case.")
        
        # if self.env.user != self.staff_id:
        #     raise UserError("Only the assigned staff can report on this case.")
    
    
            
        return {
            'name': 'Add Response to Case',
            'type': 'ir.actions.act_window',
            'res_model': 'case.response.wizard',
            'view_mode': 'form',
            'target': 'new',
            'context': {
                'default_case_id': self.id,
            }
        }
        
    def action_view_closed_cases(self):
        # Get the current user
        user = self.env.user
        user_id = user.id
        
        # Common domain for all searches - only closed cases
        closed_domain = [('status_id.name', '=', 'closed')]
        
        # Get cases the user should be able to access as creator or assigned staff
        staff_cases = self.search(closed_domain + [
            '|',
            ('user_id', '=', user_id),
            ('staff_id', '=', user_id)
        ])
        
        # Get cases where user is a supervisor
        supervisor_cases = self.search(closed_domain + [
            '|', '|',
            ('supervisor_one_id', '=', user_id),
            ('supervisor_two_id', '=', user_id),
            ('supervisor_three_id', '=', user_id)
        ])
        
        # Combine all accessible cases
        accessible_cases = (staff_cases | supervisor_cases).ids
        
        # Return the action with context and domain
        return {
            'type': 'ir.actions.act_window',
            'name': 'Closed Cases',
            'res_model': 'case',
            'view_mode': 'tree,form',
            'domain': [('id', 'in', accessible_cases)],
            'context': {'create': False},
            'views': [(self.env.ref('case_management.case_list_closed_tree_view').id, 'tree'),
                    (self.env.ref('case_management.case_closed_form_view').id, 'form')],
        }



    def action_close_case(self):
        """Closes the case - only available to the creator and when responses exist"""
        self.ensure_one()
        
        if self.env.user.id != self.user_id.id: 
            raise UserError("Only the case creator can close this case.")
            
        if not self.has_responses:
            _logger.info(f"Case {self.id} cannot be closed because the assigned staff has not responded.")
            raise UserError("This case cannot be closed until it has at least one response.")
        
        _logger.info(f"Case {self.id} can be closed because the assigned staff has responded.")
            
        # Try to find the closed status
        closed_status = self.env['case.status'].search([('name', '=', 'closed')], limit=1)
        
        # Create it if it doesn't exist
        if not closed_status:
            try:
                _logger.info("Creating missing 'closed' status")
                closed_status = self.env['case.status'].create({
                    'name': 'closed',
                    'description': 'Closed Case'
                })
            except Exception as e:
                _logger.error(f"Failed to create 'closed' status: {str(e)}")
                raise UserError("Required case status 'closed' could not be created: " + str(e))
        
        self.status_id = closed_status.id
        self.message_post(body="<p>Case has been closed.</p>", subtype_xmlid='mail.mt_note')
        
        # Send email alert
        try:
            self._send_case_closure_alert()
        except Exception as e:
            _logger.error(f"Error sending case closure alert: {str(e)}")
        
        return {
            'type': 'ir.actions.act_window',
            'res_model': 'case',
            'res_id': self.id,
            'view_mode': 'form',
            'target': 'current',
        }












    # def action_close_case(self):
    #     """Closes the case - only available to the creator and when responses exist"""
    #     self.ensure_one()
        
    #     if self.env.user.id != self.user_id.id: 
    #         raise UserError("Only the case creator can close this case.")
            
    #     if not self.has_responses:
    #         _logger.info(f"Case {self.id} cannot be closed because the assigned staff has not responded.")
    #         raise UserError("This case cannot be closed until it has at least one response.")
        
    #     _logger.info(f"Case {self.id} can be closed because the assigned staff has responded.")
            
    #     closed_status = self.env['case.status'].search([('name', '=', 'closed')], limit=1)
    #     if not closed_status:
    #         raise UserError("Required case status 'closed' not found.")
            
    #     self.status_id = closed_status.id
    #     self.message_post(body="<p>Case has been closed.</p>", subtype_xmlid='mail.mt_note')
        
    #     # Send email alert
    #     try:
    #         self._send_case_closure_alert()
    #     except Exception as e:
    #         _logger.error(f"Error sending case closure alert: {str(e)}")
        
        
    #     return {
    #     'type': 'ir.actions.act_window',
    #     'res_model': 'case',
    #     'res_id': self.id,
    #     'view_mode': 'form',
    #     'target': 'current',
    # }
        
        
    def action_add_response(self):
        """Add a response directly from the case form view"""
        _logger.info("===== action_add_response STARTED =====")
        
        for rec in self:
            if rec.response_text:
                _logger.info(f"Processing response for case {rec.id} with text: {rec.response_text[:20]}...")
                
                try:
                    # Create response record
                    new_response = self.env['case.response'].create({
                        'case_id': rec.id,
                        'response': rec.response_text,
                    })
                    _logger.info(f"Response created: {new_response.id} - {new_response.response[:30]}...")
                    
                    # Try to send email alert - THIS IS THE KEY PART
                    _logger.info(f"About to call _send_case_response_alert for case {rec.id}")
                    # try:
                    #     alert_sent = rec._send_case_response_alert(new_response)
                    #     if alert_sent:
                    #         _logger.info(f"Response alert sent successfully for case {rec.id}")
                    #     else:
                    #         _logger.warning(f"Failed to send response alert for case {rec.id}")
                    # except Exception as e:
                    #     _logger.error(f"Error sending response alert for case {rec.id}: {str(e)}")
                    #     import traceback
                    #     _logger.error(f"Traceback: {traceback.format_exc()}")
                    
                    # Log in chatter
                    rec.message_post(
                        body=f"<p>New response added:</p><p>{rec.response_text}</p>",
                        subtype_xmlid='mail.mt_note'
                    )
                    
                    # Clear the response text field
                    rec.response_text = False
                    
                    _logger.info("===== action_add_response COMPLETED SUCCESSFULLY =====")
                    
                except Exception as e:
                    _logger.error(f"Error in action_add_response for case {rec.id}: {str(e)}")
                    import traceback
                    _logger.error(f"Traceback: {traceback.format_exc()}")
        
        return {
            'type': 'ir.actions.client',
            'tag': 'reload',
        }    
        
    

    # ------------------- CRON -------------------

    @api.model
    def cron_check_overdue_cases(self):
        _logger.info("Running cron_check_overdue_cases...")
        
        # Make sure we're operating on the correct model
        case_model = self.env['case']  # Ensure we're using the right model name
        
        # Check available statuses
        status_records = self.env['case.status'].search([])
        _logger.info(f"Available statuses: {status_records.mapped('name')}")
        
        # Find the overdue status
        overdue_status = self.env['case.status'].search([('name', '=', 'overdue')], limit=1)
        if not overdue_status:
            _logger.warning("Overdue status not found!")
            return
        
        # Find the open status to verify it exists
        open_status = self.env['case.status'].search([('name', '=', 'open')], limit=1)
        if not open_status:
            _logger.warning("Open status not found! Check the exact status name.")
            return
        
        deadline = fields.Datetime.now() - timedelta(hours=48)
        
        # Search for cases with the confirmed open status
        open_cases = case_model.search([
            ('status_id', '=', open_status.id),  # Use the ID instead of the name
            ('created_at', '<=', deadline)
        ])
        
        _logger.info(f"Found {len(open_cases)} open cases past deadline.")
        
        if open_cases:
            try:
                open_cases.write({'status_id': overdue_status.id})
                _logger.info(f"Overdue status applied to cases: {open_cases.ids}")
            except Exception as e:
                _logger.error(f"Failed to update cases: {str(e)}")
        else:
            _logger.info("No cases to update.")
        
    
    
    # @api.model
    # def cron_check_overdue_cases(self):
    #     overdue_status = self.env['case.status'].search([('name', '=', 'overdue')], limit=1)
    #     if not overdue_status:
    #         _logger.warning("Overdue status not found!")
    #         return

    #     deadline = fields.Datetime.now() - timedelta(hours=1)
    #     open_cases = self.search([('status_id.name', '=', 'open'), ('created_at', '<=', deadline)])
    #     open_cases.write({'status_id': overdue_status.id})

    # ------------------- OVERRIDES -------------------
    
    @api.model
    def create(self, vals):
        # Ensure creator is properly set to current user
        vals['user_id'] = self.env.user.id

        # Create the record
        record = super(Cases, self).create(vals)

        if vals.get('attachment'):
            self._log_attachment_change(record, 'Created')

        # Flush to database to make sure record truly exists
        record.flush()

        # Make sure key computed fields are up-to-date
        record._compute_user_roles()
        record._compute_has_responses()

        # NEW: Add quick response if provided
        if vals.get('quick_response'):
            record._create_quick_response(vals['quick_response'])

        # Log the creation
        _logger.info(f"Created case {record.id} with creator {record.user_id.name} and assigned staff {record.staff_id.name}")

        # Send email alert with proper error handling
        try:
            success = record._send_case_creation_alert()
            if success:
                _logger.info(f"Successfully sent creation alert for case {record.id}")
            else:
                _logger.warning(f"Failed to send creation alert for case {record.id}")
        except Exception as e:
            _logger.error(f"Error sending case creation alert for case {record.id}: {str(e)}")

        # Trigger a message in the chatter
        record.message_post(
            body=f"<p>Case created by {record.user_id.name}.</p>",
            subtype_xmlid='mail.mt_note'
        )

        return record

        
    
    
    
    
    
    @api.model
    def fields_view_get(self, view_id=None, view_type='form', toolbar=False, submenu=False):
        res = super().fields_view_get(view_id, view_type, toolbar=toolbar, submenu=submenu)
        if self.env.context.get('show_creation_notification'):
            self = self.with_context(show_creation_notification=False)  # reset so it doesn't repeat
            res['arch'] = res['arch'].replace(
                '</form>',
                """
                <script type="text/javascript">
                    odoo.define('case_management.notify', function (require) {
                        var session = require('web.session');
                        var notification = require('web.NotificationService');
                        var core = require('web.core');

                        core.bus.trigger('notification', {
                            message: 'Case created successfully!',
                            type: 'success',
                        });
                    });
                </script>
                </form>
                """
            )
        return res




    def write(self, values):
        # Log changes for attachment field
        if 'attachment' in values:
            for case in self:
                old_attachment = case.attachment
                new_attachment = values.get('attachment')
                if old_attachment != new_attachment:
                    case._log_attachment_change(case, 'Updated')

        # Call super first
        result = super(Cases, self).write(values)

        # Handle quick response after write
        for case in self:
            note = values.get('quick_response')
            if note:
                case._create_quick_response(note)
                case.quick_response = False  # Clear it after saving

        return result


    # def write(self, values):
    #     # Log changes for attachment field
    #     if 'attachment' in values:
    #         for case in self:
    #             # Check if the attachment has changed
    #             old_attachment = case.attachment
    #             new_attachment = values.get('attachment')
    #             if old_attachment != new_attachment:
    #                 case._log_attachment_change(case, 'Updated')
    #     return super(Cases, self).write(values)
    
    
    
    def _log_attachment_change(self, case, action):
        # Log the change in the trail (you can customize this part)
        attachment_name = "Attachment"
        if action == 'Created':
            action_description = f"Created attachment: {attachment_name}"
        elif action == 'Updated':
            action_description = f"Updated attachment: {attachment_name}"

        # Log the message
        self.env['mail.message'].create({
            'model': 'case',
            'res_id': case.id,
            'message_type': 'notification',
            'body': action_description,
            'subject': action_description,
        })

    # Add this method to your class
    @api.model
    def get_formview_action(self, records):
        """Override to provide custom next action after form save"""
        result = super(Cases, self).get_formview_action(records)
        
        if self.env.context.get('next_action') == 'all_open_case':
            return {
                'type': 'ir.actions.act_window',
                'name': 'All Open Cases',
                'res_model': 'case',  
                'view_mode': 'tree,form',
                'view_id': self.env.ref('case_management.case_action').id,  
                'target': 'current',
                'domain': [('status', '=', 'open')],  
            }
            
        return result


    
    
    
    
    
    

    @api.model
    def default_get(self, fields_list):
        defaults = super(Cases, self).default_get(fields_list)
        if 'created_at' in fields_list and 'created_at' not in defaults:
            defaults['created_at'] = fields.Datetime.now()
        return defaults
    
   
   
   
    # EMAIL ALERT
    
    
    # 
    
    
    

    def _send_case_creation_alert(self):
        """Send email alert when a case is created"""
        self.ensure_one()
        template = self.env.ref('case_management.case_creation_alert_template')

        if not template:
            _logger.error("Case creation email template not found")
            return False
        
        # Prepare recipient data
        staff_user = self.staff_id
        creator_user = self.user_id
        supervisors = [self.supervisor_one_id]
        if self.supervisor_two_id:
            supervisors.append(self.supervisor_two_id)
        if self.supervisor_three_id:
            supervisors.append(self.supervisor_three_id)
        
        # CC list - creator and supervisors
        cc_emails = []
        if creator_user and creator_user.email:
            cc_emails.append(creator_user.email)
        for supervisor in supervisors:
            if supervisor and supervisor.email:
                cc_emails.append(supervisor.email)
        
        # Prepare mail values
        mail_values = {
            'email_to': staff_user.email if staff_user else '',
            'email_cc': ','.join(cc_emails),
            'email_from': 'noreply@example.com',
        }

        
        for rec in self:
            alert_id = rec._generate_alert_id()
            _logger.info(f"This alert has ID: {alert_id} + {rec.id}")
            # Extract values directly from the record fields (no self in ctx)
            event_date = rec.event_date
            alert_id = alert_id
            alert_name = rec.name
            
            # Extract just the severity level (Low, Medium, or High) from alert_name
            severity_level = ''
            if alert_name:
                if 'Low' in alert_name:
                    severity_level = 'Low'
                elif 'Medium' in alert_name:
                    severity_level = 'Medium'
                elif 'High' in alert_name:
                    severity_level = 'High'
            
            title = rec.cases_description
            attachment = rec.attachment
            rating_name = rec.rating_id.name if rec.rating_id else ''
            staff_dept = rec.team_id.name if rec.team_id else ''
            status_name = rec.status_name.capitalize()
            exception_process = rec.new_process_id.name if rec.new_process_id else ''
           # process_type = rec.root_category_id.name if rec.root_category_id else ''
            process_type = rec.new_process_id.name if rec.new_process_id else ''
            process_category = rec.new_process_category_id.name if rec.new_process_category_id else ''
            case_action = rec.cases_action
            description = rec.further_description
            response_link = f'/web#id={rec.id}&model=case&view_type=form'
            
        # Context for email template
            ctx = {
                'event_date': event_date,
                'alert_id' : alert_id,
                'attachement':attachment,
                'alert_name': alert_name,
                'severity_level':severity_level,
                'title': title,
                'rating_name': rating_name,
                'staff_dept': staff_dept,
                'status_name': status_name,
                'exception_process': exception_process,
                'process_type': process_type,
                'process_category': process_category,
                'case_action': case_action,
                'description': description,
                'response_link': response_link,
                'creator_user_name': creator_user.name if creator_user else '',
                'creator_user_email': creator_user.email if creator_user else ''
            }

            _logger.info("Rendering template with context:")
            for k, v in ctx.items():
                _logger.info(f"{k}: {v}")

            try:
                # Use the template with the context
                template_id = template.with_context(**ctx)
                
                # Render the email content using the context
                rendered_html = template_id._render_template(
                    template_id.body_html,
                    template_id.model,
                    [self.id],
                    engine='qweb',
                    add_context=ctx
                )[self.id]

                # Optional: log the rendered HTML
                _logger.debug(f"Rendered email HTML for case {self.id}:\n{rendered_html}")
                
                
                
                
                
                # Add attachment to email if available
                if self.attachment:
                    attachment_rec = self.env['ir.attachment'].create({
                        'name': f'{self.name}_attachment',  
                        'type': 'binary',
                        'datas': self.attachment,
                        'res_model': 'case',
                        'res_id': self.id,
                        'mimetype': 'application/octet-stream',
                    })
                    mail_values['attachment_ids'] = [attachment_rec.id]

                # Send the email
                email_result = template_id.send_mail(
                    self.id,
                    force_send=True,
                    raise_exception=True,
                    email_values=mail_values
                )

                _logger.info(f"Case creation alert sent for case {self.id}")
                
                # Check if the email was sent successfully
                mail = self.env['mail.mail'].browse(email_result)
                if mail.state == 'sent':
                    model_description = self._description
                    # Register the alert in alert_history
                    self.env['alert.history'].sudo(flag=True).create({
                        #'alert_id': alert_id,
                        #'attachment_data': attachment_rec and str(attachment_rec) or None,
                        #'attachment_link': attachment_rec and f'/web/content/{attachment_rec}' or None,
                        'html_body': rendered_html,
                        'ref_id': f"{self._name},{self.id}",  # Reference to the case model
                        'risk_rating': severity_level or 'Low',
                        'process_id': exception_process or None,
                        'name': alert_name,
                        'date_created': fields.Datetime.now(),
                        'narration': title,
                        'email': mail_values.get('email_to', ''),
                        'email_cc': mail_values.get('email_cc', ''),
                        'source': model_description,
                        'last_checked': fields.Datetime.now()
                    })
                    print(f"Alert for case {self.id} Logged successfully")
                    _logger.info(f"Case creation alert sent and registered in alert_history for case {self.id}")
                    
                return True

            except Exception as e:
                _logger.error(f"Failed to send case creation alert for case {self.id}: {str(e)}")
                return False


    def _send_case_response_alert(self, response):
        """Send email alert when a case receives a response"""
        _logger.info(f"Starting _send_case_response_alert for case {self.id} with response {response.id}")
        self.ensure_one()
        
        template = self.env.ref('case_management.case_response_alert_template')

        if not template:
            _logger.error("Case response alert email template not found")
            return False
        
        # Prepare recipient data
        creator_user = self.user_id
        staff_user = self.staff_id
        responder_user = response.create_uid
        supervisors = [self.supervisor_one_id]
        if self.supervisor_two_id:
            supervisors.append(self.supervisor_two_id)
        if self.supervisor_three_id:
            supervisors.append(self.supervisor_three_id)
        
        # CC list - staff and supervisors
        cc_emails = []
        if staff_user and staff_user.email:
            cc_emails.append(staff_user.email)
        for supervisor in supervisors:
            if supervisor and supervisor.email:
                cc_emails.append(supervisor.email)
        
        # Prepare mail values
        mail_values = {
            'email_to': creator_user.email if creator_user and creator_user.email else '',
            'email_cc': ','.join(cc_emails),
            'email_from': 'noreply@example.com',
        }
        
        # Check if we have valid recipient
        if not mail_values['email_to']:
            _logger.warning(f"No valid email_to address for case {self.id} - creator email missing")
            return False

        for rec in self:
            alert_id = rec._generate_alert_id()
            # Extract values directly from the record fields
            event_date = rec.event_date
            alert_id = alert_id
            alert_name = rec.name
            
            # Extract just the severity level (Low, Medium, or High) from alert_name
            severity_level = ''
            if alert_name:
                if 'Low' in alert_name:
                    severity_level = 'Low'
                elif 'Medium' in alert_name:
                    severity_level = 'Medium'
                elif 'High' in alert_name:
                    severity_level = 'High'
            
            title = rec.cases_description
            rating_name = rec.rating_id.name if rec.rating_id else ''
            staff_dept = rec.team_id.name if rec.team_id else ''
            status_name = rec.status_name.capitalize()
            exception_process = rec.new_process_id.name if rec.new_process_id else ''
           # process_type = rec.root_category_id.name if rec.root_category_id else ''
            process_type = rec.new_process_id.name if rec.new_process_id else ''
            process_category = rec.new_process_category_id.name if rec.new_process_category_id else ''
            case_action = rec.cases_action
            response_text = response.response
            user_name = rec.user_id.name if rec.user_id else ''
            user_email = rec.user_id.email if rec.user_id else ''
            creator_name = creator_user.name if creator_user else ''
            creator_email = creator_user.email if creator_user else ''
            responder_name = responder_user.name if responder_user else ''
            responder_email = responder_user.email if responder_user else ''
            response_link = f'/web#id={rec.id}&model=case&view_type=form'
            
            # Context for email template
            ctx = {
                'event_date': event_date,
                'alert_id': alert_id,
                'alert_name': alert_name,
                'severity_level':severity_level,
                'title': title,
                'rating_name': rating_name,
                'staff_dept': staff_dept,
                'status_name': status_name,
                'exception_process': exception_process,
                'process_type': process_type,
                'process_category': process_category,
                'case_action': case_action,
                'response': response_text,
                'creator_name':creator_name,
                'creator_email':creator_email,
                'user_name': user_name,
                'user_email': user_email,
                'responder_name': responder_name,
                'responder_email': responder_email,
                'response_link': response_link
            }

            _logger.info("Rendering template with context:")
            for k, v in ctx.items():
                _logger.info(f"{k}: {v}")

            try:
                # Use the template with the context
                template_id = template.with_context(**ctx)
                
                # Render the email content using the context
                rendered_html = template_id._render_template(
                    template_id.body_html,
                    template_id.model,
                    [rec.id],
                    engine='qweb',
                    add_context=ctx
                )[rec.id]

                # Optional: log the rendered HTML
                _logger.debug(f"Rendered email HTML for case {rec.id}:\n{rendered_html}")
                
                
                


                # Send the email
                email_result = template_id.send_mail(
                    rec.id,
                    force_send=True,
                    raise_exception=True,
                    email_values=mail_values
                )

                _logger.info(f"Case response alert sent for case {rec.id} - Result: {email_result}")
                
                # Check if the email was sent successfully
                mail = self.env['mail.mail'].browse(email_result)
                if mail.state == 'sent':
                    model_description = self._description
                    # Register the alert in alert_history
                    self.env['alert.history'].sudo(flag=True).create({
                        'alert_id': alert_id,
                        #'attachment_data': attachment_rec and str(attachment_rec) or None,
                        #'attachment_link': attachment_rec and f'/web/content/{attachment_rec}' or None,
                        'html_body': rendered_html,
                        'ref_id': f'case,{self.id}',  # Reference to the case model
                        'risk_rating': severity_level or 'Low',
                        'process_id': exception_process or None,
                        'name': alert_name,
                        'date_created': fields.Datetime.now(),
                        'narration': title,
                        'email': mail_values.get('email_to', ''),
                        'email_cc': mail_values.get('email_cc', ''),
                        'source': model_description,
                        'last_checked': fields.Datetime.now()
                    })
                    print(f"Alert for case {self.id} Logged successfully")
                    _logger.info(f"Case Response alert sent and registered in alert_history for case {self.id}")
                    
                
                
                
                # Add a note in the chatter about the email
                self.message_post(
                    body=f"<p>Case response notification email sent to {mail_values['email_to']}</p>",
                    subtype_xmlid='mail.mt_note'
                )
                
                return True

            except Exception as e:
                _logger.error(f"Failed to send case response alert for case {rec.id}: {str(e)}")
                return False

            
        
        
            
        
    def _send_case_closure_alert(self):
        """Send email alert when a case is closed"""
        _logger.info(f"Starting _send_case_closure_alert for case {self.id}")
        self.ensure_one()
        
        try:
            template = self.env.ref('case_management.case_closure_alert_template')
            
            if not template:
                _logger.error("Case closure email template not found")
                return False
            
            _logger.info(f"Found template: {template.name} (ID: {template.id})")
            
            # Prepare recipient data
            staff_user = self.staff_id
            creator_user = self.user_id
            supervisors = [self.supervisor_one_id]
            if self.supervisor_two_id:
                supervisors.append(self.supervisor_two_id)
            if self.supervisor_three_id:
                supervisors.append(self.supervisor_three_id)
            
            _logger.info(f"Recipients - Staff: {staff_user.name if staff_user else 'None'}, "
                        f"Creator: {creator_user.name if creator_user else 'None'}")
            
            # CC list - creator and supervisors
            cc_emails = []
            if creator_user and creator_user.email:
                cc_emails.append(creator_user.email)
            for supervisor in supervisors:
                if supervisor and supervisor.email:
                    cc_emails.append(supervisor.email)
            
            # Prepare mail values
            mail_values = {
                'email_to': staff_user.email if staff_user and staff_user.email else '',
                'email_cc': ','.join(cc_emails),
                'email_from': 'noreply@example.com',
            }
            
            _logger.info(f"Mail values: To={mail_values['email_to']}, CC={mail_values['email_cc']}")
            
            # Check if we have valid recipient
            if not mail_values['email_to']:
                _logger.warning(f"No valid email_to address for case {self.id} - staff email missing")
                return False

            for rec in self:
                _logger.info(f"Processing record ID: {rec.id}")
                alert_id = rec._generate_alert_id()
                
                # Extract values directly from the record fields
                try:
                    event_date = rec.event_date
                    alert_id =  alert_id 
                    alert_name = rec.name
                    
                    # Extract just the severity level (Low, Medium, or High) from alert_name
                    severity_level = ''
                    if alert_name:
                        if 'Low' in alert_name:
                            severity_level = 'Low'
                        elif 'Medium' in alert_name:
                            severity_level = 'Medium'
                        elif 'High' in alert_name:
                            severity_level = 'High'
                    title = rec.cases_description
                    rating_name = rec.rating_id.name if rec.rating_id else ''
                    staff_dept = rec.team_id.name if rec.team_id else ''
                    status_name = rec.status_name.capitalize()
                    exception_process = rec.new_process_id.name if rec.new_process_id else ''
                   # process_type = rec.root_category_id.name if rec.root_category_id else ''
                    process_type = rec.new_process_id.name if rec.new_process_id else ''
                    process_category = rec.new_process_category_id.name if rec.new_process_category_id else ''
                    case_action = rec.cases_action
                    close_remarks = "Case closed successfully"
                    user_name = rec.user_id.name if rec.user_id else ''
                    user_email = rec.user_id.email if rec.user_id else ''
                    response_link = f'/web#id={rec.id}&model=case&view_type=form'
                except Exception as e:
                    _logger.error(f"Error extracting field values: {str(e)}")
                    return False
                
                # Context for email template - use direct variable names, not nested dictionary
                ctx = {
                    'event_date': event_date,
                    'alert_name': alert_name,
                    'alert_id': alert_id,
                    'severity_level':severity_level,
                    'title': title,
                    'rating_name': rating_name,
                    'staff_dept': staff_dept,
                    'status_name': status_name,
                    'exception_process': exception_process,
                    'process_type': process_type,
                    'process_category': process_category,
                    'case_action': case_action,
                    'close_remarks': close_remarks,
                    'user_name': user_name,
                    'user_email': user_email,
                    'response_link': response_link,
                    'creator_user_name': creator_user.name if creator_user else '',
                    'creator_user_email': creator_user.email if creator_user else ''
                }

                _logger.info("Rendering template with context:")
                for k, v in ctx.items():
                    _logger.info(f"{k}: {v}")

                try:
                    # Use the template with the context
                    template_id = template.with_context(**ctx)
                    
                    _logger.info(f"Preparing to render template for case {rec.id}")
                    
                    # Render the email content using the context
                    rendered_html = template_id._render_template(
                        template_id.body_html,
                        template_id.model,
                        [rec.id],
                        engine='qweb',
                        add_context=ctx
                    )[rec.id]

                    _logger.info(f"Successfully rendered HTML template for case {rec.id}")
                    _logger.debug(f"Rendered email HTML for case {rec.id}:\n{rendered_html[:500]}...")

                    _logger.info(f"Preparing to send email for case {rec.id}")
                    
                    
                    # Log the alert history in alert_management module
                    # rec._log_alert_history(
                    #     email_to=mail_values['email_to'],
                    #     email_cc=mail_values['email_cc'],
                    #     alert_id=ctx['alert_id'],
                    #     html_body=rendered_html,
                    #     source='case_creation',
                    #     name=rec.name,
                    #     user_name=creator_user.name if creator_user else 'Unknown'
                    # )

                    
                    # Send the email
                    email_result = template_id.send_mail(
                        rec.id,
                        force_send=True,
                        raise_exception=True,
                        email_values=mail_values
                    )

                    _logger.info(f"Case closure alert sent for case {rec.id} - Result: {email_result}")
                    
                    # Check if the email was sent successfully
                    mail = self.env['mail.mail'].browse(email_result)
                    if mail.state == 'sent':
                        model_description = self._description
                        # Register the alert in alert_history
                        self.env['alert.history'].sudo(flag=True).create({
                            'alert_id': alert_id,
                            #'attachment_data': attachment_rec and str(attachment_rec) or None,
                            #'attachment_link': attachment_rec and f'/web/content/{attachment_rec}' or None,
                            'html_body': rendered_html,
                            'ref_id': f'case,{self.id}',  # Reference to the case model
                            'risk_rating': severity_level or 'Low',
                            'process_id': exception_process or None,
                            'name': alert_name,
                            'date_created': fields.Datetime.now(),
                            'narration': title,
                            'email': mail_values.get('email_to', ''),
                            'email_cc': mail_values.get('email_cc', ''),
                            'source': model_description,
                            'last_checked': fields.Datetime.now()
                        })
                        print(f"Alert for case {self.id} Logged successfully")
                        _logger.info(f"Case creation alert sent and registered in alert_history for case {self.id}")
                        
                        
                    return True

                except Exception as e:
                    import traceback
                    _logger.error(f"Failed to send case closure alert for case {rec.id}: {str(e)}")
                    _logger.error(f"Traceback: {traceback.format_exc()}")
                    return False
        except Exception as e:
            import traceback
            _logger.error(f"Unexpected error in _send_case_closure_alert: {str(e)}")
            _logger.error(f"Traceback: {traceback.format_exc()}")
            return False
        
        
    # Method to add to the Case model in Python
    
    def action_view_email_template(self):
        """Preview the email template rendered with context"""
        self.ensure_one()

        # Select the right template
        if not self.has_responses:
            template = self.env.ref('case_management.case_creation_alert_template')
        elif self.statuses == 'closed':
            template = self.env.ref('case_management.case_closure_alert_template')
        else:
            template = self.env.ref('case_management.case_response_alert_template')

        if not template:
            raise UserError("Email template not found.")

        # Use generate_email to render with context
        # email_content = template.generate_email(self.id)
        rendered_html = template._render_field('body_html', self.ids)[self.id]


        # Create a mail.compose.message wizard to display the preview
        ctx = {
            'default_model': 'case',  # Adjust to your model's actual technical name
            'default_res_id': self.id,
            'default_use_template': True,
            'default_template_id': template.id,
            'default_composition_mode': 'comment',
        }

        return {
            'type': 'ir.actions.act_window',
            'view_mode': 'form',
            'res_model': 'mail.compose.message',
            'target': 'new',
            'context': ctx,
        }

    
    
    # def action_view_email_template(self):
    #     """Show the email template for this case alert"""
    #     self.ensure_one()
        
    #     # Determine which template to show based on case status
    #     if not self.has_responses:
    #         template = self.env.ref('case_management.case_creation_alert_template')
    #     elif self.statuses == 'closed':
    #         template = self.env.ref('case_management.case_closure_alert_template')
    #     else:
    #         template = self.env.ref('case_management.case_response_alert_template')
        
    #     if not template:
    #         raise UserError(("Email template not found"))
        
    #     # Open the email template in form view
    #     return {
    #         'name': 'Email Template',
    #         'type': 'ir.actions.act_window',
    #         'res_model': 'mail.template',
    #         'res_id': template.id,
    #         'view_mode': 'form',
    #         'target': 'new',
    #     }
    
    
    def _log_alert_history(self, email_to, alert_id, email_cc, html_body, source, name, user_name):
        """Helper function to log email alert details in alert.history from alert_management module"""
        alert_history = self.env['alert_management.alert.history'].create({
            'alert_id': alert_id,  
            'html_body': html_body,
            'email': email_to,
            'email_cc': email_cc,
            'source': source,
            'name': name,
            'date_created': fields.Datetime.now(),
            'narration': f"Alert sent to {email_to} from {user_name}",
        })

        return alert_history

    
    
    
    def _generate_alert_id(self):
        """Generate a unique alert ID using uuid"""
        return 'ALERT-' + str(uuid.uuid4())
    
    


 