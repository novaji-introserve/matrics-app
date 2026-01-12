# models/email_template_config.py

from odoo import models, fields, api
from odoo.exceptions import ValidationError, UserError
import re
import base64
import logging

_logger = logging.getLogger(__name__)


class EmailTemplateConfig(models.Model):
    _name = 'email.template.config'
    _description = 'Email Template Configuration for Alert System'
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _order = 'is_active desc, name'
    _rec_name = 'name'
    
    # Basic Information
    name = fields.Char(
        string='Template Name',
        required=True,
        tracking=True,
        help="Name to identify this email template configuration"
    )
    
    company_id = fields.Many2one(
        'res.company',
        string='Company',
        required=True,
        default=lambda self: self.env.company,
        tracking=True
    )
    
    is_active = fields.Boolean(
        string='Active',
        default=False,
        tracking=True,
        help="Only one template can be active per company at a time"
    )
    
    description = fields.Text(
        string='Description',
        help="Optional description for this template configuration"
    )
    
    # ========================================
    # BRANDING FIELDS
    # ========================================
    
    # Logo Configuration
    use_company_logo = fields.Boolean(
        string='Use Company Logo',
        default=True,
        help="Use the company logo from company settings"
    )
    
    custom_logo = fields.Binary(
        string='Custom Logo',
        help="Upload a custom logo for email templates (PNG/JPG only)"
    )
    
    custom_logo_filename = fields.Char(string='Logo Filename')
    
    logo_width = fields.Integer(
        string='Logo Width (px)',
        default=192,
        help="Logo width in pixels (default: 192px = 12rem)"
    )
    
    logo_height = fields.Integer(
        string='Logo Height (px)',
        default=192,
        help="Logo height in pixels (default: 192px = 12rem)"
    )
    
    # Primary Brand Colors
    primary_brand_color = fields.Char(
        string='Primary Brand Color',
        default='#007046',
        help="Main brand color for headers and highlights"
    )
    
    secondary_brand_color = fields.Char(
        string='Secondary Brand Color',
        default='#28a745',
        help="Secondary color for buttons and accents"
    )
    
    # ========================================
    # DETAILED COLOR CONFIGURATION
    # ========================================
    
    # Table Colors
    table_header_bg_color = fields.Char(
        string='Table Header Background',
        default='#007046',
        help="Background color for table headers"
    )
    
    table_header_text_color = fields.Char(
        string='Table Header Text',
        default='#ffffff',
        help="Text color for table headers"
    )
    
    table_border_color = fields.Char(
        string='Table Border Color',
        default='#dddddd',
        help="Color for table borders"
    )
    
    table_row_even_color = fields.Char(
        string='Table Even Row Color',
        default='#f9f9f9',
        help="Background color for even table rows"
    )
    
    table_row_odd_color = fields.Char(
        string='Table Odd Row Color',
        default='#ffffff',
        help="Background color for odd table rows"
    )
    
    # Button Colors
    button_bg_color = fields.Char(
        string='Button Background',
        default='#28a745',
        help="Background color for buttons"
    )
    
    button_text_color = fields.Char(
        string='Button Text Color',
        default='#ffffff',
        help="Text color for buttons"
    )
    
    button_hover_color = fields.Char(
        string='Button Hover Color',
        default='#218838',
        help="Button color on hover"
    )
    
    button_border_radius = fields.Integer(
        string='Button Border Radius (px)',
        default=8,
        help="Border radius for buttons in pixels"
    )
    
    # Link Colors
    link_color = fields.Char(
        string='Link Color',
        default='#454748',
        help="Color for links in email"
    )
    
    link_hover_color = fields.Char(
        string='Link Hover Color',
        default='#007046',
        help="Link color on hover"
    )
    
    # Background Colors
    email_bg_color = fields.Char(
        string='Email Background',
        default='#ffffff',
        help="Main background color for email"
    )
    
    content_bg_color = fields.Char(
        string='Content Background',
        default='#ffffff',
        help="Background color for content areas"
    )
    
    # ========================================
    # FOOTER CONFIGURATION
    # ========================================
    
    use_company_details = fields.Boolean(
        string='Use Company Details',
        default=True,
        help="Use company name, phone, email from company settings"
    )
    
    custom_company_name = fields.Char(
        string='Custom Company Name',
        help="Override company name in footer"
    )
    
    custom_phone = fields.Char(
        string='Custom Phone',
        help="Override phone number in footer"
    )
    
    custom_email = fields.Char(
        string='Custom Email',
        help="Override email address in footer"
    )
    
    custom_website = fields.Char(
        string='Custom Website',
        help="Override website URL in footer"
    )
    
    footer_bg_color = fields.Char(
        string='Footer Background',
        default='#ffffff',
        help="Background color for footer"
    )
    
    footer_text_color = fields.Char(
        string='Footer Text Color',
        default='#454748',
        help="Text color for footer"
    )
    
    show_footer = fields.Boolean(
        string='Show Footer',
        default=True,
        help="Show/hide the footer section"
    )
    
    # ========================================
    # TYPOGRAPHY CONFIGURATION
    # ========================================
    
    font_family = fields.Selection([
        ('verdana', 'Verdana, Arial, sans-serif'),
        ('arial', 'Arial, sans-serif'),
        ('helvetica', 'Helvetica, Arial, sans-serif'),
        ('georgia', 'Georgia, serif'),
        ('times', 'Times New Roman, serif'),
        ('courier', 'Courier New, monospace'),
    ], string='Font Family', default='verdana', help="Font family for email content")
    
    heading_font_size = fields.Integer(
        string='Heading Font Size (px)',
        default=24,
        help="Font size for main headings"
    )
    
    subheading_font_size = fields.Integer(
        string='Subheading Font Size (px)',
        default=18,
        help="Font size for subheadings"
    )
    
    body_font_size = fields.Integer(
        string='Body Font Size (px)',
        default=14,
        help="Font size for body text"
    )
    
    small_font_size = fields.Integer(
        string='Small Font Size (px)',
        default=12,
        help="Font size for small text (footer, captions)"
    )
    
    # ========================================
    # EMAIL LAYOUT CONFIGURATION
    # ========================================
    
    email_width = fields.Integer(
        string='Email Width (px)',
        default=590,
        help="Maximum width of email content"
    )
    
    content_padding = fields.Integer(
        string='Content Padding (px)',
        default=16,
        help="Padding around content areas"
    )
    
    # ========================================
    # TRACKING FIELDS
    # ========================================
    
    created_by = fields.Many2one(
        'res.users',
        string='Created By',
        default=lambda self: self.env.user,
        readonly=True
    )
    
    last_used_date = fields.Datetime(
        string='Last Used',
        readonly=True,
        help="When this template was last used to send an email"
    )
    
    usage_count = fields.Integer(
        string='Usage Count',
        default=0,
        readonly=True,
        help="Number of times this template has been used"
    )
    
    # ========================================
    # COMPUTED FIELDS
    # ========================================
    
    @api.depends('custom_logo', 'company_id.logo', 'use_company_logo')
    def _compute_effective_logo(self):
        """Compute which logo to actually use"""
        for record in self:
            if record.use_company_logo and record.company_id.logo:
                record.effective_logo = record.company_id.logo
            elif record.custom_logo:
                record.effective_logo = record.custom_logo
            else:
                record.effective_logo = False
    
    effective_logo = fields.Binary(
        string='Effective Logo',
        compute='_compute_effective_logo',
        help="The logo that will actually be used in emails"
    )
    
    @api.depends('custom_company_name', 'company_id.name', 'use_company_details')
    def _compute_effective_company_name(self):
        for record in self:
            if record.use_company_details:
                record.effective_company_name = record.company_id.name
            else:
                record.effective_company_name = record.custom_company_name or record.company_id.name
    
    effective_company_name = fields.Char(
        string='Effective Company Name',
        compute='_compute_effective_company_name'
    )
    
    # ========================================
    # CONSTRAINTS AND VALIDATIONS
    # ========================================
    
    @api.constrains('custom_logo', 'custom_logo_filename')
    def _validate_logo_format(self):
        """Validate logo file format"""
        for record in self:
            if record.custom_logo and record.custom_logo_filename:
                filename = record.custom_logo_filename.lower()
                if not (filename.endswith('.png') or filename.endswith('.jpg') or filename.endswith('.jpeg')):
                    raise ValidationError("Logo must be in PNG or JPG format only.")
                
                # Check file size (max 5MB)
                if record.custom_logo:
                    logo_data = base64.b64decode(record.custom_logo)
                    if len(logo_data) > 5 * 1024 * 1024:  # 5MB
                        raise ValidationError("Logo file size must be less than 5MB.")
    
    @api.constrains('primary_brand_color', 'secondary_brand_color', 'table_header_bg_color', 
                    'table_header_text_color', 'button_bg_color', 'button_text_color',
                    'link_color', 'email_bg_color', 'footer_bg_color', 'footer_text_color')
    def _validate_hex_colors(self):
        """Validate hex color codes"""
        hex_pattern = re.compile(r'^#[0-9A-Fa-f]{6}$')
        
        color_fields = [
            'primary_brand_color', 'secondary_brand_color', 'table_header_bg_color',
            'table_header_text_color', 'table_border_color', 'table_row_even_color',
            'table_row_odd_color', 'button_bg_color', 'button_text_color', 'button_hover_color',
            'link_color', 'link_hover_color', 'email_bg_color', 'content_bg_color',
            'footer_bg_color', 'footer_text_color'
        ]
        
        for record in self:
            for field_name in color_fields:
                color_value = getattr(record, field_name)
                if color_value and not hex_pattern.match(color_value):
                    field_label = record._fields[field_name].string
                    raise ValidationError(f"{field_label} must be a valid hex color code (e.g., #007046)")
    
    @api.constrains('logo_width', 'logo_height', 'email_width')
    def _validate_dimensions(self):
        """Validate dimension values"""
        for record in self:
            if record.logo_width <= 0 or record.logo_width > 1000:
                raise ValidationError("Logo width must be between 1 and 1000 pixels.")
            if record.logo_height <= 0 or record.logo_height > 1000:
                raise ValidationError("Logo height must be between 1 and 1000 pixels.")
            if record.email_width < 300 or record.email_width > 1200:
                raise ValidationError("Email width must be between 300 and 1200 pixels.")
    
    # ========================================
    # BUSINESS METHODS
    # ========================================
    
    def action_activate_template(self):
        """Activate this template and deactivate others"""
        self.ensure_one()
        
        if self.is_active:
            raise UserError("This template is already active.")
        
        # Check if there are other active templates
        other_active = self.search([
            ('company_id', '=', self.company_id.id),
            ('is_active', '=', True),
            ('id', '!=', self.id)
        ])
        
        if other_active:
            return {
                'type': 'ir.actions.act_window',
                'name': 'Confirm Template Activation',
                'res_model': 'email.template.config.confirm',
                'view_mode': 'form',
                'target': 'new',
                'context': {
                    'default_template_id': self.id,
                    'default_other_active_templates': [(6, 0, other_active.ids)]
                }
            }
        else:
            # No other active templates, activate directly
            self.write({'is_active': True})
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': 'Template Activated',
                    'message': f'Template "{self.name}" has been activated successfully.',
                    'type': 'success'
                }
            }
    
    def action_deactivate_template(self):
        """Deactivate this template"""
        self.ensure_one()
        self.write({'is_active': False})
        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': 'Template Deactivated',
                'message': f'Template "{self.name}" has been deactivated.',
                'type': 'success'
            }
        }
    
    def action_preview_email(self):
        """Preview email with current template settings"""
        self.ensure_one()
        
        # Create a sample alert history record for preview
        sample_data = {
            'alert_id': 'PREVIEW-12345',
            'name': 'Sample Alert Preview',
            'narration': 'This is a preview of how your email template will look with the current configuration.',
            'email': 'preview@example.com',
            'email_cc': 'cc@example.com',
            'source': 'preview',
            'html_body': self._generate_sample_table(),
            'last_checked': fields.Datetime.now(),
        }
        
        # Generate preview HTML
        preview_html = self._generate_preview_html(sample_data)
        
        return {
            'type': 'ir.actions.act_window',
            'name': 'Email Template Preview',
            'res_model': 'email.template.preview',
            'view_mode': 'form',
            'target': 'new',
            'context': {
                'default_template_id': self.id,
                'default_preview_html': preview_html
            }
        }
    
    def _generate_sample_table(self):
        """Generate sample table HTML for preview"""
        return f"""
        <div style="overflow-x: auto; margin: 10px 0;">
            <table style="border-collapse: collapse; font-family: Arial, sans-serif; width: 100%;">
                <thead>
                    <tr>
                        <th style='padding: 8px; background-color: {self.table_header_bg_color}; color: {self.table_header_text_color}; border: 1px solid {self.table_border_color};'>Account Name</th>
                        <th style='padding: 8px; background-color: {self.table_header_bg_color}; color: {self.table_header_text_color}; border: 1px solid {self.table_border_color};'>Amount</th>
                        <th style='padding: 8px; background-color: {self.table_header_bg_color}; color: {self.table_header_text_color}; border: 1px solid {self.table_border_color};'>Narration</th>
                        <th style='padding: 8px; background-color: {self.table_header_bg_color}; color: {self.table_header_text_color}; border: 1px solid {self.table_border_color};'>Status</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td style='padding: 8px; border: 1px solid {self.table_border_color}; background-color: {self.table_row_even_color};'>Sample Account 1</td>
                        <td style='padding: 8px; border: 1px solid {self.table_border_color}; background-color: {self.table_row_even_color};'>$15,000.00</td>
                        <td style='padding: 8px; border: 1px solid {self.table_border_color}; background-color: {self.table_row_even_color};'>crypto transaction</td>
                        <td style='padding: 8px; border: 1px solid {self.table_border_color}; background-color: {self.table_row_even_color};'>Completed</td>
                    </tr>
                    <tr>
                        <td style='padding: 8px; border: 1px solid {self.table_border_color}; background-color: {self.table_row_odd_color};'>Sample Account 2</td>
                        <td style='padding: 8px; border: 1px solid {self.table_border_color}; background-color: {self.table_row_odd_color};'>$25,000.00</td>
                        <td style='padding: 8px; border: 1px solid {self.table_border_color}; background-color: {self.table_row_odd_color};'>crypto payment</td>
                        <td style='padding: 8px; border: 1px solid {self.table_border_color}; background-color: {self.table_row_odd_color};'>Pending</td>
                    </tr>
                </tbody>
            </table>
        </div>
        """
    
    def _generate_preview_html(self, sample_data):
        """Generate complete preview HTML"""
        # This would generate the full email HTML using the template
        # For now, return a simplified version
        return f"""
        <div style="font-family: {dict(self._fields['font_family'].selection)[self.font_family]}; background-color: {self.email_bg_color};">
            <h2 style="color: {self.primary_brand_color};">Email Template Preview</h2>
            <p>This is how your emails will look with the current configuration.</p>
            {sample_data['html_body']}
            <div style="margin-top: 20px; padding: 10px; background-color: {self.footer_bg_color}; color: {self.footer_text_color};">
                <strong>{self.effective_company_name}</strong><br/>
                Phone: {self.custom_phone or 'Not set'}<br/>
                Email: {self.custom_email or 'Not set'}
            </div>
        </div>
        """
    
    @api.model
    def get_active_template(self, company_id=None):
        """Get the active email template for a company"""
        if not company_id:
            company_id = self.env.company.id
        
        active_template = self.search([
            ('company_id', '=', company_id),
            ('is_active', '=', True)
        ], limit=1)
        
        return active_template
    
    def increment_usage(self):
        """Increment usage count and update last used date"""
        self.ensure_one()
        self.write({
            'usage_count': self.usage_count + 1,
            'last_used_date': fields.Datetime.now()
        })
    
    # ========================================
    # LIFECYCLE METHODS
    # ========================================
    
    @api.model
    def create(self, vals):
        """Override create to handle activation logic"""
        record = super(EmailTemplateConfig, self).create(vals)
        
        # If this is the first template for the company, make it active
        if not self.search([('company_id', '=', record.company_id.id), ('id', '!=', record.id)]):
            record.write({'is_active': True})
        
        return record
    
    def write(self, vals):
        """Override write to handle activation logic"""
        if 'is_active' in vals and vals['is_active']:
            # Deactivate other templates for the same company
            for record in self:
                other_templates = self.search([
                    ('company_id', '=', record.company_id.id),
                    ('id', '!=', record.id),
                    ('is_active', '=', True)
                ])
                other_templates.write({'is_active': False})
        
        return super(EmailTemplateConfig, self).write(vals)
    
    def unlink(self):
        """Prevent deletion of active templates"""
        active_templates = self.filtered('is_active')
        if active_templates:
            raise UserError("Cannot delete active email templates. Please deactivate them first.")
        return super(EmailTemplateConfig, self).unlink()


# ========================================
# CONFIRMATION WIZARD
# ========================================

class EmailTemplateConfigConfirm(models.TransientModel):
    _name = 'email.template.config.confirm'
    _description = 'Email Template Activation Confirmation'
    
    template_id = fields.Many2one('email.template.config', string='Template to Activate', required=True)
    other_active_templates = fields.Many2many('email.template.config', string='Templates to Deactivate')
    
    def action_confirm_activation(self):
        """Confirm activation and deactivate others"""
        # Deactivate other templates
        self.other_active_templates.write({'is_active': False})
        
        # Activate the selected template
        self.template_id.write({'is_active': True})
        
        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': 'Template Activated',
                'message': f'Template "{self.template_id.name}" has been activated. {len(self.other_active_templates)} other template(s) have been deactivated.',
                'type': 'success'
            }
        }


# ========================================
# PREVIEW WIZARD
# ========================================

class EmailTemplatePreview(models.TransientModel):
    _name = 'email.template.preview'
    _description = 'Email Template Preview'
    
    template_id = fields.Many2one('email.template.config', string='Template', required=True)
    preview_html = fields.Html(string='Preview', readonly=True)